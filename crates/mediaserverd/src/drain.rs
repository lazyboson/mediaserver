use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};

pub const DRAIN_TIMEOUT_ENV: &str = "MSS_DRAIN_TIMEOUT_SECS";
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
pub const EVENT_FLUSH_WINDOW: Duration = Duration::from_secs(10);

pub const STOP_ACCEPTING: &str = "stop-accepting";
pub const HAND_OFF_LEASES: &str = "hand-off-leases";
pub const CLOSE_SESSIONS: &str = "close-sessions";
pub const AWAIT_UPLOADS: &str = "await-uploads";
pub const CONTROL_PLANE_IDLE: &str = "control-plane-idle";
pub const FLUSH_EVENTS: &str = "flush-events";

const LEASE_HANDOFF_SHARE: u32 = 4;

#[derive(Default)]
pub struct DrainState {
    draining: AtomicBool,
}

impl DrainState {
    pub fn shared() -> Arc<DrainState> {
        Arc::new(DrainState::default())
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    pub fn begin(&self) -> bool {
        !self.draining.swap(true, Ordering::SeqCst)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    Interrupt,
    Terminate,
}

impl ShutdownSignal {
    pub fn name(self) -> &'static str {
        match self {
            ShutdownSignal::Interrupt => "SIGINT",
            ShutdownSignal::Terminate => "SIGTERM",
        }
    }
}

pub async fn next_shutdown_signal() -> ShutdownSignal {
    let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
    let mut terminate = match terminate {
        Ok(stream) => stream,
        Err(error) => {
            warn!(
                %error,
                "SIGTERM cannot be handled on this platform; only SIGINT will start a drain"
            );
            let _ = tokio::signal::ctrl_c().await;
            return ShutdownSignal::Interrupt;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => ShutdownSignal::Interrupt,
        _ = terminate.recv() => ShutdownSignal::Terminate,
    }
}

pub fn exit_on_second_signal() {
    tokio::spawn(async move {
        let signal = next_shutdown_signal().await;
        warn!(
            signal = signal.name(),
            "a second shutdown signal arrived; exiting without finishing the drain"
        );
        std::process::exit(0);
    });
}

pub fn drain_timeout() -> Duration {
    let configured = std::env::var(DRAIN_TIMEOUT_ENV)
        .ok()
        .filter(|configured| !configured.trim().is_empty());
    let Some(configured) = configured else {
        info!(
            env = DRAIN_TIMEOUT_ENV,
            value = "unset",
            seconds = DEFAULT_DRAIN_TIMEOUT.as_secs(),
            "the shutdown drain is bounded by the default window"
        );
        return DEFAULT_DRAIN_TIMEOUT;
    };
    match configured.trim().parse::<u64>() {
        Ok(seconds) => {
            info!(
                env = DRAIN_TIMEOUT_ENV,
                seconds, "the shutdown drain is bounded by this window"
            );
            Duration::from_secs(seconds)
        }
        Err(_) => {
            warn!(
                env = DRAIN_TIMEOUT_ENV,
                configured = %configured,
                seconds = DEFAULT_DRAIN_TIMEOUT.as_secs(),
                "the drain window must be whole seconds; falling back to the default"
            );
            DEFAULT_DRAIN_TIMEOUT
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SessionsClosed {
    pub closed: usize,
    pub failed: usize,
}

#[control_api::async_trait]
pub trait DrainSteps: Send + Sync {
    fn stop_accepting(&self);

    async fn hand_off_leases(&self) -> usize;

    async fn close_sessions(&self) -> SessionsClosed;

    async fn await_uploads(&self) -> usize {
        0
    }

    async fn await_control_plane_idle(&self);

    async fn flush_events(&self) -> u64;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DrainReport {
    pub finished: Vec<&'static str>,
    pub expired: Vec<&'static str>,
    pub skipped: Vec<&'static str>,
    pub leases_handed_off: usize,
    pub sessions: SessionsClosed,
    pub uploads_settled: usize,
    pub unsent_events: Option<u64>,
    pub elapsed: Duration,
}

impl DrainReport {
    pub fn clean(&self) -> bool {
        self.expired.is_empty() && self.skipped.is_empty() && self.sessions.failed == 0
    }

    fn finished_step(&mut self, step: &'static str, started: Instant) {
        info!(
            step,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "drain step finished"
        );
        self.finished.push(step);
    }

    fn expired_step(&mut self, step: &'static str, window: Duration) {
        warn!(
            step,
            window_ms = window.as_millis() as u64,
            "drain step did not finish inside its slice of the drain window"
        );
        self.expired.push(step);
    }

    fn skipped_step(&mut self, step: &'static str) {
        warn!(step, "drain step skipped; the drain window is spent");
        self.skipped.push(step);
    }
}

fn remaining(deadline: Instant) -> Option<Duration> {
    let now = Instant::now();
    if now >= deadline {
        return None;
    }
    Some(deadline - now)
}

fn slice(deadline: Instant, reserve: Duration, cap: Option<Duration>) -> Option<Duration> {
    let left = remaining(deadline)?.checked_sub(reserve)?;
    if left.is_zero() {
        return None;
    }
    Some(match cap {
        Some(cap) => left.min(cap),
        None => left,
    })
}

fn event_reserve(budget: Duration) -> Duration {
    EVENT_FLUSH_WINDOW.min(budget / 3)
}

pub async fn run_drain(steps: &dyn DrainSteps, budget: Duration) -> DrainReport {
    let started = Instant::now();
    let deadline = started + budget;
    let reserve = event_reserve(budget);
    let mut report = DrainReport::default();

    let step_started = Instant::now();
    steps.stop_accepting();
    report.finished_step(STOP_ACCEPTING, step_started);

    match slice(deadline, Duration::ZERO, Some(budget / LEASE_HANDOFF_SHARE)) {
        Some(window) => {
            let step_started = Instant::now();
            match tokio::time::timeout(window, steps.hand_off_leases()).await {
                Ok(handed) => {
                    report.leases_handed_off = handed;
                    report.finished_step(HAND_OFF_LEASES, step_started);
                }
                Err(_) => report.expired_step(HAND_OFF_LEASES, window),
            }
        }
        None => report.skipped_step(HAND_OFF_LEASES),
    }

    match slice(deadline, reserve, None) {
        Some(window) => {
            let step_started = Instant::now();
            match tokio::time::timeout(window, steps.close_sessions()).await {
                Ok(sessions) => {
                    report.sessions = sessions;
                    report.finished_step(CLOSE_SESSIONS, step_started);
                }
                Err(_) => report.expired_step(CLOSE_SESSIONS, window),
            }
        }
        None => report.skipped_step(CLOSE_SESSIONS),
    }

    match slice(deadline, reserve, None) {
        Some(window) => {
            let step_started = Instant::now();
            match tokio::time::timeout(window, steps.await_uploads()).await {
                Ok(settled) => {
                    report.uploads_settled = settled;
                    report.finished_step(AWAIT_UPLOADS, step_started);
                }
                Err(_) => report.expired_step(AWAIT_UPLOADS, window),
            }
        }
        None => report.skipped_step(AWAIT_UPLOADS),
    }

    match slice(deadline, reserve, None) {
        Some(window) => {
            let step_started = Instant::now();
            match tokio::time::timeout(window, steps.await_control_plane_idle()).await {
                Ok(()) => report.finished_step(CONTROL_PLANE_IDLE, step_started),
                Err(_) => report.expired_step(CONTROL_PLANE_IDLE, window),
            }
        }
        None => report.skipped_step(CONTROL_PLANE_IDLE),
    }

    match slice(deadline, Duration::ZERO, None) {
        Some(window) => {
            let step_started = Instant::now();
            match tokio::time::timeout(window, steps.flush_events()).await {
                Ok(unsent) => {
                    report.unsent_events = Some(unsent);
                    report.finished_step(FLUSH_EVENTS, step_started);
                }
                Err(_) => report.expired_step(FLUSH_EVENTS, window),
            }
        }
        None => report.skipped_step(FLUSH_EVENTS),
    }

    report.elapsed = started.elapsed();
    if report.clean() {
        info!(
            elapsed_ms = report.elapsed.as_millis() as u64,
            leases_handed_off = report.leases_handed_off,
            sessions_closed = report.sessions.closed,
            uploads_settled = report.uploads_settled,
            unsent_events = report.unsent_events,
            "drain complete"
        );
    } else {
        warn!(
            elapsed_ms = report.elapsed.as_millis() as u64,
            budget_ms = budget.as_millis() as u64,
            expired = ?report.expired,
            skipped = ?report.skipped,
            leases_handed_off = report.leases_handed_off,
            sessions_closed = report.sessions.closed,
            sessions_failed = report.sessions.failed,
            uploads_settled = report.uploads_settled,
            unsent_events = report.unsent_events,
            "drain incomplete; exiting anyway so the pod is replaced"
        );
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingSteps {
        order: Mutex<Vec<&'static str>>,
        hang: Option<&'static str>,
    }

    impl RecordingSteps {
        fn hanging_at(step: &'static str) -> RecordingSteps {
            RecordingSteps {
                order: Mutex::new(Vec::new()),
                hang: Some(step),
            }
        }

        fn note(&self, step: &'static str) {
            self.order.lock().unwrap().push(step);
        }

        fn order(&self) -> Vec<&'static str> {
            self.order.lock().unwrap().clone()
        }

        async fn hang_if_asked(&self, step: &'static str) {
            if self.hang == Some(step) {
                std::future::pending::<()>().await;
            }
        }
    }

    #[control_api::async_trait]
    impl DrainSteps for RecordingSteps {
        fn stop_accepting(&self) {
            self.note(STOP_ACCEPTING);
        }

        async fn hand_off_leases(&self) -> usize {
            self.note(HAND_OFF_LEASES);
            self.hang_if_asked(HAND_OFF_LEASES).await;
            3
        }

        async fn close_sessions(&self) -> SessionsClosed {
            self.note(CLOSE_SESSIONS);
            self.hang_if_asked(CLOSE_SESSIONS).await;
            SessionsClosed {
                closed: 2,
                failed: 0,
            }
        }

        async fn await_uploads(&self) -> usize {
            self.note(AWAIT_UPLOADS);
            self.hang_if_asked(AWAIT_UPLOADS).await;
            1
        }

        async fn await_control_plane_idle(&self) {
            self.note(CONTROL_PLANE_IDLE);
            self.hang_if_asked(CONTROL_PLANE_IDLE).await;
        }

        async fn flush_events(&self) -> u64 {
            self.note(FLUSH_EVENTS);
            self.hang_if_asked(FLUSH_EVENTS).await;
            0
        }
    }

    #[test]
    fn only_the_first_signal_begins_the_drain() {
        let state = DrainState::shared();
        assert!(!state.is_draining());
        assert!(state.begin());
        assert!(state.is_draining());
        assert!(!state.begin(), "a second signal must not restart the drain");
    }

    #[tokio::test(start_paused = true)]
    async fn the_drain_runs_every_step_in_order() {
        let steps = RecordingSteps::default();
        let report = run_drain(&steps, DEFAULT_DRAIN_TIMEOUT).await;
        assert_eq!(
            steps.order(),
            vec![
                STOP_ACCEPTING,
                HAND_OFF_LEASES,
                CLOSE_SESSIONS,
                AWAIT_UPLOADS,
                CONTROL_PLANE_IDLE,
                FLUSH_EVENTS
            ],
            "a backgrounded recording upload must settle before the event backlog is flushed, \
             or its own event never leaves this pod"
        );
        assert!(report.clean(), "{report:?}");
        assert_eq!(report.leases_handed_off, 3);
        assert_eq!(report.sessions.closed, 2);
        assert_eq!(report.uploads_settled, 1);
        assert_eq!(report.unsent_events, Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_registry_cannot_eat_the_whole_drain_window() {
        let steps = RecordingSteps::hanging_at(HAND_OFF_LEASES);
        let report = run_drain(&steps, DEFAULT_DRAIN_TIMEOUT).await;
        assert_eq!(report.expired, vec![HAND_OFF_LEASES]);
        assert!(
            report.finished.contains(&CLOSE_SESSIONS),
            "the consumers and recordings must still be closed: {report:?}"
        );
        assert!(report.finished.contains(&FLUSH_EVENTS));
        assert!(report.elapsed <= DEFAULT_DRAIN_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_session_close_still_leaves_the_events_a_window() {
        let steps = RecordingSteps::hanging_at(CLOSE_SESSIONS);
        let report = run_drain(&steps, DEFAULT_DRAIN_TIMEOUT).await;
        assert_eq!(report.expired, vec![CLOSE_SESSIONS]);
        assert_eq!(report.skipped, vec![AWAIT_UPLOADS, CONTROL_PLANE_IDLE]);
        assert_eq!(report.unsent_events, Some(0));
        assert!(report.elapsed <= DEFAULT_DRAIN_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn a_zero_window_stops_accepting_and_exits() {
        let steps = RecordingSteps::default();
        let report = run_drain(&steps, Duration::ZERO).await;
        assert_eq!(steps.order(), vec![STOP_ACCEPTING]);
        assert_eq!(
            report.skipped,
            vec![
                HAND_OFF_LEASES,
                CLOSE_SESSIONS,
                AWAIT_UPLOADS,
                CONTROL_PLANE_IDLE,
                FLUSH_EVENTS
            ]
        );
    }
}
