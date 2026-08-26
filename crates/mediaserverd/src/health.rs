use crate::drain::DrainState;
use crate::event_pump::EventTransport;
use crate::ng_transport::NgTransport;
use crate::rtpengine_capability::NodeCapabilityLog;
use crate::session_store::SessionStore;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

pub const PROBE_INTERVAL_ENV: &str = "MSS_HEALTH_PROBE_INTERVAL_SECS";
pub const DEFAULT_PROBE_INTERVAL: Duration = Duration::from_secs(10);
pub const HEALTHZ_PATH: &str = "/healthz";
pub const READYZ_PATH: &str = "/readyz";

const FIRST_RETRY_AFTER: Duration = Duration::from_secs(1);
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BACKOFF_DOUBLINGS: u32 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dependency {
    Rtpengine,
    Redis,
    Kafka,
}

pub const DEPENDENCIES: [Dependency; 3] =
    [Dependency::Rtpengine, Dependency::Redis, Dependency::Kafka];

impl Dependency {
    pub fn label(self) -> &'static str {
        match self {
            Dependency::Rtpengine => "rtpengine",
            Dependency::Redis => "redis",
            Dependency::Kafka => "kafka",
        }
    }

    fn slot(self) -> usize {
        match self {
            Dependency::Rtpengine => 0,
            Dependency::Redis => 1,
            Dependency::Kafka => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Condition {
    NotConfigured,
    NotProbedYet,
    Ready,
    Failing(String),
}

struct DependencyHealth {
    condition: Condition,
    address: Option<String>,
    last_ok: Option<Instant>,
    consecutive_failures: u32,
}

impl Default for DependencyHealth {
    fn default() -> DependencyHealth {
        DependencyHealth {
            condition: Condition::NotProbedYet,
            address: None,
            last_ok: None,
            consecutive_failures: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencySnapshot {
    pub label: &'static str,
    pub address: Option<String>,
    pub configured: bool,
    pub ready: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessSnapshot {
    pub ready: bool,
    pub draining: bool,
    pub reasons: Vec<String>,
    pub dependencies: Vec<DependencySnapshot>,
}

pub struct Verdict {
    pub ready: bool,
    pub body: String,
}

pub struct Readiness {
    drain: Arc<DrainState>,
    entries: Mutex<[DependencyHealth; 3]>,
}

impl Readiness {
    pub fn shared(drain: Arc<DrainState>) -> Arc<Readiness> {
        Arc::new(Readiness {
            drain,
            entries: Mutex::new(Default::default()),
        })
    }

    fn with_entry<T>(
        &self,
        dependency: Dependency,
        act: impl FnOnce(&mut DependencyHealth) -> T,
    ) -> T {
        let mut entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };
        act(&mut entries[dependency.slot()])
    }

    pub fn describe(&self, dependency: Dependency, address: Option<String>) {
        self.with_entry(dependency, |entry| entry.address = address);
    }

    pub fn record_not_configured(&self, dependency: Dependency) {
        self.with_entry(dependency, |entry| {
            entry.condition = Condition::NotConfigured;
            entry.consecutive_failures = 0;
        });
    }

    pub fn record_ready(&self, dependency: Dependency) {
        let recovered = self.with_entry(dependency, |entry| {
            let recovered = matches!(entry.condition, Condition::Failing(_));
            entry.condition = Condition::Ready;
            entry.consecutive_failures = 0;
            entry.last_ok = Some(Instant::now());
            recovered
        });
        if recovered {
            info!(
                dependency = dependency.label(),
                "this dependency answered again; readiness is back on"
            );
        }
    }

    pub fn record_failure(&self, dependency: Dependency, reason: impl Into<String>) {
        let reason = reason.into();
        let failures = self.with_entry(dependency, |entry| {
            entry.condition = Condition::Failing(reason.clone());
            entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
            entry.consecutive_failures
        });
        if failures == 1 {
            warn!(
                dependency = dependency.label(),
                reason = %reason,
                "this dependency did not answer its probe; readiness is off"
            );
        }
    }

    pub fn consecutive_failures(&self, dependency: Dependency) -> u32 {
        self.with_entry(dependency, |entry| entry.consecutive_failures)
    }

    pub fn snapshot(&self) -> ReadinessSnapshot {
        let draining = self.drain.is_draining();
        let mut reasons = Vec::new();
        if draining {
            reasons.push("draining".to_string());
        }
        let mut dependencies = Vec::with_capacity(DEPENDENCIES.len());
        for dependency in DEPENDENCIES {
            let (condition, address, last_ok, failures) = self.with_entry(dependency, |entry| {
                (
                    entry.condition.clone(),
                    entry.address.clone(),
                    entry.last_ok,
                    entry.consecutive_failures,
                )
            });
            let label = dependency.label();
            let (configured, ready, detail) = match &condition {
                Condition::NotConfigured => (false, true, "not configured".to_string()),
                Condition::NotProbedYet => (true, false, "not probed yet".to_string()),
                Condition::Ready => (true, true, ready_detail(last_ok)),
                Condition::Failing(reason) => (
                    true,
                    false,
                    format!(
                        "unreachable: {reason} ({failures} consecutive {}, {})",
                        if failures == 1 { "failure" } else { "failures" },
                        match last_ok {
                            Some(when) => format!("last ok {}s ago", when.elapsed().as_secs()),
                            None => "never answered".to_string(),
                        }
                    ),
                ),
            };
            if !ready {
                reasons.push(match &condition {
                    Condition::Failing(reason) => format!("{label} unreachable: {reason}"),
                    _ => format!("{label} not probed yet"),
                });
            }
            dependencies.push(DependencySnapshot {
                label,
                address,
                configured,
                ready,
                detail,
            });
        }
        ReadinessSnapshot {
            ready: reasons.is_empty(),
            draining,
            reasons,
            dependencies,
        }
    }

    pub fn verdict(&self) -> Verdict {
        let snapshot = self.snapshot();
        let mut body = String::with_capacity(256);
        if snapshot.ready {
            let _ = writeln!(body, "ready");
        } else {
            let _ = writeln!(body, "not ready: {}", snapshot.reasons.join("; "));
        }
        for dependency in &snapshot.dependencies {
            let named = match &dependency.address {
                Some(address) => format!("{} {address}", dependency.label),
                None => dependency.label.to_string(),
            };
            let _ = writeln!(body, "{named}: {}", dependency.detail);
        }
        let _ = writeln!(
            body,
            "draining: {}",
            if snapshot.draining { "yes" } else { "no" }
        );
        Verdict {
            ready: snapshot.ready,
            body,
        }
    }
}

fn ready_detail(last_ok: Option<Instant>) -> String {
    match last_ok {
        Some(when) => format!("ready (last ok {}s ago)", when.elapsed().as_secs()),
        None => "ready".to_string(),
    }
}

pub fn probe_interval() -> Duration {
    let configured = std::env::var(PROBE_INTERVAL_ENV)
        .ok()
        .filter(|configured| !configured.trim().is_empty());
    let Some(configured) = configured else {
        info!(
            env = PROBE_INTERVAL_ENV,
            value = "unset",
            seconds = DEFAULT_PROBE_INTERVAL.as_secs(),
            "dependency probes for readiness run on the default interval"
        );
        return DEFAULT_PROBE_INTERVAL;
    };
    match configured.trim().parse::<u64>() {
        Ok(seconds) if seconds > 0 => {
            info!(
                env = PROBE_INTERVAL_ENV,
                seconds, "dependency probes for readiness run on this interval"
            );
            Duration::from_secs(seconds)
        }
        _ => {
            warn!(
                env = PROBE_INTERVAL_ENV,
                configured = %configured,
                seconds = DEFAULT_PROBE_INTERVAL.as_secs(),
                "the probe interval must be a whole number of seconds above zero; \
                 falling back to the default"
            );
            DEFAULT_PROBE_INTERVAL
        }
    }
}

pub fn next_probe_delay(interval: Duration, consecutive_failures: u32) -> Duration {
    if consecutive_failures == 0 {
        return interval;
    }
    let doublings = consecutive_failures.min(MAX_BACKOFF_DOUBLINGS) - 1;
    FIRST_RETRY_AFTER
        .saturating_mul(1u32 << doublings)
        .min(interval)
}

#[control_api::async_trait]
pub trait HealthProbe: Send + Sync + 'static {
    fn dependency(&self) -> Dependency;

    async fn probe(&self) -> Result<(), String>;
}

pub fn watch(
    readiness: Arc<Readiness>,
    probe: Arc<dyn HealthProbe>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let dependency = probe.dependency();
        loop {
            let delay = next_probe_delay(interval, readiness.consecutive_failures(dependency));
            tokio::time::sleep(delay).await;
            match tokio::time::timeout(PROBE_TIMEOUT, probe.probe()).await {
                Ok(Ok(())) => readiness.record_ready(dependency),
                Ok(Err(reason)) => readiness.record_failure(dependency, reason),
                Err(_) => readiness.record_failure(
                    dependency,
                    format!("no answer within {}s", PROBE_TIMEOUT.as_secs()),
                ),
            }
        }
    })
}

pub struct SessionStoreProbe {
    store: Arc<dyn SessionStore>,
}

impl SessionStoreProbe {
    pub fn new(store: Arc<dyn SessionStore>) -> Arc<SessionStoreProbe> {
        Arc::new(SessionStoreProbe { store })
    }
}

#[control_api::async_trait]
impl HealthProbe for SessionStoreProbe {
    fn dependency(&self) -> Dependency {
        Dependency::Redis
    }

    async fn probe(&self) -> Result<(), String> {
        self.store.ping().await.map_err(|error| error.to_string())
    }
}

pub struct EventBusProbe {
    transport: Arc<dyn EventTransport>,
}

impl EventBusProbe {
    pub fn new(transport: Arc<dyn EventTransport>) -> Arc<EventBusProbe> {
        Arc::new(EventBusProbe { transport })
    }
}

#[control_api::async_trait]
impl HealthProbe for EventBusProbe {
    fn dependency(&self) -> Dependency {
        Dependency::Kafka
    }

    async fn probe(&self) -> Result<(), String> {
        self.transport.reachable().await
    }
}

pub struct NgNodeProbe {
    transport: Arc<NgTransport>,
    node: SocketAddr,
    capabilities: Arc<NodeCapabilityLog>,
}

impl NgNodeProbe {
    pub fn new(
        transport: Arc<NgTransport>,
        node: SocketAddr,
        capabilities: Arc<NodeCapabilityLog>,
    ) -> Arc<NgNodeProbe> {
        Arc::new(NgNodeProbe {
            transport,
            node,
            capabilities,
        })
    }
}

#[control_api::async_trait]
impl HealthProbe for NgNodeProbe {
    fn dependency(&self) -> Dependency {
        Dependency::Rtpengine
    }

    async fn probe(&self) -> Result<(), String> {
        match self.transport.ping().await {
            Ok(_) => {
                self.capabilities
                    .report_first_contact(self.node, &self.transport)
                    .await;
                Ok(())
            }
            Err(error) => {
                self.capabilities.forget(self.node);
                Err(error.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn readiness() -> (Arc<DrainState>, Arc<Readiness>) {
        let drain = DrainState::shared();
        let readiness = Readiness::shared(Arc::clone(&drain));
        (drain, readiness)
    }

    fn everything_ready() -> (Arc<DrainState>, Arc<Readiness>) {
        let (drain, readiness) = readiness();
        readiness.record_ready(Dependency::Rtpengine);
        readiness.record_ready(Dependency::Redis);
        readiness.record_ready(Dependency::Kafka);
        (drain, readiness)
    }

    #[test]
    fn a_pod_whose_dependencies_answered_is_ready() {
        let (_drain, readiness) = everything_ready();
        let verdict = readiness.verdict();
        assert!(verdict.ready, "{}", verdict.body);
        assert!(verdict.body.starts_with("ready\n"), "{}", verdict.body);
        assert!(verdict.body.contains("draining: no"), "{}", verdict.body);
    }

    #[test]
    fn an_unconfigured_dependency_counts_as_ready_and_says_so() {
        let (_drain, readiness) = readiness();
        readiness.record_ready(Dependency::Rtpengine);
        readiness.record_not_configured(Dependency::Redis);
        readiness.record_not_configured(Dependency::Kafka);
        let verdict = readiness.verdict();
        assert!(verdict.ready, "{}", verdict.body);
        assert!(
            verdict.body.contains("redis: not configured"),
            "{}",
            verdict.body
        );
    }

    #[test]
    fn a_dependency_nobody_has_probed_yet_is_not_ready() {
        let (_drain, readiness) = readiness();
        let verdict = readiness.verdict();
        assert!(!verdict.ready);
        assert!(verdict.body.contains("not probed yet"), "{}", verdict.body);
    }

    #[test]
    fn a_failing_dependency_is_named_in_the_first_line() {
        let (_drain, readiness) = everything_ready();
        readiness.record_failure(Dependency::Redis, "connection refused");
        let verdict = readiness.verdict();
        assert!(!verdict.ready);
        let first = verdict.body.lines().next().unwrap();
        assert!(first.starts_with("not ready: "), "{first}");
        assert!(first.contains("redis"), "{first}");
        assert!(first.contains("connection refused"), "{first}");
        assert!(
            verdict.body.contains("1 consecutive failure"),
            "{}",
            verdict.body
        );
    }

    #[test]
    fn the_node_address_is_carried_into_the_report() {
        let (_drain, readiness) = everything_ready();
        readiness.describe(
            Dependency::Rtpengine,
            Some("172.31.99.10:22222".to_string()),
        );
        let body = readiness.verdict().body;
        assert!(
            body.contains("rtpengine 172.31.99.10:22222: ready"),
            "{body}"
        );
    }

    #[test]
    fn readiness_goes_off_the_instant_a_drain_begins() {
        let (drain, readiness) = everything_ready();
        assert!(readiness.verdict().ready);
        drain.begin();
        let verdict = readiness.verdict();
        assert!(!verdict.ready);
        assert!(
            verdict.body.starts_with("not ready: draining"),
            "{}",
            verdict.body
        );
        assert!(verdict.body.contains("draining: yes"), "{}", verdict.body);
    }

    #[test]
    fn a_recovered_dependency_forgets_its_failure_streak() {
        let (_drain, readiness) = everything_ready();
        readiness.record_failure(Dependency::Kafka, "broker gone");
        readiness.record_failure(Dependency::Kafka, "broker gone");
        assert_eq!(readiness.consecutive_failures(Dependency::Kafka), 2);
        readiness.record_ready(Dependency::Kafka);
        assert_eq!(readiness.consecutive_failures(Dependency::Kafka), 0);
        assert!(readiness.verdict().ready);
    }

    #[test]
    fn failures_are_re_probed_sooner_than_the_interval_and_back_off_up_to_it() {
        let interval = Duration::from_secs(10);
        assert_eq!(next_probe_delay(interval, 0), interval);
        assert_eq!(next_probe_delay(interval, 1), Duration::from_secs(1));
        assert_eq!(next_probe_delay(interval, 2), Duration::from_secs(2));
        assert_eq!(next_probe_delay(interval, 3), Duration::from_secs(4));
        assert_eq!(next_probe_delay(interval, 4), Duration::from_secs(8));
        assert_eq!(next_probe_delay(interval, 5), interval);
        assert_eq!(next_probe_delay(interval, 400), interval);
        assert_eq!(
            next_probe_delay(Duration::from_millis(200), 3),
            Duration::from_millis(200)
        );
    }

    #[tokio::test]
    async fn the_watcher_keeps_probing_a_real_store_until_it_answers() {
        let (_drain, readiness) = readiness();
        readiness.record_ready(Dependency::Rtpengine);
        readiness.record_not_configured(Dependency::Kafka);
        let store = Arc::new(crate::session_store::MemorySessionStore::default());
        store.set_unreachable(true);
        let watcher = watch(
            Arc::clone(&readiness),
            SessionStoreProbe::new(Arc::clone(&store) as Arc<dyn SessionStore>)
                as Arc<dyn HealthProbe>,
            Duration::from_millis(5),
        );
        let refused = wait_until(Duration::from_secs(5), || {
            let verdict = readiness.verdict();
            !verdict.ready
                && verdict.body.contains("redis unreachable: ")
                && verdict.body.contains("connection refused")
        })
        .await;
        store.set_unreachable(false);
        let recovered = wait_until(Duration::from_secs(5), || readiness.verdict().ready).await;
        watcher.abort();
        assert!(refused, "{}", readiness.verdict().body);
        assert!(recovered, "{}", readiness.verdict().body);
    }

    async fn wait_until(within: Duration, settled: impl Fn() -> bool) -> bool {
        tokio::time::timeout(within, async {
            loop {
                if settled() {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap_or(false)
    }

    #[tokio::test]
    async fn a_probe_that_never_answers_is_recorded_as_a_failure() {
        let (_drain, readiness) = everything_ready();
        readiness.record_failure(Dependency::Redis, "no answer within 15s");
        let verdict = readiness.verdict();
        assert!(!verdict.ready);
        assert!(
            verdict.body.contains("no answer within"),
            "{}",
            verdict.body
        );
    }
}
