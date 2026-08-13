use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionHealth {
    Flowing,
    Stalled { since: Instant },
}

#[derive(Debug)]
pub struct AudioFlowWatchdog {
    stall_after: Duration,
    last_activity: Instant,
    stalled_since: Option<Instant>,
}

impl AudioFlowWatchdog {
    pub fn new(stall_after: Duration, now: Instant) -> Self {
        AudioFlowWatchdog {
            stall_after,
            last_activity: now,
            stalled_since: None,
        }
    }

    pub fn touch(&mut self, now: Instant) {
        self.last_activity = now;
        self.stalled_since = None;
    }

    pub fn check(&mut self, now: Instant) -> SessionHealth {
        if now.duration_since(self.last_activity) >= self.stall_after {
            let since = *self.stalled_since.get_or_insert(now);
            SessionHealth::Stalled { since }
        } else {
            SessionHealth::Flowing
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stalls_after_quiet_period_and_recovers_on_touch() {
        let t0 = Instant::now();
        let mut wd = AudioFlowWatchdog::new(Duration::from_millis(100), t0);

        assert_eq!(
            wd.check(t0 + Duration::from_millis(50)),
            SessionHealth::Flowing
        );

        let t_stall = t0 + Duration::from_millis(150);
        assert!(matches!(wd.check(t_stall), SessionHealth::Stalled { .. }));

        match wd.check(t0 + Duration::from_millis(300)) {
            SessionHealth::Stalled { since } => assert_eq!(since, t_stall),
            other => panic!("expected stalled, got {other:?}"),
        }

        wd.touch(t0 + Duration::from_millis(310));
        assert_eq!(
            wd.check(t0 + Duration::from_millis(320)),
            SessionHealth::Flowing
        );
    }
}
