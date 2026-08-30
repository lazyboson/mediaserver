use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timings {
    pub t1: Duration,
    pub t2: Duration,
    pub t4: Duration,
}

impl Default for Timings {
    fn default() -> Self {
        Timings {
            t1: Duration::from_millis(500),
            t2: Duration::from_secs(4),
            t4: Duration::from_secs(5),
        }
    }
}

impl Timings {
    pub fn give_up_after(&self) -> Duration {
        self.t1 * 64
    }

    pub fn first_retransmit_interval(&self) -> Duration {
        self.t1
    }

    pub fn next_retransmit_interval(&self, previous: Duration) -> Duration {
        let doubled = previous.saturating_mul(2);
        if doubled > self.t2 {
            self.t2
        } else {
            doubled
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reliability {
    Unreliable,
    Reliable,
}

impl Reliability {
    pub fn retransmits_responses(&self) -> bool {
        matches!(self, Reliability::Unreliable)
    }

    pub fn absorbs_retransmissions_for(&self, unreliable_window: Duration) -> Duration {
        match self {
            Reliability::Unreliable => unreliable_window,
            Reliability::Reliable => Duration::ZERO,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_are_the_values_rfc_3261_names() {
        let timings = Timings::default();
        assert_eq!(timings.t1, Duration::from_millis(500));
        assert_eq!(timings.t2, Duration::from_secs(4));
        assert_eq!(timings.t4, Duration::from_secs(5));
        assert_eq!(timings.give_up_after(), Duration::from_secs(32));
    }

    #[test]
    fn a_retransmit_interval_doubles_until_it_is_capped_at_t2() {
        let timings = Timings::default();
        let mut interval = timings.first_retransmit_interval();
        let mut seen = vec![interval];
        for _ in 0..5 {
            interval = timings.next_retransmit_interval(interval);
            seen.push(interval);
        }
        assert_eq!(
            seen,
            vec![
                Duration::from_millis(500),
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(4),
                Duration::from_secs(4),
            ]
        );
    }

    #[test]
    fn a_reliable_transport_neither_retransmits_nor_waits_out_retransmissions() {
        assert!(!Reliability::Reliable.retransmits_responses());
        assert_eq!(
            Reliability::Reliable.absorbs_retransmissions_for(Duration::from_secs(32)),
            Duration::ZERO
        );
        assert!(Reliability::Unreliable.retransmits_responses());
        assert_eq!(
            Reliability::Unreliable.absorbs_retransmissions_for(Duration::from_secs(32)),
            Duration::from_secs(32)
        );
    }
}
