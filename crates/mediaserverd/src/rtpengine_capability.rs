use crate::ng_transport::{NgTransport, TransportError};
use rtpengine_ng::{KernelForwarding, NgError, RtpengineStatistics, UNRECOGNIZED_COMMAND};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Instant;
use tracing::{info, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionReport {
    Reported(String),
    NoVersionCommandOnThisNode,
    Unavailable(String),
}

impl fmt::Display for VersionReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersionReport::Reported(version) => write!(f, "{version}"),
            VersionReport::NoVersionCommandOnThisNode => write!(
                f,
                "unknown: this rtpengine's NG protocol has no version command"
            ),
            VersionReport::Unavailable(reason) => write!(f, "unknown: {reason}"),
        }
    }
}

impl VersionReport {
    pub fn from_outcome(outcome: Result<Option<String>, TransportError>) -> VersionReport {
        match outcome {
            Ok(Some(version)) => VersionReport::Reported(version),
            Ok(None) => VersionReport::Unavailable(
                "the node answered the version command without a version field".to_string(),
            ),
            Err(TransportError::Ng(NgError::Remote(reason))) if reason == UNRECOGNIZED_COMMAND => {
                VersionReport::NoVersionCommandOnThisNode
            }
            Err(error) => VersionReport::Unavailable(error.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TapKernelVerdict {
    TranscodedTapsAreProcessedInUserspace,
    TapsMayRideTheKernelPath,
    ThisNodeIsNotUsingTheKernelModule,
    Undetermined,
}

impl fmt::Display for TapKernelVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TapKernelVerdict::TranscodedTapsAreProcessedInUserspace => write!(
                f,
                "this daemon asks rtpengine to transcode the tap, so its subscriptions are \
                 processed in rtpengine userspace; the kernel module carries no codec and \
                 cannot help them"
            ),
            TapKernelVerdict::TapsMayRideTheKernelPath => write!(
                f,
                "this daemon asks rtpengine to transcode nothing and this node forwards in \
                 the kernel, so taps are eligible for the kernel path"
            ),
            TapKernelVerdict::ThisNodeIsNotUsingTheKernelModule => write!(
                f,
                "this daemon asks rtpengine to transcode nothing, but this node relays every \
                 packet in userspace, so there is no kernel path for a tap to ride"
            ),
            TapKernelVerdict::Undetermined => write!(
                f,
                "this daemon asks rtpengine to transcode nothing; whether its taps ride the \
                 kernel path cannot be told from this node's statistics"
            ),
        }
    }
}

impl TapKernelVerdict {
    pub fn name(&self) -> &'static str {
        match self {
            TapKernelVerdict::TranscodedTapsAreProcessedInUserspace => {
                "TranscodedTapsAreProcessedInUserspace"
            }
            TapKernelVerdict::TapsMayRideTheKernelPath => "TapsMayRideTheKernelPath",
            TapKernelVerdict::ThisNodeIsNotUsingTheKernelModule => {
                "ThisNodeIsNotUsingTheKernelModule"
            }
            TapKernelVerdict::Undetermined => "Undetermined",
        }
    }

    pub fn decide(kernel: KernelForwarding, transcode_at_tap: bool) -> TapKernelVerdict {
        if transcode_at_tap {
            return TapKernelVerdict::TranscodedTapsAreProcessedInUserspace;
        }
        match kernel {
            KernelForwarding::ForwardingInKernelNow
            | KernelForwarding::ForwardedInKernelEarlier => {
                TapKernelVerdict::TapsMayRideTheKernelPath
            }
            KernelForwarding::RelayingEntirelyInUserspace => {
                TapKernelVerdict::ThisNodeIsNotUsingTheKernelModule
            }
            KernelForwarding::Undetermined(_) => TapKernelVerdict::Undetermined,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeSample {
    pub verdict: TapKernelVerdict,
    pub relayed_packets_kernel: u64,
    pub relayed_packets_user: u64,
    pub media_kernel: u64,
    pub media_userspace: u64,
    pub media_mixed: u64,
    pub transcoded_media: u64,
    pub sessions_live: u64,
    pub taken: Instant,
}

impl NodeSample {
    pub fn from_statistics(
        statistics: &RtpengineStatistics,
        transcode_at_tap: bool,
        taken: Instant,
    ) -> NodeSample {
        NodeSample {
            verdict: TapKernelVerdict::decide(statistics.kernel_forwarding(), transcode_at_tap),
            relayed_packets_kernel: statistics.totals.packets_in_kernel,
            relayed_packets_user: statistics.totals.packets_in_userspace,
            media_kernel: statistics.current.media_in_kernel,
            media_userspace: statistics.current.media_in_userspace,
            media_mixed: statistics.current.media_in_both,
            transcoded_media: statistics.current.transcoded_media,
            sessions_live: statistics.current.sessions_total,
            taken,
        }
    }

    pub fn age_seconds(&self, now: Instant) -> f64 {
        now.saturating_duration_since(self.taken).as_secs_f64()
    }
}

#[derive(Debug, Default)]
pub struct NodeCapabilityLog {
    transcode_at_tap: bool,
    reported: Mutex<HashSet<SocketAddr>>,
    last: Mutex<HashMap<SocketAddr, NodeSample>>,
}

impl NodeCapabilityLog {
    pub fn new(transcode_at_tap: bool) -> NodeCapabilityLog {
        NodeCapabilityLog {
            transcode_at_tap,
            reported: Mutex::new(HashSet::new()),
            last: Mutex::new(HashMap::new()),
        }
    }

    pub fn transcode_at_tap(&self) -> bool {
        self.transcode_at_tap
    }

    fn claim(&self, node: SocketAddr) -> bool {
        self.reported
            .lock()
            .map(|mut held| held.insert(node))
            .unwrap_or(false)
    }

    fn already_reported(&self, node: SocketAddr) -> bool {
        self.reported
            .lock()
            .map(|held| held.contains(&node))
            .unwrap_or(false)
    }

    pub fn forget(&self, node: SocketAddr) {
        if let Ok(mut held) = self.reported.lock() {
            held.remove(&node);
        }
        if let Ok(mut held) = self.last.lock() {
            held.remove(&node);
        }
    }

    pub fn record(&self, node: SocketAddr, sample: NodeSample) {
        if let Ok(mut held) = self.last.lock() {
            held.insert(node, sample);
        }
    }

    pub fn samples(&self) -> Vec<(SocketAddr, NodeSample)> {
        let mut samples: Vec<(SocketAddr, NodeSample)> = self
            .last
            .lock()
            .map(|held| held.iter().map(|(node, sample)| (*node, *sample)).collect())
            .unwrap_or_default();
        samples.sort_by_key(|(node, _)| *node);
        samples
    }

    pub async fn report_first_contact(&self, node: SocketAddr, transport: &NgTransport) {
        if self.already_reported(node) {
            return;
        }
        self.observe(node, transport).await;
    }

    pub async fn observe(&self, node: SocketAddr, transport: &NgTransport) {
        let first_contact = self.claim(node);
        if !first_contact {
            if let Ok(statistics) = transport.statistics().await {
                self.record(
                    node,
                    NodeSample::from_statistics(&statistics, self.transcode_at_tap, Instant::now()),
                );
            }
            return;
        }
        let version = VersionReport::from_outcome(
            transport
                .version()
                .await
                .map(|reply| reply.rtpengine_version().map(str::to_string)),
        );
        match transport.statistics().await {
            Ok(statistics) => {
                let kernel = statistics.kernel_forwarding();
                let verdict = TapKernelVerdict::decide(kernel, self.transcode_at_tap);
                self.record(
                    node,
                    NodeSample::from_statistics(&statistics, self.transcode_at_tap, Instant::now()),
                );
                info!(
                    %node,
                    version = %version,
                    uptime_seconds = ?statistics.uptime_seconds,
                    relayed_packets = statistics.totals.packets,
                    relayed_packets_in_kernel = statistics.totals.packets_in_kernel,
                    relayed_packets_in_userspace = statistics.totals.packets_in_userspace,
                    sessions_now = statistics.current.sessions_total,
                    transcoded_media_now = statistics.current.transcoded_media,
                    kernel_forwarding = %kernel,
                    "rtpengine node capabilities on first contact"
                );
                let chains: Vec<&str> = statistics
                    .active_transcoder_chains()
                    .iter()
                    .map(|entry| entry.chain.as_str())
                    .collect();
                if !chains.is_empty() {
                    info!(%node, chains = ?chains, "rtpengine is transcoding these codec chains");
                }
                if self.transcode_at_tap {
                    warn!(%node, verdict = %verdict, "tap kernel eligibility");
                } else {
                    info!(%node, verdict = %verdict, "tap kernel eligibility");
                }
            }
            Err(error) => warn!(
                %node,
                version = %version,
                %error,
                "rtpengine did not answer the statistics command; kernel eligibility is unknown"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtpengine_ng::{CurrentRates, RelayTotals, UndeterminedReason};

    #[test]
    fn a_transcoding_daemon_is_told_its_taps_cannot_use_the_kernel_module() {
        for kernel in [
            KernelForwarding::ForwardingInKernelNow,
            KernelForwarding::ForwardedInKernelEarlier,
            KernelForwarding::RelayingEntirelyInUserspace,
            KernelForwarding::Undetermined(UndeterminedReason::NoMediaRelayedYet),
        ] {
            assert_eq!(
                TapKernelVerdict::decide(kernel, true),
                TapKernelVerdict::TranscodedTapsAreProcessedInUserspace
            );
        }
        let said = TapKernelVerdict::TranscodedTapsAreProcessedInUserspace.to_string();
        assert!(said.contains("rtpengine userspace"), "{said}");
        assert!(said.contains("kernel module carries no codec"), "{said}");
    }

    #[test]
    fn a_non_transcoding_daemon_on_a_kernel_node_is_eligible() {
        assert_eq!(
            TapKernelVerdict::decide(KernelForwarding::ForwardingInKernelNow, false),
            TapKernelVerdict::TapsMayRideTheKernelPath
        );
        assert_eq!(
            TapKernelVerdict::decide(KernelForwarding::ForwardedInKernelEarlier, false),
            TapKernelVerdict::TapsMayRideTheKernelPath
        );
    }

    #[test]
    fn a_non_transcoding_daemon_on_a_userspace_node_is_told_there_is_no_kernel_path() {
        assert_eq!(
            TapKernelVerdict::decide(KernelForwarding::RelayingEntirelyInUserspace, false),
            TapKernelVerdict::ThisNodeIsNotUsingTheKernelModule
        );
        assert_eq!(
            TapKernelVerdict::decide(
                KernelForwarding::Undetermined(UndeterminedReason::StatisticsWithoutKernelCounters),
                false
            ),
            TapKernelVerdict::Undetermined
        );
    }

    #[test]
    fn an_unrecognized_version_command_is_reported_as_the_protocol_lacking_it() {
        let outcome = Err(TransportError::Ng(NgError::Remote(
            UNRECOGNIZED_COMMAND.to_string(),
        )));
        assert_eq!(
            VersionReport::from_outcome(outcome),
            VersionReport::NoVersionCommandOnThisNode
        );
        let said = VersionReport::NoVersionCommandOnThisNode.to_string();
        assert!(said.contains("no version command"), "{said}");
    }

    #[test]
    fn a_version_a_node_does_report_is_carried_through() {
        assert_eq!(
            VersionReport::from_outcome(Ok(Some("14.1.1.8".to_string()))),
            VersionReport::Reported("14.1.1.8".to_string())
        );
        assert_eq!(
            VersionReport::Reported("14.1.1.8".to_string()).to_string(),
            "14.1.1.8"
        );
    }

    #[test]
    fn any_other_failure_keeps_its_own_reason_instead_of_claiming_the_command_is_missing() {
        let outcome = Err(TransportError::Timeout {
            node: "127.0.0.1:22222".parse().unwrap(),
            attempts: 3,
        });
        match VersionReport::from_outcome(outcome) {
            VersionReport::Unavailable(reason) => assert!(reason.contains("no reply"), "{reason}"),
            other => panic!("expected an unavailable report, got {other:?}"),
        }
        match VersionReport::from_outcome(Ok(None)) {
            VersionReport::Unavailable(reason) => {
                assert!(reason.contains("without a version field"), "{reason}")
            }
            other => panic!("expected an unavailable report, got {other:?}"),
        }
    }

    #[test]
    fn a_node_is_reported_once_and_never_again() {
        let log = NodeCapabilityLog::new(false);
        let node: SocketAddr = "127.0.0.1:22222".parse().unwrap();
        let other: SocketAddr = "127.0.0.2:22222".parse().unwrap();
        assert!(log.claim(node));
        assert!(!log.claim(node));
        assert!(log.claim(other));
        assert!(!log.claim(other));
    }

    fn userspace_statistics() -> RtpengineStatistics {
        RtpengineStatistics {
            uptime_seconds: Some(400),
            totals: RelayTotals {
                packets: 9000,
                packets_in_kernel: 0,
                packets_in_userspace: 9000,
                ..RelayTotals::default()
            },
            current: CurrentRates {
                media_in_kernel: 0,
                media_in_userspace: 4,
                media_in_both: 0,
                transcoded_media: 2,
                sessions_total: 3,
                ..CurrentRates::default()
            },
            transcoders: Vec::new(),
            kernel_counters_present: true,
        }
    }

    #[test]
    fn a_sample_carries_the_relay_split_the_capacity_plan_needs() {
        let sample = NodeSample::from_statistics(&userspace_statistics(), false, Instant::now());
        assert_eq!(
            sample.verdict,
            TapKernelVerdict::ThisNodeIsNotUsingTheKernelModule
        );
        assert_eq!(sample.relayed_packets_kernel, 0);
        assert_eq!(sample.relayed_packets_user, 9000);
        assert_eq!(sample.media_userspace, 4);
        assert_eq!(sample.media_kernel, 0);
        assert_eq!(sample.media_mixed, 0);
        assert_eq!(sample.transcoded_media, 2);
        assert_eq!(sample.sessions_live, 3);
    }

    #[test]
    fn a_transcoding_daemon_samples_the_verdict_that_names_its_own_choice() {
        let sample = NodeSample::from_statistics(&userspace_statistics(), true, Instant::now());
        assert_eq!(
            sample.verdict,
            TapKernelVerdict::TranscodedTapsAreProcessedInUserspace
        );
        assert_eq!(
            sample.verdict.name(),
            "TranscodedTapsAreProcessedInUserspace"
        );
    }

    #[test]
    fn the_last_sample_of_each_node_is_kept_in_node_order_and_dropped_when_the_node_is_forgotten() {
        let log = NodeCapabilityLog::new(false);
        let first: SocketAddr = "127.0.0.1:22222".parse().unwrap();
        let second: SocketAddr = "127.0.0.2:22222".parse().unwrap();
        let taken = Instant::now();
        log.record(
            second,
            NodeSample::from_statistics(&userspace_statistics(), false, taken),
        );
        log.record(
            first,
            NodeSample::from_statistics(&userspace_statistics(), false, taken),
        );
        let mut fresher = userspace_statistics();
        fresher.totals.packets_in_userspace = 12000;
        log.record(first, NodeSample::from_statistics(&fresher, false, taken));
        let samples = log.samples();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].0, first);
        assert_eq!(samples[0].1.relayed_packets_user, 12000);
        assert_eq!(samples[1].0, second);
        assert_eq!(samples[1].1.relayed_packets_user, 9000);
        log.forget(first);
        let left = log.samples();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].0, second);
    }
}
