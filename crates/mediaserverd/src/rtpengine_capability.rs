use crate::ng_transport::{NgTransport, TransportError};
use rtpengine_ng::{KernelForwarding, NgError, UNRECOGNIZED_COMMAND};
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Mutex;
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

#[derive(Debug, Default)]
pub struct NodeCapabilityLog {
    transcode_at_tap: bool,
    reported: Mutex<HashSet<SocketAddr>>,
}

impl NodeCapabilityLog {
    pub fn new(transcode_at_tap: bool) -> NodeCapabilityLog {
        NodeCapabilityLog {
            transcode_at_tap,
            reported: Mutex::new(HashSet::new()),
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

    pub fn forget(&self, node: SocketAddr) {
        if let Ok(mut held) = self.reported.lock() {
            held.remove(&node);
        }
    }

    pub async fn report_first_contact(&self, node: SocketAddr, transport: &NgTransport) {
        if !self.claim(node) {
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
    use rtpengine_ng::UndeterminedReason;

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
}
