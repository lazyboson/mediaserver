use crate::bencode::Value;
use crate::commands::{NgError, NgReply};
use std::fmt;

pub const UNRECOGNIZED_COMMAND: &str = "Unrecognized command";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RelayTotals {
    pub packets: u64,
    pub packets_in_kernel: u64,
    pub packets_in_userspace: u64,
    pub bytes: u64,
    pub bytes_in_kernel: u64,
    pub bytes_in_userspace: u64,
    pub packet_errors: u64,
    pub managed_sessions: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CurrentRates {
    pub packets_per_second: u64,
    pub packets_per_second_in_kernel: u64,
    pub packets_per_second_in_userspace: u64,
    pub media_in_kernel: u64,
    pub media_in_userspace: u64,
    pub media_in_both: u64,
    pub transcoded_media: u64,
    pub sessions_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscoderChain {
    pub chain: String,
    pub packets: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RtpengineStatistics {
    pub uptime_seconds: Option<u64>,
    pub totals: RelayTotals,
    pub current: CurrentRates,
    pub transcoders: Vec<TranscoderChain>,
    pub kernel_counters_present: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndeterminedReason {
    NoMediaRelayedYet,
    StatisticsWithoutKernelCounters,
}

impl fmt::Display for UndeterminedReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UndeterminedReason::NoMediaRelayedYet => {
                write!(f, "this node has not relayed a single packet yet")
            }
            UndeterminedReason::StatisticsWithoutKernelCounters => write!(
                f,
                "this rtpengine's statistics reply carries no kernel or userspace split"
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelForwarding {
    ForwardingInKernelNow,
    ForwardedInKernelEarlier,
    RelayingEntirelyInUserspace,
    Undetermined(UndeterminedReason),
}

impl KernelForwarding {
    pub fn from_statistics(statistics: &RtpengineStatistics) -> KernelForwarding {
        if !statistics.kernel_counters_present {
            return KernelForwarding::Undetermined(
                UndeterminedReason::StatisticsWithoutKernelCounters,
            );
        }
        let current = &statistics.current;
        if current.packets_per_second_in_kernel > 0
            || current.media_in_kernel > 0
            || current.media_in_both > 0
        {
            return KernelForwarding::ForwardingInKernelNow;
        }
        if statistics.totals.packets_in_kernel > 0 {
            return KernelForwarding::ForwardedInKernelEarlier;
        }
        if statistics.totals.packets == 0 && statistics.totals.packets_in_userspace == 0 {
            return KernelForwarding::Undetermined(UndeterminedReason::NoMediaRelayedYet);
        }
        KernelForwarding::RelayingEntirelyInUserspace
    }

    pub fn module_in_play(&self) -> Option<bool> {
        match self {
            KernelForwarding::ForwardingInKernelNow
            | KernelForwarding::ForwardedInKernelEarlier => Some(true),
            KernelForwarding::RelayingEntirelyInUserspace => Some(false),
            KernelForwarding::Undetermined(_) => None,
        }
    }
}

impl fmt::Display for KernelForwarding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KernelForwarding::ForwardingInKernelNow => {
                write!(f, "the kernel module is forwarding media right now")
            }
            KernelForwarding::ForwardedInKernelEarlier => write!(
                f,
                "the kernel module has forwarded media on this node, but nothing is in the kernel at this instant"
            ),
            KernelForwarding::RelayingEntirelyInUserspace => write!(
                f,
                "every packet this node has relayed went through userspace; the kernel module is not in play"
            ),
            KernelForwarding::Undetermined(reason) => {
                write!(
                    f,
                    "cannot tell whether the kernel module is in play: {reason}"
                )
            }
        }
    }
}

fn number(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Int(int) => u64::try_from(*int).ok(),
        Value::Bytes(bytes) => std::str::from_utf8(bytes)
            .ok()?
            .trim()
            .split('.')
            .next()?
            .parse()
            .ok(),
        _ => None,
    }
}

fn field(parent: Option<&Value>, key: &str) -> Option<u64> {
    number(parent?.get(key))
}

fn text(parent: Option<&Value>, key: &str) -> Option<String> {
    parent?.get(key)?.as_str().map(str::to_string)
}

impl RtpengineStatistics {
    pub fn from_reply(reply: &NgReply) -> Result<RtpengineStatistics, NgError> {
        let statistics = reply
            .body
            .get("statistics")
            .ok_or(NgError::MissingField("statistics"))?;
        Ok(RtpengineStatistics::from_value(statistics))
    }

    pub fn from_value(statistics: &Value) -> RtpengineStatistics {
        let totals_value = statistics.get("totalstatistics");
        let current_value = statistics.get("currentstatistics");

        let totals = RelayTotals {
            packets: field(totals_value, "relayedpackets").unwrap_or_default(),
            packets_in_kernel: field(totals_value, "relayedpackets_kernel").unwrap_or_default(),
            packets_in_userspace: field(totals_value, "relayedpackets_user").unwrap_or_default(),
            bytes: field(totals_value, "relayedbytes").unwrap_or_default(),
            bytes_in_kernel: field(totals_value, "relayedbytes_kernel").unwrap_or_default(),
            bytes_in_userspace: field(totals_value, "relayedbytes_user").unwrap_or_default(),
            packet_errors: field(totals_value, "relayedpacketerrors").unwrap_or_default(),
            managed_sessions: field(totals_value, "managedsessions").unwrap_or_default(),
        };
        let current = CurrentRates {
            packets_per_second: field(current_value, "packetrate").unwrap_or_default(),
            packets_per_second_in_kernel: field(current_value, "packetrate_kernel")
                .unwrap_or_default(),
            packets_per_second_in_userspace: field(current_value, "packetrate_user")
                .unwrap_or_default(),
            media_in_kernel: field(current_value, "media_kernel").unwrap_or_default(),
            media_in_userspace: field(current_value, "media_userspace").unwrap_or_default(),
            media_in_both: field(current_value, "media_mixed").unwrap_or_default(),
            transcoded_media: field(current_value, "transcodedmedia").unwrap_or_default(),
            sessions_total: field(current_value, "sessionstotal").unwrap_or_default(),
        };
        let kernel_counters_present = totals_value
            .and_then(|value| value.get("relayedpackets_kernel"))
            .or_else(|| current_value.and_then(|value| value.get("packetrate_kernel")))
            .is_some();

        let mut transcoders = Vec::new();
        if let Some(Value::List(entries)) = statistics.get("transcoders") {
            for entry in entries {
                let Some(chain) = text(Some(entry), "chain") else {
                    continue;
                };
                transcoders.push(TranscoderChain {
                    chain,
                    packets: field(Some(entry), "packets").unwrap_or_default(),
                    bytes: field(Some(entry), "bytes").unwrap_or_default(),
                });
            }
        }

        RtpengineStatistics {
            uptime_seconds: field(totals_value, "uptime"),
            totals,
            current,
            transcoders,
            kernel_counters_present,
        }
    }

    pub fn kernel_forwarding(&self) -> KernelForwarding {
        KernelForwarding::from_statistics(self)
    }

    pub fn active_transcoder_chains(&self) -> Vec<&TranscoderChain> {
        self.transcoders
            .iter()
            .filter(|entry| entry.packets > 0)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::NgClient;

    const LAB_STATISTICS: &str = "d6:result2:ok10:statisticsd\
17:currentstatisticsd\
8:byteratei0e\
15:byterate_kerneli0e\
13:byterate_useri0e\
12:media_kerneli0e\
11:media_mixedi0e\
15:media_userspacei0e\
10:packetratei0e\
17:packetrate_kerneli0e\
15:packetrate_useri0e\
13:sessionstotali0e\
15:transcodedmediai0e\
e\
15:totalstatisticsd\
15:managedsessionsi5e\
12:relayedbytesi17964252e\
19:relayedbytes_kerneli0e\
17:relayedbytes_useri17964252e\
19:relayedpacketerrorsi0e\
14:relayedpacketsi107667e\
21:relayedpackets_kerneli0e\
19:relayedpackets_useri107667e\
6:uptime5:23129\
e\
11:transcodersl\
d5:bytesi1180480e5:chain25:PCMU/8000 -> opus/48000/23:numi0e7:packetsi7378e7:samplesi210880ee\
d5:bytesi0e5:chain25:opus/48000/2 -> PCMU/80003:numi0e7:packetsi0e7:samplesi0ee\
e\
ee";

    fn lab_reply() -> NgReply {
        let mut wire = b"c1 ".to_vec();
        wire.extend_from_slice(LAB_STATISTICS.as_bytes());
        NgClient::parse_reply(&wire).unwrap()
    }

    #[test]
    fn statistics_request_names_the_command_and_nothing_else() {
        let wire = NgClient::statistics(b"s1");
        assert_eq!(
            String::from_utf8_lossy(&wire),
            "s1 d7:command10:statisticse"
        );
    }

    #[test]
    fn version_request_names_the_command_and_nothing_else() {
        let wire = NgClient::version(b"v1");
        assert_eq!(String::from_utf8_lossy(&wire), "v1 d7:command7:versione");
    }

    #[test]
    fn the_lab_reply_parses_into_the_totals_it_reported() {
        let statistics = RtpengineStatistics::from_reply(&lab_reply()).unwrap();
        assert_eq!(statistics.totals.packets, 107_667);
        assert_eq!(statistics.totals.packets_in_userspace, 107_667);
        assert_eq!(statistics.totals.packets_in_kernel, 0);
        assert_eq!(statistics.totals.bytes, 17_964_252);
        assert_eq!(statistics.totals.managed_sessions, 5);
        assert!(statistics.kernel_counters_present);
    }

    #[test]
    fn uptime_arrives_as_a_string_and_is_still_a_number_to_us() {
        let statistics = RtpengineStatistics::from_reply(&lab_reply()).unwrap();
        assert_eq!(statistics.uptime_seconds, Some(23_129));
    }

    #[test]
    fn the_lab_node_relays_entirely_in_userspace() {
        let statistics = RtpengineStatistics::from_reply(&lab_reply()).unwrap();
        assert_eq!(
            statistics.kernel_forwarding(),
            KernelForwarding::RelayingEntirelyInUserspace
        );
        assert_eq!(statistics.kernel_forwarding().module_in_play(), Some(false));
    }

    #[test]
    fn the_transcoder_chains_name_what_rtpengine_is_converting() {
        let statistics = RtpengineStatistics::from_reply(&lab_reply()).unwrap();
        assert_eq!(statistics.transcoders.len(), 2);
        let active = statistics.active_transcoder_chains();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].chain, "PCMU/8000 -> opus/48000/2");
        assert_eq!(active[0].packets, 7378);
    }

    fn statistics_with(totals: &str, current: &str) -> RtpengineStatistics {
        let body = format!("d15:totalstatisticsd{totals}e17:currentstatisticsd{current}ee");
        RtpengineStatistics::from_value(&Value::decode(body.as_bytes()).unwrap())
    }

    #[test]
    fn a_live_kernel_packet_rate_says_the_module_is_forwarding_now() {
        let statistics = statistics_with(
            "14:relayedpacketsi400e21:relayedpackets_kerneli350e19:relayedpackets_useri50e",
            "17:packetrate_kerneli102e15:packetrate_useri0e",
        );
        assert_eq!(
            statistics.kernel_forwarding(),
            KernelForwarding::ForwardingInKernelNow
        );
    }

    #[test]
    fn a_kernel_total_without_a_live_rate_says_the_module_was_used_earlier() {
        let statistics = statistics_with(
            "14:relayedpacketsi400e21:relayedpackets_kerneli350e19:relayedpackets_useri50e",
            "17:packetrate_kerneli0e15:packetrate_useri0e",
        );
        assert_eq!(
            statistics.kernel_forwarding(),
            KernelForwarding::ForwardedInKernelEarlier
        );
    }

    #[test]
    fn a_node_that_has_relayed_nothing_is_undetermined_rather_than_userspace() {
        let statistics = statistics_with(
            "14:relayedpacketsi0e21:relayedpackets_kerneli0e19:relayedpackets_useri0e",
            "17:packetrate_kerneli0e",
        );
        assert_eq!(
            statistics.kernel_forwarding(),
            KernelForwarding::Undetermined(UndeterminedReason::NoMediaRelayedYet)
        );
        assert_eq!(statistics.kernel_forwarding().module_in_play(), None);
    }

    #[test]
    fn statistics_without_the_kernel_split_is_undetermined_with_that_reason() {
        let statistics = statistics_with("14:relayedpacketsi400e", "10:packetratei51e");
        assert_eq!(
            statistics.kernel_forwarding(),
            KernelForwarding::Undetermined(UndeterminedReason::StatisticsWithoutKernelCounters)
        );
    }

    #[test]
    fn mixed_media_counts_as_kernel_forwarding_because_some_of_it_is() {
        let statistics = statistics_with(
            "14:relayedpacketsi400e21:relayedpackets_kerneli0e19:relayedpackets_useri400e",
            "11:media_mixedi2e17:packetrate_kerneli0e",
        );
        assert_eq!(
            statistics.kernel_forwarding(),
            KernelForwarding::ForwardingInKernelNow
        );
    }

    #[test]
    fn a_reply_without_a_statistics_dict_is_an_error_not_a_zeroed_report() {
        let reply = NgClient::parse_reply(b"c1 d6:result2:oke").unwrap();
        assert_eq!(
            RtpengineStatistics::from_reply(&reply),
            Err(NgError::MissingField("statistics"))
        );
    }

    #[test]
    fn garbage_inside_the_statistics_dict_yields_zeroes_rather_than_panicking() {
        let body = "d15:totalstatisticsd14:relayedpackets3:abce17:currentstatisticsl1:xee";
        let statistics = RtpengineStatistics::from_value(&Value::decode(body.as_bytes()).unwrap());
        assert_eq!(statistics.totals.packets, 0);
        assert_eq!(statistics.current.packets_per_second, 0);
        assert!(!statistics.kernel_counters_present);
    }

    #[test]
    fn a_transcoder_entry_without_a_chain_name_is_skipped_instead_of_guessed() {
        let body = "d11:transcodersld7:packetsi9eed5:chain4:x_y_7:packetsi1eeee";
        let statistics = RtpengineStatistics::from_value(&Value::decode(body.as_bytes()).unwrap());
        assert_eq!(statistics.transcoders.len(), 1);
        assert_eq!(statistics.transcoders[0].chain, "x_y_");
    }

    #[test]
    fn the_lab_rtpengine_does_not_implement_the_version_command() {
        let wire = b"c1 d12:error-reason20:Unrecognized command6:result5:errore";
        match NgClient::parse_reply(wire) {
            Err(NgError::Remote(reason)) => assert_eq!(reason, UNRECOGNIZED_COMMAND),
            other => panic!("expected the remote refusal, got {other:?}"),
        }
    }

    #[test]
    fn a_version_field_is_read_when_a_node_ever_answers_one() {
        let wire = b"c1 d6:result2:ok7:version8:14.1.1.8e";
        let reply = NgClient::parse_reply(wire).unwrap();
        assert_eq!(reply.rtpengine_version(), Some("14.1.1.8"));
        let without = NgClient::parse_reply(b"c1 d6:result2:oke").unwrap();
        assert_eq!(without.rtpengine_version(), None);
    }
}
