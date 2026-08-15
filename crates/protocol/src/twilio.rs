use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MediaFormat {
    pub encoding: String,
    pub sample_rate: u32,
    pub channels: u8,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StartInfo {
    pub account_id: String,
    pub stream_sid: String,
    pub call_sid: String,
    pub tracks: Vec<String>,
    pub media_format: MediaFormat,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub custom_parameters: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MediaPayload {
    pub track: String,
    pub timestamp: String,
    pub payload: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DtmfInfo {
    pub track: String,
    pub digit: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MarkInfo {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "camelCase")]
pub enum Outbound {
    #[serde(rename_all = "camelCase")]
    Start {
        sequence_number: String,
        stream_sid: String,
        start: StartInfo,
    },
    #[serde(rename_all = "camelCase")]
    Media {
        sequence_number: String,
        stream_sid: String,
        media: MediaPayload,
    },
    #[serde(rename_all = "camelCase")]
    Dtmf {
        sequence_number: String,
        stream_sid: String,
        dtmf: DtmfInfo,
    },
    #[serde(rename_all = "camelCase")]
    Mark { stream_sid: String, mark: MarkInfo },
    #[serde(rename_all = "camelCase")]
    Stop {
        sequence_number: String,
        stream_sid: String,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "camelCase")]
pub enum Inbound {
    #[serde(rename_all = "camelCase")]
    Media {
        stream_sid: Option<String>,
        media: InboundMedia,
    },
    #[serde(rename_all = "camelCase")]
    Mark {
        stream_sid: Option<String>,
        mark: MarkInfo,
    },
    #[serde(rename_all = "camelCase")]
    Clear { stream_sid: Option<String> },
    #[serde(rename = "endOfInteraction", rename_all = "camelCase")]
    EndOfInteraction {
        stream_sid: Option<String>,
        #[serde(default)]
        context: Option<serde_json::Value>,
        #[serde(default)]
        stream_context: Option<serde_json::Value>,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InboundMedia {
    #[serde(default)]
    pub sample_rate: Option<u32>,
    #[serde(default)]
    pub encoding: Option<String>,
    pub payload: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_serializes_to(msg: &Outbound, expected: &str) {
        assert_eq!(serde_json::to_string(msg).unwrap(), expected);
    }

    fn start(custom_parameters: HashMap<String, String>) -> Outbound {
        Outbound::Start {
            sequence_number: "1".into(),
            stream_sid: "MZ-1".into(),
            start: StartInfo {
                account_id: "acct-1".into(),
                stream_sid: "MZ-1".into(),
                call_sid: "call-1".into(),
                tracks: vec!["inbound".into(), "outbound".into()],
                media_format: MediaFormat {
                    encoding: "PCMU".into(),
                    sample_rate: 8000,
                    channels: 1,
                },
                custom_parameters,
            },
        }
    }

    fn media() -> Outbound {
        Outbound::Media {
            sequence_number: "5".into(),
            stream_sid: "MZ-1".into(),
            media: MediaPayload {
                track: "outbound".into(),
                timestamp: "1200".into(),
                payload: "AAAA".into(),
            },
        }
    }

    fn dtmf() -> Outbound {
        Outbound::Dtmf {
            sequence_number: "7".into(),
            stream_sid: "MZ-1".into(),
            dtmf: DtmfInfo {
                track: "inbound".into(),
                digit: "5".into(),
            },
        }
    }

    fn mark() -> Outbound {
        Outbound::Mark {
            stream_sid: "MZ-1".into(),
            mark: MarkInfo {
                name: "prompt-done".into(),
            },
        }
    }

    fn stop() -> Outbound {
        Outbound::Stop {
            sequence_number: "9".into(),
            stream_sid: "MZ-1".into(),
        }
    }

    #[test]
    fn start_matches_the legacy media gateway_bytes() {
        assert_serializes_to(
            &start(HashMap::new()),
            r#"{"event":"start","sequenceNumber":"1","streamSid":"MZ-1","start":{"accountId":"acct-1","streamSid":"MZ-1","callSid":"call-1","tracks":["inbound","outbound"],"mediaFormat":{"encoding":"PCMU","sampleRate":8000,"channels":1}}}"#,
        );
    }

    #[test]
    fn start_custom_parameters_are_emitted_only_when_present() {
        let one = HashMap::from([("agentId".to_string(), "a-7".to_string())]);
        assert_serializes_to(
            &start(one),
            r#"{"event":"start","sequenceNumber":"1","streamSid":"MZ-1","start":{"accountId":"acct-1","streamSid":"MZ-1","callSid":"call-1","tracks":["inbound","outbound"],"mediaFormat":{"encoding":"PCMU","sampleRate":8000,"channels":1},"customParameters":{"agentId":"a-7"}}}"#,
        );
    }

    #[test]
    fn media_timestamp_is_a_string_because_the legacy media gateway_sends_one() {
        assert_serializes_to(
            &media(),
            r#"{"event":"media","sequenceNumber":"5","streamSid":"MZ-1","media":{"track":"outbound","timestamp":"1200","payload":"AAAA"}}"#,
        );
    }

    #[test]
    fn dtmf_matches_the legacy media gateway_bytes() {
        assert_serializes_to(
            &dtmf(),
            r#"{"event":"dtmf","sequenceNumber":"7","streamSid":"MZ-1","dtmf":{"track":"inbound","digit":"5"}}"#,
        );
    }

    #[test]
    fn mark_matches_the legacy media gateway_bytes() {
        assert_serializes_to(
            &mark(),
            r#"{"event":"mark","streamSid":"MZ-1","mark":{"name":"prompt-done"}}"#,
        );
    }

    #[test]
    fn stop_matches_the legacy media gateway_bytes() {
        assert_serializes_to(
            &stop(),
            r#"{"event":"stop","sequenceNumber":"9","streamSid":"MZ-1"}"#,
        );
    }

    #[test]
    fn mark_omits_sequence_number_while_start_media_dtmf_stop_carry_it() {
        let marked = serde_json::to_string(&mark()).unwrap();
        assert!(!marked.contains("sequenceNumber"), "{marked}");
        for carrier in [start(HashMap::new()), media(), dtmf(), stop()] {
            let json = serde_json::to_string(&carrier).unwrap();
            assert!(json.contains(r#""sequenceNumber":"#), "{json}");
        }
    }

    #[test]
    fn parses_inbound_clear_and_end_of_interaction() {
        let clear: Inbound = serde_json::from_str(r#"{"event":"clear","streamSid":"s"}"#).unwrap();
        assert_eq!(
            clear,
            Inbound::Clear {
                stream_sid: Some("s".into())
            }
        );

        let eoi: Inbound = serde_json::from_str(
            r#"{"event":"endOfInteraction","streamSid":"s","context":{"k":"v"}}"#,
        )
        .unwrap();
        match eoi {
            Inbound::EndOfInteraction { context, .. } => {
                assert_eq!(context.unwrap()["k"], "v");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn unknown_inbound_event_is_an_error_we_can_ignore_upstream() {
        let res: Result<Inbound, _> = serde_json::from_str(r#"{"event":"connected"}"#);
        assert!(res.is_err());
    }
}
