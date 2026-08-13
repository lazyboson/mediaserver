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
    pub timestamp: u64,
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

    #[test]
    fn media_message_matches_mediagateway_shape() {
        let msg = Outbound::Media {
            sequence_number: "5".into(),
            stream_sid: "sid-1".into(),
            media: MediaPayload {
                track: "outbound".into(),
                timestamp: 1200,
                payload: "AAAA".into(),
            },
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["event"], "media");
        assert_eq!(json["media"]["track"], "outbound");
        assert_eq!(json["media"]["timestamp"], 1200);
        assert_eq!(json["streamSid"], "sid-1");
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
