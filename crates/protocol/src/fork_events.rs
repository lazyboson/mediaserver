use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "camelCase")]
pub enum ForkEvent {
    #[serde(rename = "firstDtmf", rename_all = "camelCase")]
    FirstDtmf { call_sid: String },
    #[serde(rename = "dtmfResult", rename_all = "camelCase")]
    DtmfResult { call_sid: String, digits: String },
    #[serde(rename = "playbackStop", rename_all = "camelCase")]
    PlaybackStop { call_sid: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_streamfsm_wire_format() {
        let ev = ForkEvent::DtmfResult {
            call_sid: "u-1".into(),
            digits: "42#".into(),
        };
        assert_eq!(
            serde_json::to_string(&ev).unwrap(),
            r#"{"event":"dtmfResult","callSid":"u-1","digits":"42#"}"#
        );
        let parsed: ForkEvent =
            serde_json::from_str(r#"{"event":"firstDtmf","callSid":"u-2"}"#).unwrap();
        assert_eq!(
            parsed,
            ForkEvent::FirstDtmf {
                call_sid: "u-2".into()
            }
        );
    }
}
