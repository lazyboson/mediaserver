use crate::bencode::Value;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum NgError {
    #[error("datagram has no cookie separator")]
    NoCookie,
    #[error("bencode error: {0}")]
    Bencode(#[from] crate::bencode::BencodeError),
    #[error("rtpengine error: {0}")]
    Remote(String),
    #[error("reply missing field {0:?}")]
    MissingField(&'static str),
}

#[derive(Debug, Clone, Default)]
pub struct SubscribeRequest {
    pub call_id: String,
    pub from_tags: Vec<String>,
    pub mix: bool,
    pub accept_codecs: Vec<String>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NgReply {
    pub cookie: Vec<u8>,
    pub body: Value,
}

impl NgReply {
    pub fn sdp(&self) -> Option<&str> {
        self.body.get("sdp").and_then(Value::as_str)
    }

    pub fn to_tag(&self) -> Option<&str> {
        self.body.get("to-tag").and_then(Value::as_str)
    }
}

#[derive(Debug, Default)]
pub struct NgClient;

impl NgClient {
    fn build(cookie: &[u8], dict: BTreeMap<Vec<u8>, Value>) -> Vec<u8> {
        let mut out = Vec::with_capacity(cookie.len() + 1 + 64);
        out.extend_from_slice(cookie);
        out.push(b' ');
        Value::Dict(dict).encode_into(&mut out);
        out
    }

    pub fn ping(cookie: &[u8]) -> Vec<u8> {
        let mut d = BTreeMap::new();
        d.insert(b"command".to_vec(), Value::str("ping"));
        Self::build(cookie, d)
    }

    pub fn subscribe_request(cookie: &[u8], req: &SubscribeRequest) -> Vec<u8> {
        let mut d = BTreeMap::new();
        d.insert(b"command".to_vec(), Value::str("subscribe request"));
        d.insert(b"call-id".to_vec(), Value::str(&req.call_id));
        if !req.from_tags.is_empty() {
            d.insert(
                b"from-tags".to_vec(),
                Value::List(req.from_tags.iter().map(|t| Value::str(t)).collect()),
            );
        }
        let mut flags: Vec<Value> = Vec::new();
        if req.mix {
            flags.push(Value::str("mix"));
        }
        if !flags.is_empty() {
            d.insert(b"flags".to_vec(), Value::List(flags));
        }
        if !req.accept_codecs.is_empty() {
            let mut codec = BTreeMap::new();
            codec.insert(
                b"accept".to_vec(),
                Value::List(req.accept_codecs.iter().map(|c| Value::str(c)).collect()),
            );
            d.insert(b"codec".to_vec(), Value::Dict(codec));
        }
        if let Some(label) = &req.label {
            d.insert(b"set-label".to_vec(), Value::str(label));
        }
        Self::build(cookie, d)
    }

    pub fn subscribe_answer(cookie: &[u8], call_id: &str, to_tag: &str, sdp: &str) -> Vec<u8> {
        let mut d = BTreeMap::new();
        d.insert(b"command".to_vec(), Value::str("subscribe answer"));
        d.insert(b"call-id".to_vec(), Value::str(call_id));
        d.insert(b"to-tag".to_vec(), Value::str(to_tag));
        d.insert(b"sdp".to_vec(), Value::str(sdp));
        Self::build(cookie, d)
    }

    pub fn unsubscribe(cookie: &[u8], call_id: &str, to_tag: &str) -> Vec<u8> {
        let mut d = BTreeMap::new();
        d.insert(b"command".to_vec(), Value::str("unsubscribe"));
        d.insert(b"call-id".to_vec(), Value::str(call_id));
        d.insert(b"to-tag".to_vec(), Value::str(to_tag));
        Self::build(cookie, d)
    }

    pub fn split_cookie(datagram: &[u8]) -> Result<(&[u8], &[u8]), NgError> {
        let space = datagram
            .iter()
            .position(|&b| b == b' ')
            .ok_or(NgError::NoCookie)?;
        Ok((&datagram[..space], &datagram[space + 1..]))
    }

    pub fn parse_reply(datagram: &[u8]) -> Result<NgReply, NgError> {
        let (cookie_bytes, body_bytes) = Self::split_cookie(datagram)?;
        let cookie = cookie_bytes.to_vec();
        let body = Value::decode(body_bytes)?;
        match body.get("result").and_then(Value::as_str) {
            Some("error") => {
                let reason = body
                    .get("error-reason")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                Err(NgError::Remote(reason))
            }
            Some(_) => Ok(NgReply { cookie, body }),
            None => Err(NgError::MissingField("result")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_request_wire_shape() {
        let req = SubscribeRequest {
            call_id: "call-1".into(),
            from_tags: vec!["tagA".into()],
            mix: false,
            accept_codecs: vec!["PCMU".into()],
            label: Some("mss-tap".into()),
        };
        let wire = NgClient::subscribe_request(b"c1", &req);
        let s = String::from_utf8_lossy(&wire);
        assert!(s.starts_with("c1 d"), "{s}");
        assert!(s.contains("7:command17:subscribe request"), "{s}");
        assert!(s.contains("7:call-id6:call-1"), "{s}");
        assert!(s.contains("9:from-tagsl4:tagAe"), "{s}");
        assert!(s.contains("5:codecd6:acceptl4:PCMUee"), "{s}");
        assert!(!s.contains("3:mix"), "{s}");
    }

    #[test]
    fn mix_flag_is_emitted_when_set() {
        let req = SubscribeRequest {
            call_id: "c".into(),
            mix: true,
            ..Default::default()
        };
        let wire = NgClient::subscribe_request(b"c2", &req);
        let s = String::from_utf8_lossy(&wire);
        assert!(s.contains("5:flagsl3:mixe"), "{s}");
    }

    #[test]
    fn parses_ok_reply_with_sdp_and_totag() {
        let wire = b"c1 d6:result2:ok3:sdp26:v=0\r\nm=audio 30000 RTP/AVP6:to-tag5:t-abce";
        let reply = NgClient::parse_reply(wire).unwrap();
        assert_eq!(reply.cookie, b"c1");
        assert_eq!(reply.to_tag(), Some("t-abc"));
        assert!(reply.sdp().unwrap().starts_with("v=0"));
    }

    #[test]
    fn error_reply_surfaces_reason() {
        let wire = b"c9 d12:error-reason15:Unknown call-id6:result5:errore";
        match NgClient::parse_reply(wire) {
            Err(NgError::Remote(reason)) => assert_eq!(reason, "Unknown call-id"),
            other => panic!("expected remote error, got {other:?}"),
        }
    }

    #[test]
    fn garbage_is_error_not_panic() {
        assert!(NgClient::parse_reply(b"no-space-datagram").is_err());
        assert!(NgClient::parse_reply(b"c1 not-bencode").is_err());
    }

    #[test]
    fn split_cookie_survives_error_replies_so_they_stay_correlatable() {
        let wire = b"c9 d12:error-reason15:Unknown call-id6:result5:errore";
        let (cookie, body) = NgClient::split_cookie(wire).unwrap();
        assert_eq!(cookie, b"c9");
        assert!(body.starts_with(b"d12:error-reason"));
        assert!(NgClient::parse_reply(wire).is_err());
        assert_eq!(NgClient::split_cookie(b"no-space"), Err(NgError::NoCookie));
    }
}
