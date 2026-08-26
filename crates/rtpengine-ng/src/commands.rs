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
    pub transcode_codecs: Vec<String>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlayTarget {
    HeardBy(String),
    HeardByEveryone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaySource {
    Blob(Vec<u8>),
    File(String),
}

#[derive(Debug, Clone)]
pub struct PlayMedia {
    pub call_id: String,
    pub target: PlayTarget,
    pub source: PlaySource,
    pub repeat_times: Option<i64>,
    pub block_egress: bool,
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

    pub fn tags(&self) -> Vec<String> {
        let Some(Value::Dict(tags)) = self.body.get("tags") else {
            return Vec::new();
        };
        tags.keys()
            .filter_map(|tag| std::str::from_utf8(tag).ok())
            .filter(|tag| !tag.is_empty())
            .map(str::to_string)
            .collect()
    }

    pub fn tags_created(&self) -> Vec<(String, Option<i64>)> {
        let Some(Value::Dict(tags)) = self.body.get("tags") else {
            return Vec::new();
        };
        let mut found = Vec::new();
        for (tag, detail) in tags {
            let Ok(tag) = std::str::from_utf8(tag) else {
                continue;
            };
            if tag.is_empty() {
                continue;
            }
            let created = match detail.get("created") {
                Some(Value::Int(seconds)) => Some(*seconds),
                _ => None,
            };
            found.push((tag.to_string(), created));
        }
        found
    }

    pub fn rtpengine_version(&self) -> Option<&str> {
        self.body.get("version").and_then(Value::as_str)
    }

    pub fn ssrc_by_tag(&self) -> Vec<(String, u32)> {
        let mut found = Vec::new();
        let Some(Value::Dict(tags)) = self.body.get("tags") else {
            return found;
        };
        for (tag, detail) in tags {
            let Ok(tag) = std::str::from_utf8(tag) else {
                continue;
            };
            let Some(Value::List(medias)) = detail.get("medias") else {
                continue;
            };
            for media in medias {
                let Some(Value::List(streams)) = media.get("streams") else {
                    continue;
                };
                for stream in streams {
                    if let Some(Value::Int(ssrc)) = stream.get("SSRC") {
                        if let Ok(ssrc) = u32::try_from(*ssrc) {
                            found.push((tag.to_string(), ssrc));
                        }
                    }
                }
            }
        }
        found
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

    pub fn version(cookie: &[u8]) -> Vec<u8> {
        let mut d = BTreeMap::new();
        d.insert(b"command".to_vec(), Value::str("version"));
        Self::build(cookie, d)
    }

    pub fn statistics(cookie: &[u8]) -> Vec<u8> {
        let mut d = BTreeMap::new();
        d.insert(b"command".to_vec(), Value::str("statistics"));
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
        let mut codec = BTreeMap::new();
        if !req.accept_codecs.is_empty() {
            codec.insert(
                b"accept".to_vec(),
                Value::List(req.accept_codecs.iter().map(|c| Value::str(c)).collect()),
            );
        }
        if !req.transcode_codecs.is_empty() {
            codec.insert(
                b"transcode".to_vec(),
                Value::List(req.transcode_codecs.iter().map(|c| Value::str(c)).collect()),
            );
        }
        if !codec.is_empty() {
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

    pub fn query(cookie: &[u8], call_id: &str) -> Vec<u8> {
        let mut d = BTreeMap::new();
        d.insert(b"command".to_vec(), Value::str("query"));
        d.insert(b"call-id".to_vec(), Value::str(call_id));
        Self::build(cookie, d)
    }

    pub fn unsubscribe(cookie: &[u8], call_id: &str, to_tag: &str) -> Vec<u8> {
        let mut d = BTreeMap::new();
        d.insert(b"command".to_vec(), Value::str("unsubscribe"));
        d.insert(b"call-id".to_vec(), Value::str(call_id));
        d.insert(b"to-tag".to_vec(), Value::str(to_tag));
        Self::build(cookie, d)
    }

    fn insert_target(d: &mut BTreeMap<Vec<u8>, Value>, target: &PlayTarget) {
        match target {
            PlayTarget::HeardBy(tag) => {
                d.insert(b"from-tag".to_vec(), Value::str(tag));
            }
            PlayTarget::HeardByEveryone => {
                d.insert(b"all".to_vec(), Value::str("all"));
            }
        }
    }

    pub fn play_media(cookie: &[u8], play: &PlayMedia) -> Vec<u8> {
        let mut d = BTreeMap::new();
        d.insert(b"command".to_vec(), Value::str("play media"));
        d.insert(b"call-id".to_vec(), Value::str(&play.call_id));
        Self::insert_target(&mut d, &play.target);
        match &play.source {
            PlaySource::Blob(bytes) => {
                d.insert(b"blob".to_vec(), Value::Bytes(bytes.clone()));
            }
            PlaySource::File(path) => {
                d.insert(b"file".to_vec(), Value::str(path));
            }
        }
        if let Some(times) = play.repeat_times {
            d.insert(b"repeat-times".to_vec(), Value::Int(times));
        }
        if play.block_egress {
            d.insert(
                b"flags".to_vec(),
                Value::List(vec![Value::str("block-egress")]),
            );
        }
        Self::build(cookie, d)
    }

    pub fn stop_media(cookie: &[u8], call_id: &str, target: &PlayTarget) -> Vec<u8> {
        let mut d = BTreeMap::new();
        d.insert(b"command".to_vec(), Value::str("stop media"));
        d.insert(b"call-id".to_vec(), Value::str(call_id));
        Self::insert_target(&mut d, target);
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
            transcode_codecs: Vec::new(),
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
    fn transcode_is_what_makes_rtpengine_convert_an_alaw_leg() {
        let req = SubscribeRequest {
            call_id: "c".into(),
            transcode_codecs: vec!["PCMU".into()],
            ..Default::default()
        };
        let wire = NgClient::subscribe_request(b"c3", &req);
        let s = String::from_utf8_lossy(&wire);
        assert!(s.contains("5:codecd9:transcodel4:PCMUee"), "{s}");
        assert!(!s.contains("6:accept"), "{s}");
    }

    #[test]
    fn accept_and_transcode_can_be_asked_for_together() {
        let req = SubscribeRequest {
            call_id: "c".into(),
            accept_codecs: vec!["PCMU".into()],
            transcode_codecs: vec!["PCMU".into()],
            ..Default::default()
        };
        let wire = NgClient::subscribe_request(b"c4", &req);
        let s = String::from_utf8_lossy(&wire);
        assert!(
            s.contains("5:codecd6:acceptl4:PCMUe9:transcodel4:PCMUee"),
            "{s}"
        );
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

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn play_media_reaches_only_the_targeted_listener() {
        let play = PlayMedia {
            call_id: "call-1".into(),
            target: PlayTarget::HeardBy("tagA".into()),
            source: PlaySource::File("/srv/prompt.wav".into()),
            repeat_times: None,
            block_egress: false,
        };
        let wire = NgClient::play_media(b"p1", &play);
        let s = String::from_utf8_lossy(&wire);
        assert!(s.starts_with("p1 d"), "{s}");
        assert!(s.contains("7:command10:play media"), "{s}");
        assert!(s.contains("7:call-id6:call-1"), "{s}");
        assert!(s.contains("8:from-tag4:tagA"), "{s}");
        assert!(s.contains("4:file15:/srv/prompt.wav"), "{s}");
        assert!(!s.contains("3:all"), "{s}");
        assert!(!s.contains("12:repeat-times"), "{s}");
        assert!(!s.contains("5:flags"), "{s}");
    }

    #[test]
    fn play_media_to_everyone_uses_the_all_key_and_not_a_from_tag() {
        let play = PlayMedia {
            call_id: "c".into(),
            target: PlayTarget::HeardByEveryone,
            source: PlaySource::File("x.wav".into()),
            repeat_times: Some(2),
            block_egress: false,
        };
        let wire = NgClient::play_media(b"p2", &play);
        let s = String::from_utf8_lossy(&wire);
        assert!(s.contains("3:all3:all"), "{s}");
        assert!(!s.contains("8:from-tag"), "{s}");
        assert!(s.contains("12:repeat-timesi2e"), "{s}");
    }

    #[test]
    fn play_media_carries_a_binary_blob_byte_for_byte() {
        let wav = vec![0x52, 0x49, 0x46, 0x46, 0x00, 0xFF, 0x80];
        let play = PlayMedia {
            call_id: "c".into(),
            target: PlayTarget::HeardBy("tagB".into()),
            source: PlaySource::Blob(wav.clone()),
            repeat_times: None,
            block_egress: true,
        };
        let wire = NgClient::play_media(b"p3", &play);
        let mut expected = b"4:blob7:".to_vec();
        expected.extend_from_slice(&wav);
        assert!(contains(&wire, &expected), "{wire:?}");
        assert!(!contains(&wire, b"4:file"), "{wire:?}");
        assert!(contains(&wire, b"5:flagsl12:block-egresse"), "{wire:?}");
    }

    #[test]
    fn stop_media_mirrors_the_play_target() {
        let one = NgClient::stop_media(b"s1", "call-1", &PlayTarget::HeardBy("tagA".into()));
        let s = String::from_utf8_lossy(&one);
        assert!(s.contains("7:command10:stop media"), "{s}");
        assert!(s.contains("8:from-tag4:tagA"), "{s}");

        let every = NgClient::stop_media(b"s2", "call-1", &PlayTarget::HeardByEveryone);
        let s = String::from_utf8_lossy(&every);
        assert!(s.contains("3:all3:all"), "{s}");
        assert!(!s.contains("8:from-tag"), "{s}");
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

#[cfg(test)]
mod query_tests {
    use super::*;

    fn reply_from(body: &str) -> NgReply {
        NgReply {
            cookie: b"c1".to_vec(),
            body: Value::decode(body.as_bytes()).unwrap(),
        }
    }

    #[test]
    fn query_asks_for_exactly_one_call() {
        let datagram = NgClient::query(b"q1", "call-abc");
        let text = String::from_utf8_lossy(&datagram);
        assert!(text.starts_with("q1 d"));
        assert!(text.contains("7:call-id8:call-abc"));
        assert!(text.contains("7:command5:query"));
    }

    #[test]
    fn ssrc_by_tag_walks_the_shape_rtpengine_actually_returns() {
        let reply = reply_from(
            "d6:result2:ok4:tagsd8:hosttestd6:mediasld7:streamsld4:SSRCi2004318071eeeeee\
13:QF4pc39U9Dj8Fd6:mediasld7:streamsld4:SSRCi1985452339eeeeeeee",
        );
        let mut found = reply.ssrc_by_tag();
        found.sort();
        assert_eq!(
            found,
            vec![
                ("QF4pc39U9Dj8F".to_string(), 1985452339),
                ("hosttest".to_string(), 2004318071),
            ]
        );
    }

    #[test]
    fn a_reply_without_tags_or_ssrcs_yields_nothing_rather_than_failing() {
        assert!(reply_from("d6:result2:oke").ssrc_by_tag().is_empty());
        let stream_without_ssrc =
            reply_from("d4:tagsd1:ad6:mediasld7:streamsld4:porti30000eeeeeeee");
        assert!(stream_without_ssrc.ssrc_by_tag().is_empty());
    }
}

#[cfg(test)]
mod tag_tests {
    use super::*;

    fn reply(body: &str) -> NgReply {
        NgReply {
            cookie: b"c".to_vec(),
            body: Value::decode(body.as_bytes()).unwrap(),
        }
    }

    #[test]
    fn tags_lists_every_participant_rtpengine_knows() {
        let held =
            reply("d6:result2:ok4:tagsd13:QF4pc39U9Dj8Fd6:mediaslee8:hosttestd6:mediasleeee");
        let mut found = held.tags();
        found.sort();
        assert_eq!(
            found,
            vec!["QF4pc39U9Dj8F".to_string(), "hosttest".to_string()]
        );
    }

    #[test]
    fn a_reply_with_no_tags_yields_nothing_rather_than_failing() {
        assert!(reply("d6:result2:oke").tags().is_empty());
    }

    #[test]
    fn tags_created_reports_the_second_rtpengine_stamped_on_each_participant() {
        let held = reply(
            "d4:tagsd4:legAd7:createdi1787737383e3:tag4:legAe\
4:legCd7:createdi1787737395e3:tag4:legCeee",
        );
        assert_eq!(
            held.tags_created(),
            vec![
                ("legA".to_string(), Some(1787737383)),
                ("legC".to_string(), Some(1787737395)),
            ]
        );
    }

    #[test]
    fn a_participant_without_a_created_stamp_is_reported_as_unstamped() {
        let held = reply("d4:tagsd4:legAd3:tag4:legAe4:legBd7:createdi7eeee");
        assert_eq!(
            held.tags_created(),
            vec![("legA".to_string(), None), ("legB".to_string(), Some(7))]
        );
    }

    #[test]
    fn tags_created_of_a_reply_without_tags_is_empty_rather_than_a_failure() {
        assert!(reply("d6:result2:oke").tags_created().is_empty());
    }
}
