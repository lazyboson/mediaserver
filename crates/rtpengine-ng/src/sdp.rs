use media_core::{AudioFormat, Encoding};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SdpError {
    #[error("offer contains no m=audio section")]
    NoAudioMedia,
    #[error("offer contains non-audio media {0:?}")]
    NonAudioMedia(String),
    #[error("malformed m= line: {0:?}")]
    MalformedMediaLine(String),
    #[error("malformed port in m= line: {0:?}")]
    MalformedPort(String),
    #[error("malformed payload type in m= line: {0:?}")]
    MalformedPayloadType(String),
    #[error("m= line declares no payload type: {0:?}")]
    NoPayloadType(String),
    #[error("answer needs one receive port per offered stream: offered {offered}, given {given}")]
    ReceivePortCountMismatch { offered: usize, given: usize },
    #[error("encoding {0:?} has no static payload type; dynamic rtpmap is not supported yet")]
    NoStaticPayloadType(Encoding),
    #[error("channel count {0} is not supported on the subscription leg")]
    UnsupportedChannelCount(u8),
    #[error("format has no usable ptime/sample rate")]
    UnusableFormat,
    #[error("offer carries no payload type this tap can decode: offered {0:?}")]
    NoDecodableCodecOffered(Vec<u8>),
    #[error("the offer carries no payload type for {0:?}")]
    CodecNotOffered(Encoding),
    #[error("opus must be signalled at 48000 Hz per RFC 7587; the offer said {0}")]
    OpusClockRate(u32),
    #[error("an inline leg speaks pcmu or pcma; the offer carries {0:?}")]
    NoInlineCodecOffered(Vec<String>),
    #[error("an inline answer needs one audio stream; the offer carries {0}")]
    InlineStreamCount(usize),
    #[error("the offer's media port is zero; an inline leg needs a peer to send to")]
    NoPeerPort,
    #[error("the offer carries no connection address an inline leg could send to")]
    NoPeerAddress,
    #[error("ptime {0} ms is outside what an inline leg paces")]
    UnusablePtime(u32),
}

pub const TELEPHONE_EVENT: &str = "telephone-event";
pub const TELEPHONE_EVENT_FMTP: &str = "0-15";
pub const INLINE_SESSION_NAME: &str = "mss-inline";
pub const INLINE_CODECS: [Encoding; 2] = [Encoding::Pcmu, Encoding::Pcma];
pub const MAX_INLINE_PTIME_MS: u32 = 120;
pub const OPUS: &str = "opus";
pub const OPUS_CLOCK_RATE_HZ: u32 = 48000;
pub const OPUS_RTPMAP_CHANNELS: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NegotiatedCodec {
    pub payload_type: u8,
    pub encoding: Encoding,
    pub clock_rate_hz: u32,
}

impl NegotiatedCodec {
    pub fn rtpmap_line(&self) -> String {
        match self.encoding {
            Encoding::Opus => format!(
                "a=rtpmap:{} {}/{}/{}\r\n",
                self.payload_type, OPUS, OPUS_CLOCK_RATE_HZ, OPUS_RTPMAP_CHANNELS
            ),
            other => format!(
                "a=rtpmap:{} {}/{}\r\n",
                self.payload_type,
                other.rtpmap_name(),
                self.clock_rate_hz
            ),
        }
    }

    pub fn from_static_format(format: AudioFormat) -> Result<NegotiatedCodec, SdpError> {
        let payload_type = format
            .encoding
            .static_payload_type()
            .ok_or(SdpError::NoStaticPayloadType(format.encoding))?;
        Ok(NegotiatedCodec {
            payload_type,
            encoding: format.encoding,
            clock_rate_hz: format.sample_rate_hz,
        })
    }

    pub fn samples_per_packet(&self, ptime_ms: u32) -> Option<u32> {
        if ptime_ms == 0 || self.clock_rate_hz == 0 {
            return None;
        }
        Some(self.clock_rate_hz / 1000 * ptime_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpMap {
    pub payload_type: u8,
    pub encoding_name: String,
    pub clock_rate_hz: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OfferedStream {
    pub port: u16,
    pub payload_types: Vec<u8>,
    pub rtpmaps: Vec<RtpMap>,
    pub ptime_ms: Option<u32>,
    pub label: Option<String>,
    pub connection_address: Option<String>,
}

impl OfferedStream {
    pub fn telephone_event(&self) -> Option<&RtpMap> {
        self.rtpmaps
            .iter()
            .find(|map| map.encoding_name.eq_ignore_ascii_case(TELEPHONE_EVENT))
    }

    fn codec_at(&self, payload_type: u8) -> Result<Option<NegotiatedCodec>, SdpError> {
        let rtpmap = self
            .rtpmaps
            .iter()
            .find(|map| map.payload_type == payload_type);
        if let Some(encoding) = Encoding::from_static_payload_type(payload_type) {
            let clock_rate_hz = rtpmap
                .map(|map| map.clock_rate_hz)
                .or_else(|| encoding.static_clock_rate_hz())
                .unwrap_or_default();
            if clock_rate_hz == 0 {
                return Err(SdpError::UnusableFormat);
            }
            return Ok(Some(NegotiatedCodec {
                payload_type,
                encoding,
                clock_rate_hz,
            }));
        }
        let Some(rtpmap) = rtpmap else {
            return Ok(None);
        };
        if rtpmap.encoding_name.eq_ignore_ascii_case(OPUS) {
            if rtpmap.clock_rate_hz != OPUS_CLOCK_RATE_HZ {
                return Err(SdpError::OpusClockRate(rtpmap.clock_rate_hz));
            }
            return Ok(Some(NegotiatedCodec {
                payload_type,
                encoding: Encoding::Opus,
                clock_rate_hz: OPUS_CLOCK_RATE_HZ,
            }));
        }
        Ok(None)
    }

    pub fn negotiate(&self) -> Result<NegotiatedCodec, SdpError> {
        for payload_type in &self.payload_types {
            if let Some(codec) = self.codec_at(*payload_type)? {
                return Ok(codec);
            }
        }
        Err(SdpError::NoDecodableCodecOffered(
            self.payload_types.clone(),
        ))
    }

    pub fn negotiate_encoding(&self, encoding: Encoding) -> Result<NegotiatedCodec, SdpError> {
        for payload_type in &self.payload_types {
            if let Some(codec) = self.codec_at(*payload_type)? {
                if codec.encoding == encoding {
                    return Ok(codec);
                }
            }
        }
        Err(SdpError::CodecNotOffered(encoding))
    }

    pub fn offered_codec_names(&self) -> Vec<String> {
        self.payload_types
            .iter()
            .map(|payload_type| self.codec_name_of(*payload_type))
            .collect()
    }

    fn codec_name_of(&self, payload_type: u8) -> String {
        if let Some(rtpmap) = self
            .rtpmaps
            .iter()
            .find(|map| map.payload_type == payload_type)
        {
            return rtpmap.encoding_name.clone();
        }
        match Encoding::from_static_payload_type(payload_type) {
            Some(encoding) => encoding.rtpmap_name().to_string(),
            None => format!("payload type {payload_type}"),
        }
    }

    pub fn negotiate_inline(&self) -> Result<NegotiatedCodec, SdpError> {
        for payload_type in &self.payload_types {
            let Some(encoding) = Encoding::from_static_payload_type(*payload_type) else {
                continue;
            };
            if !INLINE_CODECS.contains(&encoding) {
                continue;
            }
            let clock_rate_hz = self
                .rtpmaps
                .iter()
                .find(|map| map.payload_type == *payload_type)
                .map(|map| map.clock_rate_hz)
                .or_else(|| encoding.static_clock_rate_hz())
                .unwrap_or_default();
            if clock_rate_hz == 0 {
                return Err(SdpError::UnusableFormat);
            }
            return Ok(NegotiatedCodec {
                payload_type: *payload_type,
                encoding,
                clock_rate_hz,
            });
        }
        Err(SdpError::NoInlineCodecOffered(self.offered_codec_names()))
    }

    pub fn offered_format(&self, ptime_fallback_ms: u32) -> Result<AudioFormat, SdpError> {
        let codec = self.negotiate()?;
        let format = AudioFormat {
            encoding: codec.encoding,
            sample_rate_hz: codec.clock_rate_hz,
            channels: 1,
            ptime_ms: self.ptime_ms.unwrap_or(ptime_fallback_ms),
        };
        if format.samples_per_packet().is_none() {
            return Err(SdpError::UnusableFormat);
        }
        Ok(format)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubscriptionOffer {
    pub connection_address: Option<String>,
    pub streams: Vec<OfferedStream>,
}

impl SubscriptionOffer {
    pub fn parse(sdp: &str) -> Result<Self, SdpError> {
        let mut offer = SubscriptionOffer::default();
        for raw in sdp.split('\n') {
            let line = raw.trim_end_matches('\r');
            let Some((kind, rest)) = line.split_once('=') else {
                continue;
            };
            match kind {
                "m" => offer.streams.push(parse_media_line(rest)?),
                "c" => {
                    let address = connection_address(rest);
                    match offer.streams.last_mut() {
                        Some(stream) => stream.connection_address = address,
                        None => offer.connection_address = address,
                    }
                }
                "a" => {
                    if let Some(stream) = offer.streams.last_mut() {
                        apply_media_attribute(stream, rest);
                    }
                }
                _ => {}
            }
        }
        if offer.streams.is_empty() {
            return Err(SdpError::NoAudioMedia);
        }
        Ok(offer)
    }

    pub fn stream_address(&self, index: usize) -> Option<&str> {
        let stream = self.streams.get(index)?;
        stream
            .connection_address
            .as_deref()
            .or(self.connection_address.as_deref())
    }
}

fn parse_media_line(rest: &str) -> Result<OfferedStream, SdpError> {
    let mut fields = rest.split_whitespace();
    let media = fields
        .next()
        .ok_or_else(|| SdpError::MalformedMediaLine(rest.to_string()))?;
    if media != "audio" {
        return Err(SdpError::NonAudioMedia(media.to_string()));
    }
    let port_field = fields
        .next()
        .ok_or_else(|| SdpError::MalformedMediaLine(rest.to_string()))?;
    let port = port_field
        .split('/')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .ok_or_else(|| SdpError::MalformedPort(rest.to_string()))?;
    fields
        .next()
        .ok_or_else(|| SdpError::MalformedMediaLine(rest.to_string()))?;
    let mut payload_types = Vec::new();
    for field in fields {
        let pt = field
            .parse::<u8>()
            .map_err(|_| SdpError::MalformedPayloadType(rest.to_string()))?;
        if pt > 127 {
            return Err(SdpError::MalformedPayloadType(rest.to_string()));
        }
        payload_types.push(pt);
    }
    if payload_types.is_empty() {
        return Err(SdpError::NoPayloadType(rest.to_string()));
    }
    Ok(OfferedStream {
        port,
        payload_types,
        ..Default::default()
    })
}

fn parse_rtpmap(value: &str) -> Option<RtpMap> {
    let (payload_type, description) = value.trim().split_once(' ')?;
    let mut parts = description.split('/');
    let encoding_name = parts.next()?.to_string();
    let clock_rate_hz = parts.next()?.parse().ok()?;
    Some(RtpMap {
        payload_type: payload_type.parse().ok()?,
        encoding_name,
        clock_rate_hz,
    })
}

fn connection_address(rest: &str) -> Option<String> {
    rest.split_whitespace().nth(2).map(|a| a.to_string())
}

fn apply_media_attribute(stream: &mut OfferedStream, rest: &str) {
    if let Some(value) = rest.strip_prefix("ptime:") {
        if let Ok(ptime) = value.trim().parse::<u32>() {
            stream.ptime_ms = Some(ptime);
        }
    } else if let Some(value) = rest.strip_prefix("label:") {
        stream.label = Some(value.trim().to_string());
    } else if let Some(value) = rest.strip_prefix("rtpmap:") {
        if let Some(map) = parse_rtpmap(value) {
            stream.rtpmaps.push(map);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SubscriptionAnswer<'a> {
    pub session_id: u64,
    pub local_address: &'a str,
    pub receive_ports: &'a [u16],
    pub format: AudioFormat,
    pub answer_with: NegotiatedCodec,
}

impl SubscriptionAnswer<'_> {
    pub fn to_sdp(&self, offer: &SubscriptionOffer) -> Result<String, SdpError> {
        if self.receive_ports.len() != offer.streams.len() {
            return Err(SdpError::ReceivePortCountMismatch {
                offered: offer.streams.len(),
                given: self.receive_ports.len(),
            });
        }
        if self.format.channels != 1 {
            return Err(SdpError::UnsupportedChannelCount(self.format.channels));
        }
        if self.format.samples_per_packet().is_none() {
            return Err(SdpError::UnusableFormat);
        }

        let mut sdp = String::with_capacity(256 + self.receive_ports.len() * 128);
        sdp.push_str("v=0\r\n");
        sdp.push_str(&format!(
            "o=- {id} {id} IN IP4 {addr}\r\n",
            id = self.session_id,
            addr = self.local_address
        ));
        sdp.push_str("s=mss-tap\r\n");
        sdp.push_str(&format!("c=IN IP4 {}\r\n", self.local_address));
        sdp.push_str("t=0 0\r\n");
        for (port, stream) in self.receive_ports.iter().zip(&offer.streams) {
            let answered = self.answer_with;
            let payload_type = answered.payload_type;
            let mut retained = vec![payload_type];
            retained.extend(
                stream
                    .payload_types
                    .iter()
                    .copied()
                    .filter(|offered| *offered != payload_type),
            );
            let formats = retained
                .iter()
                .map(|pt| pt.to_string())
                .collect::<Vec<_>>()
                .join(" ");
            sdp.push_str(&format!("m=audio {port} RTP/AVP {formats}\r\n"));
            sdp.push_str(&answered.rtpmap_line());
            for offered in retained.iter().skip(1) {
                if let Some(rtpmap) = stream
                    .rtpmaps
                    .iter()
                    .find(|map| map.payload_type == *offered)
                {
                    sdp.push_str(&format!(
                        "a=rtpmap:{} {}/{}\r\n",
                        rtpmap.payload_type, rtpmap.encoding_name, rtpmap.clock_rate_hz
                    ));
                }
            }
            sdp.push_str(&format!("a=ptime:{}\r\n", self.format.ptime_ms));
            sdp.push_str("a=recvonly\r\n");
        }
        Ok(sdp)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineOffer {
    pub peer_address: String,
    pub peer_port: u16,
    pub codec: NegotiatedCodec,
    pub ptime_ms: u32,
    pub telephone_event: Option<RtpMap>,
}

impl InlineOffer {
    pub fn parse(sdp: &str, ptime_fallback_ms: u32) -> Result<InlineOffer, SdpError> {
        let offer = SubscriptionOffer::parse(sdp)?;
        InlineOffer::from_offer(&offer, ptime_fallback_ms)
    }

    pub fn from_offer(
        offer: &SubscriptionOffer,
        ptime_fallback_ms: u32,
    ) -> Result<InlineOffer, SdpError> {
        if offer.streams.len() != 1 {
            return Err(SdpError::InlineStreamCount(offer.streams.len()));
        }
        let stream = &offer.streams[0];
        if stream.port == 0 {
            return Err(SdpError::NoPeerPort);
        }
        let peer_address = offer
            .stream_address(0)
            .filter(|address| !address.is_empty())
            .ok_or(SdpError::NoPeerAddress)?
            .to_string();
        let codec = stream.negotiate_inline()?;
        let ptime_ms = stream.ptime_ms.unwrap_or(ptime_fallback_ms);
        if ptime_ms == 0
            || ptime_ms > MAX_INLINE_PTIME_MS
            || codec.samples_per_packet(ptime_ms).unwrap_or_default() == 0
        {
            return Err(SdpError::UnusablePtime(ptime_ms));
        }
        Ok(InlineOffer {
            peer_address,
            peer_port: stream.port,
            codec,
            ptime_ms,
            telephone_event: stream.telephone_event().cloned(),
        })
    }

    pub fn format(&self) -> AudioFormat {
        AudioFormat {
            encoding: self.codec.encoding,
            sample_rate_hz: self.codec.clock_rate_hz,
            channels: 1,
            ptime_ms: self.ptime_ms,
        }
    }

    pub fn answer<'a>(
        &'a self,
        session_id: u64,
        local_address: &'a str,
        receive_port: u16,
    ) -> InlineAnswer<'a> {
        InlineAnswer {
            session_id,
            local_address,
            receive_port,
            answer_with: self.codec,
            ptime_ms: self.ptime_ms,
            telephone_event: self.telephone_event.as_ref(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InlineAnswer<'a> {
    pub session_id: u64,
    pub local_address: &'a str,
    pub receive_port: u16,
    pub answer_with: NegotiatedCodec,
    pub ptime_ms: u32,
    pub telephone_event: Option<&'a RtpMap>,
}

impl InlineAnswer<'_> {
    pub fn to_sdp(&self) -> Result<String, SdpError> {
        if self.receive_port == 0 {
            return Err(SdpError::NoPeerPort);
        }
        if self.ptime_ms == 0 || self.ptime_ms > MAX_INLINE_PTIME_MS {
            return Err(SdpError::UnusablePtime(self.ptime_ms));
        }
        if !INLINE_CODECS.contains(&self.answer_with.encoding) {
            return Err(SdpError::NoInlineCodecOffered(vec![self
                .answer_with
                .encoding
                .rtpmap_name()
                .to_string()]));
        }
        let mut sdp = String::with_capacity(256);
        sdp.push_str("v=0\r\n");
        sdp.push_str(&format!(
            "o=- {id} {id} IN IP4 {addr}\r\n",
            id = self.session_id,
            addr = self.local_address
        ));
        sdp.push_str(&format!("s={INLINE_SESSION_NAME}\r\n"));
        sdp.push_str(&format!("c=IN IP4 {}\r\n", self.local_address));
        sdp.push_str("t=0 0\r\n");
        match self.telephone_event {
            Some(event) => sdp.push_str(&format!(
                "m=audio {} RTP/AVP {} {}\r\n",
                self.receive_port, self.answer_with.payload_type, event.payload_type
            )),
            None => sdp.push_str(&format!(
                "m=audio {} RTP/AVP {}\r\n",
                self.receive_port, self.answer_with.payload_type
            )),
        }
        sdp.push_str(&self.answer_with.rtpmap_line());
        if let Some(event) = self.telephone_event {
            sdp.push_str(&format!(
                "a=rtpmap:{} {}/{}\r\n",
                event.payload_type, event.encoding_name, event.clock_rate_hz
            ));
            sdp.push_str(&format!(
                "a=fmtp:{} {TELEPHONE_EVENT_FMTP}\r\n",
                event.payload_type
            ));
        }
        sdp.push_str(&format!("a=ptime:{}\r\n", self.ptime_ms));
        sdp.push_str("a=sendrecv\r\n");
        Ok(sdp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INLINE_SIP_OFFER: &str = "v=0\r\n\
o=peer 42 42 IN IP4 10.9.0.4\r\n\
s=-\r\n\
c=IN IP4 10.9.0.4\r\n\
t=0 0\r\n\
m=audio 41000 RTP/AVP 0 101\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=fmtp:101 0-16\r\n\
a=ptime:20\r\n\
a=sendrecv\r\n";

    fn inline_offer_of(media: &str) -> Result<InlineOffer, SdpError> {
        InlineOffer::parse(
            &format!(
                "v=0\r\no=peer 1 1 IN IP4 10.9.0.4\r\ns=-\r\nc=IN IP4 10.9.0.4\r\nt=0 0\r\n{media}"
            ),
            20,
        )
    }

    #[test]
    fn an_inline_offer_yields_the_peer_the_codec_and_the_dtmf_payload_type() {
        let offer = InlineOffer::parse(INLINE_SIP_OFFER, 20).expect("a plain sip offer parses");
        assert_eq!(offer.peer_address, "10.9.0.4");
        assert_eq!(offer.peer_port, 41000);
        assert_eq!(offer.codec.encoding, Encoding::Pcmu);
        assert_eq!(offer.codec.payload_type, 0);
        assert_eq!(offer.codec.clock_rate_hz, 8000);
        assert_eq!(offer.ptime_ms, 20);
        assert_eq!(
            offer
                .telephone_event
                .as_ref()
                .map(|event| event.payload_type),
            Some(101)
        );
        assert_eq!(offer.format(), AudioFormat::pcmu_8k_20ms());
    }

    #[test]
    fn the_inline_answer_offers_our_port_the_chosen_codec_and_two_way_media() {
        let offer = InlineOffer::parse(INLINE_SIP_OFFER, 20).expect("offer");
        let answer = offer
            .answer(7, "172.31.98.20", 34002)
            .to_sdp()
            .expect("an answer");
        assert_eq!(
            answer,
            "v=0\r\n\
o=- 7 7 IN IP4 172.31.98.20\r\n\
s=mss-inline\r\n\
c=IN IP4 172.31.98.20\r\n\
t=0 0\r\n\
m=audio 34002 RTP/AVP 0 101\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=fmtp:101 0-15\r\n\
a=ptime:20\r\n\
a=sendrecv\r\n"
        );
    }

    #[test]
    fn our_own_answer_parses_back_to_what_we_put_in_it() {
        let offer = InlineOffer::parse(INLINE_SIP_OFFER, 20).expect("offer");
        let answer = offer
            .answer(7, "172.31.98.20", 34002)
            .to_sdp()
            .expect("answer");
        let reparsed = InlineOffer::parse(&answer, 20).expect("the answer is valid sdp");
        assert_eq!(reparsed.peer_address, "172.31.98.20");
        assert_eq!(reparsed.peer_port, 34002);
        assert_eq!(reparsed.codec.encoding, Encoding::Pcmu);
        assert_eq!(reparsed.ptime_ms, 20);
    }

    #[test]
    fn an_offer_that_prefers_pcma_is_answered_with_pcma_in_offer_order() {
        let offer = inline_offer_of(
            "m=audio 41000 RTP/AVP 8 0\r\na=rtpmap:8 PCMA/8000\r\na=rtpmap:0 PCMU/8000\r\n",
        )
        .expect("offer");
        assert_eq!(offer.codec.encoding, Encoding::Pcma);
        assert_eq!(offer.codec.payload_type, 8);
        assert!(offer.telephone_event.is_none());
        let answer = offer.answer(1, "127.0.0.1", 5000).to_sdp().expect("answer");
        assert!(answer.contains("m=audio 5000 RTP/AVP 8\r\n"), "{answer}");
        assert!(!answer.contains("telephone-event"), "{answer}");
    }

    #[test]
    fn an_offer_this_leg_cannot_speak_is_refused_by_codec_name() {
        let refused = inline_offer_of(
            "m=audio 41000 RTP/AVP 111 9 101\r\n\
a=rtpmap:111 opus/48000/2\r\n\
a=rtpmap:9 G722/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n",
        )
        .expect_err("an inline leg has no opus or g722 encoder");
        assert_eq!(
            refused,
            SdpError::NoInlineCodecOffered(vec![
                "opus".to_string(),
                "G722".to_string(),
                "telephone-event".to_string()
            ])
        );
    }

    #[test]
    fn an_offer_without_an_rtpmap_still_names_what_it_offered() {
        let refused = inline_offer_of("m=audio 41000 RTP/AVP 9\r\n")
            .expect_err("payload type 9 is not a codec this leg speaks");
        assert_eq!(
            refused,
            SdpError::NoInlineCodecOffered(vec!["payload type 9".to_string()])
        );
    }

    #[test]
    fn an_inline_leg_answers_one_stream_only() {
        let refused = inline_offer_of(
            "m=audio 41000 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n\
m=audio 41002 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n",
        )
        .expect_err("two streams are a conference, not an inline leg");
        assert_eq!(refused, SdpError::InlineStreamCount(2));
    }

    #[test]
    fn an_offer_with_no_peer_to_send_to_is_refused() {
        assert_eq!(
            inline_offer_of("m=audio 0 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n")
                .expect_err("port zero is held media"),
            SdpError::NoPeerPort
        );
        assert_eq!(
            InlineOffer::parse(
                "v=0\r\no=peer 1 1 IN IP4 10.9.0.4\r\ns=-\r\nt=0 0\r\n\
m=audio 41000 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n",
                20
            )
            .expect_err("no c= line means no address"),
            SdpError::NoPeerAddress
        );
    }

    #[test]
    fn an_offer_with_no_ptime_takes_the_configured_one_and_an_absurd_ptime_is_refused() {
        let quiet = inline_offer_of("m=audio 41000 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n")
            .expect("an offer may leave ptime out");
        assert_eq!(quiet.ptime_ms, 20);
        assert_eq!(
            inline_offer_of("m=audio 41000 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\na=ptime:500\r\n")
                .expect_err("500 ms is not a paceable frame"),
            SdpError::UnusablePtime(500)
        );
    }

    #[test]
    fn a_stream_level_connection_address_wins_over_the_session_one() {
        let offer = inline_offer_of(
            "m=audio 41000 RTP/AVP 0\r\nc=IN IP4 10.9.0.9\r\na=rtpmap:0 PCMU/8000\r\n",
        )
        .expect("offer");
        assert_eq!(offer.peer_address, "10.9.0.9");
    }

    pub(super) const TWO_LEG_OFFER: &str = "v=0\r\n\
o=- 8000 8000 IN IP4 10.0.0.5\r\n\
s=rtpengine\r\n\
c=IN IP4 10.0.0.5\r\n\
t=0 0\r\n\
m=audio 30000 RTP/AVP 0 101\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=ptime:20\r\n\
a=label:customer\r\n\
a=sendonly\r\n\
m=audio 30002 RTP/AVP 8\r\n\
c=IN IP4 10.0.0.9\r\n\
a=rtpmap:8 PCMA/8000\r\n\
a=label:agent\r\n\
a=sendonly\r\n";

    const RTPENGINE_14_SUBSCRIBE_OFFER: &str = "v=0\r\n\
o=- 1 1 IN IP4 172.31.98.20\r\n\
s=probe\r\n\
t=0 0\r\n\
m=audio 25058 RTP/AVP 0 101\r\n\
c=IN IP4 172.31.98.10\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=sendonly\r\n\
a=rtcp:25059\r\n\
a=ptime:20\r\n\
m=audio 29114 RTP/AVP 0 101\r\n\
c=IN IP4 172.31.98.10\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=sendonly\r\n\
a=rtcp:29115\r\n\
a=ptime:20\r\n";

    const ALAW_ONLY_OFFER: &str = "v=0\r\n\
o=- 9 9 IN IP4 10.0.0.5\r\n\
s=rtpengine\r\n\
c=IN IP4 10.0.0.5\r\n\
t=0 0\r\n\
m=audio 30004 RTP/AVP 8 101\r\n\
a=rtpmap:8 PCMA/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=ptime:20\r\n\
a=sendonly\r\n";

    const OPUS_ONLY_OFFER: &str = "v=0\r\n\
o=- 9 9 IN IP4 10.0.0.5\r\n\
s=rtpengine\r\n\
c=IN IP4 10.0.0.5\r\n\
t=0 0\r\n\
m=audio 30006 RTP/AVP 111 101\r\n\
a=rtpmap:111 opus/48000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=ptime:20\r\n\
a=sendonly\r\n";

    #[test]
    fn an_offer_names_the_format_the_tap_should_accept_when_nothing_is_transcoded() {
        let offer = SubscriptionOffer::parse(TWO_LEG_OFFER).unwrap();

        let customer = offer.streams[0].offered_format(20).unwrap();
        assert_eq!(customer.encoding, Encoding::Pcmu);
        assert_eq!(customer.sample_rate_hz, 8000);
        assert_eq!(customer.channels, 1);
        assert_eq!(customer.ptime_ms, 20);

        let agent = offer.streams[1].offered_format(20).unwrap();
        assert_eq!(agent.encoding, Encoding::Pcma);
        assert_eq!(agent.ptime_ms, 20);
    }

    #[test]
    fn the_answer_to_an_alaw_only_offer_adds_no_codec_the_offer_did_not_carry() {
        let offer = SubscriptionOffer::parse(ALAW_ONLY_OFFER).unwrap();
        let format = offer.streams[0].offered_format(20).unwrap();
        assert_eq!(format.encoding, Encoding::Pcma);

        let ports = [41000u16];
        let sdp = SubscriptionAnswer {
            session_id: 3,
            local_address: "10.0.0.30",
            receive_ports: &ports,
            format,
            answer_with: offer.streams[0].negotiate().unwrap(),
        }
        .to_sdp(&offer)
        .unwrap();

        assert!(sdp.contains("m=audio 41000 RTP/AVP 8 101\r\n"), "{sdp}");
        assert!(sdp.contains("a=rtpmap:8 PCMA/8000\r\n"), "{sdp}");
        assert!(
            sdp.contains("a=rtpmap:101 telephone-event/8000\r\n"),
            "{sdp}"
        );
        assert!(!sdp.contains("a=rtpmap:0 "), "{sdp}");
    }

    #[test]
    fn our_configured_codec_would_have_added_an_unoffered_payload_type() {
        let offer = SubscriptionOffer::parse(ALAW_ONLY_OFFER).unwrap();
        let ports = [41000u16];
        let sdp = SubscriptionAnswer {
            session_id: 3,
            local_address: "10.0.0.30",
            receive_ports: &ports,
            format: AudioFormat::pcmu_8k_20ms(),
            answer_with: NegotiatedCodec::from_static_format(AudioFormat::pcmu_8k_20ms()).unwrap(),
        }
        .to_sdp(&offer)
        .unwrap();

        assert!(sdp.contains("m=audio 41000 RTP/AVP 0 8 101\r\n"), "{sdp}");
    }

    #[test]
    fn the_first_offered_g711_payload_type_wins_over_any_preference_of_ours() {
        let both = ALAW_ONLY_OFFER.replace("RTP/AVP 8 101", "RTP/AVP 8 0 101");
        let offer = SubscriptionOffer::parse(&both).unwrap();
        assert_eq!(offer.streams[0].payload_types, vec![8, 0, 101]);
        assert_eq!(
            offer.streams[0].offered_format(20).unwrap().encoding,
            Encoding::Pcma
        );
    }

    #[test]
    fn an_offer_of_codecs_this_tap_cannot_decode_is_refused_by_name() {
        let evs = OPUS_ONLY_OFFER.replace("opus/48000", "EVS/16000");
        let offer = SubscriptionOffer::parse(&evs).unwrap();
        assert_eq!(
            offer.streams[0].offered_format(20),
            Err(SdpError::NoDecodableCodecOffered(vec![111, 101]))
        );
    }

    #[test]
    fn an_opus_offer_is_decodable_now_that_libopus_is_linked() {
        let offer = SubscriptionOffer::parse(OPUS_ONLY_OFFER).unwrap();
        let format = offer.streams[0].offered_format(20).unwrap();
        assert_eq!(format.encoding, Encoding::Opus);
        assert_eq!(format.sample_rate_hz, OPUS_CLOCK_RATE_HZ);
    }

    #[test]
    fn a_stream_without_a_ptime_takes_the_callers_fallback() {
        let offer = SubscriptionOffer::parse(TWO_LEG_OFFER).unwrap();
        assert_eq!(offer.streams[1].ptime_ms, None);
        assert_eq!(offer.streams[1].offered_format(40).unwrap().ptime_ms, 40);
        assert_eq!(
            offer.streams[1].offered_format(0),
            Err(SdpError::UnusableFormat)
        );
    }

    #[test]
    fn parses_a_real_rtpengine_14_subscribe_offer() {
        let offer = SubscriptionOffer::parse(RTPENGINE_14_SUBSCRIBE_OFFER).unwrap();
        assert_eq!(offer.connection_address, None);
        assert_eq!(offer.streams.len(), 2);
        assert_eq!(offer.streams[0].port, 25058);
        assert_eq!(offer.streams[1].port, 29114);
        assert_eq!(offer.streams[0].payload_types, vec![0, 101]);
        assert_eq!(offer.streams[0].ptime_ms, Some(20));
        assert_eq!(offer.streams[0].label, None);
        assert_eq!(offer.stream_address(0), Some("172.31.98.10"));
        assert_eq!(offer.stream_address(1), Some("172.31.98.10"));

        let ports = [40000u16, 40002];
        let answer = SubscriptionAnswer {
            session_id: 7,
            local_address: "172.31.99.30",
            receive_ports: &ports,
            format: AudioFormat::pcmu_8k_20ms(),
            answer_with: NegotiatedCodec::from_static_format(AudioFormat::pcmu_8k_20ms()).unwrap(),
        };
        assert!(answer.to_sdp(&offer).is_ok());
    }

    #[test]
    fn parses_both_tapped_legs_with_ports_ptime_and_labels() {
        let offer = SubscriptionOffer::parse(TWO_LEG_OFFER).unwrap();
        assert_eq!(offer.connection_address.as_deref(), Some("10.0.0.5"));
        assert_eq!(offer.streams.len(), 2);
        assert_eq!(offer.streams[0].port, 30000);
        assert_eq!(offer.streams[0].payload_types, vec![0, 101]);
        assert_eq!(offer.streams[0].ptime_ms, Some(20));
        assert_eq!(offer.streams[0].label.as_deref(), Some("customer"));
        assert_eq!(offer.streams[1].port, 30002);
        assert_eq!(offer.streams[1].payload_types, vec![8]);
        assert_eq!(offer.streams[1].ptime_ms, None);
        assert_eq!(offer.streams[1].label.as_deref(), Some("agent"));
    }

    #[test]
    fn media_level_connection_address_overrides_session_level() {
        let offer = SubscriptionOffer::parse(TWO_LEG_OFFER).unwrap();
        assert_eq!(offer.stream_address(0), Some("10.0.0.5"));
        assert_eq!(offer.stream_address(1), Some("10.0.0.9"));
        assert_eq!(offer.stream_address(2), None);
    }

    #[test]
    fn answer_mirrors_offered_stream_count_byte_exact() {
        let offer = SubscriptionOffer::parse(TWO_LEG_OFFER).unwrap();
        let answer = SubscriptionAnswer {
            session_id: 42,
            local_address: "10.1.2.3",
            receive_ports: &[40000, 40002],
            format: AudioFormat::pcmu_8k_20ms(),
            answer_with: NegotiatedCodec::from_static_format(AudioFormat::pcmu_8k_20ms()).unwrap(),
        };
        assert_eq!(
            answer.to_sdp(&offer).unwrap(),
            "v=0\r\n\
o=- 42 42 IN IP4 10.1.2.3\r\n\
s=mss-tap\r\n\
c=IN IP4 10.1.2.3\r\n\
t=0 0\r\n\
m=audio 40000 RTP/AVP 0 101\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=ptime:20\r\n\
a=recvonly\r\n\
m=audio 40002 RTP/AVP 0 8\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:8 PCMA/8000\r\n\
a=ptime:20\r\n\
a=recvonly\r\n"
        );
    }

    #[test]
    fn answer_keeps_every_offered_payload_type_with_ours_first() {
        let offer = SubscriptionOffer::parse(
            "m=audio 30000 RTP/AVP 8 0 101\r\n\
a=rtpmap:8 PCMA/8000\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=sendonly\r\n",
        )
        .unwrap();

        let ports = [40000u16];
        let sdp = SubscriptionAnswer {
            session_id: 1,
            local_address: "172.31.99.30",
            receive_ports: &ports,
            format: AudioFormat::pcmu_8k_20ms(),
            answer_with: NegotiatedCodec::from_static_format(AudioFormat::pcmu_8k_20ms()).unwrap(),
        }
        .to_sdp(&offer)
        .unwrap();

        assert!(sdp.contains("m=audio 40000 RTP/AVP 0 8 101\r\n"), "{sdp}");
        assert!(sdp.contains("a=rtpmap:0 PCMU/8000\r\n"), "{sdp}");
        assert!(sdp.contains("a=rtpmap:8 PCMA/8000\r\n"), "{sdp}");
        assert!(
            sdp.contains("a=rtpmap:101 telephone-event/8000\r\n"),
            "{sdp}"
        );
    }

    #[test]
    fn answer_echoes_offered_telephone_event_because_rtpengine_rejects_dropping_it() {
        let offer = SubscriptionOffer::parse(RTPENGINE_14_SUBSCRIBE_OFFER).unwrap();
        assert_eq!(
            offer.streams[0].telephone_event(),
            Some(&RtpMap {
                payload_type: 101,
                encoding_name: TELEPHONE_EVENT.to_string(),
                clock_rate_hz: 8000,
            })
        );

        let ports = [40000u16, 40002];
        let sdp = SubscriptionAnswer {
            session_id: 1,
            local_address: "172.31.99.30",
            receive_ports: &ports,
            format: AudioFormat::pcmu_8k_20ms(),
            answer_with: NegotiatedCodec::from_static_format(AudioFormat::pcmu_8k_20ms()).unwrap(),
        }
        .to_sdp(&offer)
        .unwrap();

        assert_eq!(sdp.matches("m=audio").count(), 2);
        assert_eq!(sdp.matches("RTP/AVP 0 101").count(), 2);
        assert_eq!(sdp.matches("a=rtpmap:101 telephone-event/8000").count(), 2);
    }

    #[test]
    fn answer_omits_telephone_event_when_the_offer_has_none() {
        let offer = SubscriptionOffer::parse(
            "m=audio 30000 RTP/AVP 8\r\na=rtpmap:8 PCMA/8000\r\na=sendonly\r\n",
        )
        .unwrap();
        assert_eq!(offer.streams[0].telephone_event(), None);

        let ports = [40000u16];
        let sdp = SubscriptionAnswer {
            session_id: 1,
            local_address: "10.0.0.1",
            receive_ports: &ports,
            format: AudioFormat::pcmu_8k_20ms(),
            answer_with: NegotiatedCodec::from_static_format(AudioFormat::pcmu_8k_20ms()).unwrap(),
        }
        .to_sdp(&offer)
        .unwrap();

        assert!(sdp.contains("m=audio 40000 RTP/AVP 0 8\r\n"), "{sdp}");
        assert!(!sdp.contains(TELEPHONE_EVENT), "{sdp}");
    }

    #[test]
    fn rtpmaps_are_parsed_per_stream_and_malformed_ones_ignored() {
        let offer = SubscriptionOffer::parse(
            "m=audio 30000 RTP/AVP 0 101\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:nonsense\r\n\
a=rtpmap:101 telephone-event/8000/1\r\n\
a=sendonly\r\n",
        )
        .unwrap();
        assert_eq!(offer.streams[0].rtpmaps.len(), 2);
        assert_eq!(offer.streams[0].rtpmaps[0].encoding_name, "PCMU");
        assert_eq!(
            offer.streams[0].telephone_event().map(|m| m.payload_type),
            Some(101)
        );
    }

    #[test]
    fn rejects_answer_with_wrong_number_of_receive_ports() {
        let offer = SubscriptionOffer::parse(TWO_LEG_OFFER).unwrap();
        let answer = SubscriptionAnswer {
            session_id: 1,
            local_address: "10.1.2.3",
            receive_ports: &[40000],
            format: AudioFormat::pcmu_8k_20ms(),
            answer_with: NegotiatedCodec::from_static_format(AudioFormat::pcmu_8k_20ms()).unwrap(),
        };
        assert_eq!(
            answer.to_sdp(&offer),
            Err(SdpError::ReceivePortCountMismatch {
                offered: 2,
                given: 1
            })
        );
    }

    #[test]
    fn rejects_formats_the_subscription_leg_cannot_signal() {
        let offer = SubscriptionOffer::parse(TWO_LEG_OFFER).unwrap();
        let ports = [40000u16, 40002];
        let base = SubscriptionAnswer {
            session_id: 1,
            local_address: "10.1.2.3",
            receive_ports: &ports,
            format: AudioFormat::pcmu_8k_20ms(),
            answer_with: NegotiatedCodec::from_static_format(AudioFormat::pcmu_8k_20ms()).unwrap(),
        };

        assert_eq!(
            NegotiatedCodec::from_static_format(AudioFormat::l16_16k_20ms()),
            Err(SdpError::NoStaticPayloadType(Encoding::L16))
        );

        let stereo = SubscriptionAnswer {
            format: AudioFormat {
                channels: 2,
                ..AudioFormat::pcmu_8k_20ms()
            },
            ..base
        };
        assert_eq!(
            stereo.to_sdp(&offer),
            Err(SdpError::UnsupportedChannelCount(2))
        );

        let zero_ptime = SubscriptionAnswer {
            format: AudioFormat {
                ptime_ms: 0,
                ..AudioFormat::pcmu_8k_20ms()
            },
            ..base
        };
        assert_eq!(zero_ptime.to_sdp(&offer), Err(SdpError::UnusableFormat));
    }

    #[test]
    fn malformed_offers_are_errors_not_panics() {
        assert_eq!(
            SubscriptionOffer::parse("v=0\r\nc=IN IP4 10.0.0.5\r\n"),
            Err(SdpError::NoAudioMedia)
        );
        assert_eq!(
            SubscriptionOffer::parse("m=video 30000 RTP/AVP 96\r\n"),
            Err(SdpError::NonAudioMedia("video".to_string()))
        );
        assert_eq!(
            SubscriptionOffer::parse("m=audio\r\n"),
            Err(SdpError::MalformedMediaLine("audio".to_string()))
        );
        assert_eq!(
            SubscriptionOffer::parse("m=audio 99999 RTP/AVP 0\r\n"),
            Err(SdpError::MalformedPort("audio 99999 RTP/AVP 0".to_string()))
        );
        assert_eq!(
            SubscriptionOffer::parse("m=audio 30000 RTP/AVP xyz\r\n"),
            Err(SdpError::MalformedPayloadType(
                "audio 30000 RTP/AVP xyz".to_string()
            ))
        );
        assert_eq!(
            SubscriptionOffer::parse("m=audio 30000 RTP/AVP\r\n"),
            Err(SdpError::NoPayloadType("audio 30000 RTP/AVP".to_string()))
        );
        assert!(SubscriptionOffer::parse("").is_err());
        assert!(SubscriptionOffer::parse("garbage without equals\r\n").is_err());
    }

    #[test]
    fn tolerates_lf_only_line_endings_and_unknown_attributes() {
        let offer =
            SubscriptionOffer::parse("m=audio 30000 RTP/AVP 0\na=unknown:whatever\na=ptime:30\n")
                .unwrap();
        assert_eq!(offer.streams.len(), 1);
        assert_eq!(offer.streams[0].ptime_ms, Some(30));
    }
}

#[cfg(test)]
mod opus_negotiation_tests {
    use super::tests::TWO_LEG_OFFER;
    use super::*;

    const OPUS_OFFER: &str = "v=0\r\n\
o=- 1 1 IN IP4 10.0.0.5\r\n\
s=rtpengine\r\n\
c=IN IP4 10.0.0.5\r\n\
t=0 0\r\n\
m=audio 30100 RTP/AVP 111 101\r\n\
a=rtpmap:111 opus/48000/2\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=ptime:20\r\n\
a=sendonly\r\n";

    #[test]
    fn opus_is_negotiated_at_the_offers_own_dynamic_payload_type() {
        let offer = SubscriptionOffer::parse(OPUS_OFFER).unwrap();
        let codec = offer.streams[0].negotiate().unwrap();
        assert_eq!(
            codec,
            NegotiatedCodec {
                payload_type: 111,
                encoding: Encoding::Opus,
                clock_rate_hz: OPUS_CLOCK_RATE_HZ,
            }
        );
        assert_eq!(codec.samples_per_packet(20), Some(960));
    }

    #[test]
    fn the_answer_echoes_the_dynamic_payload_type_and_the_rfc_7587_rtpmap() {
        let offer = SubscriptionOffer::parse(OPUS_OFFER).unwrap();
        let ports = [41100u16];
        let sdp = SubscriptionAnswer {
            session_id: 5,
            local_address: "10.0.0.30",
            receive_ports: &ports,
            format: AudioFormat::pcmu_8k_20ms(),
            answer_with: offer.streams[0].negotiate().unwrap(),
        }
        .to_sdp(&offer)
        .unwrap();

        assert!(sdp.contains("m=audio 41100 RTP/AVP 111 101\r\n"), "{sdp}");
        assert!(sdp.contains("a=rtpmap:111 opus/48000/2\r\n"), "{sdp}");
        assert!(
            sdp.contains("a=rtpmap:101 telephone-event/8000\r\n"),
            "{sdp}"
        );
        assert!(!sdp.contains("a=rtpmap:0 "), "{sdp}");
    }

    #[test]
    fn a_g711_answer_keeps_its_two_field_rtpmap() {
        let offer = SubscriptionOffer::parse(TWO_LEG_OFFER).unwrap();
        let ports = [41000u16, 41002];
        let sdp = SubscriptionAnswer {
            session_id: 5,
            local_address: "10.0.0.30",
            receive_ports: &ports,
            format: AudioFormat::pcmu_8k_20ms(),
            answer_with: offer.streams[0].negotiate().unwrap(),
        }
        .to_sdp(&offer)
        .unwrap();
        assert!(sdp.contains("a=rtpmap:0 PCMU/8000\r\n"), "{sdp}");
        assert!(sdp.contains("a=rtpmap:8 PCMA/8000\r\n"), "{sdp}");
        assert!(!sdp.contains("/8000/2"), "{sdp}");
    }

    #[test]
    fn opus_at_a_clock_rate_rfc_7587_forbids_is_refused() {
        let wrong = OPUS_OFFER.replace("opus/48000/2", "opus/16000/2");
        let offer = SubscriptionOffer::parse(&wrong).unwrap();
        assert_eq!(
            offer.streams[0].negotiate(),
            Err(SdpError::OpusClockRate(16000))
        );
    }

    #[test]
    fn a_static_codec_still_wins_when_the_offer_lists_it_first() {
        let both = OPUS_OFFER.replace("RTP/AVP 111 101", "RTP/AVP 8 111 101");
        let both = both.replace(
            "a=rtpmap:111 opus/48000/2",
            "a=rtpmap:8 PCMA/8000\r\na=rtpmap:111 opus/48000/2",
        );
        let offer = SubscriptionOffer::parse(&both).unwrap();
        assert_eq!(
            offer.streams[0].negotiate().unwrap().encoding,
            Encoding::Pcma
        );
    }

    #[test]
    fn a_dynamic_codec_we_do_not_know_is_skipped_not_guessed() {
        let unknown = OPUS_OFFER.replace("a=rtpmap:111 opus/48000/2", "a=rtpmap:111 EVS/16000");
        let offer = SubscriptionOffer::parse(&unknown).unwrap();
        assert_eq!(
            offer.streams[0].negotiate(),
            Err(SdpError::NoDecodableCodecOffered(vec![111, 101]))
        );
    }

    #[test]
    fn an_explicit_answer_codec_overrides_what_the_offer_would_have_chosen() {
        let offer = SubscriptionOffer::parse(TWO_LEG_OFFER).unwrap();
        let ports = [41000u16, 41002];
        let sdp = SubscriptionAnswer {
            session_id: 5,
            local_address: "10.0.0.30",
            receive_ports: &ports,
            format: AudioFormat::pcmu_8k_20ms(),
            answer_with: NegotiatedCodec {
                payload_type: 8,
                encoding: Encoding::Pcma,
                clock_rate_hz: 8000,
            },
        }
        .to_sdp(&offer)
        .unwrap();
        assert!(sdp.contains("m=audio 41000 RTP/AVP 8 0 101\r\n"), "{sdp}");
    }
}
