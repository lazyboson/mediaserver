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
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OfferedStream {
    pub port: u16,
    pub payload_types: Vec<u8>,
    pub ptime_ms: Option<u32>,
    pub label: Option<String>,
    pub connection_address: Option<String>,
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
        ptime_ms: None,
        label: None,
        connection_address: None,
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
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SubscriptionAnswer<'a> {
    pub session_id: u64,
    pub local_address: &'a str,
    pub receive_ports: &'a [u16],
    pub format: AudioFormat,
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
        let payload_type = self
            .format
            .encoding
            .static_payload_type()
            .ok_or(SdpError::NoStaticPayloadType(self.format.encoding))?;

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
        for port in self.receive_ports {
            sdp.push_str(&format!("m=audio {port} RTP/AVP {payload_type}\r\n"));
            sdp.push_str(&format!(
                "a=rtpmap:{payload_type} {name}/{rate}\r\n",
                name = self.format.encoding.rtpmap_name(),
                rate = self.format.sample_rate_hz
            ));
            sdp.push_str(&format!("a=ptime:{}\r\n", self.format.ptime_ms));
            sdp.push_str("a=recvonly\r\n");
        }
        Ok(sdp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TWO_LEG_OFFER: &str = "v=0\r\n\
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
        };
        assert_eq!(
            answer.to_sdp(&offer).unwrap(),
            "v=0\r\n\
o=- 42 42 IN IP4 10.1.2.3\r\n\
s=mss-tap\r\n\
c=IN IP4 10.1.2.3\r\n\
t=0 0\r\n\
m=audio 40000 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=ptime:20\r\n\
a=recvonly\r\n\
m=audio 40002 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=ptime:20\r\n\
a=recvonly\r\n"
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
        };

        let l16 = SubscriptionAnswer {
            format: AudioFormat::l16_16k_20ms(),
            ..base
        };
        assert_eq!(
            l16.to_sdp(&offer),
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
