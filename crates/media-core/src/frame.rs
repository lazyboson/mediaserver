//! Audio frame and format types shared across the pipeline.

/// Audio encodings the MSS understands natively.
///
/// G.711 and L16 are implemented in-tree; Opus will bind libopus via
/// `audiopus` when the transcode stage lands (see docs/architecture.md §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// G.711 µ-law, 8 kHz (RTP payload type 0).
    Pcmu,
    /// G.711 A-law, 8 kHz (RTP payload type 8).
    Pcma,
    /// Linear PCM, signed 16-bit little-endian host frames. The internal
    /// interchange format: everything decodes to L16 before fan-out.
    L16,
    /// Opus (dynamic payload type from SDP rtpmap).
    Opus,
}

impl Encoding {
    /// Static RTP payload type, if the codec has one.
    pub fn static_payload_type(self) -> Option<u8> {
        match self {
            Encoding::Pcmu => Some(0),
            Encoding::Pcma => Some(8),
            _ => None,
        }
    }
}

/// Negotiated audio format for a stream or consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AudioFormat {
    pub encoding: Encoding,
    pub sample_rate_hz: u32,
    pub channels: u8,
    /// Packetization time in milliseconds. Always derived from SDP/consumer
    /// negotiation — never assumed to be 20 (mediagateway lesson #2).
    pub ptime_ms: u32,
}

impl AudioFormat {
    pub const fn pcmu_8k_20ms() -> Self {
        Self {
            encoding: Encoding::Pcmu,
            sample_rate_hz: 8000,
            channels: 1,
            ptime_ms: 20,
        }
    }

    pub const fn l16_16k_20ms() -> Self {
        Self {
            encoding: Encoding::L16,
            sample_rate_hz: 16000,
            channels: 1,
            ptime_ms: 20,
        }
    }

    /// Samples per channel per packet at this format.
    ///
    /// Returns `None` instead of dividing by zero on degenerate input
    /// (mediagateway crashed on `a=ptime:0`; we refuse the format instead).
    pub fn samples_per_packet(&self) -> Option<u32> {
        if self.ptime_ms == 0 || self.sample_rate_hz == 0 {
            return None;
        }
        Some(self.sample_rate_hz / 1000 * self.ptime_ms)
    }

    /// RTP timestamp increment per packet (equals samples per packet for
    /// the audio codecs we carry).
    pub fn timestamp_increment(&self) -> Option<u32> {
        self.samples_per_packet()
    }
}

/// Which side of the call a stream belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Track {
    /// Caller / customer leg (tapped at the interconnect RTPEngine).
    Customer,
    /// Agent leg (tapped at the agent-side RTPEngine).
    Agent,
    /// Mixed feed (rtpengine `subscribe` with the `mix` flag, or MSS mixer output).
    Mixed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_per_packet_basics() {
        assert_eq!(AudioFormat::pcmu_8k_20ms().samples_per_packet(), Some(160));
        assert_eq!(AudioFormat::l16_16k_20ms().samples_per_packet(), Some(320));
    }

    #[test]
    fn zero_ptime_is_rejected_not_a_panic() {
        let f = AudioFormat {
            ptime_ms: 0,
            ..AudioFormat::pcmu_8k_20ms()
        };
        assert_eq!(f.samples_per_packet(), None);
    }
}
