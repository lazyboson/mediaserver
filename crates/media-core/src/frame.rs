#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    Pcmu,
    Pcma,
    L16,
    Opus,
}

impl Encoding {
    pub fn static_payload_type(self) -> Option<u8> {
        match self {
            Encoding::Pcmu => Some(0),
            Encoding::Pcma => Some(8),
            _ => None,
        }
    }

    pub fn from_static_payload_type(payload_type: u8) -> Option<Encoding> {
        match payload_type {
            0 => Some(Encoding::Pcmu),
            8 => Some(Encoding::Pcma),
            _ => None,
        }
    }

    pub fn static_clock_rate_hz(self) -> Option<u32> {
        match self {
            Encoding::Pcmu | Encoding::Pcma => Some(8000),
            _ => None,
        }
    }

    pub fn rtpmap_name(self) -> &'static str {
        match self {
            Encoding::Pcmu => "PCMU",
            Encoding::Pcma => "PCMA",
            Encoding::L16 => "L16",
            Encoding::Opus => "opus",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AudioFormat {
    pub encoding: Encoding,
    pub sample_rate_hz: u32,
    pub channels: u8,
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

    pub fn samples_per_packet(&self) -> Option<u32> {
        if self.ptime_ms == 0 || self.sample_rate_hz == 0 {
            return None;
        }
        Some(self.sample_rate_hz / 1000 * self.ptime_ms)
    }

    pub fn timestamp_increment(&self) -> Option<u32> {
        self.samples_per_packet()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Track {
    Customer,
    Agent,
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
    fn rtpmap_names_match_iana_registrations() {
        assert_eq!(Encoding::Pcmu.rtpmap_name(), "PCMU");
        assert_eq!(Encoding::Pcma.rtpmap_name(), "PCMA");
        assert_eq!(Encoding::L16.rtpmap_name(), "L16");
        assert_eq!(Encoding::Opus.rtpmap_name(), "opus");
    }

    #[test]
    fn only_g711_has_static_payload_types() {
        assert_eq!(Encoding::Pcmu.static_payload_type(), Some(0));
        assert_eq!(Encoding::Pcma.static_payload_type(), Some(8));
        assert_eq!(Encoding::L16.static_payload_type(), None);
        assert_eq!(Encoding::Opus.static_payload_type(), None);
    }

    #[test]
    fn the_static_payload_type_map_round_trips_both_ways() {
        for encoding in [Encoding::Pcmu, Encoding::Pcma] {
            let payload_type = encoding.static_payload_type().unwrap();
            assert_eq!(
                Encoding::from_static_payload_type(payload_type),
                Some(encoding)
            );
            assert_eq!(encoding.static_clock_rate_hz(), Some(8000));
        }
    }

    #[test]
    fn a_payload_type_we_cannot_decode_names_itself_by_returning_none() {
        for payload_type in [9u8, 11, 13, 96, 101, 111] {
            assert_eq!(Encoding::from_static_payload_type(payload_type), None);
        }
        assert_eq!(Encoding::L16.static_clock_rate_hz(), None);
        assert_eq!(Encoding::Opus.static_clock_rate_hz(), None);
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
