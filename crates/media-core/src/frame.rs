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
    fn zero_ptime_is_rejected_not_a_panic() {
        let f = AudioFormat {
            ptime_ms: 0,
            ..AudioFormat::pcmu_8k_20ms()
        };
        assert_eq!(f.samples_per_packet(), None);
    }
}
