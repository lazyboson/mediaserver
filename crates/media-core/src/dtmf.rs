#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelephoneEvent {
    pub digit: char,
    pub end: bool,
    pub volume: u8,
    pub duration: u16,
}

const DIGIT_MAP: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '*', '#', 'A', 'B', 'C', 'D',
];

pub fn decode(payload: &[u8]) -> Option<TelephoneEvent> {
    if payload.len() < 4 {
        return None;
    }
    let event = payload[0] as usize;
    if event >= DIGIT_MAP.len() {
        return None;
    }
    Some(TelephoneEvent {
        digit: DIGIT_MAP[event],
        end: payload[1] & 0x80 != 0,
        volume: payload[1] & 0x3F,
        duration: u16::from_be_bytes([payload[2], payload[3]]),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DigitPress {
    pub digit: char,
    pub duration_ms: u32,
    pub rtp_timestamp: u32,
}

const MILLIS_PER_SECOND: u32 = 1000;

#[derive(Debug)]
pub struct DtmfDetector {
    clock_rate_hz: u32,
    last_reported: Option<(char, u32)>,
}

impl DtmfDetector {
    pub fn new(clock_rate_hz: u32) -> Self {
        DtmfDetector {
            clock_rate_hz,
            last_reported: None,
        }
    }

    pub fn push(&mut self, rtp_timestamp: u32, payload: &[u8]) -> Option<DigitPress> {
        let ev = decode(payload)?;
        if !ev.end {
            return None;
        }
        if self.last_reported == Some((ev.digit, rtp_timestamp)) {
            return None;
        }
        self.last_reported = Some((ev.digit, rtp_timestamp));
        Some(DigitPress {
            digit: ev.digit,
            duration_ms: self.duration_ms(ev.duration),
            rtp_timestamp,
        })
    }

    fn duration_ms(&self, ticks: u16) -> u32 {
        if self.clock_rate_hz == 0 {
            return 0;
        }
        u32::from(ticks)
            .saturating_mul(MILLIS_PER_SECOND)
            .wrapping_div(self.clock_rate_hz)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn end_packet(event: u8) -> [u8; 4] {
        [event, 0x8A, 0x03, 0x20]
    }

    #[test]
    fn decodes_digits_and_end_bit() {
        let ev = decode(&end_packet(11)).unwrap();
        assert_eq!(ev.digit, '#');
        assert!(ev.end);
        assert_eq!(ev.volume, 0x0A);
        assert_eq!(ev.duration, 0x0320);
    }

    #[test]
    fn reports_once_despite_retransmitted_end() {
        let mut det = DtmfDetector::new(8000);
        assert_eq!(det.push(1000, &[5, 0x0A, 0, 80]), None);
        let press = det.push(1000, &end_packet(5)).unwrap();
        assert_eq!(press.digit, '5');
        assert_eq!(det.push(1000, &end_packet(5)), None);
        assert_eq!(det.push(1000, &end_packet(5)), None);
        assert_eq!(det.push(2600, &end_packet(5)).map(|p| p.digit), Some('5'));
    }

    #[test]
    fn a_press_carries_its_duration_in_milliseconds_and_its_rtp_timestamp() {
        let mut det = DtmfDetector::new(8000);
        let press = det.push(41000, &end_packet(7)).unwrap();
        assert_eq!(press.digit, '7');
        assert_eq!(press.rtp_timestamp, 41000);
        assert_eq!(press.duration_ms, 100);

        let mut wideband = DtmfDetector::new(48000);
        let press = wideband.push(9, &end_packet(7)).unwrap();
        assert_eq!(press.duration_ms, 16);

        let mut unclocked = DtmfDetector::new(0);
        assert_eq!(unclocked.push(9, &end_packet(7)).unwrap().duration_ms, 0);
    }

    #[test]
    fn ignores_short_and_unknown_events() {
        let mut det = DtmfDetector::new(8000);
        assert_eq!(det.push(1, &[5, 0x80]), None);
        assert_eq!(det.push(1, &end_packet(16)), None);
    }
}
