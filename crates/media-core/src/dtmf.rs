//! RFC 2833 / RFC 4733 telephone-event handling.
//!
//! The tapped RTP carries DTMF as telephone-events on a dynamic payload
//! type (negotiated in SDP; 101 is the conventional default). We report a
//! digit once, on its end-of-event packet, deduplicated by (digit, RTP
//! timestamp) — end packets are retransmitted 3x for robustness.

/// A decoded telephone-event payload.
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

/// Decode a telephone-event payload (the 4-byte body defined by RFC 4733).
pub fn decode(payload: &[u8]) -> Option<TelephoneEvent> {
    if payload.len() < 4 {
        return None;
    }
    let event = payload[0] as usize;
    if event >= DIGIT_MAP.len() {
        return None; // flash-hook and other events are out of scope
    }
    Some(TelephoneEvent {
        digit: DIGIT_MAP[event],
        end: payload[1] & 0x80 != 0,
        volume: payload[1] & 0x3F,
        duration: u16::from_be_bytes([payload[2], payload[3]]),
    })
}

/// Stateful per-stream DTMF reporter: emits each digit exactly once.
#[derive(Debug, Default)]
pub struct DtmfDetector {
    last_reported: Option<(char, u32)>,
}

impl DtmfDetector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed a telephone-event RTP payload with its RTP timestamp.
    /// Returns `Some(digit)` exactly once per key press.
    pub fn push(&mut self, rtp_timestamp: u32, payload: &[u8]) -> Option<char> {
        let ev = decode(payload)?;
        if !ev.end {
            return None;
        }
        if self.last_reported == Some((ev.digit, rtp_timestamp)) {
            return None; // retransmitted end packet
        }
        self.last_reported = Some((ev.digit, rtp_timestamp));
        Some(ev.digit)
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
        let mut det = DtmfDetector::new();
        assert_eq!(det.push(1000, &[5, 0x0A, 0, 80]), None); // start, not end
        assert_eq!(det.push(1000, &end_packet(5)), Some('5'));
        assert_eq!(det.push(1000, &end_packet(5)), None); // retransmit
        assert_eq!(det.push(1000, &end_packet(5)), None); // retransmit
        assert_eq!(det.push(2600, &end_packet(5)), Some('5')); // new press
    }

    #[test]
    fn ignores_short_and_unknown_events() {
        let mut det = DtmfDetector::new();
        assert_eq!(det.push(1, &[5, 0x80]), None);
        assert_eq!(det.push(1, &end_packet(16)), None); // flash-hook
    }
}
