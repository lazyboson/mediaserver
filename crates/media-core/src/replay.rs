use crate::frame::AudioFormat;
use crate::rtp::{RtpPacket, MIN_HEADER_LEN};
use thiserror::Error;

const LENGTH_PREFIX: usize = 4;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReplayError {
    #[error("datagram log truncated at byte {0}")]
    Truncated(usize),
    #[error("datagram at byte {at} claims {length} bytes but only {remaining} remain")]
    LengthOverflow {
        at: usize,
        length: usize,
        remaining: usize,
    },
    #[error("format has no static payload type or no usable ptime/sample rate")]
    UnusableFormat,
}

pub struct DatagramLog<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> DatagramLog<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        DatagramLog { bytes, cursor: 0 }
    }
}

impl<'a> Iterator for DatagramLog<'a> {
    type Item = Result<&'a [u8], ReplayError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor == self.bytes.len() {
            return None;
        }
        let at = self.cursor;
        let Some(prefix) = self.bytes.get(at..at + LENGTH_PREFIX) else {
            self.cursor = self.bytes.len();
            return Some(Err(ReplayError::Truncated(at)));
        };
        let length = u32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize;
        let start = at + LENGTH_PREFIX;
        let remaining = self.bytes.len() - start;
        if length > remaining {
            self.cursor = self.bytes.len();
            return Some(Err(ReplayError::LengthOverflow {
                at,
                length,
                remaining,
            }));
        }
        self.cursor = start + length;
        Some(Ok(&self.bytes[start..start + length]))
    }
}

pub fn encode_datagram_log<'a, I>(datagrams: I) -> Vec<u8>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut out = Vec::new();
    for datagram in datagrams {
        out.extend_from_slice(&(datagram.len() as u32).to_be_bytes());
        out.extend_from_slice(datagram);
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disturbance {
    Deliver,
    Drop,
    DeliverTwice,
    DelayOne,
}

pub fn disturb(datagrams: Vec<Vec<u8>>, script: &[Disturbance]) -> Vec<Vec<u8>> {
    let mut wire: Vec<Vec<u8>> = Vec::with_capacity(datagrams.len());
    let mut delayed: Option<Vec<u8>> = None;
    for (index, datagram) in datagrams.into_iter().enumerate() {
        match script.get(index).copied().unwrap_or(Disturbance::Deliver) {
            Disturbance::Deliver => wire.push(datagram),
            Disturbance::Drop => {}
            Disturbance::DeliverTwice => {
                wire.push(datagram.clone());
                wire.push(datagram);
            }
            Disturbance::DelayOne => {
                if let Some(previous) = delayed.replace(datagram) {
                    wire.push(previous);
                }
                continue;
            }
        }
        if let Some(previous) = delayed.take() {
            wire.push(previous);
        }
    }
    wire.extend(delayed);
    wire
}

pub const COMFORT_NOISE_PAYLOAD_TYPE: u8 = 13;

pub struct G711StreamGenerator {
    payload_type: u8,
    ssrc: u32,
    sequence: u16,
    timestamp: u32,
    samples_per_packet: u32,
    next_payload_byte: u8,
    mark_next: bool,
}

impl G711StreamGenerator {
    pub fn new(format: AudioFormat, ssrc: u32, first_sequence: u16) -> Result<Self, ReplayError> {
        let payload_type = format
            .encoding
            .static_payload_type()
            .ok_or(ReplayError::UnusableFormat)?;
        let samples_per_packet = format
            .samples_per_packet()
            .filter(|samples| *samples > 0)
            .ok_or(ReplayError::UnusableFormat)?;
        Ok(G711StreamGenerator {
            payload_type,
            ssrc,
            sequence: first_sequence,
            timestamp: 0,
            samples_per_packet,
            next_payload_byte: 0,
            mark_next: false,
        })
    }

    pub fn sequence(&self) -> u16 {
        self.sequence
    }

    pub fn timestamp(&self) -> u32 {
        self.timestamp
    }

    pub fn next_datagram(&mut self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(self.samples_per_packet as usize);
        for _ in 0..self.samples_per_packet {
            payload.push(self.next_payload_byte);
            self.next_payload_byte = self.next_payload_byte.wrapping_add(1);
        }
        let datagram = self.serialize(self.payload_type, &payload);
        self.mark_next = false;
        self.timestamp = self.timestamp.wrapping_add(self.samples_per_packet);
        self.sequence = self.sequence.wrapping_add(1);
        datagram
    }

    pub fn next_event_datagram(&mut self, payload_type: u8, payload: [u8; 4]) -> Vec<u8> {
        let datagram = self.serialize(payload_type, &payload);
        self.sequence = self.sequence.wrapping_add(1);
        datagram
    }

    pub fn next_comfort_noise_datagram(&mut self, level: u8) -> Vec<u8> {
        let datagram = self.serialize(COMFORT_NOISE_PAYLOAD_TYPE, &[level]);
        self.timestamp = self.timestamp.wrapping_add(self.samples_per_packet);
        self.sequence = self.sequence.wrapping_add(1);
        datagram
    }

    pub fn suppress_silence(&mut self, packets: u32) {
        let samples = self.samples_per_packet.wrapping_mul(packets);
        self.timestamp = self.timestamp.wrapping_add(samples);
        self.next_payload_byte = self.next_payload_byte.wrapping_add(samples as u8);
        self.mark_next = true;
    }

    pub fn skip_one(&mut self) {
        self.timestamp = self.timestamp.wrapping_add(self.samples_per_packet);
        self.sequence = self.sequence.wrapping_add(1);
        self.next_payload_byte = self
            .next_payload_byte
            .wrapping_add(self.samples_per_packet as u8);
    }

    fn serialize(&self, payload_type: u8, payload: &[u8]) -> Vec<u8> {
        let packet = RtpPacket {
            marker: self.mark_next,
            payload_type,
            sequence: self.sequence,
            timestamp: self.timestamp,
            ssrc: self.ssrc,
            payload,
        };
        let mut datagram = vec![0u8; MIN_HEADER_LEN + payload.len()];
        match packet.serialize(&mut datagram) {
            Ok(written) => datagram.truncate(written),
            Err(_) => datagram.clear(),
        }
        datagram
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generator() -> G711StreamGenerator {
        G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 1, 7).unwrap()
    }

    #[test]
    fn generated_stream_advances_sequence_and_timestamp_by_packet() {
        let mut generator = generator();
        let first = generator.next_datagram();
        let second = generator.next_datagram();
        let first = RtpPacket::parse(&first).unwrap();
        let second = RtpPacket::parse(&second).unwrap();
        assert_eq!(first.sequence, 7);
        assert_eq!(second.sequence, 8);
        assert_eq!(first.timestamp, 0);
        assert_eq!(second.timestamp, 160);
        assert_eq!(first.payload.len(), 160);
        assert_eq!(first.payload[0], 0);
        assert_eq!(second.payload[0], 160);
        assert_eq!(first.payload_type, 0);
    }

    #[test]
    fn skip_one_leaves_a_sequence_gap_the_buffer_can_detect() {
        let mut generator = generator();
        let before = generator.next_datagram();
        generator.skip_one();
        let after = generator.next_datagram();
        assert_eq!(RtpPacket::parse(&before).unwrap().sequence, 7);
        assert_eq!(RtpPacket::parse(&after).unwrap().sequence, 9);
    }

    #[test]
    fn datagram_log_roundtrips() {
        let mut generator = generator();
        let datagrams = [generator.next_datagram(), generator.next_datagram()];
        let encoded = encode_datagram_log(datagrams.iter().map(|d| d.as_slice()));
        let decoded: Vec<&[u8]> = DatagramLog::new(&encoded)
            .map(|entry| entry.unwrap())
            .collect();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], datagrams[0].as_slice());
        assert_eq!(decoded[1], datagrams[1].as_slice());
    }

    #[test]
    fn malformed_logs_are_errors_not_panics() {
        assert_eq!(DatagramLog::new(&[]).count(), 0);
        let truncated_prefix = [0u8, 0, 1];
        assert_eq!(
            DatagramLog::new(&truncated_prefix).next(),
            Some(Err(ReplayError::Truncated(0)))
        );
        let overflowing = [0u8, 0, 0, 200, 1, 2, 3];
        assert_eq!(
            DatagramLog::new(&overflowing).next(),
            Some(Err(ReplayError::LengthOverflow {
                at: 0,
                length: 200,
                remaining: 3
            }))
        );
        assert_eq!(DatagramLog::new(&overflowing).count(), 1);
    }

    #[test]
    fn disturb_drops_duplicates_and_reorders() {
        let datagrams: Vec<Vec<u8>> = (0..4u8).map(|tag| vec![tag]).collect();

        assert_eq!(
            disturb(datagrams.clone(), &[Disturbance::Drop]),
            vec![vec![1u8], vec![2], vec![3]]
        );
        assert_eq!(
            disturb(datagrams.clone(), &[Disturbance::DeliverTwice]),
            vec![vec![0u8], vec![0], vec![1], vec![2], vec![3]]
        );
        assert_eq!(
            disturb(datagrams.clone(), &[Disturbance::DelayOne]),
            vec![vec![1u8], vec![0], vec![2], vec![3]]
        );
        assert_eq!(disturb(datagrams.clone(), &[]), datagrams);
    }
}
