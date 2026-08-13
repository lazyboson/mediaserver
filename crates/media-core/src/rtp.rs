//! Minimal, allocation-free RTP header parsing and serialization (RFC 3550).
//!
//! We parse only what the MSS needs. Extension headers and CSRCs are
//! skipped, padding is honored. Serialization writes the header the MSS
//! emits (no CSRC, no extension) but *refuses* to silently drop flags the
//! caller set (mediagateway encoded a bare 12-byte header regardless of
//! struct contents).

use thiserror::Error;

pub const RTP_VERSION: u8 = 2;
pub const MIN_HEADER_LEN: usize = 12;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RtpError {
    #[error("packet too short: {0} bytes")]
    TooShort(usize),
    #[error("unsupported RTP version {0}")]
    BadVersion(u8),
    #[error("declared padding/extension exceeds packet length")]
    Truncated,
    #[error("serialization does not support CSRC or extension headers yet")]
    Unsupported,
    #[error("output buffer too small")]
    BufferTooSmall,
}

/// Parsed view of an RTP packet. `payload` borrows from the input buffer —
/// zero copies on the receive path.
#[derive(Debug, PartialEq, Eq)]
pub struct RtpPacket<'a> {
    pub marker: bool,
    pub payload_type: u8,
    pub sequence: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    pub payload: &'a [u8],
}

impl<'a> RtpPacket<'a> {
    pub fn parse(buf: &'a [u8]) -> Result<Self, RtpError> {
        if buf.len() < MIN_HEADER_LEN {
            return Err(RtpError::TooShort(buf.len()));
        }
        let version = buf[0] >> 6;
        if version != RTP_VERSION {
            return Err(RtpError::BadVersion(version));
        }
        let has_padding = buf[0] & 0x20 != 0;
        let has_extension = buf[0] & 0x10 != 0;
        let csrc_count = (buf[0] & 0x0F) as usize;

        let mut offset = MIN_HEADER_LEN + csrc_count * 4;
        if buf.len() < offset {
            return Err(RtpError::Truncated);
        }
        if has_extension {
            if buf.len() < offset + 4 {
                return Err(RtpError::Truncated);
            }
            let ext_words = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]) as usize;
            offset += 4 + ext_words * 4;
            if buf.len() < offset {
                return Err(RtpError::Truncated);
            }
        }
        let mut end = buf.len();
        if has_padding {
            let pad = *buf.last().unwrap_or(&0) as usize;
            if pad == 0 || offset + pad > end {
                return Err(RtpError::Truncated);
            }
            end -= pad;
        }

        Ok(RtpPacket {
            marker: buf[1] & 0x80 != 0,
            payload_type: buf[1] & 0x7F,
            sequence: u16::from_be_bytes([buf[2], buf[3]]),
            timestamp: u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]),
            ssrc: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
            payload: &buf[offset..end],
        })
    }

    /// Serialize into `out`, returning the number of bytes written.
    pub fn serialize(&self, out: &mut [u8]) -> Result<usize, RtpError> {
        let total = MIN_HEADER_LEN + self.payload.len();
        if out.len() < total {
            return Err(RtpError::BufferTooSmall);
        }
        out[0] = RTP_VERSION << 6;
        out[1] = (self.payload_type & 0x7F) | if self.marker { 0x80 } else { 0 };
        out[2..4].copy_from_slice(&self.sequence.to_be_bytes());
        out[4..8].copy_from_slice(&self.timestamp.to_be_bytes());
        out[8..12].copy_from_slice(&self.ssrc.to_be_bytes());
        out[12..total].copy_from_slice(self.payload);
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<u8> {
        let pkt = RtpPacket {
            marker: true,
            payload_type: 0,
            sequence: 42,
            timestamp: 160,
            ssrc: 0xDEADBEEF,
            payload: &[0xFF; 160],
        };
        let mut buf = vec![0u8; 12 + 160];
        let n = pkt.serialize(&mut buf).unwrap();
        buf.truncate(n);
        buf
    }

    #[test]
    fn roundtrip() {
        let wire = sample();
        let pkt = RtpPacket::parse(&wire).unwrap();
        assert!(pkt.marker);
        assert_eq!(pkt.payload_type, 0);
        assert_eq!(pkt.sequence, 42);
        assert_eq!(pkt.timestamp, 160);
        assert_eq!(pkt.ssrc, 0xDEADBEEF);
        assert_eq!(pkt.payload.len(), 160);
    }

    #[test]
    fn rejects_short_and_bad_version() {
        assert_eq!(RtpPacket::parse(&[0u8; 4]), Err(RtpError::TooShort(4)));
        let mut wire = sample();
        wire[0] = 0x40; // version 1
        assert_eq!(RtpPacket::parse(&wire), Err(RtpError::BadVersion(1)));
    }

    #[test]
    fn skips_csrc_and_extension() {
        // 1 CSRC + extension header with 1 word, then 2 payload bytes.
        let mut wire = vec![
            0x91, 0x60, 0x00, 0x01, // v=2, ext, cc=1 | pt=96 | seq=1
            0x00, 0x00, 0x00, 0xA0, // ts
            0x00, 0x00, 0x00, 0x01, // ssrc
            0x11, 0x22, 0x33, 0x44, // csrc[0]
            0xBE, 0xDE, 0x00, 0x01, // ext profile + len=1 word
            0xAA, 0xBB, 0xCC, 0xDD, // ext word
        ];
        wire.extend_from_slice(&[0x01, 0x02]);
        let pkt = RtpPacket::parse(&wire).unwrap();
        assert_eq!(pkt.payload, &[0x01, 0x02]);
    }

    #[test]
    fn honors_padding() {
        let mut wire = sample();
        wire[0] |= 0x20; // padding flag
        wire.extend_from_slice(&[0, 0, 3]); // 3 bytes of padding incl. count
        let pkt = RtpPacket::parse(&wire).unwrap();
        assert_eq!(pkt.payload.len(), 160);
    }

    #[test]
    fn malformed_padding_is_error_not_panic() {
        let mut wire = sample();
        wire[0] |= 0x20;
        let last = wire.len() - 1;
        wire[last] = 0xFF; // pad length larger than packet
        assert_eq!(RtpPacket::parse(&wire), Err(RtpError::Truncated));
    }
}
