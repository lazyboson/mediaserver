use crate::frame::{AudioFormat, Encoding};
use opus_ffi::{OpusDecoder, OpusFfiError};

pub use opus_ffi::{MAX_FRAME_MS as MAX_OPUS_FRAME_MS, MAX_PACKET_BYTES as MAX_OPUS_PACKET_BYTES};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum OpusError {
    #[error("{0:?} is not opus")]
    NotOpus(Encoding),
    #[error("{0}")]
    Libopus(#[from] OpusFfiError),
}

pub struct OpusStreamDecoder {
    decoder: OpusDecoder,
}

impl OpusStreamDecoder {
    pub fn new(format: AudioFormat) -> Result<Self, OpusError> {
        if format.encoding != Encoding::Opus {
            return Err(OpusError::NotOpus(format.encoding));
        }
        Ok(OpusStreamDecoder {
            decoder: OpusDecoder::new(format.sample_rate_hz, format.channels)?,
        })
    }

    pub fn max_frame_samples(&self) -> usize {
        self.decoder.max_frame_samples()
    }

    pub fn frame_samples(&self, packet: &[u8]) -> Result<usize, OpusError> {
        Ok(self.decoder.frame_samples(packet)?)
    }

    pub fn decode(&mut self, packet: &[u8], out: &mut [i16]) -> Result<usize, OpusError> {
        Ok(self.decoder.decode(packet, out)?)
    }

    pub fn conceal(&mut self, out: &mut [i16]) -> Result<usize, OpusError> {
        Ok(self.decoder.conceal(out)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opus_at(sample_rate_hz: u32) -> AudioFormat {
        AudioFormat {
            encoding: Encoding::Opus,
            sample_rate_hz,
            channels: 1,
            ptime_ms: 20,
        }
    }

    #[test]
    fn the_tap_vocabulary_maps_onto_libopus() {
        let decoder = OpusStreamDecoder::new(opus_at(48000)).unwrap();
        assert_eq!(decoder.max_frame_samples(), 2880);
        assert_eq!(
            OpusStreamDecoder::new(opus_at(16000))
                .unwrap()
                .max_frame_samples(),
            960
        );
    }

    #[test]
    fn a_format_that_is_not_opus_is_refused_before_libopus_is_asked() {
        assert_eq!(
            OpusStreamDecoder::new(AudioFormat::pcmu_8k_20ms()).err(),
            Some(OpusError::NotOpus(Encoding::Pcmu))
        );
        assert_eq!(
            OpusStreamDecoder::new(AudioFormat::l16_16k_20ms()).err(),
            Some(OpusError::NotOpus(Encoding::L16))
        );
    }

    #[test]
    fn libopus_refusals_reach_the_caller_intact() {
        assert!(matches!(
            OpusStreamDecoder::new(opus_at(44100)),
            Err(OpusError::Libopus(
                opus_ffi::OpusFfiError::UnsupportedSampleRate(44100)
            ))
        ));
        assert!(matches!(
            OpusStreamDecoder::new(AudioFormat {
                channels: 2,
                ..opus_at(48000)
            }),
            Err(OpusError::Libopus(
                opus_ffi::OpusFfiError::UnsupportedChannelCount(2)
            ))
        ));
    }
}
