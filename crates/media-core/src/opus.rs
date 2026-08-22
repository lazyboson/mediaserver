use crate::frame::{AudioFormat, Encoding};
use opus_rs::OpusDecoder;
use thiserror::Error;

pub const MAX_OPUS_PACKET_BYTES: usize = 1276;
pub const MAX_OPUS_FRAME_MS: u32 = 60;

const LEGAL_SAMPLE_RATES_HZ: [u32; 5] = [8000, 12000, 16000, 24000, 48000];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OpusError {
    #[error("{0:?} is not opus")]
    NotOpus(Encoding),
    #[error("opus decodes to 8/12/16/24/48 kHz; {0} Hz is not one of them")]
    UnsupportedSampleRate(u32),
    #[error("a tap decodes opus as mono; {0} channels were requested")]
    UnsupportedChannelCount(u8),
    #[error("the decoder refused this format: {0}")]
    Refused(&'static str),
    #[error("opus rejected the packet: {0}")]
    RejectedPacket(&'static str),
    #[error("a {samples}-sample frame does not fit the caller's {capacity}-sample buffer")]
    FrameTooLong { samples: usize, capacity: usize },
}

pub struct OpusStreamDecoder {
    decoder: OpusDecoder,
    scratch: Vec<f32>,
}

impl OpusStreamDecoder {
    pub fn new(format: AudioFormat) -> Result<Self, OpusError> {
        if format.encoding != Encoding::Opus {
            return Err(OpusError::NotOpus(format.encoding));
        }
        if format.channels != 1 {
            return Err(OpusError::UnsupportedChannelCount(format.channels));
        }
        if !LEGAL_SAMPLE_RATES_HZ.contains(&format.sample_rate_hz) {
            return Err(OpusError::UnsupportedSampleRate(format.sample_rate_hz));
        }
        let decoder =
            OpusDecoder::new(format.sample_rate_hz as i32, 1).map_err(OpusError::Refused)?;
        let longest_frame = (format.sample_rate_hz / 1000 * MAX_OPUS_FRAME_MS) as usize;
        Ok(OpusStreamDecoder {
            decoder,
            scratch: vec![0.0; longest_frame],
        })
    }

    pub fn max_frame_samples(&self) -> usize {
        self.scratch.len()
    }

    pub fn decode(&mut self, packet: &[u8], out: &mut [i16]) -> Result<usize, OpusError> {
        let capacity = self.scratch.len();
        let samples = self
            .decoder
            .decode(packet, capacity, &mut self.scratch)
            .map_err(OpusError::RejectedPacket)?;
        if samples > out.len() {
            return Err(OpusError::FrameTooLong {
                samples,
                capacity: out.len(),
            });
        }
        for (slot, value) in out.iter_mut().zip(self.scratch.iter().take(samples)) {
            *slot = (value * 32768.0).round().clamp(-32768.0, 32767.0) as i16;
        }
        Ok(samples)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opus_rs::{Application, OpusEncoder};

    fn opus_at(sample_rate_hz: u32) -> AudioFormat {
        AudioFormat {
            encoding: Encoding::Opus,
            sample_rate_hz,
            channels: 1,
            ptime_ms: 20,
        }
    }

    fn tone(sample_rate_hz: u32, samples: usize, hz: f32) -> Vec<f32> {
        (0..samples)
            .map(|n| {
                let t = n as f32 / sample_rate_hz as f32;
                (t * hz * 2.0 * std::f32::consts::PI).sin() * 0.4
            })
            .collect()
    }

    fn encode_one(sample_rate_hz: u32, pcm: &[f32]) -> Vec<u8> {
        let mut encoder = OpusEncoder::new(sample_rate_hz as i32, 1, Application::Voip).unwrap();
        encoder.bitrate_bps = 24000;
        encoder.use_cbr = true;
        let mut packet = vec![0u8; MAX_OPUS_PACKET_BYTES];
        let bytes = encoder.encode(pcm, pcm.len(), &mut packet).unwrap();
        packet.truncate(bytes);
        packet
    }

    fn rms(samples: &[i16]) -> f64 {
        let sum: f64 = samples.iter().map(|s| f64::from(*s) * f64::from(*s)).sum();
        (sum / samples.len().max(1) as f64).sqrt()
    }

    #[test]
    fn a_tone_survives_an_encode_decode_round_trip_at_every_supported_rate() {
        for rate in LEGAL_SAMPLE_RATES_HZ {
            let frame = (rate / 1000 * 20) as usize;
            let mut decoder = OpusStreamDecoder::new(opus_at(rate)).unwrap();
            let mut out = vec![0i16; decoder.max_frame_samples()];

            let mut decoded_rms = 0.0;
            for step in 0..10 {
                let input = tone(rate, frame, 440.0);
                let packet = encode_one(rate, &input);
                assert!(packet.len() <= MAX_OPUS_PACKET_BYTES);
                let samples = decoder.decode(&packet, &mut out).unwrap();
                assert_eq!(samples, frame, "rate {rate} step {step}");
                decoded_rms = rms(&out[..samples]);
            }
            assert!(
                decoded_rms > 2000.0,
                "rate {rate} decoded to rms {decoded_rms}, expected a tone"
            );
        }
    }

    #[test]
    fn silence_decodes_to_something_quiet_rather_than_noise() {
        let rate = 16000;
        let frame = 320;
        let mut decoder = OpusStreamDecoder::new(opus_at(rate)).unwrap();
        let mut out = vec![0i16; decoder.max_frame_samples()];
        let packet = encode_one(rate, &vec![0.0f32; frame]);
        let samples = decoder.decode(&packet, &mut out).unwrap();
        assert_eq!(samples, frame);
        assert!(rms(&out[..samples]) < 50.0);
    }

    #[test]
    fn a_malformed_packet_is_an_error_not_a_panic() {
        let mut decoder = OpusStreamDecoder::new(opus_at(16000)).unwrap();
        let mut out = vec![0i16; decoder.max_frame_samples()];

        assert!(matches!(
            decoder.decode(&[], &mut out),
            Err(OpusError::RejectedPacket(_))
        ));
        for junk in [
            vec![0xFFu8; 8],
            vec![0x00u8; 200],
            (0..64u8).collect::<Vec<u8>>(),
        ] {
            let outcome = decoder.decode(&junk, &mut out);
            assert!(
                outcome.is_ok() || matches!(outcome, Err(OpusError::RejectedPacket(_))),
                "junk packet produced {outcome:?}"
            );
        }
    }

    #[test]
    fn a_buffer_too_small_for_the_frame_is_refused_by_name() {
        let rate = 48000;
        let mut decoder = OpusStreamDecoder::new(opus_at(rate)).unwrap();
        let packet = encode_one(rate, &tone(rate, 960, 440.0));
        let mut tiny = [0i16; 160];
        assert!(matches!(
            decoder.decode(&packet, &mut tiny),
            Err(OpusError::FrameTooLong { .. })
        ));
    }

    #[test]
    fn formats_this_tap_cannot_decode_are_refused_by_name() {
        assert!(matches!(
            OpusStreamDecoder::new(AudioFormat::pcmu_8k_20ms()),
            Err(OpusError::NotOpus(Encoding::Pcmu))
        ));
        assert!(matches!(
            OpusStreamDecoder::new(AudioFormat {
                channels: 2,
                ..opus_at(48000)
            }),
            Err(OpusError::UnsupportedChannelCount(2))
        ));
        assert!(matches!(
            OpusStreamDecoder::new(opus_at(44100)),
            Err(OpusError::UnsupportedSampleRate(44100))
        ));
    }

    #[test]
    fn the_longest_legal_frame_fits_the_scratch_buffer_at_every_rate() {
        for rate in LEGAL_SAMPLE_RATES_HZ {
            let decoder = OpusStreamDecoder::new(opus_at(rate)).unwrap();
            assert_eq!(
                decoder.max_frame_samples(),
                (rate / 1000 * MAX_OPUS_FRAME_MS) as usize
            );
        }
    }
}
