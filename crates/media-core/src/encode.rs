use crate::frame::{AudioFormat, Encoding};
use crate::g711;
use rubato::audioadapter_buffers::direct::SequentialSlice;
use rubato::{Fft, FixedSync, Resampler};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("cannot encode the {tap:?} tap into {requested:?}: {reason}")]
    Unsupported {
        tap: AudioFormat,
        requested: AudioFormat,
        reason: &'static str,
    },
    #[error("resampler: {0}")]
    Resample(String),
}

pub struct ConsumerEncoder {
    target: Encoding,
    chunk_in: usize,
    resampler: Option<Fft<f32>>,
    float_in: Vec<f32>,
    float_out: Vec<f32>,
    bytes: Vec<u8>,
}

fn refusal(source: AudioFormat, target: AudioFormat) -> Option<&'static str> {
    if source.channels != 1 || target.channels != 1 {
        return Some("only mono is supported");
    }
    if source.ptime_ms != target.ptime_ms {
        return Some("the consumer ptime must match the tap ptime");
    }
    if source.samples_per_packet().is_none() || target.samples_per_packet().is_none() {
        return Some("sample rate and ptime must be non-zero");
    }
    match target.encoding {
        Encoding::Pcmu | Encoding::Pcma => {
            if target.sample_rate_hz != source.sample_rate_hz {
                Some("g711 output only exists at the tap rate")
            } else {
                None
            }
        }
        Encoding::L16 => None,
        Encoding::Opus => Some("opus output is not built yet"),
    }
}

impl ConsumerEncoder {
    pub fn supports(source: AudioFormat, target: AudioFormat) -> bool {
        refusal(source, target).is_none()
    }

    pub fn new(source: AudioFormat, target: AudioFormat) -> Result<Self, EncodeError> {
        if let Some(reason) = refusal(source, target) {
            return Err(EncodeError::Unsupported {
                tap: source,
                requested: target,
                reason,
            });
        }
        let chunk_in = source
            .samples_per_packet()
            .expect("refusal checked samples_per_packet") as usize;
        let resampler = if target.sample_rate_hz != source.sample_rate_hz {
            Some(
                Fft::<f32>::new(
                    source.sample_rate_hz as usize,
                    target.sample_rate_hz as usize,
                    chunk_in,
                    1,
                    FixedSync::Input,
                )
                .map_err(|error| EncodeError::Resample(error.to_string()))?,
            )
        } else {
            None
        };
        let out_capacity = resampler
            .as_ref()
            .map(|resampler| resampler.output_frames_max())
            .unwrap_or(chunk_in);
        Ok(ConsumerEncoder {
            target: target.encoding,
            chunk_in,
            resampler,
            float_in: vec![0.0; chunk_in],
            float_out: vec![0.0; out_capacity],
            bytes: Vec::with_capacity(out_capacity * 2),
        })
    }

    pub fn encode(&mut self, pcm: &[i16]) -> Result<&[u8], EncodeError> {
        self.bytes.clear();
        match self.resampler.as_mut() {
            None => {
                for sample in pcm.iter().take(self.chunk_in) {
                    match self.target {
                        Encoding::Pcmu => self.bytes.push(g711::linear_to_ulaw(*sample)),
                        Encoding::Pcma => self.bytes.push(g711::linear_to_alaw(*sample)),
                        Encoding::L16 | Encoding::Opus => {
                            self.bytes.extend_from_slice(&sample.to_le_bytes())
                        }
                    }
                }
            }
            Some(resampler) => {
                let len = pcm.len().min(self.chunk_in);
                for (slot, sample) in self.float_in.iter_mut().zip(pcm.iter().take(len)) {
                    *slot = f32::from(*sample) / 32768.0;
                }
                self.float_in[len..].fill(0.0);
                let input = SequentialSlice::new(&self.float_in[..], 1, self.chunk_in)
                    .map_err(|error| EncodeError::Resample(error.to_string()))?;
                let capacity = self.float_out.len();
                let mut output = SequentialSlice::new_mut(&mut self.float_out[..], 1, capacity)
                    .map_err(|error| EncodeError::Resample(error.to_string()))?;
                let (_, written) = resampler
                    .process_into_buffer(&input, &mut output, None)
                    .map_err(|error| EncodeError::Resample(error.to_string()))?;
                for value in self.float_out.iter().take(written) {
                    let sample = (value * 32768.0).round().clamp(-32768.0, 32767.0) as i16;
                    self.bytes.extend_from_slice(&sample.to_le_bytes());
                }
            }
        }
        Ok(&self.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pcma_8k() -> AudioFormat {
        AudioFormat {
            encoding: Encoding::Pcma,
            ..AudioFormat::pcmu_8k_20ms()
        }
    }

    fn l16_8k() -> AudioFormat {
        AudioFormat {
            encoding: Encoding::L16,
            sample_rate_hz: 8000,
            channels: 1,
            ptime_ms: 20,
        }
    }

    fn l16_48k() -> AudioFormat {
        AudioFormat {
            sample_rate_hz: 48000,
            ..AudioFormat::l16_16k_20ms()
        }
    }

    fn sine_8k(frames: usize, hz: f32) -> Vec<Vec<i16>> {
        (0..frames)
            .map(|frame| {
                (0..160)
                    .map(|n| {
                        let t = (frame * 160 + n) as f32 / 8000.0;
                        ((t * hz * 2.0 * std::f32::consts::PI).sin() * 8000.0) as i16
                    })
                    .collect()
            })
            .collect()
    }

    fn rms(samples: &[i16]) -> f64 {
        let sum: f64 = samples.iter().map(|s| f64::from(*s) * f64::from(*s)).sum();
        (sum / samples.len().max(1) as f64).sqrt()
    }

    #[test]
    fn g711_passthrough_matches_the_reference_tables() {
        let source = AudioFormat::pcmu_8k_20ms();
        let pcm = [0i16, 1000, -1000, 32000];

        let mut ulaw = ConsumerEncoder::new(source, source).unwrap();
        let expected: Vec<u8> = pcm.iter().map(|s| g711::linear_to_ulaw(*s)).collect();
        assert_eq!(ulaw.encode(&pcm).unwrap(), expected.as_slice());

        let mut alaw = ConsumerEncoder::new(source, pcma_8k()).unwrap();
        let expected: Vec<u8> = pcm.iter().map(|s| g711::linear_to_alaw(*s)).collect();
        assert_eq!(alaw.encode(&pcm).unwrap(), expected.as_slice());
    }

    #[test]
    fn l16_at_the_tap_rate_is_little_endian_identity() {
        let mut encoder = ConsumerEncoder::new(AudioFormat::pcmu_8k_20ms(), l16_8k()).unwrap();
        let bytes = encoder.encode(&[1i16, -2, 258]).unwrap();
        assert_eq!(bytes, [1, 0, 254, 255, 2, 1]);
    }

    #[test]
    fn l16_16k_doubles_the_sample_count_and_preserves_the_tone() {
        let mut encoder =
            ConsumerEncoder::new(AudioFormat::pcmu_8k_20ms(), AudioFormat::l16_16k_20ms()).unwrap();
        let frames = sine_8k(50, 400.0);
        let input_rms = rms(&frames.concat());

        let mut output = Vec::new();
        for frame in &frames {
            let bytes = encoder.encode(frame).unwrap();
            assert_eq!(bytes.len(), 640);
            for pair in bytes.chunks_exact(2) {
                output.push(i16::from_le_bytes([pair[0], pair[1]]));
            }
        }

        assert_eq!(output.len(), 50 * 320);
        let steady = &output[output.len() / 2..];
        let output_rms = rms(steady);
        assert!(
            (output_rms - input_rms).abs() / input_rms < 0.15,
            "input rms {input_rms}, resampled rms {output_rms}"
        );
    }

    #[test]
    fn l16_48k_yields_six_times_the_samples() {
        let mut encoder = ConsumerEncoder::new(AudioFormat::pcmu_8k_20ms(), l16_48k()).unwrap();
        let bytes = encoder.encode(&[0i16; 160]).unwrap();
        assert_eq!(bytes.len(), 960 * 2);
    }

    #[test]
    fn a_short_frame_is_padded_so_the_stream_stays_continuous() {
        let mut encoder =
            ConsumerEncoder::new(AudioFormat::pcmu_8k_20ms(), AudioFormat::l16_16k_20ms()).unwrap();
        let bytes = encoder.encode(&[100i16; 80]).unwrap();
        assert_eq!(bytes.len(), 640);
    }

    #[test]
    fn unsupported_targets_are_refused_by_name_not_silently_wrong() {
        let source = AudioFormat::pcmu_8k_20ms();
        let opus = AudioFormat {
            encoding: Encoding::Opus,
            sample_rate_hz: 48000,
            channels: 1,
            ptime_ms: 20,
        };
        assert!(!ConsumerEncoder::supports(source, opus));
        assert!(matches!(
            ConsumerEncoder::new(source, opus),
            Err(EncodeError::Unsupported { reason, .. }) if reason.contains("opus")
        ));

        let pcmu_16k = AudioFormat {
            sample_rate_hz: 16000,
            ..source
        };
        assert!(!ConsumerEncoder::supports(source, pcmu_16k));

        let stereo = AudioFormat {
            channels: 2,
            ..AudioFormat::l16_16k_20ms()
        };
        assert!(!ConsumerEncoder::supports(source, stereo));

        let other_ptime = AudioFormat {
            ptime_ms: 40,
            ..AudioFormat::l16_16k_20ms()
        };
        assert!(!ConsumerEncoder::supports(source, other_ptime));
    }
}
