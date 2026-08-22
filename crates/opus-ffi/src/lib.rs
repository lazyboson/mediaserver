use opusic_sys::{
    opus_decode, opus_decoder_create, opus_decoder_destroy, opus_encode, opus_encoder_create,
    opus_encoder_destroy, opus_packet_get_nb_samples, opus_strerror, OpusDecoder as RawDecoder,
    OpusEncoder as RawEncoder, OPUS_APPLICATION_VOIP, OPUS_OK,
};
use std::ffi::CStr;
use thiserror::Error;

pub const MAX_PACKET_BYTES: usize = 1276;
pub const MAX_FRAME_MS: u32 = 60;
pub const LEGAL_SAMPLE_RATES_HZ: [u32; 5] = [8000, 12000, 16000, 24000, 48000];
pub const HIGHEST_SAMPLE_RATE_HZ: u32 = 48000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OpusFfiError {
    #[error("libopus decodes to 8/12/16/24/48 kHz; {0} Hz is not one of them")]
    UnsupportedSampleRate(u32),
    #[error("this wrapper decodes mono; {0} channels were requested")]
    UnsupportedChannelCount(u8),
    #[error("libopus could not create a decoder: {0}")]
    CreateFailed(String),
    #[error("libopus rejected the packet: {0}")]
    RejectedPacket(String),
    #[error("an empty packet means loss; call conceal for that")]
    EmptyPacket,
    #[error("a {frame}-sample frame does not fit the caller's {capacity}-sample buffer")]
    FrameTooLong { frame: usize, capacity: usize },
    #[error("libopus returned a frame length of {0}")]
    NonsenseFrameLength(i32),
}

fn describe(code: i32) -> String {
    let text = unsafe { CStr::from_ptr(opus_strerror(code)) };
    text.to_string_lossy().into_owned()
}

pub struct OpusDecoder {
    raw: *mut RawDecoder,
    sample_rate_hz: u32,
    max_frame_samples: usize,
}

unsafe impl Send for OpusDecoder {}

impl OpusDecoder {
    pub fn new(sample_rate_hz: u32, channels: u8) -> Result<Self, OpusFfiError> {
        if channels != 1 {
            return Err(OpusFfiError::UnsupportedChannelCount(channels));
        }
        if !LEGAL_SAMPLE_RATES_HZ.contains(&sample_rate_hz) {
            return Err(OpusFfiError::UnsupportedSampleRate(sample_rate_hz));
        }
        let mut code = 0i32;
        let raw = unsafe { opus_decoder_create(sample_rate_hz as i32, 1, &mut code) };
        if raw.is_null() || code != OPUS_OK {
            return Err(OpusFfiError::CreateFailed(describe(code)));
        }
        Ok(OpusDecoder {
            raw,
            sample_rate_hz,
            max_frame_samples: (sample_rate_hz / 1000 * MAX_FRAME_MS) as usize,
        })
    }

    pub fn max_frame_samples(&self) -> usize {
        self.max_frame_samples
    }

    pub fn frame_samples(&self, packet: &[u8]) -> Result<usize, OpusFfiError> {
        if packet.is_empty() {
            return Err(OpusFfiError::EmptyPacket);
        }
        let samples = unsafe {
            opus_packet_get_nb_samples(
                packet.as_ptr(),
                packet.len() as i32,
                self.sample_rate_hz as i32,
            )
        };
        if samples <= 0 {
            return Err(OpusFfiError::RejectedPacket(describe(samples)));
        }
        Ok(samples as usize)
    }

    pub fn decode(&mut self, packet: &[u8], out: &mut [i16]) -> Result<usize, OpusFfiError> {
        let frame = self.frame_samples(packet)?;
        if frame > out.len() {
            return Err(OpusFfiError::FrameTooLong {
                frame,
                capacity: out.len(),
            });
        }
        let produced = unsafe {
            opus_decode(
                self.raw,
                packet.as_ptr(),
                packet.len() as i32,
                out.as_mut_ptr(),
                frame as i32,
                0,
            )
        };
        self.settle(produced, out.len())
    }

    pub fn conceal(&mut self, out: &mut [i16]) -> Result<usize, OpusFfiError> {
        if out.is_empty() {
            return Err(OpusFfiError::FrameTooLong {
                frame: 1,
                capacity: 0,
            });
        }
        let produced = unsafe {
            opus_decode(
                self.raw,
                std::ptr::null(),
                0,
                out.as_mut_ptr(),
                out.len() as i32,
                0,
            )
        };
        self.settle(produced, out.len())
    }

    fn settle(&self, produced: i32, capacity: usize) -> Result<usize, OpusFfiError> {
        if produced < 0 {
            return Err(OpusFfiError::RejectedPacket(describe(produced)));
        }
        let produced = produced as usize;
        if produced > capacity {
            return Err(OpusFfiError::NonsenseFrameLength(produced as i32));
        }
        Ok(produced)
    }
}

impl Drop for OpusDecoder {
    fn drop(&mut self) {
        unsafe { opus_decoder_destroy(self.raw) };
    }
}

pub struct OpusEncoder {
    raw: *mut RawEncoder,
}

unsafe impl Send for OpusEncoder {}

impl OpusEncoder {
    pub fn new(sample_rate_hz: u32, channels: u8) -> Result<Self, OpusFfiError> {
        if channels != 1 {
            return Err(OpusFfiError::UnsupportedChannelCount(channels));
        }
        if !LEGAL_SAMPLE_RATES_HZ.contains(&sample_rate_hz) {
            return Err(OpusFfiError::UnsupportedSampleRate(sample_rate_hz));
        }
        let mut code = 0i32;
        let raw = unsafe {
            opus_encoder_create(sample_rate_hz as i32, 1, OPUS_APPLICATION_VOIP, &mut code)
        };
        if raw.is_null() || code != OPUS_OK {
            return Err(OpusFfiError::CreateFailed(describe(code)));
        }
        Ok(OpusEncoder { raw })
    }

    pub fn encode(&mut self, pcm: &[i16], out: &mut [u8]) -> Result<usize, OpusFfiError> {
        let bytes = unsafe {
            opus_encode(
                self.raw,
                pcm.as_ptr(),
                pcm.len() as i32,
                out.as_mut_ptr(),
                out.len() as i32,
            )
        };
        if bytes < 0 {
            return Err(OpusFfiError::RejectedPacket(describe(bytes)));
        }
        Ok(bytes as usize)
    }
}

impl Drop for OpusEncoder {
    fn drop(&mut self) {
        unsafe { opus_encoder_destroy(self.raw) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn encoder_at(sample_rate_hz: u32) -> OpusEncoder {
        OpusEncoder::new(sample_rate_hz, 1).unwrap()
    }

    fn encode_one(encoder: &mut OpusEncoder, pcm: &[i16]) -> Vec<u8> {
        let mut packet = vec![0u8; MAX_PACKET_BYTES];
        let bytes = encoder.encode(pcm, &mut packet).unwrap();
        assert!(bytes > 0);
        packet.truncate(bytes);
        packet
    }

    fn tone(sample_rate_hz: u32, samples: usize, hz: f32) -> Vec<i16> {
        (0..samples)
            .map(|n| {
                let t = n as f32 / sample_rate_hz as f32;
                ((t * hz * 2.0 * std::f32::consts::PI).sin() * 12000.0) as i16
            })
            .collect()
    }

    fn rms(samples: &[i16]) -> f64 {
        let sum: f64 = samples.iter().map(|s| f64::from(*s) * f64::from(*s)).sum();
        (sum / samples.len().max(1) as f64).sqrt()
    }

    #[test]
    fn a_tone_survives_a_round_trip_through_libopus_at_every_supported_rate() {
        for rate in LEGAL_SAMPLE_RATES_HZ {
            let frame = (rate / 1000 * 20) as usize;
            let mut encoder = encoder_at(rate);
            let mut decoder = OpusDecoder::new(rate, 1).unwrap();
            let mut out = vec![0i16; decoder.max_frame_samples()];

            let mut decoded_rms = 0.0;
            for step in 0..10 {
                let packet = encode_one(&mut encoder, &tone(rate, frame, 440.0));
                assert!(packet.len() <= MAX_PACKET_BYTES);
                assert_eq!(decoder.frame_samples(&packet).unwrap(), frame);
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
        let mut encoder = encoder_at(rate);
        let mut decoder = OpusDecoder::new(rate, 1).unwrap();
        let packet = encode_one(&mut encoder, &vec![0i16; frame]);
        let mut out = vec![0i16; decoder.max_frame_samples()];
        assert_eq!(decoder.decode(&packet, &mut out).unwrap(), frame);
        assert!(rms(&out[..frame]) < 50.0);
    }

    #[test]
    fn libopus_conceals_a_lost_frame_itself() {
        let rate = 16000;
        let frame = 320;
        let mut encoder = encoder_at(rate);
        let mut decoder = OpusDecoder::new(rate, 1).unwrap();
        let mut out = vec![0i16; frame];
        for _ in 0..5 {
            let packet = encode_one(&mut encoder, &tone(rate, frame, 440.0));
            decoder.decode(&packet, &mut out).unwrap();
        }
        let voiced = rms(&out);

        let mut concealed = vec![0i16; frame];
        assert_eq!(decoder.conceal(&mut concealed).unwrap(), frame);
        assert!(
            rms(&concealed) > voiced * 0.05,
            "concealment produced rms {} against {voiced} of real audio",
            rms(&concealed)
        );
    }

    #[test]
    fn a_malformed_packet_is_an_error_not_a_crash() {
        let mut decoder = OpusDecoder::new(16000, 1).unwrap();
        let mut out = vec![0i16; decoder.max_frame_samples()];
        assert_eq!(
            decoder.decode(&[], &mut out),
            Err(OpusFfiError::EmptyPacket)
        );
        for junk in [
            vec![0xFFu8; 8],
            vec![0x00u8; 200],
            (0..64u8).collect::<Vec<u8>>(),
        ] {
            let outcome = decoder.decode(&junk, &mut out);
            assert!(
                outcome.is_ok() || matches!(outcome, Err(OpusFfiError::RejectedPacket(_))),
                "junk packet produced {outcome:?}"
            );
        }
    }

    #[test]
    fn a_buffer_too_small_for_the_frame_is_refused_before_libopus_sees_it() {
        let rate = 48000;
        let frame = 960;
        let mut encoder = encoder_at(rate);
        let mut decoder = OpusDecoder::new(rate, 1).unwrap();
        let packet = encode_one(&mut encoder, &tone(rate, frame, 440.0));
        let mut tiny = [0i16; 160];
        assert_eq!(
            decoder.decode(&packet, &mut tiny),
            Err(OpusFfiError::FrameTooLong {
                frame,
                capacity: 160
            })
        );
    }

    #[test]
    fn formats_libopus_cannot_serve_are_refused_by_name() {
        assert_eq!(
            OpusDecoder::new(44100, 1).err(),
            Some(OpusFfiError::UnsupportedSampleRate(44100))
        );
        assert_eq!(
            OpusDecoder::new(48000, 2).err(),
            Some(OpusFfiError::UnsupportedChannelCount(2))
        );
    }

    #[test]
    fn a_decoder_can_be_moved_to_another_thread() {
        let mut decoder = OpusDecoder::new(16000, 1).unwrap();
        let mut encoder = encoder_at(16000);
        let packet = encode_one(&mut encoder, &tone(16000, 320, 440.0));
        let decoded = std::thread::spawn(move || {
            let mut out = vec![0i16; decoder.max_frame_samples()];
            decoder.decode(&packet, &mut out).map(|n| rms(&out[..n]))
        })
        .join()
        .unwrap()
        .unwrap();
        assert!(decoded > 0.0);
    }
}
