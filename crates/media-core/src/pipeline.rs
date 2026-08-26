use crate::dtmf::{DigitPress, DtmfDetector};
use crate::frame::{AudioFormat, Encoding};
use crate::g711;
use crate::jitter::{
    self, JitterBuffer, JitterConfig, PopOutcome, PushOutcome, Timing, MAX_FRAME_SAMPLES,
    MAX_PAYLOAD,
};
use crate::opus::{OpusStreamDecoder, MAX_OPUS_FRAME_SAMPLES};
use crate::plc::PacketLossConcealer;
use crate::rtp::RtpPacket;
use thiserror::Error;

pub const COMFORT_NOISE_PAYLOAD_TYPE: u8 = 13;

const MAX_DEPTH_MULTIPLIER: u16 = 4;
const MICROS_PER_SECOND: u64 = 1_000_000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PipelineError {
    #[error("encoding {0:?} is not decodable by this pipeline; G.711 only until Opus lands")]
    UnsupportedEncoding(Encoding),
    #[error("format has no usable ptime/sample rate")]
    UnusableFormat,
    #[error("{samples} samples per packet exceeds the {max}-sample slot size")]
    PacketTooLong { samples: usize, max: usize },
    #[error("opus: {0}")]
    Opus(crate::opus::OpusError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    Buffered,
    Duplicate,
    TooLate,
    Oversized,
    Resynchronized,
    Dtmf(DigitPress),
    TelephoneEvent,
    ComfortNoise,
    UnknownPayloadType(u8),
    Unparsable,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Playout<'a> {
    Pcm(&'a [i16]),
    Concealed(&'a [i16]),
    Suppressed(&'a [i16]),
    Waiting,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PipelineStats {
    pub undecodable_frames: u64,
    pub frame_size_mismatch: u64,
    pub carry_overflow_samples: u64,
    pub unparsable: u64,
    pub unknown_payload_type: u64,
    pub companded: u64,
    pub telephone_events: u64,
    pub comfort_noise: u64,
    pub dtmf_digits: u64,
    pub frames_played: u64,
    pub frames_concealed: u64,
    pub frames_suppressed: u64,
}

struct Carry {
    samples: [i16; MAX_OPUS_FRAME_SAMPLES],
    len: usize,
}

impl Carry {
    fn new() -> Self {
        Carry {
            samples: [0; MAX_OPUS_FRAME_SAMPLES],
            len: 0,
        }
    }

    fn push(&mut self, decoded: &[i16]) -> usize {
        let room = self.samples.len() - self.len;
        let taken = decoded.len().min(room);
        self.samples[self.len..self.len + taken].copy_from_slice(&decoded[..taken]);
        self.len += taken;
        decoded.len() - taken
    }

    fn take(&mut self, out: &mut [i16]) -> usize {
        let taken = self.len.min(out.len());
        out[..taken].copy_from_slice(&self.samples[..taken]);
        self.samples.copy_within(taken..self.len, 0);
        self.len -= taken;
        if taken < out.len() {
            out[taken..].fill(0);
        }
        taken
    }

    fn clear(&mut self) {
        self.len = 0;
    }
}

fn companion_payload_type(encoding: Encoding) -> Option<u8> {
    match encoding {
        Encoding::Pcmu => Encoding::Pcma.static_payload_type(),
        Encoding::Pcma => Encoding::Pcmu.static_payload_type(),
        _ => None,
    }
}

enum Decoder {
    G711 { ulaw: bool },
    Opus(Box<OpusStreamDecoder>),
}

pub struct PipelineConfig {
    pub audio_payload_type: u8,
    pub clock_rate_hz: u32,
    pub decode: AudioFormat,
    pub target_depth_packets: u16,
    pub telephone_event_payload_type: Option<u8>,
}

pub struct StreamPipeline {
    audio_payload_type: u8,
    companding_payload_type: Option<u8>,
    telephone_event_payload_type: Option<u8>,
    decoder: Decoder,
    samples_per_packet: usize,
    sample_rate_hz: u32,
    jitter: JitterBuffer,
    dtmf: DtmfDetector,
    plc: PacketLossConcealer,
    pcm: [i16; MAX_FRAME_SAMPLES],
    decoded: [i16; MAX_OPUS_FRAME_SAMPLES],
    carry: Carry,
    stats: PipelineStats,
    last_audio_ssrc: Option<u32>,
}

impl StreamPipeline {
    pub fn new(
        format: AudioFormat,
        target_depth_packets: u16,
        telephone_event_payload_type: Option<u8>,
    ) -> Result<Self, PipelineError> {
        let audio_payload_type = format
            .encoding
            .static_payload_type()
            .ok_or(PipelineError::UnsupportedEncoding(format.encoding))?;
        StreamPipeline::with_config(PipelineConfig {
            audio_payload_type,
            clock_rate_hz: format.sample_rate_hz,
            decode: format,
            target_depth_packets,
            telephone_event_payload_type,
        })
    }

    pub fn with_config(config: PipelineConfig) -> Result<Self, PipelineError> {
        let PipelineConfig {
            audio_payload_type,
            clock_rate_hz,
            decode: format,
            target_depth_packets,
            telephone_event_payload_type,
        } = config;
        let decoder = match format.encoding {
            Encoding::Pcmu => Decoder::G711 { ulaw: true },
            Encoding::Pcma => Decoder::G711 { ulaw: false },
            Encoding::Opus => Decoder::Opus(Box::new(
                OpusStreamDecoder::new(format).map_err(PipelineError::Opus)?,
            )),
            other => return Err(PipelineError::UnsupportedEncoding(other)),
        };
        let timestamp_increment = if clock_rate_hz == 0 || format.ptime_ms == 0 {
            return Err(PipelineError::UnusableFormat);
        } else {
            clock_rate_hz / 1000 * format.ptime_ms
        };
        let samples_per_packet = format
            .samples_per_packet()
            .filter(|samples| *samples > 0)
            .ok_or(PipelineError::UnusableFormat)? as usize;
        if samples_per_packet > MAX_FRAME_SAMPLES {
            return Err(PipelineError::PacketTooLong {
                samples: samples_per_packet,
                max: MAX_FRAME_SAMPLES,
            });
        }
        let floor_depth = target_depth_packets.max(1);
        Ok(StreamPipeline {
            audio_payload_type,
            companding_payload_type: companion_payload_type(format.encoding),
            telephone_event_payload_type,
            decoder,
            samples_per_packet,
            sample_rate_hz: format.sample_rate_hz,
            jitter: JitterBuffer::with_config(JitterConfig {
                target_depth_packets: floor_depth,
                max_depth_packets: floor_depth.saturating_mul(MAX_DEPTH_MULTIPLIER),
                timestamp_increment,
            }),
            dtmf: DtmfDetector::new(clock_rate_hz),
            plc: PacketLossConcealer::new(format.sample_rate_hz),
            pcm: [0; MAX_FRAME_SAMPLES],
            decoded: [0; MAX_OPUS_FRAME_SAMPLES],
            carry: Carry::new(),
            stats: PipelineStats::default(),
            last_audio_ssrc: None,
        })
    }

    pub fn stats(&self) -> PipelineStats {
        self.stats
    }

    pub fn jitter_stats(&self) -> jitter::Stats {
        self.jitter.stats()
    }

    pub fn samples_per_packet(&self) -> usize {
        self.samples_per_packet
    }

    pub fn target_depth_packets(&self) -> u16 {
        self.jitter.target_depth()
    }

    pub fn ingest(&mut self, datagram: &[u8]) -> IngestOutcome {
        self.admit(datagram, None)
    }

    pub fn ingest_at(&mut self, datagram: &[u8], arrival_micros: u64) -> IngestOutcome {
        self.admit(datagram, Some(arrival_micros))
    }

    fn arrival_ticks(&self, arrival_micros: u64) -> u32 {
        (arrival_micros
            .saturating_mul(self.sample_rate_hz as u64)
            .wrapping_div(MICROS_PER_SECOND)) as u32
    }

    fn admit(&mut self, datagram: &[u8], arrival_micros: Option<u64>) -> IngestOutcome {
        let Ok(packet) = RtpPacket::parse(datagram) else {
            self.stats.unparsable += 1;
            return IngestOutcome::Unparsable;
        };
        if Some(packet.payload_type) == self.telephone_event_payload_type {
            self.stats.telephone_events += 1;
            self.jitter.account(packet.sequence);
            return match self.dtmf.push(packet.timestamp, packet.payload) {
                Some(press) => {
                    self.stats.dtmf_digits += 1;
                    IngestOutcome::Dtmf(press)
                }
                None => IngestOutcome::TelephoneEvent,
            };
        }
        if packet.payload_type == COMFORT_NOISE_PAYLOAD_TYPE {
            self.stats.comfort_noise += 1;
            self.jitter.account(packet.sequence);
            return IngestOutcome::ComfortNoise;
        }
        let mut companded = [0u8; MAX_PAYLOAD];
        let payload = if packet.payload_type == self.audio_payload_type {
            packet.payload
        } else if Some(packet.payload_type) == self.companding_payload_type
            && packet.payload.len() <= MAX_PAYLOAD
        {
            for (out, byte) in companded.iter_mut().zip(packet.payload) {
                *out = match self.decoder {
                    Decoder::G711 { ulaw: true } => {
                        g711::linear_to_ulaw(g711::alaw_to_linear(*byte))
                    }
                    _ => g711::linear_to_alaw(g711::ulaw_to_linear(*byte)),
                };
            }
            self.stats.companded += 1;
            &companded[..packet.payload.len()]
        } else {
            self.stats.unknown_payload_type += 1;
            return IngestOutcome::UnknownPayloadType(packet.payload_type);
        };
        let sender_changed = self
            .last_audio_ssrc
            .is_some_and(|previous| previous != packet.ssrc);
        self.last_audio_ssrc = Some(packet.ssrc);
        if sender_changed {
            self.jitter.restart();
            self.plc.forget();
        }
        let timing = Timing {
            timestamp: packet.timestamp,
            arrival_ticks: match arrival_micros {
                Some(micros) => self.arrival_ticks(micros),
                None => packet.timestamp,
            },
            marker: packet.marker,
        };
        let outcome = self.jitter.push_timed(packet.sequence, payload, timing);
        if matches!(
            outcome,
            PushOutcome::BufferedAfterSenderSilence | PushOutcome::Reset
        ) {
            self.plc.forget();
        }
        match outcome {
            PushOutcome::Buffered | PushOutcome::BufferedAfterSenderSilence => {
                if sender_changed {
                    IngestOutcome::Resynchronized
                } else {
                    IngestOutcome::Buffered
                }
            }
            PushOutcome::Duplicate => IngestOutcome::Duplicate,
            PushOutcome::TooLate => IngestOutcome::TooLate,
            PushOutcome::TooBig => IngestOutcome::Oversized,
            PushOutcome::Reset => IngestOutcome::Resynchronized,
        }
    }

    pub fn last_audio_ssrc(&self) -> Option<u32> {
        self.last_audio_ssrc
    }

    pub fn release(&mut self) -> Playout<'_> {
        let Self {
            jitter,
            pcm,
            plc,
            decoder,
            decoded,
            carry,
            samples_per_packet,
            stats,
            ..
        } = self;
        let frame = *samples_per_packet;
        if carry.len >= frame {
            carry.take(&mut pcm[..frame]);
            stats.frames_played += 1;
            return Playout::Pcm(&pcm[..frame]);
        }
        match jitter.pop() {
            PopOutcome::Packet(payload) => {
                match decoder {
                    Decoder::G711 { ulaw } => {
                        let samples = g711::decode_into(*ulaw, payload, pcm);
                        if samples != frame {
                            stats.frame_size_mismatch += 1;
                            pcm[samples.min(frame)..frame].fill(0);
                        }
                        plc.recover_into(&mut pcm[..frame]);
                        plc.remember(&pcm[..frame]);
                    }
                    Decoder::Opus(opus) => {
                        let samples = match opus.decode(payload, &mut decoded[..]) {
                            Ok(samples) => samples,
                            Err(_) => {
                                stats.undecodable_frames += 1;
                                decoded[..frame].fill(0);
                                frame
                            }
                        };
                        if samples != frame {
                            stats.frame_size_mismatch += 1;
                        }
                        let dropped = carry.push(&decoded[..samples]);
                        if dropped > 0 {
                            stats.carry_overflow_samples += dropped as u64;
                        }
                        carry.take(&mut pcm[..frame]);
                    }
                }
                stats.frames_played += 1;
                Playout::Pcm(&pcm[..frame])
            }
            PopOutcome::Accounted => {
                pcm[..frame].fill(0);
                plc.forget();
                carry.clear();
                stats.frames_suppressed += 1;
                Playout::Suppressed(&pcm[..frame])
            }
            PopOutcome::Lost => {
                match decoder {
                    Decoder::G711 { .. } => plc.conceal(&mut pcm[..frame]),
                    Decoder::Opus(opus) => {
                        if opus.conceal(&mut pcm[..frame]).is_err() {
                            pcm[..frame].fill(0);
                        }
                    }
                }
                stats.frames_concealed += 1;
                Playout::Concealed(&pcm[..frame])
            }
            PopOutcome::Waiting => Playout::Waiting,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{disturb, Disturbance, G711StreamGenerator};

    const TELEPHONE_EVENT_PT: u8 = 101;

    fn pipeline() -> StreamPipeline {
        StreamPipeline::new(AudioFormat::pcmu_8k_20ms(), 2, Some(TELEPHONE_EVENT_PT)).unwrap()
    }

    fn generator() -> G711StreamGenerator {
        G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 0xFEED, 1000).unwrap()
    }

    #[test]
    fn an_alaw_packet_on_a_pcmu_tap_is_companded_not_discarded() {
        let mut pipeline = pipeline();
        let mut stream = G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 7, 0).unwrap();
        let loud = g711::linear_to_alaw(8000);
        for _ in 0..4 {
            let mut datagram = stream.next_datagram();
            datagram[1] = Encoding::Pcma.static_payload_type().unwrap();
            for byte in datagram[12..].iter_mut() {
                *byte = loud;
            }
            assert_eq!(pipeline.ingest(&datagram), IngestOutcome::Buffered);
        }
        assert_eq!(pipeline.stats().companded, 4);
        assert_eq!(pipeline.stats().unknown_payload_type, 0);

        let mut played = None;
        for _ in 0..8 {
            if let Playout::Pcm(pcm) = pipeline.release() {
                played = Some(pcm[0]);
                break;
            }
        }
        let heard = played.expect("the companded packet should reach playout");
        assert!(
            (heard - 8000).abs() < 600,
            "decoded {heard}, too far from 8000 for two companding steps"
        );
    }

    #[test]
    fn a_payload_type_that_is_neither_g711_variant_is_still_unknown() {
        let mut pipeline = pipeline();
        let mut stream = G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 7, 0).unwrap();
        let mut datagram = stream.next_datagram();
        datagram[1] = 96;

        assert_eq!(
            pipeline.ingest(&datagram),
            IngestOutcome::UnknownPayloadType(96)
        );
        assert_eq!(pipeline.stats().companded, 0);
        assert_eq!(pipeline.stats().unknown_payload_type, 1);
    }

    #[test]
    fn rejects_formats_it_cannot_decode() {
        assert_eq!(
            StreamPipeline::new(AudioFormat::l16_16k_20ms(), 2, None).err(),
            Some(PipelineError::UnsupportedEncoding(Encoding::L16))
        );
        assert_eq!(
            StreamPipeline::new(
                AudioFormat {
                    ptime_ms: 0,
                    ..AudioFormat::pcmu_8k_20ms()
                },
                2,
                None
            )
            .err(),
            Some(PipelineError::UnusableFormat)
        );
        assert_eq!(
            StreamPipeline::new(
                AudioFormat {
                    ptime_ms: 200,
                    ..AudioFormat::pcmu_8k_20ms()
                },
                2,
                None
            )
            .err(),
            Some(PipelineError::PacketTooLong {
                samples: 1600,
                max: MAX_FRAME_SAMPLES
            })
        );
    }

    #[test]
    fn an_opus_pipeline_separates_the_rtp_clock_from_the_rate_it_decodes_to() {
        let opus_16k = AudioFormat {
            encoding: Encoding::Opus,
            sample_rate_hz: 16000,
            channels: 1,
            ptime_ms: 20,
        };
        let pipeline = StreamPipeline::with_config(PipelineConfig {
            audio_payload_type: 111,
            clock_rate_hz: 48000,
            decode: opus_16k,
            target_depth_packets: 3,
            telephone_event_payload_type: Some(101),
        })
        .unwrap();

        assert_eq!(pipeline.samples_per_packet(), 320);
        assert_eq!(pipeline.jitter_stats().target_depth, 3);
    }

    #[test]
    fn real_opus_rtp_decodes_into_audible_pcm_at_the_taps_rate() {
        const OPUS_PT: u8 = 111;
        let decode = AudioFormat {
            encoding: Encoding::Opus,
            sample_rate_hz: 16000,
            channels: 1,
            ptime_ms: 20,
        };
        let mut pipeline = StreamPipeline::with_config(PipelineConfig {
            audio_payload_type: OPUS_PT,
            clock_rate_hz: 48000,
            decode,
            target_depth_packets: 2,
            telephone_event_payload_type: Some(TELEPHONE_EVENT_PT),
        })
        .unwrap();

        let mut encoder = opus_ffi::OpusEncoder::new(48000, 1).unwrap();
        let mut packet = [0u8; 1276];
        let mut timestamp: u32 = 4000;
        let mut sequence: u16 = 900;
        let mut loudest = 0i16;
        let mut played = 0usize;

        for frame in 0..25 {
            let tone: Vec<i16> = (0..960)
                .map(|n| {
                    let t = (frame * 960 + n) as f32 / 48000.0;
                    ((t * 440.0 * 2.0 * std::f32::consts::PI).sin() * 10000.0) as i16
                })
                .collect();
            let bytes = encoder.encode(&tone, &mut packet).unwrap();

            let mut datagram = Vec::with_capacity(12 + bytes);
            datagram.push(0x80);
            datagram.push(OPUS_PT);
            datagram.extend_from_slice(&sequence.to_be_bytes());
            datagram.extend_from_slice(&timestamp.to_be_bytes());
            datagram.extend_from_slice(&0x0BADF00Du32.to_be_bytes());
            datagram.extend_from_slice(&packet[..bytes]);
            assert_eq!(pipeline.ingest(&datagram), IngestOutcome::Buffered);
            sequence = sequence.wrapping_add(1);
            timestamp = timestamp.wrapping_add(960);

            if let Playout::Pcm(pcm) = pipeline.release() {
                assert_eq!(pcm.len(), 320);
                played += 1;
                for sample in pcm {
                    loudest = loudest.max(sample.abs());
                }
            }
        }

        assert!(played >= 20, "only {played} frames reached playout");
        assert!(loudest > 4000, "decoded peak {loudest} is not audible tone");
        assert_eq!(pipeline.stats().unknown_payload_type, 0);
        assert_eq!(pipeline.stats().undecodable_frames, 0);
        assert_eq!(pipeline.stats().frame_size_mismatch, 0);
    }

    #[test]
    fn a_sixty_millisecond_opus_sender_keeps_every_sample_instead_of_being_truncated() {
        const OPUS_PT: u8 = 111;
        let mut pipeline = StreamPipeline::with_config(PipelineConfig {
            audio_payload_type: OPUS_PT,
            clock_rate_hz: 48000,
            decode: AudioFormat {
                encoding: Encoding::Opus,
                sample_rate_hz: 16000,
                channels: 1,
                ptime_ms: 20,
            },
            target_depth_packets: 2,
            telephone_event_payload_type: None,
        })
        .unwrap();

        let mut encoder = opus_ffi::OpusEncoder::new(48000, 1).unwrap();
        let mut packet = [0u8; 1276];
        let mut timestamp: u32 = 0;
        let mut sequence: u16 = 0;
        let long_frame_samples = 2880;

        for frame in 0..6 {
            let tone: Vec<i16> = (0..long_frame_samples)
                .map(|n| {
                    let t = (frame * long_frame_samples + n) as f32 / 48000.0;
                    ((t * 440.0 * 2.0 * std::f32::consts::PI).sin() * 10000.0) as i16
                })
                .collect();
            let bytes = encoder.encode(&tone, &mut packet).unwrap();
            let mut datagram = Vec::with_capacity(12 + bytes);
            datagram.push(0x80);
            datagram.push(OPUS_PT);
            datagram.extend_from_slice(&sequence.to_be_bytes());
            datagram.extend_from_slice(&timestamp.to_be_bytes());
            datagram.extend_from_slice(&7u32.to_be_bytes());
            datagram.extend_from_slice(&packet[..bytes]);
            assert_eq!(pipeline.ingest(&datagram), IngestOutcome::Buffered);
            sequence = sequence.wrapping_add(1);
            timestamp = timestamp.wrapping_add(long_frame_samples as u32);
        }

        let mut played = 0usize;
        let mut loud_frames = 0usize;
        for _ in 0..40 {
            if let Playout::Pcm(pcm) = pipeline.release() {
                assert_eq!(pcm.len(), 320);
                played += 1;
                if pcm.iter().any(|sample| sample.abs() > 4000) {
                    loud_frames += 1;
                }
            }
        }

        assert!(
            played >= 12,
            "a 60 ms sender should yield three 20 ms frames per packet, got {played}"
        );
        assert!(
            loud_frames >= played - 2,
            "only {loud_frames} of {played} frames carried the tone; the carry dropped audio"
        );
        assert_eq!(pipeline.stats().carry_overflow_samples, 0);
    }

    #[test]
    fn a_lost_opus_packet_is_concealed_by_libopus_not_by_the_g711_concealer() {
        const OPUS_PT: u8 = 111;
        let decode = AudioFormat {
            encoding: Encoding::Opus,
            sample_rate_hz: 16000,
            channels: 1,
            ptime_ms: 20,
        };
        let mut pipeline = StreamPipeline::with_config(PipelineConfig {
            audio_payload_type: OPUS_PT,
            clock_rate_hz: 48000,
            decode,
            target_depth_packets: 2,
            telephone_event_payload_type: None,
        })
        .unwrap();

        let mut encoder = opus_ffi::OpusEncoder::new(48000, 1).unwrap();
        let mut packet = [0u8; 1276];
        let mut timestamp: u32 = 0;
        let mut sequence: u16 = 0;

        for frame in 0..12 {
            if frame == 6 {
                sequence = sequence.wrapping_add(1);
                timestamp = timestamp.wrapping_add(960);
                continue;
            }
            let tone: Vec<i16> = (0..960)
                .map(|n| {
                    let t = (frame * 960 + n) as f32 / 48000.0;
                    ((t * 440.0 * 2.0 * std::f32::consts::PI).sin() * 10000.0) as i16
                })
                .collect();
            let bytes = encoder.encode(&tone, &mut packet).unwrap();
            let mut datagram = Vec::with_capacity(12 + bytes);
            datagram.push(0x80);
            datagram.push(OPUS_PT);
            datagram.extend_from_slice(&sequence.to_be_bytes());
            datagram.extend_from_slice(&timestamp.to_be_bytes());
            datagram.extend_from_slice(&1u32.to_be_bytes());
            datagram.extend_from_slice(&packet[..bytes]);
            pipeline.ingest(&datagram);
            sequence = sequence.wrapping_add(1);
            timestamp = timestamp.wrapping_add(960);
        }

        let mut concealed = Vec::new();
        for _ in 0..14 {
            if let Playout::Concealed(pcm) = pipeline.release() {
                concealed = pcm.to_vec();
                break;
            }
        }

        assert_eq!(concealed.len(), 320, "libopus should conceal a whole frame");
        assert!(
            concealed.iter().any(|sample| *sample != 0),
            "libopus concealment should extrapolate the tone, not emit silence"
        );
        assert_eq!(pipeline.stats().frames_concealed, 1);
    }

    #[test]
    fn an_opus_pipeline_refuses_a_rate_libopus_does_not_serve() {
        let outcome = StreamPipeline::with_config(PipelineConfig {
            audio_payload_type: 111,
            clock_rate_hz: 48000,
            decode: AudioFormat {
                encoding: Encoding::Opus,
                sample_rate_hz: 44100,
                channels: 1,
                ptime_ms: 20,
            },
            target_depth_packets: 3,
            telephone_event_payload_type: None,
        });
        assert!(matches!(outcome.err(), Some(PipelineError::Opus(_))));
    }

    #[test]
    fn replays_a_clean_stream_sample_exactly() {
        let mut pipeline = pipeline();
        let mut generator = generator();
        let datagrams: Vec<Vec<u8>> = (0..4).map(|_| generator.next_datagram()).collect();
        let expected: Vec<Vec<i16>> = datagrams
            .iter()
            .map(|d| {
                RtpPacket::parse(d)
                    .unwrap()
                    .payload
                    .iter()
                    .map(|&b| g711::ulaw_to_linear(b))
                    .collect()
            })
            .collect();

        for datagram in &datagrams {
            assert_eq!(pipeline.ingest(datagram), IngestOutcome::Buffered);
        }
        for want in &expected {
            match pipeline.release() {
                Playout::Pcm(pcm) => assert_eq!(pcm, want.as_slice()),
                other => panic!("expected pcm, got {other:?}"),
            }
        }
        assert_eq!(pipeline.release(), Playout::Waiting);
        assert_eq!(pipeline.stats().frames_played, 4);
        assert_eq!(pipeline.stats().frames_concealed, 0);
    }

    #[test]
    fn replays_reordered_and_duplicated_packets_into_the_original_order() {
        let mut pipeline = pipeline();
        let mut generator = generator();
        let clean: Vec<Vec<u8>> = (0..4).map(|_| generator.next_datagram()).collect();
        let expected: Vec<i16> = clean
            .iter()
            .map(|d| g711::ulaw_to_linear(RtpPacket::parse(d).unwrap().payload[0]))
            .collect();

        let wire = disturb(
            clean,
            &[
                Disturbance::Deliver,
                Disturbance::DelayOne,
                Disturbance::DeliverTwice,
                Disturbance::Deliver,
            ],
        );
        for datagram in &wire {
            pipeline.ingest(datagram);
        }

        let mut played = Vec::new();
        for _ in 0..expected.len() {
            match pipeline.release() {
                Playout::Pcm(pcm) => played.push(pcm[0]),
                other => panic!("expected pcm, got {other:?}"),
            }
        }
        assert_eq!(played, expected);
        assert_eq!(pipeline.jitter_stats().duplicates, 1);
        assert_eq!(pipeline.stats().frames_concealed, 0);
    }

    #[test]
    fn reorder_before_playout_starts_strands_the_earlier_packet() {
        let mut pipeline = pipeline();
        let mut generator = generator();
        let first = generator.next_datagram();
        let second = generator.next_datagram();

        assert_eq!(pipeline.ingest(&second), IngestOutcome::Buffered);
        assert_eq!(pipeline.ingest(&first), IngestOutcome::TooLate);
        assert_eq!(pipeline.jitter_stats().late_drops, 1);
    }

    #[test]
    fn conceals_a_dropped_packet_with_plc_audio_and_counts_it() {
        let mut pipeline = pipeline();
        let mut generator = generator();
        let clean: Vec<Vec<u8>> = (0..6).map(|_| generator.next_datagram()).collect();
        let wire = disturb(
            clean,
            &[
                Disturbance::Deliver,
                Disturbance::Deliver,
                Disturbance::Deliver,
                Disturbance::Deliver,
                Disturbance::Drop,
                Disturbance::Deliver,
            ],
        );
        for datagram in &wire {
            pipeline.ingest(datagram);
        }

        for _ in 0..4 {
            assert!(matches!(pipeline.release(), Playout::Pcm(_)));
        }
        match pipeline.release() {
            Playout::Concealed(pcm) => {
                assert_eq!(pcm.len(), pipeline_samples());
                assert!(
                    pcm.iter().any(|&s| s != 0),
                    "concealment must repeat audio, not write silence"
                );
            }
            other => panic!("expected concealment, got {other:?}"),
        }
        assert!(matches!(pipeline.release(), Playout::Pcm(_)));
        assert_eq!(pipeline.stats().frames_concealed, 1);
        assert_eq!(pipeline.jitter_stats().lost, 1);
    }

    fn pipeline_samples() -> usize {
        AudioFormat::pcmu_8k_20ms().samples_per_packet().unwrap() as usize
    }

    #[test]
    fn telephone_events_never_reach_the_audio_path() {
        let mut pipeline = pipeline();
        let mut generator = generator();
        let audio = generator.next_datagram();
        let digit_start = generator.next_event_datagram(TELEPHONE_EVENT_PT, [5, 0x0A, 0, 80]);
        let digit_end = generator.next_event_datagram(TELEPHONE_EVENT_PT, [5, 0x8A, 0x03, 0x20]);

        assert_eq!(pipeline.ingest(&audio), IngestOutcome::Buffered);
        assert_eq!(pipeline.ingest(&digit_start), IngestOutcome::TelephoneEvent);
        assert_eq!(
            pipeline.ingest(&digit_end),
            IngestOutcome::Dtmf(DigitPress {
                digit: '5',
                duration_ms: 100,
                rtp_timestamp: 160,
            })
        );

        assert_eq!(pipeline.stats().telephone_events, 2);
        assert_eq!(pipeline.stats().dtmf_digits, 1);
        assert_eq!(pipeline.jitter_stats().received, 3);

        assert!(matches!(pipeline.release(), Playout::Pcm(_)));
        for _ in 0..2 {
            match pipeline.release() {
                Playout::Suppressed(pcm) => assert!(pcm.iter().all(|&s| s == 0)),
                other => panic!("expected suppression, got {other:?}"),
            }
        }
        assert_eq!(pipeline.stats().frames_suppressed, 2);
        assert_eq!(pipeline.stats().frames_concealed, 0);
        assert_eq!(pipeline.jitter_stats().lost, 0);
    }

    #[test]
    fn a_dtmf_press_on_a_clean_link_reports_zero_loss() {
        let mut pipeline = pipeline();
        let mut generator = generator();
        let mut wire = vec![generator.next_datagram(), generator.next_datagram()];
        for _ in 0..3 {
            wire.push(generator.next_event_datagram(TELEPHONE_EVENT_PT, [7, 0x0A, 0x01, 0x40]));
        }
        for _ in 0..3 {
            wire.push(generator.next_event_datagram(TELEPHONE_EVENT_PT, [7, 0x8A, 0x03, 0x20]));
        }
        wire.push(generator.next_datagram());
        wire.push(generator.next_datagram());

        let mut digits = 0;
        for datagram in &wire {
            if matches!(pipeline.ingest(datagram), IngestOutcome::Dtmf(_)) {
                digits += 1;
            }
        }
        for _ in 0..wire.len() {
            pipeline.release();
        }

        assert_eq!(digits, 1);
        assert_eq!(pipeline.jitter_stats().lost, 0);
        assert_eq!(pipeline.stats().frames_concealed, 0);
        assert_eq!(pipeline.stats().frames_suppressed, 6);
        assert_eq!(pipeline.stats().frames_played, 4);
    }

    #[test]
    fn unexpected_payload_types_and_garbage_are_counted_not_decoded() {
        let mut pipeline = pipeline();
        let mut generator = generator();
        let foreign = generator.next_event_datagram(96, [0, 0, 0, 0]);

        assert_eq!(
            pipeline.ingest(&foreign),
            IngestOutcome::UnknownPayloadType(96)
        );
        assert_eq!(pipeline.ingest(&[0u8; 4]), IngestOutcome::Unparsable);
        assert_eq!(pipeline.ingest(&[]), IngestOutcome::Unparsable);

        assert_eq!(pipeline.stats().unknown_payload_type, 1);
        assert_eq!(pipeline.stats().unparsable, 2);
        assert_eq!(pipeline.jitter_stats().received, 0);
        assert_eq!(pipeline.release(), Playout::Waiting);
    }
}

#[cfg(test)]
mod impairment_matrix {
    use super::*;
    use crate::replay::{disturb, Disturbance, G711StreamGenerator};

    const TELEPHONE_EVENT_PT: u8 = 101;
    const SAMPLES: usize = 160;
    const PACKET_MICROS: u64 = 20_000;

    #[derive(Debug, Default)]
    struct Driven {
        played: u64,
        concealed: u64,
        suppressed: u64,
        waiting: u64,
        digits: u64,
        heads: Vec<i16>,
        concealed_energy: Vec<i64>,
    }

    fn pipeline_with(target_depth_packets: u16) -> StreamPipeline {
        StreamPipeline::new(
            AudioFormat::pcmu_8k_20ms(),
            target_depth_packets,
            Some(TELEPHONE_EVENT_PT),
        )
        .unwrap()
    }

    fn generator() -> G711StreamGenerator {
        G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 0x0BAD_C0DE, 4000).unwrap()
    }

    fn energy(pcm: &[i16]) -> i64 {
        pcm.iter().map(|&s| (s as i64).abs()).sum()
    }

    fn record(pipeline: &mut StreamPipeline, driven: &mut Driven) {
        match pipeline.release() {
            Playout::Pcm(pcm) => {
                driven.played += 1;
                driven.heads.push(pcm[0]);
            }
            Playout::Concealed(pcm) => {
                driven.concealed += 1;
                driven.concealed_energy.push(energy(pcm));
            }
            Playout::Suppressed(_) => driven.suppressed += 1,
            Playout::Waiting => driven.waiting += 1,
        }
    }

    fn slot_of(datagram: &[u8], first_sequence: u16) -> usize {
        let sequence = RtpPacket::parse(datagram).unwrap().sequence;
        (sequence.wrapping_sub(first_sequence) as i16).max(0) as usize
    }

    fn drive(pipeline: &mut StreamPipeline, wire: &[Vec<u8>], slots: usize) -> Driven {
        let mut driven = Driven::default();
        let first_sequence = RtpPacket::parse(&wire[0]).unwrap().sequence;
        let lag = pipeline.target_depth_packets() as usize;
        let mut arrived_slots = 0usize;
        let mut released = 0usize;
        for datagram in wire {
            if matches!(pipeline.ingest(datagram), IngestOutcome::Dtmf(_)) {
                driven.digits += 1;
            }
            arrived_slots = arrived_slots.max(slot_of(datagram, first_sequence) + 1);
            while released + lag < arrived_slots {
                record(pipeline, &mut driven);
                released += 1;
            }
        }
        while released < slots {
            record(pipeline, &mut driven);
            released += 1;
        }
        driven
    }

    fn clean_stream(packets: usize) -> (Vec<Vec<u8>>, Vec<i16>) {
        let mut generator = generator();
        let datagrams: Vec<Vec<u8>> = (0..packets).map(|_| generator.next_datagram()).collect();
        let heads = datagrams
            .iter()
            .map(|d| g711::ulaw_to_linear(RtpPacket::parse(d).unwrap().payload[0]))
            .collect();
        (datagrams, heads)
    }

    fn drop_every(packets: usize, every: usize) -> Vec<Disturbance> {
        (0..packets)
            .map(|index| {
                if index % every == every / 2 {
                    Disturbance::Drop
                } else {
                    Disturbance::Deliver
                }
            })
            .collect()
    }

    #[test]
    fn uniform_loss_is_reported_once_and_concealed_once() {
        for every in [100usize, 20] {
            let packets = 200;
            let (clean, _) = clean_stream(packets);
            let script = drop_every(packets, every);
            let dropped = script
                .iter()
                .filter(|step| **step == Disturbance::Drop)
                .count() as u64;
            let mut pipeline = pipeline_with(2);
            let driven = drive(&mut pipeline, &disturb(clean, &script), packets);

            assert_eq!(pipeline.jitter_stats().lost, dropped, "loss at 1/{every}");
            assert_eq!(driven.concealed, dropped, "concealment at 1/{every}");
            assert_eq!(
                driven.played + driven.concealed,
                packets as u64,
                "every sequence number owes exactly one frame at 1/{every}"
            );
        }
    }

    #[test]
    fn a_burst_of_loss_engages_plc_and_keeps_the_frame_clock_honest() {
        let packets = 40;
        let burst = 20..28;
        let (clean, _) = clean_stream(packets);
        let script: Vec<Disturbance> = (0..packets)
            .map(|index| {
                if burst.contains(&index) {
                    Disturbance::Drop
                } else {
                    Disturbance::Deliver
                }
            })
            .collect();
        let mut pipeline = pipeline_with(2);
        let driven = drive(&mut pipeline, &disturb(clean, &script), packets);

        assert_eq!(pipeline.jitter_stats().lost, burst.len() as u64);
        assert_eq!(driven.concealed, burst.len() as u64);
        assert_eq!(driven.played + driven.concealed, packets as u64);
        assert!(
            driven.concealed_energy[0] > 0,
            "the first concealed frame must carry repeated audio, not silence"
        );
        assert_eq!(
            driven.concealed_energy.last(),
            Some(&0),
            "a burst past 60 ms must fade to silence: {:?}",
            driven.concealed_energy
        );
    }

    #[test]
    fn reorder_inside_the_buffer_depth_costs_no_loss() {
        let packets = 60;
        let (clean, heads) = clean_stream(packets);
        let script: Vec<Disturbance> = (0..packets)
            .map(|index| {
                if index % 10 == 5 {
                    Disturbance::DelayOne
                } else {
                    Disturbance::Deliver
                }
            })
            .collect();
        let mut pipeline = pipeline_with(4);
        let driven = drive(&mut pipeline, &disturb(clean, &script), packets);

        assert_eq!(pipeline.jitter_stats().lost, 0);
        assert_eq!(pipeline.jitter_stats().late_drops, 0);
        assert_eq!(driven.played, packets as u64);
        assert_eq!(driven.heads, heads, "samples must emerge in sequence order");
    }

    #[test]
    fn reorder_beyond_the_buffer_depth_is_late_not_lost() {
        let packets = 20;
        let (clean, _) = clean_stream(packets);
        let mut wire = clean.clone();
        let straggler = wire.remove(10);
        wire.insert(17, straggler);
        let mut pipeline = pipeline_with(2);
        let driven = drive(&mut pipeline, &wire, packets);

        let jitter = pipeline.jitter_stats();
        assert_eq!(jitter.late_drops, 1);
        assert_eq!(jitter.lost, 1);
        assert_eq!(jitter.duplicates, 0);
        assert_eq!(jitter.received, wire.len() as u64);
        assert_eq!(driven.played, packets as u64 - 1);
        assert_eq!(driven.concealed, 1);
    }

    #[test]
    fn duplication_is_deduped_and_costs_no_samples() {
        let packets = 100;
        let (clean, heads) = clean_stream(packets);
        let script: Vec<Disturbance> = (0..packets)
            .map(|index| {
                if index % 20 == 10 {
                    Disturbance::DeliverTwice
                } else {
                    Disturbance::Deliver
                }
            })
            .collect();
        let duplicated = script
            .iter()
            .filter(|step| **step == Disturbance::DeliverTwice)
            .count() as u64;
        let mut pipeline = pipeline_with(2);
        let driven = drive(&mut pipeline, &disturb(clean, &script), packets);

        assert_eq!(pipeline.jitter_stats().duplicates, duplicated);
        assert_eq!(pipeline.jitter_stats().lost, 0);
        assert_eq!(driven.played, packets as u64);
        assert_eq!(driven.heads, heads);
    }

    #[test]
    fn arrival_jitter_without_loss_grows_the_cushion_and_plays_everything() {
        let packets = 300;
        let (clean, _) = clean_stream(packets);
        let swing = [0i64, 15_000, -12_000, 9_000, -15_000];
        let mut pipeline = pipeline_with(2);
        let floor = pipeline.target_depth_packets();
        let mut driven = Driven::default();
        let lag = floor as usize;
        let mut released = 0usize;
        for (index, datagram) in clean.iter().enumerate() {
            let paced = index as u64 * PACKET_MICROS;
            let arrival = (paced as i64 + swing[index % swing.len()]).max(0) as u64;
            pipeline.ingest_at(datagram, arrival);
            while released + lag < index + 1 {
                record(&mut pipeline, &mut driven);
                released += 1;
            }
        }
        while released < packets {
            record(&mut pipeline, &mut driven);
            released += 1;
        }

        assert_eq!(pipeline.jitter_stats().lost, 0);
        assert_eq!(driven.played, packets as u64);
        assert!(
            pipeline.target_depth_packets() > floor,
            "a jittery path must grow the cushion, stayed at {floor}"
        );
        assert!(
            driven.waiting <= pipeline.target_depth_packets() as u64,
            "underruns must stay bounded by the cushion, saw {}",
            driven.waiting
        );
    }

    #[test]
    fn a_silence_suppressed_talkspurt_gap_is_not_loss() {
        let mut generator = generator();
        let mut wire: Vec<Vec<u8>> = (0..6).map(|_| generator.next_datagram()).collect();
        wire.push(generator.next_comfort_noise_datagram(60));
        generator.suppress_silence(50);
        wire.extend((0..6).map(|_| generator.next_datagram()));

        let mut pipeline = pipeline_with(2);
        let driven = drive(&mut pipeline, &wire, 13);

        assert_eq!(pipeline.stats().comfort_noise, 1);
        assert_eq!(pipeline.stats().unknown_payload_type, 0);
        assert_eq!(pipeline.jitter_stats().lost, 0);
        assert_eq!(driven.suppressed, 1);
        assert_eq!(driven.played, 12);
    }

    #[test]
    fn a_dropped_comfort_noise_packet_is_absorbed_as_silence_not_loss() {
        let mut generator = generator();
        let mut wire: Vec<Vec<u8>> = (0..6).map(|_| generator.next_datagram()).collect();
        let _dropped_in_transit = generator.next_comfort_noise_datagram(60);
        generator.suppress_silence(50);
        wire.extend((0..6).map(|_| generator.next_datagram()));

        let mut pipeline = pipeline_with(2);
        let driven = drive(&mut pipeline, &wire, 13);

        assert_eq!(pipeline.jitter_stats().silence_gaps, 1);
        assert_eq!(pipeline.jitter_stats().lost, 0);
        assert_eq!(pipeline.stats().frames_concealed, 0);
        assert_eq!(driven.suppressed, 1);
        assert_eq!(driven.played, 12);
    }

    #[test]
    fn a_dtmf_press_under_loss_still_reports_one_digit_and_only_audio_loss() {
        let mut generator = generator();
        let mut wire: Vec<Vec<u8>> = Vec::new();
        for index in 0..20 {
            if index == 8 {
                generator.skip_one();
                continue;
            }
            wire.push(generator.next_datagram());
            if index == 12 {
                for _ in 0..3 {
                    wire.push(
                        generator.next_event_datagram(TELEPHONE_EVENT_PT, [7, 0x0A, 0x01, 0x40]),
                    );
                }
                for _ in 0..3 {
                    wire.push(
                        generator.next_event_datagram(TELEPHONE_EVENT_PT, [7, 0x8A, 0x03, 0x20]),
                    );
                }
            }
        }

        let mut pipeline = pipeline_with(2);
        let driven = drive(&mut pipeline, &wire, 26);

        assert_eq!(driven.digits, 1);
        assert_eq!(pipeline.jitter_stats().lost, 1);
        assert_eq!(driven.suppressed, 6);
        assert_eq!(driven.concealed, 1);
        assert_eq!(driven.played, 19);
    }

    #[test]
    fn a_new_ssrc_at_a_nearby_sequence_restarts_instead_of_dropping_late() {
        let mut pipeline = pipeline_with(2);
        let mut first = G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 0x1111, 500).unwrap();
        let mut driven = Driven::default();
        for _ in 0..6 {
            pipeline.ingest(&first.next_datagram());
            record(&mut pipeline, &mut driven);
        }

        let mut second =
            G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 0x2222, 501).unwrap();
        assert_eq!(
            pipeline.ingest(&second.next_datagram()),
            IngestOutcome::Resynchronized
        );
        for _ in 0..5 {
            pipeline.ingest(&second.next_datagram());
            record(&mut pipeline, &mut driven);
        }
        for _ in 0..8 {
            record(&mut pipeline, &mut driven);
        }

        let jitter = pipeline.jitter_stats();
        assert_eq!(jitter.late_drops, 0);
        assert_eq!(jitter.resets, 1);
        assert_eq!(jitter.lost, 0);
        assert_eq!(pipeline.last_audio_ssrc(), Some(0x2222));
        assert_eq!(
            driven.played, 11,
            "a restart costs the cushion the old sender had already filled"
        );
    }

    #[test]
    fn one_frame_of_pcm_is_always_samples_per_packet_long() {
        let (clean, _) = clean_stream(4);
        let mut pipeline = pipeline_with(2);
        for datagram in &clean {
            pipeline.ingest(datagram);
        }
        for _ in 0..4 {
            match pipeline.release() {
                Playout::Pcm(pcm) => assert_eq!(pcm.len(), SAMPLES),
                other => panic!("expected pcm, got {other:?}"),
            }
        }
    }
}

#[cfg(test)]
mod ssrc_identity_tests {
    use super::*;
    use crate::replay::G711StreamGenerator;

    #[test]
    fn only_accepted_audio_names_the_stream_ssrc() {
        let mut pipeline = StreamPipeline::new(AudioFormat::pcmu_8k_20ms(), 2, Some(101)).unwrap();
        assert_eq!(pipeline.last_audio_ssrc(), None);

        pipeline.ingest(b"garbage");
        assert_eq!(pipeline.last_audio_ssrc(), None);

        let mut wrong_pt = G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 0xAAAA_0001, 1)
            .unwrap()
            .next_datagram();
        wrong_pt[1] = 96;
        assert_eq!(
            pipeline.ingest(&wrong_pt),
            IngestOutcome::UnknownPayloadType(96)
        );
        assert_eq!(pipeline.last_audio_ssrc(), None);

        let mut speech =
            G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 0xBBBB_0002, 2).unwrap();
        pipeline.ingest(&speech.next_datagram());
        assert_eq!(pipeline.last_audio_ssrc(), Some(0xBBBB_0002));
    }
}
