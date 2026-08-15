use crate::dtmf::DtmfDetector;
use crate::frame::{AudioFormat, Encoding};
use crate::g711;
use crate::jitter::{self, JitterBuffer, PopOutcome, PushOutcome, MAX_PAYLOAD};
use crate::rtp::RtpPacket;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PipelineError {
    #[error("encoding {0:?} is not decodable by this pipeline; G.711 only until Opus lands")]
    UnsupportedEncoding(Encoding),
    #[error("format has no usable ptime/sample rate")]
    UnusableFormat,
    #[error("{samples} samples per packet exceeds the {max}-sample slot size")]
    PacketTooLong { samples: usize, max: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    Buffered,
    Duplicate,
    TooLate,
    Oversized,
    Resynchronized,
    Dtmf(char),
    TelephoneEvent,
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
    pub unparsable: u64,
    pub unknown_payload_type: u64,
    pub companded: u64,
    pub telephone_events: u64,
    pub dtmf_digits: u64,
    pub frames_played: u64,
    pub frames_concealed: u64,
    pub frames_suppressed: u64,
}

fn companion_payload_type(encoding: Encoding) -> Option<u8> {
    match encoding {
        Encoding::Pcmu => Encoding::Pcma.static_payload_type(),
        Encoding::Pcma => Encoding::Pcmu.static_payload_type(),
        _ => None,
    }
}

pub struct StreamPipeline {
    audio_payload_type: u8,
    companding_payload_type: Option<u8>,
    telephone_event_payload_type: Option<u8>,
    ulaw: bool,
    samples_per_packet: usize,
    jitter: JitterBuffer,
    dtmf: DtmfDetector,
    pcm: [i16; MAX_PAYLOAD],
    stats: PipelineStats,
}

impl StreamPipeline {
    pub fn new(
        format: AudioFormat,
        target_depth_packets: u16,
        telephone_event_payload_type: Option<u8>,
    ) -> Result<Self, PipelineError> {
        let ulaw = match format.encoding {
            Encoding::Pcmu => true,
            Encoding::Pcma => false,
            other => return Err(PipelineError::UnsupportedEncoding(other)),
        };
        let audio_payload_type = format
            .encoding
            .static_payload_type()
            .ok_or(PipelineError::UnsupportedEncoding(format.encoding))?;
        let samples_per_packet = format
            .samples_per_packet()
            .filter(|samples| *samples > 0)
            .ok_or(PipelineError::UnusableFormat)? as usize;
        if samples_per_packet > MAX_PAYLOAD {
            return Err(PipelineError::PacketTooLong {
                samples: samples_per_packet,
                max: MAX_PAYLOAD,
            });
        }
        Ok(StreamPipeline {
            audio_payload_type,
            companding_payload_type: companion_payload_type(format.encoding),
            telephone_event_payload_type,
            ulaw,
            samples_per_packet,
            jitter: JitterBuffer::new(target_depth_packets),
            dtmf: DtmfDetector::new(),
            pcm: [0; MAX_PAYLOAD],
            stats: PipelineStats::default(),
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

    pub fn ingest(&mut self, datagram: &[u8]) -> IngestOutcome {
        let Ok(packet) = RtpPacket::parse(datagram) else {
            self.stats.unparsable += 1;
            return IngestOutcome::Unparsable;
        };
        if Some(packet.payload_type) == self.telephone_event_payload_type {
            self.stats.telephone_events += 1;
            self.jitter.account(packet.sequence);
            return match self.dtmf.push(packet.timestamp, packet.payload) {
                Some(digit) => {
                    self.stats.dtmf_digits += 1;
                    IngestOutcome::Dtmf(digit)
                }
                None => IngestOutcome::TelephoneEvent,
            };
        }
        let mut companded = [0u8; MAX_PAYLOAD];
        let payload = if packet.payload_type == self.audio_payload_type {
            packet.payload
        } else if Some(packet.payload_type) == self.companding_payload_type
            && packet.payload.len() <= MAX_PAYLOAD
        {
            for (out, byte) in companded.iter_mut().zip(packet.payload) {
                *out = if self.ulaw {
                    g711::linear_to_ulaw(g711::alaw_to_linear(*byte))
                } else {
                    g711::linear_to_alaw(g711::ulaw_to_linear(*byte))
                };
            }
            self.stats.companded += 1;
            &companded[..packet.payload.len()]
        } else {
            self.stats.unknown_payload_type += 1;
            return IngestOutcome::UnknownPayloadType(packet.payload_type);
        };
        match self.jitter.push(packet.sequence, payload) {
            PushOutcome::Buffered => IngestOutcome::Buffered,
            PushOutcome::Duplicate => IngestOutcome::Duplicate,
            PushOutcome::TooLate => IngestOutcome::TooLate,
            PushOutcome::TooBig => IngestOutcome::Oversized,
            PushOutcome::Reset => IngestOutcome::Resynchronized,
        }
    }

    pub fn release(&mut self) -> Playout<'_> {
        let Self {
            jitter,
            pcm,
            ulaw,
            samples_per_packet,
            stats,
            ..
        } = self;
        match jitter.pop() {
            PopOutcome::Packet(payload) => {
                let decoded = g711::decode_into(*ulaw, payload, pcm);
                stats.frames_played += 1;
                Playout::Pcm(&pcm[..decoded])
            }
            PopOutcome::Accounted => {
                let suppressed = *samples_per_packet;
                pcm[..suppressed].fill(0);
                stats.frames_suppressed += 1;
                Playout::Suppressed(&pcm[..suppressed])
            }
            PopOutcome::Lost => {
                let concealed = *samples_per_packet;
                pcm[..concealed].fill(0);
                stats.frames_concealed += 1;
                Playout::Concealed(&pcm[..concealed])
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
                    ptime_ms: 100,
                    ..AudioFormat::pcmu_8k_20ms()
                },
                2,
                None
            )
            .err(),
            Some(PipelineError::PacketTooLong {
                samples: 800,
                max: MAX_PAYLOAD
            })
        );
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
    fn conceals_a_dropped_packet_with_silence_and_counts_it() {
        let mut pipeline = pipeline();
        let mut generator = generator();
        let clean: Vec<Vec<u8>> = (0..3).map(|_| generator.next_datagram()).collect();
        let wire = disturb(
            clean,
            &[
                Disturbance::Deliver,
                Disturbance::Drop,
                Disturbance::Deliver,
            ],
        );
        for datagram in &wire {
            pipeline.ingest(datagram);
        }

        assert!(matches!(pipeline.release(), Playout::Pcm(_)));
        match pipeline.release() {
            Playout::Concealed(pcm) => {
                assert_eq!(pcm.len(), pipeline_samples());
                assert!(pcm.iter().all(|&s| s == 0));
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
        assert_eq!(pipeline.ingest(&digit_end), IngestOutcome::Dtmf('5'));

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
