use crate::encode::{ConsumerEncoder, EncodeError};
use crate::frame::{AudioFormat, Encoding};
use crate::rtp::{RtpPacket, MIN_HEADER_LEN};
use std::time::Duration;
use thiserror::Error;

const RESAMPLER_SLACK_SAMPLES: usize = 8;

#[derive(Debug, Error)]
pub enum PacerError {
    #[error("egress encoder: {0}")]
    Encode(#[from] EncodeError),
    #[error("payload type {0} does not fit the 7-bit RTP field")]
    PayloadType(u8),
    #[error("the egress queue must hold at least one frame")]
    EmptyQueue,
    #[error("sample rate and ptime must be non-zero")]
    ZeroFrame,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnderrunPolicy {
    Silence,
    Suppress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacerConfig {
    pub source: AudioFormat,
    pub wire: AudioFormat,
    pub payload_type: u8,
    pub ssrc: u32,
    pub initial_sequence: u16,
    pub initial_timestamp: u32,
    pub queue_frames: usize,
    pub underrun: UnderrunPolicy,
}

impl PacerConfig {
    pub fn g711_egress(wire: AudioFormat, ssrc: u32) -> Option<Self> {
        Some(PacerConfig {
            source: wire,
            wire,
            payload_type: wire.encoding.static_payload_type()?,
            ssrc,
            initial_sequence: 0,
            initial_timestamp: 0,
            queue_frames: 10,
            underrun: UnderrunPolicy::Silence,
        })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PacerStats {
    pub pushed_samples: u64,
    pub dropped_samples: u64,
    pub flushed_samples: u64,
    pub packets_emitted: u64,
    pub silence_frames: u64,
    pub partial_frames: u64,
    pub suppressed_frames: u64,
    pub marker_packets: u64,
    pub late_ticks: u64,
    pub encode_errors: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Queued,
    QueuedAfterDrop(usize),
}

#[derive(Debug, PartialEq, Eq)]
pub struct PacedPacket<'a> {
    pub datagram: &'a [u8],
    pub marker: bool,
    pub sequence: u16,
    pub timestamp: u32,
    pub silence: bool,
}

struct SampleQueue {
    samples: Vec<i16>,
    read: usize,
    len: usize,
}

impl SampleQueue {
    fn with_capacity(capacity: usize) -> Self {
        SampleQueue {
            samples: vec![0; capacity],
            read: 0,
            len: 0,
        }
    }

    fn capacity(&self) -> usize {
        self.samples.len()
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, pcm: &[i16]) -> usize {
        let capacity = self.samples.len();
        if pcm.len() >= capacity {
            let dropped = self.len + (pcm.len() - capacity);
            self.samples.copy_from_slice(&pcm[pcm.len() - capacity..]);
            self.read = 0;
            self.len = capacity;
            return dropped;
        }
        let mut dropped = 0;
        let free = capacity - self.len;
        if pcm.len() > free {
            dropped = pcm.len() - free;
            self.read = (self.read + dropped) % capacity;
            self.len -= dropped;
        }
        let write = (self.read + self.len) % capacity;
        let first = (capacity - write).min(pcm.len());
        self.samples[write..write + first].copy_from_slice(&pcm[..first]);
        if first < pcm.len() {
            self.samples[..pcm.len() - first].copy_from_slice(&pcm[first..]);
        }
        self.len += pcm.len();
        dropped
    }

    fn take_into(&mut self, out: &mut [i16]) -> usize {
        let capacity = self.samples.len();
        let take = self.len.min(out.len());
        let first = (capacity - self.read).min(take);
        out[..first].copy_from_slice(&self.samples[self.read..self.read + first]);
        if first < take {
            out[first..take].copy_from_slice(&self.samples[..take - first]);
        }
        out[take..].fill(0);
        self.read = (self.read + take) % capacity;
        self.len -= take;
        take
    }

    fn clear(&mut self) -> usize {
        let discarded = self.len;
        self.read = 0;
        self.len = 0;
        discarded
    }
}

pub struct PlayoutPacer {
    encoder: ConsumerEncoder,
    queue: SampleQueue,
    pcm: Vec<i16>,
    datagram: Vec<u8>,
    ptime: Duration,
    next_deadline: Option<Duration>,
    payload_type: u8,
    ssrc: u32,
    sequence: u16,
    timestamp: u32,
    timestamp_increment: u32,
    underrun: UnderrunPolicy,
    started: bool,
    in_silence: bool,
    stats: PacerStats,
}

fn bytes_per_sample(encoding: Encoding) -> usize {
    match encoding {
        Encoding::Pcmu | Encoding::Pcma => 1,
        Encoding::L16 | Encoding::Opus => 2,
    }
}

impl PlayoutPacer {
    pub fn new(config: PacerConfig) -> Result<Self, PacerError> {
        if config.payload_type > 0x7F {
            return Err(PacerError::PayloadType(config.payload_type));
        }
        if config.queue_frames == 0 {
            return Err(PacerError::EmptyQueue);
        }
        let source_frame = config
            .source
            .samples_per_packet()
            .ok_or(PacerError::ZeroFrame)? as usize;
        let wire_frame = config
            .wire
            .samples_per_packet()
            .ok_or(PacerError::ZeroFrame)? as usize;
        let encoder = ConsumerEncoder::new(config.source, config.wire)?;
        let payload_capacity =
            (wire_frame + RESAMPLER_SLACK_SAMPLES) * bytes_per_sample(config.wire.encoding);
        Ok(PlayoutPacer {
            encoder,
            queue: SampleQueue::with_capacity(source_frame * config.queue_frames),
            pcm: vec![0; source_frame],
            datagram: vec![0; MIN_HEADER_LEN + payload_capacity],
            ptime: Duration::from_millis(u64::from(config.wire.ptime_ms)),
            next_deadline: None,
            payload_type: config.payload_type,
            ssrc: config.ssrc,
            sequence: config.initial_sequence,
            timestamp: config.initial_timestamp,
            timestamp_increment: wire_frame as u32,
            underrun: config.underrun,
            started: false,
            in_silence: true,
            stats: PacerStats::default(),
        })
    }

    pub fn stats(&self) -> PacerStats {
        self.stats
    }

    pub fn queued_samples(&self) -> usize {
        self.queue.len()
    }

    pub fn queue_capacity_samples(&self) -> usize {
        self.queue.capacity()
    }

    pub fn ssrc(&self) -> u32 {
        self.ssrc
    }

    pub fn push(&mut self, pcm: &[i16]) -> EnqueueOutcome {
        self.stats.pushed_samples += pcm.len() as u64;
        let dropped = self.queue.push(pcm);
        if dropped == 0 {
            EnqueueOutcome::Queued
        } else {
            self.stats.dropped_samples += dropped as u64;
            EnqueueOutcome::QueuedAfterDrop(dropped)
        }
    }

    pub fn clear(&mut self) -> usize {
        let discarded = self.queue.clear();
        self.stats.flushed_samples += discarded as u64;
        discarded
    }

    pub fn tick(&mut self, now: Duration) -> Option<PacedPacket<'_>> {
        let deadline = match self.next_deadline {
            None => now,
            Some(deadline) => {
                if now < deadline {
                    return None;
                }
                deadline
            }
        };
        let next = deadline + self.ptime;
        self.next_deadline = Some(next);
        if now >= next {
            self.stats.late_ticks += 1;
        }

        let filled = self.queue.take_into(&mut self.pcm);
        if filled == 0 && self.underrun == UnderrunPolicy::Suppress {
            self.stats.suppressed_frames += 1;
            self.timestamp = self.timestamp.wrapping_add(self.timestamp_increment);
            self.in_silence = true;
            return None;
        }

        let silence = filled == 0;
        if silence {
            self.stats.silence_frames += 1;
        } else if filled < self.pcm.len() {
            self.stats.partial_frames += 1;
        }
        let marker = !self.started || (self.in_silence && !silence);
        let sequence = self.sequence;
        let timestamp = self.timestamp;

        let written = match self.encoder.encode(&self.pcm) {
            Ok(payload) => {
                let packet = RtpPacket {
                    marker,
                    payload_type: self.payload_type,
                    sequence,
                    timestamp,
                    ssrc: self.ssrc,
                    payload,
                };
                packet.serialize(&mut self.datagram).unwrap_or(0)
            }
            Err(_) => 0,
        };

        self.started = true;
        self.in_silence = silence;
        self.sequence = self.sequence.wrapping_add(1);
        self.timestamp = self.timestamp.wrapping_add(self.timestamp_increment);

        if written == 0 {
            self.stats.encode_errors += 1;
            return None;
        }
        self.stats.packets_emitted += 1;
        if marker {
            self.stats.marker_packets += 1;
        }
        Some(PacedPacket {
            datagram: &self.datagram[..written],
            marker,
            sequence,
            timestamp,
            silence,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::g711;

    const L16_PAYLOAD_TYPE: u8 = 96;

    fn l16_8k() -> AudioFormat {
        AudioFormat {
            encoding: Encoding::L16,
            sample_rate_hz: 8000,
            channels: 1,
            ptime_ms: 20,
        }
    }

    fn config(queue_frames: usize, underrun: UnderrunPolicy) -> PacerConfig {
        PacerConfig {
            source: l16_8k(),
            wire: l16_8k(),
            payload_type: L16_PAYLOAD_TYPE,
            ssrc: 0x1234_5678,
            initial_sequence: 100,
            initial_timestamp: 7000,
            queue_frames,
            underrun,
        }
    }

    fn pacer(queue_frames: usize) -> PlayoutPacer {
        PlayoutPacer::new(config(queue_frames, UnderrunPolicy::Silence)).unwrap()
    }

    fn frame(value: i16) -> Vec<i16> {
        vec![value; 160]
    }

    fn samples(payload: &[u8]) -> Vec<i16> {
        payload
            .chunks_exact(2)
            .map(|pair| i16::from_le_bytes([pair[0], pair[1]]))
            .collect()
    }

    #[test]
    fn ticks_emit_one_packet_per_ptime_and_nothing_in_between() {
        let mut pacer = pacer(40);
        for value in 0..20 {
            pacer.push(&frame(value));
        }
        let mut emitted = Vec::new();
        for ms in (0..200).step_by(5) {
            if pacer.tick(Duration::from_millis(ms)).is_some() {
                emitted.push(ms);
            }
        }
        assert_eq!(emitted, (0..200).step_by(20).collect::<Vec<u64>>());
        assert_eq!(pacer.stats().packets_emitted, 10);
        assert_eq!(pacer.stats().late_ticks, 0);
        assert_eq!(pacer.stats().silence_frames, 0);
    }

    #[test]
    fn a_caller_that_wakes_up_late_is_counted_and_catches_up_one_packet_per_call() {
        let mut pacer = pacer(40);
        for value in 0..10 {
            pacer.push(&frame(value));
        }
        assert!(pacer.tick(Duration::from_millis(0)).is_some());
        let overslept = Duration::from_millis(100);
        for _ in 0..5 {
            assert!(pacer.tick(overslept).is_some());
        }
        assert_eq!(pacer.stats().packets_emitted, 6);
        assert_eq!(pacer.stats().late_ticks, 4);
        assert!(pacer.tick(overslept).is_none());
        assert!(pacer.tick(Duration::from_millis(120)).is_some());
    }

    #[test]
    fn underrun_emits_counted_silence_and_the_next_voice_packet_carries_the_marker() {
        let mut pacer = pacer(4);
        pacer.push(&frame(1000));
        let first = pacer.tick(Duration::from_millis(0)).unwrap();
        assert!(first.marker);
        assert!(!first.silence);

        for ms in [20u64, 40] {
            let quiet = pacer.tick(Duration::from_millis(ms)).unwrap();
            assert!(quiet.silence);
            assert!(!quiet.marker);
            let payload = RtpPacket::parse(quiet.datagram).unwrap().payload;
            assert!(samples(payload).iter().all(|sample| *sample == 0));
        }
        assert_eq!(pacer.stats().silence_frames, 2);

        pacer.push(&frame(2000));
        let resumed = pacer.tick(Duration::from_millis(60)).unwrap();
        assert!(resumed.marker);
        assert!(!resumed.silence);
        assert_eq!(pacer.stats().marker_packets, 2);
        assert_eq!(pacer.stats().packets_emitted, 4);
    }

    #[test]
    fn the_wire_header_matches_the_reported_state_and_the_timestamp_never_skips() {
        let mut pacer = pacer(20);
        for value in 0..4 {
            pacer.push(&frame(value));
        }
        let mut expected_sequence = 100u16;
        let mut expected_timestamp = 7000u32;
        for ms in (0..200).step_by(20) {
            let packet = pacer.tick(Duration::from_millis(ms)).unwrap();
            assert_eq!(packet.sequence, expected_sequence);
            assert_eq!(packet.timestamp, expected_timestamp);
            let wire = RtpPacket::parse(packet.datagram).unwrap();
            assert_eq!(wire.sequence, expected_sequence);
            assert_eq!(wire.timestamp, expected_timestamp);
            assert_eq!(wire.ssrc, 0x1234_5678);
            assert_eq!(wire.payload_type, L16_PAYLOAD_TYPE);
            assert_eq!(wire.marker, packet.marker);
            assert_eq!(wire.payload.len(), 320);
            expected_sequence = expected_sequence.wrapping_add(1);
            expected_timestamp = expected_timestamp.wrapping_add(160);
        }
        assert_eq!(pacer.stats().silence_frames, 6);
    }

    #[test]
    fn suppressed_silence_advances_the_timestamp_but_not_the_sequence() {
        let mut pacer = PlayoutPacer::new(config(4, UnderrunPolicy::Suppress)).unwrap();
        pacer.push(&frame(500));
        let first = pacer.tick(Duration::from_millis(0)).unwrap();
        assert_eq!(first.timestamp, 7000);
        assert_eq!(first.sequence, 100);

        for ms in [20u64, 40] {
            assert!(pacer.tick(Duration::from_millis(ms)).is_none());
        }
        assert_eq!(pacer.stats().suppressed_frames, 2);

        pacer.push(&frame(600));
        let resumed = pacer.tick(Duration::from_millis(60)).unwrap();
        assert!(resumed.marker);
        assert_eq!(resumed.sequence, 101);
        assert_eq!(resumed.timestamp, 7000 + 3 * 160);
    }

    #[test]
    fn a_full_queue_drops_the_oldest_audio_and_keeps_the_newest() {
        let mut pacer = pacer(2);
        assert_eq!(pacer.push(&frame(100)), EnqueueOutcome::Queued);
        assert_eq!(pacer.push(&frame(200)), EnqueueOutcome::Queued);
        assert_eq!(
            pacer.push(&frame(300)),
            EnqueueOutcome::QueuedAfterDrop(160)
        );
        assert_eq!(
            pacer.push(&frame(400)),
            EnqueueOutcome::QueuedAfterDrop(160)
        );
        assert_eq!(
            pacer.push(&frame(500)),
            EnqueueOutcome::QueuedAfterDrop(160)
        );
        assert_eq!(pacer.stats().dropped_samples, 480);
        assert_eq!(pacer.queued_samples(), 320);

        let kept = pacer.tick(Duration::from_millis(0)).unwrap();
        assert!(samples(RtpPacket::parse(kept.datagram).unwrap().payload)
            .iter()
            .all(|sample| *sample == 400));
        let kept = pacer.tick(Duration::from_millis(20)).unwrap();
        assert!(samples(RtpPacket::parse(kept.datagram).unwrap().payload)
            .iter()
            .all(|sample| *sample == 500));
        assert!(pacer.tick(Duration::from_millis(40)).unwrap().silence);
    }

    #[test]
    fn a_push_larger_than_the_queue_keeps_only_its_tail() {
        let mut pacer = pacer(2);
        let mut long = frame(11);
        long.extend_from_slice(&frame(22));
        long.extend_from_slice(&frame(33));
        assert_eq!(pacer.push(&long), EnqueueOutcome::QueuedAfterDrop(160));
        let kept = pacer.tick(Duration::from_millis(0)).unwrap();
        assert!(samples(RtpPacket::parse(kept.datagram).unwrap().payload)
            .iter()
            .all(|sample| *sample == 22));
    }

    #[test]
    fn ten_thousand_ticks_do_not_grow_the_internal_buffers() {
        let mut pacer = pacer(8);
        let before = (
            pacer.queue.samples.capacity(),
            pacer.pcm.capacity(),
            pacer.datagram.capacity(),
        );
        let mut now = Duration::from_millis(0);
        for tick in 0..10_000u64 {
            if (2_000..8_000).contains(&tick) {
                pacer.push(&frame(1234));
                pacer.push(&frame(4321));
            }
            assert!(pacer.tick(now).is_some());
            now += Duration::from_millis(20);
        }
        let after = (
            pacer.queue.samples.capacity(),
            pacer.pcm.capacity(),
            pacer.datagram.capacity(),
        );
        assert_eq!(before, after);
        assert_eq!(pacer.stats().packets_emitted, 10_000);
        assert!(pacer.stats().dropped_samples > 0);
        assert!(pacer.stats().silence_frames > 0);
        assert_eq!(pacer.queue_capacity_samples(), 8 * 160);
    }

    #[test]
    fn clear_flushes_the_queue_so_the_next_tick_is_silence() {
        let mut pacer = pacer(4);
        for value in 0..3 {
            pacer.push(&frame(value));
        }
        assert!(!pacer.tick(Duration::from_millis(0)).unwrap().silence);
        assert_eq!(pacer.clear(), 320);
        assert!(pacer.tick(Duration::from_millis(20)).unwrap().silence);
        assert_eq!(pacer.stats().flushed_samples, 320);
        assert_eq!(pacer.queued_samples(), 0);
    }

    #[test]
    fn a_partial_frame_is_padded_and_counted() {
        let mut pacer = pacer(4);
        pacer.push(&[777i16; 80]);
        let packet = pacer.tick(Duration::from_millis(0)).unwrap();
        let pcm = samples(RtpPacket::parse(packet.datagram).unwrap().payload);
        assert_eq!(pcm.len(), 160);
        assert!(pcm[..80].iter().all(|sample| *sample == 777));
        assert!(pcm[80..].iter().all(|sample| *sample == 0));
        assert_eq!(pacer.stats().partial_frames, 1);
        assert_eq!(pacer.stats().silence_frames, 0);
    }

    #[test]
    fn the_sequence_number_wraps_without_panicking() {
        let mut pacer = PlayoutPacer::new(PacerConfig {
            initial_sequence: u16::MAX,
            initial_timestamp: u32::MAX - 100,
            ..config(2, UnderrunPolicy::Silence)
        })
        .unwrap();
        assert_eq!(
            pacer.tick(Duration::from_millis(0)).unwrap().sequence,
            u16::MAX
        );
        let wrapped = pacer.tick(Duration::from_millis(20)).unwrap();
        assert_eq!(wrapped.sequence, 0);
        assert_eq!(wrapped.timestamp, 59);
    }

    #[test]
    fn the_g711_helper_paces_ulaw_silence_at_the_static_payload_type() {
        let config = PacerConfig::g711_egress(AudioFormat::pcmu_8k_20ms(), 42).unwrap();
        assert_eq!(config.payload_type, 0);
        let mut pacer = PlayoutPacer::new(config).unwrap();
        let packet = pacer.tick(Duration::from_millis(0)).unwrap();
        let wire = RtpPacket::parse(packet.datagram).unwrap();
        assert_eq!(wire.payload_type, 0);
        assert_eq!(wire.payload.len(), 160);
        assert!(wire
            .payload
            .iter()
            .all(|byte| *byte == g711::linear_to_ulaw(0)));
        assert!(packet.silence);
        assert!(packet.marker);
    }

    #[test]
    fn a_wideband_queue_is_resampled_down_to_the_g711_wire_frame() {
        let mut pacer = PlayoutPacer::new(PacerConfig {
            source: AudioFormat::l16_16k_20ms(),
            wire: AudioFormat::pcmu_8k_20ms(),
            payload_type: 0,
            ssrc: 9,
            initial_sequence: 0,
            initial_timestamp: 0,
            queue_frames: 4,
            underrun: UnderrunPolicy::Silence,
        })
        .unwrap();
        assert_eq!(pacer.queue_capacity_samples(), 4 * 320);
        pacer.push(&[3000i16; 320]);
        let packet = pacer.tick(Duration::from_millis(0)).unwrap();
        assert_eq!(
            RtpPacket::parse(packet.datagram).unwrap().payload.len(),
            160
        );
        assert_eq!(packet.timestamp, 0);
        assert_eq!(
            pacer.tick(Duration::from_millis(20)).unwrap().timestamp,
            160
        );
        assert_eq!(pacer.stats().encode_errors, 0);
    }

    #[test]
    fn impossible_configurations_are_refused_by_name() {
        assert!(matches!(
            PlayoutPacer::new(PacerConfig {
                payload_type: 200,
                ..config(2, UnderrunPolicy::Silence)
            }),
            Err(PacerError::PayloadType(200))
        ));
        assert!(matches!(
            PlayoutPacer::new(config(0, UnderrunPolicy::Silence)),
            Err(PacerError::EmptyQueue)
        ));
        assert!(matches!(
            PlayoutPacer::new(PacerConfig {
                source: AudioFormat {
                    ptime_ms: 0,
                    ..l16_8k()
                },
                ..config(2, UnderrunPolicy::Silence)
            }),
            Err(PacerError::ZeroFrame)
        ));
        let opus_wire = PacerConfig {
            wire: AudioFormat {
                encoding: Encoding::Opus,
                sample_rate_hz: 48000,
                channels: 1,
                ptime_ms: 20,
            },
            ..config(2, UnderrunPolicy::Silence)
        };
        assert!(matches!(
            PlayoutPacer::new(opus_wire),
            Err(PacerError::Encode(EncodeError::Unsupported { .. }))
        ));
        assert!(PacerConfig::g711_egress(AudioFormat::l16_16k_20ms(), 1).is_none());
    }
}
