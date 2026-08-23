use crossbeam_queue::ArrayQueue;
use media_core::{
    AudioFormat, EnqueueOutcome, PacerConfig, PacerError, PacerStats, PlayoutPacer, UnderrunPolicy,
};
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub const EGRESS_QUEUE_CHUNKS: usize = 64;
pub const EGRESS_CHUNK_MS: u32 = 100;
pub const EGRESS_QUEUE_FRAMES: usize = 25;
const PREBUFFER_FRAMES: usize = 2;
const MAX_CHUNKS_PER_TICK: usize = 4;

#[derive(Default)]
pub struct InlineEgressShared {
    pub chunks_queued: AtomicU64,
    pub pushed_samples: AtomicU64,
    pub drained_samples: AtomicU64,
    pub chunks_refused: AtomicU64,
    pub clears: AtomicU64,
    pub cleared_samples: AtomicU64,
    pub dropped_samples: AtomicU64,
    pub datagrams_sent: AtomicU64,
    pub send_errors: AtomicU64,
    pub silence_frames: AtomicU64,
    pub late_ticks: AtomicU64,
    pub encode_errors: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct InlineEgressTotals {
    pub chunks_queued: u64,
    pub pushed_samples: u64,
    pub drained_samples: u64,
    pub chunks_refused: u64,
    pub clears: u64,
    pub cleared_samples: u64,
    pub dropped_samples: u64,
    pub datagrams_sent: u64,
    pub send_errors: u64,
    pub silence_frames: u64,
    pub late_ticks: u64,
    pub encode_errors: u64,
}

impl InlineEgressTotals {
    pub fn add_shared(&mut self, shared: &InlineEgressShared) {
        let read = |value: &AtomicU64| value.load(Ordering::Relaxed);
        self.chunks_queued += read(&shared.chunks_queued);
        self.pushed_samples += read(&shared.pushed_samples);
        self.drained_samples += read(&shared.drained_samples);
        self.chunks_refused += read(&shared.chunks_refused);
        self.clears += read(&shared.clears);
        self.cleared_samples += read(&shared.cleared_samples);
        self.dropped_samples += read(&shared.dropped_samples);
        self.datagrams_sent += read(&shared.datagrams_sent);
        self.send_errors += read(&shared.send_errors);
        self.silence_frames += read(&shared.silence_frames);
        self.late_ticks += read(&shared.late_ticks);
        self.encode_errors += read(&shared.encode_errors);
    }
}

#[derive(Clone)]
pub struct InlineEgressHandle {
    format: AudioFormat,
    queue: Arc<ArrayQueue<Vec<i16>>>,
    flush: Arc<AtomicBool>,
    shared: Arc<InlineEgressShared>,
}

impl InlineEgressHandle {
    pub fn format(&self) -> AudioFormat {
        self.format
    }

    pub fn push(&self, pcm: Vec<i16>) -> bool {
        let samples = pcm.len() as u64;
        match self.queue.push(pcm) {
            Ok(()) => {
                self.shared.chunks_queued.fetch_add(1, Ordering::Relaxed);
                self.shared
                    .pushed_samples
                    .fetch_add(samples, Ordering::Relaxed);
                true
            }
            Err(_) => {
                self.shared.chunks_refused.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    pub fn clear(&self) {
        self.flush.store(true, Ordering::Relaxed);
    }

    pub fn watermark(&self) -> u64 {
        self.shared.pushed_samples.load(Ordering::Relaxed)
    }

    pub fn drained_samples(&self) -> u64 {
        self.shared.drained_samples.load(Ordering::Relaxed)
    }

    pub fn free_chunks(&self) -> usize {
        self.queue.capacity().saturating_sub(self.queue.len())
    }

    pub fn shared(&self) -> Arc<InlineEgressShared> {
        Arc::clone(&self.shared)
    }
}

impl control_api::InlineEgressSink for InlineEgressHandle {
    fn egress_format(&self) -> AudioFormat {
        self.format
    }

    fn push_pcm(&self, pcm: Vec<i16>) -> bool {
        self.push(pcm)
    }

    fn flush(&self) {
        self.clear()
    }

    fn pushed_watermark(&self) -> u64 {
        self.watermark()
    }

    fn drained_watermark(&self) -> u64 {
        self.drained_samples()
    }
}

pub struct InlineEgress {
    socket: UdpSocket,
    peer: SocketAddr,
    frame_samples: usize,
    pacer: PlayoutPacer,
    queue: Arc<ArrayQueue<Vec<i16>>>,
    flush: Arc<AtomicBool>,
    shared: Arc<InlineEgressShared>,
    epoch: Instant,
    discarded_from_queue: u64,
}

impl InlineEgress {
    pub fn bind(
        socket: UdpSocket,
        peer: SocketAddr,
        format: AudioFormat,
        payload_type: u8,
        ssrc: u32,
        epoch: Instant,
    ) -> Result<(InlineEgress, InlineEgressHandle), PacerError> {
        let pacer = PlayoutPacer::new(PacerConfig {
            source: format,
            wire: format,
            payload_type,
            ssrc,
            initial_sequence: (ssrc & 0xffff) as u16,
            initial_timestamp: ssrc,
            queue_frames: EGRESS_QUEUE_FRAMES,
            underrun: UnderrunPolicy::Silence,
        })?;
        let queue = Arc::new(ArrayQueue::new(EGRESS_QUEUE_CHUNKS));
        let flush = Arc::new(AtomicBool::new(false));
        let shared = Arc::new(InlineEgressShared::default());
        let handle = InlineEgressHandle {
            format,
            queue: Arc::clone(&queue),
            flush: Arc::clone(&flush),
            shared: Arc::clone(&shared),
        };
        Ok((
            InlineEgress {
                socket,
                peer,
                frame_samples: format.samples_per_packet().unwrap_or(1) as usize,
                pacer,
                queue,
                flush,
                shared,
                epoch,
                discarded_from_queue: 0,
            },
            handle,
        ))
    }

    pub fn pump(&mut self, now: Instant) {
        if self.flush.swap(false, Ordering::Relaxed) {
            let mut cleared = 0u64;
            while let Some(pending) = self.queue.pop() {
                cleared += pending.len() as u64;
                self.discarded_from_queue += pending.len() as u64;
            }
            cleared += self.pacer.clear() as u64;
            self.shared.clears.fetch_add(1, Ordering::Relaxed);
            self.shared
                .cleared_samples
                .fetch_add(cleared, Ordering::Relaxed);
        }
        let prebuffer = self.frame_samples * PREBUFFER_FRAMES;
        for _ in 0..MAX_CHUNKS_PER_TICK {
            if self.pacer.queued_samples() >= prebuffer {
                break;
            }
            let Some(pcm) = self.queue.pop() else {
                break;
            };
            if let EnqueueOutcome::QueuedAfterDrop(dropped) = self.pacer.push(&pcm) {
                self.shared
                    .dropped_samples
                    .fetch_add(dropped as u64, Ordering::Relaxed);
            }
        }
        let elapsed = now.saturating_duration_since(self.epoch);
        if let Some(packet) = self.pacer.tick(elapsed) {
            match self.socket.send_to(packet.datagram, self.peer) {
                Ok(_) => self.shared.datagrams_sent.fetch_add(1, Ordering::Relaxed),
                Err(_) => self.shared.send_errors.fetch_add(1, Ordering::Relaxed),
            };
        }
        self.publish(self.pacer.stats());
    }

    pub fn stats(&self) -> PacerStats {
        self.pacer.stats()
    }

    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    fn publish(&self, stats: PacerStats) {
        let drained =
            self.discarded_from_queue + stats.pushed_samples - self.pacer.queued_samples() as u64;
        self.shared
            .drained_samples
            .store(drained, Ordering::Relaxed);
        self.shared
            .silence_frames
            .store(stats.silence_frames, Ordering::Relaxed);
        self.shared
            .late_ticks
            .store(stats.late_ticks, Ordering::Relaxed);
        self.shared
            .encode_errors
            .store(stats.encode_errors, Ordering::Relaxed);
    }
}

pub fn egress_ssrc(session: u64, salt: u64) -> u32 {
    let mixed = session.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(31)
        ^ salt.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    ((mixed >> 32) as u32) | 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use media_core::rtp::RtpPacket;
    use media_core::{g711, Encoding};
    use std::time::Duration;

    fn totals(handle: &InlineEgressHandle) -> InlineEgressTotals {
        let mut totals = InlineEgressTotals::default();
        totals.add_shared(&handle.shared());
        totals
    }

    fn pcmu() -> AudioFormat {
        AudioFormat::pcmu_8k_20ms()
    }

    fn egress_pair() -> (InlineEgress, InlineEgressHandle, UdpSocket) {
        let peer = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        peer.set_nonblocking(true).expect("nonblocking peer");
        let ours = UdpSocket::bind("127.0.0.1:0").expect("an egress socket");
        ours.set_nonblocking(true).expect("nonblocking egress");
        let (egress, handle) = InlineEgress::bind(
            ours,
            peer.local_addr().expect("peer address"),
            pcmu(),
            0,
            0x1234_5678,
            Instant::now(),
        )
        .expect("a pacer for pcmu");
        (egress, handle, peer)
    }

    struct Seen {
        marker: bool,
        payload_type: u8,
        sequence: u16,
        timestamp: u32,
        payload: Vec<u8>,
    }

    fn drain(peer: &UdpSocket) -> Vec<Seen> {
        let mut seen = Vec::new();
        let mut buf = [0u8; 2048];
        while let Ok((len, _)) = peer.recv_from(&mut buf) {
            let packet = RtpPacket::parse(&buf[..len]).expect("the peer receives valid rtp");
            seen.push(Seen {
                marker: packet.marker,
                payload_type: packet.payload_type,
                sequence: packet.sequence,
                timestamp: packet.timestamp,
                payload: packet.payload.to_vec(),
            });
        }
        seen
    }

    #[test]
    fn queued_pcm_leaves_as_paced_rtp_the_peer_can_parse() {
        let (mut egress, handle, peer) = egress_pair();
        assert!(handle.push(vec![1_000i16; 320]));
        let epoch = Instant::now();
        for tick in 0..3 {
            egress.pump(epoch + Duration::from_millis(tick * 20));
        }

        let packets = drain(&peer);
        assert_eq!(packets.len(), 3, "one datagram per ptime tick");
        assert_eq!(packets[0].payload_type, 0);
        assert!(packets[0].marker, "the first talkspurt packet marks itself");
        assert_eq!(packets[1].sequence, packets[0].sequence.wrapping_add(1));
        assert_eq!(
            packets[1].timestamp,
            packets[0].timestamp.wrapping_add(160),
            "the timestamp advances by one frame of samples"
        );
        assert_eq!(packets[0].payload.len(), 160);
        assert_eq!(packets[0].payload[0], g711::linear_to_ulaw(1_000));
        assert_eq!(
            packets[2].payload[0],
            g711::linear_to_ulaw(0),
            "an empty queue paces silence rather than stopping"
        );
        assert_eq!(totals(&handle).datagrams_sent, 3);
    }

    #[test]
    fn a_clear_from_the_control_world_empties_both_the_queue_and_the_pacer() {
        let (mut egress, handle, peer) = egress_pair();
        assert!(handle.push(vec![2_000i16; 1_600]));
        let epoch = Instant::now();
        egress.pump(epoch);
        let _ = drain(&peer);

        handle.clear();
        egress.pump(epoch + Duration::from_millis(20));

        let packets = drain(&peer);
        assert_eq!(packets.len(), 1);
        assert_eq!(
            packets[0].payload[0],
            g711::linear_to_ulaw(0),
            "the tick after a clear is silence, so cut-through is one frame"
        );
        let totals = totals(&handle);
        assert_eq!(totals.clears, 1);
        assert!(totals.cleared_samples >= 1_440);
    }

    #[test]
    fn a_full_egress_queue_refuses_the_chunk_instead_of_blocking_the_control_world() {
        let (_egress, handle, _peer) = egress_pair();
        let mut refused = 0;
        for _ in 0..EGRESS_QUEUE_CHUNKS + 4 {
            if !handle.push(vec![0i16; 160]) {
                refused += 1;
            }
        }
        assert_eq!(refused, 4);
        assert_eq!(totals(&handle).chunks_refused, 4);
        assert_eq!(totals(&handle).chunks_queued, EGRESS_QUEUE_CHUNKS as u64);
    }

    #[test]
    fn an_unreachable_peer_costs_a_counter_not_a_stall() {
        let ours = UdpSocket::bind("127.0.0.1:0").expect("an egress socket");
        ours.set_nonblocking(true).expect("nonblocking");
        let (mut egress, handle) = InlineEgress::bind(
            ours,
            "127.0.0.1:9".parse().expect("a discard address"),
            pcmu(),
            0,
            7,
            Instant::now(),
        )
        .expect("a pacer");
        assert!(handle.push(vec![0i16; 160]));
        let epoch = Instant::now();
        egress.pump(epoch);
        let totals = totals(&handle);
        assert_eq!(totals.datagrams_sent + totals.send_errors, 1);
    }

    #[test]
    fn an_ssrc_is_never_zero_and_differs_between_sessions() {
        assert_ne!(egress_ssrc(1, 99), egress_ssrc(2, 99));
        assert_ne!(egress_ssrc(1, 99), egress_ssrc(1, 100));
        for session in 0..64 {
            assert_ne!(egress_ssrc(session, 0), 0);
        }
    }

    #[test]
    fn the_pacer_refuses_a_format_it_cannot_encode_on_the_wire() {
        let ours = UdpSocket::bind("127.0.0.1:0").expect("socket");
        let opus = AudioFormat {
            encoding: Encoding::Opus,
            sample_rate_hz: 48_000,
            channels: 1,
            ptime_ms: 20,
        };
        assert!(InlineEgress::bind(
            ours,
            "127.0.0.1:9".parse().expect("address"),
            opus,
            111,
            7,
            Instant::now()
        )
        .is_err());
    }
}
