pub const MAX_PAYLOAD: usize = 1276;
pub const MAX_FRAME_SAMPLES: usize = 960;

const CAPACITY: usize = 64;
const MAX_TARGET_DEPTH: u16 = (CAPACITY / 2) as u16;
const JITTER_ESTIMATE_DIVISOR: i64 = 16;
const JITTER_HEADROOM_MULTIPLIER: u32 = 3;
const PACKETS_BELOW_TARGET_BEFORE_SHRINK: u32 = 250;

#[derive(Clone, Copy)]
struct Slot {
    occupied: bool,
    accounted: bool,
    len: u16,
    data: [u8; MAX_PAYLOAD],
}

impl Default for Slot {
    fn default() -> Self {
        Slot {
            occupied: false,
            accounted: false,
            len: 0,
            data: [0; MAX_PAYLOAD],
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushOutcome {
    Buffered,
    BufferedAfterSenderSilence,
    Duplicate,
    TooLate,
    TooBig,
    Reset,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PopOutcome<'a> {
    Packet(&'a [u8]),
    Accounted,
    Lost,
    Waiting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    pub timestamp: u32,
    pub arrival_ticks: u32,
    pub marker: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JitterConfig {
    pub target_depth_packets: u16,
    pub max_depth_packets: u16,
    pub timestamp_increment: u32,
}

#[derive(Clone, Copy)]
struct Newest {
    seq: u16,
    timestamp: u32,
}

#[derive(Clone, Copy)]
struct Transit {
    timestamp: u32,
    arrival_ticks: u32,
}

pub struct JitterBuffer {
    slots: Box<[Slot; CAPACITY]>,
    play_seq: u16,
    max_seq: u16,
    started: bool,
    primed: bool,
    floor_depth: u16,
    ceiling_depth: u16,
    target_depth: u16,
    timestamp_increment: u32,
    newest: Option<Newest>,
    transit: Option<Transit>,
    packets_below_target: u32,
    stats: Stats,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub received: u64,
    pub duplicates: u64,
    pub late_drops: u64,
    pub lost: u64,
    pub resets: u64,
    pub silence_gaps: u64,
    pub target_depth: u16,
    pub jitter_ticks: u32,
}

impl JitterBuffer {
    pub fn new(target_depth_packets: u16) -> Self {
        Self::with_config(JitterConfig {
            target_depth_packets,
            max_depth_packets: target_depth_packets,
            timestamp_increment: 0,
        })
    }

    pub fn with_config(config: JitterConfig) -> Self {
        let floor = config.target_depth_packets.clamp(1, MAX_TARGET_DEPTH);
        let ceiling = config.max_depth_packets.clamp(floor, MAX_TARGET_DEPTH);
        JitterBuffer {
            slots: Box::new([Slot::default(); CAPACITY]),
            play_seq: 0,
            max_seq: 0,
            started: false,
            primed: false,
            floor_depth: floor,
            ceiling_depth: ceiling,
            target_depth: floor,
            timestamp_increment: config.timestamp_increment,
            newest: None,
            transit: None,
            packets_below_target: 0,
            stats: Stats {
                target_depth: floor,
                ..Stats::default()
            },
        }
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    pub fn target_depth(&self) -> u16 {
        self.target_depth
    }

    fn idx(seq: u16) -> usize {
        seq as usize % CAPACITY
    }

    fn seq_delta(a: u16, b: u16) -> i32 {
        (a.wrapping_sub(b) as i16) as i32
    }

    fn tick_delta(a: u32, b: u32) -> i64 {
        (a.wrapping_sub(b) as i32) as i64
    }

    pub fn push(&mut self, seq: u16, payload: &[u8]) -> PushOutcome {
        self.admit(seq, payload, false, None)
    }

    pub fn push_timed(&mut self, seq: u16, payload: &[u8], timing: Timing) -> PushOutcome {
        self.admit(seq, payload, false, Some(timing))
    }

    pub fn account(&mut self, seq: u16) -> PushOutcome {
        self.admit(seq, &[], true, None)
    }

    pub fn restart(&mut self) {
        for slot in self.slots.iter_mut() {
            slot.occupied = false;
        }
        self.started = false;
        self.primed = false;
        self.newest = None;
        self.transit = None;
        self.stats.resets += 1;
    }

    fn admit(
        &mut self,
        seq: u16,
        payload: &[u8],
        accounted: bool,
        timing: Option<Timing>,
    ) -> PushOutcome {
        if payload.len() > MAX_PAYLOAD {
            return PushOutcome::TooBig;
        }
        self.stats.received += 1;

        if !self.started {
            self.started = true;
            self.play_seq = seq;
            self.max_seq = seq;
            self.remember_timing(seq, timing);
        }

        let ahead = Self::seq_delta(seq, self.play_seq);
        if ahead < 0 {
            self.stats.late_drops += 1;
            return PushOutcome::TooLate;
        }
        if ahead >= CAPACITY as i32 {
            for s in self.slots.iter_mut() {
                s.occupied = false;
            }
            self.play_seq = seq;
            self.max_seq = seq;
            self.primed = false;
            self.newest = None;
            self.transit = None;
            self.stats.resets += 1;
            Self::fill(&mut self.slots[Self::idx(seq)], payload, accounted);
            self.remember_timing(seq, timing);
            return PushOutcome::Reset;
        }

        let slot = &mut self.slots[Self::idx(seq)];
        if slot.occupied {
            self.stats.duplicates += 1;
            return PushOutcome::Duplicate;
        }
        Self::fill(slot, payload, accounted);

        let mut outcome = PushOutcome::Buffered;
        if Self::seq_delta(seq, self.max_seq) > 0 {
            if let Some(timing) = timing {
                if self.sender_paused_before(seq, timing) {
                    let absorbed = self.absorb_silence(self.max_seq, seq);
                    self.stats.silence_gaps += absorbed;
                    outcome = PushOutcome::BufferedAfterSenderSilence;
                }
            }
            self.max_seq = seq;
            self.remember_timing(seq, timing);
        }
        if let Some(timing) = timing {
            self.estimate_jitter(timing);
            self.adapt_target_depth();
        }
        outcome
    }

    fn remember_timing(&mut self, seq: u16, timing: Option<Timing>) {
        if let Some(timing) = timing {
            self.newest = Some(Newest {
                seq,
                timestamp: timing.timestamp,
            });
        }
    }

    fn sender_paused_before(&self, seq: u16, timing: Timing) -> bool {
        if self.timestamp_increment == 0 {
            return false;
        }
        let Some(newest) = self.newest else {
            return false;
        };
        let span = Self::seq_delta(seq, newest.seq);
        if span <= 0 {
            return false;
        }
        let expected = span as i64 * self.timestamp_increment as i64;
        let elapsed = Self::tick_delta(timing.timestamp, newest.timestamp);
        let unexplained = elapsed - expected;
        unexplained >= self.timestamp_increment as i64 || (timing.marker && unexplained > 0)
    }

    fn absorb_silence(&mut self, after: u16, before: u16) -> u64 {
        let mut absorbed = 0;
        let mut seq = after.wrapping_add(1);
        while Self::seq_delta(before, seq) > 0 {
            let slot = &mut self.slots[Self::idx(seq)];
            if !slot.occupied {
                Self::fill(slot, &[], true);
                absorbed += 1;
            }
            seq = seq.wrapping_add(1);
        }
        absorbed
    }

    fn estimate_jitter(&mut self, timing: Timing) {
        if let Some(previous) = self.transit {
            let arrival = Self::tick_delta(timing.arrival_ticks, previous.arrival_ticks);
            let sampled = Self::tick_delta(timing.timestamp, previous.timestamp);
            let deviation = (arrival - sampled).abs();
            let estimate = self.stats.jitter_ticks as i64;
            let updated = estimate + (deviation - estimate) / JITTER_ESTIMATE_DIVISOR;
            self.stats.jitter_ticks = updated.clamp(0, u32::MAX as i64) as u32;
        }
        self.transit = Some(Transit {
            timestamp: timing.timestamp,
            arrival_ticks: timing.arrival_ticks,
        });
    }

    fn adapt_target_depth(&mut self) {
        if self.timestamp_increment == 0 || self.ceiling_depth == self.floor_depth {
            return;
        }
        let headroom = self
            .stats
            .jitter_ticks
            .saturating_mul(JITTER_HEADROOM_MULTIPLIER)
            .div_ceil(self.timestamp_increment)
            .min(u16::MAX as u32) as u16;
        let wanted = self
            .floor_depth
            .saturating_add(headroom)
            .clamp(self.floor_depth, self.ceiling_depth);
        if wanted > self.target_depth {
            self.target_depth = wanted;
            self.packets_below_target = 0;
        } else if wanted < self.target_depth {
            self.packets_below_target += 1;
            if self.packets_below_target >= PACKETS_BELOW_TARGET_BEFORE_SHRINK {
                self.target_depth -= 1;
                self.packets_below_target = 0;
            }
        } else {
            self.packets_below_target = 0;
        }
        self.stats.target_depth = self.target_depth;
    }

    fn fill(slot: &mut Slot, payload: &[u8], accounted: bool) {
        slot.occupied = true;
        slot.accounted = accounted;
        slot.len = payload.len() as u16;
        slot.data[..payload.len()].copy_from_slice(payload);
    }

    pub fn depth(&self) -> u16 {
        if !self.started {
            return 0;
        }
        (Self::seq_delta(self.max_seq, self.play_seq) + 1).max(0) as u16
    }

    pub fn pop(&mut self) -> PopOutcome<'_> {
        let depth = self.depth();
        if depth == 0 {
            return PopOutcome::Waiting;
        }
        if !self.primed {
            if depth < self.target_depth {
                return PopOutcome::Waiting;
            }
            self.primed = true;
        }
        if depth < self.target_depth && !self.slots[Self::idx(self.play_seq)].occupied {
            return PopOutcome::Waiting;
        }
        let seq = self.play_seq;
        self.play_seq = self.play_seq.wrapping_add(1);
        let idx = Self::idx(seq);
        if self.slots[idx].occupied {
            self.slots[idx].occupied = false;
            if self.slots[idx].accounted {
                return PopOutcome::Accounted;
            }
            let len = self.slots[idx].len as usize;
            PopOutcome::Packet(&self.slots[idx].data[..len])
        } else {
            self.stats.lost += 1;
            PopOutcome::Lost
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INCREMENT: u32 = 160;

    fn payload(tag: u8) -> Vec<u8> {
        vec![tag; 160]
    }

    fn assert_pops_payloads(jb: &mut JitterBuffer, tags: &[u8]) {
        for &expect in tags {
            match jb.pop() {
                PopOutcome::Packet(p) => assert_eq!(p[0], expect),
                other => panic!("expected packet {expect}, got {other:?}"),
            }
        }
    }

    fn adaptive(floor: u16, ceiling: u16) -> JitterBuffer {
        JitterBuffer::with_config(JitterConfig {
            target_depth_packets: floor,
            max_depth_packets: ceiling,
            timestamp_increment: INCREMENT,
        })
    }

    fn timing(packet: u32, arrival_ticks: u32) -> Timing {
        Timing {
            timestamp: packet * INCREMENT,
            arrival_ticks,
            marker: false,
        }
    }

    #[test]
    fn reorders_out_of_order_arrival() {
        let mut jb = JitterBuffer::new(3);
        jb.push(100, &payload(0));
        jb.push(102, &payload(2));
        jb.push(101, &payload(1));
        assert_pops_payloads(&mut jb, &[0, 1, 2]);
    }

    #[test]
    fn primes_before_playout() {
        let mut jb = JitterBuffer::new(3);
        assert_eq!(jb.depth(), 0);
        assert_eq!(jb.pop(), PopOutcome::Waiting);
        jb.push(10, &payload(0));
        assert_eq!(jb.pop(), PopOutcome::Waiting);
        jb.push(11, &payload(1));
        assert_eq!(jb.pop(), PopOutcome::Waiting);
        jb.push(12, &payload(2));
        assert_pops_payloads(&mut jb, &[0, 1, 2]);
        assert_eq!(jb.pop(), PopOutcome::Waiting);
        assert_eq!(jb.stats().lost, 0);
    }

    #[test]
    fn drained_stream_waits_instead_of_counting_loss() {
        let mut jb = JitterBuffer::new(1);
        jb.push(200, &payload(1));
        assert_pops_payloads(&mut jb, &[1]);
        assert_eq!(jb.depth(), 0);
        for _ in 0..5 {
            assert_eq!(jb.pop(), PopOutcome::Waiting);
        }
        assert_eq!(jb.stats().lost, 0);
        jb.push(201, &payload(2));
        assert_pops_payloads(&mut jb, &[2]);
        assert_eq!(jb.stats().lost, 0);
    }

    #[test]
    fn reset_reprimes_before_playout_resumes() {
        let mut jb = JitterBuffer::new(2);
        jb.push(100, &payload(1));
        jb.push(101, &payload(2));
        assert_pops_payloads(&mut jb, &[1]);
        assert_eq!(jb.push(10_000, &payload(3)), PushOutcome::Reset);
        assert_eq!(jb.pop(), PopOutcome::Waiting);
        jb.push(10_001, &payload(4));
        assert_pops_payloads(&mut jb, &[3, 4]);
    }

    #[test]
    fn reports_loss_once_playout_started() {
        let mut jb = JitterBuffer::new(1);
        jb.push(5, &payload(5));
        jb.push(7, &payload(7));
        assert!(matches!(jb.pop(), PopOutcome::Packet(_)));
        assert_eq!(jb.pop(), PopOutcome::Lost);
        assert!(matches!(jb.pop(), PopOutcome::Packet(_)));
        assert_eq!(jb.stats().lost, 1);
    }

    #[test]
    fn drops_duplicates_and_late_packets() {
        let mut jb = JitterBuffer::new(1);
        jb.push(50, &payload(0));
        assert_eq!(jb.push(50, &payload(0)), PushOutcome::Duplicate);
        assert!(matches!(jb.pop(), PopOutcome::Packet(_)));
        assert_eq!(jb.push(49, &payload(9)), PushOutcome::TooLate);
    }

    #[test]
    fn survives_sequence_wraparound() {
        let mut jb = JitterBuffer::new(1);
        jb.push(65534, &payload(1));
        jb.push(65535, &payload(2));
        jb.push(0, &payload(3));
        jb.push(1, &payload(4));
        let mut seen = vec![];
        for _ in 0..4 {
            if let PopOutcome::Packet(p) = jb.pop() {
                seen.push(p[0]);
            }
        }
        assert_eq!(seen, vec![1, 2, 3, 4]);
    }

    #[test]
    fn resets_on_large_jump() {
        let mut jb = JitterBuffer::new(1);
        jb.push(100, &payload(1));
        assert_eq!(jb.push(10_000, &payload(2)), PushOutcome::Reset);
        assert!(matches!(jb.pop(), PopOutcome::Packet(p) if p[0] == 2));
        assert_eq!(jb.stats().resets, 1);
    }

    #[test]
    fn rejects_oversized_payloads() {
        let mut jb = JitterBuffer::new(1);
        assert_eq!(jb.push(1, &[0u8; MAX_PAYLOAD + 1]), PushOutcome::TooBig);
    }

    #[test]
    fn a_paced_stream_keeps_the_floor_depth() {
        let mut jb = adaptive(2, 8);
        for packet in 0..200u32 {
            let arrival = packet * INCREMENT;
            jb.push_timed(packet as u16, &payload(0), timing(packet, arrival));
            jb.pop();
        }
        assert_eq!(jb.target_depth(), 2);
        assert_eq!(jb.stats().jitter_ticks, 0);
        assert_eq!(jb.stats().lost, 0);
    }

    #[test]
    fn observed_jitter_raises_the_target_depth_within_its_ceiling() {
        let mut jb = adaptive(2, 6);
        let swing = [0i64, 400, -400, 320, -320];
        for packet in 0..200u32 {
            let arrival = (packet as i64 * INCREMENT as i64 + swing[packet as usize % 5]).max(0);
            jb.push_timed(packet as u16, &payload(0), timing(packet, arrival as u32));
            jb.pop();
        }
        assert!(
            jb.target_depth() > 2,
            "a jittery arrival pattern must grow the cushion, stayed at {}",
            jb.target_depth()
        );
        assert!(jb.target_depth() <= 6);
    }

    #[test]
    fn the_target_depth_shrinks_only_after_a_long_quiet_spell() {
        let mut jb = adaptive(1, 6);
        let mut packet = 0u32;
        for _ in 0..40 {
            let arrival = packet * INCREMENT + if packet.is_multiple_of(2) { 480 } else { 0 };
            jb.push_timed(packet as u16, &payload(0), timing(packet, arrival));
            jb.pop();
            packet += 1;
        }
        let grown = jb.target_depth();
        assert!(grown > 1);

        for _ in 0..2 * PACKETS_BELOW_TARGET_BEFORE_SHRINK {
            let arrival = packet * INCREMENT;
            jb.push_timed(packet as u16, &payload(0), timing(packet, arrival));
            jb.pop();
            packet += 1;
        }
        assert!(
            jb.target_depth() < grown,
            "the cushion must give way once the path is calm again"
        );
    }

    #[test]
    fn a_gap_the_timestamps_explain_is_silence_not_loss() {
        let mut jb = adaptive(1, 4);
        jb.push_timed(10, &payload(1), timing(0, 0));
        let resumed = Timing {
            timestamp: 40 * INCREMENT,
            arrival_ticks: 40 * INCREMENT,
            marker: true,
        };
        assert_eq!(
            jb.push_timed(12, &payload(2), resumed),
            PushOutcome::BufferedAfterSenderSilence
        );
        assert_eq!(jb.stats().silence_gaps, 1);

        assert_pops_payloads(&mut jb, &[1]);
        assert_eq!(jb.pop(), PopOutcome::Accounted);
        assert_pops_payloads(&mut jb, &[2]);
        assert_eq!(jb.stats().lost, 0);
    }

    #[test]
    fn a_gap_the_timestamps_do_not_explain_is_still_loss() {
        let mut jb = adaptive(1, 4);
        jb.push_timed(10, &payload(1), timing(0, 0));
        assert_eq!(
            jb.push_timed(12, &payload(2), timing(2, 2 * INCREMENT)),
            PushOutcome::Buffered
        );
        assert_eq!(jb.stats().silence_gaps, 0);

        assert_pops_payloads(&mut jb, &[1]);
        assert_eq!(jb.pop(), PopOutcome::Lost);
        assert_pops_payloads(&mut jb, &[2]);
        assert_eq!(jb.stats().lost, 1);
    }

    #[test]
    fn a_restart_reanchors_at_a_nearby_sequence_instead_of_dropping_it() {
        let mut jb = adaptive(1, 4);
        for packet in 0..4u32 {
            jb.push_timed(
                100 + packet as u16,
                &payload(1),
                timing(packet, packet * INCREMENT),
            );
            jb.pop();
        }
        jb.restart();
        assert_eq!(
            jb.push_timed(101, &payload(9), timing(9000, 9000 * INCREMENT)),
            PushOutcome::Buffered
        );
        assert_eq!(jb.stats().late_drops, 0);
        assert_eq!(jb.stats().resets, 1);
        assert_pops_payloads(&mut jb, &[9]);
    }
}
