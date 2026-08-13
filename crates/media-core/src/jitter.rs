pub const MAX_PAYLOAD: usize = 480;

const CAPACITY: usize = 64;

#[derive(Clone, Copy)]
struct Slot {
    occupied: bool,
    len: u16,
    data: [u8; MAX_PAYLOAD],
}

impl Default for Slot {
    fn default() -> Self {
        Slot {
            occupied: false,
            len: 0,
            data: [0; MAX_PAYLOAD],
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushOutcome {
    Buffered,
    Duplicate,
    TooLate,
    TooBig,
    Reset,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PopOutcome<'a> {
    Packet(&'a [u8]),
    Lost,
    Waiting,
}

pub struct JitterBuffer {
    slots: Box<[Slot; CAPACITY]>,
    play_seq: u16,
    max_seq: u16,
    started: bool,
    target_depth: u16,
    stats: Stats,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub received: u64,
    pub duplicates: u64,
    pub late_drops: u64,
    pub lost: u64,
    pub resets: u64,
}

impl JitterBuffer {
    pub fn new(target_depth_packets: u16) -> Self {
        JitterBuffer {
            slots: Box::new([Slot::default(); CAPACITY]),
            play_seq: 0,
            max_seq: 0,
            started: false,
            target_depth: target_depth_packets.max(1),
            stats: Stats::default(),
        }
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    fn idx(seq: u16) -> usize {
        seq as usize % CAPACITY
    }

    fn seq_delta(a: u16, b: u16) -> i32 {
        (a.wrapping_sub(b) as i16) as i32
    }

    pub fn push(&mut self, seq: u16, payload: &[u8]) -> PushOutcome {
        if payload.len() > MAX_PAYLOAD {
            return PushOutcome::TooBig;
        }
        self.stats.received += 1;

        if !self.started {
            self.started = true;
            self.play_seq = seq;
            self.max_seq = seq;
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
            self.stats.resets += 1;
            let slot = &mut self.slots[Self::idx(seq)];
            slot.occupied = true;
            slot.len = payload.len() as u16;
            slot.data[..payload.len()].copy_from_slice(payload);
            return PushOutcome::Reset;
        }

        let slot = &mut self.slots[Self::idx(seq)];
        if slot.occupied {
            self.stats.duplicates += 1;
            return PushOutcome::Duplicate;
        }
        slot.occupied = true;
        slot.len = payload.len() as u16;
        slot.data[..payload.len()].copy_from_slice(payload);
        if Self::seq_delta(seq, self.max_seq) > 0 {
            self.max_seq = seq;
        }
        PushOutcome::Buffered
    }

    pub fn depth(&self) -> u16 {
        Self::seq_delta(self.max_seq, self.play_seq).max(0) as u16 + 1
    }

    pub fn pop(&mut self) -> PopOutcome<'_> {
        if !self.started {
            return PopOutcome::Waiting;
        }
        if self.depth() < self.target_depth && !self.slots[Self::idx(self.play_seq)].occupied {
            return PopOutcome::Waiting;
        }
        let seq = self.play_seq;
        self.play_seq = self.play_seq.wrapping_add(1);
        let idx = Self::idx(seq);
        if self.slots[idx].occupied {
            self.slots[idx].occupied = false;
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

    fn payload(tag: u8) -> Vec<u8> {
        vec![tag; 160]
    }

    #[test]
    fn reorders_out_of_order_arrival() {
        let mut jb = JitterBuffer::new(3);
        jb.push(100, &payload(0));
        jb.push(102, &payload(2));
        jb.push(101, &payload(1));
        for expect in [0u8, 1, 2] {
            match jb.pop() {
                PopOutcome::Packet(p) => assert_eq!(p[0], expect),
                other => panic!("expected packet {expect}, got {other:?}"),
            }
        }
    }

    #[test]
    fn primes_before_playout() {
        let mut jb = JitterBuffer::new(3);
        assert_eq!(jb.pop(), PopOutcome::Waiting);
        jb.push(10, &payload(0));
        assert!(matches!(jb.pop(), PopOutcome::Packet(_)));
        assert_eq!(jb.pop(), PopOutcome::Waiting);
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
}
