use thiserror::Error;

pub const MAX_MIX_FRAME_SAMPLES: usize = 5760;

const GAIN_FRACTION_BITS: u32 = 12;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MixError {
    #[error("a mixer frame must hold between 1 and 5760 samples, not {0}")]
    FrameSize(usize),
    #[error("expected a frame of {want} samples, got {got}")]
    FrameLength { want: usize, got: usize },
    #[error("contributor is not a member of this mix")]
    UnknownContributor,
    #[error("listener is not a member of this mix")]
    UnknownListener,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Gain(i32);

impl Gain {
    pub const MUTED: Gain = Gain(0);
    pub const UNITY: Gain = Gain(1 << GAIN_FRACTION_BITS);
    pub const MAX_Q12: i32 = 8 << GAIN_FRACTION_BITS;

    pub fn from_q12(q12: i32) -> Gain {
        Gain(q12.clamp(0, Self::MAX_Q12))
    }

    pub fn q12(self) -> i32 {
        self.0
    }

    pub fn is_muted(self) -> bool {
        self.0 == 0
    }

    pub fn is_unity(self) -> bool {
        self.0 == Self::UNITY.0
    }
}

impl Default for Gain {
    fn default() -> Self {
        Gain::UNITY
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpeechGate {
    pub rms_threshold: u16,
    pub attack_frames: u32,
    pub hangover_frames: u32,
}

impl SpeechGate {
    pub const fn new(rms_threshold: u16, attack_frames: u32, hangover_frames: u32) -> Self {
        SpeechGate {
            rms_threshold,
            attack_frames,
            hangover_frames,
        }
    }

    fn mean_square_threshold(&self) -> u64 {
        let level = self.rms_threshold as u64;
        level * level
    }
}

impl Default for SpeechGate {
    fn default() -> Self {
        SpeechGate {
            rms_threshold: 300,
            attack_frames: 2,
            hangover_frames: 12,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContributorId {
    index: u32,
    generation: u32,
}

impl ContributorId {
    pub fn index(self) -> usize {
        self.index as usize
    }

    pub fn generation(self) -> u32 {
        self.generation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ListenerId {
    index: u32,
    generation: u32,
}

impl ListenerId {
    pub fn index(self) -> usize {
        self.index as usize
    }

    pub fn generation(self) -> u32 {
        self.generation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Party {
    pub contributor: ContributorId,
    pub listener: ListenerId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    Accepted,
    Replaced,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MixStats {
    pub ticks: u64,
    pub pushed_frames: u64,
    pub replaced_frames: u64,
    pub absent_frames: u64,
    pub mixed_frames: u64,
    pub silent_listener_frames: u64,
    pub clipped_samples: u64,
    pub speaker_onsets: u64,
    pub speaker_releases: u64,
}

struct ContributorSlot {
    generation: u32,
    occupied: bool,
    self_listener: Option<usize>,
    frame: Vec<i16>,
    has_frame: bool,
    mean_square: u64,
    speaking: bool,
    above_run: u32,
    below_run: u32,
}

impl ContributorSlot {
    fn vacant(frame_samples: usize) -> Self {
        ContributorSlot {
            generation: 0,
            occupied: false,
            self_listener: None,
            frame: vec![0; frame_samples],
            has_frame: false,
            mean_square: 0,
            speaking: false,
            above_run: 0,
            below_run: 0,
        }
    }

    fn reset(&mut self) {
        self.self_listener = None;
        self.has_frame = false;
        self.mean_square = 0;
        self.speaking = false;
        self.above_run = 0;
        self.below_run = 0;
        for sample in self.frame.iter_mut() {
            *sample = 0;
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct ListenerSlot {
    generation: u32,
    occupied: bool,
}

pub struct MixMatrix {
    frame_samples: usize,
    speech: SpeechGate,
    contributors: Vec<ContributorSlot>,
    listeners: Vec<ListenerSlot>,
    gains: Vec<Gain>,
    listener_stride: usize,
    accumulator: Vec<i32>,
    output: Vec<i16>,
    stats: MixStats,
    tick: u64,
}

impl MixMatrix {
    pub fn new(frame_samples: usize, speech: SpeechGate) -> Result<Self, MixError> {
        Self::with_capacity(frame_samples, speech, 4, 4)
    }

    pub fn with_capacity(
        frame_samples: usize,
        speech: SpeechGate,
        contributors: usize,
        listeners: usize,
    ) -> Result<Self, MixError> {
        if frame_samples == 0 || frame_samples > MAX_MIX_FRAME_SAMPLES {
            return Err(MixError::FrameSize(frame_samples));
        }
        let listener_stride = listeners.max(1);
        let contributor_slots = contributors.max(1);
        Ok(MixMatrix {
            frame_samples,
            speech,
            contributors: (0..contributor_slots)
                .map(|_| ContributorSlot::vacant(frame_samples))
                .collect(),
            listeners: vec![ListenerSlot::default(); listener_stride],
            gains: vec![Gain::UNITY; contributor_slots * listener_stride],
            listener_stride,
            accumulator: vec![0; frame_samples],
            output: vec![0; listener_stride * frame_samples],
            stats: MixStats::default(),
            tick: 0,
        })
    }

    pub fn frame_samples(&self) -> usize {
        self.frame_samples
    }

    pub fn speech_gate(&self) -> SpeechGate {
        self.speech
    }

    pub fn tick(&self) -> u64 {
        self.tick
    }

    pub fn stats(&self) -> MixStats {
        self.stats
    }

    pub fn contributor_count(&self) -> usize {
        self.contributors
            .iter()
            .filter(|slot| slot.occupied)
            .count()
    }

    pub fn listener_count(&self) -> usize {
        self.listeners.iter().filter(|slot| slot.occupied).count()
    }

    pub fn join_party(&mut self) -> Party {
        let listener = self.join_listener();
        let contributor = self.allocate_contributor(Some(listener.index()));
        Party {
            contributor,
            listener,
        }
    }

    pub fn join_contributor(&mut self) -> ContributorId {
        self.allocate_contributor(None)
    }

    pub fn join_listener(&mut self) -> ListenerId {
        let index = match self.listeners.iter().position(|slot| !slot.occupied) {
            Some(index) => index,
            None => {
                self.grow_listeners();
                self.listeners.len() - 1
            }
        };
        self.listeners[index].occupied = true;
        let generation = self.listeners[index].generation;
        for slot in self.contributors.iter_mut() {
            if slot.self_listener == Some(index) {
                slot.self_listener = None;
            }
        }
        self.reset_listener_column(index);
        let start = index * self.frame_samples;
        for sample in self.output[start..start + self.frame_samples].iter_mut() {
            *sample = 0;
        }
        ListenerId {
            index: index as u32,
            generation,
        }
    }

    pub fn leave_party(&mut self, party: Party) -> Result<(), MixError> {
        self.leave_contributor(party.contributor)?;
        self.leave_listener(party.listener)
    }

    pub fn leave_contributor(&mut self, contributor: ContributorId) -> Result<(), MixError> {
        let index = self.contributor_index(contributor)?;
        let slot = &mut self.contributors[index];
        slot.occupied = false;
        slot.generation = slot.generation.wrapping_add(1);
        slot.reset();
        self.reset_contributor_row(index);
        Ok(())
    }

    pub fn leave_listener(&mut self, listener: ListenerId) -> Result<(), MixError> {
        let index = self.listener_index(listener)?;
        self.listeners[index].occupied = false;
        self.listeners[index].generation = self.listeners[index].generation.wrapping_add(1);
        for slot in self.contributors.iter_mut() {
            if slot.self_listener == Some(index) {
                slot.self_listener = None;
            }
        }
        self.reset_listener_column(index);
        let start = index * self.frame_samples;
        for sample in self.output[start..start + self.frame_samples].iter_mut() {
            *sample = 0;
        }
        Ok(())
    }

    pub fn gain(&self, contributor: ContributorId, listener: ListenerId) -> Result<Gain, MixError> {
        let (row, column) = self.pair(contributor, listener)?;
        Ok(self.gains[row * self.listener_stride + column])
    }

    pub fn set_gain(
        &mut self,
        contributor: ContributorId,
        listener: ListenerId,
        gain: Gain,
    ) -> Result<(), MixError> {
        let (row, column) = self.pair(contributor, listener)?;
        self.gains[row * self.listener_stride + column] = gain;
        Ok(())
    }

    pub fn route_only(
        &mut self,
        contributor: ContributorId,
        listener: ListenerId,
    ) -> Result<(), MixError> {
        let (row, column) = self.pair(contributor, listener)?;
        let stride = self.listener_stride;
        for index in 0..stride {
            self.gains[row * stride + index] = if index == column {
                Gain::UNITY
            } else {
                Gain::MUTED
            };
        }
        Ok(())
    }

    pub fn route_to_all(&mut self, contributor: ContributorId) -> Result<(), MixError> {
        let index = self.contributor_index(contributor)?;
        self.reset_contributor_row(index);
        Ok(())
    }

    pub fn mute_contributor(&mut self, contributor: ContributorId) -> Result<(), MixError> {
        let index = self.contributor_index(contributor)?;
        let stride = self.listener_stride;
        for column in 0..stride {
            self.gains[index * stride + column] = Gain::MUTED;
        }
        Ok(())
    }

    pub fn unmute_contributor(&mut self, contributor: ContributorId) -> Result<(), MixError> {
        self.route_to_all(contributor)
    }

    pub fn deafen_listener(&mut self, listener: ListenerId) -> Result<(), MixError> {
        let column = self.listener_index(listener)?;
        let stride = self.listener_stride;
        for row in 0..self.contributors.len() {
            self.gains[row * stride + column] = Gain::MUTED;
        }
        Ok(())
    }

    pub fn undeafen_listener(&mut self, listener: ListenerId) -> Result<(), MixError> {
        let column = self.listener_index(listener)?;
        self.reset_listener_column(column);
        Ok(())
    }

    pub fn push(
        &mut self,
        contributor: ContributorId,
        pcm: &[i16],
    ) -> Result<PushOutcome, MixError> {
        if pcm.len() != self.frame_samples {
            return Err(MixError::FrameLength {
                want: self.frame_samples,
                got: pcm.len(),
            });
        }
        let index = self.contributor_index(contributor)?;
        let slot = &mut self.contributors[index];
        let replaced = slot.has_frame;
        slot.frame.copy_from_slice(pcm);
        slot.has_frame = true;
        slot.mean_square = mean_square(pcm);
        self.stats.pushed_frames += 1;
        if replaced {
            self.stats.replaced_frames += 1;
            Ok(PushOutcome::Replaced)
        } else {
            Ok(PushOutcome::Accepted)
        }
    }

    pub fn mix(&mut self) -> MixOutput<'_> {
        self.tick = self.tick.wrapping_add(1);
        self.stats.ticks += 1;
        advance_speech(&mut self.contributors, &self.speech, &mut self.stats);
        {
            let MixMatrix {
                frame_samples,
                contributors,
                listeners,
                gains,
                listener_stride,
                accumulator,
                output,
                stats,
                ..
            } = self;
            let frame_samples = *frame_samples;
            let stride = *listener_stride;
            for (column, listener) in listeners.iter().enumerate() {
                if !listener.occupied {
                    continue;
                }
                for cell in accumulator.iter_mut() {
                    *cell = 0;
                }
                let mut contributed = false;
                for (row, slot) in contributors.iter().enumerate() {
                    if !slot.occupied || !slot.has_frame {
                        continue;
                    }
                    let gain = gains[row * stride + column];
                    if gain.is_muted() {
                        continue;
                    }
                    contributed = true;
                    if gain.is_unity() {
                        for (cell, sample) in accumulator.iter_mut().zip(slot.frame.iter()) {
                            *cell = cell.saturating_add(*sample as i32);
                        }
                    } else {
                        let scale = gain.q12();
                        for (cell, sample) in accumulator.iter_mut().zip(slot.frame.iter()) {
                            *cell =
                                cell.saturating_add((*sample as i32 * scale) >> GAIN_FRACTION_BITS);
                        }
                    }
                }
                let start = column * frame_samples;
                let ear = &mut output[start..start + frame_samples];
                if contributed {
                    for (sample, cell) in ear.iter_mut().zip(accumulator.iter()) {
                        if *cell > i16::MAX as i32 {
                            *sample = i16::MAX;
                            stats.clipped_samples += 1;
                        } else if *cell < i16::MIN as i32 {
                            *sample = i16::MIN;
                            stats.clipped_samples += 1;
                        } else {
                            *sample = *cell as i16;
                        }
                    }
                } else {
                    for sample in ear.iter_mut() {
                        *sample = 0;
                    }
                    stats.silent_listener_frames += 1;
                }
                stats.mixed_frames += 1;
            }
        }
        for slot in self.contributors.iter_mut() {
            slot.has_frame = false;
        }
        MixOutput { mixer: self }
    }

    pub fn speaking(&self, contributor: ContributorId) -> Result<bool, MixError> {
        let index = self.contributor_index(contributor)?;
        Ok(self.contributors[index].speaking)
    }

    pub fn level(&self, contributor: ContributorId) -> Result<u32, MixError> {
        let index = self.contributor_index(contributor)?;
        Ok(self.contributors[index].mean_square.isqrt() as u32)
    }

    pub fn active_speakers(&self) -> impl Iterator<Item = ContributorId> + '_ {
        self.contributors
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.occupied && slot.speaking)
            .map(|(index, slot)| ContributorId {
                index: index as u32,
                generation: slot.generation,
            })
    }

    fn allocate_contributor(&mut self, self_listener: Option<usize>) -> ContributorId {
        let index = match self.contributors.iter().position(|slot| !slot.occupied) {
            Some(index) => index,
            None => {
                self.grow_contributors();
                self.contributors.len() - 1
            }
        };
        let slot = &mut self.contributors[index];
        slot.reset();
        slot.occupied = true;
        slot.self_listener = self_listener;
        let generation = slot.generation;
        self.reset_contributor_row(index);
        ContributorId {
            index: index as u32,
            generation,
        }
    }

    fn grow_contributors(&mut self) {
        self.contributors
            .push(ContributorSlot::vacant(self.frame_samples));
        self.gains
            .extend(std::iter::repeat_n(Gain::UNITY, self.listener_stride));
    }

    fn grow_listeners(&mut self) {
        if self.listeners.len() == self.listener_stride {
            let stride = (self.listener_stride * 2).max(2);
            let mut gains = vec![Gain::UNITY; self.contributors.len() * stride];
            for (row, source) in self.gains.chunks(self.listener_stride).enumerate() {
                let start = row * stride;
                gains[start..start + self.listener_stride].copy_from_slice(source);
            }
            self.gains = gains;
            self.listener_stride = stride;
            self.output = vec![0; stride * self.frame_samples];
        }
        self.listeners.push(ListenerSlot::default());
    }

    fn reset_contributor_row(&mut self, row: usize) {
        let stride = self.listener_stride;
        let self_listener = self.contributors[row].self_listener;
        for column in 0..stride {
            self.gains[row * stride + column] = if self_listener == Some(column) {
                Gain::MUTED
            } else {
                Gain::UNITY
            };
        }
    }

    fn reset_listener_column(&mut self, column: usize) {
        let stride = self.listener_stride;
        for row in 0..self.contributors.len() {
            self.gains[row * stride + column] =
                if self.contributors[row].self_listener == Some(column) {
                    Gain::MUTED
                } else {
                    Gain::UNITY
                };
        }
    }

    fn contributor_index(&self, contributor: ContributorId) -> Result<usize, MixError> {
        let index = contributor.index();
        match self.contributors.get(index) {
            Some(slot) if slot.occupied && slot.generation == contributor.generation => Ok(index),
            _ => Err(MixError::UnknownContributor),
        }
    }

    fn listener_index(&self, listener: ListenerId) -> Result<usize, MixError> {
        let index = listener.index();
        match self.listeners.get(index) {
            Some(slot) if slot.occupied && slot.generation == listener.generation => Ok(index),
            _ => Err(MixError::UnknownListener),
        }
    }

    fn pair(
        &self,
        contributor: ContributorId,
        listener: ListenerId,
    ) -> Result<(usize, usize), MixError> {
        Ok((
            self.contributor_index(contributor)?,
            self.listener_index(listener)?,
        ))
    }

    fn listener_output(&self, column: usize) -> &[i16] {
        let start = column * self.frame_samples;
        &self.output[start..start + self.frame_samples]
    }
}

pub struct MixOutput<'a> {
    mixer: &'a MixMatrix,
}

impl<'a> MixOutput<'a> {
    pub fn tick(&self) -> u64 {
        self.mixer.tick
    }

    pub fn frame(&self, listener: ListenerId) -> Option<&'a [i16]> {
        let column = self.mixer.listener_index(listener).ok()?;
        Some(self.mixer.listener_output(column))
    }

    pub fn frames(&self) -> impl Iterator<Item = (ListenerId, &'a [i16])> + '_ {
        let mixer = self.mixer;
        mixer
            .listeners
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.occupied)
            .map(move |(column, slot)| {
                (
                    ListenerId {
                        index: column as u32,
                        generation: slot.generation,
                    },
                    mixer.listener_output(column),
                )
            })
    }
}

fn mean_square(pcm: &[i16]) -> u64 {
    if pcm.is_empty() {
        return 0;
    }
    let mut energy: u64 = 0;
    for sample in pcm {
        let level = *sample as i64;
        energy += (level * level) as u64;
    }
    energy / pcm.len() as u64
}

fn advance_speech(contributors: &mut [ContributorSlot], gate: &SpeechGate, stats: &mut MixStats) {
    let threshold = gate.mean_square_threshold();
    let attack = gate.attack_frames.max(1);
    for slot in contributors.iter_mut() {
        if !slot.occupied {
            continue;
        }
        if !slot.has_frame {
            stats.absent_frames += 1;
            slot.mean_square = 0;
        }
        if slot.mean_square >= threshold {
            slot.below_run = 0;
            slot.above_run = slot.above_run.saturating_add(1);
            if !slot.speaking && slot.above_run >= attack {
                slot.speaking = true;
                stats.speaker_onsets += 1;
            }
        } else {
            slot.above_run = 0;
            slot.below_run = slot.below_run.saturating_add(1);
            if slot.speaking && slot.below_run > gate.hangover_frames {
                slot.speaking = false;
                stats.speaker_releases += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FRAME: usize = 160;

    fn gate() -> SpeechGate {
        SpeechGate::new(300, 2, 3)
    }

    fn conference(parties: usize) -> (MixMatrix, Vec<Party>) {
        let mut mixer = MixMatrix::with_capacity(FRAME, gate(), parties, parties).unwrap();
        let joined = (0..parties).map(|_| mixer.join_party()).collect();
        (mixer, joined)
    }

    fn tone(level: i16) -> Vec<i16> {
        vec![level; FRAME]
    }

    fn constant(frame: &[i16]) -> i16 {
        assert_eq!(frame.len(), FRAME);
        let first = frame[0];
        assert!(frame.iter().all(|sample| *sample == first), "not constant");
        first
    }

    #[test]
    fn frame_size_is_validated() {
        assert_eq!(
            MixMatrix::new(0, gate()).err(),
            Some(MixError::FrameSize(0))
        );
        assert_eq!(
            MixMatrix::new(MAX_MIX_FRAME_SAMPLES + 1, gate()).err(),
            Some(MixError::FrameSize(MAX_MIX_FRAME_SAMPLES + 1))
        );
        assert!(MixMatrix::new(FRAME, gate()).is_ok());
    }

    #[test]
    fn two_parties_hear_each_other_and_not_themselves() {
        let (mut mixer, parties) = conference(2);
        mixer.push(parties[0].contributor, &tone(1000)).unwrap();
        mixer.push(parties[1].contributor, &tone(-250)).unwrap();
        let mixed = mixer.mix();
        assert_eq!(constant(mixed.frame(parties[0].listener).unwrap()), -250);
        assert_eq!(constant(mixed.frame(parties[1].listener).unwrap()), 1000);
        assert_eq!(mixed.tick(), 1);
    }

    #[test]
    fn three_parties_each_hear_the_other_two() {
        let (mut mixer, parties) = conference(3);
        for (index, party) in parties.iter().enumerate() {
            mixer
                .push(party.contributor, &tone(100 * (index as i16 + 1)))
                .unwrap();
        }
        let mixed = mixer.mix();
        assert_eq!(constant(mixed.frame(parties[0].listener).unwrap()), 500);
        assert_eq!(constant(mixed.frame(parties[1].listener).unwrap()), 400);
        assert_eq!(constant(mixed.frame(parties[2].listener).unwrap()), 300);
    }

    #[test]
    fn eight_parties_each_hear_everyone_but_self() {
        let (mut mixer, parties) = conference(8);
        assert_eq!(mixer.contributor_count(), 8);
        assert_eq!(mixer.listener_count(), 8);
        let mut total = 0i32;
        for (index, party) in parties.iter().enumerate() {
            let level = 100 * (index as i16 + 1);
            total += level as i32;
            mixer.push(party.contributor, &tone(level)).unwrap();
        }
        let mixed = mixer.mix();
        assert_eq!(mixed.frames().count(), 8);
        for (index, party) in parties.iter().enumerate() {
            let own = 100 * (index as i32 + 1);
            assert_eq!(
                constant(mixed.frame(party.listener).unwrap()) as i32,
                total - own,
                "listener {index}"
            );
        }
    }

    #[test]
    fn an_absent_contributor_is_silence_and_is_counted() {
        let (mut mixer, parties) = conference(3);
        mixer.push(parties[0].contributor, &tone(700)).unwrap();
        let mixed = mixer.mix();
        assert_eq!(constant(mixed.frame(parties[1].listener).unwrap()), 700);
        assert_eq!(constant(mixed.frame(parties[0].listener).unwrap()), 0);
        let stats = mixer.stats();
        assert_eq!(stats.absent_frames, 2);
        assert_eq!(stats.silent_listener_frames, 1);
        assert_eq!(stats.mixed_frames, 3);
    }

    #[test]
    fn a_frame_is_consumed_by_one_tick_only() {
        let (mut mixer, parties) = conference(2);
        mixer.push(parties[0].contributor, &tone(900)).unwrap();
        assert_eq!(
            constant(mixer.mix().frame(parties[1].listener).unwrap()),
            900
        );
        assert_eq!(constant(mixer.mix().frame(parties[1].listener).unwrap()), 0);
    }

    #[test]
    fn a_second_push_in_one_tick_replaces_the_first() {
        let (mut mixer, parties) = conference(2);
        assert_eq!(
            mixer.push(parties[0].contributor, &tone(100)).unwrap(),
            PushOutcome::Accepted
        );
        assert_eq!(
            mixer.push(parties[0].contributor, &tone(300)).unwrap(),
            PushOutcome::Replaced
        );
        assert_eq!(
            constant(mixer.mix().frame(parties[1].listener).unwrap()),
            300
        );
        assert_eq!(mixer.stats().replaced_frames, 1);
    }

    #[test]
    fn accumulation_saturates_at_the_i16_bounds() {
        let (mut mixer, parties) = conference(3);
        let monitor = mixer.join_listener();
        mixer.push(parties[0].contributor, &tone(30000)).unwrap();
        mixer.push(parties[1].contributor, &tone(20000)).unwrap();
        mixer.push(parties[2].contributor, &tone(-30000)).unwrap();
        let mixed = mixer.mix();
        assert_eq!(constant(mixed.frame(monitor).unwrap()), 20000);
        assert_eq!(
            constant(mixed.frame(parties[2].listener).unwrap()),
            i16::MAX
        );
        assert_eq!(constant(mixed.frame(parties[0].listener).unwrap()), -10000);
        assert_eq!(mixer.stats().clipped_samples, FRAME as u64);
    }

    #[test]
    fn a_negative_overflow_clamps_to_the_floor() {
        let (mut mixer, parties) = conference(3);
        let monitor = mixer.join_listener();
        for party in &parties {
            mixer.push(party.contributor, &tone(-20000)).unwrap();
        }
        let mixed = mixer.mix();
        assert_eq!(constant(mixed.frame(monitor).unwrap()), i16::MIN);
        for party in &parties {
            assert_eq!(constant(mixed.frame(party.listener).unwrap()), i16::MIN);
        }
        assert_eq!(mixer.stats().clipped_samples, 4 * FRAME as u64);
    }

    #[test]
    fn per_pair_gain_scales_one_direction_only() {
        let (mut mixer, parties) = conference(2);
        mixer
            .set_gain(
                parties[0].contributor,
                parties[1].listener,
                Gain::from_q12(2048),
            )
            .unwrap();
        mixer.push(parties[0].contributor, &tone(1000)).unwrap();
        mixer.push(parties[1].contributor, &tone(1000)).unwrap();
        let mixed = mixer.mix();
        assert_eq!(constant(mixed.frame(parties[1].listener).unwrap()), 500);
        assert_eq!(constant(mixed.frame(parties[0].listener).unwrap()), 1000);
        assert_eq!(
            mixer
                .gain(parties[0].contributor, parties[1].listener)
                .unwrap(),
            Gain::from_q12(2048)
        );
        assert_eq!(
            mixer
                .gain(parties[0].contributor, parties[0].listener)
                .unwrap(),
            Gain::MUTED
        );
    }

    #[test]
    fn gain_is_clamped_to_the_declared_range() {
        assert_eq!(Gain::from_q12(-5), Gain::MUTED);
        assert_eq!(Gain::from_q12(i32::MAX).q12(), Gain::MAX_Q12);
        assert!(Gain::UNITY.is_unity());
        assert!(Gain::MUTED.is_muted());
        assert_eq!(Gain::default(), Gain::UNITY);
    }

    #[test]
    fn whisper_reaches_one_listener_and_nobody_else() {
        let (mut mixer, parties) = conference(3);
        let whisperer = mixer.join_contributor();
        mixer.route_only(whisperer, parties[1].listener).unwrap();
        for party in &parties {
            mixer.push(party.contributor, &tone(100)).unwrap();
        }
        mixer.push(whisperer, &tone(5000)).unwrap();
        let mixed = mixer.mix();
        assert_eq!(constant(mixed.frame(parties[1].listener).unwrap()), 5200);
        assert_eq!(constant(mixed.frame(parties[0].listener).unwrap()), 200);
        assert_eq!(constant(mixed.frame(parties[2].listener).unwrap()), 200);
    }

    #[test]
    fn promoting_a_whisper_to_everyone_is_a_matrix_flip() {
        let (mut mixer, parties) = conference(2);
        let whisperer = mixer.join_contributor();
        mixer.route_only(whisperer, parties[0].listener).unwrap();
        mixer.push(whisperer, &tone(400)).unwrap();
        assert_eq!(constant(mixer.mix().frame(parties[1].listener).unwrap()), 0);
        mixer.route_to_all(whisperer).unwrap();
        mixer.push(whisperer, &tone(400)).unwrap();
        let mixed = mixer.mix();
        assert_eq!(constant(mixed.frame(parties[0].listener).unwrap()), 400);
        assert_eq!(constant(mixed.frame(parties[1].listener).unwrap()), 400);
    }

    #[test]
    fn a_monitor_hears_everyone_and_contributes_nothing() {
        let (mut mixer, parties) = conference(3);
        let monitor = mixer.join_listener();
        assert_eq!(mixer.contributor_count(), 3);
        assert_eq!(mixer.listener_count(), 4);
        for (index, party) in parties.iter().enumerate() {
            mixer
                .push(party.contributor, &tone(100 * (index as i16 + 1)))
                .unwrap();
        }
        let mixed = mixer.mix();
        assert_eq!(constant(mixed.frame(monitor).unwrap()), 600);
        assert_eq!(constant(mixed.frame(parties[0].listener).unwrap()), 500);
        assert_eq!(mixed.frames().count(), 4);
    }

    #[test]
    fn mute_zeroes_a_row_and_deafen_zeroes_a_column() {
        let (mut mixer, parties) = conference(3);
        mixer.mute_contributor(parties[0].contributor).unwrap();
        mixer.deafen_listener(parties[1].listener).unwrap();
        for party in &parties {
            mixer.push(party.contributor, &tone(100)).unwrap();
        }
        {
            let mixed = mixer.mix();
            assert_eq!(constant(mixed.frame(parties[1].listener).unwrap()), 0);
            assert_eq!(constant(mixed.frame(parties[2].listener).unwrap()), 100);
        }
        mixer.unmute_contributor(parties[0].contributor).unwrap();
        mixer.undeafen_listener(parties[1].listener).unwrap();
        for party in &parties {
            mixer.push(party.contributor, &tone(100)).unwrap();
        }
        let mixed = mixer.mix();
        assert_eq!(constant(mixed.frame(parties[1].listener).unwrap()), 200);
        assert_eq!(constant(mixed.frame(parties[2].listener).unwrap()), 200);
        assert_eq!(
            mixer
                .gain(parties[1].contributor, parties[1].listener)
                .unwrap(),
            Gain::MUTED
        );
    }

    #[test]
    fn active_speaker_flags_attack_and_hang_over() {
        let (mut mixer, parties) = conference(2);
        let speaker = parties[0].contributor;
        mixer.push(speaker, &tone(1000)).unwrap();
        mixer.mix();
        assert!(!mixer.speaking(speaker).unwrap());
        mixer.push(speaker, &tone(1000)).unwrap();
        mixer.mix();
        assert!(mixer.speaking(speaker).unwrap());
        assert_eq!(mixer.level(speaker).unwrap(), 1000);
        assert_eq!(mixer.active_speakers().collect::<Vec<_>>(), vec![speaker]);
        for hangover in 0..3 {
            mixer.push(speaker, &tone(0)).unwrap();
            mixer.mix();
            assert!(mixer.speaking(speaker).unwrap(), "released at {hangover}");
        }
        mixer.push(speaker, &tone(0)).unwrap();
        mixer.mix();
        assert!(!mixer.speaking(speaker).unwrap());
        assert_eq!(mixer.active_speakers().count(), 0);
        let stats = mixer.stats();
        assert_eq!(stats.speaker_onsets, 1);
        assert_eq!(stats.speaker_releases, 1);
    }

    #[test]
    fn an_alternating_frame_never_raises_the_flag() {
        let (mut mixer, parties) = conference(2);
        let speaker = parties[0].contributor;
        for _ in 0..8 {
            mixer.push(speaker, &tone(4000)).unwrap();
            mixer.push(parties[1].contributor, &tone(0)).unwrap();
            mixer.mix();
            assert!(!mixer.speaking(speaker).unwrap());
            mixer.push(speaker, &tone(0)).unwrap();
            mixer.mix();
            assert!(!mixer.speaking(speaker).unwrap());
        }
        assert!(!mixer.speaking(parties[1].contributor).unwrap());
        assert_eq!(mixer.stats().speaker_onsets, 0);
        assert_eq!(mixer.stats().speaker_releases, 0);
    }

    #[test]
    fn absent_frames_release_the_flag_like_silence() {
        let (mut mixer, parties) = conference(2);
        let speaker = parties[0].contributor;
        for _ in 0..2 {
            mixer.push(speaker, &tone(2000)).unwrap();
            mixer.mix();
        }
        assert!(mixer.speaking(speaker).unwrap());
        for _ in 0..4 {
            mixer.mix();
        }
        assert!(!mixer.speaking(speaker).unwrap());
    }

    #[test]
    fn a_stale_identity_is_refused_after_leaving() {
        let (mut mixer, parties) = conference(2);
        let gone = parties[1];
        mixer.leave_party(gone).unwrap();
        assert_eq!(
            mixer.push(gone.contributor, &tone(1)).err(),
            Some(MixError::UnknownContributor)
        );
        assert_eq!(
            mixer
                .set_gain(parties[0].contributor, gone.listener, Gain::MUTED)
                .err(),
            Some(MixError::UnknownListener)
        );
        assert_eq!(
            mixer.speaking(gone.contributor).err(),
            Some(MixError::UnknownContributor)
        );
        assert!(mixer.mix().frame(gone.listener).is_none());
        assert_eq!(mixer.contributor_count(), 1);
        assert_eq!(mixer.listener_count(), 1);
    }

    #[test]
    fn a_reused_slot_starts_clean() {
        let (mut mixer, parties) = conference(3);
        let leaving = parties[1];
        mixer
            .route_only(leaving.contributor, parties[0].listener)
            .unwrap();
        mixer.deafen_listener(leaving.listener).unwrap();
        mixer.push(leaving.contributor, &tone(9000)).unwrap();
        mixer.leave_party(leaving).unwrap();
        let joined = mixer.join_party();
        assert_eq!(joined.contributor.index(), leaving.contributor.index());
        assert_eq!(joined.listener.index(), leaving.listener.index());
        assert_ne!(
            joined.contributor.generation(),
            leaving.contributor.generation()
        );
        assert_eq!(
            mixer.gain(joined.contributor, parties[2].listener).unwrap(),
            Gain::UNITY
        );
        assert_eq!(
            mixer.gain(joined.contributor, joined.listener).unwrap(),
            Gain::MUTED
        );
        assert_eq!(
            mixer.gain(parties[2].contributor, joined.listener).unwrap(),
            Gain::UNITY
        );
        assert_eq!(constant(mixer.mix().frame(joined.listener).unwrap()), 0);
        mixer.push(parties[0].contributor, &tone(150)).unwrap();
        mixer.push(joined.contributor, &tone(50)).unwrap();
        let mixed = mixer.mix();
        assert_eq!(constant(mixed.frame(joined.listener).unwrap()), 150);
        assert_eq!(constant(mixed.frame(parties[2].listener).unwrap()), 200);
    }

    #[test]
    fn membership_grows_past_the_initial_capacity() {
        let mut mixer = MixMatrix::with_capacity(FRAME, gate(), 1, 1).unwrap();
        let parties: Vec<Party> = (0..5).map(|_| mixer.join_party()).collect();
        assert_eq!(mixer.contributor_count(), 5);
        assert_eq!(mixer.listener_count(), 5);
        for party in &parties {
            mixer.push(party.contributor, &tone(10)).unwrap();
        }
        let mixed = mixer.mix();
        for party in &parties {
            assert_eq!(constant(mixed.frame(party.listener).unwrap()), 40);
        }
    }

    #[test]
    fn a_frame_of_the_wrong_length_is_refused() {
        let (mut mixer, parties) = conference(2);
        assert_eq!(
            mixer.push(parties[0].contributor, &[0; 80]).err(),
            Some(MixError::FrameLength {
                want: FRAME,
                got: 80
            })
        );
        assert_eq!(mixer.frame_samples(), FRAME);
        assert_eq!(mixer.speech_gate(), gate());
    }

    #[test]
    fn ten_thousand_frames_change_no_buffer_capacity() {
        let (mut mixer, parties) = conference(8);
        let monitor = mixer.join_listener();
        let gains = (mixer.gains.len(), mixer.gains.capacity());
        let output = (mixer.output.len(), mixer.output.capacity());
        let accumulator = (mixer.accumulator.len(), mixer.accumulator.capacity());
        let frames: Vec<(usize, usize)> = mixer
            .contributors
            .iter()
            .map(|slot| (slot.frame.len(), slot.frame.capacity()))
            .collect();
        let loud = tone(6000);
        let quiet = tone(20);
        for tick in 0..10_000u32 {
            for (index, party) in parties.iter().enumerate() {
                if tick % 50 == 0 && index == 3 {
                    continue;
                }
                let frame = if (tick as usize + index) % 7 < 3 {
                    &loud
                } else {
                    &quiet
                };
                mixer.push(party.contributor, frame).unwrap();
            }
            let mixed = mixer.mix();
            assert_eq!(mixed.frame(monitor).unwrap().len(), FRAME);
        }
        assert_eq!(mixer.tick(), 10_000);
        assert_eq!((mixer.gains.len(), mixer.gains.capacity()), gains);
        assert_eq!((mixer.output.len(), mixer.output.capacity()), output);
        assert_eq!(
            (mixer.accumulator.len(), mixer.accumulator.capacity()),
            accumulator
        );
        assert_eq!(
            mixer
                .contributors
                .iter()
                .map(|slot| (slot.frame.len(), slot.frame.capacity()))
                .collect::<Vec<_>>(),
            frames
        );
        let stats = mixer.stats();
        assert_eq!(stats.ticks, 10_000);
        assert_eq!(stats.mixed_frames, 90_000);
        assert_eq!(stats.absent_frames, 200);
        assert!(stats.speaker_onsets > 0);
        assert!(stats.speaker_releases > 0);
    }
}
