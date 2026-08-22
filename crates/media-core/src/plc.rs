const HISTORY_SAMPLES: usize = 800;
const CORRELATION_SPAN_SAMPLES: usize = 160;
const MAX_PERIOD_SAMPLES: usize = 160;
const SEARCH_WINDOW_SAMPLES: usize = CORRELATION_SPAN_SAMPLES + MAX_PERIOD_SAMPLES;
const LOWEST_PITCH_HZ: u32 = 50;
const HIGHEST_PITCH_HZ: u32 = 200;
const UNATTENUATED_MS: u32 = 10;
const SILENT_AFTER_MS: u32 = 60;
const RECOVERY_FRACTION_OF_PERIOD: usize = 4;
const UNITY_GAIN: i32 = 1 << 15;

pub struct PacketLossConcealer {
    history: [i16; HISTORY_SAMPLES],
    search_window: [i16; SEARCH_WINDOW_SAMPLES],
    period: [i16; MAX_PERIOD_SAMPLES],
    write: usize,
    filled: usize,
    period_len: usize,
    period_offset: usize,
    concealed_samples: u32,
    recovery_samples: usize,
    shortest_period: usize,
    longest_period: usize,
    unattenuated_samples: u32,
    silent_after_samples: u32,
}

impl PacketLossConcealer {
    pub fn new(sample_rate_hz: u32) -> Self {
        let rate = sample_rate_hz.max(1);
        let shortest = (rate / HIGHEST_PITCH_HZ).clamp(1, MAX_PERIOD_SAMPLES as u32) as usize;
        let longest = (rate / LOWEST_PITCH_HZ).clamp(1, MAX_PERIOD_SAMPLES as u32) as usize;
        PacketLossConcealer {
            history: [0; HISTORY_SAMPLES],
            search_window: [0; SEARCH_WINDOW_SAMPLES],
            period: [0; MAX_PERIOD_SAMPLES],
            write: 0,
            filled: 0,
            period_len: 0,
            period_offset: 0,
            concealed_samples: 0,
            recovery_samples: 0,
            shortest_period: shortest,
            longest_period: longest.max(shortest),
            unattenuated_samples: rate / 1000 * UNATTENUATED_MS,
            silent_after_samples: (rate / 1000 * SILENT_AFTER_MS)
                .max(rate / 1000 * UNATTENUATED_MS + 1),
        }
    }

    pub fn concealing(&self) -> bool {
        self.concealed_samples > 0
    }

    pub fn forget(&mut self) {
        self.filled = 0;
        self.period_len = 0;
        self.period_offset = 0;
        self.concealed_samples = 0;
        self.recovery_samples = 0;
    }

    pub fn remember(&mut self, pcm: &[i16]) {
        if pcm.is_empty() {
            return;
        }
        if pcm.len() >= HISTORY_SAMPLES {
            let tail = &pcm[pcm.len() - HISTORY_SAMPLES..];
            self.history.copy_from_slice(tail);
            self.write = 0;
            self.filled = HISTORY_SAMPLES;
            return;
        }
        let room = HISTORY_SAMPLES - self.write;
        if pcm.len() <= room {
            self.history[self.write..self.write + pcm.len()].copy_from_slice(pcm);
            self.write = (self.write + pcm.len()) % HISTORY_SAMPLES;
        } else {
            let (head, tail) = pcm.split_at(room);
            self.history[self.write..].copy_from_slice(head);
            self.history[..tail.len()].copy_from_slice(tail);
            self.write = tail.len();
        }
        self.filled = (self.filled + pcm.len()).min(HISTORY_SAMPLES);
    }

    pub fn conceal(&mut self, out: &mut [i16]) {
        if self.concealed_samples == 0 {
            self.select_period();
        }
        if self.period_len == 0 {
            out.fill(0);
            self.concealed_samples = self.concealed_samples.saturating_add(out.len() as u32);
            self.recovery_samples = 0;
            return;
        }
        for sample in out.iter_mut() {
            *sample = self.next_concealed_sample();
        }
        self.recovery_samples = (self.period_len / RECOVERY_FRACTION_OF_PERIOD).max(1);
        self.remember(out);
    }

    pub fn recover_into(&mut self, pcm: &mut [i16]) {
        if self.recovery_samples == 0 {
            self.concealed_samples = 0;
            return;
        }
        let overlap = self.recovery_samples.min(pcm.len());
        let weights = (overlap + 1) as i32;
        for (index, sample) in pcm[..overlap].iter_mut().enumerate() {
            let synthetic = self.next_concealed_sample() as i32;
            let arrived = *sample as i32;
            let arrived_weight = (index + 1) as i32;
            let synthetic_weight = weights - arrived_weight;
            *sample = ((synthetic * synthetic_weight + arrived * arrived_weight) / weights) as i16;
        }
        self.recovery_samples = 0;
        self.concealed_samples = 0;
    }

    fn next_concealed_sample(&mut self) -> i16 {
        let raw = self.period[self.period_offset] as i32;
        self.period_offset += 1;
        if self.period_offset >= self.period_len {
            self.period_offset = 0;
        }
        let gain = self.gain_q15(self.concealed_samples);
        self.concealed_samples = self.concealed_samples.saturating_add(1);
        ((raw * gain) >> 15) as i16
    }

    fn gain_q15(&self, concealed_samples: u32) -> i32 {
        if concealed_samples < self.unattenuated_samples {
            return UNITY_GAIN;
        }
        if concealed_samples >= self.silent_after_samples {
            return 0;
        }
        let faded = (concealed_samples - self.unattenuated_samples) as i32;
        let span = (self.silent_after_samples - self.unattenuated_samples) as i32;
        UNITY_GAIN - UNITY_GAIN * faded / span
    }

    fn select_period(&mut self) {
        self.period_offset = 0;
        if self.filled == 0 {
            self.period_len = 0;
            return;
        }
        let span = CORRELATION_SPAN_SAMPLES.min(self.filled / 2);
        let longest = self.longest_period.min(self.filled.saturating_sub(span));
        if span == 0 || longest < self.shortest_period {
            self.period_len = self.filled.min(self.longest_period);
            self.copy_period_from_history();
            return;
        }
        let window = span + longest;
        for (index, slot) in self.search_window[..window].iter_mut().enumerate() {
            *slot = Self::sample_ago(&self.history, self.write, window - index);
        }
        let mut best = self.shortest_period;
        let mut best_difference = u64::MAX;
        for lag in self.shortest_period..=longest {
            let mut difference = 0u64;
            for j in 0..span {
                let recent = self.search_window[window - span + j] as i32;
                let earlier = self.search_window[window - span + j - lag] as i32;
                difference += (recent - earlier).unsigned_abs() as u64;
            }
            if difference < best_difference {
                best_difference = difference;
                best = lag;
            }
        }
        self.period_len = best;
        self.copy_period_from_history();
    }

    fn copy_period_from_history(&mut self) {
        for index in 0..self.period_len {
            self.period[index] =
                Self::sample_ago(&self.history, self.write, self.period_len - index);
        }
    }

    fn sample_ago(history: &[i16; HISTORY_SAMPLES], write: usize, ago: usize) -> i16 {
        let ago = ago.clamp(1, HISTORY_SAMPLES);
        history[(write + HISTORY_SAMPLES - ago) % HISTORY_SAMPLES]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RATE: u32 = 8000;
    const FRAME: usize = 160;

    fn periodic(period: usize, samples: usize, amplitude: i16) -> Vec<i16> {
        (0..samples)
            .map(|index| {
                if index % period < period / 2 {
                    amplitude
                } else {
                    -amplitude
                }
            })
            .collect()
    }

    fn energy(pcm: &[i16]) -> i64 {
        pcm.iter().map(|&s| (s as i64).abs()).sum()
    }

    #[test]
    fn conceals_by_repeating_the_detected_pitch_period() {
        let mut plc = PacketLossConcealer::new(RATE);
        let speech = periodic(80, 800, 6000);
        plc.remember(&speech);

        let mut concealed = [0i16; FRAME];
        plc.conceal(&mut concealed);

        let expected = &speech[speech.len() - FRAME..];
        let unattenuated = (RATE / 1000 * UNATTENUATED_MS) as usize;
        for (index, (&got, &want)) in concealed[..unattenuated]
            .iter()
            .zip(&expected[..unattenuated])
            .enumerate()
        {
            assert_eq!(got, want, "sample {index} of the repeat diverged");
        }
        for (index, (&got, &want)) in concealed[unattenuated..]
            .iter()
            .zip(&expected[unattenuated..])
            .enumerate()
        {
            assert_eq!(
                got.signum(),
                want.signum(),
                "sample {} lost the waveform's shape",
                index + unattenuated
            );
            assert!(got.abs() <= want.abs());
        }
    }

    #[test]
    fn with_no_history_concealment_is_silence() {
        let mut plc = PacketLossConcealer::new(RATE);
        let mut concealed = [1234i16; FRAME];
        plc.conceal(&mut concealed);
        assert!(concealed.iter().all(|&s| s == 0));
    }

    #[test]
    fn a_burst_fades_to_silence_by_sixty_milliseconds() {
        let mut plc = PacketLossConcealer::new(RATE);
        plc.remember(&periodic(80, 800, 8000));

        let mut first = [0i16; FRAME];
        plc.conceal(&mut first);
        assert!(energy(&first) > 0);

        let mut last = [0i16; FRAME];
        for _ in 0..2 {
            plc.conceal(&mut last);
        }
        assert!(
            energy(&last) < energy(&first) / 2,
            "60 ms in, concealment should be fading: {} vs {}",
            energy(&last),
            energy(&first)
        );

        let mut silent = [0i16; FRAME];
        plc.conceal(&mut silent);
        assert!(
            silent.iter().all(|&s| s == 0),
            "past 60 ms of loss the concealer must be muted"
        );
    }

    #[test]
    fn the_first_frame_after_a_gap_is_crossfaded_not_spliced() {
        let mut plc = PacketLossConcealer::new(RATE);
        plc.remember(&periodic(80, 800, 6000));
        let mut concealed = [0i16; FRAME];
        plc.conceal(&mut concealed);

        let arrived = vec![-6000i16; FRAME];
        let mut recovered = arrived.clone();
        plc.recover_into(&mut recovered);

        assert_ne!(recovered[0], arrived[0]);
        assert_eq!(recovered[FRAME - 1], arrived[FRAME - 1]);
        assert!(!plc.concealing());
    }

    #[test]
    fn forgetting_drops_the_history_a_stale_talkspurt_left() {
        let mut plc = PacketLossConcealer::new(RATE);
        plc.remember(&periodic(80, 800, 6000));
        plc.forget();
        let mut concealed = [0i16; FRAME];
        plc.conceal(&mut concealed);
        assert!(concealed.iter().all(|&s| s == 0));
    }

    #[test]
    fn a_short_history_still_repeats_instead_of_panicking() {
        let mut plc = PacketLossConcealer::new(RATE);
        plc.remember(&periodic(40, 30, 4000));
        let mut concealed = [0i16; FRAME];
        plc.conceal(&mut concealed);
        assert!(energy(&concealed) > 0);
    }

    #[test]
    fn unusual_sample_rates_stay_inside_the_fixed_buffers() {
        for rate in [8000, 16000, 48000, 1, 0] {
            let mut plc = PacketLossConcealer::new(rate);
            plc.remember(&periodic(80, 1600, 3000));
            let mut concealed = [0i16; FRAME];
            plc.conceal(&mut concealed);
            plc.recover_into(&mut concealed);
        }
    }

    #[test]
    fn history_survives_the_ring_wrapping() {
        let mut plc = PacketLossConcealer::new(RATE);
        for _ in 0..11 {
            plc.remember(&periodic(80, FRAME, 5000)[..]);
        }
        let mut concealed = [0i16; FRAME];
        plc.conceal(&mut concealed);
        assert!(energy(&concealed) > 0);
    }
}
