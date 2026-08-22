use crate::hub::{Hub, TapEvent};
use crate::supervisor::{AudioFlowWatchdog, SessionHealth};
use crossbeam_queue::ArrayQueue;
use media_core::jitter;
use media_core::pipeline::{IngestOutcome, PipelineError, PipelineStats, Playout, StreamPipeline};
use media_core::{AudioFormat, Track};
use std::io::ErrorKind;
use std::net::UdpSocket;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;

const MAX_DATAGRAM: usize = 2048;
const MAX_DATAGRAMS_PER_DRAIN: usize = 64;
const MAX_RECORDED_DIGITS: usize = 32;
const SILENCE: [i16; 480] = [0; 480];
const LOG_LENGTH_PREFIX: usize = 4;

pub const MAX_SSRC_TRACKS: usize = 8;
pub const NO_SSRC: u64 = u64::MAX;
const SSRC_TRACK_COMMANDS: usize = 4;
const SSRC_CHANGE_CONFIRMATIONS: u16 = 3;

#[derive(Debug, Error)]
pub enum SpikeError {
    #[error("socket error: {0}")]
    Io(#[from] std::io::Error),
    #[error("pipeline rejected the tap format: {0}")]
    Pipeline(#[from] PipelineError),
    #[error("wav writing failed: {0}")]
    Wav(#[from] hound::Error),
    #[error("no legs to capture")]
    NoLegs,
    #[error("{0} legs is more than a stereo capture can represent")]
    TooManyLegs(usize),
    #[error("track {0:?} appears on more than one leg")]
    DuplicateTrack(Track),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LegStats {
    pub datagrams: u64,
    pub recv_errors: u64,
    pub drain_batches_filled: u64,
    pub underruns: u64,
    pub capture_full: bool,
    pub datagram_log_full: bool,
    pub unknown_ssrc: Option<u32>,
    pub ssrcs_seen: [Option<u32>; 4],
    pub ssrc_changes: u64,
    pub reresolutions: u64,
    pub pipeline: PipelineStats,
    pub jitter: jitter::Stats,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SsrcTracks {
    entries: [Option<(u32, Track)>; MAX_SSRC_TRACKS],
}

impl SsrcTracks {
    pub fn from_pairs(pairs: &[(u32, Track)]) -> SsrcTracks {
        let mut entries = [None; MAX_SSRC_TRACKS];
        for (slot, pair) in entries.iter_mut().zip(pairs) {
            *slot = Some(*pair);
        }
        SsrcTracks { entries }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.iter().all(Option::is_none)
    }

    pub fn len(&self) -> usize {
        self.entries.iter().flatten().count()
    }

    pub fn track_of(&self, ssrc: u32) -> Option<Track> {
        self.entries
            .iter()
            .flatten()
            .find(|(known, _)| *known == ssrc)
            .map(|(_, track)| *track)
    }
}

#[derive(Clone)]
pub struct SsrcTrackPublisher {
    queue: Arc<ArrayQueue<SsrcTracks>>,
}

impl SsrcTrackPublisher {
    pub fn publish(&self, tracks: SsrcTracks) -> bool {
        self.queue.force_push(tracks).is_none()
    }
}

#[derive(Default)]
pub struct SharedLegStats {
    pub datagrams: AtomicU64,
    pub recv_errors: AtomicU64,
    pub underruns: AtomicU64,
    pub frames_played: AtomicU64,
    pub frames_concealed: AtomicU64,
    pub frames_suppressed: AtomicU64,
    pub companded: AtomicU64,
    pub unknown_payload_type: AtomicU64,
    pub unparsable: AtomicU64,
    pub telephone_events: AtomicU64,
    pub dtmf_digits: AtomicU64,
    pub jitter_lost: AtomicU64,
    pub jitter_duplicates: AtomicU64,
    pub jitter_late_drops: AtomicU64,
    pub jitter_resets: AtomicU64,
    pub jitter_silence_gaps: AtomicU64,
    pub unknown_ssrc: AtomicU64,
    pub unresolved_ssrc: AtomicU64,
    pub ssrc_changes: AtomicU64,
    pub reresolutions: AtomicU64,
    pub stalled: AtomicU64,
    pub stalls: AtomicU64,
}

impl SharedLegStats {
    pub fn unresolved_ssrc(&self) -> Option<u32> {
        if self.unknown_ssrc.load(Ordering::Relaxed) == 0 {
            return None;
        }
        u32::try_from(self.unresolved_ssrc.load(Ordering::Relaxed)).ok()
    }

    fn store(&self, stats: &LegStats, stalled: bool, stalls: u64) {
        self.datagrams.store(stats.datagrams, Ordering::Relaxed);
        self.recv_errors.store(stats.recv_errors, Ordering::Relaxed);
        self.underruns.store(stats.underruns, Ordering::Relaxed);
        self.frames_played
            .store(stats.pipeline.frames_played, Ordering::Relaxed);
        self.frames_concealed
            .store(stats.pipeline.frames_concealed, Ordering::Relaxed);
        self.frames_suppressed
            .store(stats.pipeline.frames_suppressed, Ordering::Relaxed);
        self.companded
            .store(stats.pipeline.companded, Ordering::Relaxed);
        self.unknown_payload_type
            .store(stats.pipeline.unknown_payload_type, Ordering::Relaxed);
        self.unparsable
            .store(stats.pipeline.unparsable, Ordering::Relaxed);
        self.telephone_events
            .store(stats.pipeline.telephone_events, Ordering::Relaxed);
        self.dtmf_digits
            .store(stats.pipeline.dtmf_digits, Ordering::Relaxed);
        self.jitter_lost.store(stats.jitter.lost, Ordering::Relaxed);
        self.jitter_duplicates
            .store(stats.jitter.duplicates, Ordering::Relaxed);
        self.jitter_late_drops
            .store(stats.jitter.late_drops, Ordering::Relaxed);
        self.jitter_resets
            .store(stats.jitter.resets, Ordering::Relaxed);
        self.jitter_silence_gaps
            .store(stats.jitter.silence_gaps, Ordering::Relaxed);
        self.unknown_ssrc
            .store(u64::from(stats.unknown_ssrc.is_some()), Ordering::Relaxed);
        self.unresolved_ssrc.store(
            stats.unknown_ssrc.map_or(NO_SSRC, u64::from),
            Ordering::Relaxed,
        );
        self.ssrc_changes
            .store(stats.ssrc_changes, Ordering::Relaxed);
        self.reresolutions
            .store(stats.reresolutions, Ordering::Relaxed);
        self.stalled.store(u64::from(stalled), Ordering::Relaxed);
        self.stalls.store(stalls, Ordering::Relaxed);
    }
}

pub struct TapLeg {
    track: Track,
    ssrc_tracks: SsrcTracks,
    ssrc_track_updates: Arc<ArrayQueue<SsrcTracks>>,
    track_resolved: bool,
    observed_ssrc: Option<u32>,
    candidate_ssrc: Option<u32>,
    candidate_packets: u16,
    socket: UdpSocket,
    pipeline: StreamPipeline,
    samples: Vec<i16>,
    capacity_samples: usize,
    digits: [char; MAX_RECORDED_DIGITS],
    digits_recorded: usize,
    stats: LegStats,
    ptime_ms: u64,
    frames_released: u64,
    datagram_log: Vec<u8>,
    shared: Option<Arc<SharedLegStats>>,
    watchdog: Option<AudioFlowWatchdog>,
    watched_datagrams: u64,
    stalled: bool,
    stalls: u64,
    epoch: Instant,
}

impl TapLeg {
    pub fn new(
        track: Track,
        socket: UdpSocket,
        format: AudioFormat,
        target_depth_packets: u16,
        telephone_event_payload_type: Option<u8>,
        max_capture: Duration,
    ) -> Result<Self, SpikeError> {
        socket.set_nonblocking(true)?;
        let pipeline =
            StreamPipeline::new(format, target_depth_packets, telephone_event_payload_type)?;
        let capacity_samples = capture_capacity_samples(format, max_capture);
        Ok(TapLeg {
            track,
            ssrc_tracks: SsrcTracks::default(),
            ssrc_track_updates: Arc::new(ArrayQueue::new(SSRC_TRACK_COMMANDS)),
            track_resolved: true,
            observed_ssrc: None,
            candidate_ssrc: None,
            candidate_packets: 0,
            socket,
            pipeline,
            samples: Vec::with_capacity(capacity_samples),
            capacity_samples,
            digits: [' '; MAX_RECORDED_DIGITS],
            digits_recorded: 0,
            stats: LegStats::default(),
            ptime_ms: format.ptime_ms.max(1) as u64,
            frames_released: 0,
            datagram_log: Vec::new(),
            shared: None,
            watchdog: None,
            watched_datagrams: 0,
            stalled: false,
            stalls: 0,
            epoch: Instant::now(),
        })
    }

    pub fn with_datagram_log(mut self, capacity_bytes: usize) -> Self {
        self.datagram_log = Vec::with_capacity(capacity_bytes);
        self
    }

    pub fn with_shared_stats(
        mut self,
        shared: Arc<SharedLegStats>,
        stall_after: Duration,
        now: Instant,
    ) -> Self {
        self.shared = Some(shared);
        self.watchdog = Some(AudioFlowWatchdog::new(stall_after, now));
        self
    }

    pub fn publish_shared(&mut self, now: Instant) {
        if self.shared.is_none() {
            return;
        }
        if let Some(watchdog) = self.watchdog.as_mut() {
            if self.stats.datagrams > self.watched_datagrams {
                self.watched_datagrams = self.stats.datagrams;
                watchdog.touch(now);
            }
            let stalled_now = matches!(watchdog.check(now), SessionHealth::Stalled { .. });
            if stalled_now && !self.stalled {
                self.stalls += 1;
            }
            self.stalled = stalled_now;
        }
        let stats = self.stats();
        if let Some(shared) = &self.shared {
            shared.store(&stats, self.stalled, self.stalls);
        }
    }

    pub fn with_ssrc_tracks(mut self, ssrc_tracks: Vec<(u32, Track)>) -> Self {
        self.adopt_ssrc_tracks(SsrcTracks::from_pairs(&ssrc_tracks), false);
        self
    }

    pub fn ssrc_track_publisher(&self) -> SsrcTrackPublisher {
        SsrcTrackPublisher {
            queue: Arc::clone(&self.ssrc_track_updates),
        }
    }

    pub fn poll_ssrc_tracks(&mut self) {
        while let Some(tracks) = self.ssrc_track_updates.pop() {
            self.adopt_ssrc_tracks(tracks, true);
        }
    }

    fn adopt_ssrc_tracks(&mut self, ssrc_tracks: SsrcTracks, count_as_reresolution: bool) {
        self.ssrc_tracks = ssrc_tracks;
        self.track_resolved = ssrc_tracks.is_empty();
        self.name_after_observed_ssrc(count_as_reresolution);
    }

    fn observe_ssrc(&mut self) {
        let Some(ssrc) = self.pipeline.last_audio_ssrc() else {
            return;
        };
        if self.observed_ssrc == Some(ssrc) {
            self.forget_candidate();
            return;
        }
        if self.candidate_ssrc == Some(ssrc) {
            self.candidate_packets += 1;
        } else {
            self.candidate_ssrc = Some(ssrc);
            self.candidate_packets = 1;
            self.remember_ssrc(ssrc);
        }
        let first_ssrc_on_the_leg = self.observed_ssrc.is_none();
        if !first_ssrc_on_the_leg && self.candidate_packets < SSRC_CHANGE_CONFIRMATIONS {
            return;
        }
        if !first_ssrc_on_the_leg {
            self.stats.ssrc_changes += 1;
        }
        self.observed_ssrc = Some(ssrc);
        self.forget_candidate();
        self.name_after_observed_ssrc(false);
    }

    fn forget_candidate(&mut self) {
        self.candidate_ssrc = None;
        self.candidate_packets = 0;
    }

    fn name_after_observed_ssrc(&mut self, count_as_reresolution: bool) {
        let Some(ssrc) = self.observed_ssrc else {
            return;
        };
        if self.ssrc_tracks.is_empty() {
            return;
        }
        match self.ssrc_tracks.track_of(ssrc) {
            Some(track) => {
                if count_as_reresolution
                    && (self.stats.unknown_ssrc.is_some() || self.track != track)
                {
                    self.stats.reresolutions += 1;
                }
                self.track = track;
                self.track_resolved = true;
                self.stats.unknown_ssrc = None;
            }
            None => {
                self.track_resolved = false;
                self.stats.unknown_ssrc = Some(ssrc);
            }
        }
    }

    pub fn resolved_track(&self) -> Option<Track> {
        if self.track_resolved && self.stats.unknown_ssrc.is_none() {
            Some(self.track)
        } else {
            None
        }
    }

    pub fn take_remaining_track(&mut self, taken: Track) {
        if self.track_resolved && self.stats.unknown_ssrc.is_none() {
            return;
        }
        let remaining = match taken {
            Track::Customer => Track::Agent,
            Track::Agent => Track::Customer,
            Track::Mixed => return,
        };
        self.track = remaining;
        self.track_resolved = true;
    }

    fn remember_ssrc(&mut self, ssrc: u32) {
        for slot in self.stats.ssrcs_seen.iter_mut() {
            match slot {
                Some(known) if *known == ssrc => return,
                None => {
                    *slot = Some(ssrc);
                    return;
                }
                _ => {}
            }
        }
    }

    pub fn datagram_log(&self) -> &[u8] {
        &self.datagram_log
    }

    pub fn digits_seen(&self) -> String {
        self.digits[..self.digits_recorded].iter().collect()
    }

    pub fn track(&self) -> Track {
        self.track
    }

    pub fn samples(&self) -> &[i16] {
        &self.samples
    }

    pub fn stats(&self) -> LegStats {
        LegStats {
            pipeline: self.pipeline.stats(),
            jitter: self.pipeline.jitter_stats(),
            ..self.stats
        }
    }

    fn drain(&mut self, buf: &mut [u8], mut hub: Option<&mut Hub>) {
        for received in 0..MAX_DATAGRAMS_PER_DRAIN {
            match self.socket.recv_from(buf) {
                Ok((len, _from)) => {
                    self.log_datagram(&buf[..len]);
                    let arrival_micros = Instant::now()
                        .saturating_duration_since(self.epoch)
                        .as_micros() as u64;
                    let outcome = self.pipeline.ingest_at(&buf[..len], arrival_micros);
                    self.observe_ssrc();
                    if let IngestOutcome::Dtmf(digit) = outcome {
                        if self.digits_recorded < MAX_RECORDED_DIGITS {
                            self.digits[self.digits_recorded] = digit;
                            self.digits_recorded += 1;
                        }
                        if let Some(hub) = hub.as_deref_mut() {
                            hub.publish(TapEvent::Dtmf {
                                track: self.track,
                                digit,
                            });
                        }
                    }
                    self.stats.datagrams += 1;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => return,
                Err(_) => {
                    self.stats.recv_errors += 1;
                    return;
                }
            }
            if received + 1 == MAX_DATAGRAMS_PER_DRAIN {
                self.stats.drain_batches_filled += 1;
            }
        }
    }

    fn log_datagram(&mut self, datagram: &[u8]) {
        let needed = LOG_LENGTH_PREFIX + datagram.len();
        if self.datagram_log.capacity() == 0
            || self.datagram_log.len() + needed > self.datagram_log.capacity()
        {
            self.stats.datagram_log_full |= self.datagram_log.capacity() != 0;
            return;
        }
        self.datagram_log
            .extend_from_slice(&(datagram.len() as u32).to_be_bytes());
        self.datagram_log.extend_from_slice(datagram);
    }

    fn release_frame(&mut self, hub: Option<&mut Hub>) {
        let Self {
            track,
            pipeline,
            samples,
            capacity_samples,
            stats,
            ptime_ms,
            frames_released,
            ..
        } = self;
        let frame_samples = pipeline.samples_per_packet();
        let timestamp_ms = *frames_released * *ptime_ms;
        *frames_released += 1;
        match pipeline.release() {
            Playout::Pcm(pcm) | Playout::Concealed(pcm) | Playout::Suppressed(pcm) => {
                if let Some(hub) = hub {
                    hub.publish(TapEvent::media(*track, timestamp_ms, pcm));
                }
                append_within_capacity(samples, *capacity_samples, pcm, stats)
            }
            Playout::Waiting => {
                stats.underruns += 1;
                if let Some(hub) = hub {
                    let quiet = frame_samples.min(SILENCE.len());
                    hub.publish(TapEvent::media(*track, timestamp_ms, &SILENCE[..quiet]));
                }
                if samples.len() + frame_samples > *capacity_samples {
                    stats.capture_full = true;
                } else {
                    samples.resize(samples.len() + frame_samples, 0);
                }
            }
        }
    }
}

fn append_within_capacity(
    samples: &mut Vec<i16>,
    capacity_samples: usize,
    pcm: &[i16],
    stats: &mut LegStats,
) {
    if samples.len() + pcm.len() > capacity_samples {
        stats.capture_full = true;
        return;
    }
    samples.extend_from_slice(pcm);
}

fn capture_capacity_samples(format: AudioFormat, max_capture: Duration) -> usize {
    let per_second = format.sample_rate_hz as u64;
    let seconds = max_capture.as_secs_f64().ceil() as u64;
    (per_second * seconds.max(1)) as usize
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaptureSummary {
    pub releases: u64,
    pub reanchors: u64,
    pub elapsed: Duration,
}

fn settle_by_elimination(legs: &mut [TapLeg]) {
    if legs.len() != 2 {
        return;
    }
    let resolved: Vec<Option<Track>> = legs.iter().map(TapLeg::resolved_track).collect();
    match (resolved[0], resolved[1]) {
        (Some(taken), None) => legs[1].take_remaining_track(taken),
        (None, Some(taken)) => legs[0].take_remaining_track(taken),
        _ => {}
    }
}

pub fn capture(
    legs: &mut [TapLeg],
    mut hub: Option<&mut Hub>,
    format: AudioFormat,
    max_capture: Duration,
    stop: &AtomicBool,
) -> CaptureSummary {
    let ptime = Duration::from_millis(format.ptime_ms.max(1) as u64);
    let tick = ptime / 4;
    let mut buf = [0u8; MAX_DATAGRAM];
    let started = Instant::now();
    let mut next_release = started + ptime;
    let mut releases = 0u64;
    let mut reanchors = 0u64;

    while !stop.load(Ordering::Relaxed) && started.elapsed() < max_capture {
        if let Some(hub) = hub.as_deref_mut() {
            hub.poll_commands();
        }
        for leg in legs.iter_mut() {
            leg.poll_ssrc_tracks();
            leg.drain(&mut buf, hub.as_deref_mut());
        }
        settle_by_elimination(legs);

        let now = Instant::now();
        if now >= next_release {
            for leg in legs.iter_mut() {
                leg.release_frame(hub.as_deref_mut());
                leg.publish_shared(now);
            }
            if let Some(hub) = hub.as_deref_mut() {
                if let Some(samples) = format.samples_per_packet() {
                    hub.release_injected(samples as usize, format.ptime_ms.max(1) as u64);
                }
            }
            releases += 1;
            next_release += ptime;
            let now = Instant::now();
            if next_release + ptime < now {
                next_release = now + ptime;
                reanchors += 1;
            }
            continue;
        }

        let sleep_until = next_release.min(now + tick);
        if sleep_until > now {
            std::thread::sleep(sleep_until - now);
        }
    }

    let ended = Instant::now();
    for leg in legs.iter_mut() {
        leg.publish_shared(ended);
    }

    CaptureSummary {
        releases,
        reanchors,
        elapsed: started.elapsed(),
    }
}

pub fn wav_blob(format: AudioFormat, pcm: &[i16]) -> Result<Vec<u8>, SpikeError> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: format.sample_rate_hz,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut cursor = std::io::Cursor::new(Vec::new());
    let mut writer = hound::WavWriter::new(&mut cursor, spec)?;
    for sample in pcm {
        writer.write_sample(*sample)?;
    }
    writer.finalize()?;
    Ok(cursor.into_inner())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WavSummary {
    pub channels: u16,
    pub frames: usize,
}

pub fn write_wav(
    path: &Path,
    format: AudioFormat,
    legs: &[TapLeg],
) -> Result<WavSummary, SpikeError> {
    if legs.is_empty() {
        return Err(SpikeError::NoLegs);
    }
    if legs.len() > 2 {
        return Err(SpikeError::TooManyLegs(legs.len()));
    }
    for (index, leg) in legs.iter().enumerate() {
        if legs[..index].iter().any(|other| other.track == leg.track) {
            return Err(SpikeError::DuplicateTrack(leg.track));
        }
    }

    let left = legs
        .iter()
        .find(|leg| leg.track == Track::Customer)
        .unwrap_or(&legs[0]);
    let right = legs.iter().find(|leg| leg.track != left.track);
    let channels = if right.is_some() { 2 } else { 1 };

    let spec = hound::WavSpec {
        channels,
        sample_rate: format.sample_rate_hz,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut writer = hound::WavWriter::create(path, spec)?;
    let frames = match right {
        Some(right) => left.samples.len().max(right.samples.len()),
        None => left.samples.len(),
    };
    for frame in 0..frames {
        writer.write_sample(left.samples.get(frame).copied().unwrap_or(0))?;
        if let Some(right) = right {
            writer.write_sample(right.samples.get(frame).copied().unwrap_or(0))?;
        }
    }
    writer.finalize()?;
    Ok(WavSummary { channels, frames })
}

#[cfg(test)]
mod tests {
    use super::*;
    use media_core::g711;
    use media_core::replay::G711StreamGenerator;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    const TELEPHONE_EVENT_PT: u8 = 101;

    fn loopback_pair() -> (UdpSocket, SocketAddr) {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = socket.local_addr().unwrap();
        (socket, addr)
    }

    fn leg(track: Track, socket: UdpSocket, max_capture: Duration) -> TapLeg {
        TapLeg::new(
            track,
            socket,
            AudioFormat::pcmu_8k_20ms(),
            2,
            Some(TELEPHONE_EVENT_PT),
            max_capture,
        )
        .unwrap()
    }

    fn scratch_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("mss-tap-spike-{}-{}.wav", std::process::id(), name));
        path
    }

    #[test]
    fn captures_both_legs_from_real_sockets_into_aligned_buffers() {
        let max_capture = Duration::from_millis(400);
        let (customer_socket, customer_addr) = loopback_pair();
        let (agent_socket, agent_addr) = loopback_pair();
        let mut legs = vec![
            leg(Track::Customer, customer_socket, max_capture),
            leg(Track::Agent, agent_socket, max_capture),
        ];

        let sender = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let mut customer_stream =
            G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 1, 0).unwrap();
        let mut agent_stream =
            G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 2, 5000).unwrap();
        let mut first_customer_sample = None;
        for _ in 0..10 {
            let customer = customer_stream.next_datagram();
            if first_customer_sample.is_none() {
                first_customer_sample = Some(g711::ulaw_to_linear(customer[12]));
            }
            sender.send_to(&customer, customer_addr).unwrap();
            sender
                .send_to(&agent_stream.next_datagram(), agent_addr)
                .unwrap();
        }

        let stop = AtomicBool::new(false);
        let summary = capture(
            &mut legs,
            None,
            AudioFormat::pcmu_8k_20ms(),
            max_capture,
            &stop,
        );

        assert!(summary.releases >= 15, "{summary:?}");
        assert_eq!(legs[0].samples().len(), legs[1].samples().len());
        assert_eq!(legs[0].samples()[0], first_customer_sample.unwrap());
        for leg in &legs {
            let stats = leg.stats();
            assert_eq!(stats.datagrams, 10);
            assert_eq!(stats.pipeline.frames_played, 10);
            assert_eq!(stats.recv_errors, 0);
            assert!(!stats.capture_full);
        }
    }

    #[test]
    fn records_dtmf_digits_arriving_on_the_tap() {
        let max_capture = Duration::from_millis(300);
        let (socket, addr) = loopback_pair();
        let mut legs = vec![leg(Track::Customer, socket, max_capture)];

        let sender = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let mut stream = G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 3, 100).unwrap();
        sender.send_to(&stream.next_datagram(), addr).unwrap();
        for _ in 0..3 {
            let pressing = stream.next_event_datagram(TELEPHONE_EVENT_PT, [7, 0x0A, 0x01, 0x40]);
            sender.send_to(&pressing, addr).unwrap();
        }
        for _ in 0..3 {
            let released = stream.next_event_datagram(TELEPHONE_EVENT_PT, [7, 0x8A, 0x03, 0x20]);
            sender.send_to(&released, addr).unwrap();
        }

        let stop = AtomicBool::new(false);
        capture(
            &mut legs,
            None,
            AudioFormat::pcmu_8k_20ms(),
            max_capture,
            &stop,
        );

        assert_eq!(legs[0].digits_seen(), "7");
        assert_eq!(legs[0].stats().pipeline.dtmf_digits, 1);
        assert_eq!(legs[0].stats().pipeline.telephone_events, 6);
        assert_eq!(legs[0].stats().pipeline.unknown_payload_type, 0);
    }

    #[test]
    fn silence_fills_a_leg_that_never_receives_anything() {
        let max_capture = Duration::from_millis(200);
        let (socket, _) = loopback_pair();
        let mut legs = vec![leg(Track::Customer, socket, max_capture)];

        let stop = AtomicBool::new(false);
        let summary = capture(
            &mut legs,
            None,
            AudioFormat::pcmu_8k_20ms(),
            max_capture,
            &stop,
        );

        assert!(summary.releases >= 5, "{summary:?}");
        assert_eq!(legs[0].stats().underruns, summary.releases);
        assert!(legs[0].samples().iter().all(|&s| s == 0));
        assert_eq!(legs[0].samples().len(), summary.releases as usize * 160);
    }

    #[test]
    fn captured_datagrams_round_trip_into_the_replay_tier_byte_exact() {
        let max_capture = Duration::from_millis(300);
        let (socket, addr) = loopback_pair();
        let mut legs = vec![leg(Track::Customer, socket, max_capture).with_datagram_log(64 * 1024)];

        let sender = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let mut stream = G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 9, 40).unwrap();
        let sent: Vec<Vec<u8>> = (0..5).map(|_| stream.next_datagram()).collect();
        for datagram in &sent {
            sender.send_to(datagram, addr).unwrap();
        }

        let stop = AtomicBool::new(false);
        capture(
            &mut legs,
            None,
            AudioFormat::pcmu_8k_20ms(),
            max_capture,
            &stop,
        );

        let replayed: Vec<&[u8]> = media_core::replay::DatagramLog::new(legs[0].datagram_log())
            .map(|entry| entry.unwrap())
            .collect();
        assert_eq!(replayed.len(), sent.len());
        for (replayed, sent) in replayed.iter().zip(&sent) {
            assert_eq!(*replayed, sent.as_slice());
        }
        assert!(!legs[0].stats().datagram_log_full);
    }

    #[test]
    fn a_full_datagram_log_truncates_and_says_so_instead_of_growing() {
        let max_capture = Duration::from_millis(300);
        let (socket, addr) = loopback_pair();
        let mut legs = vec![leg(Track::Customer, socket, max_capture).with_datagram_log(200)];

        let sender = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let mut stream = G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 9, 40).unwrap();
        for _ in 0..4 {
            sender.send_to(&stream.next_datagram(), addr).unwrap();
        }

        let stop = AtomicBool::new(false);
        capture(
            &mut legs,
            None,
            AudioFormat::pcmu_8k_20ms(),
            max_capture,
            &stop,
        );

        assert!(legs[0].stats().datagram_log_full);
        assert!(legs[0].datagram_log().len() <= 200);
        assert_eq!(
            media_core::replay::DatagramLog::new(legs[0].datagram_log()).count(),
            1
        );
    }

    #[test]
    fn a_silent_leg_still_feeds_the_consumer_because_asr_disconnects_on_a_gap() {
        let max_capture = Duration::from_millis(200);
        let (socket, _) = loopback_pair();
        let (mut hub, client) = Hub::new();
        let mut subscription = client
            .attach(64, crate::hub::TrackSelection::Only(Track::Customer))
            .unwrap();
        let mut legs = vec![leg(Track::Customer, socket, max_capture)];

        let stop = AtomicBool::new(false);
        let summary = capture(
            &mut legs,
            Some(&mut hub),
            AudioFormat::pcmu_8k_20ms(),
            max_capture,
            &stop,
        );

        assert_eq!(legs[0].stats().underruns, summary.releases);
        let mut offered = 0;
        while subscription.try_next().is_some() {
            offered += 1;
        }
        assert_eq!(offered as u64, summary.releases);
    }

    #[test]
    fn shared_stats_follow_the_leg_and_the_watchdog_reports_a_stall() {
        let max_capture = Duration::from_millis(400);
        let (socket, addr) = loopback_pair();
        let shared = Arc::new(SharedLegStats::default());
        let mut legs = vec![leg(Track::Customer, socket, max_capture).with_shared_stats(
            Arc::clone(&shared),
            Duration::from_millis(120),
            Instant::now(),
        )];

        let sender = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let mut stream = G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), 4, 0).unwrap();
        for _ in 0..5 {
            sender.send_to(&stream.next_datagram(), addr).unwrap();
        }

        let stop = AtomicBool::new(false);
        capture(
            &mut legs,
            None,
            AudioFormat::pcmu_8k_20ms(),
            max_capture,
            &stop,
        );

        assert_eq!(shared.datagrams.load(Ordering::Relaxed), 5);
        assert_eq!(shared.frames_played.load(Ordering::Relaxed), 5);
        assert_eq!(shared.unknown_ssrc.load(Ordering::Relaxed), 0);
        assert_eq!(shared.stalled.load(Ordering::Relaxed), 1);
        assert_eq!(shared.stalls.load(Ordering::Relaxed), 1);
        assert!(shared.underruns.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn stop_flag_ends_capture_before_max_capture() {
        let (socket, _) = loopback_pair();
        let mut legs = vec![leg(Track::Customer, socket, Duration::from_secs(30))];

        let stop = AtomicBool::new(true);
        let summary = capture(
            &mut legs,
            None,
            AudioFormat::pcmu_8k_20ms(),
            Duration::from_secs(30),
            &stop,
        );

        assert_eq!(summary.releases, 0);
        assert!(summary.elapsed < Duration::from_secs(1));
    }

    #[test]
    fn capture_stops_growing_at_its_capacity_and_says_so() {
        let max_capture = Duration::from_millis(200);
        let (socket, _) = loopback_pair();
        let mut legs = vec![leg(Track::Customer, socket, max_capture)];
        let capacity = legs[0].capacity_samples;

        let stop = AtomicBool::new(false);
        capture(
            &mut legs,
            None,
            AudioFormat::pcmu_8k_20ms(),
            Duration::from_millis(1400),
            &stop,
        );

        assert!(legs[0].stats().capture_full);
        assert!(legs[0].samples().len() <= capacity);
    }

    #[test]
    fn stereo_wav_puts_customer_left_and_agent_right() {
        let max_capture = Duration::from_millis(100);
        let (customer_socket, _) = loopback_pair();
        let (agent_socket, _) = loopback_pair();
        let mut customer = leg(Track::Customer, customer_socket, max_capture);
        let mut agent = leg(Track::Agent, agent_socket, max_capture);
        customer.samples.extend_from_slice(&[100, 200, 300]);
        agent.samples.extend_from_slice(&[-100, -200]);

        let path = scratch_path("stereo");
        let summary = write_wav(&path, AudioFormat::pcmu_8k_20ms(), &[agent, customer]).unwrap();
        assert_eq!(summary.channels, 2);
        assert_eq!(summary.frames, 3);

        let mut reader = hound::WavReader::open(&path).unwrap();
        assert_eq!(reader.spec().channels, 2);
        assert_eq!(reader.spec().sample_rate, 8000);
        let samples: Vec<i16> = reader.samples::<i16>().map(|s| s.unwrap()).collect();
        assert_eq!(samples, vec![100, -100, 200, -200, 300, 0]);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn single_leg_writes_mono_and_duplicate_tracks_are_rejected() {
        let max_capture = Duration::from_millis(100);
        let (socket, _) = loopback_pair();
        let mut only = leg(Track::Customer, socket, max_capture);
        only.samples.extend_from_slice(&[7, 8]);

        let path = scratch_path("mono");
        let summary = write_wav(&path, AudioFormat::pcmu_8k_20ms(), &[only]).unwrap();
        assert_eq!(summary.channels, 1);
        let mut reader = hound::WavReader::open(&path).unwrap();
        let samples: Vec<i16> = reader.samples::<i16>().map(|s| s.unwrap()).collect();
        assert_eq!(samples, vec![7, 8]);
        std::fs::remove_file(&path).ok();

        let (a, _) = loopback_pair();
        let (b, _) = loopback_pair();
        let legs = [
            leg(Track::Customer, a, max_capture),
            leg(Track::Customer, b, max_capture),
        ];
        match write_wav(&scratch_path("dupe"), AudioFormat::pcmu_8k_20ms(), &legs) {
            Err(SpikeError::DuplicateTrack(Track::Customer)) => {}
            other => panic!("expected duplicate track rejection, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod ssrc_track_tests {
    use super::*;
    use media_core::replay::G711StreamGenerator;
    use std::net::UdpSocket;
    use std::time::Duration;

    fn leg_with_map(map: Vec<(u32, Track)>) -> (TapLeg, UdpSocket) {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        sender.connect(socket.local_addr().unwrap()).unwrap();
        let leg = TapLeg::new(
            Track::Customer,
            socket,
            AudioFormat::pcmu_8k_20ms(),
            1,
            Some(101),
            Duration::from_secs(1),
        )
        .unwrap()
        .with_ssrc_tracks(map);
        (leg, sender)
    }

    fn feed(leg: &mut TapLeg, sender: &UdpSocket, ssrc: u32, frames: u16) {
        let mut stream = G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), ssrc, 100).unwrap();
        let mut buf = [0u8; 2048];
        for _ in 0..frames {
            sender.send(&stream.next_datagram()).unwrap();
            std::thread::sleep(Duration::from_millis(2));
            leg.drain(&mut buf, None);
        }
    }

    #[test]
    fn the_first_audio_packet_names_the_leg_after_its_speaker() {
        let (mut leg, sender) = leg_with_map(vec![
            (0xAAAA_0001, Track::Agent),
            (0xBBBB_0002, Track::Customer),
        ]);
        assert_eq!(leg.track(), Track::Customer);

        feed(&mut leg, &sender, 0xAAAA_0001, 3);

        assert_eq!(leg.track(), Track::Agent);
        assert_eq!(leg.stats().unknown_ssrc, None);
    }

    #[test]
    fn an_ssrc_the_map_never_promised_keeps_the_default_and_reports_itself() {
        let (mut leg, sender) = leg_with_map(vec![(0xAAAA_0001, Track::Agent)]);

        feed(&mut leg, &sender, 0xDEAD_BEEF, 3);

        assert_eq!(leg.track(), Track::Customer);
        assert_eq!(leg.stats().unknown_ssrc, Some(0xDEAD_BEEF));
    }

    #[test]
    fn an_empty_map_means_positional_naming_with_no_resolution_pass() {
        let (mut leg, sender) = leg_with_map(Vec::new());

        feed(&mut leg, &sender, 0xCCCC_0003, 2);

        assert_eq!(leg.track(), Track::Customer);
        assert_eq!(leg.stats().unknown_ssrc, None);
    }
}

#[cfg(test)]
mod reresolution_tests {
    use super::*;
    use media_core::replay::G711StreamGenerator;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};

    const CUSTOMER_SSRC: u32 = 0xCA11_0001;
    const AGENT_SSRC: u32 = 0xA6E0_0002;
    const REINVITE_SSRC: u32 = 0x5EED_0003;
    const STRANGER_SSRC: u32 = 0xDEAD_BEEF;

    struct Wire {
        sender: UdpSocket,
        address: SocketAddr,
    }

    fn leg_and_wire(map: &[(u32, Track)]) -> (TapLeg, Wire) {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = socket.local_addr().unwrap();
        let leg = TapLeg::new(
            Track::Customer,
            socket,
            AudioFormat::pcmu_8k_20ms(),
            1,
            Some(101),
            Duration::from_secs(2),
        )
        .unwrap()
        .with_ssrc_tracks(map.to_vec());
        let sender = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        (leg, Wire { sender, address })
    }

    fn speaker(ssrc: u32, first_sequence: u16) -> G711StreamGenerator {
        G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), ssrc, first_sequence).unwrap()
    }

    fn send(wire: &Wire, stream: &mut G711StreamGenerator, packets: usize) {
        for _ in 0..packets {
            wire.sender
                .send_to(&stream.next_datagram(), wire.address)
                .unwrap();
        }
    }

    fn run(legs: &mut [TapLeg], window: Duration) {
        let stop = AtomicBool::new(false);
        capture(legs, None, AudioFormat::pcmu_8k_20ms(), window, &stop);
    }

    #[test]
    fn a_mid_call_ssrc_change_is_renamed_by_a_fresh_speaker_map() {
        let known = [(CUSTOMER_SSRC, Track::Customer), (AGENT_SSRC, Track::Agent)];
        let (customer_leg, customer_wire) = leg_and_wire(&known);
        let (agent_leg, agent_wire) = leg_and_wire(&known);
        let publishers = [
            customer_leg.ssrc_track_publisher(),
            agent_leg.ssrc_track_publisher(),
        ];
        let mut legs = vec![customer_leg, agent_leg];

        let mut customer = speaker(CUSTOMER_SSRC, 100);
        let mut agent = speaker(AGENT_SSRC, 7000);
        send(&customer_wire, &mut customer, 5);
        send(&agent_wire, &mut agent, 5);
        run(&mut legs, Duration::from_millis(200));

        assert_eq!(legs[0].resolved_track(), Some(Track::Customer));
        assert_eq!(legs[1].resolved_track(), Some(Track::Agent));

        let mut reinvited = speaker(REINVITE_SSRC, 5000);
        send(&customer_wire, &mut reinvited, 5);
        run(&mut legs, Duration::from_millis(200));

        let changed = legs[0].stats();
        assert_eq!(changed.unknown_ssrc, Some(REINVITE_SSRC));
        assert_eq!(changed.ssrc_changes, 1);
        assert_eq!(changed.jitter.resets, 1);
        assert_eq!(legs[0].resolved_track(), None);
        assert_eq!(legs[0].track(), Track::Customer);

        let refreshed =
            SsrcTracks::from_pairs(&[(REINVITE_SSRC, Track::Customer), (AGENT_SSRC, Track::Agent)]);
        for publisher in &publishers {
            assert!(publisher.publish(refreshed));
        }
        send(&customer_wire, &mut reinvited, 5);
        run(&mut legs, Duration::from_millis(200));

        assert_eq!(legs[0].resolved_track(), Some(Track::Customer));
        assert_eq!(legs[1].resolved_track(), Some(Track::Agent));
        let recovered = legs[0].stats();
        assert_eq!(recovered.unknown_ssrc, None);
        assert_eq!(recovered.reresolutions, 1);
        assert_eq!(recovered.ssrc_changes, 1);
        assert!(recovered.pipeline.frames_played >= 10, "{recovered:?}");
        assert_eq!(legs[1].stats().reresolutions, 0);
    }

    #[test]
    fn a_leg_that_starts_carrying_the_other_speaker_flips_both_names() {
        let known = [(CUSTOMER_SSRC, Track::Customer), (AGENT_SSRC, Track::Agent)];
        let (first, first_wire) = leg_and_wire(&known);
        let (second, second_wire) = leg_and_wire(&known);
        let mut legs = vec![first, second];

        let mut customer = speaker(CUSTOMER_SSRC, 100);
        let mut stranger = speaker(STRANGER_SSRC, 300);
        send(&first_wire, &mut customer, 3);
        send(&second_wire, &mut stranger, 3);
        run(&mut legs, Duration::from_millis(160));

        assert_eq!(legs[0].resolved_track(), Some(Track::Customer));
        assert_eq!(legs[1].track(), Track::Agent);
        assert_eq!(legs[1].resolved_track(), None);

        let mut agent = speaker(AGENT_SSRC, 5000);
        send(&first_wire, &mut agent, 4);
        run(&mut legs, Duration::from_millis(160));

        assert_eq!(legs[0].resolved_track(), Some(Track::Agent));
        assert_eq!(legs[0].stats().ssrc_changes, 1);
        assert_eq!(legs[1].track(), Track::Customer);
    }

    #[test]
    fn two_ssrcs_interleaved_on_one_leg_never_rename_it() {
        let known = [(CUSTOMER_SSRC, Track::Customer), (AGENT_SSRC, Track::Agent)];
        let (mut leg, wire) = leg_and_wire(&known);
        let mut buf = [0u8; MAX_DATAGRAM];
        let mut customer = speaker(CUSTOMER_SSRC, 100);
        let mut intruder = speaker(AGENT_SSRC, 900);

        send(&wire, &mut customer, 1);
        std::thread::sleep(Duration::from_millis(5));
        leg.drain(&mut buf, None);
        assert_eq!(leg.resolved_track(), Some(Track::Customer));

        for _ in 0..6 {
            send(&wire, &mut intruder, 1);
            send(&wire, &mut customer, 1);
            std::thread::sleep(Duration::from_millis(5));
            leg.drain(&mut buf, None);
        }

        let stats = leg.stats();
        assert_eq!(leg.resolved_track(), Some(Track::Customer));
        assert_eq!(stats.ssrc_changes, 0);
        assert_eq!(stats.ssrcs_seen[0], Some(CUSTOMER_SSRC));
        assert_eq!(stats.ssrcs_seen[1], Some(AGENT_SSRC));
    }

    #[test]
    fn the_control_world_reads_the_unresolved_ssrc_and_watches_it_recover() {
        let shared = Arc::new(SharedLegStats::default());
        assert_eq!(shared.unresolved_ssrc(), None);

        let (leg, wire) = leg_and_wire(&[(CUSTOMER_SSRC, Track::Customer)]);
        let publisher = leg.ssrc_track_publisher();
        let mut leg =
            leg.with_shared_stats(Arc::clone(&shared), Duration::from_secs(10), Instant::now());

        let mut stranger = speaker(STRANGER_SSRC, 40);
        send(&wire, &mut stranger, 2);
        let mut buf = [0u8; MAX_DATAGRAM];
        std::thread::sleep(Duration::from_millis(5));
        leg.drain(&mut buf, None);
        leg.publish_shared(Instant::now());

        assert_eq!(shared.unresolved_ssrc(), Some(STRANGER_SSRC));
        assert_eq!(shared.unknown_ssrc.load(Ordering::Relaxed), 1);

        assert!(publisher.publish(SsrcTracks::from_pairs(&[(STRANGER_SSRC, Track::Agent)])));
        leg.poll_ssrc_tracks();
        leg.publish_shared(Instant::now());

        assert_eq!(leg.resolved_track(), Some(Track::Agent));
        assert_eq!(shared.unresolved_ssrc(), None);
        assert_eq!(shared.unknown_ssrc.load(Ordering::Relaxed), 0);
        assert_eq!(shared.reresolutions.load(Ordering::Relaxed), 1);
        assert_eq!(shared.ssrc_changes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn the_newest_speaker_map_wins_when_the_capture_loop_is_behind() {
        let (mut leg, wire) = leg_and_wire(&[(CUSTOMER_SSRC, Track::Customer)]);
        let publisher = leg.ssrc_track_publisher();
        let mut stranger = speaker(STRANGER_SSRC, 10);
        send(&wire, &mut stranger, 2);
        let mut buf = [0u8; MAX_DATAGRAM];
        std::thread::sleep(Duration::from_millis(5));
        leg.drain(&mut buf, None);
        assert_eq!(leg.stats().unknown_ssrc, Some(STRANGER_SSRC));

        let stale = SsrcTracks::from_pairs(&[(CUSTOMER_SSRC, Track::Customer)]);
        for _ in 0..SSRC_TRACK_COMMANDS {
            assert!(publisher.publish(stale));
        }
        let fresh = SsrcTracks::from_pairs(&[(STRANGER_SSRC, Track::Agent)]);
        assert!(!publisher.publish(fresh));

        leg.poll_ssrc_tracks();
        assert_eq!(leg.resolved_track(), Some(Track::Agent));
        assert_eq!(leg.stats().reresolutions, 1);
    }

    #[test]
    fn a_speaker_map_holds_what_a_tapped_call_can_report_and_no_more() {
        let empty = SsrcTracks::default();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.track_of(CUSTOMER_SSRC), None);

        let pairs: Vec<(u32, Track)> = (0..MAX_SSRC_TRACKS as u32 + 2)
            .map(|index| (index, Track::Agent))
            .collect();
        let overflowing = SsrcTracks::from_pairs(&pairs);
        assert_eq!(overflowing.len(), MAX_SSRC_TRACKS);
        assert_eq!(overflowing.track_of(0), Some(Track::Agent));
        assert_eq!(overflowing.track_of(MAX_SSRC_TRACKS as u32 + 1), None);

        let two =
            SsrcTracks::from_pairs(&[(CUSTOMER_SSRC, Track::Customer), (AGENT_SSRC, Track::Agent)]);
        assert!(!two.is_empty());
        assert_eq!(two.track_of(AGENT_SSRC), Some(Track::Agent));
        assert_eq!(two.track_of(REINVITE_SSRC), None);
    }
}

#[cfg(test)]
mod elimination_tests {
    use super::*;
    use media_core::replay::G711StreamGenerator;
    use std::net::UdpSocket;
    use std::time::Duration;

    fn leg(map: Vec<(u32, Track)>) -> (TapLeg, UdpSocket) {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        sender.connect(socket.local_addr().unwrap()).unwrap();
        let leg = TapLeg::new(
            Track::Customer,
            socket,
            AudioFormat::pcmu_8k_20ms(),
            1,
            Some(101),
            Duration::from_secs(1),
        )
        .unwrap()
        .with_ssrc_tracks(map);
        (leg, sender)
    }

    fn feed(leg: &mut TapLeg, sender: &UdpSocket, ssrc: u32) {
        let mut stream = G711StreamGenerator::new(AudioFormat::pcmu_8k_20ms(), ssrc, 50).unwrap();
        let mut buf = [0u8; 2048];
        for _ in 0..3 {
            sender.send(&stream.next_datagram()).unwrap();
            std::thread::sleep(Duration::from_millis(2));
            leg.drain(&mut buf, None);
        }
    }

    #[test]
    fn one_recognized_leg_names_the_other_by_elimination() {
        let map = vec![(0xCA11_0001, Track::Customer)];
        let (mut known, known_sender) = leg(map.clone());
        let (mut mystery, mystery_sender) = leg(map);

        feed(&mut known, &known_sender, 0xCA11_0001);
        feed(&mut mystery, &mystery_sender, 0x0DD_BA11);
        assert_eq!(known.resolved_track(), Some(Track::Customer));
        assert_eq!(mystery.resolved_track(), None);
        assert_eq!(mystery.stats().unknown_ssrc, Some(0x0DD_BA11));

        let mut legs = vec![known, mystery];
        settle_by_elimination(&mut legs);

        assert_eq!(legs[0].track(), Track::Customer);
        assert_eq!(legs[1].track(), Track::Agent);
    }

    #[test]
    fn two_unrecognized_legs_stay_as_named_rather_than_guessing() {
        let map = vec![(0xCA11_0001, Track::Customer)];
        let (mut first, first_sender) = leg(map.clone());
        let (mut second, second_sender) = leg(map);

        feed(&mut first, &first_sender, 0x1111_1111);
        feed(&mut second, &second_sender, 0x2222_2222);

        let mut legs = vec![first, second];
        settle_by_elimination(&mut legs);
        assert_eq!(legs[0].track(), Track::Customer);
        assert_eq!(legs[1].track(), Track::Customer);
        assert!(legs.iter().all(|leg| leg.stats().unknown_ssrc.is_some()));
    }
}
