use crate::hub::{Hub, TapEvent};
use media_core::jitter;
use media_core::pipeline::{IngestOutcome, PipelineError, PipelineStats, Playout, StreamPipeline};
use media_core::{AudioFormat, Track};
use std::io::ErrorKind;
use std::net::UdpSocket;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;

const MAX_DATAGRAM: usize = 2048;
const MAX_DATAGRAMS_PER_DRAIN: usize = 64;
const MAX_RECORDED_DIGITS: usize = 32;
const SILENCE: [i16; 480] = [0; 480];
const LOG_LENGTH_PREFIX: usize = 4;

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
    pub pipeline: PipelineStats,
    pub jitter: jitter::Stats,
}

pub struct TapLeg {
    track: Track,
    ssrc_tracks: Vec<(u32, Track)>,
    track_resolved: bool,
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
            ssrc_tracks: Vec::new(),
            track_resolved: true,
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
        })
    }

    pub fn with_datagram_log(mut self, capacity_bytes: usize) -> Self {
        self.datagram_log = Vec::with_capacity(capacity_bytes);
        self
    }

    pub fn with_ssrc_tracks(mut self, ssrc_tracks: Vec<(u32, Track)>) -> Self {
        self.track_resolved = ssrc_tracks.is_empty();
        self.ssrc_tracks = ssrc_tracks;
        self
    }

    fn resolve_track(&mut self) {
        let Some(ssrc) = self.pipeline.last_audio_ssrc() else {
            return;
        };
        self.remember_ssrc(ssrc);
        if self.track_resolved {
            return;
        }
        self.track_resolved = true;
        match self.ssrc_tracks.iter().find(|(known, _)| *known == ssrc) {
            Some((_, track)) => self.track = *track,
            None => self.stats.unknown_ssrc = Some(ssrc),
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
                    let outcome = self.pipeline.ingest(&buf[..len]);
                    self.resolve_track();
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
                    hub.publish(TapEvent::media(*track, timestamp_ms, pcm, true));
                }
                append_within_capacity(samples, *capacity_samples, pcm, stats)
            }
            Playout::Waiting => {
                stats.underruns += 1;
                if let Some(hub) = hub {
                    let quiet = frame_samples.min(SILENCE.len());
                    hub.publish(TapEvent::media(
                        *track,
                        timestamp_ms,
                        &SILENCE[..quiet],
                        true,
                    ));
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
            leg.drain(&mut buf, hub.as_deref_mut());
        }
        settle_by_elimination(legs);

        let now = Instant::now();
        if now >= next_release {
            for leg in legs.iter_mut() {
                leg.release_frame(hub.as_deref_mut());
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
