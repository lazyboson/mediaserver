use media_core::jitter;
use media_core::pipeline::{PipelineError, PipelineStats, Playout, StreamPipeline};
use media_core::{AudioFormat, Track};
use std::io::ErrorKind;
use std::net::UdpSocket;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;

const MAX_DATAGRAM: usize = 2048;
const MAX_DATAGRAMS_PER_DRAIN: usize = 64;

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
    pub pipeline: PipelineStats,
    pub jitter: jitter::Stats,
}

pub struct TapLeg {
    track: Track,
    socket: UdpSocket,
    pipeline: StreamPipeline,
    samples: Vec<i16>,
    capacity_samples: usize,
    stats: LegStats,
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
            socket,
            pipeline,
            samples: Vec::with_capacity(capacity_samples),
            capacity_samples,
            stats: LegStats::default(),
        })
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

    fn drain(&mut self, buf: &mut [u8]) {
        for received in 0..MAX_DATAGRAMS_PER_DRAIN {
            match self.socket.recv_from(buf) {
                Ok((len, _from)) => {
                    self.pipeline.ingest(&buf[..len]);
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

    fn release_frame(&mut self) {
        let Self {
            pipeline,
            samples,
            capacity_samples,
            stats,
            ..
        } = self;
        let frame_samples = pipeline.samples_per_packet();
        match pipeline.release() {
            Playout::Pcm(pcm) | Playout::Concealed(pcm) => {
                append_within_capacity(samples, *capacity_samples, pcm, stats)
            }
            Playout::Waiting => {
                stats.underruns += 1;
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

pub fn capture(
    legs: &mut [TapLeg],
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
        for leg in legs.iter_mut() {
            leg.drain(&mut buf);
        }

        let now = Instant::now();
        if now >= next_release {
            for leg in legs.iter_mut() {
                leg.release_frame();
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
        let summary = capture(&mut legs, AudioFormat::pcmu_8k_20ms(), max_capture, &stop);

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
    fn silence_fills_a_leg_that_never_receives_anything() {
        let max_capture = Duration::from_millis(200);
        let (socket, _) = loopback_pair();
        let mut legs = vec![leg(Track::Customer, socket, max_capture)];

        let stop = AtomicBool::new(false);
        let summary = capture(&mut legs, AudioFormat::pcmu_8k_20ms(), max_capture, &stop);

        assert!(summary.releases >= 5, "{summary:?}");
        assert_eq!(legs[0].stats().underruns, summary.releases);
        assert!(legs[0].samples().iter().all(|&s| s == 0));
        assert_eq!(legs[0].samples().len(), summary.releases as usize * 160);
    }

    #[test]
    fn stop_flag_ends_capture_before_max_capture() {
        let (socket, _) = loopback_pair();
        let mut legs = vec![leg(Track::Customer, socket, Duration::from_secs(30))];

        let stop = AtomicBool::new(true);
        let summary = capture(
            &mut legs,
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
