use crate::hub::{Hub, TapEvent};
use crate::inline_leg::InlineEgress;
use crate::tap_spike::{TapLeg, MAX_DATAGRAM};
use crossbeam_queue::ArrayQueue;
use media_core::{
    AudioFormat, ContributorId, Gain, ListenerId, MixMatrix, Party, SpeechGate, Track,
};
use session_core::mix::{MixRoute, MixTarget};
use session_core::SessionId;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::{info, warn};

pub const COMMAND_CAPACITY: usize = 64;
pub const MAX_CONFERENCE_MEMBERS: usize = 32;
const INITIAL_SLOTS: usize = 8;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ConferenceTotals {
    pub members_live: u64,
    pub joins: u64,
    pub leaves: u64,
    pub ticks: u64,
    pub mixed_frames: u64,
    pub clipped_samples: u64,
    pub absent_frames: u64,
    pub reanchors: u64,
    pub frames_refused: u64,
    pub route_changes: u64,
    pub whispers_live: u64,
}

#[derive(Default)]
pub struct ConferenceShared {
    pub members_live: AtomicU64,
    pub joins: AtomicU64,
    pub leaves: AtomicU64,
    pub ticks: AtomicU64,
    pub mixed_frames: AtomicU64,
    pub clipped_samples: AtomicU64,
    pub absent_frames: AtomicU64,
    pub reanchors: AtomicU64,
    pub frames_refused: AtomicU64,
    pub route_changes: AtomicU64,
    pub whispers_live: AtomicU64,
}

impl ConferenceTotals {
    pub fn add_shared(&mut self, shared: &ConferenceShared) {
        let read = |value: &AtomicU64| value.load(Ordering::Relaxed);
        self.members_live += read(&shared.members_live);
        self.joins += read(&shared.joins);
        self.leaves += read(&shared.leaves);
        self.ticks += read(&shared.ticks);
        self.mixed_frames += read(&shared.mixed_frames);
        self.clipped_samples += read(&shared.clipped_samples);
        self.absent_frames += read(&shared.absent_frames);
        self.reanchors += read(&shared.reanchors);
        self.frames_refused += read(&shared.frames_refused);
        self.route_changes += read(&shared.route_changes);
        self.whispers_live += read(&shared.whispers_live);
    }
}

pub struct ConferenceMember {
    pub session: SessionId,
    pub external_id: String,
    pub leg: TapLeg,
    pub hub: Hub,
    pub egress: InlineEgress,
}

pub enum ConferenceCommand {
    Join(Box<ConferenceMember>),
    Leave(SessionId),
    Route { session: SessionId, route: MixRoute },
}

pub struct Conference {
    format: AudioFormat,
    commands: Arc<ArrayQueue<ConferenceCommand>>,
    stop: Arc<AtomicBool>,
    shared: Arc<ConferenceShared>,
    thread: Option<JoinHandle<()>>,
    members: Vec<SessionId>,
}

impl Conference {
    pub fn start(name: &str, format: AudioFormat) -> Result<Conference, ConferenceError> {
        let frame_samples = format
            .samples_per_packet()
            .ok_or(ConferenceError::Format(format))? as usize;
        let commands = Arc::new(ArrayQueue::new(COMMAND_CAPACITY));
        let stop = Arc::new(AtomicBool::new(false));
        let shared = Arc::new(ConferenceShared::default());
        let mixed = Mixed {
            name: name.to_string(),
            format,
            frame_samples,
            commands: Arc::clone(&commands),
            shared: Arc::clone(&shared),
            stop: Arc::clone(&stop),
        };
        let thread = std::thread::Builder::new()
            .name(format!("mss-conf-{name}"))
            .spawn(move || mixed.run())
            .map_err(|error| ConferenceError::Thread(error.to_string()))?;
        Ok(Conference {
            format,
            commands,
            stop,
            shared,
            thread: Some(thread),
            members: Vec::new(),
        })
    }

    pub fn shared(&self) -> Arc<ConferenceShared> {
        Arc::clone(&self.shared)
    }

    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    pub fn accepts(&self, format: AudioFormat) -> Result<(), ConferenceError> {
        if format.sample_rate_hz != self.format.sample_rate_hz
            || format.ptime_ms != self.format.ptime_ms
            || format.channels != self.format.channels
        {
            return Err(ConferenceError::Mismatch {
                conference: self.format,
                leg: format,
            });
        }
        if self.members.len() >= MAX_CONFERENCE_MEMBERS {
            return Err(ConferenceError::Full(MAX_CONFERENCE_MEMBERS));
        }
        Ok(())
    }

    pub fn seat(&mut self, member: ConferenceMember) -> Result<(), ConferenceError> {
        let session = member.session;
        self.commands
            .push(ConferenceCommand::Join(Box::new(member)))
            .map_err(|_| ConferenceError::Busy)?;
        self.members.push(session);
        Ok(())
    }

    pub fn route(&mut self, session: SessionId, route: MixRoute) -> Result<(), ConferenceError> {
        if !self.members.contains(&session) {
            return Err(ConferenceError::NotSeated(session));
        }
        self.commands
            .push(ConferenceCommand::Route { session, route })
            .map_err(|_| ConferenceError::Busy)
    }

    pub fn unseat(&mut self, session: SessionId) -> Option<JoinHandle<()>> {
        self.members.retain(|held| *held != session);
        if self
            .commands
            .push(ConferenceCommand::Leave(session))
            .is_err()
        {
            warn!(%session, "the conference command queue is full; stopping the mix instead");
            self.members.clear();
        }
        if self.members.is_empty() {
            self.stop.store(true, Ordering::Relaxed);
            return self.thread.take();
        }
        None
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConferenceError {
    #[error("{0:?} has no frame size a conference can mix")]
    Format(AudioFormat),
    #[error(
        "this conference mixes {}/{} ms frames and the leg negotiated {}/{} ms; \
         every member must negotiate the same rate and ptime, because a conference \
         has one clock and no per-leg resampler",
        conference.sample_rate_hz,
        conference.ptime_ms,
        leg.sample_rate_hz,
        leg.ptime_ms
    )]
    Mismatch {
        conference: AudioFormat,
        leg: AudioFormat,
    },
    #[error("this conference already holds {0} members")]
    Full(usize),
    #[error("the conference is not taking commands fast enough to seat another leg")]
    Busy,
    #[error("{0} is not seated in this conference")]
    NotSeated(SessionId),
    #[error("conference thread: {0}")]
    Thread(String),
}

struct Mixed {
    name: String,
    format: AudioFormat,
    frame_samples: usize,
    commands: Arc<ArrayQueue<ConferenceCommand>>,
    shared: Arc<ConferenceShared>,
    stop: Arc<AtomicBool>,
}

struct Seated {
    session: SessionId,
    external_id: String,
    leg: TapLeg,
    hub: Hub,
    egress: InlineEgress,
    party: Party,
    injector: ContributorId,
    inject: InjectFeed,
    route: MixRoute,
}

struct InjectFeed {
    frame: Vec<i16>,
    pending: Option<Vec<i16>>,
    at: usize,
}

impl InjectFeed {
    fn with_frame(frame_samples: usize) -> InjectFeed {
        InjectFeed {
            frame: vec![0; frame_samples],
            pending: None,
            at: 0,
        }
    }

    fn discard(&mut self, egress: &mut InlineEgress) {
        if let Some(pending) = self.pending.take() {
            egress.discard_pending(pending.len().saturating_sub(self.at) as u64);
        }
        self.at = 0;
    }

    fn take_frame(&mut self, egress: &mut InlineEgress) -> Option<&[i16]> {
        let mut filled = 0;
        while filled < self.frame.len() {
            if self.pending.is_none() {
                self.pending = egress.pop_chunk();
                self.at = 0;
            }
            let Some(chunk) = self.pending.as_ref() else {
                break;
            };
            let take = (chunk.len() - self.at).min(self.frame.len() - filled);
            self.frame[filled..filled + take].copy_from_slice(&chunk[self.at..self.at + take]);
            filled += take;
            self.at += take;
            if self.at >= chunk.len() {
                self.pending = None;
                self.at = 0;
            }
        }
        if filled == 0 {
            return None;
        }
        for sample in self.frame[filled..].iter_mut() {
            *sample = 0;
        }
        egress.account_mixed_in(filled as u64);
        Some(&self.frame)
    }
}

impl Mixed {
    fn run(self) {
        let mut matrix = match MixMatrix::with_capacity(
            self.frame_samples,
            SpeechGate::default(),
            INITIAL_SLOTS,
            INITIAL_SLOTS,
        ) {
            Ok(matrix) => matrix,
            Err(error) => {
                warn!(conference = %self.name, %error, "this conference cannot mix its frame size");
                return;
            }
        };
        let monitor = matrix.join_listener();
        let ptime = Duration::from_millis(self.format.ptime_ms.max(1) as u64);
        let poll = ptime / 4;
        let mut buf = [0u8; MAX_DATAGRAM];
        let started = Instant::now();
        let mut next_release = started + ptime;
        let mut seated: Vec<Seated> = Vec::new();
        let mut frames = 0u64;
        let mut refused = 0u64;
        let mut reanchors = 0u64;

        info!(
            conference = %self.name,
            frame_samples = self.frame_samples,
            sample_rate_hz = self.format.sample_rate_hz,
            ptime_ms = self.format.ptime_ms,
            "opened a conference; one clock mixes every leg that joins it"
        );

        while !self.stop.load(Ordering::Relaxed) {
            while let Some(command) = self.commands.pop() {
                match command {
                    ConferenceCommand::Join(member) => {
                        let member = *member;
                        let party = matrix.join_party();
                        let injector = matrix.join_contributor();
                        let session = member.session;
                        seated.push(Seated {
                            session,
                            external_id: member.external_id,
                            leg: member.leg,
                            hub: member.hub,
                            egress: member.egress,
                            party,
                            injector,
                            inject: InjectFeed::with_frame(self.frame_samples),
                            route: MixRoute::private(),
                        });
                        reroute_injectors(&mut matrix, &seated, monitor, &self.name);
                        self.shared.joins.fetch_add(1, Ordering::Relaxed);
                        self.shared
                            .members_live
                            .store(seated.len() as u64, Ordering::Relaxed);
                        info!(
                            conference = %self.name,
                            %session,
                            members = seated.len(),
                            "a leg joined the conference"
                        );
                    }
                    ConferenceCommand::Leave(session) => {
                        if let Some(at) = seated.iter().position(|held| held.session == session) {
                            let leaving = seated.remove(at);
                            let _ = matrix.leave_party(leaving.party);
                            let _ = matrix.leave_contributor(leaving.injector);
                            reroute_injectors(&mut matrix, &seated, monitor, &self.name);
                            report_leg(&self.name, &leaving);
                            self.shared.leaves.fetch_add(1, Ordering::Relaxed);
                            self.shared
                                .members_live
                                .store(seated.len() as u64, Ordering::Relaxed);
                            info!(
                                conference = %self.name,
                                %session,
                                members = seated.len(),
                                "a leg left the conference; the mix keeps its clock"
                            );
                        }
                    }
                    ConferenceCommand::Route { session, route } => {
                        if let Some(member) = seated.iter_mut().find(|held| held.session == session)
                        {
                            let target = route.target_name().to_string();
                            let monitor_audible = route.monitor_audible;
                            member.route = route;
                            reroute_injectors(&mut matrix, &seated, monitor, &self.name);
                            self.shared.route_changes.fetch_add(1, Ordering::Relaxed);
                            info!(
                                conference = %self.name,
                                %session,
                                %target,
                                monitor_audible,
                                "this leg's injected audio has a new route into the mix"
                            );
                        } else {
                            warn!(
                                conference = %self.name,
                                %session,
                                "a route arrived for a leg that is no longer seated"
                            );
                        }
                    }
                }
            }
            self.shared.whispers_live.store(
                seated
                    .iter()
                    .filter(|held| !held.route.is_private())
                    .count() as u64,
                Ordering::Relaxed,
            );

            for member in seated.iter_mut() {
                member.hub.poll_commands();
                member.leg.poll_ssrc_tracks();
                member.leg.drain(&mut buf, Some(&mut member.hub));
                if member.egress.take_flush() {
                    member.inject.discard(&mut member.egress);
                }
            }

            let now = Instant::now();
            for member in seated.iter_mut() {
                member.egress.send_tick(now);
            }

            if now >= next_release {
                let timestamp_ms = frames * self.format.ptime_ms.max(1) as u64;
                for member in seated.iter_mut() {
                    let contributor = member.party.contributor;
                    let mut rejected = 0u64;
                    {
                        let mut into_mix = |pcm: &[i16]| {
                            if matrix.push(contributor, pcm).is_err() {
                                rejected += 1;
                            }
                        };
                        member
                            .leg
                            .release_frame_with(Some(&mut member.hub), Some(&mut into_mix));
                    }
                    refused += rejected;
                    if let Some(injected) = member.inject.take_frame(&mut member.egress) {
                        if matrix.push(member.injector, injected).is_err() {
                            refused += 1;
                        }
                    }
                    member.leg.publish_shared(now);
                }

                let output = matrix.mix();
                let conference_frame = output.frame(monitor);
                for member in seated.iter_mut() {
                    if let Some(ear) = output.frame(member.party.listener) {
                        member.egress.queue_frame(ear);
                    }
                    if let Some(mixed) = conference_frame {
                        member
                            .hub
                            .publish(TapEvent::media(Track::Mixed, timestamp_ms, mixed));
                    }
                }

                frames += 1;
                next_release += ptime;
                let after = Instant::now();
                if next_release + ptime < after {
                    next_release = after + ptime;
                    reanchors += 1;
                }
                let stats = matrix.stats();
                self.shared.ticks.store(stats.ticks, Ordering::Relaxed);
                self.shared
                    .clipped_samples
                    .store(stats.clipped_samples, Ordering::Relaxed);
                self.shared
                    .absent_frames
                    .store(stats.absent_frames, Ordering::Relaxed);
                self.shared.mixed_frames.store(frames, Ordering::Relaxed);
                self.shared.reanchors.store(reanchors, Ordering::Relaxed);
                self.shared.frames_refused.store(refused, Ordering::Relaxed);
                continue;
            }

            let sleep_until = next_release.min(now + poll);
            if sleep_until > now {
                std::thread::sleep(sleep_until - now);
            }
        }

        for member in seated.iter() {
            report_leg(&self.name, member);
        }
        let stats = matrix.stats();
        info!(
            conference = %self.name,
            mixed_frames = frames,
            elapsed_ms = started.elapsed().as_millis() as u64,
            ticks = stats.ticks,
            clipped_samples = stats.clipped_samples,
            absent_frames = stats.absent_frames,
            replaced_frames = stats.replaced_frames,
            frames_refused = refused,
            reanchors,
            "the last leg left this conference; the mix stopped"
        );
        self.shared.members_live.store(0, Ordering::Relaxed);
    }
}

fn reroute_injectors(
    matrix: &mut MixMatrix,
    seated: &[Seated],
    monitor: ListenerId,
    conference: &str,
) {
    for member in seated.iter() {
        let ear = match &member.route.target {
            MixTarget::Own => Some(member.party.listener),
            MixTarget::Everyone => None,
            MixTarget::Member(named) => seated
                .iter()
                .find(|held| held.external_id == *named)
                .map(|held| held.party.listener),
        };
        let routed = match (&member.route.target, ear) {
            (MixTarget::Everyone, _) => matrix.route_to_all(member.injector),
            (_, Some(ear)) => matrix.route_only(member.injector, ear),
            (MixTarget::Member(named), None) => {
                warn!(
                    conference = %conference,
                    session = %member.session,
                    target = %named,
                    "nobody by that name is in this conference, so the whisper is \
                     inaudible until they join"
                );
                matrix.mute_contributor(member.injector)
            }
            (MixTarget::Own, None) => matrix.mute_contributor(member.injector),
        };
        let audible = member.route.monitor_audible
            && !matches!((&member.route.target, ear), (MixTarget::Member(_), None));
        let routed = routed.and_then(|_| {
            matrix.set_gain(
                member.injector,
                monitor,
                if audible { Gain::UNITY } else { Gain::MUTED },
            )
        });
        if let Err(error) = routed {
            warn!(
                conference = %conference,
                session = %member.session,
                target = %member.route.target_name(),
                %error,
                "this leg's injected audio has no route into the mix"
            );
        }
    }
}

fn report_leg(conference: &str, member: &Seated) {
    let stats = member.leg.stats();
    let paced = member.egress.stats();
    info!(
        conference = %conference,
        session = %member.session,
        datagrams = stats.datagrams,
        frames_played = stats.pipeline.frames_played,
        frames_concealed = stats.pipeline.frames_concealed,
        underruns = stats.underruns,
        jitter_lost = stats.jitter.lost,
        jitter_late_drops = stats.jitter.late_drops,
        recv_errors = stats.recv_errors,
        packets_emitted = paced.packets_emitted,
        silence_frames = paced.silence_frames,
        late_ticks = paced.late_ticks,
        dropped_samples = paced.dropped_samples,
        "a conference leg finished"
    );
}
