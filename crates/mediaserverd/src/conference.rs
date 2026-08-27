use crate::hub::{Hub, HubClient, TapEvent};
use crate::inline_leg::InlineEgress;
use crate::tap_spike::{TapLeg, MAX_DATAGRAM};
use crossbeam_queue::ArrayQueue;
use media_core::{
    AudioFormat, ContributorId, Gain, ListenerId, MixError, MixMatrix, Party, SpeechGate, Track,
};
use session_core::mix::{
    MemberControl, MemberRouteView, MemberStateView, MixRoute, MixSource, MixTarget,
};
use session_core::{AttachmentId, SessionId};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};
use tracing::{info, warn};

pub const COMMAND_CAPACITY: usize = 64;
pub const PROMPT_CAPACITY: usize = 64;
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
    pub member_controls: u64,
    pub members_muted: u64,
    pub members_deaf: u64,
    pub members_held: u64,
    pub prompt_frames: u64,
    pub rooms_live: u64,
    pub rooms_auto_ended: u64,
    pub member_state_expired: u64,
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
    pub member_controls: AtomicU64,
    pub members_muted: AtomicU64,
    pub members_deaf: AtomicU64,
    pub members_held: AtomicU64,
    pub prompt_frames: AtomicU64,
    pub rooms_live: AtomicU64,
    pub rooms_auto_ended: AtomicU64,
    pub member_state_expired: AtomicU64,
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
        self.member_controls += read(&shared.member_controls);
        self.members_muted += read(&shared.members_muted);
        self.members_deaf += read(&shared.members_deaf);
        self.members_held += read(&shared.members_held);
        self.prompt_frames += read(&shared.prompt_frames);
        self.rooms_live += read(&shared.rooms_live);
        self.rooms_auto_ended += read(&shared.rooms_auto_ended);
        self.member_state_expired += read(&shared.member_state_expired);
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
    Route {
        session: SessionId,
        route: MixRoute,
    },
    Control {
        session: SessionId,
        control: MemberControl,
    },
}

struct MirroredMember {
    session: SessionId,
    external_id: String,
    mute: bool,
    deaf: bool,
    hold: bool,
    mute_until: Option<Instant>,
    deaf_until: Option<Instant>,
    hold_until: Option<Instant>,
    route: MixRoute,
    route_owner: Option<AttachmentId>,
}

impl MirroredMember {
    fn lease_expired(held: bool, until: Option<Instant>, now: Instant) -> bool {
        held && until.is_some_and(|deadline| deadline <= now)
    }

    fn expired_flags(&self, now: Instant) -> MemberControl {
        MemberControl::releasing(
            MirroredMember::lease_expired(self.mute, self.mute_until, now),
            MirroredMember::lease_expired(self.deaf, self.deaf_until, now),
            MirroredMember::lease_expired(self.hold, self.hold_until, now),
        )
    }

    fn remaining_ms(until: Option<Instant>, now: Instant) -> u64 {
        until
            .map(|deadline| deadline.saturating_duration_since(now).as_millis() as u64)
            .unwrap_or(0)
    }
}

pub struct RoomSeat {
    pub session: SessionId,
    pub external_id: String,
}

pub enum RoomFate {
    Mixing,
    Emptied,
    Stopped(JoinHandle<()>),
}

pub struct Conference {
    name: String,
    format: AudioFormat,
    opened_at: Instant,
    opened_at_wall: SystemTime,
    commands: Arc<ArrayQueue<ConferenceCommand>>,
    prompts: Arc<ArrayQueue<Vec<i16>>>,
    prompt_flush: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    shared: Arc<ConferenceShared>,
    thread: Option<JoinHandle<()>>,
    members: Vec<MirroredMember>,
    room: Option<RoomSeat>,
    room_hub: HubClient,
    seated_ever: bool,
    linger: Option<tokio::task::JoinHandle<()>>,
}

impl Conference {
    pub fn start(name: &str, format: AudioFormat) -> Result<Conference, ConferenceError> {
        let frame_samples = format
            .samples_per_packet()
            .ok_or(ConferenceError::Format(format))? as usize;
        let commands = Arc::new(ArrayQueue::new(COMMAND_CAPACITY));
        let prompts = Arc::new(ArrayQueue::new(PROMPT_CAPACITY));
        let prompt_flush = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let shared = Arc::new(ConferenceShared::default());
        let (room_hub, room_client) = Hub::new();
        let opened_at = Instant::now();
        let mixed = Mixed {
            name: name.to_string(),
            format,
            frame_samples,
            commands: Arc::clone(&commands),
            prompts: Arc::clone(&prompts),
            prompt_flush: Arc::clone(&prompt_flush),
            shared: Arc::clone(&shared),
            stop: Arc::clone(&stop),
            room_hub,
            opened_at,
        };
        let thread = std::thread::Builder::new()
            .name(format!("mss-conf-{name}"))
            .spawn(move || mixed.run())
            .map_err(|error| ConferenceError::Thread(error.to_string()))?;
        Ok(Conference {
            name: name.to_string(),
            format,
            opened_at,
            opened_at_wall: SystemTime::now(),
            commands,
            prompts,
            prompt_flush,
            stop,
            shared,
            thread: Some(thread),
            members: Vec::new(),
            room: None,
            room_hub: room_client,
            seated_ever: false,
            linger: None,
        })
    }

    pub fn format(&self) -> AudioFormat {
        self.format
    }

    pub fn opened_at_wall(&self) -> SystemTime {
        self.opened_at_wall
    }

    pub fn open_for(&self) -> Duration {
        self.opened_at.elapsed()
    }

    pub fn room_hub(&self) -> HubClient {
        self.room_hub.clone()
    }

    pub fn room(&self) -> Option<&RoomSeat> {
        self.room.as_ref()
    }

    pub fn seat_room(&mut self, seat: RoomSeat) -> Result<(), ConferenceError> {
        if let Some(held) = &self.room {
            return Err(ConferenceError::RoomTaken(held.external_id.clone()));
        }
        self.room = Some(seat);
        self.shared.rooms_live.store(1, Ordering::Relaxed);
        self.cancel_linger();
        Ok(())
    }

    pub fn unseat_room(&mut self) -> Option<JoinHandle<()>> {
        self.room = None;
        self.shared.rooms_live.store(0, Ordering::Relaxed);
        self.cancel_linger();
        if self.members.is_empty() {
            return self.stop_now();
        }
        None
    }

    pub fn emptied(&self) -> bool {
        self.members.is_empty() && self.seated_ever
    }

    pub fn stop_now(&mut self) -> Option<JoinHandle<()>> {
        self.cancel_linger();
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take()
    }

    pub fn count_auto_end(&self) {
        self.shared.rooms_auto_ended.fetch_add(1, Ordering::Relaxed);
    }

    pub fn linger_until(&mut self, task: tokio::task::JoinHandle<()>) {
        self.cancel_linger();
        self.linger = Some(task);
    }

    fn cancel_linger(&mut self) {
        if let Some(task) = self.linger.take() {
            task.abort();
        }
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
        let external_id = member.external_id.clone();
        self.cancel_linger();
        self.commands
            .push(ConferenceCommand::Join(Box::new(member)))
            .map_err(|_| ConferenceError::Busy)?;
        self.members.push(MirroredMember {
            session,
            external_id,
            mute: false,
            deaf: false,
            hold: false,
            mute_until: None,
            deaf_until: None,
            hold_until: None,
            route: MixRoute::private(),
            route_owner: None,
        });
        self.seated_ever = true;
        Ok(())
    }

    pub fn route(
        &mut self,
        session: SessionId,
        owner: Option<AttachmentId>,
        route: MixRoute,
    ) -> Result<(), ConferenceError> {
        let mirrored = self
            .mirrored(session)
            .ok_or(ConferenceError::NotSeated(session))?;
        mirrored.route = route.clone();
        mirrored.route_owner = owner;
        self.commands
            .push(ConferenceCommand::Route { session, route })
            .map_err(|_| ConferenceError::Busy)
    }

    pub fn control(
        &mut self,
        session: SessionId,
        control: MemberControl,
    ) -> Result<(), ConferenceError> {
        self.control_at(session, control, Instant::now())
    }

    pub fn control_at(
        &mut self,
        session: SessionId,
        control: MemberControl,
        now: Instant,
    ) -> Result<(), ConferenceError> {
        let lease = control
            .ttl_ms
            .filter(|ms| *ms > 0)
            .map(|ms| now + Duration::from_millis(ms));
        let seat = self
            .members
            .iter()
            .position(|held| held.session == session)
            .ok_or(ConferenceError::NotSeated(session))?;
        self.commands
            .push(ConferenceCommand::Control { session, control })
            .map_err(|_| ConferenceError::Busy)?;
        let mirrored = &mut self.members[seat];
        if let Some(mute) = control.mute {
            mirrored.mute = mute;
            mirrored.mute_until = mute.then_some(lease).flatten();
        }
        if let Some(deaf) = control.deaf {
            mirrored.deaf = deaf;
            mirrored.deaf_until = deaf.then_some(lease).flatten();
        }
        if let Some(hold) = control.hold {
            mirrored.hold = hold;
            mirrored.hold_until = hold.then_some(lease).flatten();
        }
        Ok(())
    }

    pub fn expire_member_state(&mut self, now: Instant) -> Vec<(SessionId, MemberControl)> {
        let due: Vec<(SessionId, MemberControl)> = self
            .members
            .iter()
            .map(|held| (held.session, held.expired_flags(now)))
            .filter(|(_, expired)| !expired.is_empty())
            .collect();
        let mut released = Vec::new();
        for (session, expired) in due {
            match self.control_at(session, expired, now) {
                Ok(()) => {
                    let flags = [expired.mute, expired.deaf, expired.hold]
                        .into_iter()
                        .filter(Option::is_some)
                        .count() as u64;
                    self.shared
                        .member_state_expired
                        .fetch_add(flags, Ordering::Relaxed);
                    info!(
                        %session,
                        conference = %self.name,
                        mute = ?expired.mute,
                        deaf = ?expired.deaf,
                        hold = ?expired.hold,
                        "a member state lease ran out, so this pod lifted the flag itself"
                    );
                    released.push((session, expired));
                }
                Err(error) => warn!(
                    %session,
                    conference = %self.name,
                    %error,
                    "a member state lease ran out and the conference would not take the release; \
                     the next sweep retries it"
                ),
            }
        }
        released
    }

    pub fn member_state(&self, session: SessionId) -> Option<MemberStateView> {
        let member = self.members.iter().find(|held| held.session == session)?;
        let routes = if member.route == MixRoute::private() {
            Vec::new()
        } else {
            vec![MemberRouteView {
                route: member.route.clone(),
                attachment: member.route_owner,
            }]
        };
        let now = Instant::now();
        Some(MemberStateView {
            seated: true,
            mute: member.mute,
            deaf: member.deaf,
            hold: member.hold,
            mute_expires_in_ms: MirroredMember::remaining_ms(member.mute_until, now),
            deaf_expires_in_ms: MirroredMember::remaining_ms(member.deaf_until, now),
            hold_expires_in_ms: MirroredMember::remaining_ms(member.hold_until, now),
            source: member.route.source,
            routes,
            ..self.room_state()
        })
    }

    pub fn room_state(&self) -> MemberStateView {
        MemberStateView {
            conference: self.name.clone(),
            members: self
                .members
                .iter()
                .map(|held| held.external_id.clone())
                .collect(),
            room_session: self
                .room
                .as_ref()
                .map(|seat| seat.external_id.clone())
                .unwrap_or_default(),
            opened_at: self.opened_at_wall,
            seated: false,
            mute: false,
            deaf: false,
            hold: false,
            mute_expires_in_ms: 0,
            deaf_expires_in_ms: 0,
            hold_expires_in_ms: 0,
            source: MixSource::default(),
            routes: Vec::new(),
        }
    }

    fn mirrored(&mut self, session: SessionId) -> Option<&mut MirroredMember> {
        self.members.iter_mut().find(|held| held.session == session)
    }

    pub fn free_prompt_chunks(&self) -> usize {
        self.prompts.capacity() - self.prompts.len()
    }

    pub fn prompt(&self, chunk: Vec<i16>) -> Result<(), ConferenceError> {
        self.prompts
            .push(chunk)
            .map_err(|_| ConferenceError::PromptQueue)
    }

    pub fn flush_prompts(&self) {
        self.prompt_flush.store(true, Ordering::Relaxed);
    }

    pub fn unseat(&mut self, session: SessionId) -> RoomFate {
        self.members.retain(|held| held.session != session);
        if self
            .commands
            .push(ConferenceCommand::Leave(session))
            .is_err()
        {
            warn!(%session, "the conference command queue is full; stopping the mix instead");
            self.members.clear();
        }
        if !self.members.is_empty() {
            return RoomFate::Mixing;
        }
        if self.room.is_some() {
            return RoomFate::Emptied;
        }
        match self.stop_now() {
            Some(thread) => RoomFate::Stopped(thread),
            None => RoomFate::Emptied,
        }
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
    #[error(
        "the conference prompt queue is full; the room is still playing what was \
         queued before"
    )]
    PromptQueue,
    #[error("{0} is not seated in this conference")]
    NotSeated(SessionId),
    #[error("this conference is already owned by room session {0}")]
    RoomTaken(String),
    #[error("conference thread: {0}")]
    Thread(String),
}

struct Mixed {
    name: String,
    format: AudioFormat,
    frame_samples: usize,
    commands: Arc<ArrayQueue<ConferenceCommand>>,
    prompts: Arc<ArrayQueue<Vec<i16>>>,
    prompt_flush: Arc<AtomicBool>,
    shared: Arc<ConferenceShared>,
    stop: Arc<AtomicBool>,
    room_hub: Hub,
    opened_at: Instant,
}

struct Seated {
    session: SessionId,
    external_id: String,
    seated_at_frame: u64,
    leg: TapLeg,
    hub: Hub,
    egress: InlineEgress,
    party: Party,
    injector: ContributorId,
    inject: ChunkFeed,
    route: MixRoute,
    mute: bool,
    deaf: bool,
    hold: bool,
}

impl Seated {
    fn silenced(&self) -> bool {
        self.mute || self.hold
    }

    fn deafened(&self) -> bool {
        self.deaf || self.hold
    }
}

struct ChunkFeed {
    frame: Vec<i16>,
    pending: Option<Vec<i16>>,
    at: usize,
}

impl ChunkFeed {
    fn with_frame(frame_samples: usize) -> ChunkFeed {
        ChunkFeed {
            frame: vec![0; frame_samples],
            pending: None,
            at: 0,
        }
    }

    fn discard(&mut self) -> u64 {
        let unplayed = self
            .pending
            .take()
            .map(|pending| pending.len().saturating_sub(self.at) as u64)
            .unwrap_or(0);
        self.at = 0;
        unplayed
    }

    fn fill(&mut self, pop: &mut dyn FnMut() -> Option<Vec<i16>>) -> u64 {
        let mut filled = 0;
        while filled < self.frame.len() {
            if self.pending.is_none() {
                self.pending = pop();
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
        for sample in self.frame[filled..].iter_mut() {
            *sample = 0;
        }
        filled as u64
    }

    fn frame(&self) -> &[i16] {
        &self.frame
    }
}

impl Mixed {
    fn run(mut self) {
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
        let prompt = matrix.join_contributor();
        let mut prompt_feed = ChunkFeed::with_frame(self.frame_samples);
        let mut prompt_frames = 0u64;
        let ptime = Duration::from_millis(self.format.ptime_ms.max(1) as u64);
        let poll = ptime / 4;
        let mut buf = [0u8; MAX_DATAGRAM];
        let started = self.opened_at;
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
                            seated_at_frame: frames,
                            leg: member.leg,
                            hub: member.hub,
                            egress: member.egress,
                            party,
                            injector,
                            inject: ChunkFeed::with_frame(self.frame_samples),
                            route: MixRoute::private(),
                            mute: false,
                            deaf: false,
                            hold: false,
                        });
                        apply_matrix(&mut matrix, &seated, prompt, monitor, &self.name);
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
                            apply_matrix(&mut matrix, &seated, prompt, monitor, &self.name);
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
                            apply_matrix(&mut matrix, &seated, prompt, monitor, &self.name);
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
                    ConferenceCommand::Control { session, control } => {
                        if let Some(member) = seated.iter_mut().find(|held| held.session == session)
                        {
                            if let Some(mute) = control.mute {
                                member.mute = mute;
                            }
                            if let Some(deaf) = control.deaf {
                                member.deaf = deaf;
                            }
                            if let Some(hold) = control.hold {
                                member.hold = hold;
                            }
                            let (mute, deaf, hold) = (member.mute, member.deaf, member.hold);
                            apply_matrix(&mut matrix, &seated, prompt, monitor, &self.name);
                            self.shared.member_controls.fetch_add(1, Ordering::Relaxed);
                            info!(
                                conference = %self.name,
                                %session,
                                mute,
                                deaf,
                                hold,
                                "this member is heard, hears, or is on hold on new terms"
                            );
                        } else {
                            warn!(
                                conference = %self.name,
                                %session,
                                "a member control arrived for a leg that is no longer seated"
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
            let count = |wanted: fn(&&Seated) -> bool| seated.iter().filter(wanted).count() as u64;
            self.shared
                .members_muted
                .store(count(|held| held.silenced()), Ordering::Relaxed);
            self.shared
                .members_deaf
                .store(count(|held| held.deafened()), Ordering::Relaxed);
            self.shared
                .members_held
                .store(count(|held| held.hold), Ordering::Relaxed);

            self.room_hub.poll_commands();
            for member in seated.iter_mut() {
                member.hub.poll_commands();
                member.leg.poll_ssrc_tracks();
                member.leg.drain(&mut buf, Some(&mut member.hub));
                if member.egress.take_flush() {
                    let unplayed = member.inject.discard();
                    member.egress.discard_pending(unplayed);
                }
            }
            if self.prompt_flush.swap(false, Ordering::Relaxed) {
                prompt_feed.discard();
                while self.prompts.pop().is_some() {}
            }

            let now = Instant::now();
            for member in seated.iter_mut() {
                member.egress.send_tick(now);
            }

            if now >= next_release {
                let ptime_ms = self.format.ptime_ms.max(1) as u64;
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
                    let filled = {
                        let egress = &mut member.egress;
                        let mut pop = || egress.pop_chunk();
                        member.inject.fill(&mut pop)
                    };
                    if filled > 0 {
                        member.egress.account_mixed_in(filled);
                        if matrix.push(member.injector, member.inject.frame()).is_err() {
                            refused += 1;
                        }
                    }
                    member.leg.publish_shared(now);
                }
                let prompted = {
                    let prompts = &self.prompts;
                    let mut pop = || prompts.pop();
                    prompt_feed.fill(&mut pop)
                };
                if prompted > 0 {
                    prompt_frames += 1;
                    if matrix.push(prompt, prompt_feed.frame()).is_err() {
                        refused += 1;
                    }
                }

                let output = matrix.mix();
                let conference_frame = output.frame(monitor);
                if let Some(mixed) = conference_frame {
                    self.room_hub
                        .publish(TapEvent::media(Track::Mixed, frames * ptime_ms, mixed));
                }
                for member in seated.iter_mut() {
                    if let Some(ear) = output.frame(member.party.listener) {
                        member.egress.queue_frame(ear);
                    }
                    if let Some(mixed) = conference_frame {
                        let timestamp_ms = frames.saturating_sub(member.seated_at_frame) * ptime_ms;
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
                self.shared
                    .prompt_frames
                    .store(prompt_frames, Ordering::Relaxed);
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
            room_frames_published = self.room_hub.published(),
            mixed_frames = frames,
            elapsed_ms = started.elapsed().as_millis() as u64,
            ticks = stats.ticks,
            clipped_samples = stats.clipped_samples,
            absent_frames = stats.absent_frames,
            replaced_frames = stats.replaced_frames,
            frames_refused = refused,
            prompt_frames,
            reanchors,
            "the last leg left this conference; the mix stopped"
        );
        self.shared.members_live.store(0, Ordering::Relaxed);
    }
}

fn apply_matrix(
    matrix: &mut MixMatrix,
    seated: &[Seated],
    prompt: ContributorId,
    monitor: ListenerId,
    conference: &str,
) {
    let mut addressed: Vec<(ContributorId, ListenerId)> = Vec::with_capacity(seated.len() * 2 + 1);
    if let Err(error) = matrix.route_to_all(prompt) {
        warn!(
            conference = %conference,
            %error,
            "the room prompt source has no route into the mix"
        );
    }
    for member in seated.iter() {
        let ear = match &member.route.target {
            MixTarget::Own => Some(member.party.listener),
            MixTarget::Everyone => None,
            MixTarget::Member(named) => seated
                .iter()
                .find(|held| held.external_id == *named)
                .map(|held| held.party.listener),
        };
        let missing = matches!(&member.route.target, MixTarget::Member(_)) && ear.is_none();
        if missing {
            warn!(
                conference = %conference,
                session = %member.session,
                target = %member.route.target_name(),
                "nobody by that name is in this conference, so the whisper is \
                 inaudible until they join"
            );
        }
        let from_own_leg = member.route.is_own_leg();
        let heard = if member.silenced() {
            matrix.mute_contributor(member.party.contributor)
        } else if from_own_leg {
            route_source(
                matrix,
                member.party.contributor,
                &member.route.target,
                ear,
                &mut addressed,
            )
        } else {
            matrix.route_to_all(member.party.contributor)
        };
        let injected = if from_own_leg || member.route.is_private() {
            addressed.push((member.injector, member.party.listener));
            matrix.route_only(member.injector, member.party.listener)
        } else {
            route_source(
                matrix,
                member.injector,
                &member.route.target,
                ear,
                &mut addressed,
            )
        };
        let routed_source = if from_own_leg {
            member.party.contributor
        } else {
            member.injector
        };
        let monitored = if member.silenced() && from_own_leg {
            Ok(())
        } else {
            matrix.set_gain(
                routed_source,
                monitor,
                if member.route.monitor_audible && !missing {
                    Gain::UNITY
                } else {
                    Gain::MUTED
                },
            )
        };
        if let Err(error) = heard.and(injected).and(monitored) {
            warn!(
                conference = %conference,
                session = %member.session,
                target = %member.route.target_name(),
                source = %member.route.source_name(),
                %error,
                "this leg's audio has no route into the mix"
            );
        }
    }
    for member in seated.iter().filter(|held| held.deafened()) {
        let ear = member.party.listener;
        for source in seated
            .iter()
            .flat_map(|held| [held.party.contributor, held.injector])
            .chain(std::iter::once(prompt))
        {
            if addressed.contains(&(source, ear)) {
                continue;
            }
            if let Err(error) = matrix.set_gain(source, ear, Gain::MUTED) {
                warn!(
                    conference = %conference,
                    session = %member.session,
                    %error,
                    "a deafened member's ear could not be closed to a contributor"
                );
            }
        }
    }
}

fn route_source(
    matrix: &mut MixMatrix,
    contributor: ContributorId,
    target: &MixTarget,
    ear: Option<ListenerId>,
    addressed: &mut Vec<(ContributorId, ListenerId)>,
) -> Result<(), MixError> {
    match (target, ear) {
        (MixTarget::Everyone, _) => matrix.route_to_all(contributor),
        (_, Some(ear)) => {
            addressed.push((contributor, ear));
            matrix.route_only(contributor, ear)
        }
        (_, None) => matrix.mute_contributor(contributor),
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
