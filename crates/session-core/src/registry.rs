use crate::capability::{Capabilities, Transport};
use crate::event::{ConsumerEvent, EventKind, MediaEvent, Observation};
use crate::ids::{AttachmentId, PlaybackId, SessionId};
use crate::mix::MixRoute;
use media_core::{AudioFormat, Track};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, VecDeque};
use std::hash::{Hash, Hasher};

pub const DEFAULT_MAX_ATTACHMENTS: usize = 16;
pub const OUTBOX_CAPACITY: usize = 1024;
pub const IDEMPOTENCY_CAPACITY: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionKind {
    Tap,
    Inline,
    Mix,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrackSelector {
    All,
    Only(Track),
}

impl TrackSelector {
    pub fn wants(self, track: Track) -> bool {
        match self {
            TrackSelector::All => true,
            TrackSelector::Only(only) => only == track,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ControlError {
    #[error("no session {0}")]
    UnknownSession(SessionId),
    #[error("no session for external id {0}")]
    UnknownExternalId(String),
    #[error("no attachment {0}")]
    UnknownAttachment(AttachmentId),
    #[error("no playback {0}")]
    UnknownPlayback(PlaybackId),
    #[error("external id {external_id} is already held by {holder}")]
    ExternalIdInUse {
        external_id: String,
        holder: SessionId,
    },
    #[error("session {session} already has an authoritative attachment {holder}")]
    AuthoritativeAlreadyBound {
        session: SessionId,
        holder: AttachmentId,
    },
    #[error("attachment {attachment} was granted {granted} and so may not {missing}")]
    CapabilityDenied {
        attachment: AttachmentId,
        granted: Capabilities,
        missing: Capabilities,
    },
    #[error("transport {transport} cannot carry {missing}")]
    TransportCannotCarry {
        transport: Transport,
        missing: Capabilities,
    },
    #[error("an attachment must at least declare SINK")]
    NoCapabilityDeclared,
    #[error("idempotency key {0} was reused for a different request")]
    IdempotencyConflict(String),
    #[error("session {session} already holds {limit} attachments")]
    TooManyAttachments { session: SessionId, limit: usize },
    #[error("{0}")]
    MixRoute(#[from] crate::mix::MixRouteError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSession {
    pub external_id: String,
    pub kind: SessionKind,
    pub call_id: String,
    pub from_tags: Vec<String>,
    pub rtpengine_node: String,
    pub sdp_offer: Option<String>,
    pub group: String,
    pub idempotency_key: Option<String>,
}

impl CreateSession {
    fn fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.external_id.hash(&mut hasher);
        self.kind.hash(&mut hasher);
        self.call_id.hash(&mut hasher);
        self.from_tags.hash(&mut hasher);
        self.rtpengine_node.hash(&mut hasher);
        self.sdp_offer.hash(&mut hasher);
        self.group.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AttachSpec {
    pub session: SessionId,
    pub transport: Transport,
    pub capabilities: Capabilities,
    pub selector: TrackSelector,
    pub format: AudioFormat,
    pub authoritative: bool,
    pub label: String,
    pub endpoint: String,
    pub group: String,
    pub metadata: BTreeMap<String, String>,
    pub idempotency_key: Option<String>,
}

impl AttachSpec {
    fn fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.session.hash(&mut hasher);
        self.transport.hash(&mut hasher);
        self.capabilities.hash(&mut hasher);
        self.selector.hash(&mut hasher);
        self.format.encoding.hash(&mut hasher);
        self.format.sample_rate_hz.hash(&mut hasher);
        self.format.channels.hash(&mut hasher);
        self.format.ptime_ms.hash(&mut hasher);
        self.authoritative.hash(&mut hasher);
        self.label.hash(&mut hasher);
        self.endpoint.hash(&mut hasher);
        self.group.hash(&mut hasher);
        self.metadata.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AttachmentUpdate {
    pub paused: Option<bool>,
    pub selector: Option<TrackSelector>,
    pub format: Option<AudioFormat>,
    pub metadata: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlaybackSpec {
    pub session: SessionId,
    pub requested_by: Option<AttachmentId>,
    pub target_tag: Option<String>,
    pub block_egress: bool,
    pub idempotency_key: Option<String>,
}

impl PlaybackSpec {
    fn fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.session.hash(&mut hasher);
        self.requested_by.hash(&mut hasher);
        self.target_tag.hash(&mut hasher);
        self.block_egress.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionView {
    pub id: SessionId,
    pub external_id: String,
    pub kind: SessionKind,
    pub call_id: String,
    pub from_tags: Vec<String>,
    pub rtpengine_node: String,
    pub sdp_offer: Option<String>,
    pub sdp_answer: Option<String>,
    pub group: String,
    pub attachments: Vec<AttachmentId>,
    pub authoritative: Option<AttachmentId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AttachmentView {
    pub id: AttachmentId,
    pub session: SessionId,
    pub transport: Transport,
    pub capabilities: Capabilities,
    pub selector: TrackSelector,
    pub format: AudioFormat,
    pub authoritative: bool,
    pub paused: bool,
    pub label: String,
    pub endpoint: String,
    pub group: String,
    pub metadata: BTreeMap<String, String>,
}

struct SessionRecord {
    id: SessionId,
    external_id: String,
    kind: SessionKind,
    call_id: String,
    from_tags: Vec<String>,
    rtpengine_node: String,
    sdp_offer: Option<String>,
    sdp_answer: Option<String>,
    group: String,
    attachments: Vec<AttachmentId>,
    authoritative: Option<AttachmentId>,
    next_seq: u64,
}

struct AttachmentRecord {
    id: AttachmentId,
    session: SessionId,
    transport: Transport,
    capabilities: Capabilities,
    selector: TrackSelector,
    format: AudioFormat,
    authoritative: bool,
    paused: bool,
    label: String,
    endpoint: String,
    group: String,
    metadata: BTreeMap<String, String>,
    seen_final: bool,
}

struct PlaybackRecord {
    session: SessionId,
    target_tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoppedPlayback {
    pub session: SessionId,
    pub target_tag: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Session(SessionId),
    Attachment(AttachmentId),
    Playback(PlaybackId),
}

struct IdempotentRecord {
    fingerprint: u64,
    outcome: Outcome,
}

pub struct SessionRegistry {
    sessions: BTreeMap<SessionId, SessionRecord>,
    attachments: BTreeMap<AttachmentId, AttachmentRecord>,
    playbacks: BTreeMap<PlaybackId, PlaybackRecord>,
    external_index: BTreeMap<String, SessionId>,
    idempotency: BTreeMap<String, IdempotentRecord>,
    idempotency_order: VecDeque<String>,
    outbox: VecDeque<MediaEvent>,
    events_dropped: u64,
    max_attachments: usize,
    next_id: u64,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ATTACHMENTS)
    }
}

impl SessionRegistry {
    pub fn new(max_attachments: usize) -> Self {
        SessionRegistry {
            sessions: BTreeMap::new(),
            attachments: BTreeMap::new(),
            playbacks: BTreeMap::new(),
            external_index: BTreeMap::new(),
            idempotency: BTreeMap::new(),
            idempotency_order: VecDeque::new(),
            outbox: VecDeque::new(),
            events_dropped: 0,
            max_attachments: max_attachments.max(1),
            next_id: 1,
        }
    }

    pub fn create_session(&mut self, request: CreateSession) -> Result<SessionView, ControlError> {
        if let Some(Outcome::Session(existing)) =
            self.replay(&request.idempotency_key, request.fingerprint())?
        {
            return self.session_view(existing);
        }
        if let Some(holder) = self.external_index.get(&request.external_id) {
            return Err(ControlError::ExternalIdInUse {
                external_id: request.external_id,
                holder: *holder,
            });
        }

        let id = SessionId::from_raw(self.take_id());
        self.external_index.insert(request.external_id.clone(), id);
        self.sessions.insert(
            id,
            SessionRecord {
                id,
                external_id: request.external_id.clone(),
                kind: request.kind,
                call_id: request.call_id.clone(),
                from_tags: request.from_tags.clone(),
                rtpengine_node: request.rtpengine_node.clone(),
                sdp_offer: request.sdp_offer.clone(),
                sdp_answer: None,
                group: request.group.clone(),
                attachments: Vec::new(),
                authoritative: None,
                next_seq: 0,
            },
        );
        self.remember(
            &request.idempotency_key,
            request.fingerprint(),
            Outcome::Session(id),
        );
        self.session_view(id)
    }

    pub fn destroy_session(
        &mut self,
        session: SessionId,
        reason: &str,
    ) -> Result<(), ControlError> {
        let record = self
            .sessions
            .get(&session)
            .ok_or(ControlError::UnknownSession(session))?;
        let attachments = record.attachments.clone();
        let external_id = record.external_id.clone();

        for attachment in attachments {
            if let Some(gone) = self.attachments.remove(&attachment) {
                self.push_event(
                    session,
                    Some(attachment),
                    gone.authoritative,
                    EventKind::AttachmentDown {
                        label: gone.label,
                        reason: reason.to_string(),
                    },
                );
            }
        }
        self.playbacks.retain(|_, record| record.session != session);
        self.push_event(
            session,
            None,
            true,
            EventKind::SessionEnded {
                reason: reason.to_string(),
            },
        );
        self.sessions.remove(&session);
        self.external_index.remove(&external_id);
        Ok(())
    }

    pub fn resolve(&self, external_id: &str) -> Result<SessionId, ControlError> {
        self.external_index
            .get(external_id)
            .copied()
            .ok_or_else(|| ControlError::UnknownExternalId(external_id.to_string()))
    }

    pub fn session_view(&self, session: SessionId) -> Result<SessionView, ControlError> {
        let record = self
            .sessions
            .get(&session)
            .ok_or(ControlError::UnknownSession(session))?;
        Ok(SessionView {
            id: record.id,
            external_id: record.external_id.clone(),
            kind: record.kind,
            call_id: record.call_id.clone(),
            from_tags: record.from_tags.clone(),
            rtpengine_node: record.rtpengine_node.clone(),
            sdp_offer: record.sdp_offer.clone(),
            sdp_answer: record.sdp_answer.clone(),
            group: record.group.clone(),
            attachments: record.attachments.clone(),
            authoritative: record.authoritative,
        })
    }

    pub fn record_sdp_answer(
        &mut self,
        session: SessionId,
        answer: String,
    ) -> Result<SessionView, ControlError> {
        let record = self
            .sessions
            .get_mut(&session)
            .ok_or(ControlError::UnknownSession(session))?;
        record.sdp_answer = Some(answer);
        self.session_view(session)
    }

    pub fn attach(&mut self, spec: AttachSpec) -> Result<AttachmentView, ControlError> {
        if let Some(Outcome::Attachment(existing)) =
            self.replay(&spec.idempotency_key, spec.fingerprint())?
        {
            return self.attachment_view(existing);
        }

        let record = self
            .sessions
            .get(&spec.session)
            .ok_or(ControlError::UnknownSession(spec.session))?;

        if spec.capabilities.is_empty() {
            return Err(ControlError::NoCapabilityDeclared);
        }
        let route = MixRoute::from_metadata(&spec.metadata)?;
        if let Some(route) = &route {
            route.authorize(spec.capabilities)?;
        }
        let unsupported = spec.capabilities.missing_from(spec.transport.carries());
        if !unsupported.is_empty() {
            return Err(ControlError::TransportCannotCarry {
                transport: spec.transport,
                missing: unsupported,
            });
        }
        if let Some(holder) = record.authoritative {
            if spec.authoritative {
                return Err(ControlError::AuthoritativeAlreadyBound {
                    session: spec.session,
                    holder,
                });
            }
        }
        if record.attachments.len() >= self.max_attachments {
            return Err(ControlError::TooManyAttachments {
                session: spec.session,
                limit: self.max_attachments,
            });
        }

        let id = AttachmentId::from_raw(self.take_id());
        self.attachments.insert(
            id,
            AttachmentRecord {
                id,
                session: spec.session,
                transport: spec.transport,
                capabilities: spec.capabilities,
                selector: spec.selector,
                format: spec.format,
                authoritative: spec.authoritative,
                paused: false,
                label: spec.label.clone(),
                endpoint: spec.endpoint.clone(),
                group: spec.group.clone(),
                metadata: spec.metadata.clone(),
                seen_final: false,
            },
        );
        if let Some(record) = self.sessions.get_mut(&spec.session) {
            record.attachments.push(id);
            if spec.authoritative {
                record.authoritative = Some(id);
            }
        }
        self.push_event(
            spec.session,
            Some(id),
            spec.authoritative,
            EventKind::AttachmentUp {
                label: spec.label.clone(),
            },
        );
        if let Some(route) = &route {
            self.push_event(
                spec.session,
                Some(id),
                spec.authoritative,
                EventKind::MixRouted {
                    target: route.target_name().to_string(),
                    monitor_audible: route.monitor_audible,
                },
            );
        }
        self.remember(
            &spec.idempotency_key,
            spec.fingerprint(),
            Outcome::Attachment(id),
        );
        self.attachment_view(id)
    }

    pub fn detach(&mut self, attachment: AttachmentId, reason: &str) -> Result<(), ControlError> {
        let record = self
            .attachments
            .remove(&attachment)
            .ok_or(ControlError::UnknownAttachment(attachment))?;
        if let Some(session) = self.sessions.get_mut(&record.session) {
            session.attachments.retain(|held| *held != attachment);
            if session.authoritative == Some(attachment) {
                session.authoritative = None;
            }
        }
        self.push_event(
            record.session,
            Some(attachment),
            record.authoritative,
            EventKind::AttachmentDown {
                label: record.label,
                reason: reason.to_string(),
            },
        );
        Ok(())
    }

    pub fn update_attachment(
        &mut self,
        attachment: AttachmentId,
        update: AttachmentUpdate,
    ) -> Result<AttachmentView, ControlError> {
        let record = self
            .attachments
            .get_mut(&attachment)
            .ok_or(ControlError::UnknownAttachment(attachment))?;
        let before = MixRoute::from_metadata(&record.metadata)?;
        let merged = update.metadata.map(|carried| {
            let mut merged = record.metadata.clone();
            merged.extend(carried);
            merged
        });
        let after = match &merged {
            Some(merged) => MixRoute::from_metadata(merged)?,
            None => before.clone(),
        };
        if let Some(route) = &after {
            route.authorize(record.capabilities)?;
        }
        if let Some(merged) = merged {
            record.metadata = merged;
        }
        if let Some(paused) = update.paused {
            record.paused = paused;
        }
        if let Some(selector) = update.selector {
            record.selector = selector;
        }
        if let Some(format) = update.format {
            record.format = format;
        }
        let session = record.session;
        let authoritative = record.authoritative;
        if after != before {
            if let Some(route) = after {
                self.push_event(
                    session,
                    Some(attachment),
                    authoritative,
                    EventKind::MixRouted {
                        target: route.target_name().to_string(),
                        monitor_audible: route.monitor_audible,
                    },
                );
            }
        }
        self.attachment_view(attachment)
    }

    pub fn attachment_view(
        &self,
        attachment: AttachmentId,
    ) -> Result<AttachmentView, ControlError> {
        let record = self
            .attachments
            .get(&attachment)
            .ok_or(ControlError::UnknownAttachment(attachment))?;
        Ok(AttachmentView {
            id: record.id,
            session: record.session,
            transport: record.transport,
            capabilities: record.capabilities,
            selector: record.selector,
            format: record.format,
            authoritative: record.authoritative,
            paused: record.paused,
            label: record.label.clone(),
            endpoint: record.endpoint.clone(),
            group: record.group.clone(),
            metadata: record.metadata.clone(),
        })
    }

    pub fn authorize_send_text(&self, attachment: AttachmentId) -> Result<(), ControlError> {
        let record = self
            .attachments
            .get(&attachment)
            .ok_or(ControlError::UnknownAttachment(attachment))?;
        if !record.transport.has_back_channel() {
            return Err(ControlError::TransportCannotCarry {
                transport: record.transport,
                missing: Capabilities::EVENTS,
            });
        }
        Ok(())
    }

    pub fn authorize_inject(&self, attachment: AttachmentId) -> Result<(), ControlError> {
        self.require(attachment, Capabilities::INJECT).map(|_| ())
    }

    pub fn start_playback(&mut self, spec: PlaybackSpec) -> Result<PlaybackId, ControlError> {
        if let Some(Outcome::Playback(existing)) =
            self.replay(&spec.idempotency_key, spec.fingerprint())?
        {
            return Ok(existing);
        }
        if !self.sessions.contains_key(&spec.session) {
            return Err(ControlError::UnknownSession(spec.session));
        }
        let authoritative = match spec.requested_by {
            Some(attachment) => {
                let record = self.require(attachment, Capabilities::INJECT)?;
                if record.session != spec.session {
                    return Err(ControlError::UnknownAttachment(attachment));
                }
                record.authoritative
            }
            None => true,
        };

        let id = PlaybackId::from_raw(self.take_id());
        self.playbacks.insert(
            id,
            PlaybackRecord {
                session: spec.session,
                target_tag: spec.target_tag.clone(),
            },
        );
        self.push_event(
            spec.session,
            spec.requested_by,
            authoritative,
            EventKind::PlaybackStarted { playback: id },
        );
        self.remember(
            &spec.idempotency_key,
            spec.fingerprint(),
            Outcome::Playback(id),
        );
        Ok(id)
    }

    pub fn stop_playback(
        &mut self,
        playback: PlaybackId,
        reason: &str,
    ) -> Result<StoppedPlayback, ControlError> {
        let record = self
            .playbacks
            .remove(&playback)
            .ok_or(ControlError::UnknownPlayback(playback))?;
        self.push_event(
            record.session,
            None,
            true,
            EventKind::PlaybackStopped {
                playback,
                reason: reason.to_string(),
            },
        );
        Ok(StoppedPlayback {
            session: record.session,
            target_tag: record.target_tag,
        })
    }

    pub fn report(
        &mut self,
        attachment: AttachmentId,
        event: ConsumerEvent,
    ) -> Result<(), ControlError> {
        let record = self.require(attachment, Capabilities::EVENTS)?;
        let session = record.session;
        let authoritative = record.authoritative;
        let kind = match event {
            ConsumerEvent::SpeechStarted { track } => EventKind::SpeechStarted { track },
            ConsumerEvent::Partial {
                track,
                text,
                confidence,
            } => EventKind::Partial {
                track,
                text,
                confidence,
            },
            ConsumerEvent::Final {
                track,
                text,
                confidence,
            } => {
                let first_final = match self.attachments.get_mut(&attachment) {
                    Some(record) => {
                        let first = !record.seen_final;
                        record.seen_final = true;
                        first
                    }
                    None => false,
                };
                EventKind::Final {
                    track,
                    text,
                    confidence,
                    first_final,
                }
            }
            ConsumerEvent::EndOfUtterance { track } => EventKind::EndOfUtterance { track },
            ConsumerEvent::EndOfInteraction { reason } => EventKind::EndOfInteraction { reason },
        };
        self.push_event(session, Some(attachment), authoritative, kind);
        Ok(())
    }

    pub fn observe(
        &mut self,
        session: SessionId,
        observation: Observation,
    ) -> Result<(), ControlError> {
        if !self.sessions.contains_key(&session) {
            return Err(ControlError::UnknownSession(session));
        }
        let kind = match observation {
            Observation::Dtmf { track, digit } => EventKind::Dtmf { track, digit },
            Observation::RecordingStarted {
                recording_id,
                path,
                shape,
            } => EventKind::RecordingStarted {
                recording_id,
                path,
                shape,
            },
            Observation::RecordingPaused {
                recording_id,
                paused,
                duration_ms,
            } => EventKind::RecordingPaused {
                recording_id,
                paused,
                duration_ms,
            },
            Observation::RecordingStopped {
                recording_id,
                duration_ms,
            } => EventKind::RecordingStopped {
                recording_id,
                duration_ms,
            },
            Observation::UploadCompleted { recording_id, uri } => {
                EventKind::UploadCompleted { recording_id, uri }
            }
        };
        self.push_event(session, None, true, kind);
        Ok(())
    }

    pub fn drain_events(&mut self) -> Vec<MediaEvent> {
        self.outbox.drain(..).collect()
    }

    pub fn events_dropped(&self) -> u64 {
        self.events_dropped
    }

    pub fn session_ids(&self) -> Vec<SessionId> {
        self.sessions.keys().copied().collect()
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn attachment_count(&self) -> usize {
        self.attachments.len()
    }

    fn require(
        &self,
        attachment: AttachmentId,
        wanted: Capabilities,
    ) -> Result<&AttachmentRecord, ControlError> {
        let record = self
            .attachments
            .get(&attachment)
            .ok_or(ControlError::UnknownAttachment(attachment))?;
        if !record.capabilities.contains(wanted) {
            return Err(ControlError::CapabilityDenied {
                attachment,
                granted: record.capabilities,
                missing: wanted.missing_from(record.capabilities),
            });
        }
        Ok(record)
    }

    fn push_event(
        &mut self,
        session: SessionId,
        attachment: Option<AttachmentId>,
        legacy_eligible: bool,
        kind: EventKind,
    ) {
        let (external_id, session_kind, seq) = match self.sessions.get_mut(&session) {
            Some(record) => {
                let seq = record.next_seq;
                record.next_seq += 1;
                (record.external_id.clone(), record.kind, seq)
            }
            None => return,
        };
        if self.outbox.len() >= OUTBOX_CAPACITY {
            self.outbox.pop_front();
            self.events_dropped += 1;
        }
        self.outbox.push_back(MediaEvent {
            session,
            external_id,
            session_kind,
            attachment,
            seq,
            legacy_eligible,
            kind,
        });
    }

    fn replay(
        &self,
        key: &Option<String>,
        fingerprint: u64,
    ) -> Result<Option<Outcome>, ControlError> {
        let key = match key {
            Some(key) => key,
            None => return Ok(None),
        };
        match self.idempotency.get(key) {
            Some(record) if record.fingerprint == fingerprint => Ok(Some(record.outcome)),
            Some(_) => Err(ControlError::IdempotencyConflict(key.clone())),
            None => Ok(None),
        }
    }

    fn remember(&mut self, key: &Option<String>, fingerprint: u64, outcome: Outcome) {
        let key = match key {
            Some(key) => key.clone(),
            None => return,
        };
        if self.idempotency.len() >= IDEMPOTENCY_CAPACITY {
            if let Some(oldest) = self.idempotency_order.pop_front() {
                self.idempotency.remove(&oldest);
            }
        }
        self.idempotency_order.push_back(key.clone());
        self.idempotency.insert(
            key,
            IdempotentRecord {
                fingerprint,
                outcome,
            },
        );
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tap(external_id: &str) -> CreateSession {
        CreateSession {
            external_id: external_id.to_string(),
            kind: SessionKind::Tap,
            call_id: "call-abc".to_string(),
            from_tags: vec!["from-a".to_string()],
            rtpengine_node: "rtpengine-1".to_string(),
            sdp_offer: None,
            group: String::new(),
            idempotency_key: None,
        }
    }

    fn spec(
        session: SessionId,
        transport: Transport,
        capabilities: Capabilities,
        label: &str,
    ) -> AttachSpec {
        AttachSpec {
            session,
            transport,
            capabilities,
            selector: TrackSelector::All,
            format: AudioFormat::pcmu_8k_20ms(),
            authoritative: false,
            label: label.to_string(),
            endpoint: "wss:".to_string(),
            group: String::new(),
            metadata: BTreeMap::new(),
            idempotency_key: None,
        }
    }

    fn rtt(session: SessionId) -> AttachSpec {
        spec(
            session,
            Transport::GrpcStream,
            Capabilities::SINK | Capabilities::EVENTS,
            "rtt",
        )
    }

    fn recorder(session: SessionId) -> AttachSpec {
        spec(session, Transport::FileS3, Capabilities::SINK, "recorder")
    }

    fn bridge(session: SessionId) -> AttachSpec {
        spec(
            session,
            Transport::WsTwilio,
            Capabilities::SINK | Capabilities::EVENTS | Capabilities::INJECT,
            "voice-ai",
        )
    }

    fn started() -> (SessionRegistry, SessionId) {
        let mut registry = SessionRegistry::default();
        let session = registry.create_session(tap("req-1")).unwrap().id;
        registry.drain_events();
        (registry, session)
    }

    fn transcript(text: &str) -> ConsumerEvent {
        ConsumerEvent::Final {
            track: Track::Customer,
            text: text.to_string(),
            confidence: 0.9,
        }
    }

    #[test]
    fn an_inline_session_remembers_the_offer_and_the_answer_it_was_given() {
        let mut registry = SessionRegistry::new(DEFAULT_MAX_ATTACHMENTS);
        let created = registry
            .create_session(CreateSession {
                kind: SessionKind::Inline,
                sdp_offer: Some("v=0 offer".to_string()),
                ..tap("req-inline")
            })
            .expect("an inline session");
        assert_eq!(created.sdp_offer.as_deref(), Some("v=0 offer"));
        assert_eq!(created.sdp_answer, None);

        let answered = registry
            .record_sdp_answer(created.id, "v=0 answer".to_string())
            .expect("the answer is recorded");
        assert_eq!(answered.sdp_answer.as_deref(), Some("v=0 answer"));
        assert_eq!(
            registry
                .session_view(created.id)
                .expect("the session")
                .sdp_answer
                .as_deref(),
            Some("v=0 answer")
        );
    }

    #[test]
    fn an_offer_is_part_of_what_an_idempotency_key_replays() {
        let mut registry = SessionRegistry::new(DEFAULT_MAX_ATTACHMENTS);
        let inline = CreateSession {
            kind: SessionKind::Inline,
            sdp_offer: Some("v=0 first".to_string()),
            group: String::new(),
            idempotency_key: Some("key-1".to_string()),
            ..tap("req-inline")
        };
        registry.create_session(inline.clone()).expect("created");
        let replayed = registry.create_session(inline).expect("the same request");
        assert_eq!(replayed.sdp_offer.as_deref(), Some("v=0 first"));
        assert!(matches!(
            registry.create_session(CreateSession {
                sdp_offer: Some("v=0 second".to_string()),
                ..CreateSession {
                    kind: SessionKind::Inline,
                    idempotency_key: Some("key-1".to_string()),
                    ..tap("req-inline")
                }
            }),
            Err(ControlError::IdempotencyConflict(_))
        ));
    }

    #[test]
    fn every_event_names_the_kind_of_session_that_produced_it() {
        let mut registry = SessionRegistry::new(DEFAULT_MAX_ATTACHMENTS);
        let inline = registry
            .create_session(CreateSession {
                kind: SessionKind::Inline,
                sdp_offer: Some("v=0 offer".to_string()),
                ..tap("req-inline")
            })
            .expect("an inline session");
        registry
            .destroy_session(inline.id, "hangup")
            .expect("destroyed");
        let events = registry.drain_events();
        assert!(!events.is_empty());
        for event in events {
            assert_eq!(event.session_kind, SessionKind::Inline);
        }
    }

    #[test]
    fn a_session_is_reachable_by_the_external_id_the_legacy_controller_knows_it_by() {
        let (registry, session) = started();
        assert_eq!(registry.resolve("req-1"), Ok(session));
        assert_eq!(
            registry.resolve("nope"),
            Err(ControlError::UnknownExternalId("nope".to_string()))
        );
        assert_eq!(registry.session_view(session).unwrap().call_id, "call-abc");
    }

    #[test]
    fn a_retried_create_returns_the_same_session_instead_of_a_second_one() {
        let mut registry = SessionRegistry::default();
        let mut request = tap("req-1");
        request.idempotency_key = Some("key-1".to_string());

        let first = registry.create_session(request.clone()).unwrap();
        let retry = registry.create_session(request).unwrap();

        assert_eq!(first.id, retry.id);
        assert_eq!(registry.session_count(), 1);
    }

    #[test]
    fn a_reused_idempotency_key_with_a_different_request_is_refused() {
        let mut registry = SessionRegistry::default();
        let mut first = tap("req-1");
        first.idempotency_key = Some("key-1".to_string());
        registry.create_session(first).unwrap();

        let mut second = tap("req-2");
        second.idempotency_key = Some("key-1".to_string());

        assert_eq!(
            registry.create_session(second),
            Err(ControlError::IdempotencyConflict("key-1".to_string()))
        );
    }

    #[test]
    fn an_attachment_carries_its_group_and_defaults_to_none() {
        let (mut registry, session) = started();
        let ungrouped = registry.attach(recorder(session)).unwrap();
        assert_eq!(ungrouped.group, "");

        let mut grouped = recorder(session);
        grouped.label = "alice".to_string();
        grouped.group = "conf-9".to_string();
        let view = registry.attach(grouped).unwrap();

        assert_eq!(view.group, "conf-9");
        assert_eq!(registry.attachment_view(view.id).unwrap().group, "conf-9");
    }

    #[test]
    fn a_retry_that_changes_only_the_group_is_a_different_request() {
        let (mut registry, session) = started();
        let mut first = recorder(session);
        first.group = "conf-9".to_string();
        first.idempotency_key = Some("key-1".to_string());
        registry.attach(first.clone()).unwrap();

        let replay = registry.attach(first.clone()).unwrap();
        assert_eq!(registry.attachment_view(replay.id).unwrap().group, "conf-9");

        let mut moved = first;
        moved.group = "conf-8".to_string();
        assert_eq!(
            registry.attach(moved),
            Err(ControlError::IdempotencyConflict("key-1".to_string()))
        );
    }

    #[test]
    fn two_live_sessions_cannot_hold_the_same_external_id() {
        let (mut registry, session) = started();
        assert_eq!(
            registry.create_session(tap("req-1")),
            Err(ControlError::ExternalIdInUse {
                external_id: "req-1".to_string(),
                holder: session,
            })
        );
    }

    #[test]
    fn a_recorder_cannot_be_granted_a_back_channel_by_its_transport() {
        let (mut registry, session) = started();
        let mut greedy = recorder(session);
        greedy.capabilities = Capabilities::SINK | Capabilities::INJECT;

        assert_eq!(
            registry.attach(greedy),
            Err(ControlError::TransportCannotCarry {
                transport: Transport::FileS3,
                missing: Capabilities::INJECT,
            })
        );
        assert_eq!(registry.attachment_count(), 0);
    }

    #[test]
    fn a_recorder_cannot_inject_audio_into_a_live_call() {
        let (mut registry, session) = started();
        let sink = registry.attach(recorder(session)).unwrap().id;

        let denied = registry.authorize_inject(sink).unwrap_err();
        assert_eq!(
            denied,
            ControlError::CapabilityDenied {
                attachment: sink,
                granted: Capabilities::SINK,
                missing: Capabilities::INJECT,
            }
        );
        assert!(denied.to_string().contains("may not INJECT"));
    }

    #[test]
    fn an_analytics_sink_cannot_end_the_interaction_it_only_listens_to() {
        let (mut registry, session) = started();
        let analytics = registry
            .attach(spec(
                session,
                Transport::GrpcStream,
                Capabilities::SINK,
                "analytics",
            ))
            .unwrap()
            .id;

        let refused = registry.report(
            analytics,
            ConsumerEvent::EndOfInteraction {
                reason: "goodbye".to_string(),
            },
        );

        assert!(matches!(
            refused,
            Err(ControlError::CapabilityDenied { .. })
        ));
    }

    #[test]
    fn a_second_authoritative_attachment_is_refused_not_silently_resolved() {
        let (mut registry, session) = started();
        let mut first = bridge(session);
        first.authoritative = true;
        let holder = registry.attach(first).unwrap().id;

        let mut second = rtt(session);
        second.authoritative = true;

        assert_eq!(
            registry.attach(second),
            Err(ControlError::AuthoritativeAlreadyBound { session, holder })
        );
    }

    #[test]
    fn detaching_the_authoritative_attachment_frees_the_role_for_a_successor() {
        let (mut registry, session) = started();
        let mut first = bridge(session);
        first.authoritative = true;
        let original = registry.attach(first).unwrap().id;

        registry.detach(original, "pod drained").unwrap();
        assert_eq!(registry.session_view(session).unwrap().authoritative, None);

        let mut successor = rtt(session);
        successor.authoritative = true;
        let taken = registry.attach(successor).unwrap().id;

        assert_eq!(
            registry.session_view(session).unwrap().authoritative,
            Some(taken)
        );
    }

    #[test]
    fn only_the_authoritative_attachments_speech_drives_the_legacy_state_machine() {
        let (mut registry, session) = started();
        let mut authoritative = bridge(session);
        authoritative.authoritative = true;
        let bridge_id = registry.attach(authoritative).unwrap().id;
        let rtt_id = registry.attach(rtt(session)).unwrap().id;
        registry.drain_events();

        registry
            .report(bridge_id, transcript("from the bridge"))
            .unwrap();
        registry
            .report(rtt_id, transcript("from the rtt service"))
            .unwrap();

        let events = registry.drain_events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].legacy_name(), Some("first_transcript"));
        assert_eq!(events[1].legacy_name(), None);
        assert!(events[1].kind.legacy_name().is_some());
        assert_eq!(events[1].attachment, Some(rtt_id));
    }

    #[test]
    fn the_first_final_transcript_is_marked_once_per_attachment() {
        let (mut registry, session) = started();
        let mut authoritative = bridge(session);
        authoritative.authoritative = true;
        let id = registry.attach(authoritative).unwrap().id;
        registry.drain_events();

        registry.report(id, transcript("one")).unwrap();
        registry.report(id, transcript("two")).unwrap();

        let events = registry.drain_events();
        assert_eq!(events[0].legacy_name(), Some("first_transcript"));
        assert_eq!(events[1].legacy_name(), Some("transcription"));
    }

    #[test]
    fn event_sequence_numbers_are_per_session_and_gapless() {
        let mut registry = SessionRegistry::default();
        let first = registry.create_session(tap("req-1")).unwrap().id;
        let second = registry.create_session(tap("req-2")).unwrap().id;
        let a = registry.attach(rtt(first)).unwrap().id;
        let b = registry.attach(rtt(second)).unwrap().id;

        registry.report(a, transcript("one")).unwrap();
        registry.report(b, transcript("two")).unwrap();
        registry.report(a, transcript("three")).unwrap();

        let events = registry.drain_events();
        let seqs: Vec<(SessionId, u64)> = events
            .iter()
            .map(|event| (event.session, event.seq))
            .collect();
        assert_eq!(
            seqs,
            vec![(first, 0), (second, 0), (first, 1), (second, 1), (first, 2),]
        );
        assert!(events
            .iter()
            .all(|event| event.external_id.starts_with("req-")));
    }

    #[test]
    fn every_published_event_carries_identity_the_consumer_could_not_supply() {
        let (mut registry, session) = started();
        let id = registry.attach(rtt(session)).unwrap().id;
        registry.drain_events();

        registry
            .report(
                id,
                ConsumerEvent::Partial {
                    track: Track::Customer,
                    text: "hel".to_string(),
                    confidence: 0.3,
                },
            )
            .unwrap();

        let event = registry.drain_events().pop().unwrap();
        assert_eq!(event.session, session);
        assert_eq!(event.external_id, "req-1");
        assert_eq!(event.attachment, Some(id));
        assert_eq!(event.seq, 1);
    }

    #[test]
    fn a_mix_target_is_audited_when_it_is_declared_and_every_time_it_moves() {
        let (mut registry, session) = started();
        let mut whisperer = bridge(session);
        whisperer.metadata.insert(
            crate::mix::MIX_TARGET_METADATA_KEY.to_string(),
            "agent-7".to_string(),
        );
        let id = registry.attach(whisperer).unwrap().id;
        let declared: Vec<EventKind> = registry
            .drain_events()
            .into_iter()
            .map(|event| event.kind)
            .collect();
        assert!(
            declared.contains(&EventKind::MixRouted {
                target: "agent-7".to_string(),
                monitor_audible: true,
            }),
            "attaching a whisperer says who it whispers to: {declared:?}"
        );

        let flipped = registry
            .update_attachment(
                id,
                AttachmentUpdate {
                    metadata: Some(BTreeMap::from([(
                        crate::mix::MIX_TARGET_METADATA_KEY.to_string(),
                        crate::mix::MIX_TARGET_EVERYONE.to_string(),
                    )])),
                    ..AttachmentUpdate::default()
                },
            )
            .unwrap();
        assert_eq!(
            flipped
                .metadata
                .get(crate::mix::MIX_TARGET_METADATA_KEY)
                .map(String::as_str),
            Some("all"),
            "an update merges metadata rather than replacing it"
        );
        let events = registry.drain_events();
        assert_eq!(
            events.last().map(|event| event.kind.clone()),
            Some(EventKind::MixRouted {
                target: "all".to_string(),
                monitor_audible: true,
            }),
            "the barge flip is auditable too"
        );

        registry
            .update_attachment(
                id,
                AttachmentUpdate {
                    paused: Some(true),
                    ..AttachmentUpdate::default()
                },
            )
            .unwrap();
        assert!(
            registry.drain_events().is_empty(),
            "an update that leaves the route alone says nothing about it"
        );
    }

    #[test]
    fn a_sink_only_attachment_may_not_name_a_mix_target() {
        let (mut registry, session) = started();
        let mut listener = rtt(session);
        listener.metadata.insert(
            crate::mix::MIX_TARGET_METADATA_KEY.to_string(),
            "agent-7".to_string(),
        );
        assert!(matches!(
            registry.attach(listener),
            Err(ControlError::MixRoute(_))
        ));
    }

    #[test]
    fn pausing_an_attachment_is_an_update_rather_than_a_verb_of_its_own() {
        let (mut registry, session) = started();
        let id = registry.attach(rtt(session)).unwrap().id;

        let paused = registry
            .update_attachment(
                id,
                AttachmentUpdate {
                    paused: Some(true),
                    ..AttachmentUpdate::default()
                },
            )
            .unwrap();
        assert!(paused.paused);
        assert_eq!(paused.selector, TrackSelector::All);

        let narrowed = registry
            .update_attachment(
                id,
                AttachmentUpdate {
                    paused: Some(false),
                    selector: Some(TrackSelector::Only(Track::Customer)),
                    format: Some(AudioFormat::l16_16k_20ms()),
                    metadata: None,
                },
            )
            .unwrap();
        assert!(!narrowed.paused);
        assert_eq!(narrowed.selector, TrackSelector::Only(Track::Customer));
        assert_eq!(narrowed.format, AudioFormat::l16_16k_20ms());
    }

    #[test]
    fn destroying_a_session_takes_its_attachments_down_and_frees_the_external_id() {
        let (mut registry, session) = started();
        let id = registry.attach(rtt(session)).unwrap().id;
        registry.drain_events();

        registry.destroy_session(session, "hangup").unwrap();

        let events = registry.drain_events();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].kind,
            EventKind::AttachmentDown {
                label: "rtt".to_string(),
                reason: "hangup".to_string(),
            }
        );
        assert_eq!(events[0].attachment, Some(id));
        assert_eq!(
            events[1].kind,
            EventKind::SessionEnded {
                reason: "hangup".to_string(),
            }
        );
        assert_eq!(registry.session_count(), 0);
        assert_eq!(registry.attachment_count(), 0);

        registry.create_session(tap("req-1")).unwrap();
        assert_eq!(registry.session_count(), 1);
    }

    #[test]
    fn playback_on_behalf_of_an_attachment_requires_the_right_to_inject() {
        let (mut registry, session) = started();
        let listener = registry.attach(rtt(session)).unwrap().id;
        let speaker = registry.attach(bridge(session)).unwrap().id;

        let refused = registry.start_playback(PlaybackSpec {
            session,
            requested_by: Some(listener),
            target_tag: None,
            block_egress: false,
            idempotency_key: None,
        });
        assert!(matches!(
            refused,
            Err(ControlError::CapabilityDenied { .. })
        ));

        let playback = registry
            .start_playback(PlaybackSpec {
                session,
                requested_by: Some(speaker),
                target_tag: Some("from-a".to_string()),
                block_egress: true,
                idempotency_key: None,
            })
            .unwrap();
        let stopped = registry.stop_playback(playback, "barge-in").unwrap();
        assert_eq!(stopped.session, session);
        assert_eq!(stopped.target_tag.as_deref(), Some("from-a"));

        assert_eq!(
            registry.stop_playback(playback, "again"),
            Err(ControlError::UnknownPlayback(playback))
        );
    }

    #[test]
    fn stopping_a_playback_returns_the_participant_it_was_played_to() {
        let (mut registry, session) = started();
        let everyone = registry
            .start_playback(PlaybackSpec {
                session,
                requested_by: None,
                target_tag: None,
                block_egress: false,
                idempotency_key: None,
            })
            .unwrap();
        let one = registry
            .start_playback(PlaybackSpec {
                session,
                requested_by: None,
                target_tag: Some("from-b".to_string()),
                block_egress: false,
                idempotency_key: None,
            })
            .unwrap();

        assert_eq!(
            registry.stop_playback(one, "barge-in").unwrap().target_tag,
            Some("from-b".to_string())
        );
        assert_eq!(
            registry
                .stop_playback(everyone, "prompt done")
                .unwrap()
                .target_tag,
            None
        );
    }

    #[test]
    fn attachments_are_bounded_so_one_session_cannot_grow_without_limit() {
        let mut registry = SessionRegistry::new(2);
        let session = registry.create_session(tap("req-1")).unwrap().id;
        registry.attach(rtt(session)).unwrap();
        registry.attach(rtt(session)).unwrap();

        assert_eq!(
            registry.attach(rtt(session)),
            Err(ControlError::TooManyAttachments { session, limit: 2 })
        );
    }

    #[test]
    fn an_attachment_that_declares_nothing_is_refused() {
        let (mut registry, session) = started();
        let mut silent = rtt(session);
        silent.capabilities = Capabilities::NONE;

        assert_eq!(
            registry.attach(silent),
            Err(ControlError::NoCapabilityDeclared)
        );
    }

    #[test]
    fn work_against_a_vanished_session_or_attachment_is_refused_not_ignored() {
        let (mut registry, session) = started();
        let id = registry.attach(rtt(session)).unwrap().id;
        registry.destroy_session(session, "hangup").unwrap();

        assert_eq!(
            registry.session_view(session),
            Err(ControlError::UnknownSession(session))
        );
        assert_eq!(
            registry.report(id, transcript("too late")),
            Err(ControlError::UnknownAttachment(id))
        );
        assert_eq!(
            registry.observe(
                session,
                Observation::Dtmf {
                    track: Track::Customer,
                    digit: '5',
                }
            ),
            Err(ControlError::UnknownSession(session))
        );
    }

    #[test]
    fn the_outbox_is_bounded_and_counts_what_it_had_to_drop() {
        let (mut registry, session) = started();
        let id = registry.attach(rtt(session)).unwrap().id;
        let overflow = OUTBOX_CAPACITY + 100;

        for _ in 0..overflow {
            registry.report(id, transcript("chatter")).unwrap();
        }

        let events = registry.drain_events();
        assert_eq!(events.len(), OUTBOX_CAPACITY);
        assert_eq!(registry.events_dropped(), 101);
        assert_eq!(events.last().unwrap().seq, overflow as u64);
        assert!(registry.drain_events().is_empty());
    }

    #[test]
    fn mss_owns_the_facts_it_witnesses_itself() {
        let (mut registry, session) = started();
        registry.drain_events();

        registry
            .observe(
                session,
                Observation::Dtmf {
                    track: Track::Customer,
                    digit: '7',
                },
            )
            .unwrap();
        registry
            .observe(
                session,
                Observation::UploadCompleted {
                    recording_id: "rec-1".to_string(),
                    uri: "s3:".to_string(),
                },
            )
            .unwrap();

        let events = registry.drain_events();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event.attachment.is_none()));
        assert!(events.iter().all(|event| event.legacy_eligible));
    }

    #[test]
    fn a_transport_without_a_back_channel_cannot_be_sent_text() {
        let (mut registry, session) = started();
        let sink = registry.attach(recorder(session)).unwrap().id;
        let talker = registry.attach(bridge(session)).unwrap().id;

        assert_eq!(
            registry.authorize_send_text(sink),
            Err(ControlError::TransportCannotCarry {
                transport: Transport::FileS3,
                missing: Capabilities::EVENTS,
            })
        );
        assert_eq!(registry.authorize_send_text(talker), Ok(()));
    }
}
