use crate::convert::{
    attachment_id, capabilities, capabilities_wire, event_wire, format, format_wire, playback_id,
    selector, selector_wire, session_id, session_kind, session_kind_wire, status_of, transport,
    transport_wire,
};
use crate::proto;
use crate::proto::media_control_server::{MediaControl, MediaControlServer};
use session_core::{
    AttachSpec, AttachmentId, AttachmentUpdate, AttachmentView, ControlError, CreateSession,
    EventKind, MediaEvent, Observation, PlaybackId, PlaybackSpec, SessionId, SessionRegistry,
    SessionView,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub const WATCH_CAPACITY: usize = 256;

pub trait EventSink: Send + Sync + 'static {
    fn accept(&self, event: MediaEvent);
}

pub trait ObservationSink: Send + Sync + 'static {
    fn observe(&self, session: SessionId, observation: Observation);
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlaybackSource {
    Blob(Vec<u8>),
    File(String),
    Stream,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct MediaPlaneError(pub String);

#[derive(Debug, Clone, PartialEq)]
pub enum StreamFrame {
    Media {
        track: &'static str,
        pts_ms: u64,
        payload: Vec<u8>,
    },
    Dtmf {
        track: &'static str,
        digit: char,
    },
    Text {
        json: String,
    },
}

#[tonic::async_trait]
pub trait MediaPlane: Send + Sync + 'static {
    async fn open_session(&self, session: SessionView) -> Result<(), MediaPlaneError>;

    async fn close_session(&self, session: SessionId) -> Result<(), MediaPlaneError>;

    async fn open_attachment(&self, attachment: AttachmentView) -> Result<(), MediaPlaneError>;

    async fn update_attachment(&self, attachment: AttachmentView) -> Result<(), MediaPlaneError> {
        let _ = attachment;
        Ok(())
    }

    async fn close_attachment(
        &self,
        session: SessionId,
        attachment: AttachmentId,
    ) -> Result<(), MediaPlaneError>;

    async fn send_text(
        &self,
        attachment: AttachmentId,
        json: String,
    ) -> Result<(), MediaPlaneError>;

    async fn start_playback(
        &self,
        session: SessionId,
        playback: PlaybackId,
        source: PlaybackSource,
        target_tag: Option<String>,
        block_egress: bool,
    ) -> Result<(), MediaPlaneError>;

    async fn stop_playback(
        &self,
        session: SessionId,
        playback: PlaybackId,
    ) -> Result<(), MediaPlaneError>;

    async fn open_stream(
        &self,
        session: SessionId,
        attachment: AttachmentId,
    ) -> Result<mpsc::Receiver<StreamFrame>, MediaPlaneError> {
        let _ = (session, attachment);
        Err(MediaPlaneError(
            "this media plane serves no grpc data plane".to_string(),
        ))
    }
}

pub struct SessionController {
    registry: Mutex<SessionRegistry>,
    watchers: broadcast::Sender<MediaEvent>,
    draining: Arc<watch::Sender<bool>>,
    media: Option<Arc<dyn MediaPlane>>,
    events: Option<Arc<dyn EventSink>>,
    owner: String,
}

impl SessionController {
    pub fn new(owner: impl Into<String>) -> Self {
        SessionController {
            registry: Mutex::new(SessionRegistry::default()),
            watchers: broadcast::Sender::new(WATCH_CAPACITY),
            draining: Arc::new(watch::Sender::new(false)),
            media: None,
            events: None,
            owner: owner.into(),
        }
    }

    pub fn with_event_sink(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.events = Some(sink);
        self
    }

    pub fn drain_handle(&self) -> Arc<watch::Sender<bool>> {
        Arc::clone(&self.draining)
    }

    pub fn begin_drain(&self) {
        let _ = self.draining.send(true);
    }

    pub fn with_media_plane(mut self, media: Arc<dyn MediaPlane>) -> Self {
        self.media = Some(media);
        self
    }

    pub fn into_service(self) -> MediaControlServer<Self> {
        MediaControlServer::new(self)
    }

    pub fn service_for(controller: Arc<SessionController>) -> MediaControlServer<Arc<Self>> {
        MediaControlServer::new(controller)
    }

    pub fn snapshot(&self) -> Vec<(SessionView, Vec<AttachmentView>)> {
        let registry = self.lock();
        registry
            .session_ids()
            .into_iter()
            .filter_map(|id| {
                let session = registry.session_view(id).ok()?;
                let attachments = session
                    .attachments
                    .iter()
                    .filter_map(|held| registry.attachment_view(*held).ok())
                    .collect();
                Some((session, attachments))
            })
            .collect()
    }

    pub fn holds_external_id(&self, external_id: &str) -> bool {
        self.lock().resolve(external_id).is_ok()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MediaEvent> {
        self.watchers.subscribe()
    }

    pub fn attachment(&self, attachment: AttachmentId) -> Result<AttachmentView, Status> {
        self.lock().attachment_view(attachment).map_err(status_of)
    }

    pub fn session(&self, session: SessionId) -> Result<SessionView, Status> {
        self.lock().session_view(session).map_err(status_of)
    }

    pub fn authorize_inject(&self, attachment: AttachmentId) -> Result<(), Status> {
        self.lock().authorize_inject(attachment).map_err(status_of)
    }

    pub fn record_observation(
        &self,
        session: SessionId,
        observation: Observation,
    ) -> Result<(), Status> {
        self.commit(|registry| registry.observe(session, observation))
    }

    pub fn counts(&self) -> (usize, usize) {
        let registry = self.lock();
        (registry.session_count(), registry.attachment_count())
    }

    pub fn events_dropped(&self) -> u64 {
        self.lock().events_dropped()
    }

    pub fn drain_watch(&self) -> watch::Receiver<bool> {
        self.draining.subscribe()
    }

    pub(crate) async fn open_stream(
        &self,
        session: SessionId,
        attachment: AttachmentId,
    ) -> Result<mpsc::Receiver<StreamFrame>, Status> {
        let media = self.media_plane()?.clone();
        media
            .open_stream(session, attachment)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))
    }

    fn lock(&self) -> MutexGuard<'_, SessionRegistry> {
        self.registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn commit<T>(
        &self,
        action: impl FnOnce(&mut SessionRegistry) -> Result<T, ControlError>,
    ) -> Result<T, Status> {
        let (outcome, events) = {
            let mut registry = self.lock();
            let outcome = action(&mut registry);
            let events = registry.drain_events();
            if let Some(sink) = &self.events {
                for event in &events {
                    sink.accept(event.clone());
                }
            }
            (outcome, events)
        };
        for event in events {
            let _ = self.watchers.send(event);
        }
        outcome.map_err(status_of)
    }

    fn media_plane(&self) -> Result<&Arc<dyn MediaPlane>, Status> {
        self.media
            .as_ref()
            .ok_or_else(|| Status::unavailable("this controller has no media plane attached yet"))
    }

    fn resolve(&self, reference: Option<proto::SessionRef>) -> Result<SessionId, Status> {
        let id = reference
            .and_then(|reference| reference.id)
            .ok_or_else(|| Status::invalid_argument("a session reference is required"))?;
        match id {
            proto::session_ref::Id::SessionId(text) => session_id(&text),
            proto::session_ref::Id::ExternalId(external) => {
                self.lock().resolve(&external).map_err(status_of)
            }
        }
    }

    fn session_message(&self, session: SessionId) -> Result<proto::Session, Status> {
        let registry = self.lock();
        let view = registry.session_view(session).map_err(status_of)?;
        let attachments = view
            .attachments
            .iter()
            .filter_map(|id| registry.attachment_view(*id).ok())
            .map(attachment_message)
            .collect();
        Ok(proto::Session {
            session_id: view.id.to_string(),
            external_id: view.external_id,
            kind: session_kind_wire(view.kind),
            state: proto::SessionState::Active as i32,
            call_id: view.call_id,
            from_tags: view.from_tags,
            rtpengine_node: view.rtpengine_node,
            owner_pod: self.owner.clone(),
            attachments,
        })
    }
}

impl ObservationSink for SessionController {
    fn observe(&self, session: SessionId, observation: Observation) {
        if let Err(status) = self.record_observation(session, observation) {
            tracing::warn!(
                %session,
                %status,
                "an observation had nowhere to land; the session is already gone"
            );
        }
    }
}

fn attachment_message(view: AttachmentView) -> proto::Attachment {
    proto::Attachment {
        attachment_id: view.id.to_string(),
        session_id: view.session.to_string(),
        transport: transport_wire(view.transport),
        capabilities: capabilities_wire(view.capabilities),
        selector: Some(selector_wire(view.selector)),
        format: Some(format_wire(view.format)),
        authoritative: view.authoritative,
        paused: view.paused,
        label: view.label,
        group: view.group,
    }
}

fn optional(text: String) -> Option<String> {
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

#[tonic::async_trait]
impl MediaControl for Arc<SessionController> {
    type WatchEventsStream = <SessionController as MediaControl>::WatchEventsStream;

    async fn create_session(
        &self,
        request: Request<proto::CreateSessionRequest>,
    ) -> Result<Response<proto::Session>, Status> {
        self.as_ref().create_session(request).await
    }

    async fn destroy_session(
        &self,
        request: Request<proto::SessionRef>,
    ) -> Result<Response<proto::Ack>, Status> {
        self.as_ref().destroy_session(request).await
    }

    async fn describe_session(
        &self,
        request: Request<proto::SessionRef>,
    ) -> Result<Response<proto::Session>, Status> {
        self.as_ref().describe_session(request).await
    }

    async fn attach(
        &self,
        request: Request<proto::AttachRequest>,
    ) -> Result<Response<proto::Attachment>, Status> {
        self.as_ref().attach(request).await
    }

    async fn detach(
        &self,
        request: Request<proto::AttachmentRef>,
    ) -> Result<Response<proto::Ack>, Status> {
        self.as_ref().detach(request).await
    }

    async fn update_attachment(
        &self,
        request: Request<proto::UpdateAttachmentRequest>,
    ) -> Result<Response<proto::Attachment>, Status> {
        self.as_ref().update_attachment(request).await
    }

    async fn send_to_attachment(
        &self,
        request: Request<proto::SendToAttachmentRequest>,
    ) -> Result<Response<proto::Ack>, Status> {
        self.as_ref().send_to_attachment(request).await
    }

    async fn start_playback(
        &self,
        request: Request<proto::StartPlaybackRequest>,
    ) -> Result<Response<proto::Playback>, Status> {
        self.as_ref().start_playback(request).await
    }

    async fn stop_playback(
        &self,
        request: Request<proto::PlaybackRef>,
    ) -> Result<Response<proto::Ack>, Status> {
        self.as_ref().stop_playback(request).await
    }

    async fn watch_events(
        &self,
        request: Request<proto::WatchRequest>,
    ) -> Result<Response<Self::WatchEventsStream>, Status> {
        self.as_ref().watch_events(request).await
    }
}

#[tonic::async_trait]
impl MediaControl for SessionController {
    async fn create_session(
        &self,
        request: Request<proto::CreateSessionRequest>,
    ) -> Result<Response<proto::Session>, Status> {
        let message = request.into_inner();
        if message.external_id.is_empty() {
            return Err(Status::invalid_argument("external_id is required"));
        }
        if message.mix {
            return Err(Status::unimplemented(
                "mixed subscriptions are not wired to rtpengine yet",
            ));
        }
        let kind = session_kind(message.kind)?;
        let view = self.commit(|registry| {
            registry.create_session(CreateSession {
                external_id: message.external_id,
                kind,
                call_id: message.call_id,
                from_tags: message.from_tags,
                rtpengine_node: message.rtpengine_node,
                idempotency_key: optional(message.idempotency_key),
            })
        })?;
        if let Some(media) = self.media.clone() {
            if let Err(error) = media.open_session(view.clone()).await {
                let _ = self.commit(|registry| {
                    registry.destroy_session(view.id, "the media plane refused the session")
                });
                return Err(Status::unavailable(error.to_string()));
            }
        }
        self.session_message(view.id).map(Response::new)
    }

    async fn destroy_session(
        &self,
        request: Request<proto::SessionRef>,
    ) -> Result<Response<proto::Ack>, Status> {
        let session = self.resolve(Some(request.into_inner()))?;
        self.session(session)?;
        if let Some(media) = self.media.clone() {
            if let Err(error) = media.close_session(session).await {
                tracing::warn!(%session, %error, "the media plane could not close this session");
            }
        }
        self.commit(|registry| registry.destroy_session(session, "destroy requested"))?;
        Ok(Response::new(proto::Ack {}))
    }

    async fn describe_session(
        &self,
        request: Request<proto::SessionRef>,
    ) -> Result<Response<proto::Session>, Status> {
        let session = self.resolve(Some(request.into_inner()))?;
        self.session_message(session).map(Response::new)
    }

    async fn attach(
        &self,
        request: Request<proto::AttachRequest>,
    ) -> Result<Response<proto::Attachment>, Status> {
        let message = request.into_inner();
        let session = self.resolve(message.session)?;
        let transport = transport(message.transport)?;
        let capabilities = capabilities(&message.capabilities)?;
        let selector = selector(message.selector.as_ref())?;
        let format = format(message.format.as_ref())?;
        let metadata: BTreeMap<String, String> = message.metadata.into_iter().collect();

        let view = self.commit(|registry| {
            registry.attach(AttachSpec {
                session,
                transport,
                capabilities,
                selector,
                format,
                authoritative: message.authoritative,
                label: message.label,
                endpoint: message.endpoint,
                group: message.group,
                metadata,
                idempotency_key: optional(message.idempotency_key),
            })
        })?;
        if let Some(media) = self.media.clone() {
            if let Err(error) = media.open_attachment(view.clone()).await {
                let _ =
                    self.commit(|registry| registry.detach(view.id, "the media plane refused it"));
                return Err(Status::unavailable(error.to_string()));
            }
        }
        Ok(Response::new(attachment_message(view)))
    }

    async fn detach(
        &self,
        request: Request<proto::AttachmentRef>,
    ) -> Result<Response<proto::Ack>, Status> {
        let attachment = attachment_id(&request.into_inner().attachment_id)?;
        let session = self
            .lock()
            .attachment_view(attachment)
            .map(|view| view.session)
            .map_err(status_of)?;
        self.commit(|registry| registry.detach(attachment, "detach requested"))?;
        if let Some(media) = self.media.clone() {
            if let Err(error) = media.close_attachment(session, attachment).await {
                tracing::warn!(%attachment, %error, "the media plane could not close this attachment");
            }
        }
        Ok(Response::new(proto::Ack {}))
    }

    async fn update_attachment(
        &self,
        request: Request<proto::UpdateAttachmentRequest>,
    ) -> Result<Response<proto::Attachment>, Status> {
        let message = request.into_inner();
        let attachment = attachment_id(&message.attachment_id)?;
        let selector = match message.selector.as_ref() {
            Some(wire) => Some(selector(Some(wire))?),
            None => None,
        };
        let format = match message.format.as_ref() {
            Some(wire) => Some(format(Some(wire))?),
            None => None,
        };
        let before = self.lock().attachment_view(attachment).map_err(status_of)?;
        let view = self.commit(|registry| {
            registry.update_attachment(
                attachment,
                AttachmentUpdate {
                    paused: message.paused,
                    selector,
                    format,
                },
            )
        })?;
        if let Some(media) = self.media.clone() {
            if let Err(error) = media.update_attachment(view.clone()).await {
                let _ = self.commit(|registry| {
                    registry.update_attachment(
                        attachment,
                        AttachmentUpdate {
                            paused: Some(before.paused),
                            selector: Some(before.selector),
                            format: Some(before.format),
                        },
                    )
                });
                return Err(Status::unavailable(error.to_string()));
            }
        }
        Ok(Response::new(attachment_message(view)))
    }

    async fn send_to_attachment(
        &self,
        request: Request<proto::SendToAttachmentRequest>,
    ) -> Result<Response<proto::Ack>, Status> {
        let message = request.into_inner();
        let attachment = attachment_id(&message.attachment_id)?;
        self.lock()
            .authorize_send_text(attachment)
            .map_err(status_of)?;
        let media = self.media_plane()?.clone();
        media
            .send_text(attachment, message.json)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        Ok(Response::new(proto::Ack {}))
    }

    async fn start_playback(
        &self,
        request: Request<proto::StartPlaybackRequest>,
    ) -> Result<Response<proto::Playback>, Status> {
        let message = request.into_inner();
        let session = self.resolve(message.session)?;
        let source = match message.source {
            Some(proto::start_playback_request::Source::Blob(blob)) => PlaybackSource::Blob(blob),
            Some(proto::start_playback_request::Source::File(file)) => PlaybackSource::File(file),
            Some(proto::start_playback_request::Source::Stream(true)) => PlaybackSource::Stream,
            Some(proto::start_playback_request::Source::Stream(false)) | None => {
                return Err(Status::invalid_argument("a playback source is required"))
            }
        };
        let requested_by = match optional(message.requested_by) {
            Some(text) => Some(attachment_id(&text)?),
            None => None,
        };
        let target_tag = optional(message.target_tag);

        let playback = self.commit(|registry| {
            registry.start_playback(PlaybackSpec {
                session,
                requested_by,
                target_tag: target_tag.clone(),
                block_egress: message.block_egress,
                idempotency_key: optional(message.idempotency_key),
            })
        })?;

        let started = match self.media_plane() {
            Ok(media) => {
                let media = media.clone();
                media
                    .start_playback(
                        session,
                        playback,
                        source,
                        target_tag.clone(),
                        message.block_egress,
                    )
                    .await
                    .map_err(|error| Status::unavailable(error.to_string()))
            }
            Err(status) => Err(status),
        };
        if let Err(status) = started {
            let _ = self.commit(|registry| registry.stop_playback(playback, "start failed"));
            return Err(status);
        }

        Ok(Response::new(proto::Playback {
            playback_id: playback.to_string(),
            session_id: session.to_string(),
        }))
    }

    async fn stop_playback(
        &self,
        request: Request<proto::PlaybackRef>,
    ) -> Result<Response<proto::Ack>, Status> {
        let playback = playback_id(&request.into_inner().playback_id)?;
        let session = self.commit(|registry| registry.stop_playback(playback, "stop requested"))?;
        if let Some(media) = self.media.clone() {
            if let Err(error) = media.stop_playback(session, playback).await {
                tracing::warn!(%playback, %error, "the media plane could not stop this playback");
            }
        }
        Ok(Response::new(proto::Ack {}))
    }

    type WatchEventsStream = ReceiverStream<Result<proto::MediaEvent, Status>>;

    async fn watch_events(
        &self,
        request: Request<proto::WatchRequest>,
    ) -> Result<Response<Self::WatchEventsStream>, Status> {
        let wanted = match request.into_inner().session {
            Some(reference) => Some(self.resolve(Some(reference))?),
            None => None,
        };
        let mut events = self.watchers.subscribe();
        let mut draining = self.draining.subscribe();
        let (sender, receiver) = mpsc::channel(WATCH_CAPACITY);

        tokio::spawn(async move {
            if *draining.borrow() {
                return;
            }
            loop {
                tokio::select! {
                    changed = draining.changed() => {
                        if changed.is_err() || *draining.borrow() {
                            return;
                        }
                    }
                    received = events.recv() => match received {
                        Ok(event) => {
                            if wanted.is_some_and(|session| session != event.session) {
                                continue;
                            }
                            let ends_the_watch = wanted.is_some()
                                && matches!(event.kind, EventKind::SessionEnded { .. });
                            if sender.send(Ok(event_wire(event))).await.is_err() {
                                return;
                            }
                            if ends_the_watch {
                                return;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(missed)) => {
                            tracing::warn!(missed, "a watch_events subscriber fell behind");
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    },
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
