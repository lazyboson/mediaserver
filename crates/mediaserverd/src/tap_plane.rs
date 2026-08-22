use crate::consumer_ws::{self, ConsumerConfig};
use crate::hub::{Hub, HubClient, Subscription, SubscriptionMetrics, TapEvent, TrackSelection};
use crate::ng_transport::{NgTransport, NgTransportConfig};
use crate::recorder::{
    self, Layout, RecorderCounters, RecorderHandle, RecorderSpec, RecordingIdentity,
    RecordingSupport,
};
use crate::tap_spike::{
    capture, SharedLegStats, SsrcTrackPublisher, SsrcTracks, TapLeg, MAX_SSRC_TRACKS,
};
use control_api::{MediaPlane, MediaPlaneError, ObservationSink, PlaybackSource, StreamFrame};
use media_core::{AudioFormat, ConsumerEncoder, Track};
use rtpengine_ng::{
    PlayMedia, PlaySource, PlayTarget, SubscribeRequest, SubscriptionAnswer, SubscriptionOffer,
};
use session_core::{
    AttachmentId, AttachmentView, Observation, SessionId, SessionKind, SessionView, TrackSelector,
    Transport,
};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};

pub const MAX_SESSION_DURATION: Duration = Duration::from_secs(8 * 3600);
pub const MAX_PLAYBACK_BLOB_BYTES: usize = 60_000;
pub const STALL_AFTER: Duration = Duration::from_secs(10);

const TARGET_DEPTH_PACKETS: u16 = 3;
const MAX_TAPPED_STREAMS: usize = 2;
const MAX_TAPPED_LEGS: usize = 2;
const CONSUMER_QUEUE_FRAMES: usize = 200;
const TEXT_QUEUE_DEPTH: usize = 32;
const GRPC_FRAME_QUEUE: usize = 64;
const RETAIN_NO_LOCAL_AUDIO: Duration = Duration::ZERO;
const SSRC_WATCH_INTERVAL: Duration = Duration::from_millis(500);
const MAX_REQUERIED_SSRCS: usize = 16;
const ACCOUNT_METADATA_KEY: &str = "accountId";
const STREAM_SID_METADATA_KEY: &str = "streamSid";
const DEFAULT_ACCOUNT_ID: &str = "mss";

pub struct TapPlaneConfig {
    pub default_node: Option<SocketAddr>,
    pub local_media_address: IpAddr,
    pub format: AudioFormat,
    pub transcode_at_tap: bool,
    pub cookie_prefix: u64,
    pub sdp_session_id: u64,
    pub recording: RecordingSupport,
}

struct LiveSession {
    transport: Arc<NgTransport>,
    external_id: String,
    call_id: String,
    to_tag: String,
    format: AudioFormat,
    hub: HubClient,
    stop: Arc<AtomicBool>,
    capture: Option<std::thread::JoinHandle<()>>,
    speakers: Option<tokio::task::JoinHandle<()>>,
}

enum LiveAttachment {
    Ws {
        session: SessionId,
        text: mpsc::Sender<String>,
        task:
            tokio::task::JoinHandle<Result<consumer_ws::ConsumerStats, consumer_ws::ConsumerError>>,
    },
    Grpc {
        session: SessionId,
        selection: TrackSelection,
        format: AudioFormat,
        live: Option<GrpcLive>,
    },
    Recording {
        session: SessionId,
        recording_id: String,
        handle: Option<RecorderHandle>,
    },
}

impl LiveAttachment {
    fn session(&self) -> SessionId {
        match self {
            LiveAttachment::Ws { session, .. }
            | LiveAttachment::Grpc { session, .. }
            | LiveAttachment::Recording { session, .. } => *session,
        }
    }
}

struct GrpcLive {
    frames: mpsc::Sender<StreamFrame>,
    pump: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LegTotals {
    pub datagrams: u64,
    pub recv_errors: u64,
    pub underruns: u64,
    pub frames_played: u64,
    pub frames_concealed: u64,
    pub frames_suppressed: u64,
    pub companded: u64,
    pub unknown_payload_type: u64,
    pub unparsable: u64,
    pub telephone_events: u64,
    pub dtmf_digits: u64,
    pub jitter_lost: u64,
    pub jitter_duplicates: u64,
    pub jitter_late_drops: u64,
    pub jitter_resets: u64,
    pub jitter_silence_gaps: u64,
    pub ssrc_changes: u64,
    pub reresolutions: u64,
    pub stalls: u64,
}

impl LegTotals {
    fn add_shared(&mut self, shared: &SharedLegStats) {
        let read = |value: &std::sync::atomic::AtomicU64| value.load(Ordering::Relaxed);
        self.datagrams += read(&shared.datagrams);
        self.recv_errors += read(&shared.recv_errors);
        self.underruns += read(&shared.underruns);
        self.frames_played += read(&shared.frames_played);
        self.frames_concealed += read(&shared.frames_concealed);
        self.frames_suppressed += read(&shared.frames_suppressed);
        self.companded += read(&shared.companded);
        self.unknown_payload_type += read(&shared.unknown_payload_type);
        self.unparsable += read(&shared.unparsable);
        self.telephone_events += read(&shared.telephone_events);
        self.dtmf_digits += read(&shared.dtmf_digits);
        self.jitter_lost += read(&shared.jitter_lost);
        self.jitter_duplicates += read(&shared.jitter_duplicates);
        self.jitter_late_drops += read(&shared.jitter_late_drops);
        self.jitter_resets += read(&shared.jitter_resets);
        self.jitter_silence_gaps += read(&shared.jitter_silence_gaps);
        self.ssrc_changes += read(&shared.ssrc_changes);
        self.reresolutions += read(&shared.reresolutions);
        self.stalls += read(&shared.stalls);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IngestSnapshot {
    pub totals: LegTotals,
    pub sessions_live: u64,
    pub legs_live: u64,
    pub legs_unknown_ssrc: u64,
    pub legs_stalled: u64,
    pub ssrc_requeries: u64,
    pub consumers_live: u64,
    pub consumer_dropped_oldest: u64,
    pub consumer_delivered: u64,
    pub consumer_queue_depth: u64,
    pub consumer_queue_depth_max: u64,
    pub recordings_live: u64,
    pub recordings_started: u64,
    pub recordings_stopped: u64,
    pub recording_pauses: u64,
    pub recording_uploads: u64,
    pub recording_upload_failures: u64,
    pub recording_spills: u64,
    pub recording_bytes_uploaded: u64,
    pub recording_seconds: u64,
    pub recordings_truncated: u64,
}

#[derive(Default)]
struct MetricsInner {
    retired: LegTotals,
    retired_consumer_dropped: u64,
    retired_consumer_delivered: u64,
    ssrc_requeries: u64,
    legs: HashMap<SessionId, Vec<Arc<SharedLegStats>>>,
    consumers: HashMap<AttachmentId, SubscriptionMetrics>,
    recorder: Arc<RecorderCounters>,
}

#[derive(Clone, Default)]
pub struct TapPlaneMetrics(Arc<Mutex<MetricsInner>>);

impl TapPlaneMetrics {
    fn with_recorder(recorder: Arc<RecorderCounters>) -> TapPlaneMetrics {
        TapPlaneMetrics(Arc::new(Mutex::new(MetricsInner {
            recorder,
            ..MetricsInner::default()
        })))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MetricsInner> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn register_session(&self, session: SessionId, legs: Vec<Arc<SharedLegStats>>) {
        self.lock().legs.insert(session, legs);
    }

    fn retire_session(&self, session: SessionId) {
        let mut inner = self.lock();
        if let Some(legs) = inner.legs.remove(&session) {
            for leg in legs {
                inner.retired.add_shared(&leg);
            }
        }
    }

    fn record_ssrc_requery(&self) {
        self.lock().ssrc_requeries += 1;
    }

    fn register_consumer(&self, attachment: AttachmentId, metrics: SubscriptionMetrics) {
        let mut inner = self.lock();
        if let Some(replaced) = inner.consumers.insert(attachment, metrics) {
            inner.retired_consumer_dropped += replaced.dropped_oldest();
            inner.retired_consumer_delivered += replaced.delivered();
        }
    }

    fn retire_consumer(&self, attachment: AttachmentId) {
        let mut inner = self.lock();
        if let Some(removed) = inner.consumers.remove(&attachment) {
            inner.retired_consumer_dropped += removed.dropped_oldest();
            inner.retired_consumer_delivered += removed.delivered();
        }
    }

    pub fn snapshot(&self) -> IngestSnapshot {
        let inner = self.lock();
        let recorder = &inner.recorder;
        let read = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let mut snapshot = IngestSnapshot {
            totals: inner.retired,
            sessions_live: inner.legs.len() as u64,
            consumers_live: inner.consumers.len() as u64,
            consumer_dropped_oldest: inner.retired_consumer_dropped,
            consumer_delivered: inner.retired_consumer_delivered,
            ssrc_requeries: inner.ssrc_requeries,
            recordings_live: read(&recorder.live),
            recordings_started: read(&recorder.started),
            recordings_stopped: read(&recorder.stopped),
            recording_pauses: read(&recorder.pauses),
            recording_uploads: read(&recorder.uploaded),
            recording_upload_failures: read(&recorder.upload_failures),
            recording_spills: read(&recorder.spilled),
            recording_bytes_uploaded: read(&recorder.bytes_uploaded),
            recording_seconds: read(&recorder.seconds_recorded),
            recordings_truncated: read(&recorder.truncated),
            ..IngestSnapshot::default()
        };
        for legs in inner.legs.values() {
            for leg in legs {
                snapshot.totals.add_shared(leg);
                snapshot.legs_live += 1;
                snapshot.legs_unknown_ssrc += leg.unknown_ssrc.load(Ordering::Relaxed);
                snapshot.legs_stalled += leg.stalled.load(Ordering::Relaxed);
            }
        }
        for consumer in inner.consumers.values() {
            snapshot.consumer_dropped_oldest += consumer.dropped_oldest();
            snapshot.consumer_delivered += consumer.delivered();
            let depth = consumer.queue_depth() as u64;
            snapshot.consumer_queue_depth += depth;
            snapshot.consumer_queue_depth_max = snapshot.consumer_queue_depth_max.max(depth);
        }
        snapshot
    }
}

pub struct TapPlane {
    config: TapPlaneConfig,
    sessions: Mutex<HashMap<SessionId, LiveSession>>,
    attachments: Mutex<HashMap<AttachmentId, LiveAttachment>>,
    metrics: TapPlaneMetrics,
    observations: OnceLock<Weak<dyn ObservationSink>>,
}

impl TapPlane {
    pub fn new(config: TapPlaneConfig) -> Self {
        let metrics = TapPlaneMetrics::with_recorder(Arc::clone(&config.recording.counters));
        TapPlane {
            config,
            sessions: Mutex::new(HashMap::new()),
            attachments: Mutex::new(HashMap::new()),
            metrics,
            observations: OnceLock::new(),
        }
    }

    pub fn observe_through(&self, sink: Weak<dyn ObservationSink>) {
        if self.observations.set(sink).is_err() {
            warn!("this tap plane already reports its observations somewhere");
        }
    }

    pub fn live_sessions(&self) -> usize {
        self.sessions.lock().map(|held| held.len()).unwrap_or(0)
    }

    pub fn metrics(&self) -> TapPlaneMetrics {
        self.metrics.clone()
    }

    fn observer(&self) -> Option<Weak<dyn ObservationSink>> {
        self.observations.get().cloned()
    }

    fn observe(&self, session: SessionId, observation: Observation) {
        match self.observer().as_ref().and_then(Weak::upgrade) {
            Some(sink) => sink.observe(session, observation),
            None => warn!(
                %session,
                ?observation,
                "no observation sink is wired; this callback reaches nobody"
            ),
        }
    }

    fn node_for(&self, view: &SessionView) -> Result<SocketAddr, MediaPlaneError> {
        if !view.rtpengine_node.is_empty() {
            return view.rtpengine_node.parse().map_err(|_| {
                MediaPlaneError(format!(
                    "rtpengine_node {} is not an ip:port address",
                    view.rtpengine_node
                ))
            });
        }
        self.config.default_node.ok_or_else(|| {
            MediaPlaneError(
                "this session named no rtpengine node and the daemon has no default".to_string(),
            )
        })
    }

    async fn complete_from_tags(
        &self,
        transport: &NgTransport,
        mut view: SessionView,
    ) -> Result<SessionView, MediaPlaneError> {
        if view.from_tags.len() >= MAX_TAPPED_LEGS {
            return Ok(view);
        }
        let reply = transport
            .query(&view.call_id)
            .await
            .map_err(|error| MediaPlaneError(format!("query for {}: {error}", view.call_id)))?;
        let known = reply.tags();
        if known.is_empty() {
            return Err(MediaPlaneError(format!(
                "rtpengine knows no participants for call {}; \
                 it is not anchoring that call",
                view.call_id
            )));
        }
        let caller_known = !view.from_tags.is_empty();
        for tag in known {
            if view.from_tags.len() >= MAX_TAPPED_LEGS {
                break;
            }
            if !view.from_tags.contains(&tag) {
                view.from_tags.push(tag);
            }
        }
        if caller_known {
            info!(
                call_id = %view.call_id,
                from_tags = ?view.from_tags,
                "resolved this call's participants; the caller was named so the \
                 customer leg is known"
            );
        } else {
            warn!(
                call_id = %view.call_id,
                from_tags = ?view.from_tags,
                caller_tag_key = telcompat_caller_tag_key(),
                "resolved this call's participants but nobody named the caller, so \
                 customer and agent are assigned by tag order and may be swapped; \
                 pass the caller's sip from-tag to fix it"
            );
        }
        Ok(view)
    }

    fn open_ws_attachment(&self, view: AttachmentView) -> Result<(), MediaPlaneError> {
        if view.endpoint.is_empty() {
            return Err(MediaPlaneError(
                "a ws-twilio attachment needs its endpoint url".to_string(),
            ));
        }
        if view.format != AudioFormat::pcmu_8k_20ms() {
            return Err(MediaPlaneError(
                "the ws-twilio dialect is frozen at PCMU 8k 20ms; \
                 attach over grpc-stream for other formats"
                    .to_string(),
            ));
        }

        let (_, call_id, hub, external_id, _) = self.session_handles(view.session)?;
        let subscription = hub
            .attach(CONSUMER_QUEUE_FRAMES, selection_of(view.selector))
            .ok_or_else(|| {
                MediaPlaneError("the hub would not take another consumer".to_string())
            })?;
        let subscription_metrics = subscription.metrics();

        let stream_sid = view
            .metadata
            .get(STREAM_SID_METADATA_KEY)
            .cloned()
            .unwrap_or_else(|| view.id.to_string());
        let config = ConsumerConfig {
            url: view.endpoint.clone(),
            account_id: view
                .metadata
                .get(ACCOUNT_METADATA_KEY)
                .cloned()
                .unwrap_or_else(|| DEFAULT_ACCOUNT_ID.to_string()),
            call_sid: if call_id.is_empty() {
                external_id
            } else {
                call_id
            },
            stream_sid,
            format: view.format,
            tracks: tracks_of(view.selector),
            custom_parameters: view
                .metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };

        let (text, inbound) = mpsc::channel(TEXT_QUEUE_DEPTH);
        info!(
            attachment = %view.id,
            session = %view.session,
            label = %view.label,
            url = %view.endpoint,
            "connecting a consumer for the control plane"
        );
        let task = tokio::spawn(consumer_ws::run(config, subscription, None, Some(inbound)));

        let mut held = self
            .attachments
            .lock()
            .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
        held.insert(
            view.id,
            LiveAttachment::Ws {
                session: view.session,
                text,
                task,
            },
        );
        drop(held);
        self.metrics
            .register_consumer(view.id, subscription_metrics);
        Ok(())
    }

    fn open_grpc_attachment(&self, view: AttachmentView) -> Result<(), MediaPlaneError> {
        let tap = self.session_format(view.session)?;
        if !ConsumerEncoder::supports(tap, view.format) {
            return Err(MediaPlaneError(format!(
                "a {:?} tap cannot serve {:?}: g711 at the tap rate and L16 at any rate \
                 are served; opus is not built yet",
                tap.encoding, view.format
            )));
        }
        let mut held = self
            .attachments
            .lock()
            .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
        held.insert(
            view.id,
            LiveAttachment::Grpc {
                session: view.session,
                selection: selection_of(view.selector),
                format: view.format,
                live: None,
            },
        );
        info!(
            attachment = %view.id,
            session = %view.session,
            label = %view.label,
            "a grpc-stream attachment is waiting for its consumer to subscribe"
        );
        Ok(())
    }

    fn open_recording_attachment(&self, view: AttachmentView) -> Result<(), MediaPlaneError> {
        let identity = RecordingIdentity::parse(&view.endpoint).map_err(|error| {
            MediaPlaneError(format!(
                "{error}; a file-s3 endpoint is the frozen recording identity {}",
                recorder::IDENTITY_SCHEME
            ))
        })?;
        if self.config.recording.sink.is_none() {
            return Err(MediaPlaneError(
                "no recording storage is configured on this pod, so a file-s3 \
                 attachment would record into nothing"
                    .to_string(),
            ));
        }
        let (_, _, hub, _, _) = self.session_handles(view.session)?;
        let selection = selection_of(view.selector);
        let subscription = hub
            .attach(CONSUMER_QUEUE_FRAMES, selection)
            .ok_or_else(|| {
                MediaPlaneError("the hub would not take another consumer".to_string())
            })?;
        let subscription_metrics = subscription.metrics();
        let key = identity.object_key();
        let recording_id = identity.recording_id.clone();
        let handle = recorder::spawn(
            RecorderSpec {
                session: view.session,
                identity,
                layout: layout_of(view.selector),
                sample_rate_hz: self.config.format.sample_rate_hz,
                max_duration: recorder::MAX_RECORDING,
            },
            subscription,
            self.config.recording.clone(),
            self.observer(),
        );

        let mut held = self
            .attachments
            .lock()
            .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
        held.insert(
            view.id,
            LiveAttachment::Recording {
                session: view.session,
                recording_id: recording_id.clone(),
                handle: Some(handle),
            },
        );
        drop(held);
        self.metrics
            .register_consumer(view.id, subscription_metrics);
        self.config
            .recording
            .counters
            .started
            .fetch_add(1, Ordering::Relaxed);
        info!(
            attachment = %view.id,
            session = %view.session,
            label = %view.label,
            %key,
            ?selection,
            "recording this call"
        );
        self.observe(
            view.session,
            Observation::RecordingStarted {
                recording_id,
                path: key,
            },
        );
        Ok(())
    }

    fn take_attachments_of(
        &self,
        session: SessionId,
    ) -> Result<Vec<(AttachmentId, LiveAttachment)>, MediaPlaneError> {
        let mut held = self
            .attachments
            .lock()
            .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
        let mine: Vec<AttachmentId> = held
            .iter()
            .filter(|(_, live)| live.session() == session)
            .map(|(id, _)| *id)
            .collect();
        Ok(mine
            .into_iter()
            .filter_map(|id| held.remove(&id).map(|live| (id, live)))
            .collect())
    }

    async fn finish_recording(
        &self,
        attachment: AttachmentId,
        mut live: LiveAttachment,
    ) -> Option<()> {
        let LiveAttachment::Recording {
            session,
            recording_id,
            handle,
        } = &mut live
        else {
            return None;
        };
        let handle = handle.take()?;
        let outcome = handle.finish().await;
        info!(
            %attachment,
            session = %session,
            %recording_id,
            duration_ms = outcome.as_ref().map(|done| done.duration_ms),
            uri = ?outcome.as_ref().and_then(|done| done.uri.clone()),
            "recording finished"
        );
        Some(())
    }

    fn session_format(&self, session: SessionId) -> Result<AudioFormat, MediaPlaneError> {
        let held = self
            .sessions
            .lock()
            .map_err(|_| MediaPlaneError("the session table is poisoned".to_string()))?;
        held.get(&session)
            .map(|live| live.format)
            .ok_or_else(|| MediaPlaneError(format!("{session} is not tapped here")))
    }

    fn session_handles(
        &self,
        session: SessionId,
    ) -> Result<(Arc<NgTransport>, String, HubClient, String, String), MediaPlaneError> {
        let held = self
            .sessions
            .lock()
            .map_err(|_| MediaPlaneError("the session table is poisoned".to_string()))?;
        let live = held
            .get(&session)
            .ok_or_else(|| MediaPlaneError(format!("{session} is not tapped here")))?;
        Ok((
            Arc::clone(&live.transport),
            live.call_id.clone(),
            live.hub.clone(),
            live.external_id.clone(),
            live.to_tag.clone(),
        ))
    }
}

#[control_api::async_trait]
impl MediaPlane for TapPlane {
    async fn open_session(&self, view: SessionView) -> Result<(), MediaPlaneError> {
        if view.kind != SessionKind::Tap {
            return Err(MediaPlaneError(
                "only tap sessions are implemented; inline legs are phase 3".to_string(),
            ));
        }
        if view.call_id.is_empty() {
            return Err(MediaPlaneError("a tap needs the call-id".to_string()));
        }
        let node = self.node_for(&view)?;
        let configured = self.config.format;
        let transcoding = self.config.transcode_at_tap;

        let transport = Arc::new(
            NgTransport::bind(
                SocketAddr::new(self.config.local_media_address, 0),
                node,
                NgTransportConfig::default(),
                self.config.cookie_prefix,
            )
            .await
            .map_err(|error| MediaPlaneError(format!("NG socket: {error}")))?,
        );

        let view = self.complete_from_tags(&transport, view).await?;

        let reply = transport
            .subscribe_request(&SubscribeRequest {
                call_id: view.call_id.clone(),
                from_tags: view.from_tags.clone(),
                mix: false,
                accept_codecs: Vec::new(),
                transcode_codecs: if transcoding {
                    vec![configured.encoding.rtpmap_name().to_string()]
                } else {
                    Vec::new()
                },
                label: Some("mss-tap".to_string()),
            })
            .await
            .map_err(|error| MediaPlaneError(format!("subscribe request: {error}")))?;

        let offer_sdp = reply
            .sdp()
            .ok_or_else(|| MediaPlaneError("rtpengine answered without an sdp".to_string()))?
            .to_string();
        let to_tag = reply
            .to_tag()
            .ok_or_else(|| MediaPlaneError("rtpengine answered without a to-tag".to_string()))?
            .to_string();
        let offer = SubscriptionOffer::parse(&offer_sdp)
            .map_err(|error| MediaPlaneError(format!("subscription sdp: {error}")))?;
        if offer.streams.is_empty() || offer.streams.len() > MAX_TAPPED_STREAMS {
            return Err(MediaPlaneError(format!(
                "rtpengine offered {} streams; this tap handles 1 or {MAX_TAPPED_STREAMS}",
                offer.streams.len()
            )));
        }

        for (index, stream) in offer.streams.iter().enumerate() {
            info!(
                index,
                label = ?stream.label,
                source_port = stream.port,
                payload_types = ?stream.payload_types,
                requested_tags = ?view.from_tags,
                "rtpengine offered a tap stream"
            );
        }

        let format = if transcoding {
            configured
        } else {
            offered_tap_format(&offer, configured.ptime_ms)?
        };
        info!(
            session = %view.id,
            transcoding,
            encoding = ?format.encoding,
            sample_rate_hz = format.sample_rate_hz,
            ptime_ms = format.ptime_ms,
            "settled the tap format"
        );

        let mut sockets = Vec::with_capacity(offer.streams.len());
        let mut receive_ports = Vec::with_capacity(offer.streams.len());
        for _ in &offer.streams {
            let socket = UdpSocket::bind(SocketAddr::new(self.config.local_media_address, 0))
                .map_err(|error| MediaPlaneError(format!("media socket: {error}")))?;
            receive_ports.push(
                socket
                    .local_addr()
                    .map_err(|error| MediaPlaneError(format!("media socket: {error}")))?
                    .port(),
            );
            sockets.push(socket);
        }

        let local_address = self.config.local_media_address.to_string();
        let answer_sdp = SubscriptionAnswer {
            session_id: self.config.sdp_session_id,
            local_address: &local_address,
            receive_ports: &receive_ports,
            format,
        }
        .to_sdp(&offer)
        .map_err(|error| MediaPlaneError(format!("answer sdp: {error}")))?;

        transport
            .subscribe_answer(&view.call_id, &to_tag, &answer_sdp)
            .await
            .map_err(|error| MediaPlaneError(format!("subscribe answer: {error}")))?;

        let ssrc_tracks = speaker_ssrcs(&transport, &view.call_id, &view.from_tags).await;

        let mut legs = Vec::with_capacity(sockets.len());
        let mut shared_stats = Vec::with_capacity(sockets.len());
        let mut ssrc_publishers = Vec::with_capacity(sockets.len());
        for (index, socket) in sockets.into_iter().enumerate() {
            let telephone_event = offer
                .streams
                .get(index)
                .and_then(|stream| stream.telephone_event())
                .map(|event| event.payload_type);
            let shared = Arc::new(SharedLegStats::default());
            shared_stats.push(Arc::clone(&shared));
            let leg = TapLeg::new(
                speaker_track(index),
                socket,
                format,
                TARGET_DEPTH_PACKETS,
                telephone_event,
                RETAIN_NO_LOCAL_AUDIO,
            )
            .map_err(|error| MediaPlaneError(format!("tap leg: {error}")))?
            .with_ssrc_tracks(ssrc_tracks.clone())
            .with_shared_stats(shared, STALL_AFTER, Instant::now());
            ssrc_publishers.push(leg.ssrc_track_publisher());
            legs.push(leg);
        }

        let (mut hub, hub_client) = Hub::new();
        let stop = Arc::new(AtomicBool::new(false));
        let capture_stop = Arc::clone(&stop);
        let capture_thread = std::thread::Builder::new()
            .name(format!("mss-tap-{}", view.id.raw()))
            .spawn(move || {
                let summary = capture(
                    &mut legs,
                    Some(&mut hub),
                    format,
                    MAX_SESSION_DURATION,
                    &capture_stop,
                );
                for leg in legs.iter() {
                    let stats = leg.stats();
                    info!(
                        track = ?leg.track(),
                        datagrams = stats.datagrams,
                        frames_played = stats.pipeline.frames_played,
                        frames_concealed = stats.pipeline.frames_concealed,
                        frames_suppressed = stats.pipeline.frames_suppressed,
                        underruns = stats.underruns,
                        companded = stats.pipeline.companded,
                        unknown_payload_type = stats.pipeline.unknown_payload_type,
                        unparsable = stats.pipeline.unparsable,
                        telephone_event_packets = stats.pipeline.telephone_events,
                        dtmf_digits = stats.pipeline.dtmf_digits,
                        jitter_lost = stats.jitter.lost,
                        jitter_duplicates = stats.jitter.duplicates,
                        jitter_late_drops = stats.jitter.late_drops,
                        jitter_resets = stats.jitter.resets,
                        jitter_silence_gaps = stats.jitter.silence_gaps,
                        jitter_target_depth = stats.jitter.target_depth,
                        recv_errors = stats.recv_errors,
                        unknown_ssrc = stats.unknown_ssrc,
                        ssrcs_seen = ?stats.ssrcs_seen,
                        "tap leg finished"
                    );
                }
                info!(
                    releases = summary.releases,
                    reanchors = summary.reanchors,
                    elapsed_ms = summary.elapsed.as_millis() as u64,
                    published = hub.published(),
                    "tap capture finished"
                );
            })
            .map_err(|error| MediaPlaneError(format!("capture thread: {error}")))?;

        let speakers = if ssrc_tracks.is_empty() {
            None
        } else {
            Some(tokio::spawn(reresolve_speakers(
                Arc::clone(&transport),
                view.call_id.clone(),
                view.from_tags.clone(),
                shared_stats.clone(),
                ssrc_publishers,
                Arc::clone(&stop),
                self.metrics.clone(),
            )))
        };

        info!(
            session = %view.id,
            external_id = %view.external_id,
            call_id = %view.call_id,
            %node,
            streams = offer.streams.len(),
            ?receive_ports,
            "tapping a call for the control plane"
        );

        let mut held = self
            .sessions
            .lock()
            .map_err(|_| MediaPlaneError("the session table is poisoned".to_string()))?;
        held.insert(
            view.id,
            LiveSession {
                transport,
                external_id: view.external_id.clone(),
                call_id: view.call_id.clone(),
                to_tag,
                format,
                hub: hub_client,
                stop,
                capture: Some(capture_thread),
                speakers,
            },
        );
        drop(held);
        self.metrics.register_session(view.id, shared_stats);
        Ok(())
    }

    async fn close_session(&self, session: SessionId) -> Result<(), MediaPlaneError> {
        let live = {
            let mut held = self
                .sessions
                .lock()
                .map_err(|_| MediaPlaneError("the session table is poisoned".to_string()))?;
            held.remove(&session)
        };
        let mut live = match live {
            Some(live) => live,
            None => return Ok(()),
        };

        for (attachment, held) in self.take_attachments_of(session)? {
            match held {
                LiveAttachment::Ws { task, .. } => task.abort(),
                LiveAttachment::Grpc { live, .. } => {
                    if let Some(live) = live {
                        live.pump.abort();
                    }
                }
                recording @ LiveAttachment::Recording { .. } => {
                    self.finish_recording(attachment, recording).await;
                }
            }
            self.metrics.retire_consumer(attachment);
        }

        live.stop.store(true, Ordering::Relaxed);
        if let Some(speakers) = live.speakers.take() {
            speakers.abort();
        }
        if let Err(error) = live
            .transport
            .unsubscribe(&live.call_id, &live.to_tag)
            .await
        {
            warn!(
                %session,
                %error,
                "unsubscribe failed; rtpengine keeps the subscription until it times out"
            );
        }
        if let Some(thread) = live.capture.take() {
            let joined = tokio::task::spawn_blocking(move || thread.join()).await;
            if joined.is_err() {
                warn!(%session, "the capture thread did not join cleanly");
            }
        }
        self.metrics.retire_session(session);
        info!(%session, call_id = %live.call_id, "tap closed");
        Ok(())
    }

    async fn open_attachment(&self, view: AttachmentView) -> Result<(), MediaPlaneError> {
        match view.transport {
            Transport::WsTwilio => self.open_ws_attachment(view),
            Transport::GrpcStream => self.open_grpc_attachment(view),
            Transport::FileS3 => self.open_recording_attachment(view),
            other => Err(MediaPlaneError(format!(
                "{other} attachments are not served yet; ws-twilio, grpc-stream \
                 and file-s3 are"
            ))),
        }
    }

    async fn update_attachment(&self, view: AttachmentView) -> Result<(), MediaPlaneError> {
        let paused = {
            let held = self
                .attachments
                .lock()
                .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
            match held.get(&view.id) {
                Some(LiveAttachment::Recording {
                    handle: Some(handle),
                    ..
                }) => Some(handle.set_paused(view.paused)),
                Some(LiveAttachment::Recording { handle: None, .. }) => {
                    return Err(MediaPlaneError(format!(
                        "{} is a recording that has already been closed",
                        view.id
                    )))
                }
                Some(_) => None,
                None => {
                    return Err(MediaPlaneError(format!(
                        "{} is not connected here",
                        view.id
                    )))
                }
            }
        };
        match paused {
            Some(true) => Ok(()),
            Some(false) => Err(MediaPlaneError(format!(
                "{} is not taking commands any more",
                view.id
            ))),
            None => {
                info!(
                    attachment = %view.id,
                    paused = view.paused,
                    transport = %view.transport,
                    "pause is control-plane state for this transport; \
                     its media keeps flowing"
                );
                Ok(())
            }
        }
    }

    async fn close_attachment(
        &self,
        _session: SessionId,
        attachment: AttachmentId,
    ) -> Result<(), MediaPlaneError> {
        let live = {
            let mut held = self
                .attachments
                .lock()
                .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
            held.remove(&attachment)
        };
        match live {
            Some(LiveAttachment::Ws { session, task, .. }) => {
                task.abort();
                info!(%attachment, %session, "consumer detached");
            }
            Some(LiveAttachment::Grpc { session, live, .. }) => {
                if let Some(live) = live {
                    live.pump.abort();
                }
                info!(%attachment, %session, "grpc consumer detached");
            }
            Some(recording @ LiveAttachment::Recording { .. }) => {
                self.finish_recording(attachment, recording).await;
            }
            None => {}
        }
        self.metrics.retire_consumer(attachment);
        Ok(())
    }

    async fn send_text(
        &self,
        attachment: AttachmentId,
        json: String,
    ) -> Result<(), MediaPlaneError> {
        enum Outbound {
            Ws(mpsc::Sender<String>),
            Grpc(mpsc::Sender<StreamFrame>),
        }
        let sender = {
            let held = self
                .attachments
                .lock()
                .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
            match held.get(&attachment) {
                Some(LiveAttachment::Ws { text, .. }) => Outbound::Ws(text.clone()),
                Some(LiveAttachment::Grpc {
                    live: Some(live), ..
                }) => Outbound::Grpc(live.frames.clone()),
                Some(LiveAttachment::Grpc { live: None, .. }) => {
                    return Err(MediaPlaneError(format!(
                        "{attachment} has no connected grpc consumer to send to"
                    )))
                }
                Some(LiveAttachment::Recording { .. }) => {
                    return Err(MediaPlaneError(format!(
                        "{attachment} is a recording; a file sink has no back channel"
                    )))
                }
                None => {
                    return Err(MediaPlaneError(format!(
                        "{attachment} is not connected here"
                    )))
                }
            }
        };
        let delivered = match sender {
            Outbound::Ws(text) => text.send(json).await.is_ok(),
            Outbound::Grpc(frames) => frames.send(StreamFrame::Text { json }).await.is_ok(),
        };
        if delivered {
            Ok(())
        } else {
            Err(MediaPlaneError(format!("{attachment} has stopped reading")))
        }
    }

    async fn open_stream(
        &self,
        session: SessionId,
        attachment: AttachmentId,
    ) -> Result<mpsc::Receiver<StreamFrame>, MediaPlaneError> {
        let (selection, target_format) = {
            let held = self
                .attachments
                .lock()
                .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
            match held.get(&attachment) {
                Some(LiveAttachment::Grpc {
                    session: held_session,
                    selection,
                    format,
                    live,
                }) => {
                    if *held_session != session {
                        return Err(MediaPlaneError(format!(
                            "{attachment} belongs to another session"
                        )));
                    }
                    if live.as_ref().is_some_and(|live| !live.pump.is_finished()) {
                        return Err(MediaPlaneError(format!(
                            "{attachment} already has a connected consumer"
                        )));
                    }
                    (*selection, *format)
                }
                Some(LiveAttachment::Ws { .. }) => {
                    return Err(MediaPlaneError(format!(
                        "{attachment} is a websocket attachment; it has no grpc stream"
                    )))
                }
                Some(LiveAttachment::Recording { .. }) => {
                    return Err(MediaPlaneError(format!(
                        "{attachment} is a recording; it has no grpc stream"
                    )))
                }
                None => {
                    return Err(MediaPlaneError(format!(
                        "{attachment} is not attached here"
                    )))
                }
            }
        };

        let (_, _, hub, _, _) = self.session_handles(session)?;
        let subscription = hub
            .attach(CONSUMER_QUEUE_FRAMES, selection)
            .ok_or_else(|| {
                MediaPlaneError("the hub would not take another consumer".to_string())
            })?;
        let subscription_metrics = subscription.metrics();
        let (frames, receiver) = mpsc::channel(GRPC_FRAME_QUEUE);
        let pump = tokio::spawn(pump_frames(
            subscription,
            frames.clone(),
            self.session_format(session)?,
            target_format,
        ));

        let mut held = self
            .attachments
            .lock()
            .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
        match held.get_mut(&attachment) {
            Some(LiveAttachment::Grpc { live, .. }) => {
                *live = Some(GrpcLive { frames, pump });
            }
            _ => {
                pump.abort();
                return Err(MediaPlaneError(format!(
                    "{attachment} was detached while its consumer connected"
                )));
            }
        }
        drop(held);
        self.metrics
            .register_consumer(attachment, subscription_metrics);
        info!(%attachment, %session, "grpc consumer connected to its tap");
        Ok(receiver)
    }

    async fn start_playback(
        &self,
        session: SessionId,
        _playback: session_core::PlaybackId,
        source: PlaybackSource,
        target_tag: Option<String>,
        block_egress: bool,
    ) -> Result<(), MediaPlaneError> {
        let source = match source {
            PlaybackSource::File(path) => PlaySource::File(path),
            PlaybackSource::Blob(bytes) => {
                if bytes.len() > MAX_PLAYBACK_BLOB_BYTES {
                    return Err(MediaPlaneError(format!(
                        "a {} byte blob exceeds what one NG datagram carries; \
                         chunked playback is not implemented",
                        bytes.len()
                    )));
                }
                PlaySource::Blob(bytes)
            }
            PlaybackSource::Stream => {
                return Err(MediaPlaneError(
                    "streaming playback needs the phase-3 inline leg".to_string(),
                ))
            }
        };
        let (transport, call_id, _, _, _) = self.session_handles(session)?;
        transport
            .play_media(&PlayMedia {
                call_id,
                target: target_of(target_tag),
                source,
                repeat_times: None,
                block_egress,
            })
            .await
            .map_err(|error| MediaPlaneError(format!("play media: {error}")))?;
        Ok(())
    }

    async fn stop_playback(
        &self,
        session: SessionId,
        _playback: session_core::PlaybackId,
    ) -> Result<(), MediaPlaneError> {
        let (transport, call_id, _, _, _) = self.session_handles(session)?;
        transport
            .stop_media(&call_id, &PlayTarget::HeardByEveryone)
            .await
            .map_err(|error| MediaPlaneError(format!("stop media: {error}")))?;
        Ok(())
    }
}

async fn pump_frames(
    mut subscription: Subscription,
    frames: mpsc::Sender<StreamFrame>,
    source: AudioFormat,
    target: AudioFormat,
) {
    let mut encoders: HashMap<Track, ConsumerEncoder> = HashMap::new();
    while let Some(event) = subscription.next().await {
        let frame = match event {
            TapEvent::Media {
                track,
                timestamp_ms,
                len,
                samples,
            } => {
                let encoder = match encoders.entry(track) {
                    std::collections::hash_map::Entry::Occupied(held) => held.into_mut(),
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        match ConsumerEncoder::new(source, target) {
                            Ok(encoder) => slot.insert(encoder),
                            Err(error) => {
                                warn!(%error, "this stream cannot encode; closing it");
                                return;
                            }
                        }
                    }
                };
                match encoder.encode(&samples[..len]) {
                    Ok(payload) => StreamFrame::Media {
                        track: control_api::convert::track_name(track),
                        pts_ms: timestamp_ms,
                        payload: payload.to_vec(),
                    },
                    Err(error) => {
                        warn!(%error, "a frame did not encode; closing the stream");
                        return;
                    }
                }
            }
            TapEvent::Dtmf { track, digit } => StreamFrame::Dtmf {
                track: control_api::convert::track_name(track),
                digit,
            },
        };
        if frames.send(frame).await.is_err() {
            return;
        }
    }
}

fn offered_tap_format(
    offer: &SubscriptionOffer,
    ptime_fallback_ms: u32,
) -> Result<AudioFormat, MediaPlaneError> {
    let mut settled: Option<AudioFormat> = None;
    for (index, stream) in offer.streams.iter().enumerate() {
        let format = stream.offered_format(ptime_fallback_ms).map_err(|error| {
            MediaPlaneError(format!(
                "stream {index}: {error}; this call needs transcoding at the tap"
            ))
        })?;
        match settled {
            None => settled = Some(format),
            Some(first) if first != format => {
                return Err(MediaPlaneError(format!(
                    "rtpengine offered {:?} on one stream and {:?} on another; \
                     a tap decodes one format for every leg, so this call needs \
                     transcoding at the tap",
                    first.encoding, format.encoding
                )))
            }
            Some(_) => {}
        }
    }
    settled.ok_or_else(|| MediaPlaneError("rtpengine offered no streams".to_string()))
}

fn telcompat_caller_tag_key() -> &'static str {
    control_api::telcompat::CALLER_TAG_KEY
}

fn target_of(target_tag: Option<String>) -> PlayTarget {
    match target_tag {
        Some(tag) => PlayTarget::HeardBy(tag),
        None => PlayTarget::HeardByEveryone,
    }
}

fn selection_of(selector: TrackSelector) -> TrackSelection {
    match selector {
        TrackSelector::All => TrackSelection::All,
        TrackSelector::Only(track) => TrackSelection::Only(track),
    }
}

fn layout_of(selector: TrackSelector) -> Layout {
    match selector {
        TrackSelector::All => Layout::Stereo,
        TrackSelector::Only(track) => Layout::Mono(track),
    }
}

fn tracks_of(selector: TrackSelector) -> Vec<String> {
    match selector {
        TrackSelector::All => vec![
            consumer_ws::track_name(Track::Customer).to_string(),
            consumer_ws::track_name(Track::Agent).to_string(),
        ],
        TrackSelector::Only(track) => vec![consumer_ws::track_name(track).to_string()],
    }
}

async fn speaker_ssrcs(
    transport: &NgTransport,
    call_id: &str,
    from_tags: &[String],
) -> Vec<(u32, Track)> {
    let reply = match transport.query(call_id).await {
        Ok(reply) => reply,
        Err(error) => {
            warn!(
                %call_id,
                %error,
                "query failed; leg identity falls back to stream order"
            );
            return Vec::new();
        }
    };
    let mut ssrc_tracks = Vec::new();
    for (tag, ssrc) in reply.ssrc_by_tag() {
        let Some(position) = from_tags.iter().position(|held| *held == tag) else {
            continue;
        };
        let speaker = speaker_track(position);
        info!(
            %call_id,
            %tag,
            ssrc,
            ?speaker,
            "the leg carrying this ssrc is this participant's own voice"
        );
        ssrc_tracks.push((ssrc, speaker));
    }
    if ssrc_tracks.is_empty() {
        warn!(
            %call_id,
            "rtpengine reported no ssrcs for the requested tags; \
             leg identity falls back to stream order"
        );
    }
    if ssrc_tracks.len() > MAX_SSRC_TRACKS {
        warn!(
            %call_id,
            reported = ssrc_tracks.len(),
            kept = MAX_SSRC_TRACKS,
            "rtpengine named more speaker ssrcs than a leg map holds; \
             the extras are dropped"
        );
    }
    ssrc_tracks
}

#[derive(Default)]
struct SsrcRequeries {
    asked_about: Vec<u32>,
}

impl SsrcRequeries {
    fn note(&mut self, unresolved: &[u32]) -> bool {
        let mut worth_asking = false;
        for ssrc in unresolved {
            if self.asked_about.contains(ssrc) {
                continue;
            }
            if self.asked_about.len() == MAX_REQUERIED_SSRCS {
                self.asked_about.remove(0);
            }
            self.asked_about.push(*ssrc);
            worth_asking = true;
        }
        worth_asking
    }
}

async fn reresolve_speakers(
    transport: Arc<NgTransport>,
    call_id: String,
    from_tags: Vec<String>,
    legs: Vec<Arc<SharedLegStats>>,
    publishers: Vec<SsrcTrackPublisher>,
    stop: Arc<AtomicBool>,
    metrics: TapPlaneMetrics,
) {
    let mut requeries = SsrcRequeries::default();
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(SSRC_WATCH_INTERVAL).await;
        let unresolved: Vec<u32> = legs
            .iter()
            .filter_map(|leg| leg.unresolved_ssrc())
            .collect();
        if !requeries.note(&unresolved) {
            continue;
        }
        metrics.record_ssrc_requery();
        let pairs = speaker_ssrcs(&transport, &call_id, &from_tags).await;
        if pairs.is_empty() {
            warn!(
                %call_id,
                ?unresolved,
                "a leg started carrying an ssrc nobody claims and rtpengine named none; \
                 the leg keeps the name it has"
            );
            continue;
        }
        let tracks = SsrcTracks::from_pairs(&pairs);
        let unread = publishers
            .iter()
            .filter(|publisher| !publisher.publish(tracks))
            .count();
        info!(
            %call_id,
            ?unresolved,
            speakers = tracks.len(),
            legs = publishers.len(),
            superseded = unread,
            "re-resolved the speaker map after a mid-call ssrc change"
        );
    }
}

fn speaker_track(index: usize) -> Track {
    if index == 0 {
        Track::Customer
    } else {
        Track::Agent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use media_core::AudioFormat;
    use session_core::{Capabilities, PlaybackId};
    use std::collections::BTreeMap;

    struct NowhereSink;

    #[control_api::async_trait]
    impl recorder::RecordingSink for NowhereSink {
        async fn put(
            &self,
            _key: &str,
            _content_type: &'static str,
            _body: Vec<u8>,
        ) -> Result<String, recorder::UploadError> {
            Err(recorder::UploadError::Refused(
                "this sink exists only to prove the attachment path".to_string(),
            ))
        }

        fn describe(&self) -> String {
            "nowhere".to_string()
        }
    }

    fn plane() -> TapPlane {
        plane_with_recording(RecordingSupport::default())
    }

    fn plane_with_recording(recording: RecordingSupport) -> TapPlane {
        TapPlane::new(TapPlaneConfig {
            default_node: None,
            local_media_address: IpAddr::from([127, 0, 0, 1]),
            format: AudioFormat::pcmu_8k_20ms(),
            transcode_at_tap: true,
            cookie_prefix: 1,
            sdp_session_id: 1,
            recording,
        })
    }

    fn session(kind: SessionKind, node: &str) -> SessionView {
        SessionView {
            id: SessionId::from_raw(1),
            external_id: "req-1".to_string(),
            kind,
            call_id: "call-abc".to_string(),
            from_tags: vec!["from-a".to_string()],
            rtpengine_node: node.to_string(),
            attachments: Vec::new(),
            authoritative: None,
        }
    }

    fn attachment(transport: Transport, endpoint: &str) -> AttachmentView {
        AttachmentView {
            id: AttachmentId::from_raw(2),
            session: SessionId::from_raw(1),
            transport,
            capabilities: Capabilities::SINK,
            selector: TrackSelector::All,
            format: AudioFormat::pcmu_8k_20ms(),
            authoritative: false,
            paused: false,
            label: "consumer".to_string(),
            endpoint: endpoint.to_string(),
            metadata: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn a_session_that_names_no_node_and_has_no_default_is_refused() {
        let plane = plane();
        let error = plane
            .open_session(session(SessionKind::Tap, ""))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no default"));
        assert_eq!(plane.live_sessions(), 0);
    }

    #[tokio::test]
    async fn a_malformed_node_address_is_refused_before_any_socket_is_opened() {
        let plane = plane();
        let error = plane
            .open_session(session(SessionKind::Tap, "not-an-address"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ip:port"));
    }

    #[tokio::test]
    async fn an_inline_session_is_refused_because_phase_3_has_not_landed() {
        let plane = plane();
        let error = plane
            .open_session(session(SessionKind::Inline, "127.0.0.1:22222"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("phase 3"));
    }

    #[tokio::test]
    async fn transports_without_a_bridge_are_refused_by_name() {
        let plane = plane();
        let error = plane
            .open_attachment(attachment(Transport::RtpInline, ""))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not served yet"));
        assert!(error.to_string().contains("rtp-inline"));
    }

    #[tokio::test]
    async fn a_recording_endpoint_that_is_not_the_frozen_identity_is_refused_before_anything_opens()
    {
        let plane = plane();
        for endpoint in [
            "",
            "rec-99.wav",
            "acct/deeper/rec-99.wav",
            "acct/rec-99.mp3",
        ] {
            let error = plane
                .open_attachment(attachment(Transport::FileS3, endpoint))
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("recording identity"),
                "{endpoint} was refused with {error}"
            );
        }
    }

    #[tokio::test]
    async fn a_recording_is_refused_when_this_pod_has_nowhere_to_upload_it() {
        let plane = plane();
        let error = plane
            .open_attachment(attachment(Transport::FileS3, "acct-42/rec-99.wav"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no recording storage"));
    }

    #[tokio::test]
    async fn a_configured_recording_still_needs_a_session_this_pod_taps() {
        let plane = plane_with_recording(RecordingSupport {
            sink: Some(Arc::new(NowhereSink)),
            spill_dir: None,
            counters: Arc::new(RecorderCounters::default()),
        });
        let error = plane
            .open_attachment(attachment(Transport::FileS3, "acct-42/rec-99.wav"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not tapped here"));
    }

    #[tokio::test]
    async fn pause_is_refused_for_an_attachment_this_pod_never_opened() {
        let plane = plane();
        let mut view = attachment(Transport::FileS3, "acct-42/rec-99.wav");
        view.paused = true;
        let error = plane.update_attachment(view).await.unwrap_err();
        assert!(error.to_string().contains("not connected here"));
    }

    #[test]
    fn a_selector_that_names_one_track_records_it_alone_in_mono() {
        assert_eq!(layout_of(TrackSelector::All), Layout::Stereo);
        assert_eq!(
            layout_of(TrackSelector::Only(Track::Agent)),
            Layout::Mono(Track::Agent)
        );
    }

    #[tokio::test]
    async fn a_grpc_attachment_needs_a_session_this_pod_taps() {
        let plane = plane();
        let error = plane
            .open_attachment(attachment(Transport::GrpcStream, ""))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not tapped here"));
    }

    #[tokio::test]
    async fn a_grpc_attachment_may_ask_for_l16_16k_but_not_opus() {
        let plane = plane();

        let mut l16 = attachment(Transport::GrpcStream, "");
        l16.format = AudioFormat::l16_16k_20ms();
        let error = plane.open_attachment(l16).await.unwrap_err();
        assert!(error.to_string().contains("not tapped here"));

        let mut opus = attachment(Transport::GrpcStream, "");
        opus.format = AudioFormat {
            encoding: media_core::Encoding::Opus,
            sample_rate_hz: 48000,
            channels: 1,
            ptime_ms: 20,
        };
        let error = plane.open_attachment(opus).await.unwrap_err();
        assert!(error.to_string().contains("not tapped here"));
    }

    #[test]
    fn a_consumer_format_is_judged_against_the_tap_that_exists_not_a_configured_guess() {
        let tap = AudioFormat {
            encoding: media_core::Encoding::Pcma,
            ..AudioFormat::pcmu_8k_20ms()
        };
        assert!(ConsumerEncoder::supports(tap, AudioFormat::pcmu_8k_20ms()));
        assert!(ConsumerEncoder::supports(tap, AudioFormat::l16_16k_20ms()));
        assert!(!ConsumerEncoder::supports(
            tap,
            AudioFormat {
                encoding: media_core::Encoding::Opus,
                sample_rate_hz: 48000,
                channels: 1,
                ptime_ms: 20,
            }
        ));
    }

    fn offer_of(payload_types: &str, rtpmaps: &[&str]) -> SubscriptionOffer {
        let mut sdp = String::from(
            "v=0\r\no=- 1 1 IN IP4 10.0.0.5\r\ns=rtpengine\r\nc=IN IP4 10.0.0.5\r\nt=0 0\r\n",
        );
        sdp.push_str(&format!("m=audio 30000 RTP/AVP {payload_types}\r\n"));
        for rtpmap in rtpmaps {
            sdp.push_str(&format!("a=rtpmap:{rtpmap}\r\n"));
        }
        sdp.push_str("a=ptime:20\r\na=sendonly\r\n");
        SubscriptionOffer::parse(&sdp).unwrap()
    }

    #[test]
    fn an_untranscoded_tap_takes_the_codec_the_call_is_already_using() {
        let offer = offer_of("8 101", &["8 PCMA/8000", "101 telephone-event/8000"]);
        let format = offered_tap_format(&offer, 20).unwrap();
        assert_eq!(format.encoding, media_core::Encoding::Pcma);
        assert_eq!(format.sample_rate_hz, 8000);
        assert_eq!(format.ptime_ms, 20);
    }

    #[test]
    fn a_call_whose_codec_this_pipeline_cannot_decode_names_transcoding_as_the_fix() {
        let offer = offer_of("111 101", &["111 opus/48000", "101 telephone-event/8000"]);
        let error = offered_tap_format(&offer, 20).unwrap_err();
        assert!(
            error.to_string().contains("needs transcoding at the tap"),
            "{error}"
        );
        assert!(error.to_string().contains("111"), "{error}");
    }

    #[test]
    fn legs_offered_with_different_codecs_are_refused_rather_than_half_decoded() {
        let mut offer = offer_of("0 101", &["0 PCMU/8000", "101 telephone-event/8000"]);
        let alaw = offer_of("8 101", &["8 PCMA/8000", "101 telephone-event/8000"]);
        offer.streams.push(alaw.streams[0].clone());

        let error = offered_tap_format(&offer, 20).unwrap_err();
        assert!(
            error.to_string().contains("one format for every leg"),
            "{error}"
        );
    }

    #[test]
    fn two_legs_agreeing_on_one_codec_settle_on_it() {
        let mut offer = offer_of("8 101", &["8 PCMA/8000", "101 telephone-event/8000"]);
        let second = offer.streams[0].clone();
        offer.streams.push(second);
        assert_eq!(
            offered_tap_format(&offer, 20).unwrap().encoding,
            media_core::Encoding::Pcma
        );
    }

    #[tokio::test]
    async fn the_ws_dialect_stays_frozen_at_pcmu() {
        let plane = plane();
        let mut wrong = attachment(Transport::WsTwilio, "ws-endpoint");
        wrong.format = AudioFormat::l16_16k_20ms();
        let error = plane.open_attachment(wrong).await.unwrap_err();
        assert!(error.to_string().contains("frozen at PCMU"));
    }

    #[tokio::test]
    async fn the_pump_encodes_each_track_into_the_consumers_format() {
        let (mut hub, client) = crate::hub::Hub::new();
        let subscription = client.attach(8, TrackSelection::All).unwrap();
        hub.poll_commands();

        let (frames, mut receiver) = mpsc::channel(8);
        let pump = tokio::spawn(pump_frames(
            subscription,
            frames,
            AudioFormat::pcmu_8k_20ms(),
            AudioFormat::l16_16k_20ms(),
        ));

        hub.publish(crate::hub::TapEvent::media(
            Track::Customer,
            20,
            &[100i16; 160],
        ));
        hub.publish(crate::hub::TapEvent::Dtmf {
            track: Track::Agent,
            digit: '4',
        });
        drop(hub);

        match receiver.recv().await.unwrap() {
            StreamFrame::Media {
                track,
                pts_ms,
                payload,
            } => {
                assert_eq!(track, "customer");
                assert_eq!(pts_ms, 20);
                assert_eq!(payload.len(), 640);
            }
            other => panic!("expected an encoded media frame, got {other:?}"),
        }
        match receiver.recv().await.unwrap() {
            StreamFrame::Dtmf { track, digit } => {
                assert_eq!(track, "agent");
                assert_eq!(digit, '4');
            }
            other => panic!("expected the dtmf frame, got {other:?}"),
        }
        assert!(receiver.recv().await.is_none());
        pump.await.unwrap();
    }

    #[tokio::test]
    async fn a_stream_can_only_open_against_a_grpc_attachment_this_pod_holds() {
        let plane = plane();
        let error = plane
            .open_stream(SessionId::from_raw(1), AttachmentId::from_raw(2))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not attached here"));
    }

    #[tokio::test]
    async fn a_websocket_attachment_without_an_endpoint_is_refused() {
        let plane = plane();
        let error = plane
            .open_attachment(attachment(Transport::WsTwilio, ""))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("endpoint"));
    }

    #[tokio::test]
    async fn work_against_a_session_this_pod_does_not_hold_is_refused() {
        let plane = plane();
        let error = plane
            .open_attachment(attachment(Transport::WsTwilio, "wss-endpoint"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not tapped here"));

        let error = plane
            .send_text(AttachmentId::from_raw(9), "{}".to_string())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not connected here"));
    }

    #[tokio::test]
    async fn a_blob_too_large_for_one_datagram_is_refused_rather_than_truncated() {
        let plane = plane();
        let error = plane
            .start_playback(
                SessionId::from_raw(1),
                PlaybackId::from_raw(3),
                PlaybackSource::Blob(vec![0u8; MAX_PLAYBACK_BLOB_BYTES + 1]),
                None,
                false,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("chunked playback"));
    }

    #[tokio::test]
    async fn streaming_playback_is_named_as_phase_3_work_rather_than_failing_obscurely() {
        let plane = plane();
        let error = plane
            .start_playback(
                SessionId::from_raw(1),
                PlaybackId::from_raw(3),
                PlaybackSource::Stream,
                None,
                false,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("inline leg"));
    }

    #[tokio::test]
    async fn closing_something_this_pod_never_held_is_not_an_error() {
        let plane = plane();
        assert!(plane.close_session(SessionId::from_raw(7)).await.is_ok());
        assert!(plane
            .close_attachment(SessionId::from_raw(7), AttachmentId::from_raw(8))
            .await
            .is_ok());
    }

    #[test]
    fn a_selector_becomes_the_hub_selection_and_the_track_list_it_implies() {
        assert_eq!(selection_of(TrackSelector::All), TrackSelection::All);
        assert_eq!(
            selection_of(TrackSelector::Only(Track::Agent)),
            TrackSelection::Only(Track::Agent)
        );
        assert_eq!(tracks_of(TrackSelector::All), vec!["inbound", "outbound"]);
        assert_eq!(
            tracks_of(TrackSelector::Only(Track::Customer)),
            vec!["inbound"]
        );
    }
}

#[cfg(test)]
mod leg_naming_tests {
    use super::*;

    #[test]
    fn a_leg_carries_its_own_participants_voice_first_tag_is_the_customer() {
        assert_eq!(speaker_track(0), Track::Customer);
        assert_eq!(speaker_track(1), Track::Agent);
    }

    #[test]
    fn one_unknown_ssrc_costs_one_query_however_long_it_lingers() {
        let mut requeries = SsrcRequeries::default();
        assert!(!requeries.note(&[]));
        assert!(requeries.note(&[7]));
        assert!(!requeries.note(&[7]));
        assert!(!requeries.note(&[]));
        assert!(requeries.note(&[7, 9]));
        assert!(!requeries.note(&[9, 7]));
    }

    #[test]
    fn the_requery_memory_is_bounded_and_forgets_its_oldest_ssrc() {
        let mut requeries = SsrcRequeries::default();
        for ssrc in 0..MAX_REQUERIED_SSRCS as u32 {
            assert!(requeries.note(&[ssrc]));
        }
        assert!(!requeries.note(&[0]));
        assert!(requeries.note(&[u32::MAX]));
        assert!(requeries.note(&[0]));
    }
}
