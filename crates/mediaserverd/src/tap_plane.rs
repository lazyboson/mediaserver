use crate::conference::{Conference, ConferenceMember, ConferenceShared, ConferenceTotals};
use crate::consumer_ws::{self, ConsumerConfig};
use crate::digits::DigitQueue;
use crate::discovery::NodeDiscovery;
use crate::hub::{
    Hub, HubClient, Subscription, SubscriptionControl, SubscriptionMetrics, TapEvent,
    TrackSelection, CONSUMER_QUEUE_FRAMES,
};
use crate::inline_leg::{
    egress_ssrc, InlineEgress, InlineEgressHandle, InlineEgressShared, InlineEgressTotals,
    EGRESS_CHUNK_MS,
};
use crate::media_ports::{MediaPortAllocator, PortLease};
use crate::ng_transport::{NgTransport, NgTransportConfig};
use crate::recorder::{
    self, Layout, RecorderCounters, RecorderHandle, RecorderSpec, RecordingFormat,
    RecordingIdentity, RecordingProgress, RecordingSupport, RecordingTarget,
};
use crate::recording_uploads::UploadTracker;
use crate::registry_keeper::TapSubscriptions;
use crate::rtpengine_capability::NodeCapabilityLog;
use crate::session_store::PersistedRecording;
use crate::tap_spike::{
    capture, capture_with_egress, SharedLegStats, SsrcTrackPublisher, SsrcTracks, TapLeg,
    MAX_SSRC_TRACKS,
};
use control_api::{
    InlineEgressSink, MediaPlane, MediaPlaneError, ObservationSink, OpenedSession, PlaybackSource,
    StreamFrame,
};
use media_core::pipeline::PipelineConfig;
use media_core::{AudioFormat, ConsumerEncoder, Encoding, Track};
use rtpengine_ng::{
    InlineOffer, NegotiatedCodec, PlayMedia, PlaySource, PlayTarget, SdpError, SubscribeRequest,
    SubscriptionAnswer, SubscriptionOffer,
};
use session_core::mix::{
    MemberControl, MemberStateView, MixRoute, MIX_TARGET_EVERYONE, MIX_TARGET_OWN,
};
use session_core::{
    AttachmentId, AttachmentView, Attribution, Capabilities, Observation, SessionId, SessionKind,
    SessionView, TrackSelector, Transport,
};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
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
const TEXT_QUEUE_DEPTH: usize = 32;
const GRPC_FRAME_QUEUE: usize = 64;
const RETAIN_NO_LOCAL_AUDIO: Duration = Duration::ZERO;
const SSRC_WATCH_INTERVAL: Duration = Duration::from_millis(500);
const MAX_REQUERIED_SSRCS: usize = 16;
const ACCOUNT_METADATA_KEY: &str = "accountId";
const STREAM_SID_METADATA_KEY: &str = "streamSid";
const DEFAULT_ACCOUNT_ID: &str = "mss";
const POLITE_CLOSE: Duration = Duration::from_secs(2);

pub struct TapPlaneConfig {
    pub default_node: Option<SocketAddr>,
    pub local_media_address: IpAddr,
    pub advertised_media_address: IpAddr,
    pub media_ports: Arc<MediaPortAllocator>,
    pub format: AudioFormat,
    pub transcode_at_tap: bool,
    pub opus_decode_rate_hz: u32,
    pub cookie_prefix: u64,
    pub sdp_session_id: u64,
    pub recording: RecordingSupport,
    pub capabilities: Arc<NodeCapabilityLog>,
}

struct ResolvedNode {
    node: SocketAddr,
    view: SessionView,
    caller_named: bool,
}

struct SessionHandles {
    transport: Option<Arc<NgTransport>>,
    call_id: String,
    hub: HubClient,
    external_id: String,
    attribution: Attribution,
}

struct LiveSession {
    kind: SessionKind,
    attribution: Attribution,
    media_ports: Vec<PortLease>,
    transport: Option<Arc<NgTransport>>,
    external_id: String,
    call_id: String,
    to_tag: String,
    format: AudioFormat,
    hub: HubClient,
    egress: Option<InlineEgressHandle>,
    stop: Arc<AtomicBool>,
    capture: Option<std::thread::JoinHandle<()>>,
    conference: Option<String>,
    mix_route: Option<AttachmentId>,
    speakers: Option<tokio::task::JoinHandle<()>>,
    digits: Arc<DigitQueue>,
}

enum LiveAttachment {
    Ws {
        session: SessionId,
        text: mpsc::Sender<String>,
        media: SubscriptionControl,
        task:
            tokio::task::JoinHandle<Result<consumer_ws::ConsumerStats, consumer_ws::ConsumerError>>,
    },
    Grpc {
        session: SessionId,
        selection: TrackSelection,
        format: AudioFormat,
        paused: bool,
        live: Option<GrpcLive>,
    },
    Recording {
        session: SessionId,
        recording_id: String,
        member_of: Option<GroupKey>,
        handle: Option<RecorderHandle>,
        progress: Arc<RecordingProgress>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey {
    account_id: String,
    group: String,
}

impl std::fmt::Display for GroupKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}", self.account_id, self.group)
    }
}

struct RecordingGroup {
    recording_id: String,
    format: RecordingFormat,
    opened_at: Instant,
    members: HashMap<AttachmentId, Vec<String>>,
}

impl RecordingGroup {
    fn holds(&self, participant: &str) -> bool {
        self.members
            .values()
            .any(|held| held.iter().any(|name| name == participant))
    }
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
    media: SubscriptionControl,
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
    pub dtmf_events_dropped: u64,
    pub consumers_live: u64,
    pub consumer_dropped_oldest: u64,
    pub consumer_delivered: u64,
    pub consumer_suppressed_while_paused: u64,
    pub consumer_queue_depth: u64,
    pub consumer_queue_depth_max: u64,
    pub recordings_live: u64,
    pub recordings_started: u64,
    pub recordings_stopped: u64,
    pub recording_pauses: u64,
    pub recording_uploads: u64,
    pub recording_upload_failures: u64,
    pub recording_uploads_in_flight: u64,
    pub recording_uploads_backgrounded: u64,
    pub recording_upload_settle_timeouts: u64,
    pub recording_spills: u64,
    pub recording_segments_spilled: u64,
    pub recording_segment_spill_failures: u64,
    pub recording_spill_lost_ownership: u64,
    pub recording_spill_foreign_manifests: u64,
    pub recording_salvaged: u64,
    pub recording_salvage_skipped: u64,
    pub recording_salvage_failures: u64,
    pub recording_frames_lost_on_adopt: u64,
    pub recording_bytes_uploaded: u64,
    pub recording_seconds: u64,
    pub recordings_truncated: u64,
    pub recording_groups_live: u64,
    pub recording_group_members_live: u64,
    pub recording_group_joins_refused: u64,
    pub inline_legs_live: u64,
    pub inline: InlineEgressTotals,
    pub conferences_live: u64,
    pub conference_members_live: u64,
    pub conference_whispers_live: u64,
    pub conference_members_muted: u64,
    pub conference_members_deaf: u64,
    pub conference_members_held: u64,
    pub conference: ConferenceTotals,
}

#[derive(Default)]
struct MetricsInner {
    retired: LegTotals,
    retired_consumer_dropped: u64,
    retired_consumer_delivered: u64,
    retired_consumer_suppressed: u64,
    ssrc_requeries: u64,
    dtmf_events_dropped: u64,
    legs: HashMap<SessionId, Vec<Arc<SharedLegStats>>>,
    inline: HashMap<SessionId, Arc<InlineEgressShared>>,
    retired_inline: InlineEgressTotals,
    conferences: HashMap<String, Arc<ConferenceShared>>,
    retired_conferences: ConferenceTotals,
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

    fn register_inline(&self, session: SessionId, egress: Arc<InlineEgressShared>) {
        self.lock().inline.insert(session, egress);
    }

    fn register_conference(&self, name: &str, shared: Arc<ConferenceShared>) {
        self.lock().conferences.insert(name.to_string(), shared);
    }

    fn retire_conference(&self, name: &str) {
        let mut inner = self.lock();
        if let Some(shared) = inner.conferences.remove(name) {
            inner.retired_conferences.add_shared(&shared);
        }
    }

    fn retire_session(&self, session: SessionId) {
        let mut inner = self.lock();
        if let Some(egress) = inner.inline.remove(&session) {
            inner.retired_inline.add_shared(&egress);
        }
        if let Some(legs) = inner.legs.remove(&session) {
            for leg in legs {
                inner.retired.add_shared(&leg);
            }
        }
    }

    fn record_ssrc_requery(&self) {
        self.lock().ssrc_requeries += 1;
    }

    fn record_digits_dropped(&self, dropped: u64) {
        self.lock().dtmf_events_dropped += dropped;
    }

    fn register_consumer(&self, attachment: AttachmentId, metrics: SubscriptionMetrics) {
        let mut inner = self.lock();
        if let Some(replaced) = inner.consumers.insert(attachment, metrics) {
            inner.retired_consumer_dropped += replaced.dropped_oldest();
            inner.retired_consumer_delivered += replaced.delivered();
            inner.retired_consumer_suppressed += replaced.suppressed_while_paused();
        }
    }

    fn retire_consumer(&self, attachment: AttachmentId) {
        let mut inner = self.lock();
        if let Some(removed) = inner.consumers.remove(&attachment) {
            inner.retired_consumer_dropped += removed.dropped_oldest();
            inner.retired_consumer_delivered += removed.delivered();
            inner.retired_consumer_suppressed += removed.suppressed_while_paused();
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
            consumer_suppressed_while_paused: inner.retired_consumer_suppressed,
            ssrc_requeries: inner.ssrc_requeries,
            dtmf_events_dropped: inner.dtmf_events_dropped,
            recordings_live: read(&recorder.live),
            recordings_started: read(&recorder.started),
            recordings_stopped: read(&recorder.stopped),
            recording_pauses: read(&recorder.pauses),
            recording_uploads: read(&recorder.uploaded),
            recording_upload_failures: read(&recorder.upload_failures),
            recording_uploads_in_flight: read(&recorder.uploads_in_flight),
            recording_uploads_backgrounded: read(&recorder.uploads_backgrounded),
            recording_upload_settle_timeouts: read(&recorder.upload_settle_timeouts),
            recording_spills: read(&recorder.spilled),
            recording_segments_spilled: read(&recorder.segments_spilled),
            recording_segment_spill_failures: read(&recorder.segment_spill_failures),
            recording_spill_lost_ownership: read(&recorder.spill_lost_ownership),
            recording_spill_foreign_manifests: read(&recorder.spill_foreign_manifests),
            recording_salvaged: read(&recorder.salvaged),
            recording_salvage_skipped: read(&recorder.salvage_skipped),
            recording_salvage_failures: read(&recorder.salvage_failures),
            recording_frames_lost_on_adopt: read(&recorder.frames_lost_on_adopt),
            recording_bytes_uploaded: read(&recorder.bytes_uploaded),
            recording_seconds: read(&recorder.seconds_recorded),
            recordings_truncated: read(&recorder.truncated),
            recording_groups_live: read(&recorder.groups_live),
            recording_group_members_live: read(&recorder.group_members_live),
            recording_group_joins_refused: read(&recorder.group_joins_refused),
            inline_legs_live: inner.inline.len() as u64,
            inline: inner.retired_inline,
            conferences_live: inner.conferences.len() as u64,
            conference: inner.retired_conferences,
            ..IngestSnapshot::default()
        };
        for egress in inner.inline.values() {
            snapshot.inline.add_shared(egress);
        }
        for conference in inner.conferences.values() {
            snapshot.conference.add_shared(conference);
            snapshot.conference_members_live += conference.members_live.load(Ordering::Relaxed);
            snapshot.conference_whispers_live += conference.whispers_live.load(Ordering::Relaxed);
            snapshot.conference_members_muted += conference.members_muted.load(Ordering::Relaxed);
            snapshot.conference_members_deaf += conference.members_deaf.load(Ordering::Relaxed);
            snapshot.conference_members_held += conference.members_held.load(Ordering::Relaxed);
        }
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
            snapshot.consumer_suppressed_while_paused += consumer.suppressed_while_paused();
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
    groups: Mutex<HashMap<GroupKey, RecordingGroup>>,
    conferences: Mutex<HashMap<String, Conference>>,
    metrics: TapPlaneMetrics,
    observations: OnceLock<Weak<dyn ObservationSink>>,
    discovery: OnceLock<Arc<NodeDiscovery>>,
    uploads: Arc<UploadTracker>,
}

impl TapPlane {
    pub fn new(config: TapPlaneConfig) -> Self {
        let metrics = TapPlaneMetrics::with_recorder(Arc::clone(&config.recording.counters));
        let uploads = UploadTracker::new(Arc::clone(&config.recording.counters));
        TapPlane {
            config,
            sessions: Mutex::new(HashMap::new()),
            attachments: Mutex::new(HashMap::new()),
            groups: Mutex::new(HashMap::new()),
            conferences: Mutex::new(HashMap::new()),
            metrics,
            observations: OnceLock::new(),
            discovery: OnceLock::new(),
            uploads,
        }
    }

    pub fn uploads_in_flight(&self) -> u64 {
        self.uploads.in_flight()
    }

    pub async fn await_uploads(&self) -> usize {
        self.uploads.wait_idle().await as usize
    }

    pub fn observe_through(&self, sink: Weak<dyn ObservationSink>) {
        if self.observations.set(sink).is_err() {
            warn!("this tap plane already reports its observations somewhere");
        }
    }

    pub fn discover_through(&self, discovery: Arc<NodeDiscovery>) {
        let prefix = discovery.prefix().to_string();
        if self.discovery.set(discovery).is_err() {
            warn!("this tap plane already resolves rtpengine nodes through a discovery map");
            return;
        }
        info!(
            prefix = %prefix,
            "a call whose CreateSession names no rtpengine node is looked up in the \
             discovery map under this key prefix"
        );
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

    async fn resolve_node(&self, mut view: SessionView) -> Result<ResolvedNode, MediaPlaneError> {
        let mut caller_named = !view.from_tags.is_empty();
        if !view.rtpengine_node.is_empty() {
            let node = self.node_for(&view)?;
            return Ok(ResolvedNode {
                node,
                view,
                caller_named,
            });
        }
        let discovered = match self.discovery.get() {
            Some(discovery) => discovery.resolve(&view.call_id).await,
            None => None,
        };
        let Some(discovered) = discovered else {
            let node = self.node_for(&view)?;
            return Ok(ResolvedNode {
                node,
                view,
                caller_named,
            });
        };
        if view.from_tags.is_empty() && !discovered.from_tags.is_empty() {
            view.from_tags = discovered.from_tags;
            view.from_tags.truncate(MAX_TAPPED_LEGS);
            caller_named = discovered.caller_named;
        }
        Ok(ResolvedNode {
            node: discovered.node,
            view,
            caller_named,
        })
    }

    async fn complete_from_tags(
        &self,
        transport: &NgTransport,
        mut view: SessionView,
        caller_named: bool,
    ) -> Result<SessionView, MediaPlaneError> {
        let seeded = !view.from_tags.is_empty();
        if view.from_tags.len() >= MAX_TAPPED_LEGS {
            view.attribution = if caller_named {
                Attribution::Explicit
            } else {
                Attribution::Unknown
            };
            if !caller_named {
                warn!(
                    call_id = %view.call_id,
                    from_tags = ?view.from_tags,
                    "both legs of this call are known but not which one called: this tap \
                     will not claim a direction and its tracks are leg_a and leg_b. Mark the \
                     caller in the discovery map to get customer and agent"
                );
            }
            return Ok(view);
        }
        let reply = transport
            .query(&view.call_id)
            .await
            .map_err(|error| MediaPlaneError(format!("query for {}: {error}", view.call_id)))?;
        let stamped = reply.tags_created();
        if stamped.is_empty() {
            return Err(MediaPlaneError(format!(
                "rtpengine knows no participants for call {}; \
                 it is not anchoring that call",
                view.call_id
            )));
        }
        let ordered = order_participants(&stamped);
        for tag in ordered.tags {
            if view.from_tags.len() >= MAX_TAPPED_LEGS {
                break;
            }
            if !view.from_tags.contains(&tag) {
                view.from_tags.push(tag);
            }
        }
        view.attribution = if caller_named {
            Attribution::Explicit
        } else if seeded {
            Attribution::Unknown
        } else {
            ordered.attribution
        };
        match view.attribution {
            Attribution::Explicit => info!(
                call_id = %view.call_id,
                from_tags = ?view.from_tags,
                "resolved this call's participants; the caller was named so the \
                 customer leg is known"
            ),
            Attribution::Inferred => info!(
                call_id = %view.call_id,
                from_tags = ?view.from_tags,
                ?stamped,
                "nobody named the caller, but rtpengine's participant creation times \
                 order these legs, so the earliest created leg is the customer"
            ),
            Attribution::Unknown => warn!(
                call_id = %view.call_id,
                from_tags = ?view.from_tags,
                ?stamped,
                caller_tag_key = telcompat_caller_tag_key(),
                "nobody named the caller and rtpengine stamped these legs with the \
                 same creation second, so this tap will not claim a direction: its \
                 tracks are leg_a and leg_b. Pass the caller's sip from-tag to get \
                 customer and agent"
            ),
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

        let SessionHandles {
            call_id,
            hub,
            external_id,
            ..
        } = self.session_handles(view.session)?;
        let subscription = hub
            .attach(CONSUMER_QUEUE_FRAMES, consumer_selection_of(view.selector))
            .ok_or_else(|| {
                MediaPlaneError("the hub would not take another consumer".to_string())
            })?;
        let subscription_metrics = subscription.metrics();
        let media = subscription.control();
        media.set_paused(view.paused);

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
            egress: if view.capabilities.contains(Capabilities::INJECT) {
                self.inline_egress(view.session)
            } else {
                None
            },
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
                media,
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
                selection: consumer_selection_of(view.selector),
                format: view.format,
                paused: view.paused,
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
        let SessionHandles {
            hub,
            external_id,
            attribution,
            ..
        } = self.session_handles(view.session)?;
        let selection = recording_selection_of(view.selector);
        let conferenced = self.conference_of(view.session).is_ok();
        if conferenced
            && !view.group.is_empty()
            && matches!(view.selector, TrackSelector::Only(Track::Mixed))
        {
            return Err(MediaPlaneError(format!(
                "a recording group of the mixed track would write the same conference audio                  to one object per member; record the room once with an ungrouped file-s3                  attachment whose selector is only={}, and use only={} for the                  per-participant objects",
                consumer_ws::track_name(Track::Mixed),
                consumer_ws::track_name(Track::Customer)
            )));
        }
        let grouped = if view.group.is_empty() {
            None
        } else {
            let participant = participant_of(&view, &external_id)?;
            let targets = participant_targets(&identity, &participant, view.selector, attribution);
            let key = GroupKey {
                account_id: identity.account_id.clone(),
                group: view.group.clone(),
            };
            let anchor = self.join_group(&key, view.id, &identity, &targets)?;
            Some((key, targets, anchor))
        };
        let rate = self.config.format.sample_rate_hz;
        let resume_ms = view
            .metadata
            .get(recorder::RESUME_MS_METADATA_KEY)
            .and_then(|held| held.parse::<u64>().ok())
            .unwrap_or_default();
        let spill_owner = view
            .metadata
            .get(recorder::SPILL_OWNER_METADATA_KEY)
            .cloned()
            .unwrap_or_default();
        let (member_of, spec) = match grouped {
            Some((key, targets, anchor)) => (
                Some(key),
                RecorderSpec {
                    session: view.session,
                    recording_id: identity.recording_id.clone(),
                    format: identity.format,
                    targets,
                    sample_rate_hz: rate,
                    max_duration: recorder::MAX_RECORDING,
                    group_anchor: Some(anchor),
                    resume_ms,
                },
            ),
            None => (
                None,
                RecorderSpec {
                    resume_ms,
                    ..RecorderSpec::one_object(
                        view.session,
                        &identity,
                        layout_of(view.selector),
                        rate,
                    )
                },
            ),
        };
        let subscription = match hub.attach(CONSUMER_QUEUE_FRAMES, selection) {
            Some(subscription) => subscription,
            None => {
                if let Some(key) = &member_of {
                    self.leave_group(key, view.id);
                }
                return Err(MediaPlaneError(
                    "the hub would not take another consumer".to_string(),
                ));
            }
        };
        let subscription_metrics = subscription.metrics();
        let keys: Vec<String> = spec
            .targets
            .iter()
            .map(|target| target.key.clone())
            .collect();
        let recording_id = identity.recording_id.clone();
        let shape = recorder::RecordingShape::of(
            spec.targets
                .first()
                .map(|target| target.layout)
                .unwrap_or(Layout::Stereo),
            member_of.is_some(),
        )
        .named(conferenced);
        let handle = recorder::spawn(
            spec,
            subscription,
            self.config.recording.clone(),
            self.observer(),
        );
        let progress = handle.progress();

        let mut held = self
            .attachments
            .lock()
            .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
        held.insert(
            view.id,
            LiveAttachment::Recording {
                session: view.session,
                recording_id: recording_id.clone(),
                member_of,
                handle: Some(handle),
                progress,
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
            group = %view.group,
            %shape,
            ?keys,
            ?selection,
            resume_ms,
            spill_owner = %spill_owner,
            "recording this call"
        );
        for key in keys {
            self.observe(
                view.session,
                Observation::RecordingStarted {
                    recording_id: recording_id.clone(),
                    path: key,
                    shape: shape.clone(),
                },
            );
        }
        Ok(())
    }

    fn join_group(
        &self,
        key: &GroupKey,
        attachment: AttachmentId,
        identity: &RecordingIdentity,
        targets: &[RecordingTarget],
    ) -> Result<Instant, MediaPlaneError> {
        let counters = &self.config.recording.counters;
        let mut held = self
            .groups
            .lock()
            .map_err(|_| MediaPlaneError("the recording group table is poisoned".to_string()))?;
        let refuse = |reason: String| {
            counters.group_joins_refused.fetch_add(1, Ordering::Relaxed);
            warn!(group = %key, %attachment, %reason, "this recording group refused a member");
            MediaPlaneError(reason)
        };
        let group = match held.get_mut(key) {
            Some(group) => {
                if group.recording_id != identity.recording_id {
                    return Err(refuse(format!(
                        "recording group {key} is already recording {} and one group writes \
                         one recording; attach with {}/{}.{} or a different group",
                        group.recording_id,
                        identity.account_id,
                        group.recording_id,
                        group.format.extension()
                    )));
                }
                group
            }
            None => {
                counters.groups_live.fetch_add(1, Ordering::Relaxed);
                info!(group = %key, recording_id = %identity.recording_id, "opened a recording group");
                held.entry(key.clone()).or_insert(RecordingGroup {
                    recording_id: identity.recording_id.clone(),
                    format: identity.format,
                    opened_at: Instant::now(),
                    members: HashMap::new(),
                })
            }
        };
        for target in targets {
            if group.holds(&target.key) {
                return Err(refuse(format!(
                    "recording group {key} already has a participant writing {}; \
                     each member needs a label of its own",
                    target.key
                )));
            }
        }
        group.members.insert(
            attachment,
            targets.iter().map(|target| target.key.clone()).collect(),
        );
        counters.group_members_live.fetch_add(1, Ordering::Relaxed);
        Ok(group.opened_at)
    }

    fn leave_group(&self, key: &GroupKey, attachment: AttachmentId) {
        let Ok(mut held) = self.groups.lock() else {
            warn!(group = %key, "the recording group table is poisoned; the group is left behind");
            return;
        };
        let Some(group) = held.get_mut(key) else {
            return;
        };
        if group.members.remove(&attachment).is_some() {
            self.config
                .recording
                .counters
                .group_members_live
                .fetch_sub(1, Ordering::Relaxed);
        }
        if group.members.is_empty() {
            held.remove(key);
            self.config
                .recording
                .counters
                .groups_live
                .fetch_sub(1, Ordering::Relaxed);
            info!(group = %key, "the last member left this recording group");
        }
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
            member_of,
            handle,
            ..
        } = &mut live
        else {
            return None;
        };
        let handle = handle.take()?;
        let finished = handle.finish().await;
        info!(
            %attachment,
            session = %session,
            %recording_id,
            group = ?member_of.as_ref().map(GroupKey::to_string),
            duration_ms = finished.stopped.as_ref().map(|stop| stop.duration_ms),
            frames = finished.stopped.as_ref().map(|stop| stop.frames),
            "recording stopped; its upload runs in the background"
        );
        if let Some(key) = member_of.as_ref() {
            self.leave_group(key, attachment);
        }
        self.uploads.adopt(finished, self.observer());
        Some(())
    }

    async fn open_tap_session(&self, view: SessionView) -> Result<OpenedSession, MediaPlaneError> {
        if view.call_id.is_empty() {
            return Err(MediaPlaneError("a tap needs the call-id".to_string()));
        }
        let ResolvedNode {
            node,
            view,
            caller_named,
        } = self.resolve_node(view).await?;
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

        self.config
            .capabilities
            .report_first_contact(node, &transport)
            .await;

        let view = self
            .complete_from_tags(&transport, view, caller_named)
            .await?;

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
            offered_tap_format(&offer, configured.ptime_ms, self.config.opus_decode_rate_hz)?
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
        let mut port_leases = Vec::with_capacity(offer.streams.len());
        for _ in &offer.streams {
            let bound = self
                .config
                .media_ports
                .bind(self.config.local_media_address)
                .map_err(|error| MediaPlaneError(error.to_string()))?;
            receive_ports.push(bound.port);
            port_leases.push(bound.lease);
            sockets.push(bound.socket);
        }

        let answer_with = if transcoding {
            transcoded_tap_codec(&offer, configured)?
        } else {
            negotiated_tap_codec(&offer)?
        };
        info!(
            session = %view.id,
            payload_type = answer_with.payload_type,
            clock_rate_hz = answer_with.clock_rate_hz,
            "answering the subscription with this codec"
        );

        let answer_sdp = tap_answer_sdp(
            self.config.sdp_session_id,
            self.config.advertised_media_address,
            &receive_ports,
            format,
            answer_with,
            &offer,
        )?;

        transport
            .subscribe_answer(&view.call_id, &to_tag, &answer_sdp)
            .await
            .map_err(|error| MediaPlaneError(format!("subscribe answer: {error}")))?;

        let ssrc_tracks = speaker_ssrcs(&transport, &view.call_id, &view.from_tags).await;

        let mut legs = Vec::with_capacity(sockets.len());
        let mut shared_stats = Vec::with_capacity(sockets.len());
        let mut ssrc_publishers = Vec::with_capacity(sockets.len());
        let digits = DigitQueue::new();
        tokio::spawn(publish_digits(
            view.id,
            Arc::clone(&digits),
            self.observer(),
        ));
        for (index, socket) in sockets.into_iter().enumerate() {
            let telephone_event = offer
                .streams
                .get(index)
                .and_then(|stream| stream.telephone_event())
                .map(|event| event.payload_type);
            let shared = Arc::new(SharedLegStats::default());
            shared_stats.push(Arc::clone(&shared));
            let leg = TapLeg::with_pipeline_config(
                speaker_track(index),
                socket,
                PipelineConfig {
                    audio_payload_type: answer_with.payload_type,
                    clock_rate_hz: answer_with.clock_rate_hz,
                    decode: format,
                    target_depth_packets: TARGET_DEPTH_PACKETS,
                    telephone_event_payload_type: telephone_event,
                },
                RETAIN_NO_LOCAL_AUDIO,
            )
            .map_err(|error| MediaPlaneError(format!("tap leg: {error}")))?
            .with_ssrc_tracks(ssrc_tracks.clone())
            .with_digit_sink(Arc::clone(&digits))
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
                kind: SessionKind::Tap,
                attribution: view.attribution,
                media_ports: port_leases,
                transport: Some(transport),
                external_id: view.external_id.clone(),
                call_id: view.call_id.clone(),
                to_tag,
                format,
                hub: hub_client,
                egress: None,
                stop,
                capture: Some(capture_thread),
                conference: None,
                mix_route: None,
                speakers,
                digits,
            },
        );
        drop(held);
        self.metrics.register_session(view.id, shared_stats);
        self.observe(
            view.id,
            Observation::LegsAttributed {
                attribution: view.attribution,
                tracks: control_api::convert::tracks_under(TrackSelector::All, view.attribution),
            },
        );
        Ok(OpenedSession::default())
    }

    fn open_inline_session(&self, view: SessionView) -> Result<OpenedSession, MediaPlaneError> {
        let offer_sdp = view.sdp_offer.as_deref().ok_or_else(|| {
            MediaPlaneError(
                "an inline session needs the peer's sdp offer; a tap is what listens to a \
                 call MSS is not in"
                    .to_string(),
            )
        })?;
        let offer = InlineOffer::parse(offer_sdp, self.config.format.ptime_ms)
            .map_err(|error| MediaPlaneError(format!("inline offer: {error}")))?;
        let peer_address: IpAddr = offer.peer_address.parse().map_err(|_| {
            MediaPlaneError(format!(
                "inline offer: {} is not an address this leg can send rtp to",
                offer.peer_address
            ))
        })?;
        let peer = SocketAddr::new(peer_address, offer.peer_port);
        let format = offer.format();

        let bound = self
            .config
            .media_ports
            .bind(self.config.local_media_address)
            .map_err(|error| MediaPlaneError(error.to_string()))?;
        let socket = bound.socket;
        let receive_port = bound.port;
        let port_leases = vec![bound.lease];
        socket
            .set_nonblocking(true)
            .map_err(|error| MediaPlaneError(format!("media socket: {error}")))?;
        let egress_socket = socket
            .try_clone()
            .map_err(|error| MediaPlaneError(format!("egress socket: {error}")))?;

        let answer_sdp = inline_answer_sdp(
            self.config.sdp_session_id,
            self.config.advertised_media_address,
            receive_port,
            &offer,
        )?;

        let epoch = Instant::now();
        let (mut egress, egress_handle) = InlineEgress::bind(
            egress_socket,
            peer,
            format,
            offer.codec.payload_type,
            egress_ssrc(view.id.raw(), self.config.sdp_session_id),
            epoch,
        )
        .map_err(|error| MediaPlaneError(format!("inline egress: {error}")))?;

        let shared = Arc::new(SharedLegStats::default());
        let digits = DigitQueue::new();
        tokio::spawn(publish_digits(
            view.id,
            Arc::clone(&digits),
            self.observer(),
        ));
        let leg = TapLeg::with_pipeline_config(
            Track::Customer,
            socket,
            PipelineConfig {
                audio_payload_type: offer.codec.payload_type,
                clock_rate_hz: offer.codec.clock_rate_hz,
                decode: format,
                target_depth_packets: TARGET_DEPTH_PACKETS,
                telephone_event_payload_type: offer
                    .telephone_event
                    .as_ref()
                    .map(|event| event.payload_type),
            },
            RETAIN_NO_LOCAL_AUDIO,
        )
        .map_err(|error| MediaPlaneError(format!("inline leg: {error}")))?
        .with_digit_sink(Arc::clone(&digits))
        .with_shared_stats(Arc::clone(&shared), STALL_AFTER, epoch);

        let (mut hub, hub_client) = Hub::new();
        let stop = Arc::new(AtomicBool::new(false));
        let capture_stop = Arc::clone(&stop);
        let session = view.id;
        let conference = (!view.group.is_empty()).then(|| view.group.clone());
        if let Some(name) = conference.as_deref() {
            self.seat_in_conference(
                name,
                format,
                ConferenceMember {
                    session,
                    external_id: view.external_id.clone(),
                    leg,
                    hub,
                    egress,
                },
            )?;
            let mut held = self
                .sessions
                .lock()
                .map_err(|_| MediaPlaneError("the session table is poisoned".to_string()))?;
            held.insert(
                view.id,
                LiveSession {
                    kind: SessionKind::Inline,
                    attribution: view.attribution,
                    media_ports: port_leases,
                    transport: None,
                    external_id: view.external_id.clone(),
                    call_id: view.call_id.clone(),
                    to_tag: String::new(),
                    format,
                    hub: hub_client,
                    egress: Some(egress_handle.clone()),
                    stop,
                    capture: None,
                    conference,
                    mix_route: None,
                    speakers: None,
                    digits: Arc::clone(&digits),
                },
            );
            drop(held);
            self.metrics.register_session(view.id, vec![shared]);
            self.metrics
                .register_inline(view.id, egress_handle.shared());
            info!(
                session = %view.id,
                external_id = %view.external_id,
                conference = %view.group,
                %peer,
                receive_port,
                encoding = ?format.encoding,
                sample_rate_hz = format.sample_rate_hz,
                ptime_ms = format.ptime_ms,
                "answered an inline leg and seated it in a conference"
            );
            return Ok(OpenedSession::answered(answer_sdp));
        }
        let mut legs = vec![leg];
        let capture_thread = std::thread::Builder::new()
            .name(format!("mss-inline-{}", view.id.raw()))
            .spawn(move || {
                let summary = capture_with_egress(
                    &mut legs,
                    Some(&mut hub),
                    Some(&mut egress),
                    format,
                    MAX_SESSION_DURATION,
                    &capture_stop,
                );
                for leg in legs.iter() {
                    let stats = leg.stats();
                    info!(
                        %session,
                        track = ?leg.track(),
                        datagrams = stats.datagrams,
                        frames_played = stats.pipeline.frames_played,
                        frames_concealed = stats.pipeline.frames_concealed,
                        underruns = stats.underruns,
                        telephone_event_packets = stats.pipeline.telephone_events,
                        dtmf_digits = stats.pipeline.dtmf_digits,
                        jitter_lost = stats.jitter.lost,
                        jitter_late_drops = stats.jitter.late_drops,
                        recv_errors = stats.recv_errors,
                        "inline leg ingest finished"
                    );
                }
                let paced = egress.stats();
                info!(
                    %session,
                    peer = %egress.peer(),
                    releases = summary.releases,
                    reanchors = summary.reanchors,
                    elapsed_ms = summary.elapsed.as_millis() as u64,
                    published = hub.published(),
                    packets_emitted = paced.packets_emitted,
                    silence_frames = paced.silence_frames,
                    partial_frames = paced.partial_frames,
                    marker_packets = paced.marker_packets,
                    late_ticks = paced.late_ticks,
                    dropped_samples = paced.dropped_samples,
                    flushed_samples = paced.flushed_samples,
                    encode_errors = paced.encode_errors,
                    "inline leg egress finished"
                );
            })
            .map_err(|error| MediaPlaneError(format!("capture thread: {error}")))?;

        info!(
            session = %view.id,
            external_id = %view.external_id,
            call_id = %view.call_id,
            %peer,
            receive_port,
            encoding = ?format.encoding,
            sample_rate_hz = format.sample_rate_hz,
            ptime_ms = format.ptime_ms,
            payload_type = offer.codec.payload_type,
            telephone_event = ?offer.telephone_event.as_ref().map(|event| event.payload_type),
            "answered an inline leg; MSS is the rtp endpoint for this session"
        );

        let mut held = self
            .sessions
            .lock()
            .map_err(|_| MediaPlaneError("the session table is poisoned".to_string()))?;
        held.insert(
            view.id,
            LiveSession {
                kind: SessionKind::Inline,
                attribution: view.attribution,
                media_ports: port_leases,
                transport: None,
                external_id: view.external_id.clone(),
                call_id: view.call_id.clone(),
                to_tag: String::new(),
                format,
                hub: hub_client,
                egress: Some(egress_handle.clone()),
                stop,
                capture: Some(capture_thread),
                conference: None,
                mix_route: None,
                speakers: None,
                digits: Arc::clone(&digits),
            },
        );
        drop(held);
        self.metrics.register_session(view.id, vec![shared]);
        self.metrics
            .register_inline(view.id, egress_handle.shared());
        Ok(OpenedSession::answered(answer_sdp))
    }

    fn seat_in_conference(
        &self,
        name: &str,
        format: AudioFormat,
        member: ConferenceMember,
    ) -> Result<(), MediaPlaneError> {
        let session = member.session;
        let mut held = self
            .conferences
            .lock()
            .map_err(|_| MediaPlaneError("the conference table is poisoned".to_string()))?;
        if let Some(conference) = held.get_mut(name) {
            conference
                .accepts(format)
                .map_err(|error| MediaPlaneError(format!("conference {name}: {error}")))?;
            conference
                .seat(member)
                .map_err(|error| MediaPlaneError(format!("conference {name}: {error}")))?;
            info!(
                conference = %name,
                %session,
                members = conference.member_count(),
                "seated a leg in a live conference"
            );
            return Ok(());
        }
        let mut opened = Conference::start(name, format)
            .map_err(|error| MediaPlaneError(format!("conference {name}: {error}")))?;
        if let Err(error) = opened.seat(member) {
            if let Some(thread) = opened.unseat(session) {
                drop(thread);
            }
            return Err(MediaPlaneError(format!("conference {name}: {error}")));
        }
        self.metrics.register_conference(name, opened.shared());
        held.insert(name.to_string(), opened);
        info!(conference = %name, %session, "opened a conference for its first leg");
        Ok(())
    }

    fn leave_conference(
        &self,
        name: &str,
        session: SessionId,
    ) -> Option<std::thread::JoinHandle<()>> {
        let mut held = match self.conferences.lock() {
            Ok(held) => held,
            Err(_) => {
                warn!(conference = %name, "the conference table is poisoned; the mix is left behind");
                return None;
            }
        };
        let conference = held.get_mut(name)?;
        let thread = conference.unseat(session);
        if thread.is_some() {
            held.remove(name);
            self.metrics.retire_conference(name);
            info!(conference = %name, "the last leg left this conference; it is closed");
        }
        thread
    }

    fn conference_of(&self, session: SessionId) -> Result<String, MediaPlaneError> {
        let held = self
            .sessions
            .lock()
            .map_err(|_| MediaPlaneError("the session table is poisoned".to_string()))?;
        let live = held
            .get(&session)
            .ok_or_else(|| MediaPlaneError(format!("{session} is not live on this pod")))?;
        live.conference.clone().ok_or_else(|| {
            MediaPlaneError(format!(
                "{session} is not a conference leg, so its injected audio has no mix to \
                 route into; seat the leg with create-session kind=inline group=<conference>"
            ))
        })
    }

    fn route_injection(
        &self,
        session: SessionId,
        attachment: AttachmentId,
        route: MixRoute,
    ) -> Result<(), MediaPlaneError> {
        let name = self.conference_of(session)?;
        let target = route.target_name().to_string();
        let monitor_audible = route.monitor_audible;
        let private = route.is_private();
        {
            let mut held = self
                .conferences
                .lock()
                .map_err(|_| MediaPlaneError("the conference table is poisoned".to_string()))?;
            let conference = held.get_mut(&name).ok_or_else(|| {
                MediaPlaneError(format!("conference {name} is not running on this pod"))
            })?;
            conference
                .route(session, Some(attachment), route)
                .map_err(|error| MediaPlaneError(format!("conference {name}: {error}")))?;
        }
        if let Ok(mut held) = self.sessions.lock() {
            if let Some(live) = held.get_mut(&session) {
                live.mix_route = (!private).then_some(attachment);
            }
        }
        info!(
            %session,
            %attachment,
            conference = %name,
            %target,
            monitor_audible,
            "an injecting attachment named where its audio lands in the mix"
        );
        Ok(())
    }

    fn control_member(
        &self,
        session: SessionId,
        control: MemberControl,
    ) -> Result<(), MediaPlaneError> {
        let name = self.conference_of(session)?;
        {
            let mut held = self
                .conferences
                .lock()
                .map_err(|_| MediaPlaneError("the conference table is poisoned".to_string()))?;
            let conference = held.get_mut(&name).ok_or_else(|| {
                MediaPlaneError(format!("conference {name} is not running on this pod"))
            })?;
            conference
                .control(session, control)
                .map_err(|error| MediaPlaneError(format!("conference {name}: {error}")))?;
        }
        info!(
            %session,
            conference = %name,
            mute = ?control.mute,
            deaf = ?control.deaf,
            hold = ?control.hold,
            "a member control verb arrived for this conference leg"
        );
        Ok(())
    }

    fn play_into_room(
        &self,
        session: SessionId,
        source: PlaybackSource,
    ) -> Result<(), MediaPlaneError> {
        let name = self.conference_of(session)?;
        let format = self.session_format(session)?;
        let pcm = inline_playback_pcm(&playback_wav(source)?, format)?;
        let chunk_samples =
            (format.sample_rate_hz as usize * EGRESS_CHUNK_MS as usize / 1000).max(1);
        let chunks = pcm.len().div_ceil(chunk_samples);
        let mut held = self
            .conferences
            .lock()
            .map_err(|_| MediaPlaneError("the conference table is poisoned".to_string()))?;
        let conference = held.get_mut(&name).ok_or_else(|| {
            MediaPlaneError(format!("conference {name} is not running on this pod"))
        })?;
        let free = conference.free_prompt_chunks();
        if chunks > free {
            return Err(MediaPlaneError(format!(
                "this prompt is {} ms of audio and conference {name} has room for {} ms; \
                 a long-form prompt belongs on an inject attachment routed mix_target=all",
                pcm.len() as u64 * 1000 / format.sample_rate_hz.max(1) as u64,
                free as u32 * EGRESS_CHUNK_MS
            )));
        }
        for chunk in pcm.chunks(chunk_samples) {
            conference
                .prompt(chunk.to_vec())
                .map_err(|error| MediaPlaneError(format!("conference {name}: {error}")))?;
        }
        info!(
            %session,
            conference = %name,
            samples = pcm.len(),
            chunks,
            "queued a prompt into the whole room"
        );
        Ok(())
    }

    fn flush_room_prompts(&self, session: SessionId) -> Result<(), MediaPlaneError> {
        let name = self.conference_of(session)?;
        let held = self
            .conferences
            .lock()
            .map_err(|_| MediaPlaneError("the conference table is poisoned".to_string()))?;
        let conference = held.get(&name).ok_or_else(|| {
            MediaPlaneError(format!("conference {name} is not running on this pod"))
        })?;
        conference.flush_prompts();
        info!(
            %session,
            conference = %name,
            "flushed the room's prompt queue; the next mixed frame carries no prompt"
        );
        Ok(())
    }

    fn revert_injection(&self, attachment: AttachmentId) {
        let owner = match self.sessions.lock() {
            Ok(mut held) => held
                .iter_mut()
                .find(|(_, live)| live.mix_route == Some(attachment))
                .map(|(session, live)| {
                    live.mix_route = None;
                    (*session, live.conference.clone())
                }),
            Err(_) => None,
        };
        let Some((session, Some(name))) = owner else {
            return;
        };
        let mut held = match self.conferences.lock() {
            Ok(held) => held,
            Err(_) => return,
        };
        if let Some(conference) = held.get_mut(&name) {
            match conference.route(session, None, MixRoute::private()) {
                Ok(()) => info!(
                    %session,
                    %attachment,
                    conference = %name,
                    "the whisperer detached, so this leg's injected audio is private again"
                ),
                Err(error) => warn!(
                    %session,
                    %attachment,
                    conference = %name,
                    %error,
                    "the whisperer detached but its route could not be taken back"
                ),
            }
        }
    }

    pub fn inline_egress(&self, session: SessionId) -> Option<InlineEgressHandle> {
        self.sessions
            .lock()
            .ok()?
            .get(&session)
            .and_then(|live| live.egress.clone())
    }

    fn play_into_inline_leg(
        &self,
        session: SessionId,
        source: PlaybackSource,
    ) -> Result<(), MediaPlaneError> {
        let format = self.session_format(session)?;
        let egress = self
            .inline_egress(session)
            .ok_or_else(|| MediaPlaneError(format!("{session} has no inline egress")))?;
        let pcm = inline_playback_pcm(&playback_wav(source)?, format)?;
        let chunk_samples =
            (format.sample_rate_hz as usize * EGRESS_CHUNK_MS as usize / 1000).max(1);
        let chunks = pcm.len().div_ceil(chunk_samples);
        let free = egress.free_chunks();
        if chunks > free {
            return Err(MediaPlaneError(format!(
                "this playback is {} ms of audio and the inline egress queue has room for \
                 {} ms; stream long-form audio through an inject attachment instead",
                pcm.len() as u64 * 1000 / format.sample_rate_hz.max(1) as u64,
                free as u32 * EGRESS_CHUNK_MS
            )));
        }
        for chunk in pcm.chunks(chunk_samples) {
            if !egress.push(chunk.to_vec()) {
                return Err(MediaPlaneError(
                    "the inline egress queue filled while this playback was being queued"
                        .to_string(),
                ));
            }
        }
        info!(
            %session,
            samples = pcm.len(),
            chunks,
            "queued a playback into the inline leg's egress"
        );
        Ok(())
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

    fn session_handles(&self, session: SessionId) -> Result<SessionHandles, MediaPlaneError> {
        let held = self
            .sessions
            .lock()
            .map_err(|_| MediaPlaneError("the session table is poisoned".to_string()))?;
        let live = held
            .get(&session)
            .ok_or_else(|| MediaPlaneError(format!("{session} is not tapped here")))?;
        Ok(SessionHandles {
            transport: live.transport.clone(),
            call_id: live.call_id.clone(),
            hub: live.hub.clone(),
            external_id: live.external_id.clone(),
            attribution: live.attribution,
        })
    }

    async fn end_attachment(&self, attachment: AttachmentId, held: LiveAttachment, reason: &str) {
        match held {
            LiveAttachment::Ws {
                session,
                media,
                task,
                ..
            } => {
                media.end_of_stream();
                let mut task = task;
                match tokio::time::timeout(POLITE_CLOSE, &mut task).await {
                    Ok(Ok(Ok(stats))) => info!(
                        %attachment,
                        %session,
                        %reason,
                        media_sent = stats.media_sent,
                        "the consumer websocket was closed after its stop frame"
                    ),
                    Ok(Ok(Err(error))) => warn!(
                        %attachment, %session, %error,
                        "the consumer websocket ended with an error instead of a stop frame"
                    ),
                    Ok(Err(_)) => warn!(
                        %attachment, %session,
                        "the consumer task ended before it could send its stop frame"
                    ),
                    Err(_) => {
                        task.abort();
                        warn!(
                            %attachment, %session,
                            "the consumer did not take its stop frame within {POLITE_CLOSE:?}; \
                             its socket was dropped"
                        );
                    }
                }
            }
            LiveAttachment::Grpc { session, live, .. } => {
                if let Some(GrpcLive {
                    frames,
                    media,
                    mut pump,
                }) = live
                {
                    media.end_of_stream();
                    if tokio::time::timeout(POLITE_CLOSE, &mut pump).await.is_err() {
                        pump.abort();
                        warn!(
                            %attachment, %session,
                            "the frame pump did not drain within {POLITE_CLOSE:?}"
                        );
                    }
                    if frames
                        .try_send(StreamFrame::Stop {
                            reason: reason.to_string(),
                        })
                        .is_err()
                    {
                        warn!(
                            %attachment, %session,
                            "the grpc consumer is too far behind for a stop frame; \
                             its stream is closed instead"
                        );
                    }
                }
                info!(%attachment, %session, %reason, "grpc consumer detached");
            }
            recording @ LiveAttachment::Recording { .. } => {
                self.finish_recording(attachment, recording).await;
            }
        }
        self.metrics.retire_consumer(attachment);
    }
}

#[control_api::async_trait]
impl MediaPlane for TapPlane {
    fn inline_egress_sink(&self, session: SessionId) -> Option<Arc<dyn InlineEgressSink>> {
        self.inline_egress(session)
            .map(|handle| Arc::new(handle) as Arc<dyn InlineEgressSink>)
    }

    fn member_state(&self, session: SessionId) -> Option<MemberStateView> {
        let name = self.conference_of(session).ok()?;
        self.conferences
            .lock()
            .ok()?
            .get(&name)?
            .member_state(session)
    }

    async fn open_session(&self, view: SessionView) -> Result<OpenedSession, MediaPlaneError> {
        match view.kind {
            SessionKind::Tap => self.open_tap_session(view).await,
            SessionKind::Inline => self.open_inline_session(view),
            SessionKind::Mix => Err(MediaPlaneError(
                "a mixed session is the phase-4 conference; join inline legs into a group \
                 instead"
                    .to_string(),
            )),
        }
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
            self.end_attachment(attachment, held, "the call ended")
                .await;
        }

        live.stop.store(true, Ordering::Relaxed);
        live.digits.close();
        self.metrics.record_digits_dropped(live.digits.dropped());
        if let Some(speakers) = live.speakers.take() {
            speakers.abort();
        }
        if let Some(transport) = live.transport.as_ref() {
            if let Err(error) = transport.unsubscribe(&live.call_id, &live.to_tag).await {
                warn!(
                    %session,
                    %error,
                    "unsubscribe failed; rtpengine keeps the subscription until it times out"
                );
            }
        }
        let mixing = live
            .conference
            .as_deref()
            .and_then(|name| self.leave_conference(name, session));
        if let Some(thread) = live.capture.take().or(mixing) {
            let joined = tokio::task::spawn_blocking(move || thread.join()).await;
            if joined.is_err() {
                warn!(%session, "the capture thread did not join cleanly");
            }
        }
        let released = std::mem::take(&mut live.media_ports);
        let released_ports: Vec<u16> = released.iter().map(PortLease::port).collect();
        drop(released);
        self.metrics.retire_session(session);
        info!(
            %session,
            kind = ?live.kind,
            call_id = %live.call_id,
            ?released_ports,
            "session closed"
        );
        Ok(())
    }

    async fn open_attachment(&self, view: AttachmentView) -> Result<(), MediaPlaneError> {
        if !view.group.is_empty() && view.transport != Transport::FileS3 {
            return Err(MediaPlaneError(format!(
                "a group is served for file-s3 recordings only, and {} named group {}; \
                 multi-party over the gRPC stream is not built yet, and the frozen \
                 ws-twilio dialect carries two tracks by construction",
                view.transport, view.group
            )));
        }
        let route = MixRoute::from_metadata(&view.metadata)
            .map_err(|error| MediaPlaneError(error.to_string()))?;
        let routed = match &route {
            Some(route) => {
                route
                    .authorize(view.capabilities)
                    .map_err(|error| MediaPlaneError(error.to_string()))?;
                self.route_injection(view.session, view.id, route.clone())?;
                true
            }
            None => false,
        };
        if let Some(control) = MemberControl::from_metadata(&view.metadata)
            .map_err(|error| MediaPlaneError(error.to_string()))?
        {
            self.control_member(view.session, control)?;
        }
        let attachment = view.id;
        let opened = match view.transport {
            Transport::WsTwilio => self.open_ws_attachment(view),
            Transport::GrpcStream => self.open_grpc_attachment(view),
            Transport::FileS3 => self.open_recording_attachment(view),
            other => Err(MediaPlaneError(format!(
                "{other} attachments are not served yet; ws-twilio, grpc-stream \
                 and file-s3 are"
            ))),
        };
        if opened.is_err() && routed {
            self.revert_injection(attachment);
        }
        opened
    }

    async fn update_attachment(&self, view: AttachmentView) -> Result<(), MediaPlaneError> {
        enum Applied {
            Recorder(bool),
            Consumer,
        }
        let applied = {
            let mut held = self
                .attachments
                .lock()
                .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
            match held.get_mut(&view.id) {
                Some(LiveAttachment::Recording {
                    handle: Some(handle),
                    ..
                }) => Applied::Recorder(handle.set_paused(view.paused)),
                Some(LiveAttachment::Recording { handle: None, .. }) => {
                    return Err(MediaPlaneError(format!(
                        "{} is a recording that has already been closed",
                        view.id
                    )))
                }
                Some(LiveAttachment::Ws { media, .. }) => {
                    media.set_paused(view.paused);
                    Applied::Consumer
                }
                Some(LiveAttachment::Grpc { paused, live, .. }) => {
                    *paused = view.paused;
                    if let Some(live) = live {
                        live.media.set_paused(view.paused);
                    }
                    Applied::Consumer
                }
                None => {
                    return Err(MediaPlaneError(format!(
                        "{} is not connected here",
                        view.id
                    )))
                }
            }
        };
        if let Some(route) = MixRoute::from_metadata(&view.metadata)
            .map_err(|error| MediaPlaneError(error.to_string()))?
        {
            route
                .authorize(view.capabilities)
                .map_err(|error| MediaPlaneError(error.to_string()))?;
            self.route_injection(view.session, view.id, route)?;
        }
        if let Some(control) = MemberControl::from_metadata(&view.metadata)
            .map_err(|error| MediaPlaneError(error.to_string()))?
        {
            self.control_member(view.session, control)?;
        }
        match applied {
            Applied::Recorder(true) => Ok(()),
            Applied::Recorder(false) => Err(MediaPlaneError(format!(
                "{} is not taking commands any more",
                view.id
            ))),
            Applied::Consumer => {
                info!(
                    attachment = %view.id,
                    paused = view.paused,
                    transport = %view.transport,
                    "the hub stops feeding this consumer while it is paused"
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
        self.revert_injection(attachment);
        if let Some(held) = live {
            self.end_attachment(attachment, held, "the attachment was detached")
                .await;
        }
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
        let (selection, target_format, paused) = {
            let held = self
                .attachments
                .lock()
                .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
            match held.get(&attachment) {
                Some(LiveAttachment::Grpc {
                    session: held_session,
                    selection,
                    format,
                    paused,
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
                    (*selection, *format, *paused)
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

        let SessionHandles {
            hub, attribution, ..
        } = self.session_handles(session)?;
        let subscription = hub
            .attach(CONSUMER_QUEUE_FRAMES, selection)
            .ok_or_else(|| {
                MediaPlaneError("the hub would not take another consumer".to_string())
            })?;
        let subscription_metrics = subscription.metrics();
        let media = subscription.control();
        media.set_paused(paused);
        let (frames, receiver) = mpsc::channel(GRPC_FRAME_QUEUE);
        let pump = tokio::spawn(pump_frames(
            subscription,
            frames.clone(),
            self.session_format(session)?,
            target_format,
            attribution,
        ));

        let mut held = self
            .attachments
            .lock()
            .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
        match held.get_mut(&attachment) {
            Some(LiveAttachment::Grpc { live, .. }) => {
                *live = Some(GrpcLive {
                    frames,
                    media,
                    pump,
                });
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
        if matches!(source, PlaybackSource::Stream) {
            return Err(MediaPlaneError(
                "a streaming playback is an inject-capable attachment on an inline leg, \
                 not a playback"
                    .to_string(),
            ));
        }
        let SessionHandles {
            transport, call_id, ..
        } = self.session_handles(session)?;
        if transport.is_none() {
            return match playback_reach(target_tag.as_deref())? {
                PlaybackReach::Room => self.play_into_room(session, source),
                PlaybackReach::Ear => self.play_into_inline_leg(session, source),
            };
        }
        let source = ng_play_source(source)?;
        let transport = require_subscription(session, transport)?;
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
        target_tag: Option<String>,
    ) -> Result<(), MediaPlaneError> {
        let SessionHandles {
            transport, call_id, ..
        } = self.session_handles(session)?;
        let Some(transport) = transport else {
            if matches!(playback_reach(target_tag.as_deref())?, PlaybackReach::Room) {
                return self.flush_room_prompts(session);
            }
            let egress = self.inline_egress(session).ok_or_else(|| {
                MediaPlaneError(format!("{session} has no inline egress to stop"))
            })?;
            egress.clear();
            info!(
                %session,
                "flushed an inline leg's egress queue; the next paced frame is silence"
            );
            return Ok(());
        };
        transport
            .stop_media(&call_id, &target_of(target_tag))
            .await
            .map_err(|error| MediaPlaneError(format!("stop media: {error}")))?;
        Ok(())
    }
}

#[control_api::async_trait]
impl TapSubscriptions for TapPlane {
    fn subscription_tag(&self, session: SessionId) -> Option<String> {
        self.sessions
            .lock()
            .ok()?
            .get(&session)
            .map(|live| live.to_tag.clone())
    }

    fn recording_journal(&self, attachment: AttachmentId) -> Option<PersistedRecording> {
        match self.attachments.lock().ok()?.get(&attachment)? {
            LiveAttachment::Recording {
                recording_id,
                progress,
                ..
            } => Some(PersistedRecording {
                recording_id: recording_id.clone(),
                owner: String::new(),
                recorded_ms: progress.recorded_ms(),
                spilled_ms: progress.spilled_ms(),
            }),
            _ => None,
        }
    }

    async fn unsubscribe_orphan(
        &self,
        node: &str,
        call_id: &str,
        to_tag: &str,
    ) -> Result<(), MediaPlaneError> {
        let node: SocketAddr = node.parse().map_err(|_| {
            MediaPlaneError(format!("rtpengine_node {node} is not an ip:port address"))
        })?;
        let transport = NgTransport::bind(
            SocketAddr::new(self.config.local_media_address, 0),
            node,
            NgTransportConfig::default(),
            self.config.cookie_prefix,
        )
        .await
        .map_err(|error| MediaPlaneError(format!("NG socket: {error}")))?;
        transport
            .unsubscribe(call_id, to_tag)
            .await
            .map_err(|error| MediaPlaneError(format!("unsubscribe {to_tag}: {error}")))?;
        Ok(())
    }
}

async fn pump_frames(
    mut subscription: Subscription,
    frames: mpsc::Sender<StreamFrame>,
    source: AudioFormat,
    target: AudioFormat,
    attribution: Attribution,
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
                        track: control_api::convert::track_name_under(track, attribution),
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
                track: control_api::convert::track_name_under(track, attribution),
                digit,
            },
        };
        if frames.send(frame).await.is_err() {
            return;
        }
    }
}

enum PlaybackReach {
    Ear,
    Room,
}

fn playback_reach(target_tag: Option<&str>) -> Result<PlaybackReach, MediaPlaneError> {
    match target_tag.map(str::trim) {
        None | Some("") => Ok(PlaybackReach::Ear),
        Some(MIX_TARGET_OWN) => Ok(PlaybackReach::Ear),
        Some(MIX_TARGET_EVERYONE) => Ok(PlaybackReach::Room),
        Some(other) => Err(MediaPlaneError(format!(
            "an inline leg has no sip from-tag to target, so {other:?} names nobody; \
             \"own\" or an empty target is this leg's own ear and \"all\" is the \
             conference it is seated in"
        ))),
    }
}

fn playback_wav(source: PlaybackSource) -> Result<Vec<u8>, MediaPlaneError> {
    match source {
        PlaybackSource::Blob(bytes) => Ok(bytes),
        PlaybackSource::File(path) => std::fs::read(&path)
            .map_err(|error| MediaPlaneError(format!("playback file {path}: {error}"))),
        PlaybackSource::Stream => Err(MediaPlaneError(
            "a streaming playback into an inline leg is an inject-capable attachment, \
             not a playback"
                .to_string(),
        )),
    }
}

fn inline_playback_pcm(wav: &[u8], format: AudioFormat) -> Result<Vec<i16>, MediaPlaneError> {
    let mut reader = hound::WavReader::new(std::io::Cursor::new(wav))
        .map_err(|error| MediaPlaneError(format!("playback audio is not a wav: {error}")))?;
    let spec = reader.spec();
    if spec.channels != 1 || spec.bits_per_sample != 16 {
        return Err(MediaPlaneError(format!(
            "an inline playback must be 16-bit mono; this wav is {}-bit with {} channels",
            spec.bits_per_sample, spec.channels
        )));
    }
    if spec.sample_rate != format.sample_rate_hz {
        return Err(MediaPlaneError(format!(
            "this playback is {} Hz and the leg negotiated {} Hz; resample before queueing it",
            spec.sample_rate, format.sample_rate_hz
        )));
    }
    reader
        .samples::<i16>()
        .collect::<Result<Vec<i16>, _>>()
        .map_err(|error| MediaPlaneError(format!("playback audio: {error}")))
}

fn ng_play_source(source: PlaybackSource) -> Result<PlaySource, MediaPlaneError> {
    match source {
        PlaybackSource::File(path) => Ok(PlaySource::File(path)),
        PlaybackSource::Blob(bytes) => {
            if bytes.len() > MAX_PLAYBACK_BLOB_BYTES {
                return Err(MediaPlaneError(format!(
                    "a {} byte blob exceeds what one NG datagram carries; \
                     chunked playback is not implemented",
                    bytes.len()
                )));
            }
            Ok(PlaySource::Blob(bytes))
        }
        PlaybackSource::Stream => Err(MediaPlaneError(
            "a streaming playback is an inject-capable attachment on an inline leg, \
             not a playback"
                .to_string(),
        )),
    }
}

fn require_subscription(
    session: SessionId,
    transport: Option<Arc<NgTransport>>,
) -> Result<Arc<NgTransport>, MediaPlaneError> {
    transport.ok_or_else(|| {
        MediaPlaneError(format!(
            "{session} is an inline leg, not a tap: it has no rtpengine subscription to play \
             media into, and audio reaches its peer through the egress queue"
        ))
    })
}

fn transcoded_tap_codec(
    offer: &SubscriptionOffer,
    configured: AudioFormat,
) -> Result<NegotiatedCodec, MediaPlaneError> {
    let mut settled: Option<NegotiatedCodec> = None;
    for stream in &offer.streams {
        let codec = match stream.negotiate_encoding(configured.encoding) {
            Ok(codec) => codec,
            Err(SdpError::CodecNotOffered(_)) => {
                return NegotiatedCodec::from_static_format(configured).map_err(|error| {
                    MediaPlaneError(format!(
                        "rtpengine was asked to transcode the tap to {:?} but offered no payload \
                         type for it, and {:?} has no static one either: {error}",
                        configured.encoding, configured.encoding
                    ))
                })
            }
            Err(error) => return Err(MediaPlaneError(format!("answer codec: {error}"))),
        };
        match settled {
            None => settled = Some(codec),
            Some(ref first) if first.payload_type != codec.payload_type => {
                return Err(MediaPlaneError(format!(
                    "rtpengine offered {:?} on payload type {} for one leg and {} for another; \
                     a tap answers with one codec for every leg",
                    configured.encoding, first.payload_type, codec.payload_type
                )))
            }
            Some(_) => {}
        }
    }
    settled.ok_or_else(|| MediaPlaneError("rtpengine offered no streams".to_string()))
}

fn tap_answer_sdp(
    session_id: u64,
    advertised: IpAddr,
    receive_ports: &[u16],
    format: AudioFormat,
    answer_with: NegotiatedCodec,
    offer: &SubscriptionOffer,
) -> Result<String, MediaPlaneError> {
    let advertised = advertised.to_string();
    SubscriptionAnswer {
        session_id,
        local_address: &advertised,
        receive_ports,
        format,
        answer_with,
    }
    .to_sdp(offer)
    .map_err(|error| MediaPlaneError(format!("answer sdp: {error}")))
}

fn inline_answer_sdp(
    session_id: u64,
    advertised: IpAddr,
    receive_port: u16,
    offer: &InlineOffer,
) -> Result<String, MediaPlaneError> {
    let advertised = advertised.to_string();
    offer
        .answer(session_id, &advertised, receive_port)
        .to_sdp()
        .map_err(|error| MediaPlaneError(format!("inline answer: {error}")))
}

fn negotiated_tap_codec(offer: &SubscriptionOffer) -> Result<NegotiatedCodec, MediaPlaneError> {
    let mut settled: Option<NegotiatedCodec> = None;
    for (index, stream) in offer.streams.iter().enumerate() {
        let codec = stream.negotiate().map_err(|error| {
            MediaPlaneError(format!(
                "stream {index}: {error}; this call needs transcoding at the tap"
            ))
        })?;
        match settled {
            None => settled = Some(codec),
            Some(first) if first != codec => {
                return Err(MediaPlaneError(format!(
                    "rtpengine offered {:?} on one stream and {:?} on another; \
                     a tap decodes one codec for every leg, so this call needs \
                     transcoding at the tap",
                    first.encoding, codec.encoding
                )))
            }
            Some(_) => {}
        }
    }
    settled.ok_or_else(|| MediaPlaneError("rtpengine offered no streams".to_string()))
}

fn offered_tap_format(
    offer: &SubscriptionOffer,
    ptime_fallback_ms: u32,
    opus_decode_rate_hz: u32,
) -> Result<AudioFormat, MediaPlaneError> {
    let mut settled: Option<AudioFormat> = None;
    for (index, stream) in offer.streams.iter().enumerate() {
        let offered = stream.offered_format(ptime_fallback_ms).map_err(|error| {
            MediaPlaneError(format!(
                "stream {index}: {error}; this call needs transcoding at the tap"
            ))
        })?;
        let format = if offered.encoding == Encoding::Opus {
            AudioFormat {
                sample_rate_hz: opus_decode_rate_hz,
                ..offered
            }
        } else {
            offered
        };
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

struct OrderedParticipants {
    tags: Vec<String>,
    attribution: Attribution,
}

fn order_participants(stamped: &[(String, Option<i64>)]) -> OrderedParticipants {
    let mut ordered: Vec<(String, Option<i64>)> = stamped.to_vec();
    ordered.sort_by(|left, right| match (left.1, right.1) {
        (Some(one), Some(other)) => one.cmp(&other),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    let separated = match (ordered.first(), ordered.get(1)) {
        (Some((_, Some(first))), Some((_, Some(second)))) => first < second,
        _ => false,
    };
    OrderedParticipants {
        tags: ordered.into_iter().map(|(tag, _)| tag).collect(),
        attribution: if separated {
            Attribution::Inferred
        } else {
            Attribution::Unknown
        },
    }
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

fn consumer_selection_of(selector: TrackSelector) -> TrackSelection {
    match selector {
        TrackSelector::All => TrackSelection::Speakers,
        TrackSelector::Only(track) => TrackSelection::Only(track),
    }
}

fn recording_selection_of(selector: TrackSelector) -> TrackSelection {
    match selector {
        TrackSelector::All => TrackSelection::All,
        TrackSelector::Only(track) => TrackSelection::Only(track),
    }
}

fn participant_of(view: &AttachmentView, external_id: &str) -> Result<String, MediaPlaneError> {
    let named = if view.label.is_empty() {
        external_id
    } else {
        view.label.as_str()
    };
    recorder::participant_label(named)
        .map(str::to_string)
        .map_err(|error| {
            MediaPlaneError(format!(
                "{error}; a recording group names each participant's file after its \
                 attachment label"
            ))
        })
}

fn participant_targets(
    identity: &RecordingIdentity,
    participant: &str,
    selector: TrackSelector,
    attribution: Attribution,
) -> Vec<RecordingTarget> {
    match selector {
        TrackSelector::Only(track) => vec![RecordingTarget {
            key: identity.participant_key(participant),
            layout: Layout::Mono(track),
        }],
        TrackSelector::All => [Track::Customer, Track::Agent]
            .into_iter()
            .map(|track| RecordingTarget {
                key: identity.participant_key(&format!(
                    "{participant}.{}",
                    control_api::convert::track_name_under(track, attribution)
                )),
                layout: Layout::Mono(track),
            })
            .collect(),
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

async fn publish_digits(
    session: SessionId,
    digits: Arc<DigitQueue>,
    sink: Option<Weak<dyn ObservationSink>>,
) {
    while let Some(press) = digits.next().await {
        info!(
            %session,
            digit = %press.digit,
            track = ?press.track,
            duration_ms = press.duration_ms,
            rtp_timestamp = press.rtp_timestamp,
            "a digit was pressed on this call"
        );
        match sink.as_ref().and_then(Weak::upgrade) {
            Some(sink) => sink.observe(
                session,
                Observation::Dtmf {
                    track: press.track,
                    digit: press.digit,
                    duration_ms: press.duration_ms,
                    rtp_timestamp: press.rtp_timestamp,
                },
            ),
            None => warn!(
                %session,
                digit = %press.digit,
                "no observation sink is wired; this digit reaches nobody"
            ),
        }
    }
    if digits.dropped() > 0 {
        warn!(
            %session,
            dropped = digits.dropped(),
            published = digits.published(),
            "digits arrived faster than the event bus drained them"
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
    use session_core::mix::{
        MEMBER_DEAF_METADATA_KEY, MEMBER_FLAG_OFF, MEMBER_FLAG_ON, MEMBER_HOLD_METADATA_KEY,
        MEMBER_MUTE_METADATA_KEY, MIX_MONITOR_EXCLUDE, MIX_MONITOR_METADATA_KEY, MIX_SOURCE_LEG,
        MIX_SOURCE_METADATA_KEY, MIX_TARGET_EVERYONE, MIX_TARGET_METADATA_KEY,
    };
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

        async fn exists(&self, _key: &str) -> Result<bool, recorder::UploadError> {
            Ok(false)
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>, recorder::UploadError> {
            Err(recorder::UploadError::Missing(key.to_string()))
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>, recorder::UploadError> {
            Ok(Vec::new())
        }

        async fn delete(&self, _key: &str) -> Result<(), recorder::UploadError> {
            Ok(())
        }

        fn describe(&self) -> String {
            "nowhere".to_string()
        }
    }

    #[derive(Default)]
    struct BucketSink {
        puts: Mutex<Vec<(String, Vec<u8>)>>,
        slow_by: Duration,
    }

    impl BucketSink {
        fn slow(slow_by: Duration) -> BucketSink {
            BucketSink {
                puts: Mutex::new(Vec::new()),
                slow_by,
            }
        }

        fn keys(&self) -> Vec<String> {
            self.puts
                .lock()
                .unwrap()
                .iter()
                .map(|(key, _)| key.clone())
                .collect()
        }

        fn body(&self, key: &str) -> Vec<u8> {
            let held = self
                .puts
                .lock()
                .unwrap()
                .iter()
                .find(|(held, _)| held == key)
                .map(|(_, body)| body.clone());
            held.unwrap_or_else(|| panic!("{key} was never uploaded; got {:?}", self.keys()))
        }
    }

    #[control_api::async_trait]
    impl recorder::RecordingSink for BucketSink {
        async fn put(
            &self,
            key: &str,
            _content_type: &'static str,
            body: Vec<u8>,
        ) -> Result<String, recorder::UploadError> {
            if !self.slow_by.is_zero() {
                tokio::time::sleep(self.slow_by).await;
            }
            self.puts.lock().unwrap().push((key.to_string(), body));
            Ok(format!("s3:/{}/{key}", "/lab-recordings"))
        }

        async fn exists(&self, key: &str) -> Result<bool, recorder::UploadError> {
            Ok(self
                .puts
                .lock()
                .unwrap()
                .iter()
                .any(|(held, _)| held == key))
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>, recorder::UploadError> {
            self.puts
                .lock()
                .unwrap()
                .iter()
                .find(|(held, _)| held == key)
                .map(|(_, body)| body.clone())
                .ok_or_else(|| recorder::UploadError::Missing(key.to_string()))
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, recorder::UploadError> {
            Ok(self
                .puts
                .lock()
                .unwrap()
                .iter()
                .filter(|(held, _)| held.starts_with(prefix))
                .map(|(held, _)| held.clone())
                .collect())
        }

        async fn delete(&self, key: &str) -> Result<(), recorder::UploadError> {
            self.puts.lock().unwrap().retain(|(held, _)| held != key);
            Ok(())
        }

        fn describe(&self) -> String {
            "an in-memory bucket".to_string()
        }
    }

    fn recorded_wav(body: &[u8]) -> (u16, Vec<i16>) {
        let reader = hound::WavReader::new(std::io::Cursor::new(body.to_vec())).expect("a wav");
        let channels = reader.spec().channels;
        let samples = reader
            .into_samples::<i16>()
            .map(|sample| sample.expect("a sample"))
            .collect();
        (channels, samples)
    }

    fn plane() -> TapPlane {
        plane_with_recording(RecordingSupport::default())
    }

    fn plane_with_recording(recording: RecordingSupport) -> TapPlane {
        TapPlane::new(TapPlaneConfig {
            default_node: None,
            local_media_address: IpAddr::from([127, 0, 0, 1]),
            advertised_media_address: IpAddr::from([127, 0, 0, 1]),
            media_ports: MediaPortAllocator::ephemeral(),
            format: AudioFormat::pcmu_8k_20ms(),
            transcode_at_tap: true,
            opus_decode_rate_hz: 16000,
            cookie_prefix: 1,
            sdp_session_id: 1,
            recording,
            capabilities: Arc::new(NodeCapabilityLog::new(true)),
        })
    }

    use std::net::UdpSocket;

    fn plane_with_media(advertised: IpAddr, ports: Arc<MediaPortAllocator>) -> TapPlane {
        TapPlane::new(TapPlaneConfig {
            default_node: None,
            local_media_address: IpAddr::from([127, 0, 0, 1]),
            advertised_media_address: advertised,
            media_ports: ports,
            format: AudioFormat::pcmu_8k_20ms(),
            transcode_at_tap: true,
            opus_decode_rate_hz: 16000,
            cookie_prefix: 1,
            sdp_session_id: 1,
            recording: RecordingSupport::default(),
            capabilities: Arc::new(NodeCapabilityLog::new(true)),
        })
    }

    fn session(kind: SessionKind, node: &str) -> SessionView {
        SessionView {
            id: SessionId::from_raw(1),
            external_id: "req-1".to_string(),
            kind,
            call_id: "call-abc".to_string(),
            from_tags: vec!["from-a".to_string()],
            attribution: Attribution::Explicit,
            rtpengine_node: node.to_string(),
            sdp_offer: None,
            sdp_answer: None,
            group: String::new(),
            attachments: Vec::new(),
            authoritative: None,
        }
    }

    fn plane_with_default_node(node: &str) -> TapPlane {
        TapPlane::new(TapPlaneConfig {
            default_node: Some(node.parse().expect("a default node")),
            local_media_address: IpAddr::from([127, 0, 0, 1]),
            advertised_media_address: IpAddr::from([127, 0, 0, 1]),
            media_ports: MediaPortAllocator::ephemeral(),
            format: AudioFormat::pcmu_8k_20ms(),
            transcode_at_tap: true,
            opus_decode_rate_hz: 16000,
            cookie_prefix: 1,
            sdp_session_id: 1,
            recording: RecordingSupport::default(),
            capabilities: Arc::new(NodeCapabilityLog::new(true)),
        })
    }

    fn mapped(
        plane: &TapPlane,
        map: crate::discovery::FixedNodeMap,
    ) -> Arc<crate::discovery::DiscoveryCounters> {
        let counters = Arc::new(crate::discovery::DiscoveryCounters::default());
        plane.discover_through(Arc::new(crate::discovery::NodeDiscovery::new(
            Arc::new(map),
            "mss:call-node:".to_string(),
            Arc::clone(&counters),
        )));
        counters
    }

    fn tap_without_a_node() -> SessionView {
        SessionView {
            from_tags: Vec::new(),
            attribution: Attribution::Unknown,
            ..session(SessionKind::Tap, "")
        }
    }

    #[tokio::test]
    async fn a_mapped_call_goes_to_the_mapped_node_not_the_default_one() {
        let plane = plane_with_default_node("10.0.0.9:22222");
        let counters = mapped(
            &plane,
            crate::discovery::FixedNodeMap::holding(
                r#"{"node":"10.0.0.5:22222","caller_tag":"caller-tag"}"#,
            ),
        );
        let resolved = plane
            .resolve_node(tap_without_a_node())
            .await
            .expect("the map answers");
        assert_eq!(resolved.node.to_string(), "10.0.0.5:22222");
        assert_eq!(resolved.view.from_tags, vec!["caller-tag".to_string()]);
        assert!(resolved.caller_named);
        assert_eq!(counters.hits.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn an_unmapped_call_falls_back_to_the_default_node() {
        let plane = plane_with_default_node("10.0.0.9:22222");
        let counters = mapped(&plane, crate::discovery::FixedNodeMap::empty());
        let resolved = plane
            .resolve_node(tap_without_a_node())
            .await
            .expect("the default node answers");
        assert_eq!(resolved.node.to_string(), "10.0.0.9:22222");
        assert!(resolved.view.from_tags.is_empty());
        assert!(!resolved.caller_named);
        assert_eq!(
            counters.misses.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn an_unreachable_map_never_fails_the_session() {
        let plane = plane_with_default_node("10.0.0.9:22222");
        let counters = mapped(&plane, crate::discovery::FixedNodeMap::unreachable());
        let resolved = plane
            .resolve_node(tap_without_a_node())
            .await
            .expect("a broken map is not fatal");
        assert_eq!(resolved.node.to_string(), "10.0.0.9:22222");
        assert_eq!(
            counters.errors.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn a_caller_that_names_its_node_is_never_looked_up() {
        let plane = plane_with_default_node("10.0.0.9:22222");
        let counters = mapped(
            &plane,
            crate::discovery::FixedNodeMap::holding("10.0.0.5:22222"),
        );
        let resolved = plane
            .resolve_node(session(SessionKind::Tap, "10.0.0.7:22222"))
            .await
            .expect("the named node stands");
        assert_eq!(resolved.node.to_string(), "10.0.0.7:22222");
        assert!(resolved.caller_named);
        assert_eq!(counters.hits.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(
            counters.misses.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn the_map_never_overrides_from_tags_the_caller_gave() {
        let plane = plane_with_default_node("10.0.0.9:22222");
        mapped(
            &plane,
            crate::discovery::FixedNodeMap::holding(
                r#"{"node":"10.0.0.5:22222","caller_tag":"mapped-tag"}"#,
            ),
        );
        let resolved = plane
            .resolve_node(session(SessionKind::Tap, ""))
            .await
            .expect("the map answers");
        assert_eq!(resolved.node.to_string(), "10.0.0.5:22222");
        assert_eq!(resolved.view.from_tags, vec!["from-a".to_string()]);
        assert!(resolved.caller_named);
    }

    #[tokio::test]
    async fn tags_from_a_map_that_names_no_caller_claim_no_direction() {
        let plane = plane_with_default_node("10.0.0.9:22222");
        mapped(
            &plane,
            crate::discovery::FixedNodeMap::holding(
                r#"{"node":"10.0.0.5:22222","from_tags":["one","two","three"]}"#,
            ),
        );
        let resolved = plane
            .resolve_node(tap_without_a_node())
            .await
            .expect("the map answers");
        assert_eq!(
            resolved.view.from_tags,
            vec!["one".to_string(), "two".to_string()]
        );
        assert!(!resolved.caller_named);
    }

    fn inline_session(offer: &str) -> SessionView {
        SessionView {
            sdp_offer: Some(offer.to_string()),
            ..session(SessionKind::Inline, "")
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
            group: String::new(),
            metadata: BTreeMap::new(),
        }
    }

    fn recording_support() -> RecordingSupport {
        RecordingSupport {
            sink: Some(Arc::new(NowhereSink)),
            spill_dir: None,
            journal: None,
            spill_every: recorder::SPILL_EVERY,
            counters: Arc::new(RecorderCounters::default()),
            owner: "pod-a".to_string(),
            upload_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
                recorder::DEFAULT_UPLOAD_CONCURRENCY,
            )),
        }
    }

    fn identity(endpoint: &str) -> RecordingIdentity {
        RecordingIdentity::parse(endpoint).unwrap()
    }

    #[tokio::test]
    async fn cancelling_a_dead_pods_tap_puts_an_unsubscribe_on_the_wire() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let node = socket.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            while let Ok((len, from)) = socket.recv_from(&mut buf).await {
                let datagram = buf[..len].to_vec();
                let (cookie, _) = rtpengine_ng::NgClient::split_cookie(&datagram).unwrap();
                let mut reply = cookie.to_vec();
                reply.extend_from_slice(b" d6:result2:oke");
                log.lock().unwrap().push(datagram);
                let _ = socket.send_to(&reply, from).await;
            }
        });

        let plane = plane();
        plane
            .unsubscribe_orphan(&node.to_string(), "call-abc", "tap-of-a-dead-pod")
            .await
            .unwrap();

        let sent = seen.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let wire = String::from_utf8_lossy(&sent[0]);
        assert!(wire.contains("7:command11:unsubscribe"), "{wire}");
        assert!(wire.contains("7:call-id8:call-abc"), "{wire}");
        assert!(wire.contains("6:to-tag17:tap-of-a-dead-pod"), "{wire}");
    }

    #[tokio::test]
    async fn cancelling_a_tap_on_an_unparseable_node_is_refused_without_a_socket() {
        let plane = plane();
        let error = plane
            .unsubscribe_orphan("not-an-address", "call-abc", "tap-a")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ip:port"));
    }

    #[tokio::test]
    async fn a_session_this_pod_never_tapped_has_no_subscription_tag() {
        let plane = plane();
        assert!(plane.subscription_tag(SessionId::from_raw(1)).is_none());
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

    fn inline_offer_sdp(peer_port: u16) -> String {
        format!(
            "v=0\r\no=peer 1 1 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\n\
m=audio {peer_port} RTP/AVP 0 101\r\na=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\na=ptime:20\r\n"
        )
    }

    fn tone_datagram(sequence: u16, sample: i16) -> Vec<u8> {
        let payload: Vec<u8> = (0..160)
            .map(|_| media_core::g711::linear_to_ulaw(sample))
            .collect();
        let packet = media_core::rtp::RtpPacket {
            marker: sequence == 0,
            payload_type: 0,
            sequence,
            timestamp: u32::from(sequence) * 160,
            ssrc: 0x0bad_cafe,
            payload: &payload,
        };
        let mut datagram = vec![0u8; 12 + payload.len()];
        let written = packet.serialize(&mut datagram).expect("an rtp datagram");
        datagram.truncate(written);
        datagram
    }

    #[tokio::test]
    async fn an_inline_leg_hears_its_peer_and_paces_queued_audio_back_to_it() {
        let peer = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        peer.set_nonblocking(true).expect("nonblocking peer");
        let peer_port = peer.local_addr().expect("peer address").port();
        let plane = plane();
        let session = SessionId::from_raw(1);

        let opened = plane
            .open_session(inline_session(&inline_offer_sdp(peer_port)))
            .await
            .expect("an inline leg answers a pcmu offer");
        let answer = opened
            .sdp_answer
            .expect("an inline session answers with sdp");
        let answered = InlineOffer::parse(&answer, 20).expect("our own answer is valid sdp");
        assert_eq!(answered.codec.payload_type, 0);
        assert_eq!(
            answered.telephone_event.map(|event| event.payload_type),
            Some(101)
        );
        let ours = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), answered.peer_port);

        let SessionHandles { transport, hub, .. } = plane
            .session_handles(session)
            .expect("the inline session is live here");
        assert!(
            transport.is_none(),
            "an inline leg holds no rtpengine subscription"
        );
        let mut subscription = hub
            .attach(64, TrackSelection::All)
            .expect("the hub takes a consumer");

        for sequence in 0..8u16 {
            peer.send_to(&tone_datagram(sequence, 4_000), ours)
                .expect("the peer can reach the inline leg");
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut heard = 0;
        let mut loudest = 0i16;
        while let Some(event) = subscription.try_next() {
            if let TapEvent::Media {
                track,
                len,
                samples,
                ..
            } = event
            {
                assert_ne!(track, Track::Agent, "an inline leg has one leg, not two");
                if track == Track::Customer && len > 0 {
                    heard += 1;
                    loudest = loudest.max(samples[..len].iter().copied().max().unwrap_or(0));
                }
            }
        }
        assert!(heard >= 4, "the hub saw {heard} frames from the peer");
        assert!(
            loudest > 3_000,
            "the peer's tone reached the hub: {loudest}"
        );

        let pcm: Vec<i16> = (0..1_600)
            .map(|index| ((index % 40) * 200) as i16)
            .collect();
        let wav =
            crate::tap_spike::wav_blob(AudioFormat::pcmu_8k_20ms(), &pcm).expect("a playback wav");
        plane
            .start_playback(
                session,
                session_core::PlaybackId::from_raw(3),
                PlaybackSource::Blob(wav),
                None,
                false,
            )
            .await
            .expect("a wav queues into the inline egress");
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut datagrams = 0;
        let mut sequences = Vec::new();
        let mut payload_types = Vec::new();
        let mut buf = [0u8; 2048];
        while let Ok((len, from)) = peer.recv_from(&mut buf) {
            assert_eq!(
                from, ours,
                "the inline leg sends from the port it answered on"
            );
            let packet = media_core::rtp::RtpPacket::parse(&buf[..len])
                .expect("the peer receives parsable rtp");
            datagrams += 1;
            sequences.push(packet.sequence);
            payload_types.push(packet.payload_type);
            assert_eq!(packet.payload.len(), 160);
        }
        assert!(datagrams >= 8, "the peer heard {datagrams} paced datagrams");
        assert!(
            payload_types.iter().all(|payload_type| *payload_type == 0),
            "every egress packet is pcmu: {payload_types:?}"
        );
        for pair in sequences.windows(2) {
            assert_eq!(
                pair[1],
                pair[0].wrapping_add(1),
                "the egress sequence never skips: {sequences:?}"
            );
        }

        plane
            .close_session(session)
            .await
            .expect("the inline leg closes");
    }

    #[derive(Default)]
    struct WitnessedObservations(Mutex<Vec<(SessionId, Observation)>>);

    impl WitnessedObservations {
        fn digits(&self) -> Vec<Observation> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, seen)| matches!(seen, Observation::Dtmf { .. }))
                .map(|(_, seen)| seen.clone())
                .collect()
        }
    }

    impl ObservationSink for WitnessedObservations {
        fn observe(&self, session: SessionId, observation: Observation) {
            self.0.lock().unwrap().push((session, observation));
        }
    }

    fn digit_datagram(sequence: u16, timestamp: u32, event: u8, end: bool, ticks: u16) -> Vec<u8> {
        let ticks = ticks.to_be_bytes();
        let payload = [event, if end { 0x8A } else { 0x0A }, ticks[0], ticks[1]];
        let packet = media_core::rtp::RtpPacket {
            marker: false,
            payload_type: 101,
            sequence,
            timestamp,
            ssrc: 0x0bad_cafe,
            payload: &payload,
        };
        let mut datagram = vec![0u8; 12 + payload.len()];
        let written = packet.serialize(&mut datagram).expect("an rtp datagram");
        datagram.truncate(written);
        datagram
    }

    #[tokio::test]
    async fn a_digit_pressed_on_a_leg_reaches_the_event_bus_once_with_no_consumer_attached() {
        let peer = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        peer.set_nonblocking(true).expect("nonblocking peer");
        let peer_port = peer.local_addr().expect("peer address").port();
        let plane = plane();
        let witness = Arc::new(WitnessedObservations::default());
        plane.observe_through(Arc::downgrade(&witness) as Weak<dyn ObservationSink>);
        let session = SessionId::from_raw(1);

        let opened = plane
            .open_session(inline_session(&inline_offer_sdp(peer_port)))
            .await
            .expect("an inline leg answers a pcmu offer");
        let answered = InlineOffer::parse(
            &opened
                .sdp_answer
                .expect("an inline session answers with sdp"),
            20,
        )
        .expect("our own answer is valid sdp");
        let ours = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), answered.peer_port);

        peer.send_to(&tone_datagram(0, 4_000), ours)
            .expect("the peer can reach the inline leg");
        peer.send_to(&digit_datagram(1, 160, 1, false, 400), ours)
            .expect("a digit begins");
        for sequence in 2..5u16 {
            peer.send_to(&digit_datagram(sequence, 160, 1, true, 800), ours)
                .expect("rfc 4733 repeats the end packet three times");
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(
            witness.digits(),
            vec![Observation::Dtmf {
                track: Track::Customer,
                digit: '1',
                duration_ms: 100,
                rtp_timestamp: 160,
            }],
            "one press is one event, whatever the end packet is repeated"
        );

        plane
            .close_session(session)
            .await
            .expect("the inline leg closes");
    }

    fn inline_offer_with_ptime(peer_port: u16, ptime_ms: u32) -> String {
        format!(
            "v=0\r\no=peer 1 1 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\n\
m=audio {peer_port} RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\na=ptime:{ptime_ms}\r\n"
        )
    }

    fn conference_session(id: u64, offer: &str, group: &str) -> SessionView {
        SessionView {
            id: SessionId::from_raw(id),
            external_id: format!("req-{id}"),
            sdp_offer: Some(offer.to_string()),
            group: group.to_string(),
            ..session(SessionKind::Inline, "")
        }
    }

    struct FakePeer {
        socket: UdpSocket,
        leg: SocketAddr,
        session: SessionId,
        sequence: u16,
    }

    impl FakePeer {
        fn speak(&mut self, level: i16, packets: u16) {
            for _ in 0..packets {
                let datagram = tone_datagram(self.sequence, level);
                self.socket
                    .send_to(&datagram, self.leg)
                    .expect("the peer can reach its conference leg");
                self.sequence = self.sequence.wrapping_add(1);
            }
        }

        fn loudest_ear(&self) -> i16 {
            let mut buf = [0u8; 2048];
            let mut loudest = 0i16;
            while let Ok((len, _)) = self.socket.recv_from(&mut buf) {
                let Ok(packet) = media_core::rtp::RtpPacket::parse(&buf[..len]) else {
                    continue;
                };
                for byte in packet.payload {
                    loudest = loudest.max(media_core::g711::ulaw_to_linear(*byte));
                }
            }
            loudest
        }
    }

    async fn seat_peer(plane: &TapPlane, id: u64, group: &str) -> FakePeer {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        socket.set_nonblocking(true).expect("nonblocking peer");
        let peer_port = socket.local_addr().expect("peer address").port();
        let opened = plane
            .open_session(conference_session(id, &inline_offer_sdp(peer_port), group))
            .await
            .expect("a conference leg answers a pcmu offer");
        let answer = opened
            .sdp_answer
            .expect("a conference leg answers with sdp");
        let answered = InlineOffer::parse(&answer, 20).expect("our own answer is valid sdp");
        FakePeer {
            socket,
            leg: SocketAddr::new(IpAddr::from([127, 0, 0, 1]), answered.peer_port),
            session: SessionId::from_raw(id),
            sequence: 0,
        }
    }

    #[tokio::test]
    async fn three_conference_legs_each_hear_the_other_two_and_never_themselves() {
        let plane = plane();
        let mut alice = seat_peer(&plane, 1, "sales-standup").await;
        let mut bob = seat_peer(&plane, 2, "sales-standup").await;
        let mut carol = seat_peer(&plane, 3, "sales-standup").await;
        assert_eq!(
            plane.metrics().snapshot().conferences_live,
            1,
            "three legs naming one group share one mix"
        );

        let SessionHandles { hub, .. } = plane
            .session_handles(alice.session)
            .expect("the conference leg is live here");
        let mut monitor = hub
            .attach(512, TrackSelection::All)
            .expect("the hub takes a monitor");

        for _ in 0..10 {
            alice.speak(1_000, 4);
            bob.speak(2_000, 4);
            carol.speak(4_000, 4);
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        let alice_ear = alice.loudest_ear();
        let bob_ear = bob.loudest_ear();
        let carol_ear = carol.loudest_ear();
        assert!(
            (5_600..=6_400).contains(&alice_ear),
            "alice hears bob plus carol and not her own 1000: {alice_ear}"
        );
        assert!(
            (4_600..=5_400).contains(&bob_ear),
            "bob hears alice plus carol and not his own 2000: {bob_ear}"
        );
        assert!(
            (2_700..=3_300).contains(&carol_ear),
            "carol hears alice plus bob and not her own 4000: {carol_ear}"
        );

        let mut mixed_frames = 0;
        let mut loudest_mixed = 0i16;
        while let Some(event) = monitor.try_next() {
            if let TapEvent::Media {
                track: Track::Mixed,
                len,
                samples,
                ..
            } = event
            {
                mixed_frames += 1;
                loudest_mixed =
                    loudest_mixed.max(samples[..len].iter().copied().max().unwrap_or(0));
            }
        }
        assert!(
            mixed_frames >= 10,
            "the hub carries the conference mix: {mixed_frames} frames"
        );
        assert!(
            (6_500..=7_500).contains(&loudest_mixed),
            "the mixed track carries all three legs, self included: {loudest_mixed}"
        );

        for peer in [&alice, &bob, &carol] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
        assert_eq!(
            plane.metrics().snapshot().conferences_live,
            0,
            "the last leg out closes the conference"
        );
        assert_eq!(plane.live_sessions(), 0);
    }

    fn injecting_attachment(
        session: SessionId,
        id: u64,
        metadata: &[(&str, &str)],
    ) -> AttachmentView {
        AttachmentView {
            id: AttachmentId::from_raw(id),
            session,
            capabilities: Capabilities::SINK | Capabilities::INJECT,
            metadata: metadata
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            ..attachment(Transport::GrpcStream, "grpc-consumer")
        }
    }

    fn inject(plane: &TapPlane, session: SessionId, level: i16, chunks: usize) {
        let egress = plane
            .inline_egress(session)
            .expect("a conference leg has an egress to inject into");
        for _ in 0..chunks {
            assert!(
                egress.push(vec![level; 800]),
                "the egress queue takes the injected audio"
            );
        }
    }

    fn loudest_mixed(monitor: &mut Subscription) -> i16 {
        let mut loudest = 0i16;
        while let Some(event) = monitor.try_next() {
            if let TapEvent::Media {
                track: Track::Mixed,
                len,
                samples,
                ..
            } = event
            {
                loudest = loudest.max(samples[..len].iter().copied().max().unwrap_or(0));
            }
        }
        loudest
    }

    fn recording_attachment(
        id: u64,
        session: SessionId,
        endpoint: &str,
        selector: TrackSelector,
        group: &str,
        label: &str,
    ) -> AttachmentView {
        AttachmentView {
            id: AttachmentId::from_raw(id),
            session,
            selector,
            group: group.to_string(),
            label: label.to_string(),
            ..attachment(Transport::FileS3, endpoint)
        }
    }

    fn bucket_plane(bucket: &Arc<BucketSink>) -> TapPlane {
        plane_with_recording(RecordingSupport {
            sink: Some(Arc::clone(bucket) as Arc<dyn recorder::RecordingSink>),
            journal: None,
            spill_dir: None,
            spill_every: recorder::SPILL_EVERY,
            counters: Arc::new(RecorderCounters::default()),
            owner: "pod-a".to_string(),
            upload_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
                recorder::DEFAULT_UPLOAD_CONCURRENCY,
            )),
        })
    }

    #[tokio::test]
    async fn one_conference_records_the_whole_room_and_every_participant_at_once() {
        let bucket = Arc::new(BucketSink::default());
        let plane = bucket_plane(&bucket);
        let mut alice = seat_peer(&plane, 1, "board-room").await;
        let mut bob = seat_peer(&plane, 2, "board-room").await;

        plane
            .open_attachment(recording_attachment(
                11,
                alice.session,
                "acct-7/room.wav",
                TrackSelector::Only(Track::Mixed),
                "",
                "room",
            ))
            .await
            .expect("the mixed track of any member records the whole room as one object");
        for (id, session, label) in [(12u64, alice.session, "alice"), (13, bob.session, "bob")] {
            plane
                .open_attachment(recording_attachment(
                    id,
                    session,
                    "acct-7/parties.wav",
                    TrackSelector::Only(Track::Customer),
                    "parties",
                    label,
                ))
                .await
                .expect("a recording group over conference members records one object each");
        }

        for _ in 0..6 {
            alice.speak(1_000, 4);
            bob.speak(2_000, 4);
            tokio::time::sleep(Duration::from_millis(80)).await;
        }

        let mut carol = seat_peer(&plane, 3, "board-room").await;
        plane
            .open_attachment(recording_attachment(
                14,
                carol.session,
                "acct-7/parties.wav",
                TrackSelector::Only(Track::Customer),
                "parties",
                "carol",
            ))
            .await
            .expect("a member that joins late joins the recording group late");

        for _ in 0..6 {
            alice.speak(1_000, 4);
            bob.speak(2_000, 4);
            carol.speak(4_000, 4);
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        for (session, id) in [
            (alice.session, 11u64),
            (alice.session, 12),
            (bob.session, 13),
            (carol.session, 14),
        ] {
            plane
                .close_attachment(session, AttachmentId::from_raw(id))
                .await
                .expect("a recording attachment closes and uploads");
        }
        plane.await_uploads().await;

        let mut keys = bucket.keys();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "acct-7/parties/alice.wav".to_string(),
                "acct-7/parties/bob.wav".to_string(),
                "acct-7/parties/carol.wav".to_string(),
                "acct-7/room.wav".to_string(),
            ],
            "both shapes coexist: one room object plus one object per participant"
        );

        let (room_channels, room) = recorded_wav(&bucket.body("acct-7/room.wav"));
        assert_eq!(room_channels, 1, "the room records as one mono mix");
        let room_loudest = room.iter().copied().max().unwrap_or(0);
        assert!(
            (6_500..=7_600).contains(&room_loudest),
            "the room object carries every party summed: {room_loudest}"
        );

        let mut lengths = Vec::new();
        for (label, level) in [("alice", 1_000i16), ("bob", 2_000), ("carol", 4_000)] {
            let (channels, samples) =
                recorded_wav(&bucket.body(&format!("acct-7/parties/{label}.wav")));
            assert_eq!(channels, 1, "a participant object is mono");
            let loudest = samples.iter().copied().max().unwrap_or(0);
            assert!(
                (level - 200..=level + 200).contains(&loudest),
                "{label} records only {label}, not the room: {loudest}"
            );
            lengths.push((label, samples.len()));
        }

        let alice_frames = lengths[0].1;
        let carol_frames = lengths[2].1;
        let slack = 8_000usize / 2;
        assert!(
            carol_frames + slack >= alice_frames && carol_frames <= alice_frames + slack,
            "the group anchor pads a late member back to t=0: {lengths:?}"
        );
        let (_, carol_samples) = recorded_wav(&bucket.body("acct-7/parties/carol.wav"));
        let lead = carol_samples
            .iter()
            .position(|sample| *sample != 0)
            .unwrap_or(carol_samples.len());
        assert!(
            lead >= 8_000 / 4,
            "carol joined a quarter second or more after the group opened: {lead} samples"
        );

        for peer in [&alice, &bob, &carol] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
    }

    #[tokio::test]
    async fn a_late_members_own_track_and_the_room_share_one_clock_in_a_stereo_object() {
        let bucket = Arc::new(BucketSink::default());
        let plane = bucket_plane(&bucket);
        let mut alice = seat_peer(&plane, 1, "one-clock").await;

        for _ in 0..8 {
            alice.speak(1_000, 4);
            tokio::time::sleep(Duration::from_millis(80)).await;
        }

        let mut bob = seat_peer(&plane, 2, "one-clock").await;
        plane
            .open_attachment(recording_attachment(
                11,
                bob.session,
                "acct-7/late.wav",
                TrackSelector::All,
                "",
                "late",
            ))
            .await
            .expect("a conference member records itself and the room in stereo");

        for _ in 0..8 {
            alice.speak(1_000, 4);
            bob.speak(4_000, 4);
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        plane
            .close_attachment(bob.session, AttachmentId::from_raw(11))
            .await
            .expect("the stereo object uploads");
        plane.await_uploads().await;

        let (channels, samples) = recorded_wav(&bucket.body("acct-7/late.wav"));
        assert_eq!(channels, 2);
        let lead = |offset: usize| {
            samples
                .iter()
                .skip(offset)
                .step_by(2)
                .position(|sample| *sample != 0)
                .unwrap_or(samples.len())
        };
        let own = lead(0);
        let room = lead(1);
        assert!(
            own < 8_000,
            "the member's own track is in the object: {own}"
        );
        assert!(room < 8_000, "the room is in the object: {room}");
        assert!(
            own.abs_diff(room) <= 4 * 160,
            "a member that joined the conference late still hears the mix on its own \
             clock: own track opens at {own}, the room at {room}"
        );

        for peer in [&alice, &bob] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
    }

    #[tokio::test]
    async fn a_recording_group_of_the_mixed_track_is_refused_on_a_conference_by_name() {
        let bucket = Arc::new(BucketSink::default());
        let plane = bucket_plane(&bucket);
        let alice = seat_peer(&plane, 1, "duplicated").await;
        let error = plane
            .open_attachment(recording_attachment(
                11,
                alice.session,
                "acct-7/room.wav",
                TrackSelector::Only(Track::Mixed),
                "parties",
                "alice",
            ))
            .await
            .expect_err("one object per member of the same room is the same audio N times");
        assert!(
            error.to_string().contains("record the room once"),
            "{error}"
        );
        assert!(bucket.keys().is_empty());
        plane
            .close_session(alice.session)
            .await
            .expect("a conference leg closes");
    }

    const WHISPER_LEVEL: i16 = 8_000;
    const HEARD: std::ops::RangeInclusive<i16> = 7_000..=9_500;
    const SILENT: i16 = 300;

    #[tokio::test]
    async fn a_monitor_attachment_wants_the_mixed_track_only_and_hears_the_whole_conference() {
        let plane = plane();
        let mut alice = seat_peer(&plane, 1, "monitored").await;
        let mut bob = seat_peer(&plane, 2, "monitored").await;
        let mut carol = seat_peer(&plane, 3, "monitored").await;

        let mut supervisor = injecting_attachment(alice.session, 7, &[]);
        supervisor.capabilities = Capabilities::SINK;
        supervisor.selector = TrackSelector::Only(Track::Mixed);
        plane
            .open_attachment(supervisor)
            .await
            .expect("a monitor is a consumer that selects the mixed track and injects nothing");

        let SessionHandles { hub, .. } = plane
            .session_handles(alice.session)
            .expect("the conference leg is live here");
        let mut monitor = hub
            .attach(512, TrackSelection::Only(Track::Mixed))
            .expect("the hub takes a monitor");
        for _ in 0..10 {
            alice.speak(1_000, 4);
            bob.speak(2_000, 4);
            carol.speak(4_000, 4);
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mixed = loudest_mixed(&mut monitor);
        assert!(
            (6_500..=7_500).contains(&mixed),
            "a monitor hears every member, itself included: {mixed}"
        );

        for peer in [&alice, &bob, &carol] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
    }

    #[tokio::test]
    async fn a_whisper_reaches_its_target_and_the_mixed_track_and_nobody_else() {
        let plane = plane();
        let alice = seat_peer(&plane, 1, "supervision").await;
        let bob = seat_peer(&plane, 2, "supervision").await;
        let carol = seat_peer(&plane, 3, "supervision").await;
        let SessionHandles { hub, .. } = plane
            .session_handles(bob.session)
            .expect("the conference leg is live here");
        let mut monitor = hub
            .attach(512, TrackSelection::Only(Track::Mixed))
            .expect("the hub takes a monitor that wants the mix only");

        plane
            .open_attachment(injecting_attachment(
                carol.session,
                7,
                &[(MIX_TARGET_METADATA_KEY, "req-2")],
            ))
            .await
            .expect("an injecting attachment may name another member as its target");
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            plane.metrics().snapshot().conference_whispers_live,
            1,
            "one leg is whispering"
        );

        for peer in [&alice, &bob, &carol] {
            peer.loudest_ear();
        }
        let _ = loudest_mixed(&mut monitor);
        inject(&plane, carol.session, WHISPER_LEVEL, 4);
        tokio::time::sleep(Duration::from_millis(700)).await;

        let bob_ear = bob.loudest_ear();
        let alice_ear = alice.loudest_ear();
        let carol_ear = carol.loudest_ear();
        let mixed = loudest_mixed(&mut monitor);
        assert!(
            HEARD.contains(&bob_ear),
            "the named member hears the whisper: {bob_ear}"
        );
        assert!(
            alice_ear < SILENT,
            "nobody else in the conference hears it: alice {alice_ear}"
        );
        assert!(
            carol_ear < SILENT,
            "not even the leg it was injected on hears it: carol {carol_ear}"
        );
        assert!(
            HEARD.contains(&mixed),
            "the mixed track is the record of what was said in this conference: {mixed}"
        );

        plane
            .update_attachment(injecting_attachment(
                carol.session,
                7,
                &[
                    (MIX_TARGET_METADATA_KEY, "req-2"),
                    (MIX_MONITOR_METADATA_KEY, MIX_MONITOR_EXCLUDE),
                ],
            ))
            .await
            .expect("mix_monitor takes the whisper off the mixed track");
        for peer in [&alice, &bob, &carol] {
            peer.loudest_ear();
        }
        let _ = loudest_mixed(&mut monitor);
        inject(&plane, carol.session, WHISPER_LEVEL, 4);
        tokio::time::sleep(Duration::from_millis(700)).await;
        let bob_ear = bob.loudest_ear();
        let mixed = loudest_mixed(&mut monitor);
        assert!(
            HEARD.contains(&bob_ear),
            "the target still hears the whisper: {bob_ear}"
        );
        assert!(
            mixed < SILENT,
            "and the mixed track no longer carries it: {mixed}"
        );

        plane
            .close_attachment(carol.session, AttachmentId::from_raw(7))
            .await
            .expect("the whisperer detaches");
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            plane.metrics().snapshot().conference_whispers_live,
            0,
            "the route went back to private when the whisperer detached"
        );
        for peer in [&alice, &bob, &carol] {
            peer.loudest_ear();
        }
        inject(&plane, carol.session, WHISPER_LEVEL, 4);
        tokio::time::sleep(Duration::from_millis(700)).await;
        let carol_ear = carol.loudest_ear();
        let bob_ear = bob.loudest_ear();
        assert!(
            HEARD.contains(&carol_ear),
            "injected audio is private playback again: carol {carol_ear}"
        );
        assert!(
            bob_ear < SILENT,
            "and the whisper target hears nothing more: bob {bob_ear}"
        );

        for peer in [&alice, &bob, &carol] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
    }

    #[tokio::test]
    async fn flipping_the_target_to_all_turns_a_whisper_into_a_barge() {
        let plane = plane();
        let alice = seat_peer(&plane, 1, "barge").await;
        let bob = seat_peer(&plane, 2, "barge").await;
        let carol = seat_peer(&plane, 3, "barge").await;
        plane
            .open_attachment(injecting_attachment(
                carol.session,
                7,
                &[(MIX_TARGET_METADATA_KEY, "req-2")],
            ))
            .await
            .expect("the whisper is attached");
        plane
            .update_attachment(injecting_attachment(
                carol.session,
                7,
                &[(MIX_TARGET_METADATA_KEY, MIX_TARGET_EVERYONE)],
            ))
            .await
            .expect("update-attachment metadata is the barge verb");

        for peer in [&alice, &bob, &carol] {
            peer.loudest_ear();
        }
        inject(&plane, carol.session, WHISPER_LEVEL, 4);
        tokio::time::sleep(Duration::from_millis(700)).await;
        for (name, ear) in [
            ("alice", alice.loudest_ear()),
            ("bob", bob.loudest_ear()),
            ("carol", carol.loudest_ear()),
        ] {
            assert!(
                HEARD.contains(&ear),
                "a barge is heard by every listener: {name} {ear}"
            );
        }
        assert!(plane.metrics().snapshot().conference.route_changes >= 2);

        for peer in [&alice, &bob, &carol] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
    }

    #[tokio::test]
    async fn a_whisper_waits_for_a_target_that_joins_later_and_survives_the_join() {
        let plane = plane();
        let alice = seat_peer(&plane, 1, "handover").await;
        let bob = seat_peer(&plane, 2, "handover").await;
        let carol = seat_peer(&plane, 3, "handover").await;
        plane
            .open_attachment(injecting_attachment(
                carol.session,
                7,
                &[(MIX_TARGET_METADATA_KEY, "req-4")],
            ))
            .await
            .expect("a whisper may name a member who has not joined yet");

        for peer in [&alice, &bob, &carol] {
            peer.loudest_ear();
        }
        inject(&plane, carol.session, WHISPER_LEVEL, 4);
        tokio::time::sleep(Duration::from_millis(700)).await;
        for (name, ear) in [
            ("alice", alice.loudest_ear()),
            ("bob", bob.loudest_ear()),
            ("carol", carol.loudest_ear()),
        ] {
            assert!(
                ear < SILENT,
                "a whisper to nobody is inaudible, not broadcast: {name} {ear}"
            );
        }

        let dave = seat_peer(&plane, 4, "handover").await;
        for peer in [&alice, &bob, &carol, &dave] {
            peer.loudest_ear();
        }
        inject(&plane, carol.session, WHISPER_LEVEL, 4);
        tokio::time::sleep(Duration::from_millis(700)).await;
        let dave_ear = dave.loudest_ear();
        assert!(
            HEARD.contains(&dave_ear),
            "the route resolved when its target joined: dave {dave_ear}"
        );
        for (name, ear) in [
            ("alice", alice.loudest_ear()),
            ("bob", bob.loudest_ear()),
            ("carol", carol.loudest_ear()),
        ] {
            assert!(
                ear < SILENT,
                "and the join did not leak the whisper to anyone else: {name} {ear}"
            );
        }

        for peer in [&alice, &bob, &carol, &dave] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
    }

    #[tokio::test]
    async fn a_mix_target_is_refused_without_inject_and_off_a_conference() {
        let plane = plane();
        let alice = seat_peer(&plane, 1, "refusals").await;
        let mut listener =
            injecting_attachment(alice.session, 7, &[(MIX_TARGET_METADATA_KEY, "req-1")]);
        listener.capabilities = Capabilities::SINK;
        let refused = plane
            .open_attachment(listener)
            .await
            .expect_err("a sink-only attachment may not route injected audio");
        assert!(
            refused.to_string().contains("INJECT"),
            "refused by name: {refused}"
        );

        let socket = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        let port = socket.local_addr().expect("peer address").port();
        let two_party = SessionView {
            id: SessionId::from_raw(9),
            external_id: "req-9".to_string(),
            sdp_offer: Some(inline_offer_sdp(port)),
            ..session(SessionKind::Inline, "")
        };
        plane
            .open_session(two_party)
            .await
            .expect("a plain inline leg answers");
        let refused = plane
            .open_attachment(injecting_attachment(
                SessionId::from_raw(9),
                8,
                &[(MIX_TARGET_METADATA_KEY, "req-1")],
            ))
            .await
            .expect_err("a leg that is in no conference has no mix to route into");
        assert!(
            refused.to_string().contains("not a conference leg"),
            "refused by name: {refused}"
        );

        plane
            .close_session(SessionId::from_raw(9))
            .await
            .expect("the inline leg closes");
        plane
            .close_session(alice.session)
            .await
            .expect("the conference leg closes");
    }

    #[tokio::test]
    async fn a_leg_leaving_a_conference_does_not_disturb_the_legs_that_stay() {
        let plane = plane();
        let mut alice = seat_peer(&plane, 1, "handoff").await;
        let mut bob = seat_peer(&plane, 2, "handoff").await;
        let mut carol = seat_peer(&plane, 3, "handoff").await;

        for _ in 0..6 {
            alice.speak(1_000, 4);
            bob.speak(2_000, 4);
            carol.speak(4_000, 4);
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        let before = alice.loudest_ear();
        assert!(
            (5_600..=6_400).contains(&before),
            "alice hears both of the others first: {before}"
        );

        plane
            .close_session(carol.session)
            .await
            .expect("carol leaves the conference");
        tokio::time::sleep(Duration::from_millis(400)).await;
        let _ = alice.loudest_ear();
        let _ = bob.loudest_ear();

        for _ in 0..6 {
            alice.speak(1_000, 4);
            bob.speak(2_000, 4);
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;

        let alice_ear = alice.loudest_ear();
        let bob_ear = bob.loudest_ear();
        assert!(
            (1_700..=2_300).contains(&alice_ear),
            "alice hears bob alone once carol is gone: {alice_ear}"
        );
        assert!(
            (700..=1_300).contains(&bob_ear),
            "bob still hears alice, uninterrupted: {bob_ear}"
        );
        assert_eq!(
            plane.metrics().snapshot().conferences_live,
            1,
            "the mix keeps running for the survivors"
        );

        plane.close_session(alice.session).await.expect("alice out");
        plane.close_session(bob.session).await.expect("bob out");
        assert_eq!(plane.metrics().snapshot().conferences_live, 0);
    }

    fn member_attachment(session: SessionId, id: u64, metadata: &[(&str, &str)]) -> AttachmentView {
        let mut view = injecting_attachment(session, id, metadata);
        view.capabilities = Capabilities::SINK;
        view
    }

    async fn everybody_speaks(alice: &mut FakePeer, bob: &mut FakePeer, carol: &mut FakePeer) {
        for peer in [&*alice, &*bob, &*carol] {
            peer.loudest_ear();
        }
        for _ in 0..10 {
            alice.speak(1_000, 4);
            bob.speak(2_000, 4);
            carol.speak(4_000, 4);
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    fn about(level: i16, ear: i16) -> bool {
        let slack = (level / 10).max(200);
        (level - slack..=level + slack).contains(&ear)
    }

    #[tokio::test]
    async fn a_muted_member_is_heard_by_nobody_and_leaves_the_recording_feed_too() {
        let plane = plane();
        let mut alice = seat_peer(&plane, 1, "muting").await;
        let mut bob = seat_peer(&plane, 2, "muting").await;
        let mut carol = seat_peer(&plane, 3, "muting").await;
        let SessionHandles { hub, .. } = plane
            .session_handles(alice.session)
            .expect("the conference leg is live here");
        let mut monitor = hub
            .attach(512, TrackSelection::Only(Track::Mixed))
            .expect("the hub takes a monitor that wants the mix only");

        plane
            .open_attachment(member_attachment(
                bob.session,
                7,
                &[(MEMBER_MUTE_METADATA_KEY, MEMBER_FLAG_ON)],
            ))
            .await
            .expect("a member verb rides on an attachment of that member's own session");
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            plane.metrics().snapshot().conference_members_muted,
            1,
            "one member is muted"
        );

        let _ = loudest_mixed(&mut monitor);
        everybody_speaks(&mut alice, &mut bob, &mut carol).await;
        let alice_ear = alice.loudest_ear();
        let bob_ear = bob.loudest_ear();
        let carol_ear = carol.loudest_ear();
        let mixed = loudest_mixed(&mut monitor);
        assert!(
            about(4_000, alice_ear),
            "alice hears carol alone, because bob is muted: {alice_ear}"
        );
        assert!(
            about(1_000, carol_ear),
            "carol hears alice alone: {carol_ear}"
        );
        assert!(
            about(5_000, bob_ear),
            "a muted member still hears the room: {bob_ear}"
        );
        assert!(
            about(5_000, mixed),
            "and the mixed track, which is the recording feed, has lost him too: {mixed}"
        );

        plane
            .update_attachment(member_attachment(
                bob.session,
                7,
                &[(MEMBER_MUTE_METADATA_KEY, MEMBER_FLAG_OFF)],
            ))
            .await
            .expect("off gives the member their voice back");
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(plane.metrics().snapshot().conference_members_muted, 0);
        let _ = loudest_mixed(&mut monitor);
        everybody_speaks(&mut alice, &mut bob, &mut carol).await;
        let alice_ear = alice.loudest_ear();
        let mixed = loudest_mixed(&mut monitor);
        assert!(
            about(6_000, alice_ear),
            "alice hears bob and carol again: {alice_ear}"
        );
        assert!(
            about(7_000, mixed),
            "the whole room is on the record: {mixed}"
        );

        for peer in [&alice, &bob, &carol] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
    }

    #[tokio::test]
    async fn a_deaf_member_still_speaks_into_a_room_it_cannot_hear() {
        let plane = plane();
        let mut alice = seat_peer(&plane, 1, "deafness").await;
        let mut bob = seat_peer(&plane, 2, "deafness").await;
        let mut carol = seat_peer(&plane, 3, "deafness").await;
        let SessionHandles { hub, .. } = plane
            .session_handles(alice.session)
            .expect("the conference leg is live here");
        let mut monitor = hub
            .attach(512, TrackSelection::Only(Track::Mixed))
            .expect("the hub takes a monitor that wants the mix only");

        plane
            .open_attachment(member_attachment(
                carol.session,
                7,
                &[(MEMBER_DEAF_METADATA_KEY, MEMBER_FLAG_ON)],
            ))
            .await
            .expect("deaf is a member verb like mute");
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(plane.metrics().snapshot().conference_members_deaf, 1);
        assert_eq!(
            plane.metrics().snapshot().conference_members_muted,
            0,
            "deaf closes an ear, it does not close a mouth"
        );

        let _ = loudest_mixed(&mut monitor);
        everybody_speaks(&mut alice, &mut bob, &mut carol).await;
        let alice_ear = alice.loudest_ear();
        let carol_ear = carol.loudest_ear();
        let mixed = loudest_mixed(&mut monitor);
        assert!(
            carol_ear < SILENT,
            "a deafened member's ear is silent: {carol_ear}"
        );
        assert!(
            about(6_000, alice_ear),
            "and she is still heard by everybody else: {alice_ear}"
        );
        assert!(
            about(7_000, mixed),
            "the recording feed is unchanged by who is listening: {mixed}"
        );

        for peer in [&alice, &bob, &carol] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
    }

    #[tokio::test]
    async fn a_member_on_hold_hears_its_hold_audio_alone_and_the_room_loses_it() {
        let plane = plane();
        let mut alice = seat_peer(&plane, 1, "holding").await;
        let mut bob = seat_peer(&plane, 2, "holding").await;
        let mut carol = seat_peer(&plane, 3, "holding").await;
        let SessionHandles { hub, .. } = plane
            .session_handles(alice.session)
            .expect("the conference leg is live here");
        let mut monitor = hub
            .attach(512, TrackSelection::Only(Track::Mixed))
            .expect("the hub takes a monitor that wants the mix only");

        plane
            .open_attachment(member_attachment(
                bob.session,
                7,
                &[(MEMBER_HOLD_METADATA_KEY, MEMBER_FLAG_ON)],
            ))
            .await
            .expect("hold is mute and deaf in one verb");
        tokio::time::sleep(Duration::from_millis(60)).await;
        let snapshot = plane.metrics().snapshot();
        assert_eq!(snapshot.conference_members_held, 1);
        assert_eq!(snapshot.conference_members_muted, 1, "hold implies mute");
        assert_eq!(snapshot.conference_members_deaf, 1, "hold implies deaf");

        let hold_audio =
            crate::tap_spike::wav_blob(AudioFormat::pcmu_8k_20ms(), &[3_000i16; 16_000])
                .expect("two seconds of hold audio");
        plane
            .start_playback(
                bob.session,
                PlaybackId::from_raw(9),
                PlaybackSource::Blob(hold_audio),
                None,
                false,
            )
            .await
            .expect("hold audio is an ordinary playback into the held member's own ear");

        let _ = loudest_mixed(&mut monitor);
        everybody_speaks(&mut alice, &mut bob, &mut carol).await;
        let alice_ear = alice.loudest_ear();
        let bob_ear = bob.loudest_ear();
        let mixed = loudest_mixed(&mut monitor);
        assert!(
            about(3_000, bob_ear),
            "the held member hears his hold audio and nothing of the room: {bob_ear}"
        );
        assert!(
            about(4_000, alice_ear),
            "the room hears carol alone; the held member is gone from it: {alice_ear}"
        );
        assert!(
            about(5_000, mixed),
            "and the hold audio is nobody else's business, not even the record's: {mixed}"
        );

        for peer in [&alice, &bob, &carol] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
    }

    #[tokio::test]
    async fn a_prompt_played_into_the_room_is_heard_by_every_member_until_it_is_stopped() {
        let plane = plane();
        let alice = seat_peer(&plane, 1, "prompted").await;
        let bob = seat_peer(&plane, 2, "prompted").await;
        let carol = seat_peer(&plane, 3, "prompted").await;
        let SessionHandles { hub, .. } = plane
            .session_handles(bob.session)
            .expect("the conference leg is live here");
        let mut monitor = hub
            .attach(512, TrackSelection::Only(Track::Mixed))
            .expect("the hub takes a monitor that wants the mix only");

        let prompt = crate::tap_spike::wav_blob(AudioFormat::pcmu_8k_20ms(), &[3_000i16; 16_000])
            .expect("two seconds of prompt");
        for peer in [&alice, &bob, &carol] {
            peer.loudest_ear();
        }
        let _ = loudest_mixed(&mut monitor);
        plane
            .start_playback(
                alice.session,
                PlaybackId::from_raw(9),
                PlaybackSource::Blob(prompt),
                Some(MIX_TARGET_EVERYONE.to_string()),
                false,
            )
            .await
            .expect("a playback targeting all is a prompt into the room");
        tokio::time::sleep(Duration::from_millis(400)).await;

        let ears = [alice.loudest_ear(), bob.loudest_ear(), carol.loudest_ear()];
        let mixed = loudest_mixed(&mut monitor);
        for (peer, ear) in ["alice", "bob", "carol"].iter().zip(ears) {
            assert!(
                about(3_000, ear),
                "{peer} hears the room prompt, whoever it was played through: {ear}"
            );
        }
        assert!(
            about(3_000, mixed),
            "and the recording feed carries it: {mixed}"
        );
        assert!(
            plane.metrics().snapshot().conference.prompt_frames > 10,
            "the room prompt contributed frames to the mix"
        );

        plane
            .stop_playback(
                carol.session,
                PlaybackId::from_raw(9),
                Some(MIX_TARGET_EVERYONE.to_string()),
            )
            .await
            .expect("stopping a room prompt flushes what is left of it");
        tokio::time::sleep(Duration::from_millis(150)).await;
        for peer in [&alice, &bob, &carol] {
            peer.loudest_ear();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let ears = [alice.loudest_ear(), bob.loudest_ear(), carol.loudest_ear()];
        for (peer, ear) in ["alice", "bob", "carol"].iter().zip(ears) {
            assert!(ear < SILENT, "{peer}'s ear went quiet again: {ear}");
        }

        for peer in [&alice, &bob, &carol] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
    }

    #[tokio::test]
    async fn a_coach_can_have_its_own_voice_heard_by_one_member_alone() {
        let plane = plane();
        let mut coach = seat_peer(&plane, 1, "coaching").await;
        let mut agent = seat_peer(&plane, 2, "coaching").await;
        let mut customer = seat_peer(&plane, 3, "coaching").await;
        let SessionHandles { hub, .. } = plane
            .session_handles(agent.session)
            .expect("the conference leg is live here");
        let mut monitor = hub
            .attach(512, TrackSelection::Only(Track::Mixed))
            .expect("the hub takes a monitor that wants the mix only");

        plane
            .open_attachment(injecting_attachment(
                coach.session,
                7,
                &[
                    (MIX_TARGET_METADATA_KEY, "req-2"),
                    (MIX_SOURCE_METADATA_KEY, MIX_SOURCE_LEG),
                ],
            ))
            .await
            .expect("mix_source=leg routes the member's own rtp instead of injected audio");
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(plane.metrics().snapshot().conference_whispers_live, 1);

        let _ = loudest_mixed(&mut monitor);
        everybody_speaks(&mut coach, &mut agent, &mut customer).await;
        let coach_ear = coach.loudest_ear();
        let agent_ear = agent.loudest_ear();
        let customer_ear = customer.loudest_ear();
        let mixed = loudest_mixed(&mut monitor);
        assert!(
            about(5_000, agent_ear),
            "the agent hears the coach on top of the customer: {agent_ear}"
        );
        assert!(
            about(2_000, customer_ear),
            "the customer hears the agent alone, never the coach: {customer_ear}"
        );
        assert!(
            about(6_000, coach_ear),
            "the coach still hears the whole call: {coach_ear}"
        );
        assert!(
            about(7_000, mixed),
            "and coaching is on the record by default: {mixed}"
        );

        for peer in [&coach, &agent, &customer] {
            plane
                .close_session(peer.session)
                .await
                .expect("a conference leg closes");
        }
    }

    #[tokio::test]
    async fn member_verbs_are_refused_by_name_on_a_bad_flag_and_off_a_conference() {
        let plane = plane();
        let alice = seat_peer(&plane, 1, "member-refusals").await;
        let refused = plane
            .open_attachment(member_attachment(
                alice.session,
                7,
                &[(MEMBER_MUTE_METADATA_KEY, "maybe")],
            ))
            .await
            .expect_err("a member flag is on or off");
        assert!(
            refused.to_string().contains("member_mute"),
            "refused by name: {refused}"
        );

        let socket = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        let port = socket.local_addr().expect("peer address").port();
        plane
            .open_session(SessionView {
                id: SessionId::from_raw(9),
                external_id: "req-9".to_string(),
                sdp_offer: Some(inline_offer_sdp(port)),
                ..session(SessionKind::Inline, "")
            })
            .await
            .expect("a plain inline leg answers");
        let refused = plane
            .open_attachment(member_attachment(
                SessionId::from_raw(9),
                8,
                &[(MEMBER_HOLD_METADATA_KEY, MEMBER_FLAG_ON)],
            ))
            .await
            .expect_err("a two-party leg has no mix to be held out of");
        assert!(
            refused.to_string().contains("not a conference leg"),
            "refused by name: {refused}"
        );
        let refused = plane
            .start_playback(
                SessionId::from_raw(9),
                PlaybackId::from_raw(9),
                PlaybackSource::Blob(Vec::new()),
                Some("from-tag-7".to_string()),
                false,
            )
            .await
            .expect_err("an inline leg has no sip from-tag to play at");
        assert!(
            refused.to_string().contains("from-tag"),
            "refused by name: {refused}"
        );

        plane
            .close_session(SessionId::from_raw(9))
            .await
            .expect("the inline leg closes");
        plane
            .close_session(alice.session)
            .await
            .expect("the conference leg closes");
    }

    #[tokio::test]
    async fn audio_played_into_one_conference_leg_is_heard_by_that_leg_alone() {
        let plane = plane();
        let alice = seat_peer(&plane, 1, "prompt").await;
        let bob = seat_peer(&plane, 2, "prompt").await;

        let pcm = vec![3_000i16; 1_600];
        let wav =
            crate::tap_spike::wav_blob(AudioFormat::pcmu_8k_20ms(), &pcm).expect("a playback wav");
        plane
            .start_playback(
                alice.session,
                session_core::PlaybackId::from_raw(9),
                PlaybackSource::Blob(wav),
                None,
                false,
            )
            .await
            .expect("a wav queues into the conference leg's egress");
        tokio::time::sleep(Duration::from_millis(400)).await;

        let alice_ear = alice.loudest_ear();
        let bob_ear = bob.loudest_ear();
        assert!(
            alice_ear > 2_700,
            "the prompt reaches the leg it was played into: {alice_ear}"
        );
        assert!(
            bob_ear < 300,
            "a prompt played into one leg is not in the other's ear: {bob_ear}"
        );

        plane.close_session(alice.session).await.expect("alice out");
        plane.close_session(bob.session).await.expect("bob out");
    }

    #[tokio::test]
    async fn a_conference_refuses_a_leg_whose_frame_the_mix_cannot_carry() {
        let plane = plane();
        let alice = seat_peer(&plane, 1, "mixed-rates").await;
        let slow = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        let slow_port = slow.local_addr().expect("peer address").port();
        let error = plane
            .open_session(conference_session(
                2,
                &inline_offer_with_ptime(slow_port, 40),
                "mixed-rates",
            ))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("same rate and ptime"),
            "the refusal names the rule: {error}"
        );
        assert_eq!(
            plane.live_sessions(),
            1,
            "a refused leg leaves no half-open session behind"
        );
        plane.close_session(alice.session).await.expect("alice out");
    }

    #[tokio::test]
    async fn an_offer_this_leg_cannot_speak_is_refused_before_a_socket_is_bound() {
        let plane = plane();
        let error = plane
            .open_session(inline_session(
                "v=0\r\no=peer 1 1 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\n\
m=audio 41000 RTP/AVP 111\r\na=rtpmap:111 opus/48000/2\r\n",
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("opus"), "{error}");
        assert_eq!(plane.live_sessions(), 0);
    }

    #[tokio::test]
    async fn stopping_playback_on_an_inline_leg_flushes_its_egress_instead_of_calling_rtpengine() {
        let peer = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        let plane = plane();
        let session = SessionId::from_raw(1);
        plane
            .open_session(inline_session(&inline_offer_sdp(
                peer.local_addr().expect("peer address").port(),
            )))
            .await
            .expect("an inline leg");

        let egress = plane.inline_egress(session).expect("an egress handle");
        assert!(egress.push(vec![1_000i16; 8_000]));
        plane
            .stop_playback(session, session_core::PlaybackId::from_raw(3), None)
            .await
            .expect("stopping playback on an inline leg is a flush");
        tokio::time::sleep(Duration::from_millis(60)).await;

        let mut totals = crate::inline_leg::InlineEgressTotals::default();
        totals.add_shared(&egress.shared());
        assert_eq!(totals.clears, 1);
        assert!(totals.cleared_samples > 0);
        plane.close_session(session).await.expect("close");
    }

    #[tokio::test]
    async fn a_playback_longer_than_the_egress_queue_is_refused_rather_than_truncated() {
        let peer = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        let plane = plane();
        let session = SessionId::from_raw(1);
        plane
            .open_session(inline_session(&inline_offer_sdp(
                peer.local_addr().expect("peer address").port(),
            )))
            .await
            .expect("an inline leg");

        let wav = crate::tap_spike::wav_blob(AudioFormat::pcmu_8k_20ms(), &vec![0i16; 8_000 * 30])
            .expect("a long wav");
        let error = plane
            .start_playback(
                session,
                session_core::PlaybackId::from_raw(3),
                PlaybackSource::Blob(wav),
                None,
                false,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("inject attachment"), "{error}");
        plane.close_session(session).await.expect("close");
    }

    #[tokio::test]
    async fn a_playback_at_the_wrong_sample_rate_is_refused_by_name() {
        let peer = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        let plane = plane();
        let session = SessionId::from_raw(1);
        plane
            .open_session(inline_session(&inline_offer_sdp(
                peer.local_addr().expect("peer address").port(),
            )))
            .await
            .expect("an inline leg");

        let wide = AudioFormat {
            encoding: Encoding::L16,
            sample_rate_hz: 16_000,
            channels: 1,
            ptime_ms: 20,
        };
        let wav = crate::tap_spike::wav_blob(wide, &vec![0i16; 1_600]).expect("a 16k wav");
        let error = plane
            .start_playback(
                session,
                session_core::PlaybackId::from_raw(3),
                PlaybackSource::Blob(wav),
                None,
                false,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("resample"), "{error}");
        plane.close_session(session).await.expect("close");
    }

    #[tokio::test]
    async fn an_inline_session_without_an_offer_is_refused_by_name() {
        let plane = plane();
        let error = plane
            .open_session(session(SessionKind::Inline, ""))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("sdp offer"), "{error}");
    }

    #[tokio::test]
    async fn a_mixed_session_names_the_conference_phase_rather_than_half_opening() {
        let plane = plane();
        let error = plane
            .open_session(session(SessionKind::Mix, ""))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("phase-4"), "{error}");
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
            journal: None,
            spill_every: recorder::SPILL_EVERY,
            counters: Arc::new(RecorderCounters::default()),
            owner: "pod-a".to_string(),
            upload_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
                recorder::DEFAULT_UPLOAD_CONCURRENCY,
            )),
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

    #[tokio::test]
    async fn a_group_is_refused_on_every_transport_but_file_s3() {
        let plane = plane();
        for transport in [
            Transport::WsTwilio,
            Transport::GrpcStream,
            Transport::RtpInline,
        ] {
            let mut view = attachment(transport, "endpoint");
            view.group = "conf-9".to_string();
            let error = plane.open_attachment(view).await.unwrap_err();
            assert!(
                error.to_string().contains("file-s3 recordings only"),
                "{transport} was refused with {error}"
            );
            assert!(error.to_string().contains("conf-9"), "{error}");
        }
    }

    #[test]
    fn an_ungrouped_recording_still_writes_the_frozen_two_leg_identity() {
        let identity = identity("acct-42/rec-99.wav");
        let session = SessionId::from_raw(1);
        for selector in [TrackSelector::All, TrackSelector::Only(Track::Customer)] {
            let spec = RecorderSpec::one_object(session, &identity, layout_of(selector), 8000);
            assert_eq!(
                spec.targets,
                vec![RecordingTarget {
                    key: "acct-42/rec-99.wav".to_string(),
                    layout: layout_of(selector),
                }]
            );
            assert_eq!(spec.recording_id, "rec-99");
        }
    }

    #[test]
    fn a_group_member_writes_one_mono_object_per_track_it_selected() {
        let identity = identity("acct-42/rec-99.wav");
        assert_eq!(
            participant_targets(
                &identity,
                "alice",
                TrackSelector::Only(Track::Customer),
                Attribution::Explicit
            ),
            vec![RecordingTarget {
                key: "acct-42/rec-99/alice.wav".to_string(),
                layout: Layout::Mono(Track::Customer),
            }]
        );
        assert_eq!(
            participant_targets(
                &identity,
                "alice",
                TrackSelector::All,
                Attribution::Explicit
            ),
            vec![
                RecordingTarget {
                    key: "acct-42/rec-99/alice.customer.wav".to_string(),
                    layout: Layout::Mono(Track::Customer),
                },
                RecordingTarget {
                    key: "acct-42/rec-99/alice.agent.wav".to_string(),
                    layout: Layout::Mono(Track::Agent),
                },
            ]
        );
    }

    #[test]
    fn a_member_with_no_label_is_named_after_its_session() {
        let mut view = attachment(Transport::FileS3, "acct-42/rec-99.wav");
        assert_eq!(participant_of(&view, "req-7").unwrap(), "consumer");
        view.label = String::new();
        assert_eq!(participant_of(&view, "req-7").unwrap(), "req-7");
        view.label = "../escape".to_string();
        let error = participant_of(&view, "req-7").unwrap_err();
        assert!(error.to_string().contains("attachment label"), "{error}");
    }

    #[test]
    fn a_group_refuses_a_second_member_that_would_overwrite_the_first() {
        let plane = plane_with_recording(recording_support());
        let identity = identity("acct-42/rec-99.wav");
        let key = GroupKey {
            account_id: identity.account_id.clone(),
            group: "conf-9".to_string(),
        };
        let alice = participant_targets(
            &identity,
            "alice",
            TrackSelector::All,
            Attribution::Explicit,
        );
        let bob = participant_targets(&identity, "bob", TrackSelector::All, Attribution::Explicit);

        let opened = plane
            .join_group(&key, AttachmentId::from_raw(2), &identity, &alice)
            .expect("the first member opens the group");
        let joined_late = plane
            .join_group(&key, AttachmentId::from_raw(3), &identity, &bob)
            .expect("a second participant is the whole point");
        assert_eq!(
            opened, joined_late,
            "every member of a group anchors on the instant the group opened"
        );
        let error = plane
            .join_group(&key, AttachmentId::from_raw(4), &identity, &alice)
            .unwrap_err();

        assert!(error.to_string().contains("label of its own"), "{error}");
        assert!(
            error
                .to_string()
                .contains("acct-42/rec-99/alice.customer.wav"),
            "{error}"
        );
        let counters = &plane.config.recording.counters;
        assert_eq!(counters.groups_live.load(Ordering::Relaxed), 1);
        assert_eq!(counters.group_members_live.load(Ordering::Relaxed), 2);
        assert_eq!(counters.group_joins_refused.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn one_group_records_one_recording_and_says_so_when_asked_for_two() {
        let plane = plane_with_recording(recording_support());
        let first = identity("acct-42/rec-99.wav");
        let second = identity("acct-42/rec-100.wav");
        let key = GroupKey {
            account_id: first.account_id.clone(),
            group: "conf-9".to_string(),
        };

        plane
            .join_group(
                &key,
                AttachmentId::from_raw(2),
                &first,
                &participant_targets(&first, "alice", TrackSelector::All, Attribution::Explicit),
            )
            .unwrap();
        let error = plane
            .join_group(
                &key,
                AttachmentId::from_raw(3),
                &second,
                &participant_targets(&second, "bob", TrackSelector::All, Attribution::Explicit),
            )
            .unwrap_err();

        assert!(error.to_string().contains("rec-99"), "{error}");
        assert!(
            error.to_string().contains("one group writes one recording"),
            "{error}"
        );
    }

    #[test]
    fn a_group_dies_with_its_last_member() {
        let plane = plane_with_recording(recording_support());
        let identity = identity("acct-42/rec-99.wav");
        let key = GroupKey {
            account_id: identity.account_id.clone(),
            group: "conf-9".to_string(),
        };
        let members = [AttachmentId::from_raw(2), AttachmentId::from_raw(3)];
        for (index, member) in members.iter().enumerate() {
            plane
                .join_group(
                    &key,
                    *member,
                    &identity,
                    &participant_targets(
                        &identity,
                        &format!("caller-{index}"),
                        TrackSelector::Only(Track::Customer),
                        Attribution::Explicit,
                    ),
                )
                .unwrap();
        }
        let counters = &plane.config.recording.counters;
        assert_eq!(counters.group_members_live.load(Ordering::Relaxed), 2);

        plane.leave_group(&key, members[0]);
        assert_eq!(counters.groups_live.load(Ordering::Relaxed), 1);
        plane.leave_group(&key, members[1]);

        assert_eq!(counters.groups_live.load(Ordering::Relaxed), 0);
        assert_eq!(counters.group_members_live.load(Ordering::Relaxed), 0);
        assert!(plane.groups.lock().unwrap().is_empty());

        plane
            .join_group(
                &key,
                members[0],
                &identity,
                &participant_targets(
                    &identity,
                    "caller-0",
                    TrackSelector::Only(Track::Customer),
                    Attribution::Explicit,
                ),
            )
            .expect("the same group name is free once the last member has gone");
        assert_eq!(counters.groups_live.load(Ordering::Relaxed), 1);
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

    #[test]
    fn a_tap_answer_carries_the_advertised_address_not_the_bind_address() {
        let offer = offer_of("0 101", &["0 PCMU/8000", "101 telephone-event/8000"]);
        let answer_with = negotiated_tap_codec(&offer).expect("pcmu is negotiable");
        let sdp = tap_answer_sdp(
            1,
            IpAddr::from([198, 51, 100, 9]),
            &[41000],
            AudioFormat::pcmu_8k_20ms(),
            answer_with,
            &offer,
        )
        .expect("an answer");
        assert!(sdp.contains("c=IN IP4 198.51.100.9"), "{sdp}");
        assert!(sdp.contains("m=audio 41000"), "{sdp}");
        assert!(!sdp.contains("127.0.0.1"), "{sdp}");
    }

    #[tokio::test]
    async fn an_inline_answer_carries_the_advertised_address_while_the_socket_binds_locally() {
        let peer = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        let peer_port = peer.local_addr().expect("peer address").port();
        let plane = plane_with_media(
            IpAddr::from([203, 0, 113, 7]),
            MediaPortAllocator::ephemeral(),
        );

        let answer = plane
            .open_session(inline_session(&inline_offer_sdp(peer_port)))
            .await
            .expect("an inline leg answers a pcmu offer")
            .sdp_answer
            .expect("an inline session answers with sdp");

        assert!(answer.contains("203.0.113.7"), "{answer}");
        assert!(!answer.contains("127.0.0.1"), "{answer}");
    }

    #[tokio::test]
    async fn a_media_port_range_bounds_inline_sockets_and_frees_them_on_close() {
        let peer = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        let peer_port = peer.local_addr().expect("peer address").port();
        let plane = plane_with_media(
            IpAddr::from([127, 0, 0, 1]),
            MediaPortAllocator::over_range(41500, 41501),
        );

        let answer = plane
            .open_session(inline_session(&inline_offer_sdp(peer_port)))
            .await
            .expect("the first inline leg takes the only rtp port in the range")
            .sdp_answer
            .expect("an inline session answers with sdp");
        assert!(answer.contains("m=audio 41500"), "{answer}");

        let error = plane
            .open_session(SessionView {
                id: SessionId::from_raw(2),
                ..inline_session(&inline_offer_sdp(peer_port))
            })
            .await
            .expect_err("the range holds one rtp port");
        assert!(
            error.to_string().contains("every port in 41500-41501"),
            "{error}"
        );

        plane
            .close_session(SessionId::from_raw(1))
            .await
            .expect("the first session closes");

        let answer = plane
            .open_session(SessionView {
                id: SessionId::from_raw(3),
                ..inline_session(&inline_offer_sdp(peer_port))
            })
            .await
            .expect("the freed port is handed out again")
            .sdp_answer
            .expect("an inline session answers with sdp");
        assert!(answer.contains("m=audio 41500"), "{answer}");
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
        let format = offered_tap_format(&offer, 20, 16000).unwrap();
        assert_eq!(format.encoding, media_core::Encoding::Pcma);
        assert_eq!(format.sample_rate_hz, 8000);
        assert_eq!(format.ptime_ms, 20);
    }

    #[test]
    fn a_call_whose_codec_this_pipeline_cannot_decode_names_transcoding_as_the_fix() {
        let offer = offer_of("111 101", &["111 EVS/16000", "101 telephone-event/8000"]);
        let error = offered_tap_format(&offer, 20, 16000).unwrap_err();
        assert!(
            error.to_string().contains("needs transcoding at the tap"),
            "{error}"
        );
        assert!(error.to_string().contains("111"), "{error}");
    }

    #[test]
    fn an_opus_call_is_negotiated_at_the_offers_dynamic_payload_type() {
        let offer = offer_of("111 101", &["111 opus/48000/2", "101 telephone-event/8000"]);
        let codec = negotiated_tap_codec(&offer).unwrap();
        assert_eq!(codec.payload_type, 111);
        assert_eq!(codec.encoding, media_core::Encoding::Opus);
        assert_eq!(codec.clock_rate_hz, 48000);
        assert_eq!(codec.samples_per_packet(20), Some(960));
    }

    #[test]
    fn a_tap_transcoded_to_opus_answers_with_the_payload_type_rtpengine_offered() {
        let offer = offer_of("111 101", &["111 opus/48000/2", "101 telephone-event/8000"]);
        let configured = AudioFormat {
            encoding: media_core::Encoding::Opus,
            sample_rate_hz: 16000,
            channels: 1,
            ptime_ms: 20,
        };

        let codec = transcoded_tap_codec(&offer, configured).unwrap();

        assert_eq!(codec.payload_type, 111);
        assert_eq!(codec.encoding, media_core::Encoding::Opus);
        assert_eq!(codec.clock_rate_hz, 48000);
    }

    #[test]
    fn a_transcoded_tap_picks_the_codec_it_asked_for_not_the_first_one_offered() {
        let offer = offer_of(
            "8 0 101",
            &["8 PCMA/8000", "0 PCMU/8000", "101 telephone-event/8000"],
        );

        let codec = transcoded_tap_codec(&offer, AudioFormat::pcmu_8k_20ms()).unwrap();

        assert_eq!(codec.payload_type, 0);
        assert_eq!(codec.encoding, media_core::Encoding::Pcmu);
        assert_eq!(
            negotiated_tap_codec(&offer).unwrap().encoding,
            media_core::Encoding::Pcma,
            "without transcoding the tap takes what the call leads with"
        );
    }

    #[test]
    fn a_transcoded_tap_falls_back_to_the_static_mapping_when_the_offer_omits_the_codec() {
        let offer = offer_of("8 101", &["8 PCMA/8000", "101 telephone-event/8000"]);

        let codec = transcoded_tap_codec(&offer, AudioFormat::pcmu_8k_20ms()).unwrap();

        assert_eq!(codec.payload_type, 0);
        assert_eq!(codec.encoding, media_core::Encoding::Pcmu);
    }

    #[test]
    fn an_opus_transcode_that_rtpengine_did_not_offer_is_refused_by_name() {
        let offer = offer_of("8 101", &["8 PCMA/8000", "101 telephone-event/8000"]);
        let configured = AudioFormat {
            encoding: media_core::Encoding::Opus,
            sample_rate_hz: 16000,
            channels: 1,
            ptime_ms: 20,
        };

        let error = transcoded_tap_codec(&offer, configured).unwrap_err();

        assert!(error.to_string().contains("Opus"), "{error}");
        assert!(error.to_string().contains("no payload type"), "{error}");
    }

    #[test]
    fn an_opus_tap_decodes_at_the_configured_rate_not_the_rtp_clock() {
        let offer = offer_of("111 101", &["111 opus/48000/2", "101 telephone-event/8000"]);

        let format = offered_tap_format(&offer, 20, 16000).unwrap();
        assert_eq!(format.encoding, media_core::Encoding::Opus);
        assert_eq!(format.sample_rate_hz, 16000);
        assert_eq!(format.channels, 1);
        assert_eq!(format.samples_per_packet(), Some(320));

        assert_eq!(
            offered_tap_format(&offer, 20, 48000)
                .unwrap()
                .sample_rate_hz,
            48000
        );
    }

    #[test]
    fn an_opus_tap_builds_a_pipeline_that_admits_the_dynamic_payload_type() {
        let offer = offer_of("111 101", &["111 opus/48000/2", "101 telephone-event/8000"]);
        let codec = negotiated_tap_codec(&offer).unwrap();
        let format = offered_tap_format(&offer, 20, 16000).unwrap();

        let pipeline = media_core::pipeline::StreamPipeline::with_config(PipelineConfig {
            audio_payload_type: codec.payload_type,
            clock_rate_hz: codec.clock_rate_hz,
            decode: format,
            target_depth_packets: TARGET_DEPTH_PACKETS,
            telephone_event_payload_type: Some(101),
        })
        .unwrap();

        assert_eq!(pipeline.samples_per_packet(), 320);
    }

    #[test]
    fn legs_offered_with_different_codecs_are_refused_at_the_codec_level_too() {
        let mut offer = offer_of("111 101", &["111 opus/48000/2", "101 telephone-event/8000"]);
        let g711 = offer_of("8 101", &["8 PCMA/8000", "101 telephone-event/8000"]);
        offer.streams.push(g711.streams[0].clone());
        let error = negotiated_tap_codec(&offer).unwrap_err();
        assert!(
            error.to_string().contains("one codec for every leg"),
            "{error}"
        );
    }

    #[test]
    fn legs_offered_with_different_codecs_are_refused_rather_than_half_decoded() {
        let mut offer = offer_of("0 101", &["0 PCMU/8000", "101 telephone-event/8000"]);
        let alaw = offer_of("8 101", &["8 PCMA/8000", "101 telephone-event/8000"]);
        offer.streams.push(alaw.streams[0].clone());

        let error = offered_tap_format(&offer, 20, 16000).unwrap_err();
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
            offered_tap_format(&offer, 20, 16000).unwrap().encoding,
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
            Attribution::Explicit,
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

    #[test]
    fn a_blob_too_large_for_one_datagram_is_refused_rather_than_truncated() {
        let error = ng_play_source(PlaybackSource::Blob(vec![0u8; MAX_PLAYBACK_BLOB_BYTES + 1]))
            .unwrap_err();
        assert!(error.to_string().contains("chunked playback"));
        assert!(ng_play_source(PlaybackSource::Blob(vec![0u8; 16])).is_ok());
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

    #[tokio::test]
    async fn detaching_a_websocket_consumer_lets_it_finish_instead_of_being_aborted() {
        let plane = plane();
        let (mut hub, client) = crate::hub::Hub::new();
        let mut subscription = client.attach(8, TrackSelection::Speakers).unwrap();
        hub.poll_commands();
        let media = subscription.control();
        let (text, _inbound) = mpsc::channel(TEXT_QUEUE_DEPTH);
        let finished = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&finished);
        let task = tokio::spawn(async move {
            let mut seen = 0u64;
            while subscription.next().await.is_some() {
                seen += 1;
            }
            flag.store(true, Ordering::Relaxed);
            Ok(consumer_ws::ConsumerStats {
                media_sent: seen,
                ..consumer_ws::ConsumerStats::default()
            })
        });

        hub.publish(TapEvent::media(Track::Customer, 0, &[0i16; 160]));
        plane
            .end_attachment(
                AttachmentId::from_raw(2),
                LiveAttachment::Ws {
                    session: SessionId::from_raw(1),
                    text,
                    media,
                    task,
                },
                "the attachment was detached",
            )
            .await;

        assert!(
            finished.load(Ordering::Relaxed),
            "the consumer was aborted rather than being allowed to close"
        );
        assert_eq!(hub.published(), 1);
    }

    #[tokio::test]
    async fn detaching_a_grpc_consumer_ends_its_stream_with_a_stop_frame() {
        let plane = plane();
        let (mut hub, client) = crate::hub::Hub::new();
        let subscription = client.attach(8, TrackSelection::Speakers).unwrap();
        hub.poll_commands();
        let media = subscription.control();
        let (frames, mut receiver) = mpsc::channel(GRPC_FRAME_QUEUE);
        let pump = tokio::spawn(pump_frames(
            subscription,
            frames.clone(),
            AudioFormat::pcmu_8k_20ms(),
            AudioFormat::pcmu_8k_20ms(),
            Attribution::Explicit,
        ));

        hub.publish(TapEvent::media(Track::Customer, 0, &[0i16; 160]));
        plane
            .end_attachment(
                AttachmentId::from_raw(2),
                LiveAttachment::Grpc {
                    session: SessionId::from_raw(1),
                    selection: TrackSelection::Speakers,
                    format: AudioFormat::pcmu_8k_20ms(),
                    paused: false,
                    live: Some(GrpcLive {
                        frames,
                        media,
                        pump,
                    }),
                },
                "the attachment was detached",
            )
            .await;

        let mut seen = Vec::new();
        while let Some(frame) = receiver.recv().await {
            seen.push(frame);
        }
        match seen.last() {
            Some(StreamFrame::Stop { reason }) => assert_eq!(reason, "the attachment was detached"),
            other => panic!("expected a stop frame last, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pausing_a_consumer_attachment_stops_the_hub_feeding_it() {
        let plane = plane();
        let (mut hub, client) = crate::hub::Hub::new();
        let mut subscription = client.attach(8, TrackSelection::Speakers).unwrap();
        hub.poll_commands();
        let media = subscription.control();
        let (text, _inbound) = mpsc::channel(TEXT_QUEUE_DEPTH);
        let task = tokio::spawn(async { Ok(consumer_ws::ConsumerStats::default()) });
        plane.attachments.lock().unwrap().insert(
            AttachmentId::from_raw(2),
            LiveAttachment::Ws {
                session: SessionId::from_raw(1),
                text,
                media,
                task,
            },
        );

        let mut view = attachment(Transport::WsTwilio, "ws-endpoint");
        view.paused = true;
        plane.update_attachment(view.clone()).await.unwrap();
        hub.publish(TapEvent::media(Track::Customer, 0, &[0i16; 160]));
        hub.publish(TapEvent::media(Track::Customer, 20, &[0i16; 160]));

        view.paused = false;
        plane.update_attachment(view).await.unwrap();
        hub.publish(TapEvent::media(Track::Customer, 40, &[0i16; 160]));

        let mut stamps = Vec::new();
        while let Some(TapEvent::Media { timestamp_ms, .. }) = subscription.try_next() {
            stamps.push(timestamp_ms);
        }
        assert_eq!(stamps, vec![40]);
        assert_eq!(subscription.suppressed_while_paused(), 2);
    }

    #[test]
    fn a_selector_becomes_the_hub_selection_and_the_track_list_it_implies() {
        assert_eq!(
            consumer_selection_of(TrackSelector::All),
            TrackSelection::Speakers
        );
        assert_eq!(
            recording_selection_of(TrackSelector::All),
            TrackSelection::All
        );
        assert_eq!(
            consumer_selection_of(TrackSelector::Only(Track::Agent)),
            TrackSelection::Only(Track::Agent)
        );
        assert_eq!(
            recording_selection_of(TrackSelector::Only(Track::Mixed)),
            TrackSelection::Only(Track::Mixed)
        );
        assert_eq!(tracks_of(TrackSelector::All), vec!["inbound", "outbound"]);
        assert_eq!(
            tracks_of(TrackSelector::Only(Track::Customer)),
            vec!["inbound"]
        );
    }
    const SCHEME_SEPARATOR: &str = "\x2f\x2f";

    struct WiredRoom {
        endpoint: String,
        stop: Option<tokio::sync::oneshot::Sender<()>>,
        serving: tokio::task::JoinHandle<()>,
    }

    impl WiredRoom {
        async fn open() -> WiredRoom {
            WiredRoom::with_plane(Arc::new(plane())).await.0
        }

        async fn with_plane(
            plane: Arc<TapPlane>,
        ) -> (WiredRoom, Arc<control_api::SessionController>) {
            let media: Arc<dyn MediaPlane> = Arc::clone(&plane) as Arc<dyn MediaPlane>;
            let controller =
                Arc::new(control_api::SessionController::new("wire-pod").with_media_plane(media));
            plane.observe_through(Arc::downgrade(&controller) as Weak<dyn ObservationSink>);
            let room = WiredRoom::serving(Arc::clone(&controller)).await;
            (room, controller)
        }

        async fn serving(controller: Arc<control_api::SessionController>) -> WiredRoom {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("a control port");
            let port = listener.local_addr().expect("the control address").port();
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let serving = tokio::spawn(async move {
                control_api::serve_shared_until(controller, listener, async {
                    let _ = stopped.await;
                })
                .await
                .expect("the control plane serves");
            });
            WiredRoom {
                endpoint: format!("http:{SCHEME_SEPARATOR}127.0.0.1:{port}"),
                stop: Some(stop),
                serving,
            }
        }

        async fn close(mut self) {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
            let _ = self.serving.await;
        }
    }

    fn silent_peer() -> (UdpSocket, String) {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("a fake peer socket");
        socket.set_nonblocking(true).expect("nonblocking peer");
        let port = socket.local_addr().expect("peer address").port();
        let offer = inline_offer_sdp(port);
        (socket, offer)
    }

    fn wire_reference(external_id: &str) -> control_api::proto::SessionRef {
        control_api::proto::SessionRef {
            id: Some(control_api::proto::session_ref::Id::ExternalId(
                external_id.to_string(),
            )),
        }
    }

    #[tokio::test]
    async fn describing_a_member_reads_back_its_own_state_and_enumerates_the_room() {
        use control_api::proto;
        use control_api::proto::media_control_client::MediaControlClient;

        let room = WiredRoom::open().await;
        let mut client = MediaControlClient::connect(room.endpoint.clone())
            .await
            .expect("a client reaches the control port");

        let mut attachments = Vec::new();
        let mut peers = Vec::new();
        for member in ["alice", "bob", "carol"] {
            let (peer, offer) = silent_peer();
            peers.push(peer);
            client
                .create_session(proto::CreateSessionRequest {
                    external_id: member.to_string(),
                    kind: proto::SessionKind::Inline as i32,
                    call_id: format!("call-{member}"),
                    from_tags: Vec::new(),
                    rtpengine_node: String::new(),
                    mix: false,
                    idempotency_key: String::new(),
                    sdp_offer: offer,
                    group: "sales-standup".to_string(),
                })
                .await
                .expect("an inline leg is answered and seated")
                .into_inner();
            let attachment = client
                .attach(proto::AttachRequest {
                    session: Some(wire_reference(member)),
                    transport: proto::Transport::GrpcStream as i32,
                    capabilities: vec![
                        proto::Capability::Sink as i32,
                        proto::Capability::Inject as i32,
                    ],
                    selector: None,
                    format: None,
                    authoritative: false,
                    label: member.to_string(),
                    endpoint: "grpc-target".to_string(),
                    group: String::new(),
                    metadata: Default::default(),
                    idempotency_key: String::new(),
                })
                .await
                .expect("an inject attachment on a member session")
                .into_inner();
            attachments.push(attachment.attachment_id);
        }

        let update =
            |attachment: String, pairs: Vec<(&str, &str)>| proto::UpdateAttachmentRequest {
                attachment_id: attachment,
                paused: None,
                selector: None,
                format: None,
                idempotency_key: String::new(),
                metadata: pairs
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            };
        client
            .update_attachment(update(
                attachments[1].clone(),
                vec![(MEMBER_MUTE_METADATA_KEY, MEMBER_FLAG_ON)],
            ))
            .await
            .expect("bob is muted through his own attachment");
        client
            .update_attachment(update(
                attachments[0].clone(),
                vec![(MIX_TARGET_METADATA_KEY, "carol")],
            ))
            .await
            .expect("alice whispers to carol");

        let describe = |mut client: proto::media_control_client::MediaControlClient<
            control_api::tonic::transport::Channel,
        >,
                        member: &'static str| async move {
            client
                .describe_session(wire_reference(member))
                .await
                .expect("a member describes")
                .into_inner()
        };

        let alice = describe(client.clone(), "alice").await;
        let member = alice.member.expect("a seated member reads its state back");
        assert!(!member.mute && !member.deaf && !member.hold);
        assert_eq!(member.mix_source, "inject");
        assert_eq!(member.routes.len(), 1, "the whisper is the only route");
        assert_eq!(member.routes[0].target, "carol");
        assert_eq!(member.routes[0].source, "inject");
        assert!(member.routes[0].monitor_audible);
        assert_eq!(member.routes[0].attachment_id, attachments[0]);
        let room_view = alice.conference.expect("a member enumerates its room");
        assert_eq!(room_view.group, "sales-standup");
        assert_eq!(room_view.member_count, 3);
        assert_eq!(
            room_view.members,
            vec!["alice".to_string(), "bob".to_string(), "carol".to_string()]
        );

        let bob = describe(client.clone(), "bob").await;
        let muted = bob.member.expect("bob reads his state back");
        assert!(muted.mute, "the mute verb is visible to the next reader");
        assert!(!muted.deaf && !muted.hold);
        assert!(
            muted.routes.is_empty(),
            "a plain member routes nothing anywhere else"
        );

        let carol = describe(client.clone(), "carol").await;
        let whispered_at = carol.member.expect("carol reads her state back");
        assert!(!whispered_at.mute && !whispered_at.deaf && !whispered_at.hold);
        assert!(
            whispered_at.routes.is_empty(),
            "being whispered at is not a route of one's own"
        );

        client
            .destroy_session(wire_reference("carol"))
            .await
            .expect("carol leaves the room");

        let after = describe(client.clone(), "alice").await;
        let room_after = after.conference.expect("the room is still enumerable");
        assert_eq!(room_after.member_count, 2);
        assert_eq!(
            room_after.members,
            vec!["alice".to_string(), "bob".to_string()]
        );
        let alice_after = after.member.expect("alice still reads her state back");
        assert_eq!(
            alice_after.routes[0].target, "carol",
            "the route outlives the member it named, which is what makes it auditable"
        );

        let bob_after = describe(client.clone(), "bob").await;
        assert!(
            bob_after.member.expect("bob is still seated").mute,
            "member state has no lease, so it survives every other membership change"
        );

        room.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_detach_is_answered_before_the_upload_and_the_upload_event_ends_the_sequence() {
        use control_api::proto;
        use control_api::proto::media_control_client::MediaControlClient;

        const SLOW_BY: Duration = Duration::from_millis(1_500);
        const ANSWERED_WITHIN: Duration = Duration::from_millis(100);

        let bucket = Arc::new(BucketSink::slow(SLOW_BY));
        let plane = Arc::new(bucket_plane(&bucket));
        let counters = Arc::clone(&plane.config.recording.counters);
        let (room, controller) = WiredRoom::with_plane(Arc::clone(&plane)).await;
        let mut events = controller.subscribe();
        let mut client = MediaControlClient::connect(room.endpoint.clone())
            .await
            .expect("a client reaches the control port");

        let (_peer, offer) = silent_peer();
        client
            .create_session(proto::CreateSessionRequest {
                external_id: "call-7".to_string(),
                kind: proto::SessionKind::Inline as i32,
                call_id: "call-7".to_string(),
                from_tags: Vec::new(),
                rtpengine_node: String::new(),
                mix: false,
                idempotency_key: String::new(),
                sdp_offer: offer,
                group: String::new(),
            })
            .await
            .expect("an inline leg to record");

        let record = |mut client: MediaControlClient<control_api::tonic::transport::Channel>,
                      endpoint: &'static str| async move {
            client
                .attach(proto::AttachRequest {
                    session: Some(wire_reference("call-7")),
                    transport: proto::Transport::FileS3 as i32,
                    capabilities: vec![proto::Capability::Sink as i32],
                    selector: None,
                    format: None,
                    authoritative: false,
                    label: String::new(),
                    endpoint: endpoint.to_string(),
                    group: String::new(),
                    metadata: Default::default(),
                    idempotency_key: String::new(),
                })
                .await
                .expect("a recording attachment")
                .into_inner()
                .attachment_id
        };

        let first = record(client.clone(), "acct-42/rec-99.wav").await;
        let asked = Instant::now();
        client
            .detach(proto::AttachmentRef {
                attachment_id: first.clone(),
            })
            .await
            .expect("the recording is stopped");
        let answered = asked.elapsed();
        assert!(
            answered < ANSWERED_WITHIN,
            "Detach answered in {answered:?}, so it waited for the upload"
        );
        assert_eq!(plane.uploads_in_flight(), 1);

        let second = record(client.clone(), "acct-42/rec-100.wav").await;
        let asked_again = Instant::now();
        client
            .destroy_session(wire_reference("call-7"))
            .await
            .expect("the call ends while its recording is still uploading");
        let ended = asked_again.elapsed();
        assert!(
            ended < ANSWERED_WITHIN,
            "DestroySession answered in {ended:?}, so it waited for the upload"
        );
        assert!(
            client
                .describe_session(wire_reference("call-7"))
                .await
                .is_err(),
            "an ended session is not describable by its external id, finishing or not"
        );

        let mut seen: Vec<(u64, session_core::EventKind)> = Vec::new();
        let deadline = tokio::time::Instant::now() + SLOW_BY * 4;
        while seen
            .iter()
            .filter(|(_, kind)| matches!(kind, session_core::EventKind::UploadCompleted { .. }))
            .count()
            < 2
        {
            let event = tokio::time::timeout_at(deadline, events.recv())
                .await
                .unwrap_or_else(|_| panic!("the upload events never arrived; saw {seen:?}"))
                .expect("the event watcher stayed open");
            assert_eq!(event.external_id, "call-7");
            seen.push((event.seq, event.kind));
        }

        let seqs: Vec<u64> = seen.iter().map(|(seq, _)| *seq).collect();
        assert_eq!(
            seqs,
            (0..seen.len() as u64).collect::<Vec<u64>>(),
            "the sequence of a session whose uploads outlive it stays gapless and in order: \
             {seen:?}"
        );
        let names: Vec<&str> = seen.iter().map(|(_, kind)| event_name(kind)).collect();
        assert_eq!(
            names,
            vec![
                "attachment-up",
                "recording-started",
                "attachment-down",
                "recording-stopped",
                "attachment-up",
                "recording-started",
                "recording-stopped",
                "attachment-down",
                "session-ended",
                "upload-completed",
                "upload-completed",
            ],
            "a stop is always published before its caller is answered, and both uploads report \
             themselves after the session that owned them has already ended: {seen:?}"
        );
        assert!(
            !second.is_empty() && second != first,
            "the two recordings were different attachments"
        );
        assert_eq!(
            bucket.keys(),
            vec![
                "acct-42/rec-99.wav".to_string(),
                "acct-42/rec-100.wav".to_string()
            ]
        );
        assert_eq!(counters.uploads_backgrounded.load(Ordering::Relaxed), 2);
        assert_eq!(counters.uploaded.load(Ordering::Relaxed), 2);
        plane.await_uploads().await;
        assert_eq!(plane.uploads_in_flight(), 0);
        assert_eq!(controller.sessions_finishing(), 0);
        assert_eq!(controller.counts(), (0, 0));

        room.close().await;
    }

    fn event_name(kind: &session_core::EventKind) -> &'static str {
        use session_core::EventKind;
        match kind {
            EventKind::AttachmentUp { .. } => "attachment-up",
            EventKind::AttachmentDown { .. } => "attachment-down",
            EventKind::RecordingStarted { .. } => "recording-started",
            EventKind::RecordingStopped { .. } => "recording-stopped",
            EventKind::RecordingPaused { .. } => "recording-paused",
            EventKind::UploadCompleted { .. } => "upload-completed",
            EventKind::UploadFailed { .. } => "upload-failed",
            EventKind::SessionEnded { .. } => "session-ended",
            _ => "other",
        }
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

    fn stamped(pairs: &[(&str, Option<i64>)]) -> Vec<(String, Option<i64>)> {
        pairs
            .iter()
            .map(|(tag, created)| ((*tag).to_string(), *created))
            .collect()
    }

    #[test]
    fn creation_times_that_separate_two_legs_make_the_earliest_one_the_customer() {
        let ordered = order_participants(&stamped(&[
            ("fs-side", Some(1787737395)),
            ("carrier-side", Some(1787737383)),
        ]));
        assert_eq!(ordered.attribution, Attribution::Inferred);
        assert_eq!(ordered.tags, vec!["carrier-side", "fs-side"]);
    }

    #[test]
    fn one_creation_second_for_both_legs_claims_no_direction_at_all() {
        let ordered = order_participants(&stamped(&[
            ("fs-side", Some(1787737383)),
            ("carrier-side", Some(1787737383)),
        ]));
        assert_eq!(ordered.attribution, Attribution::Unknown);
        assert_eq!(ordered.tags, vec!["fs-side", "carrier-side"]);
    }

    #[test]
    fn a_participant_rtpengine_never_stamped_sorts_last_and_settles_nothing() {
        let ordered = order_participants(&stamped(&[("late", None), ("early", None)]));
        assert_eq!(ordered.attribution, Attribution::Unknown);
        assert_eq!(ordered.tags, vec!["late", "early"]);
        let half = order_participants(&stamped(&[("unstamped", None), ("stamped", Some(9))]));
        assert_eq!(half.attribution, Attribution::Unknown);
        assert_eq!(half.tags, vec!["stamped", "unstamped"]);
    }

    #[test]
    fn a_second_dialogue_on_one_call_id_sorts_after_the_first_two_legs() {
        let ordered = order_participants(&stamped(&[
            ("legC", Some(1787737395)),
            ("legD", Some(1787737395)),
            ("legA", Some(1787737383)),
            ("legB", Some(1787737383)),
        ]));
        assert_eq!(ordered.tags, vec!["legA", "legB", "legC", "legD"]);
        assert_eq!(ordered.attribution, Attribution::Unknown);
    }

    #[test]
    fn only_unknown_attribution_renames_the_grpc_and_recording_tracks() {
        use control_api::convert::track_name_under;
        for settled in [Attribution::Explicit, Attribution::Inferred] {
            assert_eq!(track_name_under(Track::Customer, settled), "customer");
            assert_eq!(track_name_under(Track::Agent, settled), "agent");
        }
        assert_eq!(
            track_name_under(Track::Customer, Attribution::Unknown),
            "leg_a"
        );
        assert_eq!(
            track_name_under(Track::Agent, Attribution::Unknown),
            "leg_b"
        );
        assert_eq!(
            track_name_under(Track::Mixed, Attribution::Unknown),
            "mixed"
        );
    }

    #[test]
    fn an_unattributed_recording_names_its_objects_leg_a_and_leg_b() {
        let identity = RecordingIdentity::parse("acct-42/rec-99.wav").unwrap();
        let guessing =
            participant_targets(&identity, "alice", TrackSelector::All, Attribution::Unknown);
        let keys: Vec<String> = guessing.iter().map(|target| target.key.clone()).collect();
        assert_eq!(
            keys,
            vec![
                "acct-42/rec-99/alice.leg_a.wav".to_string(),
                "acct-42/rec-99/alice.leg_b.wav".to_string()
            ]
        );
        let told = participant_targets(
            &identity,
            "alice",
            TrackSelector::All,
            Attribution::Explicit,
        );
        let keys: Vec<String> = told.iter().map(|target| target.key.clone()).collect();
        assert_eq!(
            keys,
            vec![
                "acct-42/rec-99/alice.customer.wav".to_string(),
                "acct-42/rec-99/alice.agent.wav".to_string()
            ]
        );
    }

    #[test]
    fn the_frozen_twilio_track_names_never_move_however_the_legs_were_labelled() {
        assert_eq!(
            tracks_of(TrackSelector::All),
            vec!["inbound".to_string(), "outbound".to_string()]
        );
        assert_eq!(consumer_ws::track_name(Track::Customer), "inbound");
        assert_eq!(consumer_ws::track_name(Track::Agent), "outbound");
    }
}
