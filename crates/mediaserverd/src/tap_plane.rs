use crate::consumer_ws::{self, ConsumerConfig};
use crate::hub::{
    Hub, HubClient, Subscription, SubscriptionControl, SubscriptionMetrics, TapEvent,
    TrackSelection,
};
use crate::inline_leg::{
    egress_ssrc, InlineEgress, InlineEgressHandle, InlineEgressShared, InlineEgressTotals,
    EGRESS_CHUNK_MS,
};
use crate::ng_transport::{NgTransport, NgTransportConfig};
use crate::recorder::{
    self, Layout, RecorderCounters, RecorderHandle, RecorderSpec, RecordingFormat,
    RecordingIdentity, RecordingProgress, RecordingSupport, RecordingTarget,
};
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
use session_core::{
    AttachmentId, AttachmentView, Capabilities, Observation, SessionId, SessionKind, SessionView,
    TrackSelector, Transport,
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
const POLITE_CLOSE: Duration = Duration::from_secs(2);

pub struct TapPlaneConfig {
    pub default_node: Option<SocketAddr>,
    pub local_media_address: IpAddr,
    pub format: AudioFormat,
    pub transcode_at_tap: bool,
    pub opus_decode_rate_hz: u32,
    pub cookie_prefix: u64,
    pub sdp_session_id: u64,
    pub recording: RecordingSupport,
    pub capabilities: Arc<NodeCapabilityLog>,
}

struct SessionHandles {
    transport: Option<Arc<NgTransport>>,
    call_id: String,
    hub: HubClient,
    external_id: String,
}

struct LiveSession {
    kind: SessionKind,
    transport: Option<Arc<NgTransport>>,
    external_id: String,
    call_id: String,
    to_tag: String,
    format: AudioFormat,
    hub: HubClient,
    egress: Option<InlineEgressHandle>,
    stop: Arc<AtomicBool>,
    capture: Option<std::thread::JoinHandle<()>>,
    speakers: Option<tokio::task::JoinHandle<()>>,
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
    pub recording_spills: u64,
    pub recording_segments_spilled: u64,
    pub recording_segment_spill_failures: u64,
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
}

#[derive(Default)]
struct MetricsInner {
    retired: LegTotals,
    retired_consumer_dropped: u64,
    retired_consumer_delivered: u64,
    retired_consumer_suppressed: u64,
    ssrc_requeries: u64,
    legs: HashMap<SessionId, Vec<Arc<SharedLegStats>>>,
    inline: HashMap<SessionId, Arc<InlineEgressShared>>,
    retired_inline: InlineEgressTotals,
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
            recordings_live: read(&recorder.live),
            recordings_started: read(&recorder.started),
            recordings_stopped: read(&recorder.stopped),
            recording_pauses: read(&recorder.pauses),
            recording_uploads: read(&recorder.uploaded),
            recording_upload_failures: read(&recorder.upload_failures),
            recording_spills: read(&recorder.spilled),
            recording_segments_spilled: read(&recorder.segments_spilled),
            recording_segment_spill_failures: read(&recorder.segment_spill_failures),
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
            ..IngestSnapshot::default()
        };
        for egress in inner.inline.values() {
            snapshot.inline.add_shared(egress);
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
            groups: Mutex::new(HashMap::new()),
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
            hub, external_id, ..
        } = self.session_handles(view.session)?;
        let selection = recording_selection_of(view.selector);
        let grouped = if view.group.is_empty() {
            None
        } else {
            let participant = participant_of(&view, &external_id)?;
            let targets = participant_targets(&identity, &participant, view.selector);
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
        let outcome = handle.finish().await;
        info!(
            %attachment,
            session = %session,
            %recording_id,
            group = ?member_of.as_ref().map(GroupKey::to_string),
            duration_ms = outcome.as_ref().map(|done| done.duration_ms),
            uris = ?outcome.as_ref().map(|done| done.uris.clone()),
            "recording finished"
        );
        if let Some(key) = member_of.as_ref() {
            self.leave_group(key, attachment);
        }
        Some(())
    }

    async fn open_tap_session(&self, view: SessionView) -> Result<OpenedSession, MediaPlaneError> {
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

        self.config
            .capabilities
            .report_first_contact(node, &transport)
            .await;

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

        let local_address = self.config.local_media_address.to_string();
        let answer_sdp = SubscriptionAnswer {
            session_id: self.config.sdp_session_id,
            local_address: &local_address,
            receive_ports: &receive_ports,
            format,
            answer_with,
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
                transport: Some(transport),
                external_id: view.external_id.clone(),
                call_id: view.call_id.clone(),
                to_tag,
                format,
                hub: hub_client,
                egress: None,
                stop,
                capture: Some(capture_thread),
                speakers,
            },
        );
        drop(held);
        self.metrics.register_session(view.id, shared_stats);
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

        let socket = UdpSocket::bind(SocketAddr::new(self.config.local_media_address, 0))
            .map_err(|error| MediaPlaneError(format!("media socket: {error}")))?;
        socket
            .set_nonblocking(true)
            .map_err(|error| MediaPlaneError(format!("media socket: {error}")))?;
        let receive_port = socket
            .local_addr()
            .map_err(|error| MediaPlaneError(format!("media socket: {error}")))?
            .port();
        let egress_socket = socket
            .try_clone()
            .map_err(|error| MediaPlaneError(format!("egress socket: {error}")))?;

        let local_address = self.config.local_media_address.to_string();
        let answer_sdp = offer
            .answer(self.config.sdp_session_id, &local_address, receive_port)
            .to_sdp()
            .map_err(|error| MediaPlaneError(format!("inline answer: {error}")))?;

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
        .with_shared_stats(Arc::clone(&shared), STALL_AFTER, epoch);

        let (mut hub, hub_client) = Hub::new();
        let stop = Arc::new(AtomicBool::new(false));
        let capture_stop = Arc::clone(&stop);
        let session = view.id;
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
                transport: None,
                external_id: view.external_id.clone(),
                call_id: view.call_id.clone(),
                to_tag: String::new(),
                format,
                hub: hub_client,
                egress: Some(egress_handle.clone()),
                stop,
                capture: Some(capture_thread),
                speakers: None,
            },
        );
        drop(held);
        self.metrics.register_session(view.id, vec![shared]);
        self.metrics
            .register_inline(view.id, egress_handle.shared());
        Ok(OpenedSession::answered(answer_sdp))
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
        let wav = match source {
            PlaybackSource::Blob(bytes) => bytes,
            PlaybackSource::File(path) => std::fs::read(&path)
                .map_err(|error| MediaPlaneError(format!("playback file {path}: {error}")))?,
            PlaybackSource::Stream => {
                return Err(MediaPlaneError(
                    "a streaming playback into an inline leg is an inject-capable \
                     attachment, not a playback"
                        .to_string(),
                ))
            }
        };
        let pcm = inline_playback_pcm(&wav, format)?;
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
        if let Some(thread) = live.capture.take() {
            let joined = tokio::task::spawn_blocking(move || thread.join()).await;
            if joined.is_err() {
                warn!(%session, "the capture thread did not join cleanly");
            }
        }
        self.metrics.retire_session(session);
        info!(%session, kind = ?live.kind, call_id = %live.call_id, "session closed");
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

        let SessionHandles { hub, .. } = self.session_handles(session)?;
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
            return self.play_into_inline_leg(session, source);
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
                    control_api::convert::track_name(track)
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

        async fn exists(&self, _key: &str) -> Result<bool, recorder::UploadError> {
            Ok(false)
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
            opus_decode_rate_hz: 16000,
            cookie_prefix: 1,
            sdp_session_id: 1,
            recording,
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
            rtpengine_node: node.to_string(),
            sdp_offer: None,
            sdp_answer: None,
            attachments: Vec::new(),
            authoritative: None,
        }
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
            spill_every: recorder::SPILL_EVERY,
            counters: Arc::new(RecorderCounters::default()),
            owner: "pod-a".to_string(),
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
            spill_every: recorder::SPILL_EVERY,
            counters: Arc::new(RecorderCounters::default()),
            owner: "pod-a".to_string(),
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
            participant_targets(&identity, "alice", TrackSelector::Only(Track::Customer)),
            vec![RecordingTarget {
                key: "acct-42/rec-99/alice.wav".to_string(),
                layout: Layout::Mono(Track::Customer),
            }]
        );
        assert_eq!(
            participant_targets(&identity, "alice", TrackSelector::All),
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
        let alice = participant_targets(&identity, "alice", TrackSelector::All);
        let bob = participant_targets(&identity, "bob", TrackSelector::All);

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
                &participant_targets(&first, "alice", TrackSelector::All),
            )
            .unwrap();
        let error = plane
            .join_group(
                &key,
                AttachmentId::from_raw(3),
                &second,
                &participant_targets(&second, "bob", TrackSelector::All),
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
                &participant_targets(&identity, "caller-0", TrackSelector::Only(Track::Customer)),
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
