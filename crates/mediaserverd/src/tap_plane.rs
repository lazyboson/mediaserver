use crate::consumer_ws::{self, ConsumerConfig};
use crate::hub::{Hub, HubClient, Subscription, SubscriptionMetrics, TapEvent, TrackSelection};
use crate::ng_transport::{NgTransport, NgTransportConfig};
use crate::tap_spike::{capture, SharedLegStats, TapLeg};
use control_api::{MediaPlane, MediaPlaneError, PlaybackSource, StreamFrame};
use media_core::{AudioFormat, Track};
use rtpengine_ng::{
    PlayMedia, PlaySource, PlayTarget, SubscribeRequest, SubscriptionAnswer, SubscriptionOffer,
};
use session_core::{
    AttachmentId, AttachmentView, SessionId, SessionKind, SessionView, TrackSelector, Transport,
};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
const ACCOUNT_METADATA_KEY: &str = "accountId";
const STREAM_SID_METADATA_KEY: &str = "streamSid";
const DEFAULT_ACCOUNT_ID: &str = "mss";

pub struct TapPlaneConfig {
    pub default_node: Option<SocketAddr>,
    pub local_media_address: IpAddr,
    pub format: AudioFormat,
    pub cookie_prefix: u64,
    pub sdp_session_id: u64,
}

struct LiveSession {
    transport: Arc<NgTransport>,
    external_id: String,
    call_id: String,
    to_tag: String,
    hub: HubClient,
    stop: Arc<AtomicBool>,
    capture: Option<std::thread::JoinHandle<()>>,
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
        live: Option<GrpcLive>,
    },
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
    pub consumers_live: u64,
    pub consumer_dropped_oldest: u64,
    pub consumer_delivered: u64,
    pub consumer_queue_depth: u64,
    pub consumer_queue_depth_max: u64,
}

#[derive(Default)]
struct MetricsInner {
    retired: LegTotals,
    retired_consumer_dropped: u64,
    retired_consumer_delivered: u64,
    legs: HashMap<SessionId, Vec<Arc<SharedLegStats>>>,
    consumers: HashMap<AttachmentId, SubscriptionMetrics>,
}

#[derive(Clone, Default)]
pub struct TapPlaneMetrics(Arc<Mutex<MetricsInner>>);

impl TapPlaneMetrics {
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
        let mut snapshot = IngestSnapshot {
            totals: inner.retired,
            sessions_live: inner.legs.len() as u64,
            consumers_live: inner.consumers.len() as u64,
            consumer_dropped_oldest: inner.retired_consumer_dropped,
            consumer_delivered: inner.retired_consumer_delivered,
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
}

impl TapPlane {
    pub fn new(config: TapPlaneConfig) -> Self {
        TapPlane {
            config,
            sessions: Mutex::new(HashMap::new()),
            attachments: Mutex::new(HashMap::new()),
            metrics: TapPlaneMetrics::default(),
        }
    }

    pub fn live_sessions(&self) -> usize {
        self.sessions.lock().map(|held| held.len()).unwrap_or(0)
    }

    pub fn metrics(&self) -> TapPlaneMetrics {
        self.metrics.clone()
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
        if view.format != self.config.format {
            return Err(MediaPlaneError(format!(
                "a grpc-stream attachment must use the tap format {:?}; \
                 per-consumer re-encode is not built yet",
                self.config.format
            )));
        }
        self.session_handles(view.session)?;
        let mut held = self
            .attachments
            .lock()
            .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
        held.insert(
            view.id,
            LiveAttachment::Grpc {
                session: view.session,
                selection: selection_of(view.selector),
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
        let format = self.config.format;

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
                transcode_codecs: vec![format.encoding.rtpmap_name().to_string()],
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

        let ssrc_tracks = speaker_ssrcs(&transport, &view).await;

        let mut legs = Vec::with_capacity(sockets.len());
        let mut shared_stats = Vec::with_capacity(sockets.len());
        for (index, socket) in sockets.into_iter().enumerate() {
            let telephone_event = offer
                .streams
                .get(index)
                .and_then(|stream| stream.telephone_event())
                .map(|event| event.payload_type);
            let shared = Arc::new(SharedLegStats::default());
            shared_stats.push(Arc::clone(&shared));
            legs.push(
                TapLeg::new(
                    speaker_track(index),
                    socket,
                    format,
                    TARGET_DEPTH_PACKETS,
                    telephone_event,
                    RETAIN_NO_LOCAL_AUDIO,
                )
                .map_err(|error| MediaPlaneError(format!("tap leg: {error}")))?
                .with_ssrc_tracks(ssrc_tracks.clone())
                .with_shared_stats(shared, STALL_AFTER, Instant::now()),
            );
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
                hub: hub_client,
                stop,
                capture: Some(capture_thread),
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

        live.stop.store(true, Ordering::Relaxed);
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
            other => Err(MediaPlaneError(format!(
                "{other} attachments are not served yet; ws-twilio and grpc-stream are"
            ))),
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
        let selection = {
            let held = self
                .attachments
                .lock()
                .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
            match held.get(&attachment) {
                Some(LiveAttachment::Grpc {
                    session: held_session,
                    selection,
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
                    *selection
                }
                Some(LiveAttachment::Ws { .. }) => {
                    return Err(MediaPlaneError(format!(
                        "{attachment} is a websocket attachment; it has no grpc stream"
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
        let pump = tokio::spawn(pump_frames(subscription, frames.clone()));

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

async fn pump_frames(mut subscription: Subscription, frames: mpsc::Sender<StreamFrame>) {
    while let Some(event) = subscription.next().await {
        let frame = match event {
            TapEvent::Media {
                track,
                timestamp_ms,
                len,
                bytes,
            } => StreamFrame::Media {
                track: control_api::convert::track_name(track),
                pts_ms: timestamp_ms,
                payload: bytes[..len].to_vec(),
            },
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

fn tracks_of(selector: TrackSelector) -> Vec<String> {
    match selector {
        TrackSelector::All => vec![
            consumer_ws::track_name(Track::Customer).to_string(),
            consumer_ws::track_name(Track::Agent).to_string(),
        ],
        TrackSelector::Only(track) => vec![consumer_ws::track_name(track).to_string()],
    }
}

async fn speaker_ssrcs(transport: &NgTransport, view: &SessionView) -> Vec<(u32, Track)> {
    let reply = match transport.query(&view.call_id).await {
        Ok(reply) => reply,
        Err(error) => {
            warn!(
                call_id = %view.call_id,
                %error,
                "query failed; leg identity falls back to stream order"
            );
            return Vec::new();
        }
    };
    let mut ssrc_tracks = Vec::new();
    for (tag, ssrc) in reply.ssrc_by_tag() {
        let Some(position) = view.from_tags.iter().position(|held| *held == tag) else {
            continue;
        };
        let speaker = speaker_track(position);
        info!(
            call_id = %view.call_id,
            %tag,
            ssrc,
            ?speaker,
            "the leg carrying this ssrc is this participant's own voice"
        );
        ssrc_tracks.push((ssrc, speaker));
    }
    if ssrc_tracks.is_empty() {
        warn!(
            call_id = %view.call_id,
            "rtpengine reported no ssrcs for the requested tags; \
             leg identity falls back to stream order"
        );
    }
    ssrc_tracks
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

    fn plane() -> TapPlane {
        TapPlane::new(TapPlaneConfig {
            default_node: None,
            local_media_address: IpAddr::from([127, 0, 0, 1]),
            format: AudioFormat::pcmu_8k_20ms(),
            cookie_prefix: 1,
            sdp_session_id: 1,
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
            .open_attachment(attachment(Transport::FileS3, "acct/rec.wav"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not served yet"));
        assert!(error.to_string().contains("file-s3"));
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
    async fn a_grpc_attachment_must_use_the_tap_format_until_reencode_exists() {
        let plane = plane();
        let mut wrong = attachment(Transport::GrpcStream, "");
        wrong.format = AudioFormat::l16_16k_20ms();
        let error = plane.open_attachment(wrong).await.unwrap_err();
        assert!(error.to_string().contains("re-encode"));
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
}
