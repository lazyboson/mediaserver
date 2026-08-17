use crate::consumer_ws::{self, ConsumerConfig};
use crate::hub::{Hub, HubClient, TrackSelection};
use crate::ng_transport::{NgTransport, NgTransportConfig};
use crate::tap_spike::{capture, TapLeg};
use control_api::{MediaPlane, MediaPlaneError, PlaybackSource};
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
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

pub const MAX_SESSION_DURATION: Duration = Duration::from_secs(8 * 3600);
pub const MAX_PLAYBACK_BLOB_BYTES: usize = 60_000;

const TARGET_DEPTH_PACKETS: u16 = 3;
const MAX_TAPPED_STREAMS: usize = 2;
const CONSUMER_QUEUE_FRAMES: usize = 200;
const TEXT_QUEUE_DEPTH: usize = 32;
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

struct LiveAttachment {
    session: SessionId,
    text: mpsc::Sender<String>,
    task: tokio::task::JoinHandle<Result<consumer_ws::ConsumerStats, consumer_ws::ConsumerError>>,
}

pub struct TapPlane {
    config: TapPlaneConfig,
    sessions: Mutex<HashMap<SessionId, LiveSession>>,
    attachments: Mutex<HashMap<AttachmentId, LiveAttachment>>,
}

impl TapPlane {
    pub fn new(config: TapPlaneConfig) -> Self {
        TapPlane {
            config,
            sessions: Mutex::new(HashMap::new()),
            attachments: Mutex::new(HashMap::new()),
        }
    }

    pub fn live_sessions(&self) -> usize {
        self.sessions.lock().map(|held| held.len()).unwrap_or(0)
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

        let mut legs = Vec::with_capacity(sockets.len());
        for (index, socket) in sockets.into_iter().enumerate() {
            let telephone_event = offer
                .streams
                .get(index)
                .and_then(|stream| stream.telephone_event())
                .map(|event| event.payload_type);
            legs.push(
                TapLeg::new(
                    track_for_stream(index),
                    socket,
                    format,
                    TARGET_DEPTH_PACKETS,
                    telephone_event,
                    RETAIN_NO_LOCAL_AUDIO,
                )
                .map_err(|error| MediaPlaneError(format!("tap leg: {error}")))?,
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
        info!(%session, call_id = %live.call_id, "tap closed");
        Ok(())
    }

    async fn open_attachment(&self, view: AttachmentView) -> Result<(), MediaPlaneError> {
        if view.transport != Transport::WsTwilio {
            return Err(MediaPlaneError(format!(
                "{} attachments are not served yet; only ws-twilio is",
                view.transport
            )));
        }
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
            LiveAttachment {
                session: view.session,
                text,
                task,
            },
        );
        Ok(())
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
        if let Some(live) = live {
            live.task.abort();
            info!(%attachment, session = %live.session, "consumer detached");
        }
        Ok(())
    }

    async fn send_text(
        &self,
        attachment: AttachmentId,
        json: String,
    ) -> Result<(), MediaPlaneError> {
        let sender = {
            let held = self
                .attachments
                .lock()
                .map_err(|_| MediaPlaneError("the attachment table is poisoned".to_string()))?;
            held.get(&attachment)
                .map(|live| live.text.clone())
                .ok_or_else(|| MediaPlaneError(format!("{attachment} is not connected here")))?
        };
        sender
            .send(json)
            .await
            .map_err(|_| MediaPlaneError(format!("{attachment} has stopped reading")))
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

fn track_for_stream(index: usize) -> Track {
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
    async fn only_the_websocket_transport_is_served_today() {
        let plane = plane();
        let error = plane
            .open_attachment(attachment(Transport::GrpcStream, "grpc-target"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not served yet"));
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
