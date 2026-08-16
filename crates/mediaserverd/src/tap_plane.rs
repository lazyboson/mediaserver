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
const MAX_TAPPED_LEGS: usize = 2;
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
    subscriptions: Vec<Subscription>,
    hub: HubClient,
    stop: Arc<AtomicBool>,
    capture: Option<std::thread::JoinHandle<()>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Subscription {
    from_tag: String,
    to_tag: String,
    track: Track,
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
    ) -> Result<(Arc<NgTransport>, String, HubClient, String), MediaPlaneError> {
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

        if view.from_tags.is_empty() || view.from_tags.len() > MAX_TAPPED_LEGS {
            return Err(MediaPlaneError(format!(
                "a tap needs 1 to {MAX_TAPPED_LEGS} from-tags; this session named {}",
                view.from_tags.len()
            )));
        }

        let mut legs = Vec::with_capacity(view.from_tags.len());
        let mut subscriptions: Vec<Subscription> = Vec::with_capacity(view.from_tags.len());

        for (index, from_tag) in view.from_tags.iter().enumerate() {
            let track = voice_the_participant_hears(index);
            let opened = subscribe_one_leg(
                &transport,
                &view.call_id,
                from_tag,
                track,
                self.config.local_media_address,
                self.config.sdp_session_id.wrapping_add(index as u64),
                format,
            )
            .await;
            match opened {
                Ok((leg, to_tag)) => {
                    legs.push(leg);
                    subscriptions.push(Subscription {
                        from_tag: from_tag.clone(),
                        to_tag,
                        track,
                    });
                }
                Err(error) => {
                    for opened in &subscriptions {
                        if let Err(cleanup) =
                            transport.unsubscribe(&view.call_id, &opened.to_tag).await
                        {
                            warn!(
                                from_tag = %opened.from_tag,
                                %cleanup,
                                "could not undo a subscription after a later leg failed"
                            );
                        }
                    }
                    return Err(error);
                }
            }
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
            legs = subscriptions.len(),
            tracks = ?subscriptions.iter().map(|held| held.track).collect::<Vec<_>>(),
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
                subscriptions,
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
        for held in &live.subscriptions {
            if let Err(error) = live
                .transport
                .unsubscribe(&live.call_id, &held.to_tag)
                .await
            {
                warn!(
                    %session,
                    from_tag = %held.from_tag,
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

        let (_, call_id, hub, external_id) = self.session_handles(view.session)?;
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
        let (transport, call_id, _, _) = self.session_handles(session)?;
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
        let (transport, call_id, _, _) = self.session_handles(session)?;
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

#[allow(clippy::too_many_arguments)]
async fn subscribe_one_leg(
    transport: &NgTransport,
    call_id: &str,
    from_tag: &str,
    track: Track,
    local_media_address: IpAddr,
    sdp_session_id: u64,
    format: AudioFormat,
) -> Result<(TapLeg, String), MediaPlaneError> {
    let reply = transport
        .subscribe_request(&SubscribeRequest {
            call_id: call_id.to_string(),
            from_tags: vec![from_tag.to_string()],
            mix: false,
            accept_codecs: Vec::new(),
            transcode_codecs: vec![format.encoding.rtpmap_name().to_string()],
            label: Some(format!("mss-tap-{}", track_label(track))),
        })
        .await
        .map_err(|error| MediaPlaneError(format!("subscribe request for {from_tag}: {error}")))?;

    let offer_sdp = reply
        .sdp()
        .ok_or_else(|| MediaPlaneError(format!("no sdp in the answer for {from_tag}")))?
        .to_string();
    let to_tag = reply
        .to_tag()
        .ok_or_else(|| MediaPlaneError(format!("no to-tag in the answer for {from_tag}")))?
        .to_string();
    let offer = SubscriptionOffer::parse(&offer_sdp)
        .map_err(|error| MediaPlaneError(format!("subscription sdp for {from_tag}: {error}")))?;

    if offer.streams.len() != 1 {
        if let Err(cleanup) = transport.unsubscribe(call_id, &to_tag).await {
            warn!(%from_tag, %cleanup, "could not undo an unusable subscription");
        }
        return Err(MediaPlaneError(format!(
            "asked rtpengine for {from_tag} alone and it offered {} streams, \
             so leg identity would be a guess again",
            offer.streams.len()
        )));
    }

    let socket = UdpSocket::bind(SocketAddr::new(local_media_address, 0))
        .map_err(|error| MediaPlaneError(format!("media socket: {error}")))?;
    let port = socket
        .local_addr()
        .map_err(|error| MediaPlaneError(format!("media socket: {error}")))?
        .port();
    let local_address = local_media_address.to_string();
    let answer_sdp = SubscriptionAnswer {
        session_id: sdp_session_id,
        local_address: &local_address,
        receive_ports: &[port],
        format,
    }
    .to_sdp(&offer)
    .map_err(|error| MediaPlaneError(format!("answer sdp for {from_tag}: {error}")))?;

    transport
        .subscribe_answer(call_id, &to_tag, &answer_sdp)
        .await
        .map_err(|error| MediaPlaneError(format!("subscribe answer for {from_tag}: {error}")))?;

    let telephone_event = offer.streams[0]
        .telephone_event()
        .map(|event| event.payload_type);
    info!(
        %from_tag,
        ?track,
        receive_port = port,
        source_port = offer.streams[0].port,
        payload_types = ?offer.streams[0].payload_types,
        "subscribed to one participant so its identity is not a guess"
    );

    let leg = TapLeg::new(
        track,
        socket,
        format,
        TARGET_DEPTH_PACKETS,
        telephone_event,
        RETAIN_NO_LOCAL_AUDIO,
    )
    .map_err(|error| MediaPlaneError(format!("tap leg for {from_tag}: {error}")))?;
    Ok((leg, to_tag))
}

fn voice_the_participant_hears(index: usize) -> Track {
    if index == 0 {
        Track::Agent
    } else {
        Track::Customer
    }
}

fn track_label(track: Track) -> &'static str {
    match track {
        Track::Customer => "customer",
        Track::Agent => "agent",
        Track::Mixed => "mixed",
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

    const ONE_STREAM_OFFER: &str = "v=0\r\n\
o=- 8000 8000 IN IP4 127.0.0.1\r\n\
s=rtpengine\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 30000 RTP/AVP 0 101\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=sendonly\r\n";

    fn bencode_reply(cookie: &[u8], sdp: Option<&str>, to_tag: Option<&str>) -> Vec<u8> {
        let mut out = Vec::from(cookie);
        out.push(b' ');
        out.extend_from_slice(b"d6:result2:ok");
        if let Some(sdp) = sdp {
            out.extend_from_slice(format!("3:sdp{}:{}", sdp.len(), sdp).as_bytes());
        }
        if let Some(tag) = to_tag {
            out.extend_from_slice(format!("6:to-tag{}:{}", tag.len(), tag).as_bytes());
        }
        out.push(b'e');
        out
    }

    struct FakeNode {
        addr: SocketAddr,
        commands: Arc<Mutex<Vec<String>>>,
    }

    impl FakeNode {
        async fn spawn(fail_after: usize) -> FakeNode {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            let commands = Arc::new(Mutex::new(Vec::new()));
            let log = Arc::clone(&commands);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let mut subscribes = 0usize;
                loop {
                    let Ok((len, from)) = socket.recv_from(&mut buf).await else {
                        return;
                    };
                    let datagram = &buf[..len];
                    let space = datagram.iter().position(|byte| *byte == b' ').unwrap();
                    let cookie = &datagram[..space];
                    let body = String::from_utf8_lossy(&datagram[space + 1..]).to_string();
                    log.lock().unwrap().push(body.clone());

                    let reply = if body.contains("subscribe request") {
                        subscribes += 1;
                        if fail_after > 0 && subscribes > fail_after {
                            let mut out = Vec::from(cookie);
                            out.push(b' ');
                            out.extend_from_slice(
                                b"d6:result5:error12:error-reason12:Unknown calle",
                            );
                            out
                        } else {
                            bencode_reply(
                                cookie,
                                Some(ONE_STREAM_OFFER),
                                Some(&format!("to-tag-{subscribes}")),
                            )
                        }
                    } else {
                        bencode_reply(cookie, None, None)
                    };
                    let _ = socket.send_to(&reply, from).await;
                }
            });
            FakeNode { addr, commands }
        }

        fn subscribe_tags(&self) -> Vec<String> {
            self.commands
                .lock()
                .unwrap()
                .iter()
                .filter(|body| body.contains("subscribe request"))
                .map(|body| {
                    let at = body.find("from-tags").unwrap();
                    body[at..].chars().take(40).collect::<String>()
                })
                .collect()
        }

        fn unsubscribes(&self) -> usize {
            self.commands
                .lock()
                .unwrap()
                .iter()
                .filter(|body| body.contains("unsubscribe"))
                .count()
        }
    }

    fn plane_for(node: SocketAddr) -> TapPlane {
        TapPlane::new(TapPlaneConfig {
            default_node: Some(node),
            local_media_address: IpAddr::from([127, 0, 0, 1]),
            format: AudioFormat::pcmu_8k_20ms(),
            cookie_prefix: 7,
            sdp_session_id: 7,
        })
    }

    #[tokio::test]
    async fn each_participant_gets_its_own_subscription_so_identity_is_not_positional() {
        let node = FakeNode::spawn(0).await;
        let plane = plane_for(node.addr);
        let mut view = session(SessionKind::Tap, "");
        view.from_tags = vec!["caller-tag".to_string(), "agent-tag".to_string()];

        plane.open_session(view).await.unwrap();

        let asked = node.subscribe_tags();
        assert_eq!(asked.len(), 2, "one subscribe per participant");
        assert!(asked[0].contains("caller-tag"), "{:?}", asked[0]);
        assert!(!asked[0].contains("agent-tag"), "{:?}", asked[0]);
        assert!(asked[1].contains("agent-tag"), "{:?}", asked[1]);
        assert!(!asked[1].contains("caller-tag"), "{:?}", asked[1]);
        assert_eq!(plane.live_sessions(), 1);

        plane.close_session(SessionId::from_raw(1)).await.unwrap();
        assert_eq!(node.unsubscribes(), 2, "every subscription is undone");
        assert_eq!(plane.live_sessions(), 0);
    }

    #[tokio::test]
    async fn the_first_leg_is_undone_when_a_later_one_cannot_be_subscribed() {
        let node = FakeNode::spawn(1).await;
        let plane = plane_for(node.addr);
        let mut view = session(SessionKind::Tap, "");
        view.from_tags = vec!["caller-tag".to_string(), "agent-tag".to_string()];

        let error = plane.open_session(view).await.unwrap_err();

        assert!(error.to_string().contains("agent-tag"), "{error}");
        assert_eq!(plane.live_sessions(), 0, "no half-built session survives");
        assert_eq!(node.unsubscribes(), 1, "the leg that did open is undone");
    }

    #[tokio::test]
    async fn a_single_tag_session_taps_exactly_one_leg() {
        let node = FakeNode::spawn(0).await;
        let plane = plane_for(node.addr);
        let mut view = session(SessionKind::Tap, "");
        view.from_tags = vec!["only-tag".to_string()];

        plane.open_session(view).await.unwrap();

        assert_eq!(node.subscribe_tags().len(), 1);
        assert_eq!(plane.live_sessions(), 1);
        plane.close_session(SessionId::from_raw(1)).await.unwrap();
    }

    #[tokio::test]
    async fn more_participants_than_a_tap_handles_is_refused() {
        let plane = plane();
        let mut view = session(SessionKind::Tap, "127.0.0.1:22222");
        view.from_tags = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let error = plane.open_session(view).await.unwrap_err();
        assert!(error.to_string().contains("from-tags"), "{error}");

        let mut empty = session(SessionKind::Tap, "127.0.0.1:22222");
        empty.from_tags = Vec::new();
        let error = plane.open_session(empty).await.unwrap_err();
        assert!(error.to_string().contains("from-tags"), "{error}");
    }

    #[test]
    fn a_subscription_is_named_for_the_voice_it_carries_not_the_tag_it_used() {
        assert_eq!(voice_the_participant_hears(0), Track::Agent);
        assert_eq!(voice_the_participant_hears(1), Track::Customer);
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
