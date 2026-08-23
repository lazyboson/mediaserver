use control_api::proto::media_control_client::MediaControlClient;
use control_api::proto::media_stream_client::MediaStreamClient;
use control_api::proto::{self, consumer_to_server, server_to_consumer};
use control_api::{
    serve_authenticated_until, AuthPolicy, MediaPlane, MediaPlaneError, OpenedSession,
    PlaybackSource, SessionController, StreamFrame,
};
use session_core::{AttachmentId, PlaybackId, SessionId};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const SCHEME_SEPARATOR: &str = "\x2f\x2f";

#[derive(Default)]
struct FakePlane {
    stream: Mutex<Option<mpsc::Receiver<StreamFrame>>>,
    playbacks: Mutex<Vec<Vec<u8>>>,
    stopped: Mutex<Vec<String>>,
}

#[control_api::async_trait]
impl MediaPlane for FakePlane {
    async fn open_session(
        &self,
        _session: session_core::SessionView,
    ) -> Result<OpenedSession, MediaPlaneError> {
        Ok(OpenedSession::default())
    }

    async fn close_session(&self, _session: SessionId) -> Result<(), MediaPlaneError> {
        Ok(())
    }

    async fn open_attachment(
        &self,
        _attachment: session_core::AttachmentView,
    ) -> Result<(), MediaPlaneError> {
        Ok(())
    }

    async fn close_attachment(
        &self,
        _session: SessionId,
        _attachment: AttachmentId,
    ) -> Result<(), MediaPlaneError> {
        Ok(())
    }

    async fn send_text(
        &self,
        _attachment: AttachmentId,
        _json: String,
    ) -> Result<(), MediaPlaneError> {
        Ok(())
    }

    async fn start_playback(
        &self,
        _session: SessionId,
        _playback: PlaybackId,
        source: PlaybackSource,
        _target_tag: Option<String>,
        block_egress: bool,
    ) -> Result<(), MediaPlaneError> {
        assert!(block_egress);
        match source {
            PlaybackSource::Blob(blob) => self.playbacks.lock().unwrap().push(blob),
            other => panic!("expected a blob playback, got {other:?}"),
        }
        Ok(())
    }

    async fn stop_playback(
        &self,
        _session: SessionId,
        playback: PlaybackId,
        target_tag: Option<String>,
    ) -> Result<(), MediaPlaneError> {
        self.stopped
            .lock()
            .unwrap()
            .push(format!("{playback}/{}", target_tag.unwrap_or_default()));
        Ok(())
    }

    async fn open_stream(
        &self,
        _session: SessionId,
        _attachment: AttachmentId,
    ) -> Result<mpsc::Receiver<StreamFrame>, MediaPlaneError> {
        self.stream
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| MediaPlaneError("this fake has no stream left".to_string()))
    }
}

struct Wire {
    endpoint: String,
    plane: Arc<FakePlane>,
    frames: mpsc::Sender<StreamFrame>,
    stop: oneshot::Sender<()>,
    serving: tokio::task::JoinHandle<()>,
}

async fn listening(auth: AuthPolicy) -> Wire {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (frames, receiver) = mpsc::channel(64);
    let plane = Arc::new(FakePlane::default());
    *plane.stream.lock().unwrap() = Some(receiver);
    let serving_plane = Arc::clone(&plane);
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(async move {
        let controller =
            Arc::new(SessionController::new("stream-pod").with_media_plane(serving_plane));
        serve_authenticated_until(controller, auth, listener, async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    Wire {
        endpoint: format!("http:{SCHEME_SEPARATOR}127.0.0.1:{port}"),
        plane,
        frames,
        stop,
        serving,
    }
}

async fn session_with_grpc_attachment(
    endpoint: &str,
    capabilities: Vec<i32>,
    bearer: Option<&str>,
) -> String {
    let mut client = MediaControlClient::connect(endpoint.to_string())
        .await
        .unwrap();
    let mut create = Request::new(proto::CreateSessionRequest {
        external_id: "req-stream".to_string(),
        kind: proto::SessionKind::Tap as i32,
        call_id: "call-stream".to_string(),
        from_tags: vec!["from-a".to_string()],
        rtpengine_node: "rtpengine-1".to_string(),
        mix: false,
        idempotency_key: String::new(),
        sdp_offer: String::new(),
    });
    if let Some(token) = bearer {
        create
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    let session = client.create_session(create).await.unwrap().into_inner();

    let mut attach = Request::new(proto::AttachRequest {
        session: Some(proto::SessionRef {
            id: Some(proto::session_ref::Id::SessionId(session.session_id)),
        }),
        transport: proto::Transport::GrpcStream as i32,
        capabilities,
        selector: Some(proto::TrackSelector {
            select: Some(proto::track_selector::Select::Only("customer".to_string())),
        }),
        format: None,
        authoritative: false,
        label: "grpc-consumer".to_string(),
        endpoint: String::new(),
        group: String::new(),
        metadata: [("streamSid".to_string(), "MZ-stream".to_string())]
            .into_iter()
            .collect(),
        idempotency_key: String::new(),
    });
    if let Some(token) = bearer {
        attach
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    client
        .attach(attach)
        .await
        .unwrap()
        .into_inner()
        .attachment_id
}

fn hello(attachment_id: &str, token: &str) -> proto::ConsumerToServer {
    proto::ConsumerToServer {
        msg: Some(consumer_to_server::Msg::Hello(proto::ConsumerHello {
            attachment_id: attachment_id.to_string(),
            token: token.to_string(),
            requested_format: None,
        })),
    }
}

fn inject(payload: Vec<u8>) -> proto::ConsumerToServer {
    proto::ConsumerToServer {
        msg: Some(consumer_to_server::Msg::Inject(proto::AudioFrame {
            track: String::new(),
            seq: 0,
            pts_ms: 0,
            payload,
        })),
    }
}

fn mark(name: &str) -> proto::ConsumerToServer {
    proto::ConsumerToServer {
        msg: Some(consumer_to_server::Msg::Mark(proto::Mark {
            name: name.to_string(),
        })),
    }
}

fn speech_report(kind: proto::SpeechReportKind, text: &str) -> proto::ConsumerToServer {
    proto::ConsumerToServer {
        msg: Some(consumer_to_server::Msg::Report(proto::SpeechReport {
            kind: kind as i32,
            track: "customer".to_string(),
            text: text.to_string(),
            confidence: 0.9,
            observed_at: None,
            reason: String::new(),
        })),
    }
}

fn clear() -> proto::ConsumerToServer {
    proto::ConsumerToServer {
        msg: Some(consumer_to_server::Msg::Clear(proto::Clear {})),
    }
}

async fn poll_until(deadline: Duration, mut done: impl FnMut() -> bool) -> bool {
    let started = tokio::time::Instant::now();
    while started.elapsed() < deadline {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    done()
}

#[tokio::test]
async fn a_grpc_consumer_receives_start_frames_dtmf_and_a_stop() {
    let wire = listening(AuthPolicy::open()).await;
    let attachment =
        session_with_grpc_attachment(&wire.endpoint, vec![proto::Capability::Sink as i32], None)
            .await;

    let mut client = MediaStreamClient::connect(wire.endpoint.clone())
        .await
        .unwrap();
    let (to_server, outbound) = mpsc::channel(8);
    to_server.send(hello(&attachment, "")).await.unwrap();
    let mut inbound = client
        .subscribe(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();

    let start = inbound.message().await.unwrap().unwrap();
    match start.msg {
        Some(server_to_consumer::Msg::Start(start)) => {
            assert_eq!(start.external_id, "req-stream");
            assert_eq!(start.attachment_id, attachment);
            assert_eq!(start.stream_sid, "MZ-stream");
            assert_eq!(start.tracks, vec!["customer".to_string()]);
            assert_eq!(start.format.unwrap().encoding, proto::Encoding::Pcmu as i32);
        }
        other => panic!("expected the start frame first, got {other:?}"),
    }

    wire.frames
        .send(StreamFrame::Media {
            track: "customer",
            pts_ms: 20,
            payload: vec![0xFF; 160],
        })
        .await
        .unwrap();
    let media = inbound.message().await.unwrap().unwrap();
    match media.msg {
        Some(server_to_consumer::Msg::Frame(frame)) => {
            assert_eq!(frame.track, "customer");
            assert_eq!(frame.seq, 1);
            assert_eq!(frame.pts_ms, 20);
            assert_eq!(frame.payload.len(), 160);
        }
        other => panic!("expected a media frame, got {other:?}"),
    }

    wire.frames
        .send(StreamFrame::Dtmf {
            track: "customer",
            digit: '7',
        })
        .await
        .unwrap();
    let dtmf = inbound.message().await.unwrap().unwrap();
    match dtmf.msg {
        Some(server_to_consumer::Msg::Dtmf(dtmf)) => {
            assert_eq!(dtmf.digit, "7");
        }
        other => panic!("expected a dtmf frame, got {other:?}"),
    }

    drop(wire.frames);
    let stop = inbound.message().await.unwrap().unwrap();
    match stop.msg {
        Some(server_to_consumer::Msg::Stop(stop)) => {
            assert!(stop.reason.contains("ended"));
        }
        other => panic!("expected a stop frame, got {other:?}"),
    }
    assert!(inbound.message().await.unwrap().is_none());

    let _ = wire.stop.send(());
    wire.serving.await.unwrap();
}

#[tokio::test]
async fn a_detached_attachment_reaches_the_consumer_as_a_stop_frame_carrying_its_reason() {
    let wire = listening(AuthPolicy::open()).await;
    let attachment =
        session_with_grpc_attachment(&wire.endpoint, vec![proto::Capability::Sink as i32], None)
            .await;

    let mut client = MediaStreamClient::connect(wire.endpoint.clone())
        .await
        .unwrap();
    let (to_server, outbound) = mpsc::channel(8);
    to_server.send(hello(&attachment, "")).await.unwrap();
    let mut inbound = client
        .subscribe(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    inbound.message().await.unwrap().unwrap();

    wire.frames
        .send(StreamFrame::Stop {
            reason: "the attachment was detached".to_string(),
        })
        .await
        .unwrap();

    let stop = inbound.message().await.unwrap().unwrap();
    match stop.msg {
        Some(server_to_consumer::Msg::Stop(stop)) => {
            assert_eq!(stop.reason, "the attachment was detached");
        }
        other => panic!("expected a stop frame, got {other:?}"),
    }
    assert!(inbound.message().await.unwrap().is_none());

    let _ = wire.stop.send(());
    wire.serving.await.unwrap();
}

#[tokio::test]
async fn an_unprivileged_inject_is_a_protocol_violation_not_a_no_op() {
    let wire = listening(AuthPolicy::open()).await;
    let attachment =
        session_with_grpc_attachment(&wire.endpoint, vec![proto::Capability::Sink as i32], None)
            .await;

    let mut client = MediaStreamClient::connect(wire.endpoint.clone())
        .await
        .unwrap();
    let (to_server, outbound) = mpsc::channel(8);
    to_server.send(hello(&attachment, "")).await.unwrap();
    let mut inbound = client
        .subscribe(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    inbound.message().await.unwrap().unwrap();

    to_server.send(inject(vec![0xFF; 160])).await.unwrap();
    let denied = loop {
        match inbound.message().await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("the stream ended without the denial"),
            Err(status) => break status,
        }
    };
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    assert!(denied.message().contains("INJECT"));

    let _ = wire.stop.send(());
    wire.serving.await.unwrap();
}

#[tokio::test]
async fn an_authorized_utterance_becomes_a_playback_and_clear_stops_it() {
    let wire = listening(AuthPolicy::open()).await;
    let attachment = session_with_grpc_attachment(
        &wire.endpoint,
        vec![
            proto::Capability::Sink as i32,
            proto::Capability::Inject as i32,
        ],
        None,
    )
    .await;

    let mut client = MediaStreamClient::connect(wire.endpoint.clone())
        .await
        .unwrap();
    let (to_server, outbound) = mpsc::channel(8);
    to_server.send(hello(&attachment, "")).await.unwrap();
    let mut inbound = client
        .subscribe(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    inbound.message().await.unwrap().unwrap();

    to_server.send(inject(vec![0x00; 320])).await.unwrap();
    to_server.send(mark("utt-1")).await.unwrap();
    let plane = Arc::clone(&wire.plane);
    assert!(
        poll_until(Duration::from_secs(5), || !plane
            .playbacks
            .lock()
            .unwrap()
            .is_empty())
        .await,
        "the marked utterance never reached the media plane"
    );
    {
        let playbacks = wire.plane.playbacks.lock().unwrap();
        assert_eq!(playbacks.len(), 1);
        assert_eq!(&playbacks[0][..4], b"RIFF");
        assert_eq!(&playbacks[0][8..12], b"WAVE");
    }

    to_server.send(clear()).await.unwrap();
    let plane = Arc::clone(&wire.plane);
    assert!(
        poll_until(Duration::from_secs(5), || !plane
            .stopped
            .lock()
            .unwrap()
            .is_empty())
        .await,
        "clear never stopped the playback"
    );

    let _ = wire.stop.send(());
    wire.serving.await.unwrap();
}

#[tokio::test]
async fn a_subscribe_stream_ends_when_the_pod_drains() {
    let wire = listening(AuthPolicy::open()).await;
    let attachment =
        session_with_grpc_attachment(&wire.endpoint, vec![proto::Capability::Sink as i32], None)
            .await;

    let mut client = MediaStreamClient::connect(wire.endpoint.clone())
        .await
        .unwrap();
    let (to_server, outbound) = mpsc::channel(8);
    to_server.send(hello(&attachment, "")).await.unwrap();
    let mut inbound = client
        .subscribe(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    inbound.message().await.unwrap().unwrap();

    let _ = wire.stop.send(());
    let mut saw_draining_stop = false;
    while let Ok(Some(message)) = inbound.message().await {
        if let Some(server_to_consumer::Msg::Stop(stop)) = message.msg {
            saw_draining_stop = stop.reason.contains("draining");
        }
    }
    assert!(saw_draining_stop);

    tokio::time::timeout(Duration::from_secs(5), wire.serving)
        .await
        .expect("draining must not wait forever on an open subscribe stream")
        .unwrap();
}

#[tokio::test]
async fn the_shared_secret_guards_media_control_and_the_consumer_hello() {
    let wire = listening(AuthPolicy::shared_secret("s3cret")).await;

    let mut control = MediaControlClient::connect(wire.endpoint.clone())
        .await
        .unwrap();
    let refused = control
        .create_session(proto::CreateSessionRequest {
            external_id: "req-auth".to_string(),
            kind: proto::SessionKind::Tap as i32,
            call_id: "call-auth".to_string(),
            from_tags: Vec::new(),
            rtpengine_node: String::new(),
            mix: false,
            idempotency_key: String::new(),
            sdp_offer: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);

    let attachment = session_with_grpc_attachment(
        &wire.endpoint,
        vec![proto::Capability::Sink as i32],
        Some("s3cret"),
    )
    .await;

    let mut stream = MediaStreamClient::connect(wire.endpoint.clone())
        .await
        .unwrap();
    let (to_server, outbound) = mpsc::channel(8);
    to_server.send(hello(&attachment, "wrong")).await.unwrap();
    let refused = stream
        .subscribe(ReceiverStream::new(outbound))
        .await
        .map(|_| ())
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);

    let (to_server, outbound) = mpsc::channel(8);
    to_server.send(hello(&attachment, "s3cret")).await.unwrap();
    let mut inbound = stream
        .subscribe(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    let start = inbound.message().await.unwrap().unwrap();
    assert!(matches!(start.msg, Some(server_to_consumer::Msg::Start(_))));

    let _ = wire.stop.send(());
    wire.serving.await.unwrap();
}

#[tokio::test]
async fn legacy_telsvc_verbs_stay_open_because_their_clients_cannot_change() {
    use control_api::telcompat_proto::tel_service_client::TelServiceClient;
    use control_api::telcompat_proto::StreamRequest;

    let wire = listening(AuthPolicy::shared_secret("s3cret")).await;
    let mut legacy = TelServiceClient::connect(wire.endpoint.clone())
        .await
        .unwrap();
    legacy
        .start_stream(StreamRequest {
            request_uuid: "9f14e45f-ea34-4b2c-9a3f-1d2e3f4a5b6c".to_string(),
            acc_id: "acct-1".to_string(),
            stream_sid: "MZ-1".to_string(),
            ws_url: "wss-endpoint".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    let _ = wire.stop.send(());
    wire.serving.await.unwrap();
}

#[tokio::test]
async fn a_consumer_speech_report_reaches_the_event_bus_as_the_event_it_names() {
    let wire = listening(AuthPolicy::open()).await;
    let attachment = session_with_grpc_attachment(
        &wire.endpoint,
        vec![
            proto::Capability::Sink as i32,
            proto::Capability::Events as i32,
        ],
        None,
    )
    .await;

    let mut control = MediaControlClient::connect(wire.endpoint.clone())
        .await
        .unwrap();
    let mut events = control
        .watch_events(Request::new(proto::WatchRequest { session: None }))
        .await
        .unwrap()
        .into_inner();

    let mut client = MediaStreamClient::connect(wire.endpoint.clone())
        .await
        .unwrap();
    let (to_server, outbound) = mpsc::channel(8);
    to_server.send(hello(&attachment, "")).await.unwrap();
    let mut inbound = client
        .subscribe(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    inbound.message().await.unwrap().unwrap();

    to_server
        .send(speech_report(proto::SpeechReportKind::Started, ""))
        .await
        .unwrap();
    to_server
        .send(speech_report(
            proto::SpeechReportKind::Final,
            "stop talking",
        ))
        .await
        .unwrap();

    let mut seen = Vec::new();
    while seen.len() < 2 {
        let event = tokio::time::timeout(Duration::from_secs(5), events.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event.attachment_id, attachment);
        match event.payload {
            Some(proto::media_event::Payload::SpeechStarted(started)) => {
                seen.push(format!("started/{}", started.track))
            }
            Some(proto::media_event::Payload::Final(transcript)) => seen.push(format!(
                "final/{}/{}/{}",
                transcript.track, transcript.text, transcript.first_final
            )),
            _ => continue,
        }
    }
    assert_eq!(
        seen,
        vec![
            "started/customer".to_string(),
            "final/customer/stop talking/true".to_string()
        ]
    );

    let _ = wire.stop.send(());
    wire.serving.await.unwrap();
}

#[tokio::test]
async fn a_speech_report_without_the_events_capability_is_a_protocol_violation() {
    let wire = listening(AuthPolicy::open()).await;
    let attachment =
        session_with_grpc_attachment(&wire.endpoint, vec![proto::Capability::Sink as i32], None)
            .await;

    let mut client = MediaStreamClient::connect(wire.endpoint.clone())
        .await
        .unwrap();
    let (to_server, outbound) = mpsc::channel(8);
    to_server.send(hello(&attachment, "")).await.unwrap();
    let mut inbound = client
        .subscribe(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    inbound.message().await.unwrap().unwrap();

    to_server
        .send(speech_report(proto::SpeechReportKind::Started, ""))
        .await
        .unwrap();
    let denied = loop {
        match inbound.message().await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("the stream ended without the denial"),
            Err(status) => break status,
        }
    };
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    assert!(denied.message().contains("EVENTS"));

    let _ = wire.stop.send(());
    wire.serving.await.unwrap();
}
