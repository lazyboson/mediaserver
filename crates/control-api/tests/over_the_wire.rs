use control_api::proto::media_control_client::MediaControlClient;
use control_api::proto::{self, media_event::Payload};
use control_api::{serve_on_until, SessionController};
use tokio::sync::oneshot;

const SCHEME_SEPARATOR: &str = "\x2f\x2f";

async fn listening() -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(async move {
        let controller = SessionController::new("wire-pod");
        serve_on_until(controller, listener, async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    (
        format!("http:{SCHEME_SEPARATOR}127.0.0.1:{port}"),
        stop,
        serving,
    )
}

#[tokio::test]
async fn a_client_drives_a_whole_session_across_a_real_socket() {
    let (endpoint, stop, serving) = listening().await;
    let mut client = MediaControlClient::connect(endpoint).await.unwrap();

    let session = client
        .create_session(proto::CreateSessionRequest {
            external_id: "req-wire".to_string(),
            kind: proto::SessionKind::Tap as i32,
            call_id: "call-wire".to_string(),
            from_tags: vec!["from-a".to_string()],
            rtpengine_node: "rtpengine-1".to_string(),
            mix: false,
            idempotency_key: "key-wire".to_string(),
            sdp_offer: String::new(),
            group: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(session.owner_pod, "wire-pod");

    let reference = proto::SessionRef {
        id: Some(proto::session_ref::Id::SessionId(
            session.session_id.clone(),
        )),
    };

    let mut events = client
        .watch_events(proto::WatchRequest {
            session: Some(reference.clone()),
        })
        .await
        .unwrap()
        .into_inner();

    let attachment = client
        .attach(proto::AttachRequest {
            session: Some(reference.clone()),
            transport: proto::Transport::GrpcStream as i32,
            capabilities: vec![
                proto::Capability::Sink as i32,
                proto::Capability::Events as i32,
            ],
            selector: Some(proto::TrackSelector {
                select: Some(proto::track_selector::Select::Only("agent".to_string())),
            }),
            format: None,
            authoritative: true,
            label: "rtt".to_string(),
            endpoint: "grpc-target".to_string(),
            group: String::new(),
            metadata: Default::default(),
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(attachment.authoritative);

    let up = events.message().await.unwrap().unwrap();
    assert_eq!(up.session_id, session.session_id);
    assert_eq!(up.attachment_id, attachment.attachment_id);
    assert!(up.legacy_eligible);
    assert!(matches!(up.payload, Some(Payload::AttachmentUp(_))));

    let described = client
        .describe_session(reference.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(described.attachments.len(), 1);
    assert_eq!(
        described.attachments[0].selector.as_ref().unwrap().select,
        Some(proto::track_selector::Select::Only("agent".to_string()))
    );

    client.destroy_session(reference.clone()).await.unwrap();

    let missing = client.describe_session(reference).await.unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    let _ = stop.send(());
    serving.await.unwrap();
}

#[tokio::test]
async fn a_refusal_reaches_the_client_as_a_status_not_a_broken_connection() {
    let (endpoint, stop, serving) = listening().await;
    let mut client = MediaControlClient::connect(endpoint).await.unwrap();

    let session = client
        .create_session(proto::CreateSessionRequest {
            external_id: "req-wire".to_string(),
            kind: proto::SessionKind::Tap as i32,
            call_id: "call-wire".to_string(),
            from_tags: Vec::new(),
            rtpengine_node: String::new(),
            mix: false,
            idempotency_key: String::new(),
            sdp_offer: String::new(),
            group: String::new(),
        })
        .await
        .unwrap()
        .into_inner();

    let refused = client
        .attach(proto::AttachRequest {
            session: Some(proto::SessionRef {
                id: Some(proto::session_ref::Id::SessionId(session.session_id)),
            }),
            transport: proto::Transport::FileS3 as i32,
            capabilities: vec![
                proto::Capability::Sink as i32,
                proto::Capability::Inject as i32,
            ],
            selector: None,
            format: None,
            authoritative: false,
            label: "recorder".to_string(),
            endpoint: "s3-prefix".to_string(),
            group: String::new(),
            metadata: Default::default(),
            idempotency_key: String::new(),
        })
        .await
        .unwrap_err();

    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(refused.message().contains("INJECT"));

    let still_alive = client
        .describe_session(proto::SessionRef {
            id: Some(proto::session_ref::Id::ExternalId("req-wire".to_string())),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(still_alive.attachments.is_empty());

    let _ = stop.send(());
    serving.await.unwrap();
}

#[tokio::test]
async fn a_watcher_that_never_reads_does_not_pin_the_server_open_on_drain() {
    let (endpoint, stop, serving) = listening().await;
    let mut client = MediaControlClient::connect(endpoint).await.unwrap();

    let _watching_everything = client
        .watch_events(proto::WatchRequest { session: None })
        .await
        .unwrap()
        .into_inner();

    let _ = stop.send(());

    tokio::time::timeout(std::time::Duration::from_secs(5), serving)
        .await
        .expect("draining must not wait forever on an open watch stream")
        .unwrap();
}

#[tokio::test]
async fn a_watch_on_one_session_ends_when_that_session_does() {
    let (endpoint, stop, serving) = listening().await;
    let mut client = MediaControlClient::connect(endpoint).await.unwrap();

    let session = client
        .create_session(proto::CreateSessionRequest {
            external_id: "req-ending".to_string(),
            kind: proto::SessionKind::Tap as i32,
            call_id: "call-ending".to_string(),
            from_tags: Vec::new(),
            rtpengine_node: String::new(),
            mix: false,
            idempotency_key: String::new(),
            sdp_offer: String::new(),
            group: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let reference = proto::SessionRef {
        id: Some(proto::session_ref::Id::SessionId(session.session_id)),
    };

    let mut events = client
        .watch_events(proto::WatchRequest {
            session: Some(reference.clone()),
        })
        .await
        .unwrap()
        .into_inner();

    client.destroy_session(reference).await.unwrap();

    let ended = events.message().await.unwrap().unwrap();
    assert!(matches!(ended.payload, Some(Payload::SessionEnded(_))));
    assert!(events.message().await.unwrap().is_none());

    let _ = stop.send(());
    tokio::time::timeout(std::time::Duration::from_secs(5), serving)
        .await
        .expect("the server should drain once its watchers are done")
        .unwrap();
}

#[tokio::test]
async fn one_port_serves_both_the_new_api_and_the_legacy_telsvc_verbs() {
    use control_api::telcompat_proto::tel_service_client::TelServiceClient;
    use control_api::telcompat_proto::StreamRequest;

    let (endpoint, stop, serving) = listening().await;
    let call = "8f14e45f-ea34-4b2c-9a3f-1d2e3f4a5b6c";

    let mut legacy = TelServiceClient::connect(endpoint.clone()).await.unwrap();
    legacy
        .start_stream(StreamRequest {
            request_uuid: call.to_string(),
            acc_id: "acct-1".to_string(),
            stream_sid: "MZ-1".to_string(),
            ws_url: "wss-endpoint".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut native = MediaControlClient::connect(endpoint).await.unwrap();
    let session = native
        .describe_session(proto::SessionRef {
            id: Some(proto::session_ref::Id::ExternalId(call.to_string())),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(session.external_id, call);
    assert_eq!(session.attachments.len(), 1);
    assert_eq!(session.attachments[0].label, "stream");
    assert!(session.attachments[0].authoritative);

    let _ = stop.send(());
    tokio::time::timeout(std::time::Duration::from_secs(5), serving)
        .await
        .expect("drain")
        .unwrap();
}
