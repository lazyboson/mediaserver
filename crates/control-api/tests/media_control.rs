use control_api::proto::media_control_server::MediaControl;
use control_api::proto::{self, media_event::Payload};
use control_api::{MediaPlane, MediaPlaneError, OpenedSession, PlaybackSource, SessionController};
use session_core::{AttachmentId, AttachmentView, PlaybackId, SessionId, SessionView};
use std::sync::{Arc, Mutex};
use tokio_stream::StreamExt;
use tonic::{Code, Request};

const ANSWERED_SDP: &str = "v=0\r\no=- 1 1 IN IP4 10.0.0.7\r\ns=mss-inline\r\n";

#[derive(Default)]
struct RecordingMediaPlane {
    text: Mutex<Vec<(String, String)>>,
    playbacks: Mutex<Vec<String>>,
    stopped_playbacks: Mutex<Vec<(String, Option<String>)>>,
    opened_sessions: Mutex<Vec<String>>,
    opened_attachments: Mutex<Vec<String>>,
    closed_attachments: Mutex<Vec<String>>,
    updated_attachments: Mutex<Vec<(String, bool)>>,
    refuse_sessions: bool,
    refuse_attachments: bool,
    refuse_playback: bool,
    refuse_updates: bool,
}

#[tonic::async_trait]
impl MediaPlane for RecordingMediaPlane {
    async fn open_session(&self, session: SessionView) -> Result<OpenedSession, MediaPlaneError> {
        if self.refuse_sessions {
            return Err(MediaPlaneError("no rtpengine here".to_string()));
        }
        self.opened_sessions
            .lock()
            .unwrap()
            .push(session.id.to_string());
        match session.sdp_offer {
            Some(_) => Ok(OpenedSession::answered(ANSWERED_SDP.to_string())),
            None => Ok(OpenedSession::default()),
        }
    }

    async fn close_session(&self, _session: SessionId) -> Result<(), MediaPlaneError> {
        Ok(())
    }

    async fn open_attachment(&self, attachment: AttachmentView) -> Result<(), MediaPlaneError> {
        if self.refuse_attachments {
            return Err(MediaPlaneError("the consumer refused us".to_string()));
        }
        self.opened_attachments
            .lock()
            .unwrap()
            .push(attachment.id.to_string());
        Ok(())
    }

    async fn update_attachment(&self, attachment: AttachmentView) -> Result<(), MediaPlaneError> {
        if self.refuse_updates {
            return Err(MediaPlaneError(
                "the recorder is already closed".to_string(),
            ));
        }
        self.updated_attachments
            .lock()
            .unwrap()
            .push((attachment.id.to_string(), attachment.paused));
        Ok(())
    }

    async fn close_attachment(
        &self,
        _session: SessionId,
        attachment: AttachmentId,
    ) -> Result<(), MediaPlaneError> {
        self.closed_attachments
            .lock()
            .unwrap()
            .push(attachment.to_string());
        Ok(())
    }

    async fn send_text(
        &self,
        attachment: AttachmentId,
        json: String,
    ) -> Result<(), MediaPlaneError> {
        if self.refuse_playback {
            return Err(MediaPlaneError("the far end is gone".to_string()));
        }
        self.text
            .lock()
            .unwrap()
            .push((attachment.to_string(), json));
        Ok(())
    }

    async fn start_playback(
        &self,
        _session: SessionId,
        playback: PlaybackId,
        _source: PlaybackSource,
        _target_tag: Option<String>,
        _block_egress: bool,
    ) -> Result<(), MediaPlaneError> {
        if self.refuse_playback {
            return Err(MediaPlaneError("nothing is listening".to_string()));
        }
        self.playbacks.lock().unwrap().push(playback.to_string());
        Ok(())
    }

    async fn stop_playback(
        &self,
        _session: SessionId,
        playback: PlaybackId,
        target_tag: Option<String>,
    ) -> Result<(), MediaPlaneError> {
        self.stopped_playbacks
            .lock()
            .unwrap()
            .push((playback.to_string(), target_tag));
        Ok(())
    }
}

fn controller() -> SessionController {
    SessionController::new("test-pod")
}

fn create(external_id: &str) -> proto::CreateSessionRequest {
    proto::CreateSessionRequest {
        external_id: external_id.to_string(),
        kind: proto::SessionKind::Tap as i32,
        call_id: "call-abc".to_string(),
        from_tags: vec!["from-a".to_string()],
        rtpengine_node: "rtpengine-1".to_string(),
        mix: false,
        idempotency_key: String::new(),
        sdp_offer: String::new(),
        group: String::new(),
    }
}

fn attach(
    session_id: &str,
    transport: proto::Transport,
    caps: &[proto::Capability],
) -> proto::AttachRequest {
    proto::AttachRequest {
        session: Some(proto::SessionRef {
            id: Some(proto::session_ref::Id::SessionId(session_id.to_string())),
        }),
        transport: transport as i32,
        capabilities: caps.iter().map(|capability| *capability as i32).collect(),
        selector: None,
        format: None,
        authoritative: false,
        label: "consumer".to_string(),
        endpoint: "wss:".to_string(),
        group: String::new(),
        metadata: Default::default(),
        idempotency_key: String::new(),
    }
}

async fn session_with(controller: &SessionController, external_id: &str) -> String {
    controller
        .create_session(Request::new(create(external_id)))
        .await
        .unwrap()
        .into_inner()
        .session_id
}

#[tokio::test]
async fn an_inline_session_carries_the_media_plane_s_answer_back_to_the_caller() {
    let plane = Arc::new(RecordingMediaPlane::default());
    let controller = controller().with_media_plane(plane.clone());
    let request = proto::CreateSessionRequest {
        kind: proto::SessionKind::Inline as i32,
        sdp_offer: "v=0\r\nc=IN IP4 10.9.0.4\r\nm=audio 41000 RTP/AVP 0\r\n".to_string(),
        ..create("req-inline")
    };

    let session = controller
        .create_session(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(session.kind, proto::SessionKind::Inline as i32);
    assert_eq!(session.sdp_answer, ANSWERED_SDP);

    let described = controller
        .describe_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::ExternalId("req-inline".to_string())),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        described.sdp_answer, ANSWERED_SDP,
        "the answer stays readable for the life of the session"
    );
}

#[tokio::test]
async fn a_conference_group_rides_on_an_inline_leg_and_nowhere_else() {
    let plane = Arc::new(RecordingMediaPlane::default());
    let controller = controller().with_media_plane(plane.clone());
    let conferenced = controller
        .create_session(Request::new(proto::CreateSessionRequest {
            kind: proto::SessionKind::Inline as i32,
            sdp_offer: "v=0\r\nc=IN IP4 10.9.0.4\r\nm=audio 41000 RTP/AVP 0\r\n".to_string(),
            group: "standup".to_string(),
            ..create("req-conf")
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(conferenced.group, "standup");

    let described = controller
        .describe_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::ExternalId("req-conf".to_string())),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        described.group, "standup",
        "the conference a leg is mixed into stays readable"
    );

    let tap_with_group = controller
        .create_session(Request::new(proto::CreateSessionRequest {
            group: "standup".to_string(),
            ..create("req-tap-group")
        }))
        .await
        .unwrap_err();
    assert_eq!(tap_with_group.code(), Code::InvalidArgument);
    assert!(
        tap_with_group.message().contains("conference"),
        "{tap_with_group}"
    );
}

#[tokio::test]
async fn an_inline_session_without_an_offer_and_a_tap_with_one_are_both_refused() {
    let controller = controller();
    let no_offer = controller
        .create_session(Request::new(proto::CreateSessionRequest {
            kind: proto::SessionKind::Inline as i32,
            ..create("req-inline")
        }))
        .await
        .unwrap_err();
    assert_eq!(no_offer.code(), Code::InvalidArgument);
    assert!(no_offer.message().contains("sdp_offer"), "{no_offer}");

    let tap_with_offer = controller
        .create_session(Request::new(proto::CreateSessionRequest {
            sdp_offer: "v=0\r\n".to_string(),
            ..create("req-tap")
        }))
        .await
        .unwrap_err();
    assert_eq!(tap_with_offer.code(), Code::InvalidArgument);
    assert!(
        tap_with_offer.message().contains("rtpengine"),
        "{tap_with_offer}"
    );
}

#[tokio::test]
async fn a_tap_answers_nothing_because_it_never_negotiated_anything() {
    let plane = Arc::new(RecordingMediaPlane::default());
    let controller = controller().with_media_plane(plane);
    let session = controller
        .create_session(Request::new(create("req-1")))
        .await
        .unwrap()
        .into_inner();
    assert!(session.sdp_answer.is_empty());
}

#[tokio::test]
async fn a_created_session_describes_itself_with_the_pod_that_owns_it() {
    let controller = controller();
    let session = controller
        .create_session(Request::new(create("req-1")))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(session.external_id, "req-1");
    assert_eq!(session.owner_pod, "test-pod");
    assert_eq!(session.kind, proto::SessionKind::Tap as i32);
    assert_eq!(session.state, proto::SessionState::Active as i32);

    let described = controller
        .describe_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::ExternalId("req-1".to_string())),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(described.session_id, session.session_id);
}

#[tokio::test]
async fn a_retried_create_is_answered_with_the_session_the_first_call_made() {
    let controller = controller();
    let mut request = create("req-1");
    request.idempotency_key = "key-1".to_string();

    let first = controller
        .create_session(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    let retry = controller
        .create_session(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(first.session_id, retry.session_id);
}

#[tokio::test]
async fn the_same_key_for_a_different_request_is_aborted_rather_than_answered_wrongly() {
    let controller = controller();
    let mut first = create("req-1");
    first.idempotency_key = "key-1".to_string();
    controller
        .create_session(Request::new(first))
        .await
        .unwrap();

    let mut second = create("req-2");
    second.idempotency_key = "key-1".to_string();

    let status = controller
        .create_session(Request::new(second))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Aborted);
}

#[tokio::test]
async fn a_recorder_that_asks_to_inject_is_refused_before_it_ever_attaches() {
    let controller = controller();
    let session = session_with(&controller, "req-1").await;

    let status = controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::FileS3,
            &[proto::Capability::Sink, proto::Capability::Inject],
        )))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("INJECT"));
}

#[tokio::test]
async fn a_sink_that_tries_to_inject_after_attaching_is_denied_permission() {
    let controller = controller();
    let session = session_with(&controller, "req-1").await;
    let recorder = controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::FileS3,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap()
        .into_inner();

    let status = controller
        .start_playback(Request::new(proto::StartPlaybackRequest {
            session: Some(proto::SessionRef {
                id: Some(proto::session_ref::Id::SessionId(session.clone())),
            }),
            source: Some(proto::start_playback_request::Source::File(
                "moh".to_string(),
            )),
            target_tag: String::new(),
            repeat_times: 0,
            block_egress: false,
            requested_by: recorder.attachment_id,
            idempotency_key: String::new(),
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn a_second_authoritative_attachment_is_a_failed_precondition() {
    let controller = controller();
    let session = session_with(&controller, "req-1").await;

    let mut first = attach(
        &session,
        proto::Transport::WsTwilio,
        &[proto::Capability::Sink, proto::Capability::Events],
    );
    first.authoritative = true;
    controller
        .attach(Request::new(first.clone()))
        .await
        .unwrap();

    let mut second = attach(
        &session,
        proto::Transport::GrpcStream,
        &[proto::Capability::Sink, proto::Capability::Events],
    );
    second.authoritative = true;
    let status = controller.attach(Request::new(second)).await.unwrap_err();

    assert_eq!(status.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn pausing_and_reselecting_an_attachment_is_one_update_call() {
    let controller = controller();
    let session = session_with(&controller, "req-1").await;
    let attachment = controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::GrpcStream,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap()
        .into_inner();

    let updated = controller
        .update_attachment(Request::new(proto::UpdateAttachmentRequest {
            attachment_id: attachment.attachment_id.clone(),
            paused: Some(true),
            selector: Some(proto::TrackSelector {
                select: Some(proto::track_selector::Select::Only("customer".to_string())),
            }),
            format: None,
            idempotency_key: String::new(),
            metadata: Default::default(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(updated.paused);
    assert_eq!(
        updated.selector.unwrap().select,
        Some(proto::track_selector::Select::Only("customer".to_string()))
    );
}

#[tokio::test]
async fn an_unknown_or_malformed_identifier_is_reported_not_guessed_at() {
    let controller = controller();

    let missing = controller
        .describe_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::ExternalId("nobody".to_string())),
        }))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);

    let malformed = controller
        .detach(Request::new(proto::AttachmentRef {
            attachment_id: "not-an-id".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(malformed.code(), Code::InvalidArgument);

    let unreferenced = controller
        .describe_session(Request::new(proto::SessionRef { id: None }))
        .await
        .unwrap_err();
    assert_eq!(unreferenced.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn work_that_needs_the_media_plane_is_unavailable_until_one_is_attached() {
    let controller = controller();
    let session = session_with(&controller, "req-1").await;
    let bridge = controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::WsTwilio,
            &[proto::Capability::Sink, proto::Capability::Events],
        )))
        .await
        .unwrap()
        .into_inner();

    let status = controller
        .send_to_attachment(Request::new(proto::SendToAttachmentRequest {
            attachment_id: bridge.attachment_id,
            json: "{}".to_string(),
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::Unavailable);
}

#[tokio::test]
async fn text_reaches_the_media_plane_only_for_a_transport_that_can_carry_it() {
    let media = Arc::new(RecordingMediaPlane::default());
    let controller = controller().with_media_plane(media.clone());
    let session = session_with(&controller, "req-1").await;

    let bridge = controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::WsTwilio,
            &[proto::Capability::Sink, proto::Capability::Events],
        )))
        .await
        .unwrap()
        .into_inner();
    let recorder = controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::FileS3,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap()
        .into_inner();

    controller
        .send_to_attachment(Request::new(proto::SendToAttachmentRequest {
            attachment_id: bridge.attachment_id.clone(),
            json: "{\"event\":\"send_text\"}".to_string(),
        }))
        .await
        .unwrap();

    let refused = controller
        .send_to_attachment(Request::new(proto::SendToAttachmentRequest {
            attachment_id: recorder.attachment_id,
            json: "{}".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);

    let delivered = media.text.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].0, bridge.attachment_id);
}

#[tokio::test]
async fn a_playback_the_media_plane_refuses_leaves_no_orphan_behind() {
    let media = Arc::new(RecordingMediaPlane {
        refuse_playback: true,
        ..RecordingMediaPlane::default()
    });
    let controller = controller().with_media_plane(media);
    let session = session_with(&controller, "req-1").await;
    let mut watch = controller.subscribe();

    let status = controller
        .start_playback(Request::new(proto::StartPlaybackRequest {
            session: Some(proto::SessionRef {
                id: Some(proto::session_ref::Id::SessionId(session)),
            }),
            source: Some(proto::start_playback_request::Source::Blob(vec![1, 2, 3])),
            target_tag: "from-a".to_string(),
            repeat_times: 0,
            block_egress: true,
            requested_by: String::new(),
            idempotency_key: String::new(),
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::Unavailable);

    let mut kinds = Vec::new();
    while let Ok(event) = watch.try_recv() {
        kinds.push(format!("{:?}", event.kind));
    }
    assert!(kinds.iter().any(|kind| kind.starts_with("PlaybackStarted")));
    assert!(kinds.iter().any(|kind| kind.starts_with("PlaybackStopped")));
}

#[tokio::test]
async fn stopping_a_playback_tells_the_media_plane_which_participant_it_was_played_to() {
    let media = Arc::new(RecordingMediaPlane::default());
    let plane: Arc<dyn MediaPlane> = media.clone();
    let controller = controller().with_media_plane(plane);
    let session = session_with(&controller, "req-1").await;

    let mut playbacks = Vec::new();
    for target in ["from-b", ""] {
        let started = controller
            .start_playback(Request::new(proto::StartPlaybackRequest {
                session: Some(proto::SessionRef {
                    id: Some(proto::session_ref::Id::SessionId(session.clone())),
                }),
                source: Some(proto::start_playback_request::Source::File(
                    "moh".to_string(),
                )),
                target_tag: target.to_string(),
                repeat_times: 0,
                block_egress: false,
                requested_by: String::new(),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        playbacks.push(started.playback_id);
    }

    for playback in &playbacks {
        controller
            .stop_playback(Request::new(proto::PlaybackRef {
                playback_id: playback.clone(),
            }))
            .await
            .unwrap();
    }

    let stopped = media.stopped_playbacks.lock().unwrap().clone();
    assert_eq!(
        stopped,
        vec![
            (playbacks[0].clone(), Some("from-b".to_string())),
            (playbacks[1].clone(), None),
        ]
    );
}

#[tokio::test]
async fn a_playback_source_is_required_rather_than_defaulted() {
    let controller = controller();
    let session = session_with(&controller, "req-1").await;

    let status = controller
        .start_playback(Request::new(proto::StartPlaybackRequest {
            session: Some(proto::SessionRef {
                id: Some(proto::session_ref::Id::SessionId(session)),
            }),
            source: None,
            target_tag: String::new(),
            repeat_times: 0,
            block_egress: false,
            requested_by: String::new(),
            idempotency_key: String::new(),
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn watch_events_carries_one_sessions_events_and_not_anothers() {
    let controller = controller();
    let wanted = session_with(&controller, "req-1").await;
    let other = session_with(&controller, "req-2").await;

    let mut stream = controller
        .watch_events(Request::new(proto::WatchRequest {
            session: Some(proto::SessionRef {
                id: Some(proto::session_ref::Id::SessionId(wanted.clone())),
            }),
        }))
        .await
        .unwrap()
        .into_inner();

    controller
        .attach(Request::new(attach(
            &other,
            proto::Transport::GrpcStream,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap();
    controller
        .attach(Request::new(attach(
            &wanted,
            proto::Transport::GrpcStream,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap();

    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.session_id, wanted);
    assert_eq!(event.seq, 0);
    assert!(matches!(event.payload, Some(Payload::AttachmentUp(_))));
    assert!(event.at.is_some());
}

#[tokio::test]
async fn destroying_a_session_announces_the_end_and_frees_the_external_id() {
    let controller = controller();
    let session = session_with(&controller, "req-1").await;
    controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::GrpcStream,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap();
    let mut watch = controller.subscribe();

    controller
        .destroy_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::SessionId(session.clone())),
        }))
        .await
        .unwrap();

    let mut kinds = Vec::new();
    while let Ok(event) = watch.try_recv() {
        kinds.push(format!("{:?}", event.kind));
    }
    assert!(kinds.iter().any(|kind| kind.starts_with("AttachmentDown")));
    assert!(kinds.iter().any(|kind| kind.starts_with("SessionEnded")));

    let gone = controller
        .describe_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::SessionId(session)),
        }))
        .await
        .unwrap_err();
    assert_eq!(gone.code(), Code::NotFound);

    controller
        .create_session(Request::new(create("req-1")))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_mixed_subscription_is_refused_rather_than_quietly_ignored() {
    let controller = controller();
    let mut request = create("req-1");
    request.mix = true;

    let status = controller
        .create_session(Request::new(request))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::Unimplemented);
}

#[tokio::test]
async fn a_session_the_media_plane_cannot_open_is_not_left_behind_in_the_registry() {
    let media = Arc::new(RecordingMediaPlane {
        refuse_sessions: true,
        ..RecordingMediaPlane::default()
    });
    let controller = controller().with_media_plane(media);

    let status = controller
        .create_session(Request::new(create("req-1")))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unavailable);

    let gone = controller
        .describe_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::ExternalId("req-1".to_string())),
        }))
        .await
        .unwrap_err();
    assert_eq!(gone.code(), Code::NotFound);
}

#[tokio::test]
async fn an_attachment_the_media_plane_cannot_open_is_rolled_back() {
    let media = Arc::new(RecordingMediaPlane {
        refuse_attachments: true,
        ..RecordingMediaPlane::default()
    });
    let controller = controller().with_media_plane(media);
    let session = session_with(&controller, "req-1").await;

    let status = controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::GrpcStream,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unavailable);

    let described = controller
        .describe_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::SessionId(session)),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(described.attachments.is_empty());
}

#[tokio::test]
async fn the_media_plane_is_told_to_open_and_close_what_the_api_creates() {
    let media = Arc::new(RecordingMediaPlane::default());
    let controller = controller().with_media_plane(media.clone());
    let session = session_with(&controller, "req-1").await;

    let attachment = controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::GrpcStream,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap()
        .into_inner();

    controller
        .detach(Request::new(proto::AttachmentRef {
            attachment_id: attachment.attachment_id.clone(),
        }))
        .await
        .unwrap();

    assert_eq!(media.opened_sessions.lock().unwrap().as_slice(), [session]);
    assert_eq!(
        media.opened_attachments.lock().unwrap().as_slice(),
        std::slice::from_ref(&attachment.attachment_id)
    );
    assert_eq!(
        media.closed_attachments.lock().unwrap().as_slice(),
        std::slice::from_ref(&attachment.attachment_id)
    );
}

#[tokio::test]
async fn every_committed_event_reaches_the_sink_in_sequence_order() {
    #[derive(Default)]
    struct RecordingSink {
        seen: Mutex<Vec<(String, u64, String)>>,
    }
    impl control_api::EventSink for RecordingSink {
        fn accept(&self, event: session_core::MediaEvent) {
            self.seen.lock().unwrap().push((
                event.external_id.clone(),
                event.seq,
                format!("{:?}", event.kind),
            ));
        }
    }

    let sink = Arc::new(RecordingSink::default());
    let controller = SessionController::new("sink-pod").with_event_sink(sink.clone());
    let session = session_with(&controller, "req-sink").await;
    let attachment = controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::GrpcStream,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap()
        .into_inner();
    controller
        .detach(Request::new(proto::AttachmentRef {
            attachment_id: attachment.attachment_id,
        }))
        .await
        .unwrap();
    controller
        .destroy_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::SessionId(session)),
        }))
        .await
        .unwrap();

    let seen = sink.seen.lock().unwrap().clone();
    let seqs: Vec<u64> = seen.iter().map(|(_, seq, _)| *seq).collect();
    assert_eq!(seqs, vec![0, 1, 2]);
    assert!(seen.iter().all(|(id, _, _)| id == "req-sink"));
    assert!(seen[0].2.starts_with("AttachmentUp"));
    assert!(seen[1].2.starts_with("AttachmentDown"));
    assert!(seen[2].2.starts_with("SessionEnded"));
}

#[tokio::test]
async fn pausing_an_attachment_reaches_the_media_plane() {
    let media = Arc::new(RecordingMediaPlane::default());
    let controller = controller().with_media_plane(media.clone());
    let session = session_with(&controller, "req-pause").await;
    let attachment = controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::FileS3,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap()
        .into_inner();

    for paused in [true, false] {
        controller
            .update_attachment(Request::new(proto::UpdateAttachmentRequest {
                attachment_id: attachment.attachment_id.clone(),
                paused: Some(paused),
                selector: None,
                format: None,
                idempotency_key: String::new(),
                metadata: Default::default(),
            }))
            .await
            .unwrap();
    }

    let seen = media.updated_attachments.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec![
            (attachment.attachment_id.clone(), true),
            (attachment.attachment_id, false),
        ]
    );
}

#[tokio::test]
async fn an_update_the_media_plane_refuses_leaves_the_registry_as_it_was() {
    let media = Arc::new(RecordingMediaPlane {
        refuse_updates: true,
        ..RecordingMediaPlane::default()
    });
    let controller = controller().with_media_plane(media);
    let session = session_with(&controller, "req-pause").await;
    let attachment = controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::FileS3,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap()
        .into_inner();
    assert!(!attachment.paused);

    let status = controller
        .update_attachment(Request::new(proto::UpdateAttachmentRequest {
            attachment_id: attachment.attachment_id.clone(),
            paused: Some(true),
            selector: None,
            format: None,
            idempotency_key: String::new(),
            metadata: Default::default(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unavailable);

    let described = controller
        .describe_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::SessionId(session)),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        !described.attachments[0].paused,
        "a refused pause must not stick in the registry"
    );
}

#[derive(Default)]
struct ClosingObserverPlane {
    controller: Mutex<Option<std::sync::Weak<SessionController>>>,
}

#[tonic::async_trait]
impl MediaPlane for ClosingObserverPlane {
    async fn open_session(&self, _session: SessionView) -> Result<OpenedSession, MediaPlaneError> {
        Ok(OpenedSession::default())
    }

    async fn close_session(&self, session: SessionId) -> Result<(), MediaPlaneError> {
        let held = self.controller.lock().unwrap().clone();
        if let Some(controller) = held.and_then(|weak| weak.upgrade()) {
            for observation in [
                session_core::Observation::RecordingStopped {
                    recording_id: "rec-99".to_string(),
                    duration_ms: 4_000,
                },
                session_core::Observation::UploadCompleted {
                    recording_id: "rec-99".to_string(),
                    uri: "s3-uri".to_string(),
                },
            ] {
                control_api::ObservationSink::observe(controller.as_ref(), session, observation);
            }
        }
        Ok(())
    }

    async fn open_attachment(&self, _attachment: AttachmentView) -> Result<(), MediaPlaneError> {
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
        _source: PlaybackSource,
        _target_tag: Option<String>,
        _block_egress: bool,
    ) -> Result<(), MediaPlaneError> {
        Ok(())
    }

    async fn stop_playback(
        &self,
        _session: SessionId,
        _playback: PlaybackId,
        _target_tag: Option<String>,
    ) -> Result<(), MediaPlaneError> {
        Ok(())
    }
}

#[tokio::test]
async fn a_recording_closed_by_a_hangup_still_gets_its_callbacks_before_the_session_ends() {
    #[derive(Default)]
    struct Collected {
        seen: Mutex<Vec<String>>,
    }
    impl control_api::EventSink for Collected {
        fn accept(&self, event: session_core::MediaEvent) {
            let debug = format!("{:?}", event.kind);
            let name = debug.split([' ', '{']).next().unwrap_or_default();
            self.seen.lock().unwrap().push(name.to_string());
        }
    }

    let plane = Arc::new(ClosingObserverPlane::default());
    let media: Arc<dyn MediaPlane> = plane.clone();
    let events = Arc::new(Collected::default());
    let controller = Arc::new(
        SessionController::new("record-pod")
            .with_media_plane(media)
            .with_event_sink(events.clone()),
    );
    *plane.controller.lock().unwrap() = Some(Arc::downgrade(&controller));

    let session = controller
        .create_session(Request::new(create("req-hangup")))
        .await
        .unwrap()
        .into_inner()
        .session_id;
    controller
        .attach(Request::new(attach(
            &session,
            proto::Transport::FileS3,
            &[proto::Capability::Sink],
        )))
        .await
        .unwrap();
    controller
        .destroy_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::SessionId(session)),
        }))
        .await
        .unwrap();

    let seen = events.seen.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec![
            "AttachmentUp",
            "RecordingStopped",
            "UploadCompleted",
            "AttachmentDown",
            "SessionEnded",
        ],
        "the recording callbacks must land before the session is forgotten: {seen:?}"
    );
}
