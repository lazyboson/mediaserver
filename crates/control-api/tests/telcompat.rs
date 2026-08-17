use control_api::proto::media_control_server::MediaControl;
use control_api::proto::{self};
use control_api::telcompat::{RECORDER_LABEL, STREAM_LABEL};
use control_api::telcompat_proto::tel_service_server::TelService;
use control_api::telcompat_proto::{PlayAndGatherRequest, RecordRequest, StreamRequest};
use control_api::{MediaPlane, MediaPlaneError, PlaybackSource, SessionController, TelCompat};
use session_core::{AttachmentId, AttachmentView, PlaybackId, SessionId, SessionView};
use std::sync::{Arc, Mutex};
use tonic::{Code, Request};

const CALL: &str = "8f14e45f-ea34-4b2c-9a3f-1d2e3f4a5b6c";

#[derive(Default)]
struct FakeMedia {
    text: Mutex<Vec<String>>,
    playbacks: Mutex<Vec<String>>,
}

#[control_api::async_trait]
impl MediaPlane for FakeMedia {
    async fn open_session(&self, _session: SessionView) -> Result<(), MediaPlaneError> {
        Ok(())
    }
    async fn close_session(&self, _session: SessionId) -> Result<(), MediaPlaneError> {
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
        json: String,
    ) -> Result<(), MediaPlaneError> {
        self.text.lock().unwrap().push(json);
        Ok(())
    }
    async fn start_playback(
        &self,
        _session: SessionId,
        playback: PlaybackId,
        source: PlaybackSource,
        _target_tag: Option<String>,
        _block_egress: bool,
    ) -> Result<(), MediaPlaneError> {
        self.playbacks
            .lock()
            .unwrap()
            .push(format!("{playback}:{source:?}"));
        Ok(())
    }
    async fn stop_playback(
        &self,
        _session: SessionId,
        _playback: PlaybackId,
    ) -> Result<(), MediaPlaneError> {
        Ok(())
    }
}

fn wired() -> (TelCompat, Arc<SessionController>, Arc<FakeMedia>) {
    let media = Arc::new(FakeMedia::default());
    let controller =
        Arc::new(SessionController::new("telcompat-pod").with_media_plane(media.clone()));
    (TelCompat::new(controller.clone()), controller, media)
}

fn stream(uuid: &str) -> StreamRequest {
    StreamRequest {
        request_uuid: uuid.to_string(),
        app_uuid: "app-1".to_string(),
        acc_id: "acct-1".to_string(),
        stream_sid: "MZ-1".to_string(),
        call_sid: "call-1".to_string(),
        ws_url: "wss-endpoint".to_string(),
        mix_type: "mono".to_string(),
        sampling_rate: "8000".to_string(),
        track: String::new(),
        data: String::new(),
        play_file: String::new(),
        metadata: Default::default(),
    }
}

fn record(uuid: &str) -> RecordRequest {
    RecordRequest {
        request_uuid: uuid.to_string(),
        app_uuid: "app-1".to_string(),
        acc_id: "acct-1".to_string(),
        record_id: "rec-9".to_string(),
        file_format: "wav".to_string(),
        recording_channels: "2".to_string(),
        ..RecordRequest::default()
    }
}

fn gather(uuid: &str) -> PlayAndGatherRequest {
    PlayAndGatherRequest {
        request_uuid: uuid.to_string(),
        ..PlayAndGatherRequest::default()
    }
}

async fn describe(controller: &SessionController, uuid: &str) -> Option<proto::Session> {
    controller
        .describe_session(Request::new(proto::SessionRef {
            id: Some(proto::session_ref::Id::ExternalId(uuid.to_string())),
        }))
        .await
        .ok()
        .map(|response| response.into_inner())
}

#[tokio::test]
async fn start_stream_creates_a_tap_session_and_the_authoritative_attachment() {
    let (compat, controller, _) = wired();

    compat
        .start_stream(Request::new(stream(CALL)))
        .await
        .unwrap();

    let session = describe(&controller, CALL).await.expect("session");
    assert_eq!(session.external_id, CALL);
    assert_eq!(session.kind, proto::SessionKind::Tap as i32);
    assert_eq!(session.attachments.len(), 1);

    let attachment = &session.attachments[0];
    assert_eq!(attachment.label, STREAM_LABEL);
    assert_eq!(attachment.transport, proto::Transport::WsTwilio as i32);
    assert!(attachment.authoritative, "the fork owns the call's events");
    assert!(attachment
        .capabilities
        .contains(&(proto::Capability::Inject as i32)));
}

#[tokio::test]
async fn a_retried_start_stream_does_not_attach_twice() {
    let (compat, controller, _) = wired();

    compat
        .start_stream(Request::new(stream(CALL)))
        .await
        .unwrap();
    compat
        .start_stream(Request::new(stream(CALL)))
        .await
        .unwrap();

    let session = describe(&controller, CALL).await.expect("session");
    assert_eq!(
        session.attachments.len(),
        1,
        "an idempotency key should make the retry a no-op, not a second fork"
    );
}

#[tokio::test]
async fn pause_and_resume_move_the_attachment_rather_than_a_media_bug() {
    let (compat, controller, _) = wired();
    compat
        .start_stream(Request::new(stream(CALL)))
        .await
        .unwrap();

    compat
        .stream_pause(Request::new(stream(CALL)))
        .await
        .unwrap();
    let paused = describe(&controller, CALL).await.unwrap();
    assert!(paused.attachments[0].paused);

    compat
        .stream_resume(Request::new(stream(CALL)))
        .await
        .unwrap();
    let resumed = describe(&controller, CALL).await.unwrap();
    assert!(!resumed.attachments[0].paused);
}

#[tokio::test]
async fn send_text_reaches_the_far_end_verbatim() {
    let (compat, _, media) = wired();
    compat
        .start_stream(Request::new(stream(CALL)))
        .await
        .unwrap();

    let mut request = stream(CALL);
    request.data = "{\"event\":\"send_text\"}".to_string();
    compat
        .stream_send_text(Request::new(request))
        .await
        .unwrap();

    assert_eq!(
        media.text.lock().unwrap().as_slice(),
        ["{\"event\":\"send_text\"}".to_string()]
    );
}

#[tokio::test]
async fn play_file_becomes_a_playback_attributed_to_the_fork() {
    let (compat, _, media) = wired();
    compat
        .start_stream(Request::new(stream(CALL)))
        .await
        .unwrap();

    let mut request = stream(CALL);
    request.play_file = "prompt.wav".to_string();
    compat
        .stream_play_file(Request::new(request))
        .await
        .unwrap();

    let playbacks = media.playbacks.lock().unwrap().clone();
    assert_eq!(playbacks.len(), 1);
    assert!(playbacks[0].contains("prompt.wav"), "{:?}", playbacks[0]);
}

#[tokio::test]
async fn start_recording_keeps_the_identity_downstream_tooling_expects() {
    let (compat, controller, _) = wired();

    compat
        .start_recording(Request::new(record(CALL)))
        .await
        .unwrap();

    let session = describe(&controller, CALL).await.expect("session");
    let attachment = &session.attachments[0];
    assert_eq!(attachment.label, RECORDER_LABEL);
    assert_eq!(attachment.transport, proto::Transport::FileS3 as i32);
    assert_eq!(
        attachment.capabilities,
        vec![proto::Capability::Sink as i32]
    );
}

#[tokio::test]
async fn a_recording_without_its_identity_is_refused() {
    let (compat, _, _) = wired();
    let mut request = record(CALL);
    request.record_id = String::new();

    let status = compat
        .start_recording(Request::new(request))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn stopping_the_last_attachment_ends_the_session() {
    let (compat, controller, _) = wired();
    compat
        .start_stream(Request::new(stream(CALL)))
        .await
        .unwrap();
    compat
        .start_recording(Request::new(record(CALL)))
        .await
        .unwrap();

    compat
        .stop_stream(Request::new(stream(CALL)))
        .await
        .unwrap();
    let after_stream = describe(&controller, CALL).await.expect("still tapping");
    assert_eq!(
        after_stream.attachments.len(),
        1,
        "the recorder still wants audio"
    );

    compat
        .stop_recording(Request::new(record(CALL)))
        .await
        .unwrap();
    assert!(
        describe(&controller, CALL).await.is_none(),
        "with nothing attached the tap should be released"
    );
}

#[tokio::test]
async fn recording_and_streaming_share_one_session_but_only_one_is_authoritative() {
    let (compat, controller, _) = wired();

    compat
        .start_stream(Request::new(stream(CALL)))
        .await
        .unwrap();
    compat
        .start_recording(Request::new(record(CALL)))
        .await
        .unwrap();

    let session = describe(&controller, CALL).await.expect("session");
    assert_eq!(session.attachments.len(), 2, "one tap serves both");
    let authoritative: Vec<&str> = session
        .attachments
        .iter()
        .filter(|attachment| attachment.authoritative)
        .map(|attachment| attachment.label.as_str())
        .collect();
    assert_eq!(
        authoritative,
        vec![STREAM_LABEL],
        "the recorder must never drive streamfsm"
    );
}

#[tokio::test]
async fn verbs_against_a_call_with_no_session_are_not_found_not_panics() {
    let (compat, _, _) = wired();

    for status in [
        compat
            .stop_stream(Request::new(stream(CALL)))
            .await
            .unwrap_err(),
        compat
            .stream_pause(Request::new(stream(CALL)))
            .await
            .unwrap_err(),
        compat
            .stream_send_text(Request::new(stream(CALL)))
            .await
            .unwrap_err(),
        compat
            .stop_recording(Request::new(record(CALL)))
            .await
            .unwrap_err(),
        compat
            .stop_playback(Request::new(gather(CALL)))
            .await
            .unwrap_err(),
    ] {
        assert_eq!(status.code(), Code::NotFound, "{status:?}");
    }
}

#[tokio::test]
async fn a_missing_request_uuid_is_refused_before_anything_is_created() {
    let (compat, _, _) = wired();
    let status = compat
        .start_stream(Request::new(stream("")))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn transcription_says_what_it_needs_rather_than_guessing_an_endpoint() {
    let (compat, _, _) = wired();
    let status = compat
        .start_call_transcription(Request::new(gather(CALL)))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::Unimplemented);
    assert!(status.message().contains("ASR endpoint"), "{status:?}");
}

#[tokio::test]
async fn start_stream_carries_the_sip_call_identity_so_the_tap_can_resolve_itself() {
    let (compat, controller, _) = wired();
    let mut request = stream(CALL);
    request.metadata.insert(
        "sipCallId".to_string(),
        "1b2c3d4e@softphone.example".to_string(),
    );
    request
        .metadata
        .insert("callerFromTag".to_string(), "caller-tag-9".to_string());

    compat.start_stream(Request::new(request)).await.unwrap();

    let session = describe(&controller, CALL).await.expect("session");
    assert_eq!(
        session.call_id, "1b2c3d4e@softphone.example",
        "without the sip call-id rtpengine cannot be asked about this call"
    );
    assert_eq!(
        session.from_tags,
        vec!["caller-tag-9".to_string()],
        "the caller's tag must lead so the customer leg is named, not guessed"
    );
}

#[tokio::test]
async fn a_stream_without_the_sip_call_id_still_attaches_but_cannot_identify_the_call() {
    let (compat, controller, _) = wired();

    compat
        .start_stream(Request::new(stream(CALL)))
        .await
        .unwrap();

    let session = describe(&controller, CALL).await.expect("session");
    assert!(
        session.call_id.is_empty() && session.from_tags.is_empty(),
        "an absent identity must stay absent rather than be invented"
    );
}

#[tokio::test]
async fn a_recording_only_session_has_no_call_identity_to_offer() {
    let (compat, controller, _) = wired();

    compat
        .start_recording(Request::new(record(CALL)))
        .await
        .unwrap();

    let session = describe(&controller, CALL).await.expect("session");
    assert!(
        session.call_id.is_empty(),
        "RecordRequest carries no metadata map, so it cannot name the sip call"
    );
}
