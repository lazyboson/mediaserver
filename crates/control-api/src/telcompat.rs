use crate::controller::SessionController;
use crate::proto;
use crate::proto::media_control_server::MediaControl;
use crate::telcompat_proto::tel_service_server::{TelService, TelServiceServer};
use crate::telcompat_proto::{CallResponse, PlayAndGatherRequest, RecordRequest, StreamRequest};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub const RECORDER_LABEL: &str = "recording";
pub const STREAM_LABEL: &str = "stream";
pub const TRANSCRIPTION_LABEL: &str = "transcription";
const STREAM_SID_KEY: &str = "streamSid";
const ACCOUNT_KEY: &str = "accountId";
const CALL_SID_KEY: &str = "callSid";
const RECORD_ID_KEY: &str = "recordingId";
const FILE_FORMAT_KEY: &str = "fileFormat";
const CHANNELS_KEY: &str = "recordingChannels";
const TRACK_BOTH: &str = "both";
pub const SIP_CALL_ID_KEY: &str = "sipCallId";
pub const CALLER_TAG_KEY: &str = "callerFromTag";

#[derive(Clone, Default)]
struct CallIdentity {
    sip_call_id: String,
    caller_tag: Option<String>,
}

impl CallIdentity {
    fn recorder_has_none() -> CallIdentity {
        CallIdentity::default()
    }

    fn from_metadata(metadata: &HashMap<String, String>) -> CallIdentity {
        CallIdentity {
            sip_call_id: metadata.get(SIP_CALL_ID_KEY).cloned().unwrap_or_default(),
            caller_tag: metadata
                .get(CALLER_TAG_KEY)
                .filter(|tag| !tag.is_empty())
                .cloned(),
        }
    }
}

struct SinkSpec {
    label: &'static str,
    identity: CallIdentity,
    transport: proto::Transport,
    endpoint: String,
    selector: Option<proto::TrackSelector>,
    metadata: HashMap<String, String>,
    capabilities: Vec<i32>,
    authoritative: bool,
}

pub struct TelCompat {
    controller: Arc<SessionController>,
}

impl TelCompat {
    pub fn new(controller: Arc<SessionController>) -> Self {
        TelCompat { controller }
    }

    pub fn into_service(self) -> TelServiceServer<Self> {
        TelServiceServer::new(self)
    }

    fn session_ref(external_id: &str) -> Result<proto::SessionRef, Status> {
        if external_id.is_empty() {
            return Err(Status::invalid_argument("request_uuid is required"));
        }
        Ok(proto::SessionRef {
            id: Some(proto::session_ref::Id::ExternalId(external_id.to_string())),
        })
    }

    async fn session_for(&self, external_id: &str) -> Result<proto::Session, Status> {
        self.controller
            .describe_session(Request::new(Self::session_ref(external_id)?))
            .await
            .map(Response::into_inner)
    }

    async fn attachment_labelled(
        &self,
        external_id: &str,
        label: &str,
    ) -> Result<proto::Attachment, Status> {
        let session = self.session_for(external_id).await?;
        session
            .attachments
            .into_iter()
            .find(|attachment| attachment.label == label)
            .ok_or_else(|| Status::not_found(format!("{external_id} has no {label} attachment")))
    }

    async fn ensure_session(
        &self,
        external_id: &str,
        identity: CallIdentity,
    ) -> Result<proto::Session, Status> {
        match self.session_for(external_id).await {
            Ok(session) => Ok(session),
            Err(status) if status.code() == tonic::Code::NotFound => self
                .controller
                .create_session(Request::new(proto::CreateSessionRequest {
                    external_id: external_id.to_string(),
                    kind: proto::SessionKind::Tap as i32,
                    call_id: identity.sip_call_id,
                    from_tags: identity.caller_tag.into_iter().collect(),
                    rtpengine_node: String::new(),
                    mix: false,
                    idempotency_key: format!("telcompat-session-{external_id}"),
                    sdp_offer: String::new(),
                    group: String::new(),
                }))
                .await
                .map(Response::into_inner),
            Err(status) => Err(status),
        }
    }

    async fn attach_sink(
        &self,
        external_id: &str,
        spec: SinkSpec,
    ) -> Result<proto::Attachment, Status> {
        self.ensure_session(external_id, spec.identity.clone())
            .await?;
        let label = spec.label;
        self.controller
            .attach(Request::new(proto::AttachRequest {
                session: Some(Self::session_ref(external_id)?),
                transport: spec.transport as i32,
                capabilities: spec.capabilities,
                selector: spec.selector,
                format: None,
                authoritative: spec.authoritative,
                label: label.to_string(),
                endpoint: spec.endpoint,
                group: String::new(),
                metadata: spec.metadata,
                idempotency_key: format!("telcompat-{label}-{external_id}"),
            }))
            .await
            .map(Response::into_inner)
    }

    async fn detach_labelled(&self, external_id: &str, label: &str) -> Result<(), Status> {
        let attachment = self.attachment_labelled(external_id, label).await?;
        self.controller
            .detach(Request::new(proto::AttachmentRef {
                attachment_id: attachment.attachment_id,
            }))
            .await?;

        let session = self.session_for(external_id).await?;
        if session.attachments.is_empty() {
            self.controller
                .destroy_session(Request::new(Self::session_ref(external_id)?))
                .await?;
        }
        Ok(())
    }

    async fn set_paused(&self, external_id: &str, paused: bool) -> Result<(), Status> {
        let attachment = self.attachment_labelled(external_id, STREAM_LABEL).await?;
        self.controller
            .update_attachment(Request::new(proto::UpdateAttachmentRequest {
                attachment_id: attachment.attachment_id,
                paused: Some(paused),
                selector: None,
                format: None,
                idempotency_key: String::new(),
            }))
            .await?;
        Ok(())
    }
}

fn track_selector(track: &str) -> Option<proto::TrackSelector> {
    match track {
        "" | TRACK_BOTH => None,
        only => Some(proto::TrackSelector {
            select: Some(proto::track_selector::Select::Only(only.to_string())),
        }),
    }
}

fn stream_metadata(request: &StreamRequest) -> HashMap<String, String> {
    let mut metadata = request.metadata.clone();
    if !request.stream_sid.is_empty() {
        metadata.insert(STREAM_SID_KEY.to_string(), request.stream_sid.clone());
    }
    if !request.acc_id.is_empty() {
        metadata.insert(ACCOUNT_KEY.to_string(), request.acc_id.clone());
    }
    if !request.call_sid.is_empty() {
        metadata.insert(CALL_SID_KEY.to_string(), request.call_sid.clone());
    }
    metadata
}

fn recording_metadata(request: &RecordRequest) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    if !request.acc_id.is_empty() {
        metadata.insert(ACCOUNT_KEY.to_string(), request.acc_id.clone());
    }
    if !request.record_id.is_empty() {
        metadata.insert(RECORD_ID_KEY.to_string(), request.record_id.clone());
    }
    if !request.file_format.is_empty() {
        metadata.insert(FILE_FORMAT_KEY.to_string(), request.file_format.clone());
    }
    if !request.recording_channels.is_empty() {
        metadata.insert(CHANNELS_KEY.to_string(), request.recording_channels.clone());
    }
    metadata
}

fn acknowledged() -> Response<CallResponse> {
    Response::new(CallResponse::default())
}

#[tonic::async_trait]
impl TelService for TelCompat {
    async fn start_stream(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        let message = request.into_inner();
        if message.ws_url.is_empty() {
            return Err(Status::invalid_argument("ws_url is required"));
        }
        let metadata = stream_metadata(&message);
        self.attach_sink(
            &message.request_uuid,
            SinkSpec {
                label: STREAM_LABEL,
                identity: CallIdentity::from_metadata(&metadata),
                transport: proto::Transport::WsTwilio,
                endpoint: message.ws_url.clone(),
                selector: track_selector(&message.track),
                metadata,
                capabilities: vec![
                    proto::Capability::Sink as i32,
                    proto::Capability::Events as i32,
                    proto::Capability::Inject as i32,
                ],
                authoritative: true,
            },
        )
        .await?;
        Ok(acknowledged())
    }

    async fn stop_stream(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        self.detach_labelled(&request.into_inner().request_uuid, STREAM_LABEL)
            .await?;
        Ok(acknowledged())
    }

    async fn stream_pause(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        self.set_paused(&request.into_inner().request_uuid, true)
            .await?;
        Ok(acknowledged())
    }

    async fn stream_resume(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        self.set_paused(&request.into_inner().request_uuid, false)
            .await?;
        Ok(acknowledged())
    }

    async fn stream_send_text(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        let message = request.into_inner();
        let attachment = self
            .attachment_labelled(&message.request_uuid, STREAM_LABEL)
            .await?;
        self.controller
            .send_to_attachment(Request::new(proto::SendToAttachmentRequest {
                attachment_id: attachment.attachment_id,
                json: message.data,
            }))
            .await?;
        Ok(acknowledged())
    }

    async fn stream_play_file(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        let message = request.into_inner();
        if message.play_file.is_empty() {
            return Err(Status::invalid_argument("play_file is required"));
        }
        let attachment = self
            .attachment_labelled(&message.request_uuid, STREAM_LABEL)
            .await?;
        self.controller
            .start_playback(Request::new(proto::StartPlaybackRequest {
                session: Some(Self::session_ref(&message.request_uuid)?),
                source: Some(proto::start_playback_request::Source::File(
                    message.play_file,
                )),
                target_tag: String::new(),
                repeat_times: 0,
                block_egress: false,
                requested_by: attachment.attachment_id,
                idempotency_key: String::new(),
            }))
            .await?;
        Ok(acknowledged())
    }

    async fn start_call_transcription(
        &self,
        request: Request<PlayAndGatherRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        let message = request.into_inner();
        Self::session_ref(&message.request_uuid)?;
        Err(Status::unimplemented(
            "StartCallTranscription needs the tenant's ASR endpoint, which \
             PlayAndGatherRequest does not carry; call StartStream with ws_url \
             instead until the ASR config is plumbed",
        ))
    }

    async fn stop_call_transcription(
        &self,
        request: Request<PlayAndGatherRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        self.detach_labelled(&request.into_inner().request_uuid, TRANSCRIPTION_LABEL)
            .await?;
        Ok(acknowledged())
    }

    async fn stop_playback(
        &self,
        request: Request<PlayAndGatherRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        let message = request.into_inner();
        let session = self.session_for(&message.request_uuid).await?;
        self.controller
            .stop_playback(Request::new(proto::PlaybackRef {
                playback_id: session.session_id,
            }))
            .await
            .map(|_| acknowledged())
    }

    async fn start_recording(
        &self,
        request: Request<RecordRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        let message = request.into_inner();
        if message.record_id.is_empty() || message.acc_id.is_empty() {
            return Err(Status::invalid_argument(
                "acc_id and record_id are required; they are the recording identity",
            ));
        }
        let endpoint = format!(
            "{}/{}.{}",
            message.acc_id,
            message.record_id,
            if message.file_format.is_empty() {
                "wav"
            } else {
                &message.file_format
            }
        );
        self.attach_sink(
            &message.request_uuid,
            SinkSpec {
                label: RECORDER_LABEL,
                identity: CallIdentity::recorder_has_none(),
                transport: proto::Transport::FileS3,
                endpoint,
                selector: None,
                metadata: recording_metadata(&message),
                capabilities: vec![proto::Capability::Sink as i32],
                authoritative: false,
            },
        )
        .await?;
        Ok(acknowledged())
    }

    async fn stop_recording(
        &self,
        request: Request<RecordRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        self.detach_labelled(&request.into_inner().request_uuid, RECORDER_LABEL)
            .await?;
        Ok(acknowledged())
    }
}
