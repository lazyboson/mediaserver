use crate::auth::AuthPolicy;
use crate::controller::{InlineEgressSink, SessionController, StreamFrame};
use crate::convert::{
    attachment_id, format, format_wire, observed_lag_ms, speech_report, tracks_under,
};
use crate::proto;
use crate::proto::media_control_server::MediaControl;
use crate::proto::media_stream_server::{MediaStream, MediaStreamServer};
use media_core::{g711, Encoding};
use session_core::{AttachmentView, SessionKind, SessionView, Transport};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

pub const STREAM_QUEUE_DEPTH: usize = 64;
pub const MAX_UTTERANCE_SAMPLES: usize = 29_900;
pub const MARK_POLL_INTERVAL: Duration = Duration::from_millis(20);
pub const MAX_PENDING_MARKS: usize = 64;
const STREAM_SID_METADATA_KEY: &str = "streamSid";

pub struct MediaStreamService {
    controller: Arc<SessionController>,
    auth: AuthPolicy,
}

impl MediaStreamService {
    pub fn new(controller: Arc<SessionController>, auth: AuthPolicy) -> Self {
        MediaStreamService { controller, auth }
    }

    pub fn into_service(self) -> MediaStreamServer<Self> {
        MediaStreamServer::new(self)
    }
}

#[tonic::async_trait]
impl MediaStream for MediaStreamService {
    type SubscribeStream = ReceiverStream<Result<proto::ServerToConsumer, Status>>;

    async fn subscribe(
        &self,
        request: Request<Streaming<proto::ConsumerToServer>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let draining = self.controller.drain_watch();
        if *draining.borrow() {
            return Err(Status::unavailable("this pod is draining"));
        }
        let mut inbound = request.into_inner();

        let hello = match inbound.message().await? {
            Some(proto::ConsumerToServer {
                msg: Some(proto::consumer_to_server::Msg::Hello(hello)),
            }) => hello,
            Some(_) => {
                return Err(Status::invalid_argument(
                    "the first message on a subscribe stream must be a consumer hello",
                ))
            }
            None => {
                return Err(Status::invalid_argument(
                    "the stream ended before a consumer hello arrived",
                ))
            }
        };
        self.auth.check_hello_token(&hello.token)?;

        let attachment = attachment_id(&hello.attachment_id)?;
        let view = self.controller.attachment(attachment)?;
        if view.transport != Transport::GrpcStream {
            return Err(Status::failed_precondition(format!(
                "attachment {attachment} is {}; only grpc-stream attachments subscribe here",
                view.transport
            )));
        }
        if let Some(requested) = hello.requested_format.as_ref() {
            let requested = format(Some(requested))?;
            if requested != view.format {
                return Err(Status::unimplemented(
                    "per-consumer re-encode is not built yet; \
                     request the format the attachment declared or omit it",
                ));
            }
        }
        let session = self.controller.session(view.session)?;
        let frames = self.controller.open_stream(view.session, view.id).await?;
        info!(
            %attachment,
            session = %view.session,
            external_id = %session.external_id,
            "a grpc consumer subscribed to its attachment"
        );

        let (sender, receiver) = mpsc::channel(STREAM_QUEUE_DEPTH);
        let controller = Arc::clone(&self.controller);
        tokio::spawn(async move {
            serve_stream(controller, session, view, inbound, frames, sender, draining).await;
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

type OutboundSender = mpsc::Sender<Result<proto::ServerToConsumer, Status>>;

async fn serve_stream(
    controller: Arc<SessionController>,
    session: SessionView,
    view: AttachmentView,
    mut inbound: Streaming<proto::ConsumerToServer>,
    mut frames: mpsc::Receiver<StreamFrame>,
    sender: OutboundSender,
    mut draining: tokio::sync::watch::Receiver<bool>,
) {
    if send_message(&sender, start_message(&session, &view))
        .await
        .is_err()
    {
        return;
    }

    let mut seq: u64 = 0;
    let mut inject = match (session.kind, controller.inline_egress_sink(view.session)) {
        (SessionKind::Inline, Some(sink)) => {
            InjectState::inline(view.format.encoding, view.format.sample_rate_hz, sink)
        }
        _ => InjectState::new(view.format.encoding, view.format.sample_rate_hz),
    };

    let mut mark_poll = tokio::time::interval(MARK_POLL_INTERVAL);
    mark_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let marks_pending = !inject.marks.is_empty();
        tokio::select! {
            _ = mark_poll.tick(), if marks_pending => {
                for name in inject.drained_marks() {
                    if send_message(&sender, mark_message(name)).await.is_err() {
                        return;
                    }
                }
            }
            changed = draining.changed() => {
                if changed.is_err() || *draining.borrow() {
                    let _ = send_stop(&sender, "this pod is draining").await;
                    return;
                }
            }
            frame = frames.recv() => match frame {
                Some(StreamFrame::Stop { reason }) => {
                    let _ = send_stop(&sender, &reason).await;
                    return;
                }
                Some(frame) => {
                    seq += 1;
                    if send_message(&sender, frame_message(frame, seq)).await.is_err() {
                        return;
                    }
                }
                None => {
                    let _ = send_stop(&sender, "the tap ended").await;
                    return;
                }
            },
            message = inbound.message() => match message {
                Ok(Some(message)) => {
                    if let Err(status) =
                        handle_consumer(&controller, &view, &mut inject, message).await
                    {
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                }
                Ok(None) | Err(_) => return,
            },
        }
    }
}

struct PendingMark {
    name: String,
    watermark: u64,
}

struct InjectState {
    encoding: Encoding,
    sample_rate_hz: u32,
    utterance: Vec<i16>,
    authorized: bool,
    last_playback: Option<String>,
    inline: Option<Arc<dyn InlineEgressSink>>,
    marks: VecDeque<PendingMark>,
}

impl InjectState {
    fn new(encoding: Encoding, sample_rate_hz: u32) -> InjectState {
        InjectState {
            encoding,
            sample_rate_hz,
            utterance: Vec::new(),
            authorized: false,
            last_playback: None,
            inline: None,
            marks: VecDeque::new(),
        }
    }

    fn inline(
        encoding: Encoding,
        sample_rate_hz: u32,
        sink: Arc<dyn InlineEgressSink>,
    ) -> InjectState {
        InjectState {
            inline: Some(sink),
            ..InjectState::new(encoding, sample_rate_hz)
        }
    }

    fn drained_marks(&mut self) -> Vec<String> {
        let Some(sink) = self.inline.as_ref() else {
            return Vec::new();
        };
        let drained = sink.drained_watermark();
        let mut acked = Vec::new();
        while let Some(mark) = self.marks.front() {
            if mark.watermark > drained {
                break;
            }
            acked.push(
                self.marks
                    .pop_front()
                    .expect("the front was just read")
                    .name,
            );
        }
        acked
    }
}

fn inline_inject_refusal(
    declared: media_core::AudioFormat,
    egress: media_core::AudioFormat,
) -> Option<Status> {
    if declared.channels != 1 {
        return Some(Status::failed_precondition(
            "an inline inject stream must be mono",
        ));
    }
    if declared.sample_rate_hz != egress.sample_rate_hz {
        return Some(Status::failed_precondition(format!(
            "this attachment declared {} Hz and the inline leg negotiated {} Hz; \
             inject-path resampling is not built, so attach at the leg's rate",
            declared.sample_rate_hz, egress.sample_rate_hz
        )));
    }
    None
}

async fn handle_consumer(
    controller: &Arc<SessionController>,
    view: &AttachmentView,
    inject: &mut InjectState,
    message: proto::ConsumerToServer,
) -> Result<(), Status> {
    match message.msg {
        Some(proto::consumer_to_server::Msg::Hello(_)) => Err(Status::invalid_argument(
            "a consumer hello arrives exactly once, first",
        )),
        Some(proto::consumer_to_server::Msg::Inject(frame)) => {
            authorize_inject_once(controller, view, inject)?;
            if let Some(sink) = inject.inline.clone() {
                let mut pcm = Vec::new();
                decode_inject(inject.encoding, &frame.payload, &mut pcm)?;
                if pcm.is_empty() {
                    return Ok(());
                }
                if !sink.push_pcm(pcm) {
                    warn!(
                        attachment = %view.id,
                        session = %view.session,
                        "the inline egress queue is full; an injected chunk was dropped"
                    );
                }
                return Ok(());
            }
            if inject.utterance.len() + frame.payload.len() > MAX_UTTERANCE_SAMPLES {
                return Err(Status::resource_exhausted(format!(
                    "an injected utterance is capped at {MAX_UTTERANCE_SAMPLES} samples \
                     because one playback datagram carries no more; \
                     chunked playback is not implemented"
                )));
            }
            decode_inject(inject.encoding, &frame.payload, &mut inject.utterance)
        }
        Some(proto::consumer_to_server::Msg::Mark(mark)) => {
            if let Some(sink) = inject.inline.clone() {
                authorize_inject_once(controller, view, inject)?;
                if inject.marks.len() >= MAX_PENDING_MARKS {
                    return Err(Status::resource_exhausted(format!(
                        "an inline inject stream carries at most {MAX_PENDING_MARKS} \
                         unacked marks"
                    )));
                }
                inject.marks.push_back(PendingMark {
                    name: mark.name,
                    watermark: sink.pushed_watermark(),
                });
                return Ok(());
            }
            if inject.utterance.is_empty() {
                return Ok(());
            }
            let blob = wav_blob(inject.sample_rate_hz, &inject.utterance)?;
            inject.utterance.clear();
            let request = proto::StartPlaybackRequest {
                session: Some(proto::SessionRef {
                    id: Some(proto::session_ref::Id::SessionId(view.session.to_string())),
                }),
                source: Some(proto::start_playback_request::Source::Blob(blob)),
                target_tag: String::new(),
                repeat_times: 0,
                block_egress: true,
                requested_by: view.id.to_string(),
                idempotency_key: String::new(),
            };
            let playback = controller
                .as_ref()
                .start_playback(Request::new(request))
                .await?;
            inject.last_playback = Some(playback.into_inner().playback_id);
            Ok(())
        }
        Some(proto::consumer_to_server::Msg::Clear(_)) => {
            if let Some(sink) = inject.inline.clone() {
                authorize_inject_once(controller, view, inject)?;
                sink.flush();
                inject.marks.clear();
                return Ok(());
            }
            inject.utterance.clear();
            let Some(playback_id) = inject.last_playback.take() else {
                return Ok(());
            };
            let stopped = controller
                .as_ref()
                .stop_playback(Request::new(proto::PlaybackRef { playback_id }))
                .await;
            match stopped {
                Ok(_) => Ok(()),
                Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
                Err(status) => Err(status),
            }
        }
        Some(proto::consumer_to_server::Msg::Report(report)) => {
            if let Some(lag_ms) = observed_lag_ms(report.observed_at.as_ref()) {
                debug!(
                    attachment = %view.id,
                    kind = report.kind,
                    lag_ms,
                    "a consumer reported speech it observed on its own clock"
                );
            }
            let event = speech_report(report)?;
            controller.record_report(view.id, event)
        }
        None => Err(Status::invalid_argument(
            "a consumer message carried no payload",
        )),
    }
}

fn authorize_inject_once(
    controller: &Arc<SessionController>,
    view: &AttachmentView,
    inject: &mut InjectState,
) -> Result<(), Status> {
    if inject.authorized {
        return Ok(());
    }
    controller.authorize_inject(view.id)?;
    if let Some(sink) = inject.inline.as_ref() {
        if let Some(status) = inline_inject_refusal(view.format, sink.egress_format()) {
            return Err(status);
        }
    }
    inject.authorized = true;
    Ok(())
}

fn mark_message(name: String) -> proto::ServerToConsumer {
    proto::ServerToConsumer {
        msg: Some(proto::server_to_consumer::Msg::Mark(proto::Mark { name })),
    }
}

fn decode_inject(encoding: Encoding, payload: &[u8], out: &mut Vec<i16>) -> Result<(), Status> {
    match encoding {
        Encoding::Pcmu => out.extend(payload.iter().map(|byte| g711::ulaw_to_linear(*byte))),
        Encoding::Pcma => out.extend(payload.iter().map(|byte| g711::alaw_to_linear(*byte))),
        Encoding::L16 => {
            if !payload.len().is_multiple_of(2) {
                return Err(Status::invalid_argument(
                    "an L16 inject payload must be whole little-endian samples",
                ));
            }
            out.extend(
                payload
                    .chunks_exact(2)
                    .map(|pair| i16::from_le_bytes([pair[0], pair[1]])),
            );
        }
        Encoding::Opus => {
            return Err(Status::unimplemented(
                "opus inject is not built; attach with g711 or L16",
            ))
        }
    }
    Ok(())
}

fn wav_blob(sample_rate_hz: u32, pcm: &[i16]) -> Result<Vec<u8>, Status> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: sample_rate_hz,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut cursor = std::io::Cursor::new(Vec::new());
    let mut writer = hound::WavWriter::new(&mut cursor, spec)
        .map_err(|error| Status::internal(format!("wav header: {error}")))?;
    for sample in pcm {
        writer
            .write_sample(*sample)
            .map_err(|error| Status::internal(format!("wav body: {error}")))?;
    }
    writer
        .finalize()
        .map_err(|error| Status::internal(format!("wav finalize: {error}")))?;
    Ok(cursor.into_inner())
}

fn start_message(session: &SessionView, view: &AttachmentView) -> proto::ServerToConsumer {
    let tracks = tracks_under(view.selector, session.attribution);
    proto::ServerToConsumer {
        msg: Some(proto::server_to_consumer::Msg::Start(proto::StreamStart {
            session_id: session.id.to_string(),
            external_id: session.external_id.clone(),
            attachment_id: view.id.to_string(),
            stream_sid: view
                .metadata
                .get(STREAM_SID_METADATA_KEY)
                .cloned()
                .unwrap_or_else(|| view.id.to_string()),
            tracks,
            format: Some(format_wire(view.format)),
            custom_parameters: view
                .metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        })),
    }
}

fn frame_message(frame: StreamFrame, seq: u64) -> proto::ServerToConsumer {
    let msg = match frame {
        StreamFrame::Media {
            track,
            pts_ms,
            payload,
        } => proto::server_to_consumer::Msg::Frame(proto::AudioFrame {
            track: track.to_string(),
            seq,
            pts_ms,
            payload,
        }),
        StreamFrame::Dtmf { track, digit } => {
            proto::server_to_consumer::Msg::Dtmf(proto::DtmfFrame {
                track: track.to_string(),
                digit: digit.to_string(),
            })
        }
        StreamFrame::Text { json } => {
            proto::server_to_consumer::Msg::Text(proto::TextFrame { json })
        }
        StreamFrame::Stop { reason } => {
            proto::server_to_consumer::Msg::Stop(proto::StreamStop { reason })
        }
    };
    proto::ServerToConsumer { msg: Some(msg) }
}

async fn send_message(sender: &OutboundSender, message: proto::ServerToConsumer) -> Result<(), ()> {
    sender.send(Ok(message)).await.map_err(|_| ())
}

async fn send_stop(sender: &OutboundSender, reason: &str) -> Result<(), ()> {
    send_message(
        sender,
        proto::ServerToConsumer {
            msg: Some(proto::server_to_consumer::Msg::Stop(proto::StreamStop {
                reason: reason.to_string(),
            })),
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_injected_utterance_becomes_a_mono_wav_of_the_attachment_rate() {
        let blob = wav_blob(8_000, &[0i16, 100, -100]).unwrap();
        assert_eq!(&blob[..4], b"RIFF");
        assert_eq!(&blob[8..12], b"WAVE");
        let mut reader = hound::WavReader::new(std::io::Cursor::new(blob)).unwrap();
        assert_eq!(reader.spec().channels, 1);
        assert_eq!(reader.spec().sample_rate, 8_000);
        let samples: Vec<i16> = reader.samples::<i16>().map(|s| s.unwrap()).collect();
        assert_eq!(samples, vec![0, 100, -100]);
    }

    #[test]
    fn inject_decodes_g711_and_l16_and_refuses_opus() {
        let mut out = Vec::new();
        decode_inject(Encoding::Pcmu, &[0xFF, 0x7F], &mut out).unwrap();
        assert_eq!(out.len(), 2);
        decode_inject(Encoding::Pcma, &[0xD5], &mut out).unwrap();
        assert_eq!(out.len(), 3);

        out.clear();
        decode_inject(Encoding::L16, &[1, 0, 254, 255], &mut out).unwrap();
        assert_eq!(out, vec![1, -2]);
        let ragged = decode_inject(Encoding::L16, &[1, 0, 254], &mut out).unwrap_err();
        assert_eq!(ragged.code(), tonic::Code::InvalidArgument);

        let refused = decode_inject(Encoding::Opus, &[0, 0], &mut out).unwrap_err();
        assert_eq!(refused.code(), tonic::Code::Unimplemented);
    }
}
