use crate::proto;
use media_core::{AudioFormat, Encoding, Track};
use session_core::{
    AttachmentId, Capabilities, ConsumerEvent, ControlError, EventKind, MediaEvent, PlaybackId,
    SessionId, SessionKind, TrackSelector, Transport,
};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Status;

pub fn status_of(error: ControlError) -> Status {
    match error {
        ControlError::UnknownSession(_)
        | ControlError::UnknownExternalId(_)
        | ControlError::UnknownAttachment(_)
        | ControlError::UnknownPlayback(_) => Status::not_found(error.to_string()),
        ControlError::CapabilityDenied { .. } => Status::permission_denied(error.to_string()),
        ControlError::AuthoritativeAlreadyBound { .. }
        | ControlError::TransportCannotCarry { .. } => {
            Status::failed_precondition(error.to_string())
        }
        ControlError::ExternalIdInUse { .. } => Status::already_exists(error.to_string()),
        ControlError::IdempotencyConflict(_) => Status::aborted(error.to_string()),
        ControlError::TooManyAttachments { .. } => Status::resource_exhausted(error.to_string()),
        ControlError::NoCapabilityDeclared | ControlError::MixRoute(_) => {
            Status::invalid_argument(error.to_string())
        }
    }
}

pub fn session_id(text: &str) -> Result<SessionId, Status> {
    SessionId::from_str(text).map_err(|error| Status::invalid_argument(error.to_string()))
}

pub fn attachment_id(text: &str) -> Result<AttachmentId, Status> {
    AttachmentId::from_str(text).map_err(|error| Status::invalid_argument(error.to_string()))
}

pub fn playback_id(text: &str) -> Result<PlaybackId, Status> {
    PlaybackId::from_str(text).map_err(|error| Status::invalid_argument(error.to_string()))
}

pub fn session_kind(wire: i32) -> Result<SessionKind, Status> {
    match proto::SessionKind::try_from(wire) {
        Ok(proto::SessionKind::Tap) => Ok(SessionKind::Tap),
        Ok(proto::SessionKind::Inline) => Ok(SessionKind::Inline),
        Ok(proto::SessionKind::Mix) => Ok(SessionKind::Mix),
        Ok(proto::SessionKind::Unspecified) | Err(_) => {
            Err(Status::invalid_argument("session kind is required"))
        }
    }
}

pub fn session_kind_wire(kind: SessionKind) -> i32 {
    match kind {
        SessionKind::Tap => proto::SessionKind::Tap as i32,
        SessionKind::Inline => proto::SessionKind::Inline as i32,
        SessionKind::Mix => proto::SessionKind::Mix as i32,
    }
}

pub fn transport(wire: i32) -> Result<Transport, Status> {
    match proto::Transport::try_from(wire) {
        Ok(proto::Transport::WsTwilio) => Ok(Transport::WsTwilio),
        Ok(proto::Transport::GrpcStream) => Ok(Transport::GrpcStream),
        Ok(proto::Transport::FileS3) => Ok(Transport::FileS3),
        Ok(proto::Transport::RtpInline) => Ok(Transport::RtpInline),
        Ok(proto::Transport::Unspecified) | Err(_) => {
            Err(Status::invalid_argument("transport is required"))
        }
    }
}

pub fn transport_wire(transport: Transport) -> i32 {
    match transport {
        Transport::WsTwilio => proto::Transport::WsTwilio as i32,
        Transport::GrpcStream => proto::Transport::GrpcStream as i32,
        Transport::FileS3 => proto::Transport::FileS3 as i32,
        Transport::RtpInline => proto::Transport::RtpInline as i32,
    }
}

pub fn capabilities(wire: &[i32]) -> Result<Capabilities, Status> {
    let mut granted = Capabilities::NONE;
    for value in wire {
        granted = granted
            | match proto::Capability::try_from(*value) {
                Ok(proto::Capability::Sink) => Capabilities::SINK,
                Ok(proto::Capability::Events) => Capabilities::EVENTS,
                Ok(proto::Capability::Inject) => Capabilities::INJECT,
                Ok(proto::Capability::Unspecified) | Err(_) => {
                    return Err(Status::invalid_argument("unrecognized capability"))
                }
            };
    }
    Ok(granted)
}

pub fn capabilities_wire(granted: Capabilities) -> Vec<i32> {
    let mut wire = Vec::new();
    for (capability, value) in [
        (Capabilities::SINK, proto::Capability::Sink),
        (Capabilities::EVENTS, proto::Capability::Events),
        (Capabilities::INJECT, proto::Capability::Inject),
    ] {
        if granted.contains(capability) {
            wire.push(value as i32);
        }
    }
    wire
}

pub fn track(name: &str) -> Result<Track, Status> {
    match name {
        "customer" | "inbound" => Ok(Track::Customer),
        "agent" | "outbound" => Ok(Track::Agent),
        "mixed" => Ok(Track::Mixed),
        _ => Err(Status::invalid_argument(format!("unknown track {name}"))),
    }
}

pub fn track_name(track: Track) -> &'static str {
    match track {
        Track::Customer => "customer",
        Track::Agent => "agent",
        Track::Mixed => "mixed",
    }
}

pub fn selector(wire: Option<&proto::TrackSelector>) -> Result<TrackSelector, Status> {
    match wire.and_then(|selector| selector.select.as_ref()) {
        None => Ok(TrackSelector::All),
        Some(proto::track_selector::Select::All(_)) => Ok(TrackSelector::All),
        Some(proto::track_selector::Select::Only(name)) => track(name).map(TrackSelector::Only),
    }
}

pub fn selector_wire(selector: TrackSelector) -> proto::TrackSelector {
    proto::TrackSelector {
        select: Some(match selector {
            TrackSelector::All => proto::track_selector::Select::All(true),
            TrackSelector::Only(only) => {
                proto::track_selector::Select::Only(track_name(only).to_string())
            }
        }),
    }
}

pub fn format(wire: Option<&proto::AudioFormat>) -> Result<AudioFormat, Status> {
    let wire = match wire {
        Some(format) => format,
        None => return Ok(AudioFormat::pcmu_8k_20ms()),
    };
    let encoding = match proto::Encoding::try_from(wire.encoding) {
        Ok(proto::Encoding::Pcmu) => Encoding::Pcmu,
        Ok(proto::Encoding::Pcma) => Encoding::Pcma,
        Ok(proto::Encoding::L16) => Encoding::L16,
        Ok(proto::Encoding::Opus) => Encoding::Opus,
        Ok(proto::Encoding::Unspecified) | Err(_) => {
            return Err(Status::invalid_argument("encoding is required"))
        }
    };
    let channels = u8::try_from(wire.channels)
        .map_err(|_| Status::invalid_argument("channel count is out of range"))?;
    if channels == 0 || wire.sample_rate_hz == 0 || wire.ptime_ms == 0 {
        return Err(Status::invalid_argument(
            "sample rate, channels and ptime must all be non-zero",
        ));
    }
    Ok(AudioFormat {
        encoding,
        sample_rate_hz: wire.sample_rate_hz,
        channels,
        ptime_ms: wire.ptime_ms,
    })
}

pub fn format_wire(format: AudioFormat) -> proto::AudioFormat {
    proto::AudioFormat {
        encoding: match format.encoding {
            Encoding::Pcmu => proto::Encoding::Pcmu as i32,
            Encoding::Pcma => proto::Encoding::Pcma as i32,
            Encoding::L16 => proto::Encoding::L16 as i32,
            Encoding::Opus => proto::Encoding::Opus as i32,
        },
        sample_rate_hz: format.sample_rate_hz,
        channels: u32::from(format.channels),
        ptime_ms: format.ptime_ms,
    }
}

pub fn event_bytes(event: MediaEvent) -> Vec<u8> {
    use prost::Message;
    event_wire(event).encode_to_vec()
}

pub fn event_from_bytes(bytes: &[u8]) -> Result<proto::MediaEvent, String> {
    use prost::Message;
    proto::MediaEvent::decode(bytes).map_err(|error| error.to_string())
}

pub fn event_wire(event: MediaEvent) -> proto::MediaEvent {
    let legacy_eligible = event.legacy_eligible;
    let session_kind = session_kind_wire(event.session_kind);
    proto::MediaEvent {
        session_id: event.session.to_string(),
        external_id: event.external_id,
        session_kind,
        attachment_id: event
            .attachment
            .map(|id| id.to_string())
            .unwrap_or_default(),
        seq: event.seq,
        at: Some(now()),
        legacy_eligible,
        payload: Some(payload_wire(event.kind)),
    }
}

fn payload_wire(kind: EventKind) -> proto::media_event::Payload {
    use proto::media_event::Payload;
    match kind {
        EventKind::SpeechStarted { track } => Payload::SpeechStarted(proto::SpeechStarted {
            track: track_name(track).to_string(),
        }),
        EventKind::Partial {
            track,
            text,
            confidence,
        } => Payload::Partial(proto::PartialTranscript {
            track: track_name(track).to_string(),
            text,
            confidence: f64::from(confidence),
        }),
        EventKind::Final {
            track,
            text,
            confidence,
            first_final,
        } => Payload::Final(proto::FinalTranscript {
            track: track_name(track).to_string(),
            text,
            confidence: f64::from(confidence),
            first_final,
        }),
        EventKind::EndOfUtterance { track } => Payload::EndOfUtterance(proto::EndOfUtterance {
            track: track_name(track).to_string(),
        }),
        EventKind::EndOfInteraction { reason } => {
            Payload::EndOfInteraction(proto::EndOfInteraction { reason })
        }
        EventKind::Dtmf { track, digit } => Payload::Dtmf(proto::Dtmf {
            track: track_name(track).to_string(),
            digit: digit.to_string(),
        }),
        EventKind::RecordingStarted {
            recording_id,
            path,
            shape,
        } => Payload::RecordingStarted(proto::RecordingStarted {
            recording_id,
            path,
            shape,
        }),
        EventKind::RecordingPaused {
            recording_id,
            paused,
            duration_ms,
        } => Payload::RecordingPaused(proto::RecordingPaused {
            recording_id,
            paused,
            duration_ms,
        }),
        EventKind::RecordingStopped {
            recording_id,
            duration_ms,
        } => Payload::RecordingStopped(proto::RecordingStopped {
            recording_id,
            duration_ms,
        }),
        EventKind::UploadCompleted { recording_id, uri } => {
            Payload::UploadCompleted(proto::UploadCompleted { recording_id, uri })
        }
        EventKind::PlaybackStarted { playback } => {
            Payload::PlaybackStarted(proto::PlaybackStarted {
                playback_id: playback.to_string(),
            })
        }
        EventKind::PlaybackStopped { playback, reason } => {
            Payload::PlaybackStopped(proto::PlaybackStopped {
                playback_id: playback.to_string(),
                reason,
            })
        }
        EventKind::AttachmentUp { label } => Payload::AttachmentUp(proto::AttachmentUp { label }),
        EventKind::MixRouted {
            target,
            monitor_audible,
        } => Payload::MixRouted(proto::MixRouted {
            mix_target: target,
            monitor_audible,
        }),
        EventKind::MemberControlled { mute, deaf, hold } => {
            Payload::MemberControlled(proto::MemberControlled { mute, deaf, hold })
        }
        EventKind::AttachmentDown { label, reason } => {
            Payload::AttachmentDown(proto::AttachmentDown { label, reason })
        }
        EventKind::SessionEnded { reason } => Payload::SessionEnded(proto::SessionEnded { reason }),
    }
}

fn now() -> prost_types::Timestamp {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(since) => prost_types::Timestamp {
            seconds: i64::try_from(since.as_secs()).unwrap_or_default(),
            nanos: i32::try_from(since.subsec_nanos()).unwrap_or_default(),
        },
        Err(_) => prost_types::Timestamp::default(),
    }
}

pub fn speech_report(report: proto::SpeechReport) -> Result<ConsumerEvent, Status> {
    let kind = proto::SpeechReportKind::try_from(report.kind).unwrap_or_default();
    let confidence = report.confidence as f32;
    match kind {
        proto::SpeechReportKind::Unspecified => Err(Status::invalid_argument(
            "a speech report must name its kind",
        )),
        proto::SpeechReportKind::Started => Ok(ConsumerEvent::SpeechStarted {
            track: track(&report.track)?,
        }),
        proto::SpeechReportKind::Partial => Ok(ConsumerEvent::Partial {
            track: track(&report.track)?,
            text: report.text,
            confidence,
        }),
        proto::SpeechReportKind::Final => Ok(ConsumerEvent::Final {
            track: track(&report.track)?,
            text: report.text,
            confidence,
        }),
        proto::SpeechReportKind::EndOfUtterance => Ok(ConsumerEvent::EndOfUtterance {
            track: track(&report.track)?,
        }),
        proto::SpeechReportKind::EndOfInteraction => Ok(ConsumerEvent::EndOfInteraction {
            reason: report.reason,
        }),
    }
}

pub fn observed_lag_ms(observed_at: Option<&prost_types::Timestamp>) -> Option<i64> {
    let observed = observed_at?;
    let now = now();
    let seconds = now.seconds.checked_sub(observed.seconds)?;
    let nanos = i64::from(now.nanos) - i64::from(observed.nanos);
    seconds.checked_mul(1_000)?.checked_add(nanos / 1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(kind: proto::SpeechReportKind) -> proto::SpeechReport {
        proto::SpeechReport {
            kind: kind as i32,
            track: "customer".to_string(),
            text: "hello".to_string(),
            confidence: 0.75,
            observed_at: None,
            reason: "hangup".to_string(),
        }
    }

    #[test]
    fn every_speech_report_kind_becomes_the_consumer_event_it_names() {
        assert_eq!(
            speech_report(report(proto::SpeechReportKind::Started)).unwrap(),
            ConsumerEvent::SpeechStarted {
                track: Track::Customer
            }
        );
        assert_eq!(
            speech_report(report(proto::SpeechReportKind::Partial)).unwrap(),
            ConsumerEvent::Partial {
                track: Track::Customer,
                text: "hello".to_string(),
                confidence: 0.75
            }
        );
        assert_eq!(
            speech_report(report(proto::SpeechReportKind::Final)).unwrap(),
            ConsumerEvent::Final {
                track: Track::Customer,
                text: "hello".to_string(),
                confidence: 0.75
            }
        );
        assert_eq!(
            speech_report(report(proto::SpeechReportKind::EndOfUtterance)).unwrap(),
            ConsumerEvent::EndOfUtterance {
                track: Track::Customer
            }
        );
        assert_eq!(
            speech_report(report(proto::SpeechReportKind::EndOfInteraction)).unwrap(),
            ConsumerEvent::EndOfInteraction {
                reason: "hangup".to_string()
            }
        );
    }

    #[test]
    fn a_speech_report_with_no_kind_or_an_unknown_track_is_an_argument_error() {
        let unspecified = speech_report(report(proto::SpeechReportKind::Unspecified)).unwrap_err();
        assert_eq!(unspecified.code(), tonic::Code::InvalidArgument);

        let mut unknown = report(proto::SpeechReportKind::Started);
        unknown.track = "sidecar".to_string();
        assert_eq!(
            speech_report(unknown).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let mut future = report(proto::SpeechReportKind::Started);
        future.kind = 99;
        assert_eq!(
            speech_report(future).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn an_end_of_interaction_report_needs_no_track_because_it_names_no_track() {
        let mut report = report(proto::SpeechReportKind::EndOfInteraction);
        report.track = String::new();
        assert_eq!(
            speech_report(report).unwrap(),
            ConsumerEvent::EndOfInteraction {
                reason: "hangup".to_string()
            }
        );
    }

    #[test]
    fn the_consumers_own_clock_yields_a_lag_only_when_it_sent_one() {
        assert_eq!(observed_lag_ms(None), None);
        let recent = now();
        let lag = observed_lag_ms(Some(&recent)).unwrap();
        assert!((0..5_000).contains(&lag), "implausible lag {lag} ms");
        let earlier = prost_types::Timestamp {
            seconds: recent.seconds - 2,
            nanos: recent.nanos,
        };
        let older = observed_lag_ms(Some(&earlier)).unwrap();
        assert!(
            (1_900..3_000).contains(&older),
            "expected ~2 s, got {older}"
        );
    }

    #[test]
    fn a_capability_list_round_trips_through_the_wire_form() {
        let granted = Capabilities::SINK | Capabilities::INJECT;
        let wire = capabilities_wire(granted);
        assert_eq!(capabilities(&wire).unwrap(), granted);
        assert_eq!(capabilities(&[]).unwrap(), Capabilities::NONE);
    }

    #[test]
    fn an_unspecified_enum_is_an_argument_error_rather_than_a_default() {
        assert_eq!(
            session_kind(proto::SessionKind::Unspecified as i32)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            transport(proto::Transport::Unspecified as i32)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            capabilities(&[proto::Capability::Unspecified as i32])
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            session_kind(9999).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn a_format_with_a_zero_field_is_refused_because_it_would_divide_by_it() {
        let zero_ptime = proto::AudioFormat {
            encoding: proto::Encoding::Pcmu as i32,
            sample_rate_hz: 8000,
            channels: 1,
            ptime_ms: 0,
        };
        assert_eq!(
            format(Some(&zero_ptime)).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let huge_channels = proto::AudioFormat {
            encoding: proto::Encoding::Pcmu as i32,
            sample_rate_hz: 8000,
            channels: 900,
            ptime_ms: 20,
        };
        assert_eq!(
            format(Some(&huge_channels)).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn an_absent_format_falls_back_to_the_dialect_every_consumer_speaks() {
        assert_eq!(format(None).unwrap(), AudioFormat::pcmu_8k_20ms());
        let wire = format_wire(AudioFormat::l16_16k_20ms());
        assert_eq!(format(Some(&wire)).unwrap(), AudioFormat::l16_16k_20ms());
    }

    #[test]
    fn the_twilio_track_names_are_accepted_alongside_our_own() {
        assert_eq!(track("inbound").unwrap(), Track::Customer);
        assert_eq!(track("customer").unwrap(), Track::Customer);
        assert_eq!(track("outbound").unwrap(), Track::Agent);
        assert!(track("sideways").is_err());
    }

    #[test]
    fn a_selector_round_trips_and_defaults_to_every_track() {
        assert_eq!(selector(None).unwrap(), TrackSelector::All);
        let wire = selector_wire(TrackSelector::Only(Track::Agent));
        assert_eq!(
            selector(Some(&wire)).unwrap(),
            TrackSelector::Only(Track::Agent)
        );
    }

    #[test]
    fn each_control_error_maps_to_the_status_a_caller_can_act_on() {
        let denied = ControlError::CapabilityDenied {
            attachment: AttachmentId::from_raw(1),
            granted: Capabilities::SINK,
            missing: Capabilities::INJECT,
        };
        assert_eq!(status_of(denied).code(), tonic::Code::PermissionDenied);
        assert_eq!(
            status_of(ControlError::UnknownSession(SessionId::from_raw(1))).code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            status_of(ControlError::AuthoritativeAlreadyBound {
                session: SessionId::from_raw(1),
                holder: AttachmentId::from_raw(2),
            })
            .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            status_of(ControlError::IdempotencyConflict("k".to_string())).code(),
            tonic::Code::Aborted
        );
        assert_eq!(
            status_of(ControlError::ExternalIdInUse {
                external_id: "req".to_string(),
                holder: SessionId::from_raw(1),
            })
            .code(),
            tonic::Code::AlreadyExists
        );
        assert_eq!(
            status_of(ControlError::TooManyAttachments {
                session: SessionId::from_raw(1),
                limit: 16,
            })
            .code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[test]
    fn a_malformed_identifier_is_an_argument_error_not_a_lookup_miss() {
        assert_eq!(
            session_id("att-000000000001").unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            attachment_id("garbage").unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert!(playback_id(&PlaybackId::from_raw(3).to_string()).is_ok());
    }
}
