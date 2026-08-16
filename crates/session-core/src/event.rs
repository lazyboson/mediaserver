use crate::ids::{AttachmentId, PlaybackId, SessionId};
use media_core::Track;

#[derive(Debug, Clone, PartialEq)]
pub enum ConsumerEvent {
    SpeechStarted {
        track: Track,
    },
    Partial {
        track: Track,
        text: String,
        confidence: f32,
    },
    Final {
        track: Track,
        text: String,
        confidence: f32,
    },
    EndOfUtterance {
        track: Track,
    },
    EndOfInteraction {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Observation {
    Dtmf {
        track: Track,
        digit: char,
    },
    RecordingStarted {
        recording_id: String,
        path: String,
    },
    RecordingStopped {
        recording_id: String,
        duration_ms: u64,
    },
    UploadCompleted {
        recording_id: String,
        uri: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventKind {
    SpeechStarted {
        track: Track,
    },
    Partial {
        track: Track,
        text: String,
        confidence: f32,
    },
    Final {
        track: Track,
        text: String,
        confidence: f32,
        first_final: bool,
    },
    EndOfUtterance {
        track: Track,
    },
    EndOfInteraction {
        reason: String,
    },
    Dtmf {
        track: Track,
        digit: char,
    },
    RecordingStarted {
        recording_id: String,
        path: String,
    },
    RecordingStopped {
        recording_id: String,
        duration_ms: u64,
    },
    UploadCompleted {
        recording_id: String,
        uri: String,
    },
    PlaybackStarted {
        playback: PlaybackId,
    },
    PlaybackStopped {
        playback: PlaybackId,
        reason: String,
    },
    AttachmentUp {
        label: String,
    },
    AttachmentDown {
        label: String,
        reason: String,
    },
    SessionEnded {
        reason: String,
    },
}

impl EventKind {
    pub fn legacy_name(&self) -> Option<&'static str> {
        match self {
            EventKind::SpeechStarted { .. } => Some("start_of_transcript"),
            EventKind::Partial { .. } => Some("partial_speech_result"),
            EventKind::Final { first_final, .. } => Some(if *first_final {
                "first_transcript"
            } else {
                "transcription"
            }),
            EventKind::EndOfUtterance { .. } => Some("end_of_utterance"),
            EventKind::EndOfInteraction { .. } => Some("end_of_interaction"),
            EventKind::PlaybackStarted { .. } => Some("play_audio"),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MediaEvent {
    pub session: SessionId,
    pub external_id: String,
    pub attachment: Option<AttachmentId>,
    pub seq: u64,
    pub legacy_eligible: bool,
    pub kind: EventKind,
}

impl MediaEvent {
    pub fn legacy_name(&self) -> Option<&'static str> {
        if !self.legacy_eligible {
            return None;
        }
        self.kind.legacy_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_final_transcript_renders_a_different_legacy_name() {
        let first = EventKind::Final {
            track: Track::Customer,
            text: "hello".to_string(),
            confidence: 0.9,
            first_final: true,
        };
        let later = EventKind::Final {
            track: Track::Customer,
            text: "world".to_string(),
            confidence: 0.9,
            first_final: false,
        };
        assert_eq!(first.legacy_name(), Some("first_transcript"));
        assert_eq!(later.legacy_name(), Some("transcription"));
    }

    #[test]
    fn events_with_no_legacy_counterpart_render_no_name() {
        let ended = EventKind::SessionEnded {
            reason: "hangup".to_string(),
        };
        assert_eq!(ended.legacy_name(), None);
        assert_eq!(
            EventKind::AttachmentUp {
                label: "rtt".to_string()
            }
            .legacy_name(),
            None
        );
    }

    #[test]
    fn an_ineligible_event_never_renders_a_legacy_name_even_when_one_exists() {
        let event = MediaEvent {
            session: SessionId::from_raw(1),
            external_id: "req-1".to_string(),
            attachment: Some(AttachmentId::from_raw(2)),
            seq: 7,
            legacy_eligible: false,
            kind: EventKind::Partial {
                track: Track::Customer,
                text: "hel".to_string(),
                confidence: 0.4,
            },
        };
        assert_eq!(event.kind.legacy_name(), Some("partial_speech_result"));
        assert_eq!(event.legacy_name(), None);
    }
}
