#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex};

use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};

pub const STREAM_ENV: &str = "MSS_CALL_EVENT_STREAM";
pub const REDIS_URL_ENV: &str = "MSS_CALL_EVENT_REDIS_URL";
pub const REDIS_URL_FALLBACK_ENV: &str = "MSS_REDIS_URL";
pub const DEFAULT_STREAM: &str = "mss:call-events";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Invited,
    Answered,
    #[serde(rename = "end_of_interaction")]
    EndOfInteraction,
    Ended,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallEvent {
    pub kind: EventKind,
    #[serde(rename = "externalId")]
    pub external_id: String,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub from: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CallIdentity {
    pub external_id: String,
    pub group: String,
    pub from: String,
}

impl CallIdentity {
    pub fn event(&self, kind: EventKind) -> CallEvent {
        self.event_because(kind, String::new())
    }

    pub fn event_because(&self, kind: EventKind, reason: String) -> CallEvent {
        CallEvent {
            kind,
            external_id: self.external_id.clone(),
            group: self.group.clone(),
            from: self.from.clone(),
            reason,
        }
    }
}

pub trait CallEventSink: Send + Sync {
    fn publish(&self, event: CallEvent);
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("the call-event redis at {url} was unreachable: {source}")]
    Unreachable {
        url: String,
        #[source]
        source: redis::RedisError,
    },
}

pub fn stream_name(configured: Option<&str>) -> Option<String> {
    match configured {
        Some(value) if value.trim().is_empty() => None,
        Some(value) => Some(value.to_string()),
        None => Some(DEFAULT_STREAM.to_string()),
    }
}

fn redis_url_from_env() -> Option<String> {
    std::env::var(REDIS_URL_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var(REDIS_URL_FALLBACK_ENV)
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
}

pub async fn sink_from_env() -> Result<Option<Arc<dyn CallEventSink>>, ConnectError> {
    let stream = match std::env::var(STREAM_ENV) {
        Ok(value) => stream_name(Some(&value)),
        Err(_) => stream_name(None),
    };
    let Some(stream) = stream else {
        return Ok(None);
    };
    let Some(url) = redis_url_from_env() else {
        tracing::warn!(
            stream,
            "sip answer and bye are not published; set MSS_REDIS_URL or MSS_CALL_EVENT_REDIS_URL"
        );
        return Ok(None);
    };
    let sink = RedisStreamSink::connect(&url, stream).await?;
    Ok(Some(Arc::new(sink)))
}

pub struct RedisStreamSink {
    client: redis::Client,
    stream: String,
}

impl RedisStreamSink {
    pub async fn connect(url: &str, stream: String) -> Result<Self, ConnectError> {
        let client = redis::Client::open(url).map_err(|source| ConnectError::Unreachable {
            url: url.to_string(),
            source,
        })?;
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|source| ConnectError::Unreachable {
                url: url.to_string(),
                source,
            })?;
        redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .map_err(|source| ConnectError::Unreachable {
                url: url.to_string(),
                source,
            })?;
        Ok(Self { client, stream })
    }
}

impl CallEventSink for RedisStreamSink {
    fn publish(&self, event: CallEvent) {
        let client = self.client.clone();
        let stream = self.stream.clone();
        tokio::spawn(async move {
            if let Err(error) = xadd(&client, &stream, &event).await {
                tracing::warn!(
                    %error,
                    stream,
                    external_id = %event.external_id,
                    "the call-event stream rejected an XADD"
                );
            }
        });
    }
}

async fn xadd(
    client: &redis::Client,
    stream: &str,
    event: &CallEvent,
) -> Result<(), redis::RedisError> {
    let payload = serde_json::to_string(event).unwrap_or_else(|_| "{}".to_string());
    let mut connection: MultiplexedConnection = client.get_multiplexed_async_connection().await?;
    redis::cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("event")
        .arg(payload)
        .query_async::<String>(&mut connection)
        .await
        .map(|_| ())
}

#[derive(Clone, Default)]
pub struct RecordingSink {
    events: Arc<Mutex<Vec<CallEvent>>>,
}

impl RecordingSink {
    pub fn snapshot(&self) -> Vec<CallEvent> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl CallEventSink for RecordingSink {
    fn publish(&self, event: CallEvent) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> CallIdentity {
        CallIdentity {
            external_id: "sip-abc".into(),
            group: "7200".into(),
            from: "1001".into(),
        }
    }

    fn json_of(event: &CallEvent) -> String {
        serde_json::to_string(event).unwrap()
    }

    #[test]
    fn answered_json_matches_the_call_control_contract() {
        assert_eq!(
            json_of(&identity().event(EventKind::Answered)),
            r#"{"kind":"answered","externalId":"sip-abc","group":"7200","from":"1001"}"#
        );
    }

    #[test]
    fn ended_json_matches_the_call_control_contract() {
        assert_eq!(
            json_of(&identity().event(EventKind::Ended)),
            r#"{"kind":"ended","externalId":"sip-abc","group":"7200","from":"1001"}"#
        );
    }

    #[test]
    fn invited_is_the_parked_kind_and_serialises_lowercase() {
        assert_eq!(
            json_of(&identity().event(EventKind::Invited)),
            r#"{"kind":"invited","externalId":"sip-abc","group":"7200","from":"1001"}"#
        );
    }

    #[test]
    fn end_of_interaction_serialises_snake_cased_and_carries_its_reason() {
        assert_eq!(
            json_of(&identity().event_because(
                EventKind::EndOfInteraction,
                "the caller said goodbye".into()
            )),
            r#"{"kind":"end_of_interaction","externalId":"sip-abc","group":"7200","from":"1001","reason":"the caller said goodbye"}"#
        );
    }

    #[test]
    fn an_empty_reason_is_left_off_the_wire_so_the_old_kinds_are_unchanged() {
        let event = identity().event_because(EventKind::EndOfInteraction, String::new());
        assert_eq!(
            json_of(&event),
            r#"{"kind":"end_of_interaction","externalId":"sip-abc","group":"7200","from":"1001"}"#
        );
    }

    #[test]
    fn every_kind_round_trips_through_the_wire_form() {
        for kind in [
            EventKind::Invited,
            EventKind::Answered,
            EventKind::EndOfInteraction,
            EventKind::Ended,
        ] {
            let event = identity().event_because(kind, "because".into());
            let parsed: CallEvent = serde_json::from_str(&json_of(&event)).unwrap();
            assert_eq!(parsed, event, "{kind:?} did not survive the round trip");
        }
    }

    #[test]
    fn a_record_written_before_reason_existed_still_parses() {
        let parsed: CallEvent = serde_json::from_str(
            r#"{"kind":"ended","externalId":"sip-abc","group":"7200","from":"1001"}"#,
        )
        .unwrap();
        assert_eq!(parsed, identity().event(EventKind::Ended));
        assert!(parsed.reason.is_empty());
    }

    #[test]
    fn recording_sink_keeps_publish_order() {
        let sink = RecordingSink::default();
        sink.publish(identity().event(EventKind::Answered));
        sink.publish(identity().event(EventKind::Ended));
        assert_eq!(sink.snapshot()[0].kind, EventKind::Answered);
        assert_eq!(sink.snapshot()[1].kind, EventKind::Ended);
    }

    #[test]
    fn an_unset_stream_name_uses_the_default() {
        assert_eq!(stream_name(None).as_deref(), Some(DEFAULT_STREAM));
    }

    #[test]
    fn an_empty_stream_name_turns_the_publisher_off() {
        assert_eq!(stream_name(Some("")), None);
        assert_eq!(stream_name(Some("   ")), None);
    }
}
