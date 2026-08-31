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
    Answered,
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

    #[test]
    fn answered_json_matches_the_call_control_contract() {
        let event = CallEvent {
            kind: EventKind::Answered,
            external_id: "sip-abc".into(),
            group: "7200".into(),
            from: "1001".into(),
        };
        let raw = serde_json::to_string(&event).unwrap();
        assert_eq!(
            raw,
            r#"{"kind":"answered","externalId":"sip-abc","group":"7200","from":"1001"}"#
        );
    }

    #[test]
    fn recording_sink_keeps_publish_order() {
        let sink = RecordingSink::default();
        sink.publish(CallEvent {
            kind: EventKind::Answered,
            external_id: "sip-abc".into(),
            group: "7200".into(),
            from: String::new(),
        });
        sink.publish(CallEvent {
            kind: EventKind::Ended,
            external_id: "sip-abc".into(),
            group: "7200".into(),
            from: String::new(),
        });
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
