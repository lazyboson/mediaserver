use control_api::convert::event_from_bytes;
use control_api::EventSink;
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use session_core::{EventKind, MediaEvent, SessionId};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
#[path = "../src/event_pump.rs"]
mod event_pump;

use event_pump::{KafkaEventPump, RskafkaTransport};

const BROKERS_ENV: &str = "MSS_TEST_KAFKA_BROKERS";
const TOPIC_ENV: &str = "MSS_TEST_EVENTS_TOPIC";
const COUNT_ENV: &str = "MSS_TEST_EVENT_COUNT";
const INTERVAL_ENV: &str = "MSS_TEST_EVENT_INTERVAL_MS";
const DEFAULT_TOPIC: &str = "mss.events.drill";
const DEFAULT_COUNT: u64 = 60;
const DEFAULT_INTERVAL_MS: u64 = 1_000;
const DRAIN_WINDOW: Duration = Duration::from_secs(180);
const FETCH_MIN_BYTES: i32 = 1;
const FETCH_MAX_BYTES: i32 = 1_000_000;
const FETCH_WAIT_MS: i32 = 1_000;

fn from_env<T: std::str::FromStr>(name: &str, fallback: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn event(external_id: &str, seq: u64) -> MediaEvent {
    MediaEvent {
        session: SessionId::from_raw(seq + 1),
        external_id: external_id.to_string(),
        attachment: None,
        seq,
        legacy_eligible: true,
        kind: EventKind::SessionEnded {
            reason: "outage-drill".to_string(),
        },
    }
}

async fn seqs_on_the_topic(brokers: &str, topic: &str, key: &str) -> Vec<u64> {
    let client = ClientBuilder::new(brokers.split(',').map(str::to_string).collect())
        .build()
        .await
        .unwrap();
    let known = client.list_topics().await.unwrap();
    let partitions: Vec<i32> = known
        .iter()
        .find(|held| held.name == topic)
        .map(|held| held.partitions.iter().copied().collect())
        .unwrap_or_default();

    let mut seqs = Vec::new();
    for partition in partitions {
        let reader = client
            .partition_client(topic, partition, UnknownTopicHandling::Error)
            .await
            .unwrap();
        let end = reader.get_offset(OffsetAt::Latest).await.unwrap();
        let mut offset = reader.get_offset(OffsetAt::Earliest).await.unwrap();
        while offset < end {
            let (records, _) = reader
                .fetch_records(offset, FETCH_MIN_BYTES..FETCH_MAX_BYTES, FETCH_WAIT_MS)
                .await
                .unwrap();
            if records.is_empty() {
                break;
            }
            for held in records {
                offset = held.offset + 1;
                let matches_key = held
                    .record
                    .key
                    .as_deref()
                    .map(|bytes| bytes == key.as_bytes())
                    .unwrap_or(false);
                if !matches_key {
                    continue;
                }
                let decoded = held
                    .record
                    .value
                    .as_deref()
                    .map(event_from_bytes)
                    .expect("an event record carries a payload")
                    .expect("an event record decodes");
                seqs.push(decoded.seq);
            }
        }
    }
    seqs
}

#[tokio::test]
async fn a_broker_outage_mid_run_costs_no_event_once_the_broker_returns() {
    let Ok(brokers) = std::env::var(BROKERS_ENV) else {
        eprintln!("{BROKERS_ENV} not set; skipping");
        return;
    };
    let topic = std::env::var(TOPIC_ENV).unwrap_or_else(|_| DEFAULT_TOPIC.to_string());
    let count: u64 = from_env(COUNT_ENV, DEFAULT_COUNT);
    let interval = Duration::from_millis(from_env(INTERVAL_ENV, DEFAULT_INTERVAL_MS));
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let external_id = format!("drill-{stamp}");

    let transport = RskafkaTransport::connect(
        brokers.split(',').map(str::to_string).collect(),
        &topic,
        event_pump::DEFAULT_PARTITIONS,
    )
    .await
    .expect("the drill needs a reachable broker to start");
    let (pump, worker) = KafkaEventPump::start(Arc::new(transport));
    let counters = pump.counters();

    eprintln!("drill {external_id}: sending {count} events every {interval:?} to {topic}");
    for seq in 0..count {
        pump.accept(event(&external_id, seq));
        tokio::time::sleep(interval).await;
    }
    let unsent = event_pump::await_empty_backlog(&counters, DRAIN_WINDOW).await;
    eprintln!(
        "drill {external_id}: accepted={} published={} failed={} retried={} dropped={} dropped_oldest={} unsent={unsent}",
        counters.accepted.load(Ordering::Relaxed),
        counters.published.load(Ordering::Relaxed),
        counters.failed.load(Ordering::Relaxed),
        counters.retried.load(Ordering::Relaxed),
        counters.dropped.load(Ordering::Relaxed),
        counters.dropped_oldest.load(Ordering::Relaxed),
    );
    drop(pump);
    worker.await.unwrap();

    assert_eq!(unsent, 0, "the backlog never drained");
    assert_eq!(counters.accepted.load(Ordering::Relaxed), count);
    assert_eq!(counters.dropped.load(Ordering::Relaxed), 0);
    assert_eq!(counters.dropped_oldest.load(Ordering::Relaxed), 0);
    assert_eq!(counters.abandoned.load(Ordering::Relaxed), 0);
    assert!(counters.published.load(Ordering::Relaxed) >= count);

    let landed = seqs_on_the_topic(&brokers, &topic, &external_id).await;
    let distinct: BTreeSet<u64> = landed.iter().copied().collect();
    let duplicates = landed.len() - distinct.len();
    eprintln!(
        "drill {external_id}: {} records on the topic, {} distinct, {duplicates} duplicated",
        landed.len(),
        distinct.len()
    );
    assert_eq!(
        distinct,
        (0..count).collect::<BTreeSet<u64>>(),
        "the seq run has holes"
    );
    let mut ordered = landed.clone();
    ordered.dedup();
    assert_eq!(
        ordered,
        {
            let mut ascending: Vec<u64> = distinct.iter().copied().collect();
            ascending.dedup();
            ascending
        },
        "events landed out of order"
    );
}
