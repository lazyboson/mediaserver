use control_api::convert::event_bytes;
use control_api::EventSink;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;
use session_core::MediaEvent;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

pub const QUEUE_CAPACITY: usize = 1024;
pub const DEFAULT_TOPIC: &str = "mss.events";
pub const DEFAULT_PARTITIONS: i32 = 4;
const CREATE_TOPIC_TIMEOUT_MS: i32 = 5_000;
const REPLICATION_FACTOR: i16 = 1;

#[control_api::async_trait]
pub trait EventTransport: Send + Sync + 'static {
    fn partitions(&self) -> usize;

    async fn send(&self, partition: usize, key: Vec<u8>, payload: Vec<u8>) -> Result<(), String>;
}

#[derive(Default)]
pub struct PumpCounters {
    pub accepted: AtomicU64,
    pub dropped: AtomicU64,
    pub published: AtomicU64,
    pub failed: AtomicU64,
}

pub struct KafkaEventPump {
    queue: mpsc::Sender<MediaEvent>,
    counters: Arc<PumpCounters>,
}

impl KafkaEventPump {
    pub fn start(
        transport: Arc<dyn EventTransport>,
    ) -> (KafkaEventPump, tokio::task::JoinHandle<()>) {
        let (queue, inbox) = mpsc::channel(QUEUE_CAPACITY);
        let counters = Arc::new(PumpCounters::default());
        let worker = tokio::spawn(pump(inbox, transport, Arc::clone(&counters)));
        (KafkaEventPump { queue, counters }, worker)
    }

    pub fn counters(&self) -> Arc<PumpCounters> {
        Arc::clone(&self.counters)
    }
}

impl EventSink for KafkaEventPump {
    fn accept(&self, event: MediaEvent) {
        self.counters.accepted.fetch_add(1, Ordering::Relaxed);
        if self.queue.try_send(event).is_err() {
            let dropped = self.counters.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(dropped, "the event queue is full; dropping an event");
        }
    }
}

pub fn partition_for(key: &str, partitions: usize) -> usize {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % partitions.max(1) as u64) as usize
}

async fn pump(
    mut inbox: mpsc::Receiver<MediaEvent>,
    transport: Arc<dyn EventTransport>,
    counters: Arc<PumpCounters>,
) {
    while let Some(event) = inbox.recv().await {
        let partition = partition_for(&event.external_id, transport.partitions());
        let key = event.external_id.clone().into_bytes();
        let session = event.session;
        let seq = event.seq;
        let payload = event_bytes(event);
        match transport.send(partition, key, payload).await {
            Ok(()) => {
                counters.published.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                counters.failed.fetch_add(1, Ordering::Relaxed);
                warn!(%session, seq, partition, %error, "an event did not reach the bus");
            }
        }
    }
    info!(
        published = counters.published.load(Ordering::Relaxed),
        failed = counters.failed.load(Ordering::Relaxed),
        dropped = counters.dropped.load(Ordering::Relaxed),
        "event pump stopped"
    );
}

pub struct RskafkaTransport {
    clients: Vec<PartitionClient>,
}

impl RskafkaTransport {
    pub async fn connect(
        brokers: Vec<String>,
        topic: &str,
        partitions: i32,
    ) -> Result<RskafkaTransport, String> {
        let client = ClientBuilder::new(brokers)
            .build()
            .await
            .map_err(|error| format!("kafka connect: {error}"))?;

        let known = client
            .list_topics()
            .await
            .map_err(|error| format!("kafka list topics: {error}"))?;
        let existing = known.iter().find(|held| held.name == topic);
        let partition_ids: Vec<i32> = match existing {
            Some(held) => held.partitions.iter().copied().collect(),
            None => {
                let controller = client
                    .controller_client()
                    .map_err(|error| format!("kafka controller: {error}"))?;
                controller
                    .create_topic(
                        topic,
                        partitions,
                        REPLICATION_FACTOR,
                        CREATE_TOPIC_TIMEOUT_MS,
                    )
                    .await
                    .map_err(|error| format!("kafka create topic {topic}: {error}"))?;
                (0..partitions).collect()
            }
        };

        let mut clients = Vec::with_capacity(partition_ids.len());
        for partition in partition_ids {
            clients.push(
                client
                    .partition_client(topic, partition, UnknownTopicHandling::Retry)
                    .await
                    .map_err(|error| {
                        format!("kafka partition client {topic}/{partition}: {error}")
                    })?,
            );
        }
        info!(topic, partitions = clients.len(), "event bus connected");
        Ok(RskafkaTransport { clients })
    }
}

#[control_api::async_trait]
impl EventTransport for RskafkaTransport {
    fn partitions(&self) -> usize {
        self.clients.len()
    }

    async fn send(&self, partition: usize, key: Vec<u8>, payload: Vec<u8>) -> Result<(), String> {
        let client = self
            .clients
            .get(partition)
            .ok_or_else(|| format!("no client for partition {partition}"))?;
        let record = Record {
            key: Some(key),
            value: Some(payload),
            headers: Default::default(),
            timestamp: chrono::Utc::now(),
        };
        client
            .produce(vec![record], Compression::NoCompression)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use control_api::convert::event_from_bytes;
    use control_api::proto::media_event::Payload;
    use session_core::{EventKind, SessionId};
    use std::sync::Mutex;
    use std::time::Duration;

    type SentRecord = (usize, Vec<u8>, Vec<u8>);

    struct RecordingTransport {
        partitions: usize,
        sent: Mutex<Vec<SentRecord>>,
        refuse: bool,
    }

    impl RecordingTransport {
        fn new(partitions: usize) -> Arc<RecordingTransport> {
            Arc::new(RecordingTransport {
                partitions,
                sent: Mutex::new(Vec::new()),
                refuse: false,
            })
        }
    }

    #[control_api::async_trait]
    impl EventTransport for RecordingTransport {
        fn partitions(&self) -> usize {
            self.partitions
        }

        async fn send(
            &self,
            partition: usize,
            key: Vec<u8>,
            payload: Vec<u8>,
        ) -> Result<(), String> {
            if self.refuse {
                return Err("broker down".to_string());
            }
            self.sent.lock().unwrap().push((partition, key, payload));
            Ok(())
        }
    }

    fn event(external_id: &str, seq: u64) -> MediaEvent {
        MediaEvent {
            session: SessionId::from_raw(1),
            external_id: external_id.to_string(),
            attachment: None,
            seq,
            legacy_eligible: true,
            kind: EventKind::SessionEnded {
                reason: "test".to_string(),
            },
        }
    }

    async fn settle(counters: &PumpCounters, expect_published: u64) {
        for _ in 0..100 {
            if counters.published.load(Ordering::Relaxed) >= expect_published {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn events_reach_the_transport_in_order_keyed_and_decodable() {
        let transport = RecordingTransport::new(4);
        let (pump, worker) = KafkaEventPump::start(transport.clone());

        for seq in 0..3 {
            pump.accept(event("req-1", seq));
        }
        settle(&pump.counters(), 3).await;

        let sent = transport.sent.lock().unwrap().clone();
        assert_eq!(sent.len(), 3);
        let expected_partition = partition_for("req-1", 4);
        for (index, (partition, key, payload)) in sent.iter().enumerate() {
            assert_eq!(*partition, expected_partition);
            assert_eq!(key, b"req-1");
            let decoded = event_from_bytes(payload).unwrap();
            assert_eq!(decoded.seq, index as u64);
            assert_eq!(decoded.external_id, "req-1");
            assert!(matches!(decoded.payload, Some(Payload::SessionEnded(_))));
        }

        drop(pump);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn one_call_always_lands_on_one_partition() {
        let spread: Vec<usize> = (0..32)
            .map(|n| partition_for(&format!("call-{n}"), 4))
            .collect();
        assert!(spread.iter().any(|p| *p != spread[0]));
        for key in ["req-1", "req-2", "abc"] {
            assert_eq!(partition_for(key, 4), partition_for(key, 4));
            assert!(partition_for(key, 4) < 4);
        }
        assert_eq!(partition_for("anything", 0), 0);
    }

    #[tokio::test]
    async fn a_failing_broker_is_counted_not_fatal() {
        let transport = Arc::new(RecordingTransport {
            partitions: 1,
            sent: Mutex::new(Vec::new()),
            refuse: true,
        });
        let (pump, worker) = KafkaEventPump::start(transport);

        pump.accept(event("req-1", 0));
        pump.accept(event("req-1", 1));
        for _ in 0..100 {
            if pump.counters().failed.load(Ordering::Relaxed) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        assert_eq!(pump.counters().failed.load(Ordering::Relaxed), 2);
        assert_eq!(pump.counters().published.load(Ordering::Relaxed), 0);
        drop(pump);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn a_full_queue_drops_and_counts_instead_of_blocking_the_controller() {
        let (queue, inbox) = mpsc::channel(2);
        let counters = Arc::new(PumpCounters::default());
        let sink = KafkaEventPump {
            queue,
            counters: Arc::clone(&counters),
        };

        for seq in 0..5 {
            sink.accept(event("req-1", seq));
        }

        assert_eq!(counters.accepted.load(Ordering::Relaxed), 5);
        assert_eq!(counters.dropped.load(Ordering::Relaxed), 3);
        drop(inbox);
    }
}
