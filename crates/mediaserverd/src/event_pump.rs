use control_api::convert::event_bytes;
use control_api::EventSink;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;
use session_core::{MediaEvent, SessionId};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

pub const QUEUE_CAPACITY: usize = 1024;
pub const DEFAULT_TOPIC: &str = "mss.events";
pub const DEFAULT_PARTITIONS: i32 = 4;
pub const DEFAULT_BACKLOG_CAPACITY: usize = 8192;
pub const DEFAULT_FIRST_RETRY_DELAY: Duration = Duration::from_millis(100);
pub const DEFAULT_MAX_RETRY_DELAY: Duration = Duration::from_secs(5);
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_SHUTDOWN_ATTEMPTS: u32 = 3;
const BACKLOG_POLL_INTERVAL: Duration = Duration::from_millis(25);
const CREATE_TOPIC_TIMEOUT_MS: i32 = 5_000;
const REPLICATION_FACTOR: i16 = 1;

#[control_api::async_trait]
pub trait EventTransport: Send + Sync + 'static {
    fn partitions(&self) -> usize;

    async fn reachable(&self) -> Result<(), String> {
        Ok(())
    }

    async fn send(&self, partition: usize, key: Vec<u8>, payload: Vec<u8>) -> Result<(), String>;
}

#[derive(Clone, Copy)]
pub struct PumpTuning {
    pub backlog_capacity: usize,
    pub first_retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub send_timeout: Duration,
    pub shutdown_attempts: u32,
}

impl Default for PumpTuning {
    fn default() -> PumpTuning {
        PumpTuning {
            backlog_capacity: DEFAULT_BACKLOG_CAPACITY,
            first_retry_delay: DEFAULT_FIRST_RETRY_DELAY,
            max_retry_delay: DEFAULT_MAX_RETRY_DELAY,
            send_timeout: DEFAULT_SEND_TIMEOUT,
            shutdown_attempts: DEFAULT_SHUTDOWN_ATTEMPTS,
        }
    }
}

#[derive(Default)]
pub struct PumpCounters {
    pub accepted: AtomicU64,
    pub dropped: AtomicU64,
    pub published: AtomicU64,
    pub failed: AtomicU64,
    pub retried: AtomicU64,
    pub dropped_oldest: AtomicU64,
    pub abandoned: AtomicU64,
    pub retry_depth: AtomicU64,
}

pub struct KafkaEventPump {
    queue: mpsc::Sender<MediaEvent>,
    counters: Arc<PumpCounters>,
}

impl KafkaEventPump {
    pub fn start(
        transport: Arc<dyn EventTransport>,
    ) -> (KafkaEventPump, tokio::task::JoinHandle<()>) {
        KafkaEventPump::start_tuned(transport, PumpTuning::default())
    }

    pub fn start_tuned(
        transport: Arc<dyn EventTransport>,
        tuning: PumpTuning,
    ) -> (KafkaEventPump, tokio::task::JoinHandle<()>) {
        let (queue, inbox) = mpsc::channel(QUEUE_CAPACITY);
        let counters = Arc::new(PumpCounters::default());
        let worker = tokio::spawn(pump(inbox, transport, Arc::clone(&counters), tuning));
        (KafkaEventPump { queue, counters }, worker)
    }

    pub fn counters(&self) -> Arc<PumpCounters> {
        Arc::clone(&self.counters)
    }
}

pub async fn await_empty_backlog(counters: &PumpCounters, within: Duration) -> u64 {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let depth = counters.retry_depth.load(Ordering::Relaxed);
        if depth == 0 || tokio::time::Instant::now() >= deadline {
            return depth;
        }
        tokio::time::sleep(BACKLOG_POLL_INTERVAL).await;
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

struct QueuedEvent {
    partition: usize,
    key: Vec<u8>,
    payload: Vec<u8>,
    session: SessionId,
    seq: u64,
}

fn queue_event(event: MediaEvent, partitions: usize) -> QueuedEvent {
    let partition = partition_for(&event.external_id, partitions);
    let key = event.external_id.clone().into_bytes();
    let session = event.session;
    let seq = event.seq;
    QueuedEvent {
        partition,
        key,
        payload: event_bytes(event),
        session,
        seq,
    }
}

fn retry_delay(attempt: u32, tuning: &PumpTuning) -> Duration {
    let doubling = attempt.saturating_sub(1).min(u32::BITS - 1);
    tuning
        .first_retry_delay
        .saturating_mul(1u32 << doubling)
        .min(tuning.max_retry_delay)
}

fn admit(
    backlog: &mut VecDeque<QueuedEvent>,
    queued: QueuedEvent,
    counters: &PumpCounters,
    tuning: &PumpTuning,
) -> bool {
    let capacity = tuning.backlog_capacity.max(1);
    let mut evicted_head = false;
    while backlog.len() >= capacity {
        match backlog.pop_front() {
            Some(evicted) => {
                evicted_head = true;
                let dropped_oldest = counters.dropped_oldest.fetch_add(1, Ordering::Relaxed) + 1;
                warn!(
                    session = %evicted.session,
                    seq = evicted.seq,
                    dropped_oldest,
                    "the event backlog is at its cap; dropping the oldest unsent event"
                );
            }
            None => break,
        }
    }
    backlog.push_back(queued);
    counters
        .retry_depth
        .store(backlog.len() as u64, Ordering::Relaxed);
    evicted_head
}

async fn pump(
    mut inbox: mpsc::Receiver<MediaEvent>,
    transport: Arc<dyn EventTransport>,
    counters: Arc<PumpCounters>,
    tuning: PumpTuning,
) {
    let partitions = transport.partitions();
    let mut backlog: VecDeque<QueuedEvent> = VecDeque::new();
    let mut attempts_on_head: u32 = 0;
    let mut inbox_open = true;

    loop {
        if backlog.is_empty() {
            if !inbox_open {
                break;
            }
            match inbox.recv().await {
                Some(event) => {
                    admit(
                        &mut backlog,
                        queue_event(event, partitions),
                        &counters,
                        &tuning,
                    );
                }
                None => inbox_open = false,
            }
            continue;
        }
        let (partition, session, seq, key, payload) = match backlog.front() {
            Some(head) => (
                head.partition,
                head.session,
                head.seq,
                head.key.clone(),
                head.payload.clone(),
            ),
            None => continue,
        };

        if attempts_on_head > 0 {
            counters.retried.fetch_add(1, Ordering::Relaxed);
        }
        let outcome =
            tokio::time::timeout(tuning.send_timeout, transport.send(partition, key, payload))
                .await;
        let error = match outcome {
            Ok(Ok(())) => {
                counters.published.fetch_add(1, Ordering::Relaxed);
                backlog.pop_front();
                counters
                    .retry_depth
                    .store(backlog.len() as u64, Ordering::Relaxed);
                attempts_on_head = 0;
                continue;
            }
            Ok(Err(error)) => error,
            Err(_) => format!("no answer within {:?}", tuning.send_timeout),
        };

        counters.failed.fetch_add(1, Ordering::Relaxed);
        attempts_on_head = attempts_on_head.saturating_add(1);
        warn!(
            %session,
            seq,
            partition,
            %error,
            attempts = attempts_on_head,
            depth = backlog.len(),
            "an event did not reach the bus; it stays at the head of the backlog"
        );

        if !inbox_open && attempts_on_head >= tuning.shutdown_attempts {
            let abandoned = backlog.len() as u64;
            counters.abandoned.fetch_add(abandoned, Ordering::Relaxed);
            backlog.clear();
            counters.retry_depth.store(0, Ordering::Relaxed);
            warn!(
                abandoned,
                "the bus is still refusing events while the pump is shutting down; giving up"
            );
            break;
        }

        let resume_at = tokio::time::Instant::now() + retry_delay(attempts_on_head, &tuning);
        loop {
            tokio::select! {
                received = inbox.recv(), if inbox_open => match received {
                    Some(event) => {
                        if admit(&mut backlog, queue_event(event, partitions), &counters, &tuning) {
                            attempts_on_head = 0;
                        }
                    }
                    None => inbox_open = false,
                },
                _ = tokio::time::sleep_until(resume_at) => break,
            }
        }
    }

    info!(
        published = counters.published.load(Ordering::Relaxed),
        failed = counters.failed.load(Ordering::Relaxed),
        retried = counters.retried.load(Ordering::Relaxed),
        dropped = counters.dropped.load(Ordering::Relaxed),
        dropped_oldest = counters.dropped_oldest.load(Ordering::Relaxed),
        abandoned = counters.abandoned.load(Ordering::Relaxed),
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

    async fn reachable(&self) -> Result<(), String> {
        let client = self
            .clients
            .first()
            .ok_or_else(|| "the event topic has no partitions".to_string())?;
        client
            .get_offset(OffsetAt::Latest)
            .await
            .map(|_| ())
            .map_err(|error| format!("kafka offsets: {error}"))
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
    use std::sync::atomic::AtomicI64;
    use std::sync::Mutex;

    type SentRecord = (usize, Vec<u8>, Vec<u8>);

    const OUTAGE_UNTIL_RECOVERED: i64 = -1;

    struct RecordingTransport {
        partitions: usize,
        sent: Mutex<Vec<SentRecord>>,
        refusals_left: AtomicI64,
        attempts: AtomicU64,
    }

    impl RecordingTransport {
        fn new(partitions: usize) -> Arc<RecordingTransport> {
            RecordingTransport::refusing(partitions, 0)
        }

        fn refusing(partitions: usize, refusals: i64) -> Arc<RecordingTransport> {
            Arc::new(RecordingTransport {
                partitions,
                sent: Mutex::new(Vec::new()),
                refusals_left: AtomicI64::new(refusals),
                attempts: AtomicU64::new(0),
            })
        }

        fn recover(&self) {
            self.refusals_left.store(0, Ordering::Relaxed);
        }

        fn seqs_sent(&self) -> Vec<u64> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|(_, _, payload)| event_from_bytes(payload).unwrap().seq)
                .collect()
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
            self.attempts.fetch_add(1, Ordering::Relaxed);
            let refusals = self.refusals_left.load(Ordering::Relaxed);
            if refusals != 0 {
                if refusals > 0 {
                    self.refusals_left.store(refusals - 1, Ordering::Relaxed);
                }
                return Err("broker down".to_string());
            }
            self.sent.lock().unwrap().push((partition, key, payload));
            Ok(())
        }
    }

    fn brisk_tuning(backlog_capacity: usize) -> PumpTuning {
        PumpTuning {
            backlog_capacity,
            first_retry_delay: Duration::from_millis(1),
            max_retry_delay: Duration::from_millis(4),
            send_timeout: Duration::from_millis(200),
            shutdown_attempts: 2,
        }
    }

    async fn until<F: Fn() -> bool>(condition: F) -> bool {
        for _ in 0..500 {
            if condition() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        condition()
    }

    fn event(external_id: &str, seq: u64) -> MediaEvent {
        MediaEvent {
            session: SessionId::from_raw(1),
            external_id: external_id.to_string(),
            session_kind: session_core::SessionKind::Tap,
            attachment: None,
            seq,
            legacy_eligible: true,
            attribution: session_core::Attribution::Explicit,
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
            assert_eq!(
                decoded.session_kind,
                control_api::proto::SessionKind::Tap as i32,
                "a bus consumer can tell a tap's events from an inline leg's"
            );
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
    async fn a_failing_broker_is_counted_not_fatal_and_the_events_wait_in_the_backlog() {
        let transport = RecordingTransport::refusing(1, OUTAGE_UNTIL_RECOVERED);
        let (pump, worker) = KafkaEventPump::start_tuned(transport.clone(), brisk_tuning(16));
        let counters = pump.counters();

        pump.accept(event("req-1", 0));
        pump.accept(event("req-1", 1));
        assert!(until(|| counters.failed.load(Ordering::Relaxed) >= 3).await);

        assert_eq!(counters.published.load(Ordering::Relaxed), 0);
        assert!(counters.retried.load(Ordering::Relaxed) >= 1);
        assert!(until(|| counters.retry_depth.load(Ordering::Relaxed) == 2).await);
        assert!(transport.sent.lock().unwrap().is_empty());

        drop(pump);
        worker.await.unwrap();
        assert_eq!(counters.abandoned.load(Ordering::Relaxed), 2);
        assert_eq!(counters.retry_depth.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn an_outage_costs_no_event_and_no_reordering_once_the_broker_returns() {
        let transport = RecordingTransport::refusing(1, OUTAGE_UNTIL_RECOVERED);
        let (pump, worker) = KafkaEventPump::start_tuned(transport.clone(), brisk_tuning(64));
        let counters = pump.counters();

        for seq in 0..8 {
            pump.accept(event("req-1", seq));
        }
        assert!(until(|| counters.failed.load(Ordering::Relaxed) >= 4).await);
        assert!(until(|| counters.retry_depth.load(Ordering::Relaxed) == 8).await);

        transport.recover();
        assert!(until(|| counters.published.load(Ordering::Relaxed) == 8).await);

        assert_eq!(transport.seqs_sent(), (0..8).collect::<Vec<u64>>());
        assert_eq!(counters.dropped.load(Ordering::Relaxed), 0);
        assert_eq!(counters.dropped_oldest.load(Ordering::Relaxed), 0);
        assert_eq!(counters.abandoned.load(Ordering::Relaxed), 0);
        assert_eq!(counters.retry_depth.load(Ordering::Relaxed), 0);
        assert_eq!(
            await_empty_backlog(&counters, Duration::from_millis(50)).await,
            0
        );

        drop(pump);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn a_backlog_past_its_cap_drops_the_oldest_and_keeps_the_survivors_in_order() {
        let transport = RecordingTransport::refusing(1, OUTAGE_UNTIL_RECOVERED);
        let (pump, worker) = KafkaEventPump::start_tuned(transport.clone(), brisk_tuning(4));
        let counters = pump.counters();

        for seq in 0..10 {
            pump.accept(event("req-1", seq));
        }
        assert!(until(|| counters.dropped_oldest.load(Ordering::Relaxed) == 6).await);
        assert!(until(|| counters.retry_depth.load(Ordering::Relaxed) == 4).await);

        transport.recover();
        assert!(until(|| counters.published.load(Ordering::Relaxed) == 4).await);
        assert_eq!(transport.seqs_sent(), vec![6, 7, 8, 9]);
        assert_eq!(counters.accepted.load(Ordering::Relaxed), 10);

        drop(pump);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn a_transient_refusal_is_retried_without_a_duplicate_landing() {
        let transport = RecordingTransport::refusing(4, 3);
        let (pump, worker) = KafkaEventPump::start_tuned(transport.clone(), brisk_tuning(16));
        let counters = pump.counters();

        pump.accept(event("req-9", 0));
        pump.accept(event("req-9", 1));
        assert!(until(|| counters.published.load(Ordering::Relaxed) == 2).await);

        assert_eq!(transport.seqs_sent(), vec![0, 1]);
        assert_eq!(counters.failed.load(Ordering::Relaxed), 3);
        assert_eq!(counters.retried.load(Ordering::Relaxed), 3);
        assert_eq!(transport.attempts.load(Ordering::Relaxed), 5);

        drop(pump);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn a_send_that_never_answers_is_treated_as_a_failure_not_a_stall() {
        struct SilentTransport;

        #[control_api::async_trait]
        impl EventTransport for SilentTransport {
            fn partitions(&self) -> usize {
                1
            }

            async fn send(&self, _: usize, _: Vec<u8>, _: Vec<u8>) -> Result<(), String> {
                std::future::pending::<()>().await;
                Ok(())
            }
        }

        let tuning = PumpTuning {
            send_timeout: Duration::from_millis(20),
            ..brisk_tuning(8)
        };
        let (pump, worker) = KafkaEventPump::start_tuned(Arc::new(SilentTransport), tuning);
        let counters = pump.counters();

        pump.accept(event("req-1", 0));
        assert!(until(|| counters.failed.load(Ordering::Relaxed) >= 2).await);
        assert_eq!(counters.published.load(Ordering::Relaxed), 0);

        drop(pump);
        worker.await.unwrap();
        assert_eq!(counters.abandoned.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn the_retry_delay_doubles_up_to_the_cap_and_never_overflows() {
        let tuning = PumpTuning {
            first_retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(5),
            ..PumpTuning::default()
        };
        assert_eq!(retry_delay(1, &tuning), Duration::from_millis(100));
        assert_eq!(retry_delay(2, &tuning), Duration::from_millis(200));
        assert_eq!(retry_delay(4, &tuning), Duration::from_millis(800));
        assert_eq!(retry_delay(7, &tuning), Duration::from_secs(5));
        assert_eq!(retry_delay(u32::MAX, &tuning), Duration::from_secs(5));
        assert_eq!(retry_delay(0, &tuning), Duration::from_millis(100));
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
