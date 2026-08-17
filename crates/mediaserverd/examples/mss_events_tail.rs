use control_api::convert::event_from_bytes;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::ClientBuilder;

const FETCH_MIN_BYTES: i32 = 1;
const FETCH_MAX_BYTES: i32 = 1_000_000;
const FETCH_WAIT_MS: i32 = 2_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let brokers = args.next().unwrap_or_else(|| "127.0.0.1:9092".to_string());
    let topic = args.next().unwrap_or_else(|| "mss.events".to_string());
    let seconds: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(30);

    let client = ClientBuilder::new(brokers.split(',').map(str::to_string).collect())
        .build()
        .await?;
    let known = client.list_topics().await?;
    let partitions: Vec<i32> = known
        .iter()
        .find(|held| held.name == topic)
        .map(|held| held.partitions.iter().copied().collect())
        .ok_or_else(|| format!("topic {topic} does not exist on {brokers}"))?;
    eprintln!(
        "tailing {topic} ({} partitions) for {seconds}s",
        partitions.len()
    );

    let mut clients = Vec::new();
    for partition in partitions {
        let pc = client
            .partition_client(&topic, partition, UnknownTopicHandling::Error)
            .await?;
        let start = pc
            .get_offset(rskafka::client::partition::OffsetAt::Earliest)
            .await?;
        clients.push((partition, pc, start));
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(seconds);
    let mut total = 0usize;
    while std::time::Instant::now() < deadline {
        for (partition, pc, offset) in clients.iter_mut() {
            let (records, _) = pc
                .fetch_records(*offset, FETCH_MIN_BYTES..FETCH_MAX_BYTES, FETCH_WAIT_MS)
                .await?;
            for held in records {
                *offset = held.offset + 1;
                total += 1;
                let key = held
                    .record
                    .key
                    .as_deref()
                    .map(String::from_utf8_lossy)
                    .unwrap_or_default()
                    .to_string();
                match held.record.value.as_deref().map(event_from_bytes) {
                    Some(Ok(event)) => println!(
                        "p{partition} o{} key={key} session={} seq={} legacy={} payload={:?}",
                        held.offset,
                        event.session_id,
                        event.seq,
                        event.legacy_eligible,
                        event.payload
                    ),
                    Some(Err(error)) => println!(
                        "p{partition} o{} key={key} UNDECODABLE: {error}",
                        held.offset
                    ),
                    None => println!("p{partition} o{} key={key} EMPTY", held.offset),
                }
            }
        }
    }
    eprintln!("saw {total} events");
    Ok(())
}
