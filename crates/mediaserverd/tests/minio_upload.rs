use control_api::ObservationSink;
use media_core::Track;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStoreExt;
use session_core::{Observation, SessionId};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
#[path = "../src/hub.rs"]
mod hub;

#[allow(dead_code)]
#[path = "../src/recorder.rs"]
mod recorder;

use hub::{Hub, TapEvent, TrackSelection};
use recorder::{
    Layout, RecorderCounters, RecorderSpec, RecordingIdentity, RecordingSink, RecordingSupport,
    S3RecordingSink,
};

const ENDPOINT_ENV: &str = "MSS_TEST_S3_ENDPOINT";
const BUCKET_ENV: &str = "MSS_TEST_S3_BUCKET";
const ACCESS_KEY_ENV: &str = "MSS_TEST_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "MSS_TEST_S3_SECRET_ACCESS_KEY";
const DEFAULT_BUCKET: &str = "lab-recordings";
const DEFAULT_ACCESS_KEY: &str = "minioadmin";
const DEFAULT_SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const RATE: u32 = 8000;
const FRAME: usize = 160;
const SPOKEN_FRAMES: u64 = 25;
const PAUSED_FRAMES: u64 = 25;
const CUSTOMER_TONE: i16 = 4000;
const AGENT_TONE: i16 = -4000;
const PAUSED_TONE: i16 = 9999;

#[derive(Default)]
struct Collected(Mutex<Vec<Observation>>);

impl Collected {
    fn seen(&self) -> Vec<Observation> {
        self.0.lock().unwrap().clone()
    }
}

impl ObservationSink for Collected {
    fn observe(&self, _session: SessionId, observation: Observation) {
        self.0.lock().unwrap().push(observation);
    }
}

async fn wait_for(seen: &Collected, count: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.seen().len() < count {
        assert!(
            std::time::Instant::now() < deadline,
            "the recorder reported {} observations, not {count}",
            seen.seen().len()
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_recording_lands_in_a_real_bucket_under_the_frozen_identity() {
    let Ok(endpoint) = std::env::var(ENDPOINT_ENV) else {
        eprintln!("{ENDPOINT_ENV} not set; skipping");
        return;
    };
    let bucket = std::env::var(BUCKET_ENV).unwrap_or_else(|_| DEFAULT_BUCKET.to_string());
    let access_key =
        std::env::var(ACCESS_KEY_ENV).unwrap_or_else(|_| DEFAULT_ACCESS_KEY.to_string());
    let secret_key =
        std::env::var(SECRET_KEY_ENV).unwrap_or_else(|_| DEFAULT_SECRET_KEY.to_string());

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let endpoint_for_sink = endpoint.clone();
    let sink = S3RecordingSink::new(
        bucket.clone(),
        REGION.to_string(),
        Some(endpoint_for_sink),
        Some((access_key.clone(), secret_key.clone())),
    )
    .expect("the drill needs a usable bucket configuration");
    eprintln!("drill: uploading to {}", sink.describe());

    let identity = RecordingIdentity::parse(&format!("acct-drill/rec-{stamp}.wav")).unwrap();
    let key = identity.object_key();
    let support = RecordingSupport {
        sink: Some(Arc::new(sink) as Arc<dyn RecordingSink>),
        spill_dir: None,
        counters: Arc::new(RecorderCounters::default()),
    };
    let counters = Arc::clone(&support.counters);

    let (mut live, client) = Hub::new();
    let subscription = client.attach(512, TrackSelection::All).unwrap();
    live.poll_commands();
    let seen = Arc::new(Collected::default());
    let strong: Arc<dyn ObservationSink> = seen.clone();
    let observer: Weak<dyn ObservationSink> = Arc::downgrade(&strong);

    let handle = recorder::spawn(
        RecorderSpec {
            session: SessionId::from_raw(1),
            identity,
            layout: Layout::Stereo,
            sample_rate_hz: RATE,
            max_duration: recorder::MAX_RECORDING,
        },
        subscription,
        support,
        Some(observer),
    );

    for at in 0..SPOKEN_FRAMES {
        live.publish(TapEvent::media(
            Track::Customer,
            at * 20,
            &[CUSTOMER_TONE; FRAME],
        ));
        live.publish(TapEvent::media(Track::Agent, at * 20, &[AGENT_TONE; FRAME]));
    }
    assert!(handle.set_paused(true));
    wait_for(&seen, 1).await;
    for at in SPOKEN_FRAMES..SPOKEN_FRAMES + PAUSED_FRAMES {
        live.publish(TapEvent::media(
            Track::Customer,
            at * 20,
            &[PAUSED_TONE; FRAME],
        ));
    }
    assert!(handle.set_paused(false));
    wait_for(&seen, 2).await;
    let resumed = SPOKEN_FRAMES + PAUSED_FRAMES;
    for at in resumed..resumed + SPOKEN_FRAMES {
        live.publish(TapEvent::media(
            Track::Customer,
            at * 20,
            &[CUSTOMER_TONE; FRAME],
        ));
        live.publish(TapEvent::media(Track::Agent, at * 20, &[AGENT_TONE; FRAME]));
    }

    let outcome = handle.finish().await.expect("the recorder had no outcome");
    drop(live);
    let expected_ms = (SPOKEN_FRAMES + SPOKEN_FRAMES) * 20;
    assert_eq!(outcome.duration_ms, expected_ms);
    let uri = outcome.uri.clone().expect("nothing was uploaded");
    eprintln!(
        "drill: {uri} duration_ms={} bytes={} segments={}",
        outcome.duration_ms, outcome.bytes, outcome.stats.segments
    );
    assert!(uri.ends_with(&key), "{uri} does not end with {key}");
    assert_eq!(
        counters.uploaded.load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    let observed = seen.seen();
    assert_eq!(
        observed,
        vec![
            Observation::RecordingPaused {
                recording_id: format!("rec-{stamp}"),
                paused: true,
                duration_ms: SPOKEN_FRAMES * 20,
            },
            Observation::RecordingPaused {
                recording_id: format!("rec-{stamp}"),
                paused: false,
                duration_ms: SPOKEN_FRAMES * 20,
            },
            Observation::RecordingStopped {
                recording_id: format!("rec-{stamp}"),
                duration_ms: expected_ms,
            },
            Observation::UploadCompleted {
                recording_id: format!("rec-{stamp}"),
                uri,
            },
        ]
    );

    let reader = AmazonS3Builder::new()
        .with_bucket_name(&bucket)
        .with_region(REGION)
        .with_endpoint(endpoint)
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .build()
        .expect("the reader needs a usable bucket configuration");
    let stored = reader
        .get(&object_store::path::Path::parse(&key).unwrap())
        .await
        .expect("the object is not in the bucket")
        .bytes()
        .await
        .expect("the object did not download");
    assert_eq!(stored.len(), outcome.bytes);

    let wav = hound::WavReader::new(std::io::Cursor::new(stored.to_vec())).unwrap();
    let spec = wav.spec();
    assert_eq!(spec.channels, 2);
    assert_eq!(spec.sample_rate, RATE);
    assert_eq!(spec.bits_per_sample, 16);
    let samples: Vec<i16> = wav
        .into_samples::<i16>()
        .map(|held| held.unwrap())
        .collect();
    assert_eq!(
        samples.len(),
        FRAME * (SPOKEN_FRAMES + SPOKEN_FRAMES) as usize * 2
    );
    assert!(
        samples.iter().all(|sample| *sample != PAUSED_TONE),
        "the paused interval is audible in the uploaded file"
    );
    let left: Vec<i16> = samples.iter().step_by(2).copied().collect();
    let right: Vec<i16> = samples.iter().skip(1).step_by(2).copied().collect();
    assert!(left.iter().all(|sample| *sample == CUSTOMER_TONE));
    assert!(right.iter().all(|sample| *sample == AGENT_TONE));
    eprintln!(
        "drill: verified {} stereo frames at {key} in bucket {bucket}",
        left.len()
    );
}
