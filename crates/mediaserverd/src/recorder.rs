use crate::hub::{Subscription, TapEvent};
use control_api::ObservationSink;
use media_core::Track;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, PutPayload, RetryConfig};
use session_core::{Observation, SessionId};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};

pub const IDENTITY_SCHEME: &str = "${accountID}/${recordingID}.${format}";
pub const MAX_RECORDING: Duration = Duration::from_secs(2 * 3600);
pub const UPLOAD_TIMEOUT: Duration = Duration::from_secs(60);
pub const FINISH_TIMEOUT: Duration = Duration::from_secs(90);

const COMMAND_DEPTH: usize = 8;
const URI_SCHEME_SEPARATOR: &str = "\x2f\x2f";
const BUCKET_ENV: &str = "MSS_RECORDING_BUCKET";
const ENDPOINT_ENV: &str = "MSS_RECORDING_S3_ENDPOINT";
const REGION_ENV: &str = "MSS_RECORDING_S3_REGION";
const ACCESS_KEY_ENV: &str = "MSS_RECORDING_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "MSS_RECORDING_S3_SECRET_ACCESS_KEY";
const SPILL_DIR_ENV: &str = "MSS_RECORDING_SPILL_DIR";
const DEFAULT_REGION: &str = "us-east-1";
const UPLOAD_RETRIES: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordingFormat {
    Wav,
}

impl RecordingFormat {
    fn parse(text: &str) -> Option<RecordingFormat> {
        match text {
            "wav" => Some(RecordingFormat::Wav),
            _ => None,
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            RecordingFormat::Wav => "wav",
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            RecordingFormat::Wav => "audio/wav",
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IdentityError {
    #[error("a file-s3 endpoint carries the recording identity and cannot be empty")]
    Empty,
    #[error("a recording identity has no room for whitespace or control characters")]
    NotPrintable,
    #[error("{0} names no account before the separator")]
    NoAccount(String),
    #[error("{0} has no separator, so it names no account")]
    NoSeparator(String),
    #[error("{0} nests the recording under a prefix, and exactly one separator is allowed")]
    Nested(String),
    #[error("{0} uses a relative path segment")]
    Traversal(String),
    #[error("{0} names no recording id")]
    NoRecordingId(String),
    #[error("{0} carries no format extension")]
    NoFormat(String),
    #[error("{0} asks for format {1}, and this recorder writes wav only")]
    UnsupportedFormat(String, String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingIdentity {
    pub account_id: String,
    pub recording_id: String,
    pub format: RecordingFormat,
}

impl RecordingIdentity {
    pub fn parse(endpoint: &str) -> Result<RecordingIdentity, IdentityError> {
        if endpoint.is_empty() {
            return Err(IdentityError::Empty);
        }
        if endpoint
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
        {
            return Err(IdentityError::NotPrintable);
        }
        let owned = endpoint.to_string();
        let mut segments = endpoint.split('/');
        let account = segments.next().unwrap_or_default();
        let file = segments
            .next()
            .ok_or_else(|| IdentityError::NoSeparator(owned.clone()))?;
        if segments.next().is_some() {
            return Err(IdentityError::Nested(owned));
        }
        if account.is_empty() {
            return Err(IdentityError::NoAccount(owned));
        }
        if is_relative_segment(account) {
            return Err(IdentityError::Traversal(owned));
        }
        let (recording_id, extension) = file
            .rsplit_once('.')
            .ok_or_else(|| IdentityError::NoFormat(owned.clone()))?;
        if recording_id.is_empty() {
            return Err(IdentityError::NoRecordingId(owned));
        }
        if is_relative_segment(recording_id) {
            return Err(IdentityError::Traversal(owned));
        }
        let format = RecordingFormat::parse(extension)
            .ok_or_else(|| IdentityError::UnsupportedFormat(owned, extension.to_string()))?;
        Ok(RecordingIdentity {
            account_id: account.to_string(),
            recording_id: recording_id.to_string(),
            format,
        })
    }

    pub fn object_key(&self) -> String {
        format!(
            "{}/{}.{}",
            self.account_id,
            self.recording_id,
            self.format.extension()
        )
    }

    pub fn participant_key(&self, participant: &str) -> String {
        format!(
            "{}/{}/{}.{}",
            self.account_id,
            self.recording_id,
            participant,
            self.format.extension()
        )
    }
}

fn is_relative_segment(segment: &str) -> bool {
    segment == "." || segment == ".."
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LabelError {
    #[error("a recording group member needs a label to name its own file")]
    Empty,
    #[error("{0} cannot name one file: a participant label carries no separator, whitespace or control character")]
    NotOneSegment(String),
    #[error("{0} is a relative path segment")]
    Traversal(String),
}

pub fn participant_label(label: &str) -> Result<&str, LabelError> {
    if label.is_empty() {
        return Err(LabelError::Empty);
    }
    if label.contains('/')
        || label
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(LabelError::NotOneSegment(label.to_string()));
    }
    if is_relative_segment(label) {
        return Err(LabelError::Traversal(label.to_string()));
    }
    Ok(label)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    Stereo,
    Mono(Track),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingTarget {
    pub key: String,
    pub layout: Layout,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SegmenterStats {
    pub frames_written: u64,
    pub frames_while_paused: u64,
    pub frames_out_of_order: u64,
    pub frames_beyond_cap: u64,
    pub segments: u64,
    pub lead_silence_frames: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedAudio {
    pub channels: u16,
    pub samples: Vec<i16>,
}

pub struct Segmenter {
    sample_rate_hz: u32,
    max_frames: usize,
    customer: Vec<i16>,
    agent: Vec<i16>,
    mixed: Vec<i16>,
    anchor_ms: Option<u64>,
    segment_start: usize,
    paused: bool,
    stats: SegmenterStats,
}

impl Segmenter {
    pub fn new(sample_rate_hz: u32, max_duration: Duration) -> Segmenter {
        let seconds = max_duration.as_secs().max(1);
        let max_frames = (sample_rate_hz.max(1) as u64).saturating_mul(seconds) as usize;
        Segmenter {
            sample_rate_hz: sample_rate_hz.max(1),
            max_frames,
            customer: Vec::new(),
            agent: Vec::new(),
            mixed: Vec::new(),
            anchor_ms: None,
            segment_start: 0,
            paused: false,
            stats: SegmenterStats {
                segments: 1,
                ..SegmenterStats::default()
            },
        }
    }

    pub fn lead_with_silence(&mut self, lead: Duration) -> bool {
        if self.stats.frames_written > 0 || self.stats.lead_silence_frames > 0 {
            return false;
        }
        let frames = lead
            .as_millis()
            .saturating_mul(self.sample_rate_hz as u128)
            .saturating_div(1000)
            .min(self.max_frames as u128) as usize;
        if frames == 0 {
            return false;
        }
        self.stats.lead_silence_frames = frames as u64;
        self.segment_start = frames;
        true
    }

    pub fn accept(&mut self, track: Track, timestamp_ms: u64, samples: &[i16]) {
        if samples.is_empty() {
            return;
        }
        if self.paused {
            self.stats.frames_while_paused += 1;
            return;
        }
        let anchor = *self.anchor_ms.get_or_insert(timestamp_ms);
        if timestamp_ms < anchor {
            self.stats.frames_out_of_order += 1;
        }
        let offset_frames = timestamp_ms.saturating_sub(anchor) * self.sample_rate_hz as u64 / 1000;
        let position = self.segment_start.saturating_add(offset_frames as usize);
        if position + samples.len() > self.max_frames {
            self.stats.frames_beyond_cap += 1;
            return;
        }
        let target = match track {
            Track::Customer => &mut self.customer,
            Track::Agent => &mut self.agent,
            Track::Mixed => &mut self.mixed,
        };
        place(target, position, samples, track == Track::Mixed);
        self.stats.frames_written += 1;
    }

    pub fn pause(&mut self) -> bool {
        if self.paused {
            return false;
        }
        self.paused = true;
        self.segment_start = self.frames().max(self.segment_start);
        self.anchor_ms = None;
        true
    }

    pub fn resume(&mut self) -> bool {
        if !self.paused {
            return false;
        }
        self.paused = false;
        self.stats.segments += 1;
        true
    }

    pub fn frames(&self) -> usize {
        self.customer
            .len()
            .max(self.agent.len())
            .max(self.mixed.len())
    }

    pub fn duration_ms(&self) -> u64 {
        self.frames() as u64 * 1000 / self.sample_rate_hz as u64
    }

    pub fn stats(&self) -> SegmenterStats {
        self.stats
    }

    pub fn render(&self, layout: Layout) -> RecordedAudio {
        let frames = self.frames();
        match layout {
            Layout::Mono(only) => RecordedAudio {
                channels: 1,
                samples: match only {
                    Track::Customer => self.customer.clone(),
                    Track::Agent => self.agent.clone(),
                    Track::Mixed => self.mixed.clone(),
                },
            },
            Layout::Stereo => {
                let mut samples = Vec::with_capacity(frames * 2);
                for frame in 0..frames {
                    samples.push(at(&self.customer, frame));
                    samples.push(at(&self.agent, frame).saturating_add(at(&self.mixed, frame)));
                }
                RecordedAudio {
                    channels: 2,
                    samples,
                }
            }
        }
    }
}

fn at(buffer: &[i16], frame: usize) -> i16 {
    buffer.get(frame).copied().unwrap_or(0)
}

fn place(buffer: &mut Vec<i16>, position: usize, samples: &[i16], sum: bool) {
    if buffer.len() < position + samples.len() {
        buffer.resize(position + samples.len(), 0);
    }
    for (offset, sample) in samples.iter().enumerate() {
        let slot = &mut buffer[position + offset];
        *slot = if sum {
            slot.saturating_add(*sample)
        } else {
            *sample
        };
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecorderError {
    #[error("wav: {0}")]
    Wav(#[from] hound::Error),
    #[error("{0}")]
    Encoding(String),
}

pub fn wav_bytes(
    sample_rate_hz: u32,
    channels: u16,
    samples: &[i16],
) -> Result<Vec<u8>, RecorderError> {
    let spec = hound::WavSpec {
        channels: channels.max(1),
        sample_rate: sample_rate_hz,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut cursor = std::io::Cursor::new(Vec::with_capacity(44 + samples.len() * 2));
    let mut writer = hound::WavWriter::new(&mut cursor, spec)?;
    for sample in samples {
        writer.write_sample(*sample)?;
    }
    writer.finalize()?;
    Ok(cursor.into_inner())
}

#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("recording storage is misconfigured: {0}")]
    Configuration(String),
    #[error("{0} is not a usable object key: {1}")]
    Key(String, String),
    #[error("the object store refused the upload: {0}")]
    Refused(String),
    #[error("the upload did not finish within {0:?}")]
    TimedOut(Duration),
}

#[control_api::async_trait]
pub trait RecordingSink: Send + Sync + 'static {
    async fn put(
        &self,
        key: &str,
        content_type: &'static str,
        body: Vec<u8>,
    ) -> Result<String, UploadError>;

    fn describe(&self) -> String;
}

pub struct S3RecordingSink {
    store: object_store::aws::AmazonS3,
    bucket: String,
    endpoint: Option<String>,
}

fn install_crypto_provider() {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.get_or_init(|| {
        if rustls::crypto::ring::default_provider()
            .install_default()
            .is_err()
        {
            info!("this process already had a rustls crypto provider");
        }
    });
}

impl S3RecordingSink {
    pub fn new(
        bucket: String,
        region: String,
        endpoint: Option<String>,
        credentials: Option<(String, String)>,
    ) -> Result<S3RecordingSink, UploadError> {
        install_crypto_provider();
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&bucket)
            .with_region(region)
            .with_retry(RetryConfig {
                max_retries: UPLOAD_RETRIES,
                retry_timeout: UPLOAD_TIMEOUT,
                ..RetryConfig::default()
            });
        if let Some(endpoint) = &endpoint {
            builder = builder
                .with_endpoint(endpoint.clone())
                .with_virtual_hosted_style_request(false)
                .with_allow_http(!endpoint.starts_with("https:"));
        }
        if let Some((access_key_id, secret_access_key)) = credentials {
            builder = builder
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key);
        }
        let store = builder
            .build()
            .map_err(|error| UploadError::Configuration(error.to_string()))?;
        Ok(S3RecordingSink {
            store,
            bucket,
            endpoint,
        })
    }
}

#[control_api::async_trait]
impl RecordingSink for S3RecordingSink {
    async fn put(
        &self,
        key: &str,
        content_type: &'static str,
        body: Vec<u8>,
    ) -> Result<String, UploadError> {
        let path = object_store::path::Path::parse(key)
            .map_err(|error| UploadError::Key(key.to_string(), error.to_string()))?;
        let options = object_store::PutOptions {
            attributes: object_store::Attributes::from_iter([(
                object_store::Attribute::ContentType,
                object_store::AttributeValue::from(content_type),
            )]),
            ..object_store::PutOptions::default()
        };
        self.store
            .put_opts(&path, PutPayload::from(body), options)
            .await
            .map_err(|error| UploadError::Refused(error.to_string()))?;
        Ok(format!(
            "s3:{URI_SCHEME_SEPARATOR}{}/{key}",
            self.bucket.as_str()
        ))
    }

    fn describe(&self) -> String {
        match &self.endpoint {
            Some(endpoint) => format!("bucket {} at {endpoint}", self.bucket),
            None => format!("bucket {} on aws s3", self.bucket),
        }
    }
}

#[derive(Default)]
pub struct RecorderCounters {
    pub started: AtomicU64,
    pub stopped: AtomicU64,
    pub pauses: AtomicU64,
    pub uploaded: AtomicU64,
    pub upload_failures: AtomicU64,
    pub spilled: AtomicU64,
    pub bytes_uploaded: AtomicU64,
    pub seconds_recorded: AtomicU64,
    pub truncated: AtomicU64,
    pub live: AtomicU64,
    pub groups_live: AtomicU64,
    pub group_members_live: AtomicU64,
    pub group_joins_refused: AtomicU64,
}

#[derive(Clone, Default)]
pub struct RecordingSupport {
    pub sink: Option<Arc<dyn RecordingSink>>,
    pub spill_dir: Option<PathBuf>,
    pub counters: Arc<RecorderCounters>,
}

impl RecordingSupport {
    pub fn from_env() -> Result<RecordingSupport, UploadError> {
        let spill_dir = std::env::var(SPILL_DIR_ENV).ok().map(PathBuf::from);
        let Ok(bucket) = std::env::var(BUCKET_ENV) else {
            return Ok(RecordingSupport {
                sink: None,
                spill_dir,
                counters: Arc::new(RecorderCounters::default()),
            });
        };
        if bucket.is_empty() {
            return Err(UploadError::Configuration(format!(
                "{BUCKET_ENV} is set but names no bucket"
            )));
        }
        let region = std::env::var(REGION_ENV).unwrap_or_else(|_| DEFAULT_REGION.to_string());
        let endpoint = std::env::var(ENDPOINT_ENV).ok().filter(|it| !it.is_empty());
        let credentials = match (
            std::env::var(ACCESS_KEY_ENV).ok(),
            std::env::var(SECRET_KEY_ENV).ok(),
        ) {
            (Some(access_key_id), Some(secret_access_key)) => {
                Some((access_key_id, secret_access_key))
            }
            _ => None,
        };
        let sink = S3RecordingSink::new(bucket, region, endpoint, credentials)?;
        info!(
            storage = %sink.describe(),
            spill_dir = ?spill_dir,
            "recording uploads are configured"
        );
        Ok(RecordingSupport {
            sink: Some(Arc::new(sink)),
            spill_dir,
            counters: Arc::new(RecorderCounters::default()),
        })
    }
}

pub struct RecorderSpec {
    pub session: SessionId,
    pub recording_id: String,
    pub format: RecordingFormat,
    pub targets: Vec<RecordingTarget>,
    pub sample_rate_hz: u32,
    pub max_duration: Duration,
    pub group_anchor: Option<Instant>,
}

impl RecorderSpec {
    pub fn one_object(
        session: SessionId,
        identity: &RecordingIdentity,
        layout: Layout,
        sample_rate_hz: u32,
    ) -> RecorderSpec {
        RecorderSpec {
            session,
            recording_id: identity.recording_id.clone(),
            format: identity.format,
            targets: vec![RecordingTarget {
                key: identity.object_key(),
                layout,
            }],
            sample_rate_hz,
            max_duration: MAX_RECORDING,
            group_anchor: None,
        }
    }
}

enum RecorderCommand {
    Pause,
    Resume,
    Finish,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingOutcome {
    pub duration_ms: u64,
    pub frames: usize,
    pub bytes: usize,
    pub uris: Vec<String>,
    pub stats: SegmenterStats,
}

pub struct RecorderHandle {
    commands: mpsc::Sender<RecorderCommand>,
    task: tokio::task::JoinHandle<RecordingOutcome>,
}

impl RecorderHandle {
    pub fn set_paused(&self, paused: bool) -> bool {
        let command = if paused {
            RecorderCommand::Pause
        } else {
            RecorderCommand::Resume
        };
        self.commands.try_send(command).is_ok()
    }

    pub async fn finish(self) -> Option<RecordingOutcome> {
        let _ = self.commands.try_send(RecorderCommand::Finish);
        let aborter = self.task.abort_handle();
        match tokio::time::timeout(FINISH_TIMEOUT, self.task).await {
            Ok(Ok(outcome)) => Some(outcome),
            Ok(Err(error)) => {
                warn!(%error, "a recorder task ended without an outcome");
                None
            }
            Err(_) => {
                aborter.abort();
                warn!(
                    timeout_ms = FINISH_TIMEOUT.as_millis() as u64,
                    "a recorder did not finish in time; its audio is lost"
                );
                None
            }
        }
    }
}

pub fn spawn(
    spec: RecorderSpec,
    subscription: Subscription,
    support: RecordingSupport,
    observer: Option<Weak<dyn ObservationSink>>,
) -> RecorderHandle {
    let (commands, inbox) = mpsc::channel(COMMAND_DEPTH);
    let task = tokio::spawn(run(spec, subscription, inbox, support, observer));
    RecorderHandle { commands, task }
}

async fn run(
    spec: RecorderSpec,
    mut subscription: Subscription,
    mut commands: mpsc::Receiver<RecorderCommand>,
    support: RecordingSupport,
    observer: Option<Weak<dyn ObservationSink>>,
) -> RecordingOutcome {
    let counters = Arc::clone(&support.counters);
    counters.live.fetch_add(1, Ordering::Relaxed);
    let recording_id = spec.recording_id.clone();
    let mut segmenter = Segmenter::new(spec.sample_rate_hz, spec.max_duration);
    let mut group_anchor = spec.group_anchor;

    loop {
        tokio::select! {
            biased;
            event = subscription.next() => match event {
                Some(event) => absorb(&mut segmenter, event, &mut group_anchor),
                None => break,
            },
            command = commands.recv() => match command {
                Some(RecorderCommand::Pause) => {
                    if segmenter.pause() {
                        counters.pauses.fetch_add(1, Ordering::Relaxed);
                        observe(
                            &observer,
                            spec.session,
                            Observation::RecordingPaused {
                                recording_id: recording_id.clone(),
                                paused: true,
                                duration_ms: segmenter.duration_ms(),
                            },
                        );
                    }
                }
                Some(RecorderCommand::Resume) => {
                    if segmenter.resume() {
                        observe(
                            &observer,
                            spec.session,
                            Observation::RecordingPaused {
                                recording_id: recording_id.clone(),
                                paused: false,
                                duration_ms: segmenter.duration_ms(),
                            },
                        );
                    }
                }
                Some(RecorderCommand::Finish) | None => break,
            },
        }
    }
    while let Some(event) = subscription.try_next() {
        absorb(&mut segmenter, event, &mut group_anchor);
    }

    let stats = segmenter.stats();
    let duration_ms = segmenter.duration_ms();
    let frames = segmenter.frames();
    counters.live.fetch_sub(1, Ordering::Relaxed);
    counters.stopped.fetch_add(1, Ordering::Relaxed);
    counters
        .seconds_recorded
        .fetch_add(duration_ms / 1000, Ordering::Relaxed);
    if stats.frames_beyond_cap > 0 {
        counters.truncated.fetch_add(1, Ordering::Relaxed);
        warn!(
            %recording_id,
            frames_beyond_cap = stats.frames_beyond_cap,
            max_seconds = spec.max_duration.as_secs(),
            "this recording hit its length cap; the tail is missing"
        );
    }
    observe(
        &observer,
        spec.session,
        Observation::RecordingStopped {
            recording_id: recording_id.clone(),
            duration_ms,
        },
    );

    let rate = spec.sample_rate_hz;
    let content_type = spec.format.content_type();
    let mut uris = Vec::with_capacity(spec.targets.len());
    let mut total = 0usize;
    for target in &spec.targets {
        let audio = segmenter.render(target.layout);
        let channels = audio.channels;
        let built =
            tokio::task::spawn_blocking(move || wav_bytes(rate, audio.channels, &audio.samples))
                .await;
        let bytes = match built.unwrap_or_else(|error| {
            Err(RecorderError::Encoding(format!(
                "the wav writer thread died: {error}"
            )))
        }) {
            Ok(built) => built,
            Err(error) => {
                counters.upload_failures.fetch_add(1, Ordering::Relaxed);
                warn!(
                    %recording_id,
                    key = %target.key,
                    %error,
                    "this recording could not be encoded as wav"
                );
                continue;
            }
        };
        let size = bytes.len();
        info!(
            %recording_id,
            key = %target.key,
            duration_ms,
            frames,
            channels,
            segments = stats.segments,
            lead_silence_frames = stats.lead_silence_frames,
            bytes = size,
            "recording closed; uploading"
        );
        let uri = upload(&support, &target.key, content_type, bytes, &recording_id).await;
        if let Some(uri) = uri {
            counters.uploaded.fetch_add(1, Ordering::Relaxed);
            counters
                .bytes_uploaded
                .fetch_add(size as u64, Ordering::Relaxed);
            total += size;
            observe(
                &observer,
                spec.session,
                Observation::UploadCompleted {
                    recording_id: recording_id.clone(),
                    uri: uri.clone(),
                },
            );
            uris.push(uri);
        }
    }
    RecordingOutcome {
        duration_ms,
        frames,
        bytes: total,
        uris,
        stats,
    }
}

async fn upload(
    support: &RecordingSupport,
    key: &str,
    content_type: &'static str,
    bytes: Vec<u8>,
    recording_id: &str,
) -> Option<String> {
    let Some(sink) = support.sink.as_ref() else {
        support
            .counters
            .upload_failures
            .fetch_add(1, Ordering::Relaxed);
        warn!(
            %recording_id,
            %key,
            env = BUCKET_ENV,
            "no recording storage is configured; this recording is lost"
        );
        spill(support, key, bytes, recording_id).await;
        return None;
    };
    let keep = support.spill_dir.as_ref().map(|_| bytes.clone());
    let attempt = tokio::time::timeout(UPLOAD_TIMEOUT, sink.put(key, content_type, bytes)).await;
    let outcome = match attempt {
        Ok(outcome) => outcome,
        Err(_) => Err(UploadError::TimedOut(UPLOAD_TIMEOUT)),
    };
    match outcome {
        Ok(uri) => {
            info!(%recording_id, %uri, "recording uploaded");
            Some(uri)
        }
        Err(error) => {
            support
                .counters
                .upload_failures
                .fetch_add(1, Ordering::Relaxed);
            warn!(%recording_id, %key, %error, "the recording upload failed");
            if let Some(bytes) = keep {
                spill(support, key, bytes, recording_id).await;
            }
            None
        }
    }
}

async fn spill(support: &RecordingSupport, key: &str, bytes: Vec<u8>, recording_id: &str) {
    let Some(dir) = support.spill_dir.as_ref() else {
        return;
    };
    let path = dir.join(key);
    let target = path.clone();
    let written = tokio::task::spawn_blocking(move || write_spill(&target, &bytes))
        .await
        .unwrap_or_else(|error| {
            Err(std::io::Error::other(format!(
                "the spill thread died: {error}"
            )))
        });
    match written {
        Ok(()) => {
            support.counters.spilled.fetch_add(1, Ordering::Relaxed);
            warn!(
                %recording_id,
                path = %path.display(),
                "the recording was kept on local disk for a later upload"
            );
        }
        Err(error) => warn!(
            %recording_id,
            path = %path.display(),
            %error,
            "the recording could not even be spilled to disk"
        ),
    }
}

fn write_spill(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, bytes)
}

fn absorb(segmenter: &mut Segmenter, event: TapEvent, group_anchor: &mut Option<Instant>) {
    if let TapEvent::Media {
        track,
        timestamp_ms,
        len,
        samples,
    } = event
    {
        if let Some(anchor) = group_anchor.take() {
            let lead = Instant::now().saturating_duration_since(anchor);
            if segmenter.lead_with_silence(lead) {
                info!(
                    lead_silence_ms = lead.as_millis() as u64,
                    "this recording group member joined late; its file opens with silence"
                );
            }
        }
        segmenter.accept(track, timestamp_ms, &samples[..len]);
    }
}

fn observe(
    observer: &Option<Weak<dyn ObservationSink>>,
    session: SessionId,
    observation: Observation,
) {
    match observer.as_ref().and_then(Weak::upgrade) {
        Some(sink) => sink.observe(session, observation),
        None => warn!(
            %session,
            ?observation,
            "no observation sink is wired; this recording callback reaches nobody"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::{Hub, TrackSelection};
    use std::sync::Mutex;

    const RATE: u32 = 8000;
    const FRAME: usize = 160;

    struct MemorySink {
        puts: Mutex<Vec<(String, Vec<u8>)>>,
        refuse: bool,
        stall: bool,
    }

    impl MemorySink {
        fn accepting() -> MemorySink {
            MemorySink {
                puts: Mutex::new(Vec::new()),
                refuse: false,
                stall: false,
            }
        }

        fn refusing() -> MemorySink {
            MemorySink {
                puts: Mutex::new(Vec::new()),
                refuse: true,
                stall: false,
            }
        }

        fn last(&self) -> Option<(String, Vec<u8>)> {
            self.puts.lock().unwrap().last().cloned()
        }

        fn keys(&self) -> Vec<String> {
            self.puts
                .lock()
                .unwrap()
                .iter()
                .map(|(key, _)| key.clone())
                .collect()
        }

        fn body(&self, key: &str) -> Vec<u8> {
            self.puts
                .lock()
                .unwrap()
                .iter()
                .find(|(held, _)| held == key)
                .map(|(_, body)| body.clone())
                .unwrap_or_else(|| panic!("{key} was never uploaded"))
        }
    }

    #[control_api::async_trait]
    impl RecordingSink for MemorySink {
        async fn put(
            &self,
            key: &str,
            content_type: &'static str,
            body: Vec<u8>,
        ) -> Result<String, UploadError> {
            assert_eq!(content_type, "audio/wav");
            if self.stall {
                tokio::time::sleep(UPLOAD_TIMEOUT * 3).await;
            }
            if self.refuse {
                return Err(UploadError::Refused("the bucket said no".to_string()));
            }
            self.puts.lock().unwrap().push((key.to_string(), body));
            Ok(format!("s3:{URI_SCHEME_SEPARATOR}lab-recordings/{key}"))
        }

        fn describe(&self) -> String {
            "an in-memory bucket".to_string()
        }
    }

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

    fn identity() -> RecordingIdentity {
        RecordingIdentity::parse("acct-42/rec-99.wav").unwrap()
    }

    fn tone(value: i16) -> Vec<i16> {
        vec![value; FRAME]
    }

    fn segmenter() -> Segmenter {
        Segmenter::new(RATE, MAX_RECORDING)
    }

    #[test]
    fn the_frozen_identity_round_trips_byte_exact() {
        for endpoint in [
            "acct-42/rec-99.wav",
            "1/2.wav",
            "acct_42/recording.with.dots.wav",
            "ACCT/REC.wav",
        ] {
            let parsed = RecordingIdentity::parse(endpoint)
                .unwrap_or_else(|error| panic!("{endpoint} was refused: {error}"));
            assert_eq!(parsed.object_key(), endpoint);
        }
        let parsed = identity();
        assert_eq!(parsed.account_id, "acct-42");
        assert_eq!(parsed.recording_id, "rec-99");
        assert_eq!(parsed.format, RecordingFormat::Wav);
    }

    #[test]
    fn anything_that_is_not_the_frozen_identity_is_refused_by_name() {
        let cases = [
            ("", IdentityError::Empty),
            ("acct 42/rec.wav", IdentityError::NotPrintable),
            (
                "rec-99.wav",
                IdentityError::NoSeparator("rec-99.wav".to_string()),
            ),
            (
                "/rec-99.wav",
                IdentityError::NoAccount("/rec-99.wav".to_string()),
            ),
            (
                "acct/nested/rec.wav",
                IdentityError::Nested("acct/nested/rec.wav".to_string()),
            ),
            (
                "../rec-99.wav",
                IdentityError::Traversal("../rec-99.wav".to_string()),
            ),
            (
                "acct-42/.wav",
                IdentityError::NoRecordingId("acct-42/.wav".to_string()),
            ),
            (
                "acct-42/rec-99",
                IdentityError::NoFormat("acct-42/rec-99".to_string()),
            ),
            (
                "acct-42/rec-99.mp3",
                IdentityError::UnsupportedFormat(
                    "acct-42/rec-99.mp3".to_string(),
                    "mp3".to_string(),
                ),
            ),
        ];
        for (endpoint, expected) in cases {
            assert_eq!(
                RecordingIdentity::parse(endpoint).unwrap_err(),
                expected,
                "{endpoint} was not refused the way we expect"
            );
        }
    }

    #[test]
    fn stereo_puts_the_customer_left_and_the_agent_right() {
        let mut held = segmenter();
        held.accept(Track::Customer, 0, &tone(100));
        held.accept(Track::Agent, 0, &tone(-100));
        held.accept(Track::Customer, 20, &tone(200));
        held.accept(Track::Agent, 20, &tone(-200));

        let audio = held.render(Layout::Stereo);
        assert_eq!(audio.channels, 2);
        assert_eq!(audio.samples.len(), FRAME * 2 * 2);
        assert_eq!(&audio.samples[..2], &[100, -100]);
        assert_eq!(
            &audio.samples[FRAME * 2..FRAME * 2 + 2],
            &[200, -200],
            "the second frame is not where wall-clock says it is"
        );
        assert_eq!(held.duration_ms(), 40);
    }

    #[test]
    fn a_leg_that_falls_silent_is_zero_filled_so_the_other_stays_wall_aligned() {
        let mut held = segmenter();
        held.accept(Track::Customer, 0, &tone(100));
        held.accept(Track::Customer, 200, &tone(300));

        let audio = held.render(Layout::Stereo);
        let frames = audio.samples.len() / 2;
        assert_eq!(frames, FRAME * 11);
        assert_eq!(audio.samples[0], 100);
        assert_eq!(audio.samples[1], 0);
        let resumed = FRAME * 10 * 2;
        assert_eq!(audio.samples[resumed], 300);
        assert_eq!(held.duration_ms(), 220);
    }

    #[test]
    fn a_paused_interval_is_absent_from_the_audio_but_the_duration_still_adds_up() {
        let mut held = segmenter();
        held.accept(Track::Customer, 0, &tone(11));
        held.accept(Track::Customer, 20, &tone(22));
        assert!(held.pause());
        assert!(!held.pause());
        for at in 0..50 {
            held.accept(Track::Customer, 40 + at * 20, &tone(99));
        }
        assert!(held.resume());
        assert!(!held.resume());
        held.accept(Track::Customer, 1040, &tone(33));
        held.accept(Track::Customer, 1060, &tone(44));

        let audio = held.render(Layout::Stereo);
        let left: Vec<i16> = audio.samples.iter().step_by(2).copied().collect();
        assert_eq!(left.len(), FRAME * 4, "the paused second is still in there");
        assert_eq!(
            [left[0], left[FRAME], left[FRAME * 2], left[FRAME * 3]],
            [11, 22, 33, 44],
            "the segments were not joined end to end"
        );
        assert_eq!(held.duration_ms(), 80);
        assert_eq!(held.stats().segments, 2);
        assert_eq!(held.stats().frames_while_paused, 50);
    }

    #[test]
    fn injected_bot_speech_is_summed_into_the_agent_channel() {
        let mut held = segmenter();
        held.accept(Track::Agent, 0, &tone(1000));
        held.accept(Track::Mixed, 0, &tone(2000));
        held.accept(Track::Mixed, 20, &tone(i16::MAX));
        held.accept(Track::Agent, 20, &tone(1000));

        let audio = held.render(Layout::Stereo);
        assert_eq!(audio.samples[1], 3000);
        assert_eq!(
            audio.samples[FRAME * 2 + 1],
            i16::MAX,
            "the sum must saturate rather than wrap"
        );
    }

    #[test]
    fn a_mono_recording_keeps_only_the_track_it_asked_for() {
        let mut held = segmenter();
        held.accept(Track::Customer, 0, &tone(7));
        held.accept(Track::Agent, 0, &tone(9));

        let audio = held.render(Layout::Mono(Track::Customer));
        assert_eq!(audio.channels, 1);
        assert_eq!(audio.samples.len(), FRAME);
        assert!(audio.samples.iter().all(|sample| *sample == 7));

        let other = held.render(Layout::Mono(Track::Agent));
        assert!(other.samples.iter().all(|sample| *sample == 9));
    }

    #[test]
    fn a_recording_stops_growing_at_its_cap_and_says_so() {
        let mut held = Segmenter::new(RATE, Duration::from_secs(1));
        for at in 0..60 {
            held.accept(Track::Customer, at * 20, &tone(5));
        }
        assert_eq!(held.frames(), RATE as usize);
        assert_eq!(held.stats().frames_beyond_cap, 10);
        assert_eq!(held.duration_ms(), 1000);
    }

    #[test]
    fn the_wav_is_sixteen_bit_stereo_at_the_tap_rate() {
        let bytes = wav_bytes(RATE, 2, &[1, -1, 2, -2]).unwrap();
        let reader = hound::WavReader::new(std::io::Cursor::new(bytes)).unwrap();
        let spec = reader.spec();
        assert_eq!(spec.channels, 2);
        assert_eq!(spec.sample_rate, RATE);
        assert_eq!(spec.bits_per_sample, 16);
        assert_eq!(reader.len(), 4);
    }

    fn support(sink: Arc<dyn RecordingSink>, spill_dir: Option<PathBuf>) -> RecordingSupport {
        RecordingSupport {
            sink: Some(sink),
            spill_dir,
            counters: Arc::new(RecorderCounters::default()),
        }
    }

    async fn wait_for(seen: &Collected, count: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while seen.seen().len() < count {
            assert!(
                std::time::Instant::now() < deadline,
                "the recorder reported {} observations, not {count}",
                seen.seen().len()
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    fn spec() -> RecorderSpec {
        RecorderSpec::one_object(SessionId::from_raw(1), &identity(), Layout::Stereo, RATE)
    }

    fn group_spec(labels: &[(&str, Layout)]) -> RecorderSpec {
        let identity = identity();
        RecorderSpec {
            session: SessionId::from_raw(1),
            recording_id: identity.recording_id.clone(),
            format: identity.format,
            targets: labels
                .iter()
                .map(|(participant, layout)| RecordingTarget {
                    key: identity.participant_key(participant),
                    layout: *layout,
                })
                .collect(),
            sample_rate_hz: RATE,
            max_duration: MAX_RECORDING,
            group_anchor: None,
        }
    }

    fn group_spec_anchored(labels: &[(&str, Layout)], lead: Duration) -> RecorderSpec {
        RecorderSpec {
            group_anchor: Some(
                std::time::Instant::now()
                    .checked_sub(lead)
                    .expect("this machine's clock has no room for a lead"),
            ),
            ..group_spec(labels)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_recording_reports_start_pause_stop_and_upload_in_that_order() {
        let (mut hub, client) = Hub::new();
        let subscription = client.attach(64, TrackSelection::All).unwrap();
        hub.poll_commands();
        let sink = Arc::new(MemorySink::accepting());
        let seen = Arc::new(Collected::default());
        let strong: Arc<dyn ObservationSink> = seen.clone();
        let observer = Arc::downgrade(&strong);
        let support = support(Arc::clone(&sink) as Arc<dyn RecordingSink>, None);
        let counters = Arc::clone(&support.counters);

        let handle = spawn(spec(), subscription, support, Some(observer));
        for at in 0..5u64 {
            hub.publish(TapEvent::media(Track::Customer, at * 20, &tone(64)));
            hub.publish(TapEvent::media(Track::Agent, at * 20, &tone(-64)));
        }
        assert!(handle.set_paused(true));
        wait_for(&seen, 1).await;
        for at in 5..25u64 {
            hub.publish(TapEvent::media(Track::Customer, at * 20, &tone(9)));
        }
        assert!(handle.set_paused(false));
        wait_for(&seen, 2).await;
        for at in 25..30u64 {
            hub.publish(TapEvent::media(Track::Customer, at * 20, &tone(32)));
            hub.publish(TapEvent::media(Track::Agent, at * 20, &tone(-32)));
        }

        let outcome = handle.finish().await.expect("the recorder had no outcome");
        drop(hub);

        assert_eq!(outcome.duration_ms, 200);
        assert_eq!(outcome.stats.segments, 2);
        let uri = outcome.uris.first().cloned().expect("nothing was uploaded");
        assert_eq!(outcome.uris.len(), 1);
        assert!(uri.ends_with("acct-42/rec-99.wav"), "{uri}");

        let events = seen.seen();
        assert_eq!(
            events,
            vec![
                Observation::RecordingPaused {
                    recording_id: "rec-99".to_string(),
                    paused: true,
                    duration_ms: 100,
                },
                Observation::RecordingPaused {
                    recording_id: "rec-99".to_string(),
                    paused: false,
                    duration_ms: 100,
                },
                Observation::RecordingStopped {
                    recording_id: "rec-99".to_string(),
                    duration_ms: 200,
                },
                Observation::UploadCompleted {
                    recording_id: "rec-99".to_string(),
                    uri,
                },
            ]
        );

        let (key, body) = sink.last().expect("the sink saw no object");
        assert_eq!(key, "acct-42/rec-99.wav");
        let reader = hound::WavReader::new(std::io::Cursor::new(body)).unwrap();
        assert_eq!(reader.spec().channels, 2);
        assert_eq!(reader.len() as usize, FRAME * 10 * 2);
        assert_eq!(counters.stopped.load(Ordering::Relaxed), 1);
        assert_eq!(counters.uploaded.load(Ordering::Relaxed), 1);
        assert_eq!(counters.pauses.load(Ordering::Relaxed), 1);
        assert_eq!(counters.live.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_hub_that_dies_closes_the_recording_without_losing_what_it_had() {
        let (mut hub, client) = Hub::new();
        let subscription = client.attach(64, TrackSelection::All).unwrap();
        hub.poll_commands();
        let sink = Arc::new(MemorySink::accepting());
        let seen = Arc::new(Collected::default());
        let strong: Arc<dyn ObservationSink> = seen.clone();
        let observer = Arc::downgrade(&strong);
        let handle = spawn(
            spec(),
            subscription,
            support(Arc::clone(&sink) as Arc<dyn RecordingSink>, None),
            Some(observer),
        );

        for at in 0..3u64 {
            hub.publish(TapEvent::media(Track::Customer, at * 20, &tone(5)));
        }
        drop(hub);

        let outcome = handle.finish().await.expect("the recorder had no outcome");
        assert_eq!(outcome.duration_ms, 60);
        assert_eq!(outcome.uris.len(), 1);
        assert!(matches!(
            seen.seen().first(),
            Some(Observation::RecordingStopped { .. })
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_upload_still_reports_the_stop_and_keeps_the_audio_on_disk() {
        let directory = std::env::temp_dir().join(format!(
            "mss-recorder-spill-{}",
            std::process::id() as u64 * 7 + 1
        ));
        let _ = std::fs::remove_dir_all(&directory);
        let (mut hub, client) = Hub::new();
        let subscription = client.attach(64, TrackSelection::All).unwrap();
        hub.poll_commands();
        let sink = Arc::new(MemorySink::refusing());
        let seen = Arc::new(Collected::default());
        let strong: Arc<dyn ObservationSink> = seen.clone();
        let observer = Arc::downgrade(&strong);
        let support = support(
            Arc::clone(&sink) as Arc<dyn RecordingSink>,
            Some(directory.clone()),
        );
        let counters = Arc::clone(&support.counters);
        let handle = spawn(spec(), subscription, support, Some(observer));

        hub.publish(TapEvent::media(Track::Customer, 0, &tone(5)));
        let outcome = handle.finish().await.expect("the recorder had no outcome");
        drop(hub);

        assert!(outcome.uris.is_empty());
        assert_eq!(
            seen.seen(),
            vec![Observation::RecordingStopped {
                recording_id: "rec-99".to_string(),
                duration_ms: 20,
            }]
        );
        assert_eq!(counters.upload_failures.load(Ordering::Relaxed), 1);
        assert_eq!(counters.spilled.load(Ordering::Relaxed), 1);
        let spilled = directory.join("acct-42/rec-99.wav");
        assert!(spilled.is_file(), "{} is missing", spilled.display());
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn a_group_member_gets_its_own_object_under_the_recordings_own_prefix() {
        let identity = identity();
        assert_eq!(identity.object_key(), "acct-42/rec-99.wav");
        assert_eq!(
            identity.participant_key("alice"),
            "acct-42/rec-99/alice.wav"
        );
        assert_eq!(
            identity.participant_key("alice.customer"),
            "acct-42/rec-99/alice.customer.wav"
        );
    }

    #[test]
    fn a_participant_label_that_could_escape_its_prefix_is_refused_by_name() {
        assert_eq!(participant_label("alice"), Ok("alice"));
        assert_eq!(participant_label("agent-7_b.left"), Ok("agent-7_b.left"));
        assert_eq!(participant_label(""), Err(LabelError::Empty));
        assert_eq!(
            participant_label("a/b"),
            Err(LabelError::NotOneSegment("a/b".to_string()))
        );
        assert_eq!(
            participant_label("two words"),
            Err(LabelError::NotOneSegment("two words".to_string()))
        );
        assert_eq!(
            participant_label(".."),
            Err(LabelError::Traversal("..".to_string()))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_member_that_records_both_tracks_uploads_one_mono_object_per_track() {
        let (mut hub, client) = Hub::new();
        let subscription = client.attach(64, TrackSelection::All).unwrap();
        hub.poll_commands();
        let sink = Arc::new(MemorySink::accepting());
        let seen = Arc::new(Collected::default());
        let strong: Arc<dyn ObservationSink> = seen.clone();
        let observer = Arc::downgrade(&strong);
        let support = support(Arc::clone(&sink) as Arc<dyn RecordingSink>, None);

        let handle = spawn(
            group_spec(&[
                ("alice.customer", Layout::Mono(Track::Customer)),
                ("alice.agent", Layout::Mono(Track::Agent)),
            ]),
            subscription,
            support,
            Some(observer),
        );
        for at in 0..3u64 {
            hub.publish(TapEvent::media(Track::Customer, at * 20, &tone(300)));
            hub.publish(TapEvent::media(Track::Agent, at * 20, &tone(-300)));
        }
        let outcome = handle.finish().await.expect("the recorder had no outcome");
        drop(hub);

        assert_eq!(outcome.duration_ms, 60);
        assert_eq!(outcome.uris.len(), 2);
        assert_eq!(
            sink.keys(),
            vec![
                "acct-42/rec-99/alice.customer.wav".to_string(),
                "acct-42/rec-99/alice.agent.wav".to_string(),
            ]
        );

        for (key, expected) in [
            ("acct-42/rec-99/alice.customer.wav", 300i16),
            ("acct-42/rec-99/alice.agent.wav", -300),
        ] {
            let reader = hound::WavReader::new(std::io::Cursor::new(sink.body(key))).unwrap();
            assert_eq!(reader.spec().channels, 1, "{key} is not mono");
            let samples: Vec<i16> = reader
                .into_samples::<i16>()
                .map(|held| held.unwrap())
                .collect();
            assert_eq!(samples.len(), FRAME * 3);
            assert!(
                samples.iter().all(|sample| *sample == expected),
                "{key} carries the other participant's audio"
            );
        }

        let events = seen.seen();
        assert_eq!(
            events
                .iter()
                .filter(|held| matches!(held, Observation::RecordingStopped { .. }))
                .count(),
            1,
            "a member reports one stop however many files it writes"
        );
        let uploads: Vec<&Observation> = events
            .iter()
            .filter(|held| matches!(held, Observation::UploadCompleted { .. }))
            .collect();
        assert_eq!(uploads.len(), 2);
        for upload in uploads {
            match upload {
                Observation::UploadCompleted { recording_id, uri } => {
                    assert_eq!(recording_id, "rec-99");
                    assert!(uri.contains("/rec-99/alice."), "{uri}");
                }
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn a_late_group_member_opens_its_file_with_silence_back_to_the_group_anchor() {
        let mut late = segmenter();
        assert!(late.lead_with_silence(Duration::from_millis(1000)));
        assert_eq!(late.stats().lead_silence_frames, RATE as u64);
        for at in 0..10u64 {
            late.accept(Track::Customer, at * 20, &tone(700));
        }

        assert_eq!(late.frames(), RATE as usize + FRAME * 10);
        assert_eq!(late.duration_ms(), 1200);
        let rendered = late.render(Layout::Mono(Track::Customer));
        assert_eq!(rendered.samples.len(), RATE as usize + FRAME * 10);
        assert!(
            rendered.samples[..RATE as usize]
                .iter()
                .all(|sample| *sample == 0),
            "the lead is not silent"
        );
        assert!(
            rendered.samples[RATE as usize..]
                .iter()
                .all(|sample| *sample == 700),
            "the member's own audio did not land after the lead"
        );
    }

    #[test]
    fn two_members_of_one_group_that_end_together_render_the_same_length() {
        let mut early = segmenter();
        for at in 0..100u64 {
            early.accept(Track::Customer, at * 20, &tone(100));
        }
        let mut late = segmenter();
        assert!(late.lead_with_silence(Duration::from_millis(1000)));
        for at in 0..50u64 {
            late.accept(Track::Customer, at * 20, &tone(-100));
        }

        assert_eq!(early.frames(), late.frames());
        assert_eq!(early.duration_ms(), 2000);
        assert_eq!(late.duration_ms(), 2000);
        assert_eq!(
            early.render(Layout::Stereo).samples.len(),
            late.render(Layout::Stereo).samples.len()
        );
    }

    #[test]
    fn a_paused_late_member_pads_its_lead_once_and_never_again() {
        let mut late = segmenter();
        assert!(late.lead_with_silence(Duration::from_millis(500)));
        for at in 0..10u64 {
            late.accept(Track::Customer, at * 20, &tone(21));
        }
        assert!(late.pause());
        for at in 10..40u64 {
            late.accept(Track::Customer, at * 20, &tone(21));
        }
        assert!(late.resume());
        for at in 0..10u64 {
            late.accept(Track::Customer, at * 20, &tone(22));
        }

        let lead = RATE as usize / 2;
        assert_eq!(late.frames(), lead + FRAME * 20);
        assert_eq!(late.stats().lead_silence_frames, lead as u64);
        assert_eq!(late.stats().segments, 2);
        assert!(
            !late.lead_with_silence(Duration::from_millis(500)),
            "a lead is padded once per recording, not once per segment"
        );
        assert_eq!(late.frames(), lead + FRAME * 20);
        let rendered = late.render(Layout::Mono(Track::Customer));
        assert!(rendered.samples[..lead].iter().all(|sample| *sample == 0));
        assert_eq!(rendered.samples[lead + FRAME * 10], 22);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_group_member_that_attaches_late_uploads_a_file_anchored_on_the_group() {
        let (mut hub, client) = Hub::new();
        let subscription = client.attach(64, TrackSelection::All).unwrap();
        hub.poll_commands();
        let sink = Arc::new(MemorySink::accepting());
        let support = support(Arc::clone(&sink) as Arc<dyn RecordingSink>, None);

        let handle = spawn(
            group_spec_anchored(
                &[("bob.customer", Layout::Mono(Track::Customer))],
                Duration::from_millis(400),
            ),
            subscription,
            support,
            None,
        );
        for at in 0..5u64 {
            hub.publish(TapEvent::media(Track::Customer, at * 20, &tone(500)));
        }
        let outcome = handle.finish().await.expect("the recorder had no outcome");
        drop(hub);

        let lead = outcome.stats.lead_silence_frames as usize;
        assert!(
            lead >= RATE as usize * 4 / 10,
            "the member padded {lead} frames for a 400 ms lead"
        );
        assert_eq!(outcome.frames, lead + FRAME * 5);
        let samples: Vec<i16> = hound::WavReader::new(std::io::Cursor::new(
            sink.body("acct-42/rec-99/bob.customer.wav"),
        ))
        .unwrap()
        .into_samples::<i16>()
        .map(|held| held.unwrap())
        .collect();
        assert_eq!(samples.len(), lead + FRAME * 5);
        assert!(samples[..lead].iter().all(|sample| *sample == 0));
        assert!(samples[lead..].iter().all(|sample| *sample == 500));
    }

    #[test]
    fn recording_storage_stays_off_until_a_bucket_is_named() {
        let support = RecordingSupport::default();
        assert!(support.sink.is_none());
        assert!(support.spill_dir.is_none());
    }

    #[test]
    fn a_minio_endpoint_is_allowed_to_be_plain_http_and_path_styled() {
        let sink = S3RecordingSink::new(
            "lab-recordings".to_string(),
            DEFAULT_REGION.to_string(),
            Some(format!("http:{URI_SCHEME_SEPARATOR}127.0.0.1:9000")),
            Some(("minioadmin".to_string(), "minioadmin".to_string())),
        )
        .expect("the lab sink did not build");
        assert!(sink.describe().contains("lab-recordings"));
    }
}
