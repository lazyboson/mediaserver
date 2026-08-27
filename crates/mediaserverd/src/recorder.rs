use crate::hub::{Subscription, TapEvent};
use crate::recording_spill::{
    DiskSpill, ObjectSpill, SegmentJournal, SpillStore, SpillWritten, SPILL_TIMEOUT,
};
use control_api::ObservationSink;
use media_core::Track;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, RetryConfig};
use session_core::{Observation, SessionId};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::{info, warn};

pub const IDENTITY_SCHEME: &str = "${accountID}/${recordingID}.${format}";
pub const RESUME_MS_METADATA_KEY: &str = "mss.recording.resumeMs";
pub const SPILL_OWNER_METADATA_KEY: &str = "mss.recording.spillOwner";
pub const MAX_RECORDING: Duration = Duration::from_secs(2 * 3600);
pub const UPLOAD_TIMEOUT: Duration = Duration::from_secs(60);
pub const STOP_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 4;
pub const SPILL_EVERY: Duration = Duration::from_secs(30);
pub const MAX_ADOPT_LEAD: Duration = Duration::from_secs(300);
pub const CONFERENCE_SHAPE_PREFIX: &str = "conference-";

const COMMAND_DEPTH: usize = 8;
const URI_SCHEME_SEPARATOR: &str = "\x2f\x2f";
const BUCKET_ENV: &str = "MSS_RECORDING_BUCKET";
const ENDPOINT_ENV: &str = "MSS_RECORDING_S3_ENDPOINT";
const REGION_ENV: &str = "MSS_RECORDING_S3_REGION";
const ACCESS_KEY_ENV: &str = "MSS_RECORDING_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "MSS_RECORDING_S3_SECRET_ACCESS_KEY";
const SPILL_DIR_ENV: &str = "MSS_RECORDING_SPILL_DIR";
const SPILL_SECONDS_ENV: &str = "MSS_RECORDING_SPILL_SECONDS";
pub const SPILL_TO_ENV: &str = "MSS_RECORDING_SPILL_TO";
pub const SPILL_PREFIX_ENV: &str = "MSS_RECORDING_SPILL_PREFIX";
pub const UPLOAD_CONCURRENCY_ENV: &str = "MSS_RECORDING_UPLOAD_CONCURRENCY";
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordingShape {
    Stereo,
    Track,
    Mixed,
    Participant,
}

impl RecordingShape {
    pub fn of(layout: Layout, grouped: bool) -> RecordingShape {
        match (grouped, layout) {
            (true, _) => RecordingShape::Participant,
            (false, Layout::Mono(Track::Mixed)) => RecordingShape::Mixed,
            (false, Layout::Mono(_)) => RecordingShape::Track,
            (false, Layout::Stereo) => RecordingShape::Stereo,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RecordingShape::Stereo => "stereo",
            RecordingShape::Track => "track",
            RecordingShape::Mixed => "mixed",
            RecordingShape::Participant => "participant",
        }
    }

    pub fn named(self, conferenced: bool) -> String {
        if conferenced {
            format!("{CONFERENCE_SHAPE_PREFIX}{}", self.as_str())
        } else {
            self.as_str().to_string()
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SegmenterStats {
    pub frames_written: u64,
    pub frames_while_paused: u64,
    pub frames_out_of_order: u64,
    pub frames_beyond_cap: u64,
    pub segments: u64,
    pub lead_silence_frames: u64,
    pub segments_spilled: u64,
    pub frames_lost_on_adopt: u64,
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
    spilled_frames: usize,
    sealed_frames: usize,
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
            spilled_frames: 0,
            sealed_frames: 0,
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

    pub fn resume_after(&mut self, frames_on_disk: usize) {
        self.spilled_frames = frames_on_disk;
    }

    pub fn lose_on_adopt(&mut self, frames: u64) {
        self.stats.frames_lost_on_adopt = self.stats.frames_lost_on_adopt.saturating_add(frames);
    }

    pub fn closable_frames(&self) -> usize {
        let frames_per_ms = (self.sample_rate_hz / 1000) as usize;
        if frames_per_ms == 0 {
            return 0;
        }
        self.frames() / frames_per_ms * frames_per_ms
    }

    pub fn render_closable(&self, frames: usize, layout: Layout) -> RecordedAudio {
        render_tracks(
            head(&self.customer, frames),
            head(&self.agent, frames),
            head(&self.mixed, frames),
            frames,
            layout,
        )
    }

    pub fn seal(&mut self, frames: usize) {
        self.sealed_frames = frames;
    }

    pub fn unseal(&mut self) {
        self.sealed_frames = 0;
    }

    pub fn close_segment(&mut self, frames: usize) {
        self.sealed_frames = 0;
        let frames_per_ms = (self.sample_rate_hz / 1000) as usize;
        if frames == 0 || frames_per_ms == 0 {
            return;
        }
        drop_front(&mut self.customer, frames);
        drop_front(&mut self.agent, frames);
        drop_front(&mut self.mixed, frames);
        let past_lead = frames.saturating_sub(self.segment_start);
        self.segment_start = self.segment_start.saturating_sub(frames);
        self.spilled_frames = self.spilled_frames.saturating_add(frames);
        if let Some(anchor) = self.anchor_ms.as_mut() {
            *anchor += (past_lead / frames_per_ms) as u64;
        }
        self.stats.segments_spilled += 1;
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
        let mut position = self.segment_start.saturating_add(offset_frames as usize);
        if position < self.sealed_frames {
            self.stats.frames_out_of_order += 1;
            position = self.sealed_frames;
        }
        if self.spilled_frames + position + samples.len() > self.max_frames {
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

    pub fn total_frames(&self) -> usize {
        self.spilled_frames.saturating_add(self.frames())
    }

    pub fn duration_ms(&self) -> u64 {
        self.total_frames() as u64 * 1000 / self.sample_rate_hz as u64
    }

    pub fn stats(&self) -> SegmenterStats {
        self.stats
    }

    pub fn render(&self, layout: Layout) -> RecordedAudio {
        render_tracks(
            &self.customer,
            &self.agent,
            &self.mixed,
            self.frames(),
            layout,
        )
    }
}

fn render_tracks(
    customer: &[i16],
    agent: &[i16],
    mixed: &[i16],
    frames: usize,
    layout: Layout,
) -> RecordedAudio {
    match layout {
        Layout::Mono(only) => {
            let track = match only {
                Track::Customer => customer,
                Track::Agent => agent,
                Track::Mixed => mixed,
            };
            let mut samples = track.to_vec();
            samples.resize(frames, 0);
            RecordedAudio {
                channels: 1,
                samples,
            }
        }
        Layout::Stereo => {
            let mut samples = Vec::with_capacity(frames * 2);
            for frame in 0..frames {
                samples.push(at(customer, frame));
                samples.push(at(agent, frame).saturating_add(at(mixed, frame)));
            }
            RecordedAudio {
                channels: 2,
                samples,
            }
        }
    }
}

fn head(buffer: &[i16], frames: usize) -> &[i16] {
    &buffer[..frames.min(buffer.len())]
}

fn drop_front(buffer: &mut Vec<i16>, frames: usize) {
    let take = buffer.len().min(frames);
    buffer.drain(..take);
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
    #[error("{0} is not in storage")]
    Missing(String),
}

#[control_api::async_trait]
pub trait RecordingSink: Send + Sync + 'static {
    async fn put(
        &self,
        key: &str,
        content_type: &'static str,
        body: Vec<u8>,
    ) -> Result<String, UploadError>;

    async fn exists(&self, key: &str) -> Result<bool, UploadError>;

    async fn get(&self, key: &str) -> Result<Vec<u8>, UploadError>;

    async fn list(&self, prefix: &str) -> Result<Vec<String>, UploadError>;

    async fn delete(&self, key: &str) -> Result<(), UploadError>;

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

    async fn exists(&self, key: &str) -> Result<bool, UploadError> {
        let path = object_store::path::Path::parse(key)
            .map_err(|error| UploadError::Key(key.to_string(), error.to_string()))?;
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(UploadError::Refused(error.to_string())),
        }
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, UploadError> {
        let path = object_store::path::Path::parse(key)
            .map_err(|error| UploadError::Key(key.to_string(), error.to_string()))?;
        let fetched = match self.store.get(&path).await {
            Ok(fetched) => fetched,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(UploadError::Missing(key.to_string()))
            }
            Err(error) => return Err(UploadError::Refused(error.to_string())),
        };
        let bytes = fetched
            .bytes()
            .await
            .map_err(|error| UploadError::Refused(error.to_string()))?;
        Ok(bytes.to_vec())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, UploadError> {
        let trimmed = prefix.trim_end_matches('/');
        let under = if trimmed.is_empty() {
            None
        } else {
            Some(
                object_store::path::Path::parse(trimmed)
                    .map_err(|error| UploadError::Key(prefix.to_string(), error.to_string()))?,
            )
        };
        let mut listing = self.store.list(under.as_ref());
        let mut keys = Vec::new();
        while let Some(entry) = futures_util::StreamExt::next(&mut listing).await {
            let meta = entry.map_err(|error| UploadError::Refused(error.to_string()))?;
            keys.push(meta.location.as_ref().to_string());
        }
        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<(), UploadError> {
        let path = object_store::path::Path::parse(key)
            .map_err(|error| UploadError::Key(key.to_string(), error.to_string()))?;
        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(UploadError::Refused(error.to_string())),
        }
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
    pub uploads_in_flight: AtomicU64,
    pub uploads_backgrounded: AtomicU64,
    pub upload_settle_timeouts: AtomicU64,
    pub spilled: AtomicU64,
    pub segments_spilled: AtomicU64,
    pub segment_spill_failures: AtomicU64,
    pub spill_lost_ownership: AtomicU64,
    pub spill_foreign_manifests: AtomicU64,
    pub salvaged: AtomicU64,
    pub salvage_skipped: AtomicU64,
    pub salvage_failures: AtomicU64,
    pub frames_lost_on_adopt: AtomicU64,
    pub bytes_uploaded: AtomicU64,
    pub seconds_recorded: AtomicU64,
    pub truncated: AtomicU64,
    pub live: AtomicU64,
    pub groups_live: AtomicU64,
    pub group_members_live: AtomicU64,
    pub group_joins_refused: AtomicU64,
}

#[derive(Clone)]
pub struct RecordingSupport {
    pub sink: Option<Arc<dyn RecordingSink>>,
    pub spill_dir: Option<PathBuf>,
    pub journal: Option<Arc<dyn SpillStore>>,
    pub spill_every: Duration,
    pub counters: Arc<RecorderCounters>,
    pub owner: String,
    pub upload_permits: Arc<Semaphore>,
}

impl Default for RecordingSupport {
    fn default() -> RecordingSupport {
        RecordingSupport {
            sink: None,
            spill_dir: None,
            journal: None,
            spill_every: SPILL_EVERY,
            counters: Arc::new(RecorderCounters::default()),
            owner: String::new(),
            upload_permits: Arc::new(Semaphore::new(DEFAULT_UPLOAD_CONCURRENCY)),
        }
    }
}

pub fn upload_concurrency_from_env() -> usize {
    let configured = std::env::var(UPLOAD_CONCURRENCY_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty());
    let Some(configured) = configured else {
        info!(
            env = UPLOAD_CONCURRENCY_ENV,
            value = "unset",
            uploads = DEFAULT_UPLOAD_CONCURRENCY,
            "recording uploads run in the background, this many at a time"
        );
        return DEFAULT_UPLOAD_CONCURRENCY;
    };
    match configured.trim().parse::<usize>() {
        Ok(uploads) if uploads > 0 => {
            info!(
                env = UPLOAD_CONCURRENCY_ENV,
                uploads, "recording uploads run in the background, this many at a time"
            );
            uploads
        }
        _ => {
            warn!(
                env = UPLOAD_CONCURRENCY_ENV,
                configured = %configured,
                uploads = DEFAULT_UPLOAD_CONCURRENCY,
                "the background upload concurrency must be a whole number above zero; \
                 falling back to the default"
            );
            DEFAULT_UPLOAD_CONCURRENCY
        }
    }
}

fn disk_journal(dir: PathBuf) -> Arc<dyn SpillStore> {
    Arc::new(DiskSpill::new(dir))
}

fn spill_to_object_store_from_env() -> bool {
    let configured = std::env::var(SPILL_TO_ENV).unwrap_or_default();
    match configured.trim().to_ascii_lowercase().as_str() {
        "" | "disk" => false,
        "s3" => true,
        other => {
            warn!(
                env = SPILL_TO_ENV,
                configured = %other,
                "a recording spill goes to disk or to s3; falling back to disk"
            );
            false
        }
    }
}

fn spill_prefix_from_env() -> String {
    let configured = std::env::var(SPILL_PREFIX_ENV).unwrap_or_default();
    let normalized = crate::recording_spill::normalized_prefix(&configured);
    if normalized.is_empty() {
        crate::recording_spill::DEFAULT_SPILL_PREFIX.to_string()
    } else {
        normalized
    }
}

impl RecordingSupport {
    pub fn from_env(owner: &str) -> Result<RecordingSupport, UploadError> {
        let upload_permits = Arc::new(Semaphore::new(upload_concurrency_from_env()));
        let spill_dir = std::env::var(SPILL_DIR_ENV).ok().map(PathBuf::from);
        let spill_every = std::env::var(SPILL_SECONDS_ENV)
            .ok()
            .and_then(|configured| configured.parse::<u64>().ok())
            .filter(|seconds| *seconds > 0)
            .map(Duration::from_secs)
            .unwrap_or(SPILL_EVERY);
        let to_object_store = spill_to_object_store_from_env();
        let Ok(bucket) = std::env::var(BUCKET_ENV) else {
            if to_object_store {
                warn!(
                    env = SPILL_TO_ENV,
                    bucket = BUCKET_ENV,
                    "the recording bucket is where the spill journal was asked to live, and no \
                     bucket is configured; nothing spills"
                );
            }
            return Ok(RecordingSupport {
                sink: None,
                journal: spill_dir.clone().map(disk_journal),
                spill_dir,
                spill_every,
                counters: Arc::new(RecorderCounters::default()),
                owner: owner.to_string(),
                upload_permits,
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
        let sink: Arc<dyn RecordingSink> =
            Arc::new(S3RecordingSink::new(bucket, region, endpoint, credentials)?);
        let journal: Option<Arc<dyn SpillStore>> = if to_object_store {
            let prefix = spill_prefix_from_env();
            info!(
                env = SPILL_TO_ENV,
                prefix = %prefix,
                "closed recording segments spill into the recording bucket itself, so any pod \
                 can read back what a dead pod held"
            );
            Some(Arc::new(ObjectSpill::new(Arc::clone(&sink), &prefix)))
        } else {
            spill_dir.clone().map(disk_journal)
        };
        info!(
            storage = %sink.describe(),
            spill_dir = ?spill_dir,
            spill_in_bucket = to_object_store,
            "recording uploads are configured"
        );
        Ok(RecordingSupport {
            sink: Some(sink),
            journal,
            spill_dir,
            spill_every,
            counters: Arc::new(RecorderCounters::default()),
            owner: owner.to_string(),
            upload_permits,
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
    pub resume_ms: u64,
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
            resume_ms: 0,
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

#[derive(Default)]
pub struct RecordingProgress {
    recorded_ms: AtomicU64,
    spilled_ms: AtomicU64,
}

impl RecordingProgress {
    pub fn recorded_ms(&self) -> u64 {
        self.recorded_ms.load(Ordering::Relaxed)
    }

    pub fn spilled_ms(&self) -> u64 {
        self.spilled_ms.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopReport {
    pub duration_ms: u64,
    pub frames: usize,
    pub stats: SegmenterStats,
}

pub struct FinishedCapture {
    pub session: SessionId,
    pub recording_id: String,
    pub stopped: Option<StopReport>,
    upload: tokio::task::JoinHandle<RecordingOutcome>,
}

#[cfg(test)]
pub const FINISH_TIMEOUT: Duration = Duration::from_secs(90);

impl FinishedCapture {
    pub fn into_upload(self) -> tokio::task::JoinHandle<RecordingOutcome> {
        self.upload
    }

    #[cfg(test)]
    pub async fn settle(self) -> Option<RecordingOutcome> {
        let recording_id = self.recording_id.clone();
        let upload = self.upload;
        let aborter = upload.abort_handle();
        match tokio::time::timeout(FINISH_TIMEOUT, upload).await {
            Ok(Ok(outcome)) => Some(outcome),
            Ok(Err(error)) => {
                warn!(%recording_id, %error, "a recorder task ended without an outcome");
                None
            }
            Err(_) => {
                aborter.abort();
                warn!(
                    %recording_id,
                    timeout_ms = FINISH_TIMEOUT.as_millis() as u64,
                    "a recording upload did not finish in time; its audio is lost"
                );
                None
            }
        }
    }
}

pub struct RecorderHandle {
    session: SessionId,
    recording_id: String,
    commands: mpsc::Sender<RecorderCommand>,
    stopped: oneshot::Receiver<StopReport>,
    task: tokio::task::JoinHandle<RecordingOutcome>,
    progress: Arc<RecordingProgress>,
}

impl RecorderHandle {
    pub fn progress(&self) -> Arc<RecordingProgress> {
        Arc::clone(&self.progress)
    }

    pub fn set_paused(&self, paused: bool) -> bool {
        let command = if paused {
            RecorderCommand::Pause
        } else {
            RecorderCommand::Resume
        };
        self.commands.try_send(command).is_ok()
    }

    pub async fn finish(self) -> FinishedCapture {
        let RecorderHandle {
            session,
            recording_id,
            commands,
            stopped,
            task,
            ..
        } = self;
        let _ = commands.try_send(RecorderCommand::Finish);
        drop(commands);
        let stopped = match tokio::time::timeout(STOP_TIMEOUT, stopped).await {
            Ok(Ok(report)) => Some(report),
            Ok(Err(_)) => {
                warn!(
                    %recording_id,
                    "a recorder ended before it reported its stop; its upload is watched anyway"
                );
                None
            }
            Err(_) => {
                warn!(
                    %recording_id,
                    timeout_ms = STOP_TIMEOUT.as_millis() as u64,
                    "a recorder did not close its segment in time; the detach is answered \
                     without a stop report and the upload is watched anyway"
                );
                None
            }
        };
        FinishedCapture {
            session,
            recording_id,
            stopped,
            upload: task,
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
    let (reporter, stopped) = oneshot::channel();
    let progress = Arc::new(RecordingProgress::default());
    let session = spec.session;
    let recording_id = spec.recording_id.clone();
    let task = tokio::spawn(run(
        spec,
        subscription,
        inbox,
        support,
        observer,
        Arc::clone(&progress),
        reporter,
    ));
    RecorderHandle {
        session,
        recording_id,
        commands,
        stopped,
        task,
        progress,
    }
}

async fn run(
    spec: RecorderSpec,
    mut subscription: Subscription,
    mut commands: mpsc::Receiver<RecorderCommand>,
    support: RecordingSupport,
    observer: Option<Weak<dyn ObservationSink>>,
    progress: Arc<RecordingProgress>,
    reporter: oneshot::Sender<StopReport>,
) -> RecordingOutcome {
    let counters = Arc::clone(&support.counters);
    counters.live.fetch_add(1, Ordering::Relaxed);
    let recording_id = spec.recording_id.clone();
    let mut segmenter = Segmenter::new(spec.sample_rate_hz, spec.max_duration);
    let mut group_anchor = spec.group_anchor;
    let frames_per_second = spec.sample_rate_hz.max(1) as u64;
    let mut journal = SegmentJournal::open(
        &support,
        &spec.recording_id,
        &support.owner,
        spec.sample_rate_hz,
        &spec
            .targets
            .iter()
            .map(|target| (target.key.clone(), channels_of(target.layout)))
            .collect::<Vec<(String, u16)>>(),
    )
    .await;
    let recovered_frames = journal
        .as_ref()
        .map(SegmentJournal::frames_on_disk)
        .unwrap_or_default();
    segmenter.resume_after(recovered_frames as usize);
    progress.spilled_ms.store(
        recovered_frames * 1000 / frames_per_second,
        Ordering::Relaxed,
    );
    if spec.resume_ms > 0 {
        group_anchor = None;
        let recovered_ms = recovered_frames * 1000 / frames_per_second;
        let missing_ms = spec.resume_ms.saturating_sub(recovered_ms);
        let lost_frames = missing_ms * frames_per_second / 1000;
        segmenter.lose_on_adopt(lost_frames);
        counters
            .frames_lost_on_adopt
            .fetch_add(lost_frames, Ordering::Relaxed);
        let missing = Duration::from_millis(missing_ms);
        let padded = missing <= MAX_ADOPT_LEAD && segmenter.lead_with_silence(missing);
        warn!(
            %recording_id,
            resume_ms = spec.resume_ms,
            recovered_ms,
            missing_ms,
            padded,
            "this recording was adopted from another pod; only what the dead pod had spilled              to a store this pod can read is recovered, and the rest is counted silence"
        );
    }
    let mut segment_close = tokio::time::interval_at(
        tokio::time::Instant::now() + support.spill_every,
        support.spill_every,
    );
    let mut in_flight: Option<InFlightSpill> = None;

    loop {
        tokio::select! {
            biased;
            event = subscription.next() => match event {
                Some(event) => {
                    absorb(&mut segmenter, event, &mut group_anchor);
                    progress
                        .recorded_ms
                        .store(segmenter.duration_ms(), Ordering::Relaxed);
                }
                None => break,
            },
            command = commands.recv() => match command {
                Some(RecorderCommand::Pause) => {
                    if segmenter.pause() {
                        counters.pauses.fetch_add(1, Ordering::Relaxed);
                        begin_spill(&mut in_flight, &journal, &mut segmenter, &spec, &counters);
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
            written = spill_landed(&mut in_flight), if in_flight.is_some() => {
                if let Some(pending) = in_flight.take() {
                    settle_spill(
                        written,
                        pending.frames,
                        &mut journal,
                        &mut segmenter,
                        &spec,
                        &counters,
                        &progress,
                    );
                }
            }
            _ = segment_close.tick(), if journal.is_some() && in_flight.is_none() => {
                begin_spill(&mut in_flight, &journal, &mut segmenter, &spec, &counters);
            }
        }
    }
    while let Some(event) = subscription.try_next() {
        absorb(&mut segmenter, event, &mut group_anchor);
    }

    let stats = segmenter.stats();
    let duration_ms = segmenter.duration_ms();
    let frames = segmenter.total_frames();
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
    if reporter
        .send(StopReport {
            duration_ms,
            frames,
            stats,
        })
        .is_err()
    {
        info!(
            %recording_id,
            "nobody waited for this recording's stop report; its upload runs on regardless"
        );
    }
    if let Some(mut pending) = in_flight.take() {
        let written = match tokio::time::timeout(SPILL_TIMEOUT, pending.landed()).await {
            Ok(written) => written,
            Err(_) => SpillWritten::Failed(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "the spill write still pending when the recording stopped did not land \
                     within {} ms",
                    SPILL_TIMEOUT.as_millis()
                ),
            )),
        };
        settle_spill(
            written,
            pending.frames,
            &mut journal,
            &mut segmenter,
            &spec,
            &counters,
            &progress,
        );
    }
    let stats = segmenter.stats();
    let permit = acquire_upload_slot(&support, &recording_id).await;

    let rate = spec.sample_rate_hz;
    let content_type = spec.format.content_type();
    let mut uris = Vec::with_capacity(spec.targets.len());
    let mut total = 0usize;
    let mut uploaded_all = true;
    for (index, target) in spec.targets.iter().enumerate() {
        let mut audio = segmenter.render(target.layout);
        if let Some(held) = journal.as_ref() {
            let mut spilled = held.read_back(index).await;
            spilled.append(&mut audio.samples);
            audio.samples = spilled;
        }
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
                uploaded_all = false;
                warn!(
                    %recording_id,
                    key = %target.key,
                    %error,
                    "this recording could not be encoded as wav"
                );
                observe(
                    &observer,
                    spec.session,
                    Observation::UploadFailed {
                        recording_id: recording_id.clone(),
                        key: target.key.clone(),
                        error: error.to_string(),
                    },
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
            segments_spilled = stats.segments_spilled,
            frames_lost_on_adopt = stats.frames_lost_on_adopt,
            lead_silence_frames = stats.lead_silence_frames,
            bytes = size,
            "recording closed; uploading"
        );
        let uri = upload(&support, &target.key, content_type, bytes, &recording_id).await;
        match uri {
            Ok(uri) => {
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
            Err(error) => {
                uploaded_all = false;
                observe(
                    &observer,
                    spec.session,
                    Observation::UploadFailed {
                        recording_id: recording_id.clone(),
                        key: target.key.clone(),
                        error,
                    },
                );
            }
        }
    }
    drop(permit);
    if let Some(held) = journal {
        if uploaded_all {
            held.discard().await;
        } else {
            warn!(
                %recording_id,
                journal = %held.describe(),
                "this recording did not reach storage; its spilled segments stay in the spill \
                 store for the next start of this pod to salvage"
            );
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

async fn acquire_upload_slot(
    support: &RecordingSupport,
    recording_id: &str,
) -> Option<OwnedSemaphorePermit> {
    let waited = Instant::now();
    let permit = Arc::clone(&support.upload_permits).acquire_owned().await;
    match permit {
        Ok(permit) => {
            let waited_ms = waited.elapsed().as_millis() as u64;
            if waited_ms > 0 {
                info!(
                    %recording_id,
                    waited_ms,
                    env = UPLOAD_CONCURRENCY_ENV,
                    "this upload queued behind the ones already running"
                );
            }
            Some(permit)
        }
        Err(_) => {
            warn!(
                %recording_id,
                "the upload slots are closed; this upload runs without one"
            );
            None
        }
    }
}

async fn upload(
    support: &RecordingSupport,
    key: &str,
    content_type: &'static str,
    bytes: Vec<u8>,
    recording_id: &str,
) -> Result<String, String> {
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
        return Err(format!("{BUCKET_ENV} names no recording storage"));
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
            Ok(uri)
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
            Err(error.to_string())
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

fn channels_of(layout: Layout) -> u16 {
    match layout {
        Layout::Stereo => 2,
        Layout::Mono(_) => 1,
    }
}

struct InFlightSpill {
    frames: usize,
    task: tokio::task::JoinHandle<SpillWritten>,
}

impl InFlightSpill {
    async fn landed(&mut self) -> SpillWritten {
        match (&mut self.task).await {
            Ok(written) => written,
            Err(error) => SpillWritten::Failed(std::io::Error::other(format!(
                "the spill write task died: {error}"
            ))),
        }
    }
}

impl Drop for InFlightSpill {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn spill_landed(in_flight: &mut Option<InFlightSpill>) -> SpillWritten {
    match in_flight.as_mut() {
        Some(pending) => pending.landed().await,
        None => std::future::pending().await,
    }
}

fn begin_spill(
    in_flight: &mut Option<InFlightSpill>,
    journal: &Option<SegmentJournal>,
    segmenter: &mut Segmenter,
    spec: &RecorderSpec,
    counters: &RecorderCounters,
) {
    if in_flight.is_some() {
        return;
    }
    let Some(held) = journal.as_ref() else {
        return;
    };
    let frames = segmenter.closable_frames();
    if frames == 0 {
        return;
    }
    let rendered: Vec<RecordedAudio> = spec
        .targets
        .iter()
        .map(|target| segmenter.render_closable(frames, target.layout))
        .collect();
    match held.begin_append(rendered, frames as u64) {
        Ok(write) => {
            segmenter.seal(frames);
            *in_flight = Some(InFlightSpill {
                frames,
                task: tokio::spawn(write.perform()),
            });
        }
        Err(error) => {
            counters
                .segment_spill_failures
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                recording_id = %spec.recording_id,
                %error,
                "a closed recording segment could not be prepared for the spill store; it \
                 stays in memory"
            );
        }
    }
}

fn settle_spill(
    written: SpillWritten,
    frames: usize,
    journal: &mut Option<SegmentJournal>,
    segmenter: &mut Segmenter,
    spec: &RecorderSpec,
    counters: &RecorderCounters,
    progress: &RecordingProgress,
) {
    let Some(held) = journal.as_mut() else {
        segmenter.unseal();
        return;
    };
    match written {
        SpillWritten::Landed(manifest) => {
            held.commit(manifest);
            segmenter.close_segment(frames);
            counters.segments_spilled.fetch_add(1, Ordering::Relaxed);
            progress.spilled_ms.store(
                held.frames_on_disk() * 1000 / spec.sample_rate_hz.max(1) as u64,
                Ordering::Relaxed,
            );
        }
        SpillWritten::Surrendered(owner) => {
            segmenter.unseal();
            counters
                .spill_lost_ownership
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                recording_id = %spec.recording_id,
                journal = %held.describe(),
                new_owner = %owner,
                "another pod has adopted this recording's spill journal, so this pod stops \
                 writing it and will not delete it; the audio it still holds stays in memory"
            );
            *journal = None;
        }
        SpillWritten::Failed(error) => {
            segmenter.unseal();
            counters
                .segment_spill_failures
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                recording_id = %spec.recording_id,
                %error,
                "a closed recording segment could not be spilled; it stays in memory and dies \
                 with this pod"
            );
        }
    }
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
        refuse: std::sync::atomic::AtomicBool,
        stall: bool,
        slow_by: Duration,
    }

    impl MemorySink {
        fn accepting() -> MemorySink {
            MemorySink::slow(Duration::ZERO)
        }

        fn refusing() -> MemorySink {
            MemorySink::slow_refusing(Duration::ZERO)
        }

        fn slow(slow_by: Duration) -> MemorySink {
            MemorySink {
                puts: Mutex::new(Vec::new()),
                refuse: std::sync::atomic::AtomicBool::new(false),
                stall: false,
                slow_by,
            }
        }

        fn slow_refusing(slow_by: Duration) -> MemorySink {
            MemorySink {
                puts: Mutex::new(Vec::new()),
                refuse: std::sync::atomic::AtomicBool::new(true),
                stall: false,
                slow_by,
            }
        }

        fn accept_from_now_on(&self) {
            self.refuse.store(false, Ordering::Relaxed);
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
            assert!(
                matches!(
                    content_type,
                    "audio/wav" | "application/json" | "application/octet-stream"
                ),
                "{content_type} is not a content type this recorder writes"
            );
            if !self.slow_by.is_zero() {
                tokio::time::sleep(self.slow_by).await;
            }
            if self.stall {
                tokio::time::sleep(UPLOAD_TIMEOUT * 3).await;
            }
            if self.refuse.load(Ordering::Relaxed) {
                return Err(UploadError::Refused("the bucket said no".to_string()));
            }
            let mut held = self.puts.lock().unwrap();
            held.retain(|(stored, _)| stored != key);
            held.push((key.to_string(), body));
            Ok(format!("s3:{URI_SCHEME_SEPARATOR}lab-recordings/{key}"))
        }

        async fn exists(&self, key: &str) -> Result<bool, UploadError> {
            Ok(self
                .puts
                .lock()
                .unwrap()
                .iter()
                .any(|(held, _)| held == key))
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>, UploadError> {
            self.puts
                .lock()
                .unwrap()
                .iter()
                .find(|(held, _)| held == key)
                .map(|(_, body)| body.clone())
                .ok_or_else(|| UploadError::Missing(key.to_string()))
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, UploadError> {
            Ok(self
                .puts
                .lock()
                .unwrap()
                .iter()
                .filter(|(held, _)| held.starts_with(prefix))
                .map(|(held, _)| held.clone())
                .collect())
        }

        async fn delete(&self, key: &str) -> Result<(), UploadError> {
            self.puts.lock().unwrap().retain(|(held, _)| held != key);
            Ok(())
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
            journal: spill_dir.clone().map(disk_journal),
            spill_dir,
            spill_every: SPILL_EVERY,
            counters: Arc::new(RecorderCounters::default()),
            owner: "pod-a".to_string(),
            upload_permits: Arc::new(Semaphore::new(DEFAULT_UPLOAD_CONCURRENCY)),
        }
    }

    fn support_spilling_to_the_bucket(
        sink: Arc<MemorySink>,
        owner: &str,
        every: Duration,
    ) -> RecordingSupport {
        let journal: Arc<dyn SpillStore> = Arc::new(crate::recording_spill::ObjectSpill::new(
            Arc::clone(&sink) as Arc<dyn RecordingSink>,
            crate::recording_spill::DEFAULT_SPILL_PREFIX,
        ));
        RecordingSupport {
            sink: Some(sink as Arc<dyn RecordingSink>),
            journal: Some(journal),
            spill_dir: None,
            spill_every: every,
            counters: Arc::new(RecorderCounters::default()),
            owner: owner.to_string(),
            upload_permits: Arc::new(Semaphore::new(DEFAULT_UPLOAD_CONCURRENCY)),
        }
    }

    fn scratch(name: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "mss-spill-{}-{name}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("a scratch directory");
        directory
    }

    fn support_spilling(
        sink: Arc<dyn RecordingSink>,
        spill_dir: PathBuf,
        every: Duration,
    ) -> RecordingSupport {
        RecordingSupport {
            spill_every: every,
            ..support(sink, Some(spill_dir))
        }
    }

    async fn wait_for_segments(counters: &RecorderCounters, segments: u64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while counters.segments_spilled.load(Ordering::Relaxed) < segments {
            assert!(
                std::time::Instant::now() < deadline,
                "only {} segments reached disk",
                counters.segments_spilled.load(Ordering::Relaxed)
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    fn frames_of(body: &[u8]) -> usize {
        let reader = hound::WavReader::new(std::io::Cursor::new(body.to_vec())).expect("a wav");
        let channels = reader.spec().channels as usize;
        reader.len() as usize / channels.max(1)
    }

    fn samples_of(body: &[u8]) -> Vec<i16> {
        hound::WavReader::new(std::io::Cursor::new(body.to_vec()))
            .expect("a wav")
            .into_samples::<i16>()
            .map(|sample| sample.expect("a sample"))
            .collect()
    }

    #[test]
    fn a_closed_segment_leaves_memory_without_moving_the_recordings_clock() {
        let mut segmenter = segmenter();
        segmenter.accept(Track::Customer, 0, &tone(5));
        segmenter.accept(Track::Customer, 20, &tone(6));

        let closable = segmenter.closable_frames();
        assert_eq!(closable, 2 * FRAME);
        let closed = segmenter.render_closable(closable, Layout::Mono(Track::Customer));
        assert_eq!(closed.samples.len(), 2 * FRAME);
        assert_eq!(closed.samples[0], 5);
        assert_eq!(closed.samples[FRAME], 6);

        segmenter.close_segment(closable);
        assert_eq!(segmenter.frames(), 0, "closed audio must leave memory");
        segmenter.accept(Track::Customer, 40, &tone(7));

        let tail = segmenter.render(Layout::Mono(Track::Customer));
        assert_eq!(
            tail.samples.len(),
            FRAME,
            "the tail must not carry the closed segment's silence again"
        );
        assert_eq!(tail.samples[0], 7);
        assert_eq!(segmenter.total_frames(), 3 * FRAME);
        assert_eq!(segmenter.duration_ms(), 60);
        assert_eq!(segmenter.stats().segments_spilled, 1);
    }

    #[test]
    fn a_frame_that_lands_inside_a_sealed_prefix_is_kept_at_the_boundary() {
        let mut segmenter = segmenter();
        segmenter.accept(Track::Customer, 0, &tone(5));
        segmenter.accept(Track::Customer, 20, &tone(6));
        let sealed = segmenter.closable_frames();
        segmenter.seal(sealed);

        segmenter.accept(Track::Agent, 0, &tone(9));
        segmenter.accept(Track::Customer, 40, &tone(7));
        assert_eq!(
            segmenter.stats().frames_out_of_order,
            1,
            "a frame for audio already handed to the store is late, and is counted as such"
        );
        let sealed_prefix = segmenter.render_closable(sealed, Layout::Stereo);
        assert!(
            sealed_prefix
                .samples
                .iter()
                .skip(1)
                .step_by(2)
                .all(|s| *s == 0),
            "the prefix a write is carrying must not change under it"
        );

        segmenter.close_segment(sealed);
        let tail = segmenter.render(Layout::Stereo);
        assert_eq!(tail.samples.len(), 2 * FRAME, "one stereo frame of tail");
        assert_eq!(
            tail.samples[0], 7,
            "the customer frame lands where its timestamp says"
        );
        assert_eq!(
            tail.samples[1], 9,
            "the late agent frame opens the next segment instead of vanishing"
        );

        let mut unsealed = Segmenter::new(RATE, MAX_RECORDING);
        unsealed.accept(Track::Customer, 0, &tone(5));
        unsealed.accept(Track::Customer, 20, &tone(6));
        unsealed.seal(unsealed.closable_frames());
        unsealed.unseal();
        unsealed.accept(Track::Agent, 0, &tone(9));
        assert_eq!(unsealed.stats().frames_out_of_order, 0);
        assert_eq!(
            unsealed.render(Layout::Stereo).samples[1],
            9,
            "once a write has failed the prefix is ordinary memory again"
        );
    }

    #[test]
    fn a_recording_that_spills_still_stops_at_its_cap() {
        let mut segmenter = Segmenter::new(RATE, Duration::from_secs(1));
        for index in 0..25u64 {
            segmenter.accept(Track::Customer, index * 20, &tone(1));
        }
        segmenter.close_segment(segmenter.closable_frames());
        for index in 25..51u64 {
            segmenter.accept(Track::Customer, index * 20, &tone(2));
        }

        assert_eq!(
            segmenter.stats().frames_beyond_cap,
            1,
            "spilling a segment must not hand the recording a fresh length budget"
        );
        assert_eq!(segmenter.total_frames(), RATE as usize);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_spilled_segment_and_the_tail_in_memory_upload_as_one_recording() {
        let directory = scratch("stitch");
        let (mut hub, client) = Hub::new();
        let subscription = client.attach(64, TrackSelection::All).unwrap();
        hub.poll_commands();
        let sink = Arc::new(MemorySink::accepting());
        let support = support_spilling(
            Arc::clone(&sink) as Arc<dyn RecordingSink>,
            directory.clone(),
            Duration::from_millis(10),
        );
        let counters = Arc::clone(&support.counters);
        let handle = spawn(spec(), subscription, support, None);

        hub.publish(TapEvent::media(Track::Customer, 0, &tone(5)));
        wait_for_segments(&counters, 1).await;
        hub.publish(TapEvent::media(Track::Customer, 20, &tone(6)));
        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
        drop(hub);

        assert_eq!(outcome.frames, 2 * FRAME, "{outcome:?}");
        assert_eq!(outcome.duration_ms, 40);
        let (key, body) = sink.last().expect("nothing was uploaded");
        assert_eq!(key, "acct-42/rec-99.wav");
        assert_eq!(frames_of(&body), 2 * FRAME);
        let samples = samples_of(&body);
        assert_eq!(samples[0], 5, "the spilled segment must open the object");
        assert_eq!(
            samples[2 * FRAME],
            6,
            "the tail held in memory must follow it without a gap"
        );
        assert!(
            !directory
                .join(crate::recording_spill::JOURNAL_DIR)
                .join(&key)
                .exists(),
            "an uploaded recording must not leave its segments on disk"
        );
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[tokio::test]
    async fn a_recording_left_behind_by_a_dead_pod_is_uploaded_on_the_next_start() {
        let directory = scratch("salvage");
        let sink = Arc::new(MemorySink::accepting());
        let support = support_spilling(
            Arc::clone(&sink) as Arc<dyn RecordingSink>,
            directory.clone(),
            SPILL_EVERY,
        );
        let key = identity().object_key();
        let mut journal = crate::recording_spill::SegmentJournal::open(
            &support,
            "rec-99",
            "pod-a",
            RATE,
            &[(key.clone(), 2)],
        )
        .await
        .expect("a journal needs a spill directory");
        journal
            .append(
                vec![RecordedAudio {
                    channels: 2,
                    samples: vec![9; 2 * FRAME],
                }],
                FRAME as u64,
            )
            .await
            .unwrap();
        drop(journal);

        let summary = crate::recording_spill::salvage(&support).await;

        assert_eq!(summary.uploaded, 1, "{summary:?}");
        assert_eq!(support.counters.salvaged.load(Ordering::Relaxed), 1);
        let (uploaded, body) = sink.last().expect("nothing was salvaged");
        assert_eq!(uploaded, key);
        assert_eq!(frames_of(&body), FRAME);
        assert_eq!(samples_of(&body)[0], 9);
        assert!(
            !directory
                .join(crate::recording_spill::JOURNAL_DIR)
                .join(&key)
                .exists(),
            "a salvaged journal must be cleaned up"
        );
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[tokio::test]
    async fn salvage_never_overwrites_a_recording_that_already_reached_storage() {
        let directory = scratch("no-clobber");
        let sink = Arc::new(MemorySink::accepting());
        let support = support_spilling(
            Arc::clone(&sink) as Arc<dyn RecordingSink>,
            directory.clone(),
            SPILL_EVERY,
        );
        let key = identity().object_key();
        let mut journal = crate::recording_spill::SegmentJournal::open(
            &support,
            "rec-99",
            "pod-a",
            RATE,
            &[(key.clone(), 2)],
        )
        .await
        .expect("a journal needs a spill directory");
        journal
            .append(
                vec![RecordedAudio {
                    channels: 2,
                    samples: vec![9; 2 * FRAME],
                }],
                FRAME as u64,
            )
            .await
            .unwrap();
        drop(journal);
        sink.put(&key, "audio/wav", vec![1, 2, 3]).await.unwrap();

        let summary = crate::recording_spill::salvage(&support).await;

        assert_eq!(
            summary,
            crate::recording_spill::SalvageSummary {
                uploaded: 0,
                already_present: 1,
                failed: 0,
                foreign: 0,
            }
        );
        assert_eq!(
            sink.body(&key),
            vec![1, 2, 3],
            "a pod coming back must not replace the object another pod finished"
        );
        assert!(
            directory
                .join(crate::recording_spill::JOURNAL_DIR)
                .join(&key)
                .exists(),
            "the segments it could not use must stay for an operator to judge"
        );
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_adopted_recording_pads_what_its_dead_pod_never_spilled_and_counts_it() {
        let directory = scratch("adopt-nothing");
        let (mut hub, client) = Hub::new();
        let subscription = client.attach(64, TrackSelection::All).unwrap();
        hub.poll_commands();
        let sink = Arc::new(MemorySink::accepting());
        let support = support_spilling(
            Arc::clone(&sink) as Arc<dyn RecordingSink>,
            directory.clone(),
            SPILL_EVERY,
        );
        let counters = Arc::clone(&support.counters);
        let handle = spawn(
            RecorderSpec {
                resume_ms: 1_000,
                ..spec()
            },
            subscription,
            support,
            None,
        );

        hub.publish(TapEvent::media(Track::Customer, 0, &tone(5)));
        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
        drop(hub);

        assert_eq!(
            counters.frames_lost_on_adopt.load(Ordering::Relaxed),
            8_000,
            "a second of audio the dead pod never spilled must be counted as lost"
        );
        assert_eq!(outcome.stats.frames_lost_on_adopt, 8_000);
        let (_, body) = sink.last().expect("nothing was uploaded");
        assert_eq!(frames_of(&body), 8_000 + FRAME);
        let samples = samples_of(&body);
        assert_eq!(samples[0], 0, "the lost second must read as silence");
        assert_eq!(
            samples[2 * 8_000],
            5,
            "audio after the adoption must sit at its own wall-clock offset"
        );
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_adopted_recording_that_finds_its_own_spill_keeps_that_audio() {
        let directory = scratch("adopt-spill");
        let sink = Arc::new(MemorySink::accepting());
        let support = support_spilling(
            Arc::clone(&sink) as Arc<dyn RecordingSink>,
            directory.clone(),
            SPILL_EVERY,
        );
        let key = identity().object_key();
        let mut journal = crate::recording_spill::SegmentJournal::open(
            &support,
            "rec-99",
            "pod-a",
            RATE,
            &[(key.clone(), 2)],
        )
        .await
        .expect("a journal needs a spill directory");
        journal
            .append(
                vec![RecordedAudio {
                    channels: 2,
                    samples: vec![9; 2 * FRAME],
                }],
                FRAME as u64,
            )
            .await
            .unwrap();
        drop(journal);

        let (mut hub, client) = Hub::new();
        let subscription = client.attach(64, TrackSelection::All).unwrap();
        hub.poll_commands();
        let counters = Arc::clone(&support.counters);
        let handle = spawn(
            RecorderSpec {
                resume_ms: 60,
                ..spec()
            },
            subscription,
            support.clone(),
            None,
        );

        hub.publish(TapEvent::media(Track::Customer, 0, &tone(5)));
        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
        drop(hub);

        assert_eq!(
            counters.frames_lost_on_adopt.load(Ordering::Relaxed),
            2 * FRAME as u64,
            "only the 40 ms this pod could not read may count as lost"
        );
        assert_eq!(outcome.duration_ms, 80);
        let (_, body) = sink.last().expect("nothing was uploaded");
        assert_eq!(frames_of(&body), 4 * FRAME);
        let samples = samples_of(&body);
        assert_eq!(samples[0], 9, "the spilled segment must open the object");
        assert_eq!(samples[2 * FRAME], 0, "the lost 40 ms must read as silence");
        assert_eq!(samples[2 * 3 * FRAME], 5);
        let _ = std::fs::remove_dir_all(&directory);
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

    fn mixed_spec() -> RecorderSpec {
        RecorderSpec::one_object(
            SessionId::from_raw(1),
            &identity(),
            Layout::Mono(Track::Mixed),
            RATE,
        )
    }

    fn channels_of(body: &[u8]) -> u16 {
        hound::WavReader::new(std::io::Cursor::new(body.to_vec()))
            .expect("a wav")
            .spec()
            .channels
    }

    #[test]
    fn a_recording_names_its_shape_so_a_conference_object_is_never_mistaken_for_a_call() {
        assert_eq!(
            RecordingShape::of(Layout::Stereo, false).named(false),
            "stereo"
        );
        assert_eq!(
            RecordingShape::of(Layout::Mono(Track::Customer), false).named(false),
            "track"
        );
        assert_eq!(
            RecordingShape::of(Layout::Mono(Track::Mixed), false).named(false),
            "mixed"
        );
        assert_eq!(
            RecordingShape::of(Layout::Mono(Track::Mixed), false).named(true),
            "conference-mixed"
        );
        assert_eq!(
            RecordingShape::of(Layout::Mono(Track::Customer), true).named(true),
            "conference-participant"
        );
        assert_eq!(
            RecordingShape::of(Layout::Stereo, true).named(false),
            "participant"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_mixed_only_recording_is_one_mono_object_of_the_whole_room() {
        let (mut hub, client) = Hub::new();
        let subscription = client
            .attach(64, TrackSelection::Only(Track::Mixed))
            .unwrap();
        hub.poll_commands();
        let sink = Arc::new(MemorySink::accepting());
        let handle = spawn(
            mixed_spec(),
            subscription,
            support(Arc::clone(&sink) as Arc<dyn RecordingSink>, None),
            None,
        );

        for at in 0..5u64 {
            hub.publish(TapEvent::media(Track::Customer, at * 20, &tone(111)));
            hub.publish(TapEvent::media(Track::Agent, at * 20, &tone(-111)));
            hub.publish(TapEvent::media(Track::Mixed, at * 20, &tone(700)));
        }

        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
        drop(hub);

        assert_eq!(outcome.duration_ms, 100);
        let (key, body) = sink.last().expect("nothing was uploaded");
        assert_eq!(
            key, "acct-42/rec-99.wav",
            "the room object keeps the frozen identity"
        );
        assert_eq!(
            channels_of(&body),
            1,
            "a conference records as one mono mix"
        );
        assert_eq!(frames_of(&body), 5 * FRAME);
        assert!(
            samples_of(&body).iter().all(|sample| *sample == 700),
            "only the mixed track may reach a mixed-only object"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pausing_a_mixed_only_recording_cuts_the_room_out_of_the_object() {
        let (mut hub, client) = Hub::new();
        let subscription = client
            .attach(256, TrackSelection::Only(Track::Mixed))
            .unwrap();
        hub.poll_commands();
        let sink = Arc::new(MemorySink::accepting());
        let seen = Arc::new(Collected::default());
        let strong: Arc<dyn ObservationSink> = seen.clone();
        let observer = Arc::downgrade(&strong);
        let handle = spawn(
            mixed_spec(),
            subscription,
            support(Arc::clone(&sink) as Arc<dyn RecordingSink>, None),
            Some(observer),
        );

        for at in 0..5u64 {
            hub.publish(TapEvent::media(Track::Mixed, at * 20, &tone(700)));
        }
        assert!(handle.set_paused(true));
        wait_for(&seen, 1).await;
        for at in 5..25u64 {
            hub.publish(TapEvent::media(Track::Mixed, at * 20, &tone(9)));
        }
        assert!(handle.set_paused(false));
        wait_for(&seen, 2).await;
        for at in 25..30u64 {
            hub.publish(TapEvent::media(Track::Mixed, at * 20, &tone(-700)));
        }

        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
        drop(hub);

        assert_eq!(
            outcome.duration_ms, 200,
            "a paused conference recording accumulates only what it kept"
        );
        assert_eq!(outcome.stats.frames_while_paused, 20);
        let (_, body) = sink.last().expect("nothing was uploaded");
        assert_eq!(channels_of(&body), 1);
        assert_eq!(frames_of(&body), 10 * FRAME);
        let samples = samples_of(&body);
        assert!(
            samples.iter().all(|sample| *sample != 9),
            "the room the recording was deaf to must not be in the object"
        );
        assert_eq!(samples[0], 700);
        assert_eq!(
            samples[5 * FRAME],
            -700,
            "the audio after resume follows the audio before pause with no gap"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_mixed_only_recording_spills_mono_segments_and_stitches_them_back() {
        let directory = scratch("room");
        let (mut hub, client) = Hub::new();
        let subscription = client
            .attach(64, TrackSelection::Only(Track::Mixed))
            .unwrap();
        hub.poll_commands();
        let sink = Arc::new(MemorySink::accepting());
        let support = support_spilling(
            Arc::clone(&sink) as Arc<dyn RecordingSink>,
            directory.clone(),
            Duration::from_millis(10),
        );
        let counters = Arc::clone(&support.counters);
        let handle = spawn(mixed_spec(), subscription, support, None);

        hub.publish(TapEvent::media(Track::Mixed, 0, &tone(5)));
        wait_for_segments(&counters, 1).await;
        hub.publish(TapEvent::media(Track::Mixed, 20, &tone(6)));

        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
        drop(hub);

        assert_eq!(outcome.frames, 2 * FRAME, "{outcome:?}");
        let (key, body) = sink.last().expect("nothing was uploaded");
        assert_eq!(key, "acct-42/rec-99.wav");
        assert_eq!(channels_of(&body), 1, "a spilled room segment stays mono");
        assert_eq!(frames_of(&body), 2 * FRAME);
        let samples = samples_of(&body);
        assert_eq!(samples[0], 5, "the spilled segment opens the room object");
        assert_eq!(samples[FRAME], 6, "the tail in memory follows it");
        let _ = std::fs::remove_dir_all(&directory);
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
            resume_ms: 0,
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

        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
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

        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
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
        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
        drop(hub);

        assert!(outcome.uris.is_empty());
        assert_eq!(
            seen.seen(),
            vec![
                Observation::RecordingStopped {
                    recording_id: "rec-99".to_string(),
                    duration_ms: 20,
                },
                Observation::UploadFailed {
                    recording_id: "rec-99".to_string(),
                    key: "acct-42/rec-99.wav".to_string(),
                    error: "the object store refused the upload: the bucket said no".to_string(),
                }
            ],
            "a recording that never reached storage says so, so an integrator does not wait \
             forever for an upload event that is never coming"
        );
        assert_eq!(counters.upload_failures.load(Ordering::Relaxed), 1);
        assert_eq!(counters.spilled.load(Ordering::Relaxed), 1);
        let spilled = directory.join("acct-42/rec-99.wav");
        assert!(spilled.is_file(), "{} is missing", spilled.display());
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stop_is_reported_before_the_upload_starts_and_the_upload_runs_on_alone() {
        let (mut hub, client) = Hub::new();
        let subscription = client.attach(64, TrackSelection::All).unwrap();
        hub.poll_commands();
        let sink = Arc::new(MemorySink::slow(Duration::from_millis(1_500)));
        let seen = Arc::new(Collected::default());
        let strong: Arc<dyn ObservationSink> = seen.clone();
        let observer = Arc::downgrade(&strong);
        let support = support(Arc::clone(&sink) as Arc<dyn RecordingSink>, None);
        let counters = Arc::clone(&support.counters);
        let handle = spawn(spec(), subscription, support, Some(observer));

        for at in 0..5u64 {
            hub.publish(TapEvent::media(Track::Customer, at * 20, &tone(111)));
        }

        let asked = Instant::now();
        let finished = handle.finish().await;
        let answered = asked.elapsed();

        assert!(
            answered < Duration::from_millis(100),
            "closing the segment took {answered:?}, so a detach would have waited for the upload"
        );
        let stopped = finished.stopped.clone().expect("a stop report");
        assert_eq!(stopped.duration_ms, 100);
        assert_eq!(stopped.frames, 5 * FRAME);
        assert_eq!(
            seen.seen(),
            vec![Observation::RecordingStopped {
                recording_id: "rec-99".to_string(),
                duration_ms: 100,
            }],
            "the stop is published before the caller is released, the upload is not"
        );
        assert_eq!(counters.uploaded.load(Ordering::Relaxed), 0);

        let outcome = finished
            .settle()
            .await
            .expect("the recorder had no outcome");
        drop(hub);

        assert!(
            asked.elapsed() >= Duration::from_millis(1_500),
            "the slow upload did not actually take its time"
        );
        assert_eq!(outcome.uris.len(), 1);
        assert_eq!(
            seen.seen().last(),
            Some(&Observation::UploadCompleted {
                recording_id: "rec-99".to_string(),
                uri: format!("s3:{URI_SCHEME_SEPARATOR}lab-recordings/acct-42/rec-99.wav"),
            }),
            "the upload reports itself once it lands, long after the stop"
        );
        assert_eq!(counters.uploaded.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn background_uploads_run_no_wider_than_their_configured_concurrency() {
        const SLOW_BY: Duration = Duration::from_millis(400);
        let sink = Arc::new(MemorySink::slow(SLOW_BY));
        let mut support = support(Arc::clone(&sink) as Arc<dyn RecordingSink>, None);
        support.upload_permits = Arc::new(Semaphore::new(1));

        let mut hubs = Vec::new();
        let mut finishing = Vec::new();
        for _ in 0..2 {
            let (mut hub, client) = Hub::new();
            let subscription = client.attach(64, TrackSelection::All).unwrap();
            hub.poll_commands();
            let handle = spawn(spec(), subscription, support.clone(), None);
            hub.publish(TapEvent::media(Track::Customer, 0, &tone(9)));
            hubs.push(hub);
            finishing.push(handle);
        }

        let asked = Instant::now();
        let mut settling = Vec::new();
        for handle in finishing {
            settling.push(handle.finish().await);
        }
        assert!(
            asked.elapsed() < Duration::from_millis(100),
            "both recorders must be released before either upload starts"
        );
        let uploads = settling.into_iter().map(FinishedCapture::settle);
        for outcome in futures_util::future::join_all(uploads).await {
            assert_eq!(outcome.expect("a recorder had no outcome").uris.len(), 1);
        }
        drop(hubs);

        assert!(
            asked.elapsed() >= SLOW_BY * 2,
            "one permit means one upload at a time; these two overlapped in {:?}",
            asked.elapsed()
        );
        assert_eq!(support.counters.uploaded.load(Ordering::Relaxed), 2);
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
        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
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
    fn a_padded_member_keeps_its_whole_tail_across_a_spill() {
        let mut early = segmenter();
        let mut late = segmenter();
        assert!(late.lead_with_silence(Duration::from_millis(1000)));
        for at in 0..50u64 {
            early.accept(Track::Customer, at * 20, &tone(100));
            late.accept(Track::Customer, at * 20, &tone(-100));
        }
        for cut in [&mut early, &mut late] {
            let closable = cut.closable_frames();
            cut.render_closable(closable, Layout::Mono(Track::Customer));
            cut.close_segment(closable);
        }
        for at in 50..100u64 {
            early.accept(Track::Customer, at * 20, &tone(100));
            late.accept(Track::Customer, at * 20, &tone(-100));
        }

        assert_eq!(early.total_frames(), RATE as usize * 2);
        assert_eq!(
            late.total_frames(),
            early.total_frames() + RATE as usize,
            "closing a segment must not advance the anchor past the lead silence, \
             or the padded member loses its lead's worth of audio off the tail"
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
        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
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

    fn mono_spec(resume_ms: u64) -> RecorderSpec {
        RecorderSpec {
            targets: vec![RecordingTarget {
                key: identity().object_key(),
                layout: Layout::Mono(Track::Customer),
            }],
            resume_ms,
            ..spec()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_adopter_on_any_pod_loses_at_most_one_spill_interval_when_the_journal_is_in_the_bucket(
    ) {
        let sink = Arc::new(MemorySink::accepting());
        let every = Duration::from_millis(20);
        let first = support_spilling_to_the_bucket(Arc::clone(&sink), "pod-a", every);
        let dying = Arc::clone(&first.counters);
        let (mut dying_hub, client) = Hub::new();
        let subscription = client.attach(64, TrackSelection::All).unwrap();
        dying_hub.poll_commands();
        let handle = spawn(mono_spec(0), subscription, first, None);
        let progress = handle.progress();

        for at in 0..3u64 {
            dying_hub.publish(TapEvent::media(Track::Customer, at * 20, &tone(11)));
        }
        wait_for_segments(&dying, 1).await;
        for at in 3..5u64 {
            dying_hub.publish(TapEvent::media(Track::Customer, at * 20, &tone(11)));
        }
        wait_for_segments(&dying, 2).await;
        handle.task.abort();
        drop(dying_hub);
        let resume_ms = progress.recorded_ms();
        assert!(
            resume_ms >= 100,
            "pod A recorded {resume_ms} ms before it died"
        );

        let second = support_spilling_to_the_bucket(Arc::clone(&sink), "pod-b", every);
        let adopting = Arc::clone(&second.counters);
        let (mut live, client) = Hub::new();
        let subscription = client.attach(64, TrackSelection::All).unwrap();
        live.poll_commands();
        let handle = spawn(mono_spec(resume_ms), subscription, second, None);
        live.publish(TapEvent::media(Track::Customer, 0, &tone(22)));
        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the adopting pod had no outcome");
        drop(live);

        let resume_frames = (resume_ms * RATE as u64 / 1000) as usize;
        let lost = adopting.frames_lost_on_adopt.load(Ordering::Relaxed) as usize;
        let interval_frames = every.as_millis() as usize * RATE as usize / 1000;
        assert!(
            lost <= interval_frames.max(FRAME),
            "the adopting pod lost {lost} frames, more than one spill interval"
        );
        assert_eq!(outcome.frames, resume_frames + FRAME, "{outcome:?}");
        let body = sink.body(&identity().object_key());
        let samples = samples_of(&body);
        assert_eq!(samples.len(), resume_frames + FRAME);
        let recovered = resume_frames - lost;
        assert!(
            recovered >= 5 * FRAME,
            "only {recovered} frames were read back"
        );
        assert!(
            samples[..recovered].iter().all(|sample| *sample == 11),
            "the dead pod's spilled audio is not at the front of the object"
        );
        assert!(
            samples[recovered..resume_frames]
                .iter()
                .all(|sample| *sample == 0),
            "what neither pod held must be silence, not another pod's audio"
        );
        assert!(
            samples[resume_frames..].iter().all(|sample| *sample == 22),
            "the adopting pod's own audio must follow the recovered prefix"
        );
        assert!(
            sink.keys()
                .iter()
                .all(|key| !key.starts_with(crate::recording_spill::DEFAULT_SPILL_PREFIX)),
            "an uploaded recording must leave nothing in the reserved spill namespace: {:?}",
            sink.keys()
        );
    }

    #[tokio::test]
    async fn a_pod_that_lost_its_journal_to_an_adopter_stops_spilling_into_it() {
        let sink = Arc::new(MemorySink::accepting());
        let first = support_spilling_to_the_bucket(Arc::clone(&sink), "pod-a", SPILL_EVERY);
        let second = support_spilling_to_the_bucket(Arc::clone(&sink), "pod-b", SPILL_EVERY);
        let key = identity().object_key();
        let mut dying = crate::recording_spill::SegmentJournal::open(
            &first,
            "rec-99",
            &first.owner,
            RATE,
            &[(key.clone(), 1)],
        )
        .await
        .expect("a journal needs a spill store");
        dying
            .append(
                vec![RecordedAudio {
                    channels: 1,
                    samples: vec![11; FRAME],
                }],
                FRAME as u64,
            )
            .await
            .expect("the first pod could not spill");

        let mut adopter = crate::recording_spill::SegmentJournal::open(
            &second,
            "rec-99",
            &second.owner,
            RATE,
            &[(key.clone(), 1)],
        )
        .await
        .expect("the adopter found no journal");
        assert_eq!(
            adopter.frames_on_disk(),
            FRAME as u64,
            "the adopter must read the dead pod's frame count out of the manifest"
        );
        adopter
            .append(
                vec![RecordedAudio {
                    channels: 1,
                    samples: vec![22; FRAME],
                }],
                FRAME as u64,
            )
            .await
            .expect("the adopter could not spill");

        let refused = dying
            .append(
                vec![RecordedAudio {
                    channels: 1,
                    samples: vec![33; FRAME],
                }],
                FRAME as u64,
            )
            .await;
        assert!(
            refused.is_err(),
            "a partitioned pod overwrote an adopted journal"
        );
        assert!(
            dying.surrendered(),
            "the refusal must be reported as lost ownership, not as a spill failure"
        );

        let held = adopter.read_back(0).await;
        assert_eq!(held.len(), 2 * FRAME);
        assert_eq!(held[0], 11);
        assert_eq!(held[FRAME], 22);
        assert!(
            !held.contains(&33),
            "the partitioned pod's audio reached an adopted journal"
        );
    }

    #[tokio::test]
    async fn salvage_leaves_another_pods_journal_in_the_bucket_alone() {
        let sink = Arc::new(MemorySink::accepting());
        let foreign = support_spilling_to_the_bucket(Arc::clone(&sink), "pod-z", SPILL_EVERY);
        let key = identity().object_key();
        let mut journal = crate::recording_spill::SegmentJournal::open(
            &foreign,
            "rec-99",
            &foreign.owner,
            RATE,
            &[(key.clone(), 1)],
        )
        .await
        .expect("a journal needs a spill store");
        journal
            .append(
                vec![RecordedAudio {
                    channels: 1,
                    samples: vec![11; FRAME],
                }],
                FRAME as u64,
            )
            .await
            .unwrap();
        drop(journal);

        let ours = support_spilling_to_the_bucket(Arc::clone(&sink), "pod-a", SPILL_EVERY);
        let summary = crate::recording_spill::salvage(&ours).await;

        assert_eq!(summary.foreign, 1, "{summary:?}");
        assert_eq!(summary.uploaded, 0, "{summary:?}");
        assert_eq!(
            ours.counters
                .spill_foreign_manifests
                .load(Ordering::Relaxed),
            1
        );
        assert!(
            !sink.keys().contains(&key),
            "salvage uploaded a recording another pod is still writing"
        );
        assert!(
            sink.keys()
                .iter()
                .any(|held| held.starts_with(crate::recording_spill::DEFAULT_SPILL_PREFIX)),
            "a foreign journal must be left where its owner can still finish it"
        );
    }

    const PTIME: Duration = Duration::from_millis(20);

    async fn publish_at_ptime(hub: &mut Hub, from: u64, frames: u64, value: i16) {
        for at in from..from + frames {
            hub.publish(TapEvent::media(Track::Customer, at * 20, &tone(value)));
            tokio::time::sleep(PTIME).await;
        }
    }

    async fn wait_for_spill_failures(counters: &RecorderCounters, failures: u64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while counters.segment_spill_failures.load(Ordering::Relaxed) < failures {
            assert!(
                std::time::Instant::now() < deadline,
                "only {} spill writes failed",
                counters.segment_spill_failures.load(Ordering::Relaxed)
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_spill_store_never_costs_the_recording_a_frame() {
        const FRAMES_SENT: u64 = 500;
        let slow_by = Duration::from_secs(3);
        let sink = Arc::new(MemorySink::slow(slow_by));
        let support =
            support_spilling_to_the_bucket(Arc::clone(&sink), "pod-a", Duration::from_millis(100));
        let counters = Arc::clone(&support.counters);
        let (mut hub, client) = Hub::new();
        let subscription = client
            .attach(crate::hub::CONSUMER_QUEUE_FRAMES, TrackSelection::All)
            .unwrap();
        let queue = subscription.metrics();
        hub.poll_commands();
        let handle = spawn(mono_spec(0), subscription, support, None);

        publish_at_ptime(&mut hub, 0, FRAMES_SENT, 33).await;

        assert!(
            counters.segments_spilled.load(Ordering::Relaxed) >= 1,
            "a {slow_by:?} spill write must have landed while the frames kept coming"
        );
        assert_eq!(
            queue.dropped_oldest(),
            0,
            "the recorder stopped draining its subscription while a spill write was pending"
        );

        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
        drop(hub);

        assert_eq!(queue.dropped_oldest(), 0);
        assert_eq!(outcome.frames, FRAMES_SENT as usize * FRAME, "{outcome:?}");
        assert_eq!(outcome.stats.frames_written, FRAMES_SENT);
        let body = sink.body(&identity().object_key());
        assert_eq!(frames_of(&body), FRAMES_SENT as usize * FRAME);
        assert!(
            samples_of(&body).iter().all(|sample| *sample == 33),
            "every frame sent must be in the object, in order and unbroken"
        );
        assert!(
            sink.keys()
                .iter()
                .all(|key| !key.starts_with(crate::recording_spill::DEFAULT_SPILL_PREFIX)),
            "an uploaded recording must leave nothing in the reserved spill namespace: {:?}",
            sink.keys()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_spill_write_that_fails_leaves_its_frames_in_memory_for_the_next_tick() {
        let sink = Arc::new(MemorySink::slow_refusing(Duration::from_millis(500)));
        let support =
            support_spilling_to_the_bucket(Arc::clone(&sink), "pod-a", Duration::from_millis(100));
        let counters = Arc::clone(&support.counters);
        let (mut hub, client) = Hub::new();
        let subscription = client
            .attach(crate::hub::CONSUMER_QUEUE_FRAMES, TrackSelection::All)
            .unwrap();
        let queue = subscription.metrics();
        hub.poll_commands();
        let handle = spawn(mono_spec(0), subscription, support, None);
        let progress = handle.progress();

        publish_at_ptime(&mut hub, 0, 30, 44).await;
        wait_for_spill_failures(&counters, 1).await;
        assert_eq!(counters.segments_spilled.load(Ordering::Relaxed), 0);
        assert_eq!(
            progress.spilled_ms(),
            0,
            "a refused write must not be counted as spilled"
        );
        assert_eq!(
            progress.recorded_ms(),
            600,
            "the refused frames are still recorded"
        );

        sink.accept_from_now_on();
        publish_at_ptime(&mut hub, 30, 30, 44).await;
        wait_for_segments(&counters, 1).await;
        assert!(
            progress.spilled_ms() >= 600,
            "the retry must carry the frames the refused write held and everything since; \
             it carried {} ms",
            progress.spilled_ms()
        );
        publish_at_ptime(&mut hub, 60, 5, 44).await;

        let outcome = handle
            .finish()
            .await
            .settle()
            .await
            .expect("the recorder had no outcome");
        drop(hub);

        assert_eq!(queue.dropped_oldest(), 0);
        assert_eq!(counters.segment_spill_failures.load(Ordering::Relaxed), 1);
        assert_eq!(outcome.frames, 65 * FRAME, "{outcome:?}");
        let body = sink.body(&identity().object_key());
        assert_eq!(frames_of(&body), 65 * FRAME);
        assert!(
            samples_of(&body).iter().all(|sample| *sample == 44),
            "the frames a failed write held must reach the object through the retry"
        );
        assert!(
            sink.keys()
                .iter()
                .all(|key| !key.starts_with(crate::recording_spill::DEFAULT_SPILL_PREFIX)),
            "an uploaded recording must leave nothing in the reserved spill namespace: {:?}",
            sink.keys()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn finishing_while_a_spill_write_is_failing_still_uploads_every_frame() {
        let journal_sink = Arc::new(MemorySink::slow_refusing(Duration::from_secs(1)));
        let sink = Arc::new(MemorySink::accepting());
        let journal: Arc<dyn SpillStore> = Arc::new(crate::recording_spill::ObjectSpill::new(
            Arc::clone(&journal_sink) as Arc<dyn RecordingSink>,
            crate::recording_spill::DEFAULT_SPILL_PREFIX,
        ));
        let support = RecordingSupport {
            sink: Some(Arc::clone(&sink) as Arc<dyn RecordingSink>),
            journal: Some(journal),
            spill_dir: None,
            spill_every: Duration::from_millis(100),
            counters: Arc::new(RecorderCounters::default()),
            owner: "pod-a".to_string(),
            upload_permits: Arc::new(Semaphore::new(DEFAULT_UPLOAD_CONCURRENCY)),
        };
        let counters = Arc::clone(&support.counters);
        let (mut hub, client) = Hub::new();
        let subscription = client
            .attach(crate::hub::CONSUMER_QUEUE_FRAMES, TrackSelection::All)
            .unwrap();
        let queue = subscription.metrics();
        hub.poll_commands();
        let handle = spawn(mono_spec(0), subscription, support, None);

        publish_at_ptime(&mut hub, 0, 20, 55).await;
        assert_eq!(
            counters.segment_spill_failures.load(Ordering::Relaxed),
            0,
            "the doomed write must still be in flight when the recording stops"
        );
        assert_eq!(counters.segments_spilled.load(Ordering::Relaxed), 0);

        let asked = tokio::time::Instant::now();
        let finished = handle.finish().await;
        assert!(
            asked.elapsed() < Duration::from_millis(700),
            "the stop report waited {:?} on a pending spill write",
            asked.elapsed()
        );
        let stopped = finished.stopped.clone().expect("a stop report");
        assert_eq!(stopped.frames, 20 * FRAME);

        let outcome = finished
            .settle()
            .await
            .expect("the recorder had no outcome");
        drop(hub);

        assert_eq!(queue.dropped_oldest(), 0);
        assert_eq!(counters.segment_spill_failures.load(Ordering::Relaxed), 1);
        assert_eq!(counters.segments_spilled.load(Ordering::Relaxed), 0);
        assert_eq!(outcome.stats.segments_spilled, 0);
        assert_eq!(outcome.frames, 20 * FRAME, "{outcome:?}");
        let body = sink.body(&identity().object_key());
        assert_eq!(frames_of(&body), 20 * FRAME);
        assert!(
            samples_of(&body).iter().all(|sample| *sample == 55),
            "the frames the failed write held must still be in the object"
        );
        assert!(
            journal_sink.keys().is_empty(),
            "nothing landed in the spill store: {:?}",
            journal_sink.keys()
        );
    }

    #[test]
    fn the_reserved_spill_namespace_always_ends_in_one_separator() {
        assert_eq!(
            crate::recording_spill::normalized_prefix("_spill"),
            "_spill/"
        );
        assert_eq!(
            crate::recording_spill::normalized_prefix("/_spill/"),
            "_spill/"
        );
        assert_eq!(
            crate::recording_spill::normalized_prefix(" journals/held/ "),
            "journals/held/"
        );
        assert!(crate::recording_spill::normalized_prefix("  ").is_empty());
        assert_eq!(
            crate::recording_spill::DEFAULT_SPILL_PREFIX,
            "_spill/",
            "the default reserved namespace is documented in deploy.md and must not drift"
        );
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
