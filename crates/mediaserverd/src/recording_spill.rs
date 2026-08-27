use crate::recorder::{wav_bytes, RecordedAudio, RecordingSink, RecordingSupport, UploadError};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

pub const JOURNAL_DIR: &str = "journal";
pub const DEFAULT_SPILL_PREFIX: &str = "_spill/";
pub const SPILL_TIMEOUT: Duration = Duration::from_secs(10);
const MANIFEST_FILE: &str = "manifest.json";
const MANIFEST_TEMP: &str = "manifest.json.writing";
const CHUNK_EXTENSION: &str = "pcm";
const MANIFEST_CONTENT_TYPE: &str = "application/json";
const CHUNK_CONTENT_TYPE: &str = "application/octet-stream";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpilledTarget {
    pub key: String,
    pub channels: u16,
    pub chunks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpillManifest {
    pub recording_id: String,
    pub sample_rate_hz: u32,
    pub owner: String,
    pub frames: u64,
    pub targets: Vec<SpilledTarget>,
}

impl SpillManifest {
    fn continues(&self, recording_id: &str, sample_rate_hz: u32, keys: &[String]) -> bool {
        self.recording_id == recording_id
            && self.sample_rate_hz == sample_rate_hz
            && self.targets.len() == keys.len()
            && self
                .targets
                .iter()
                .zip(keys)
                .all(|(target, key)| &target.key == key)
    }
}

#[control_api::async_trait]
pub trait SpillStore: Send + Sync + 'static {
    async fn write(
        &self,
        journal: &str,
        manifest: Vec<u8>,
        chunks: Vec<(String, Vec<i16>)>,
    ) -> std::io::Result<()>;

    async fn read(&self, journal: &str, chunks: &[String]) -> Vec<i16>;

    async fn read_manifest(&self, journal: &str) -> Option<SpillManifest>;

    async fn list_manifests(&self) -> Vec<(String, SpillManifest)>;

    async fn remove(&self, journal: &str);

    fn describe(&self, journal: &str) -> String;
}

pub struct DiskSpill {
    root: PathBuf,
}

impl DiskSpill {
    pub fn new(root: PathBuf) -> DiskSpill {
        DiskSpill { root }
    }

    fn dir(&self, journal: &str) -> PathBuf {
        self.root.join(JOURNAL_DIR).join(journal)
    }
}

#[control_api::async_trait]
impl SpillStore for DiskSpill {
    async fn write(
        &self,
        journal: &str,
        manifest: Vec<u8>,
        chunks: Vec<(String, Vec<i16>)>,
    ) -> std::io::Result<()> {
        let dir = self.dir(journal);
        let written: Vec<(PathBuf, Vec<i16>)> = chunks
            .into_iter()
            .map(|(name, samples)| (dir.join(name), samples))
            .collect();
        tokio::task::spawn_blocking(move || write_segment(&dir, &manifest, written))
            .await
            .unwrap_or_else(|error| {
                Err(std::io::Error::other(format!(
                    "the spill thread died: {error}"
                )))
            })
    }

    async fn read(&self, journal: &str, chunks: &[String]) -> Vec<i16> {
        let dir = self.dir(journal);
        let paths: Vec<PathBuf> = chunks.iter().map(|name| dir.join(name)).collect();
        tokio::task::spawn_blocking(move || read_chunks(&paths))
            .await
            .unwrap_or_else(|error| {
                warn!(%error, "the spill reader thread died");
                Vec::new()
            })
    }

    async fn read_manifest(&self, journal: &str) -> Option<SpillManifest> {
        let path = self.dir(journal).join(MANIFEST_FILE);
        tokio::task::spawn_blocking(move || read_manifest(&path))
            .await
            .ok()
            .flatten()
    }

    async fn list_manifests(&self) -> Vec<(String, SpillManifest)> {
        let root = self.root.join(JOURNAL_DIR);
        tokio::task::spawn_blocking(move || collect_manifests(&root))
            .await
            .unwrap_or_default()
    }

    async fn remove(&self, journal: &str) {
        let dir = self.dir(journal);
        let removed = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&dir)).await;
        if let Ok(Err(error)) = removed {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(%error, "a spill journal could not be removed");
            }
        }
    }

    fn describe(&self, journal: &str) -> String {
        self.dir(journal).display().to_string()
    }
}

pub struct ObjectSpill {
    sink: Arc<dyn RecordingSink>,
    prefix: String,
}

impl ObjectSpill {
    pub fn new(sink: Arc<dyn RecordingSink>, prefix: &str) -> ObjectSpill {
        ObjectSpill {
            sink,
            prefix: normalized_prefix(prefix),
        }
    }

    fn key(&self, journal: &str, name: &str) -> String {
        format!("{}{journal}/{name}", self.prefix)
    }
}

pub fn normalized_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.ends_with('/') {
        trimmed.to_string()
    } else {
        format!("{trimmed}/")
    }
}

#[control_api::async_trait]
impl SpillStore for ObjectSpill {
    async fn write(
        &self,
        journal: &str,
        manifest: Vec<u8>,
        chunks: Vec<(String, Vec<i16>)>,
    ) -> std::io::Result<()> {
        for (name, samples) in chunks {
            let key = self.key(journal, &name);
            let body = pcm_bytes(&samples);
            bounded(self.sink.put(&key, CHUNK_CONTENT_TYPE, body))
                .await
                .map_err(|error| std::io::Error::other(format!("{key}: {error}")))?;
        }
        let key = self.key(journal, MANIFEST_FILE);
        bounded(self.sink.put(&key, MANIFEST_CONTENT_TYPE, manifest))
            .await
            .map_err(|error| std::io::Error::other(format!("{key}: {error}")))?;
        Ok(())
    }

    async fn read(&self, journal: &str, chunks: &[String]) -> Vec<i16> {
        let mut samples = Vec::new();
        for name in chunks {
            let key = self.key(journal, name);
            match bounded(self.sink.get(&key)).await {
                Ok(bytes) => samples.extend(
                    bytes
                        .chunks_exact(2)
                        .map(|pair| i16::from_le_bytes([pair[0], pair[1]])),
                ),
                Err(error) => warn!(
                    %key,
                    %error,
                    "a spilled recording segment could not be read back from the recording \
                     bucket; its audio is missing"
                ),
            }
        }
        samples
    }

    async fn read_manifest(&self, journal: &str) -> Option<SpillManifest> {
        let key = self.key(journal, MANIFEST_FILE);
        let body = match bounded(self.sink.get(&key)).await {
            Ok(body) => body,
            Err(UploadError::Missing(_)) => return None,
            Err(error) => {
                warn!(%key, %error, "a spill manifest in the recording bucket is unreachable");
                return None;
            }
        };
        decode_manifest(&key, &body)
    }

    async fn list_manifests(&self) -> Vec<(String, SpillManifest)> {
        let listed = match bounded(self.sink.list(&self.prefix)).await {
            Ok(listed) => listed,
            Err(error) => {
                warn!(
                    prefix = %self.prefix,
                    %error,
                    "the reserved spill namespace could not be listed; nothing is salvaged"
                );
                return Vec::new();
            }
        };
        let suffix = format!("/{MANIFEST_FILE}");
        let mut found = Vec::new();
        for key in listed {
            let Some(journal) = key
                .strip_prefix(&self.prefix)
                .and_then(|rest| rest.strip_suffix(&suffix))
            else {
                continue;
            };
            if let Some(manifest) = self.read_manifest(journal).await {
                found.push((journal.to_string(), manifest));
            }
        }
        found
    }

    async fn remove(&self, journal: &str) {
        let under = format!("{}{journal}/", self.prefix);
        let listed = match bounded(self.sink.list(&under)).await {
            Ok(listed) => listed,
            Err(error) => {
                warn!(prefix = %under, %error, "a spill journal could not be listed for removal");
                return;
            }
        };
        for key in listed {
            if let Err(error) = bounded(self.sink.delete(&key)).await {
                warn!(%key, %error, "a spilled recording segment could not be deleted");
            }
        }
    }

    fn describe(&self, journal: &str) -> String {
        format!("{}{journal} in {}", self.prefix, self.sink.describe())
    }
}

async fn bounded<T>(
    work: impl std::future::Future<Output = Result<T, UploadError>>,
) -> Result<T, UploadError> {
    match tokio::time::timeout(SPILL_TIMEOUT, work).await {
        Ok(outcome) => outcome,
        Err(_) => Err(UploadError::TimedOut(SPILL_TIMEOUT)),
    }
}

pub struct SpillWrite {
    store: Arc<dyn SpillStore>,
    journal: String,
    owner: String,
    next: SpillManifest,
    body: Vec<u8>,
    chunks: Vec<(String, Vec<i16>)>,
}

pub enum SpillWritten {
    Landed(SpillManifest),
    Surrendered(String),
    Failed(std::io::Error),
}

impl SpillWrite {
    pub async fn perform(self) -> SpillWritten {
        match tokio::time::timeout(SPILL_TIMEOUT, self.run()).await {
            Ok(written) => written,
            Err(_) => SpillWritten::Failed(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "the spill write did not finish within {} ms",
                    SPILL_TIMEOUT.as_millis()
                ),
            )),
        }
    }

    async fn run(self) -> SpillWritten {
        if let Some(held) = self.store.read_manifest(&self.journal).await {
            if held.owner != self.owner {
                return SpillWritten::Surrendered(held.owner);
            }
        }
        match self
            .store
            .write(&self.journal, self.body, self.chunks)
            .await
        {
            Ok(()) => SpillWritten::Landed(self.next),
            Err(error) => SpillWritten::Failed(error),
        }
    }
}

pub struct SegmentJournal {
    store: Arc<dyn SpillStore>,
    journal: String,
    owner: String,
    manifest: SpillManifest,
    sequence: usize,
    #[cfg(test)]
    surrendered: bool,
}

impl SegmentJournal {
    pub async fn open(
        support: &RecordingSupport,
        recording_id: &str,
        owner: &str,
        sample_rate_hz: u32,
        targets: &[(String, u16)],
    ) -> Option<SegmentJournal> {
        let store = support.journal.clone()?;
        let (first, _) = targets.first()?;
        let journal = first.clone();
        let keys: Vec<String> = targets.iter().map(|(key, _)| key.clone()).collect();
        let held = store.read_manifest(&journal).await;
        let mut adopted_from = None;
        let manifest = match held {
            Some(held) if held.continues(recording_id, sample_rate_hz, &keys) => {
                info!(
                    journal = %store.describe(&journal),
                    frames = held.frames,
                    previous_owner = %held.owner,
                    "this recording continues a spill journal a pod left behind"
                );
                if held.owner != owner {
                    adopted_from = Some(held.owner.clone());
                }
                SpillManifest {
                    owner: owner.to_string(),
                    ..held
                }
            }
            other => {
                if other.is_some() {
                    warn!(
                        journal = %store.describe(&journal),
                        "a spill journal under this object key belongs to another recording; \
                         it is replaced and its audio is unreachable"
                    );
                }
                store.remove(&journal).await;
                SpillManifest {
                    recording_id: recording_id.to_string(),
                    sample_rate_hz,
                    owner: owner.to_string(),
                    frames: 0,
                    targets: targets
                        .iter()
                        .map(|(key, channels)| SpilledTarget {
                            key: key.clone(),
                            channels: *channels,
                            chunks: Vec::new(),
                        })
                        .collect(),
                }
            }
        };
        if let Some(previous) = adopted_from {
            match serde_json::to_vec_pretty(&manifest) {
                Ok(body) => match store.write(&journal, body, Vec::new()).await {
                    Ok(()) => info!(
                        journal = %store.describe(&journal),
                        previous_owner = %previous,
                        "this pod has taken ownership of the spill journal it adopted, so the \
                         pod that left it behind stops writing to it"
                    ),
                    Err(error) => warn!(
                        journal = %store.describe(&journal),
                        %error,
                        "the adopted spill journal could not be claimed; the pod that left it \
                         behind may still be writing to it"
                    ),
                },
                Err(error) => warn!(%error, "an adopted spill manifest could not be re-encoded"),
            }
        }
        let sequence = manifest
            .targets
            .first()
            .map(|target| target.chunks.len())
            .unwrap_or_default();
        Some(SegmentJournal {
            store,
            journal,
            owner: owner.to_string(),
            manifest,
            sequence,
            #[cfg(test)]
            surrendered: false,
        })
    }

    pub fn frames_on_disk(&self) -> u64 {
        self.manifest.frames
    }

    #[cfg(test)]
    pub fn surrendered(&self) -> bool {
        self.surrendered
    }

    pub fn describe(&self) -> String {
        self.store.describe(&self.journal)
    }

    pub fn begin_append(
        &self,
        rendered: Vec<RecordedAudio>,
        frames: u64,
    ) -> std::io::Result<SpillWrite> {
        if rendered.len() != self.manifest.targets.len() {
            return Err(std::io::Error::other(
                "a spilled segment does not carry one rendering per recording target",
            ));
        }
        let mut chunks = Vec::with_capacity(rendered.len());
        let mut names = Vec::with_capacity(rendered.len());
        for (index, audio) in rendered.into_iter().enumerate() {
            let name = format!("{index}-{:06}.{CHUNK_EXTENSION}", self.sequence);
            chunks.push((name.clone(), audio.samples));
            names.push(name);
        }
        let mut next = self.manifest.clone();
        next.frames = next.frames.saturating_add(frames);
        for (target, name) in next.targets.iter_mut().zip(names) {
            target.chunks.push(name);
        }
        let body = serde_json::to_vec_pretty(&next).map_err(std::io::Error::other)?;
        Ok(SpillWrite {
            store: Arc::clone(&self.store),
            journal: self.journal.clone(),
            owner: self.owner.clone(),
            next,
            body,
            chunks,
        })
    }

    pub fn commit(&mut self, landed: SpillManifest) {
        self.manifest = landed;
        self.sequence += 1;
    }

    #[cfg(test)]
    pub async fn append(
        &mut self,
        rendered: Vec<RecordedAudio>,
        frames: u64,
    ) -> std::io::Result<()> {
        match self.begin_append(rendered, frames)?.perform().await {
            SpillWritten::Landed(manifest) => {
                self.commit(manifest);
                Ok(())
            }
            SpillWritten::Surrendered(owner) => {
                self.surrendered = true;
                Err(std::io::Error::other(format!(
                    "this spill journal now belongs to {owner}, so this pod stops writing it"
                )))
            }
            SpillWritten::Failed(error) => Err(error),
        }
    }

    pub async fn read_back(&self, index: usize) -> Vec<i16> {
        let Some(target) = self.manifest.targets.get(index) else {
            return Vec::new();
        };
        self.store.read(&self.journal, &target.chunks).await
    }

    pub async fn discard(self) {
        self.store.remove(&self.journal).await;
    }
}

fn write_segment(
    dir: &Path,
    manifest: &[u8],
    chunks: Vec<(PathBuf, Vec<i16>)>,
) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    for (path, samples) in chunks {
        std::fs::write(&path, pcm_bytes(&samples))?;
    }
    let temporary = dir.join(MANIFEST_TEMP);
    std::fs::write(&temporary, manifest)?;
    std::fs::rename(&temporary, dir.join(MANIFEST_FILE))
}

fn pcm_bytes(samples: &[i16]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    bytes
}

fn read_chunks(paths: &[PathBuf]) -> Vec<i16> {
    let mut samples = Vec::new();
    for path in paths {
        match std::fs::read(path) {
            Ok(bytes) => samples.extend(
                bytes
                    .chunks_exact(2)
                    .map(|pair| i16::from_le_bytes([pair[0], pair[1]])),
            ),
            Err(error) => warn!(
                path = %path.display(),
                %error,
                "a spilled recording segment could not be read back; its audio is missing"
            ),
        }
    }
    samples
}

fn decode_manifest(named: &str, body: &[u8]) -> Option<SpillManifest> {
    match serde_json::from_slice::<SpillManifest>(body) {
        Ok(manifest) => Some(manifest),
        Err(error) => {
            warn!(
                journal = %named,
                %error,
                "a spill manifest is unreadable; the segments beside it are unreachable"
            );
            None
        }
    }
}

fn read_manifest(path: &Path) -> Option<SpillManifest> {
    let body = std::fs::read(path).ok()?;
    decode_manifest(&path.display().to_string(), &body)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SalvageSummary {
    pub uploaded: u64,
    pub already_present: u64,
    pub failed: u64,
    pub foreign: u64,
}

pub async fn salvage(support: &RecordingSupport) -> SalvageSummary {
    let mut summary = SalvageSummary::default();
    let Some(store) = support.journal.clone() else {
        return summary;
    };
    let Some(sink) = support.sink.clone() else {
        return summary;
    };
    let journals = store.list_manifests().await;
    if journals.is_empty() {
        return summary;
    }
    info!(
        journals = journals.len(),
        "recording segments were left behind by a pod that stopped mid-call"
    );
    for (journal, manifest) in journals {
        if manifest.owner != support.owner {
            summary.foreign += 1;
            support
                .counters
                .spill_foreign_manifests
                .fetch_add(1, Ordering::Relaxed);
            info!(
                journal = %store.describe(&journal),
                owner = %manifest.owner,
                frames = manifest.frames,
                "this spill journal belongs to another pod; adoption, not salvage, is the \
                 cross-pod path, so it is left alone"
            );
            continue;
        }
        let mut salvaged_all = true;
        for target in manifest.targets.iter() {
            match sink.exists(&target.key).await {
                Ok(true) => {
                    summary.already_present += 1;
                    support
                        .counters
                        .salvage_skipped
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        key = %target.key,
                        journal = %store.describe(&journal),
                        frames = manifest.frames,
                        "this recording was already uploaded, so the segments spilled here are \
                         not reuploaded; they stay where they are for an operator to judge"
                    );
                    salvaged_all = false;
                    continue;
                }
                Ok(false) => {}
                Err(error) => {
                    summary.failed += 1;
                    support
                        .counters
                        .salvage_failures
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(key = %target.key, %error, "could not ask storage about this recording");
                    salvaged_all = false;
                    continue;
                }
            }
            let samples = store.read(&journal, &target.chunks).await;
            if samples.is_empty() {
                summary.failed += 1;
                support
                    .counters
                    .salvage_failures
                    .fetch_add(1, Ordering::Relaxed);
                salvaged_all = false;
                continue;
            }
            let rate = manifest.sample_rate_hz;
            let channels = target.channels;
            let built = tokio::task::spawn_blocking(move || wav_bytes(rate, channels, &samples))
                .await
                .map_err(|error| error.to_string())
                .and_then(|built| built.map_err(|error| error.to_string()));
            let bytes = match built {
                Ok(bytes) => bytes,
                Err(error) => {
                    summary.failed += 1;
                    support
                        .counters
                        .salvage_failures
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(key = %target.key, %error, "a salvaged recording could not be encoded");
                    salvaged_all = false;
                    continue;
                }
            };
            let size = bytes.len();
            match sink.put(&target.key, "audio/wav", bytes).await {
                Ok(uri) => {
                    summary.uploaded += 1;
                    support.counters.salvaged.fetch_add(1, Ordering::Relaxed);
                    support.counters.uploaded.fetch_add(1, Ordering::Relaxed);
                    support
                        .counters
                        .bytes_uploaded
                        .fetch_add(size as u64, Ordering::Relaxed);
                    info!(
                        %uri,
                        recording_id = %manifest.recording_id,
                        frames = manifest.frames,
                        "a recording whose pod died was salvaged from its spill journal; the \
                         audio the dead pod had not spilled is still missing"
                    );
                }
                Err(error) => {
                    summary.failed += 1;
                    support
                        .counters
                        .salvage_failures
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(key = %target.key, %error, "a salvaged recording could not be uploaded");
                    salvaged_all = false;
                }
            }
        }
        if salvaged_all {
            store.remove(&journal).await;
        }
    }
    summary
}

fn collect_manifests(root: &Path) -> Vec<(String, SpillManifest)> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|name| name == MANIFEST_FILE) {
                if let Some(manifest) = read_manifest(&path) {
                    if let Ok(relative) = dir.strip_prefix(root) {
                        found.push((relative.to_string_lossy().replace('\\', "/"), manifest));
                    }
                }
            }
        }
    }
    found
}
