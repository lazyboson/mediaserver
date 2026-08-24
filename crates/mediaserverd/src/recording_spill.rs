use crate::recorder::{wav_bytes, RecordedAudio, RecordingSupport};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use tracing::{info, warn};

pub const JOURNAL_DIR: &str = "journal";
const MANIFEST_FILE: &str = "manifest.json";
const MANIFEST_TEMP: &str = "manifest.json.writing";
const CHUNK_EXTENSION: &str = "pcm";

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

pub struct SegmentJournal {
    dir: PathBuf,
    manifest: SpillManifest,
    sequence: usize,
}

impl SegmentJournal {
    pub async fn open(
        support: &RecordingSupport,
        recording_id: &str,
        owner: &str,
        sample_rate_hz: u32,
        targets: &[(String, u16)],
    ) -> Option<SegmentJournal> {
        let root = support.spill_dir.as_ref()?;
        let (first, _) = targets.first()?;
        let dir = root.join(JOURNAL_DIR).join(first);
        let keys: Vec<String> = targets.iter().map(|(key, _)| key.clone()).collect();
        let held = read_manifest(&dir.join(MANIFEST_FILE));
        let manifest = match held {
            Some(held) if held.continues(recording_id, sample_rate_hz, &keys) => {
                info!(
                    dir = %dir.display(),
                    frames = held.frames,
                    previous_owner = %held.owner,
                    "this recording continues a spill journal left on local disk"
                );
                SpillManifest {
                    owner: owner.to_string(),
                    ..held
                }
            }
            other => {
                if other.is_some() {
                    warn!(
                        dir = %dir.display(),
                        "a spill journal under this object key belongs to another recording; \
                         it is replaced and its audio is unreachable"
                    );
                }
                let cleared = dir.clone();
                let _ =
                    tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&cleared)).await;
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
        let sequence = manifest
            .targets
            .first()
            .map(|target| target.chunks.len())
            .unwrap_or_default();
        Some(SegmentJournal {
            dir,
            manifest,
            sequence,
        })
    }

    pub fn frames_on_disk(&self) -> u64 {
        self.manifest.frames
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub async fn append(
        &mut self,
        rendered: Vec<RecordedAudio>,
        frames: u64,
    ) -> std::io::Result<()> {
        if rendered.len() != self.manifest.targets.len() {
            return Err(std::io::Error::other(
                "a spilled segment does not carry one rendering per recording target",
            ));
        }
        let mut written = Vec::with_capacity(rendered.len());
        let mut names = Vec::with_capacity(rendered.len());
        for (index, audio) in rendered.into_iter().enumerate() {
            let name = format!("{index}-{:06}.{CHUNK_EXTENSION}", self.sequence);
            written.push((self.dir.join(&name), audio.samples));
            names.push(name);
        }
        let mut next = self.manifest.clone();
        next.frames = next.frames.saturating_add(frames);
        for (target, name) in next.targets.iter_mut().zip(names) {
            target.chunks.push(name);
        }
        let dir = self.dir.clone();
        let body = serde_json::to_vec_pretty(&next).map_err(std::io::Error::other)?;
        tokio::task::spawn_blocking(move || write_segment(&dir, &body, written))
            .await
            .unwrap_or_else(|error| {
                Err(std::io::Error::other(format!(
                    "the spill thread died: {error}"
                )))
            })?;
        self.manifest = next;
        self.sequence += 1;
        Ok(())
    }

    pub async fn read_back(&self, index: usize) -> Vec<i16> {
        let Some(target) = self.manifest.targets.get(index) else {
            return Vec::new();
        };
        let paths: Vec<PathBuf> = target
            .chunks
            .iter()
            .map(|name| self.dir.join(name))
            .collect();
        let key = target.key.clone();
        tokio::task::spawn_blocking(move || read_chunks(&paths))
            .await
            .unwrap_or_else(|error| {
                warn!(%key, %error, "the spill reader thread died");
                Vec::new()
            })
    }

    pub async fn discard(self) {
        let dir = self.dir;
        let removed = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&dir)).await;
        if let Ok(Err(error)) = removed {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(%error, "a spill journal could not be removed after its upload");
            }
        }
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

fn read_manifest(path: &Path) -> Option<SpillManifest> {
    let body = std::fs::read(path).ok()?;
    match serde_json::from_slice::<SpillManifest>(&body) {
        Ok(manifest) => Some(manifest),
        Err(error) => {
            warn!(
                path = %path.display(),
                %error,
                "a spill manifest is unreadable; the segments beside it are unreachable"
            );
            None
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SalvageSummary {
    pub uploaded: u64,
    pub already_present: u64,
    pub failed: u64,
}

pub async fn salvage(support: &RecordingSupport) -> SalvageSummary {
    let mut summary = SalvageSummary::default();
    let Some(root) = support.spill_dir.clone() else {
        return summary;
    };
    let Some(sink) = support.sink.clone() else {
        return summary;
    };
    let journals = tokio::task::spawn_blocking(move || collect_manifests(&root.join(JOURNAL_DIR)))
        .await
        .unwrap_or_default();
    if journals.is_empty() {
        return summary;
    }
    info!(
        journals = journals.len(),
        "recording segments were left on local disk by an earlier life of this pod"
    );
    for (dir, manifest) in journals {
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
                        dir = %dir.display(),
                        frames = manifest.frames,
                        "this recording was already uploaded, so the segments spilled here are \
                         not reuploaded; they stay on disk for an operator to judge"
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
            let paths: Vec<PathBuf> = target.chunks.iter().map(|name| dir.join(name)).collect();
            let samples = tokio::task::spawn_blocking(move || read_chunks(&paths))
                .await
                .unwrap_or_default();
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
                        "a recording whose pod died was salvaged from local disk; the audio the \
                         dead pod had not spilled is still missing"
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
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&dir)).await;
        }
    }
    summary
}

fn collect_manifests(root: &Path) -> Vec<(PathBuf, SpillManifest)> {
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
                    found.push((dir.clone(), manifest));
                }
            }
        }
    }
    found
}
