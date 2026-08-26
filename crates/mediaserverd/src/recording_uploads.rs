use crate::recorder::{FinishedCapture, RecorderCounters};
use control_api::ObservationSink;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{info, warn};

pub const UPLOAD_SETTLE_TIMEOUT: Duration = Duration::from_secs(600);

pub struct UploadTracker {
    counters: Arc<RecorderCounters>,
    in_flight: AtomicU64,
    idle: Notify,
}

impl UploadTracker {
    pub fn new(counters: Arc<RecorderCounters>) -> Arc<UploadTracker> {
        Arc::new(UploadTracker {
            counters,
            in_flight: AtomicU64::new(0),
            idle: Notify::new(),
        })
    }

    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::SeqCst)
    }

    pub fn adopt(
        self: &Arc<Self>,
        finished: FinishedCapture,
        observer: Option<Weak<dyn ObservationSink>>,
    ) {
        let session = finished.session;
        let recording_id = finished.recording_id.clone();
        let held = observer.as_ref().and_then(Weak::upgrade);
        let retained = match held.as_ref() {
            Some(sink) => sink.retain_for_upload(session),
            None => {
                warn!(
                    %session,
                    %recording_id,
                    "no observation sink is wired; this upload's own event reaches nobody"
                );
                false
            }
        };
        let upload = finished.into_upload();
        self.counters
            .uploads_backgrounded
            .fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        self.counters
            .uploads_in_flight
            .store(self.in_flight(), Ordering::Relaxed);
        let tracker = Arc::clone(self);
        tokio::spawn(async move {
            let aborter = upload.abort_handle();
            match tokio::time::timeout(UPLOAD_SETTLE_TIMEOUT, upload).await {
                Ok(Ok(outcome)) => info!(
                    %session,
                    %recording_id,
                    duration_ms = outcome.duration_ms,
                    bytes = outcome.bytes,
                    uris = ?outcome.uris,
                    "a backgrounded recording upload settled"
                ),
                Ok(Err(error)) => warn!(
                    %session,
                    %recording_id,
                    %error,
                    "a backgrounded recording upload ended without an outcome"
                ),
                Err(_) => {
                    aborter.abort();
                    tracker
                        .counters
                        .upload_settle_timeouts
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        %session,
                        %recording_id,
                        timeout_ms = UPLOAD_SETTLE_TIMEOUT.as_millis() as u64,
                        "a backgrounded recording upload never settled; its audio is on this \
                         pod's spill disk for the salvage pass"
                    );
                }
            }
            if retained {
                if let Some(sink) = held {
                    sink.release_after_upload(session);
                }
            }
            tracker.settled();
        });
    }

    pub async fn wait_idle(&self) -> u64 {
        let started = self.in_flight();
        loop {
            let idle = self.idle.notified();
            if self.in_flight() == 0 {
                return started;
            }
            idle.await;
        }
    }

    fn settled(&self) {
        let left = self
            .in_flight
            .fetch_sub(1, Ordering::SeqCst)
            .saturating_sub(1);
        self.counters
            .uploads_in_flight
            .store(left, Ordering::Relaxed);
        if left == 0 {
            self.idle.notify_waiters();
        }
    }
}
