use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::info;

#[derive(Debug, Clone)]
pub struct MediaConfig {
    pub workers: usize,
    pub tick: Duration,
}

impl Default for MediaConfig {
    fn default() -> Self {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2);
        MediaConfig {
            workers: workers.saturating_sub(1).max(1),
            tick: Duration::from_millis(5),
        }
    }
}

pub struct MediaWorld {
    stop: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
    pub ticks: Arc<AtomicU64>,
}

impl MediaWorld {
    pub fn spawn(config: MediaConfig) -> MediaWorld {
        let stop = Arc::new(AtomicBool::new(false));
        let ticks = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::with_capacity(config.workers);
        for worker_id in 0..config.workers {
            let stop = Arc::clone(&stop);
            let ticks = Arc::clone(&ticks);
            let tick = config.tick;
            let handle = std::thread::Builder::new()
                .name(format!("mss-media-{worker_id}"))
                .spawn(move || worker_loop(worker_id, tick, &stop, &ticks))
                .expect("failed to spawn media worker");
            handles.push(handle);
        }
        info!(
            workers = config.workers,
            tick_ms = config.tick.as_millis() as u64,
            "media world up"
        );
        MediaWorld {
            stop,
            handles,
            ticks,
        }
    }

    pub fn shutdown(self) {
        self.stop.store(true, Ordering::Relaxed);
        for handle in self.handles {
            let _ = handle.join();
        }
        info!(
            total_ticks = self.ticks.load(Ordering::Relaxed),
            "media world stopped"
        );
    }
}

fn worker_loop(worker_id: usize, tick: Duration, stop: &AtomicBool, ticks: &AtomicU64) {
    info!(worker_id, "media worker started");
    let mut next = Instant::now() + tick;
    while !stop.load(Ordering::Relaxed) {
        ticks.fetch_add(1, Ordering::Relaxed);

        let now = Instant::now();
        if next > now {
            std::thread::sleep(next - now);
        }
        next += tick;
        if next < Instant::now() {
            next = Instant::now() + tick;
        }
    }
    info!(worker_id, "media worker stopping");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawns_ticks_and_shuts_down_cleanly() {
        let world = MediaWorld::spawn(MediaConfig {
            workers: 2,
            tick: Duration::from_millis(1),
        });
        std::thread::sleep(Duration::from_millis(30));
        assert!(world.ticks.load(Ordering::Relaxed) > 10);
        world.shutdown();
    }
}
