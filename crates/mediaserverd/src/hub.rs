use crossbeam_queue::ArrayQueue;
use media_core::{g711, Track};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

pub const MAX_FRAME_BYTES: usize = 320;
const COMMAND_CAPACITY: usize = 64;
const INJECTED_CAPACITY: usize = 16;
const INJECT_SILENCE: [i16; 480] = [0; 480];

#[derive(Clone, Copy)]
#[allow(clippy::large_enum_variant)]
pub enum TapEvent {
    Media {
        track: Track,
        timestamp_ms: u64,
        len: usize,
        bytes: [u8; MAX_FRAME_BYTES],
    },
    Dtmf {
        track: Track,
        digit: char,
    },
}

impl TapEvent {
    pub fn media(track: Track, timestamp_ms: u64, pcm: &[i16], ulaw: bool) -> TapEvent {
        let mut bytes = [0u8; MAX_FRAME_BYTES];
        let len = g711::encode_into(ulaw, pcm, &mut bytes);
        TapEvent::Media {
            track,
            timestamp_ms,
            len,
            bytes,
        }
    }

    fn track(&self) -> Track {
        match self {
            TapEvent::Media { track, .. } | TapEvent::Dtmf { track, .. } => *track,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackSelection {
    All,
    Only(Track),
}

impl TrackSelection {
    pub fn wants(self, track: Track) -> bool {
        match self {
            TrackSelection::All => true,
            TrackSelection::Only(only) => only == track,
        }
    }
}

struct Shared {
    frames: ArrayQueue<TapEvent>,
    dropped_oldest: AtomicU64,
    delivered: AtomicU64,
    closed: AtomicBool,
    wake: Notify,
}

impl Shared {
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.wake.notify_waiters();
        self.wake.notify_one();
    }
}

struct Consumer {
    id: u64,
    selection: TrackSelection,
    shared: Arc<Shared>,
}

enum HubCommand {
    Attach(Consumer),
    Detach(u64),
}

pub struct Hub {
    consumers: Vec<Consumer>,
    commands: Arc<ArrayQueue<HubCommand>>,
    injected: Arc<ArrayQueue<Vec<i16>>>,
    injecting: Option<(Vec<i16>, usize)>,
    injected_frames: u64,
    published: u64,
}

impl Hub {
    pub fn new() -> (Hub, HubClient) {
        let commands = Arc::new(ArrayQueue::new(COMMAND_CAPACITY));
        let injected = Arc::new(ArrayQueue::new(INJECTED_CAPACITY));
        (
            Hub {
                consumers: Vec::new(),
                commands: Arc::clone(&commands),
                injected: Arc::clone(&injected),
                injecting: None,
                injected_frames: 0,
                published: 0,
            },
            HubClient {
                commands,
                injected,
                next_id: Arc::new(AtomicU64::new(1)),
            },
        )
    }

    pub fn poll_commands(&mut self) {
        while let Some(command) = self.commands.pop() {
            match command {
                HubCommand::Attach(consumer) => self.consumers.push(consumer),
                HubCommand::Detach(id) => self.consumers.retain(|consumer| {
                    let keep = consumer.id != id;
                    if !keep {
                        consumer.shared.close();
                    }
                    keep
                }),
            }
        }
    }

    pub fn publish(&mut self, event: TapEvent) {
        self.published += 1;
        for consumer in &self.consumers {
            if !consumer.selection.wants(event.track()) {
                continue;
            }
            if consumer.shared.frames.force_push(event).is_some() {
                consumer
                    .shared
                    .dropped_oldest
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                consumer.shared.delivered.fetch_add(1, Ordering::Relaxed);
            }
            consumer.shared.wake.notify_one();
        }
    }

    pub fn release_injected(&mut self, samples_per_frame: usize, ptime_ms: u64) {
        let timestamp_ms = self.injected_frames * ptime_ms;
        self.injected_frames += 1;
        if self.injecting.is_none() {
            self.injecting = self.injected.pop().map(|pcm| (pcm, 0));
        }
        let frame = samples_per_frame.max(1).min(INJECT_SILENCE.len());
        let event = match self.injecting.as_mut() {
            Some((pcm, at)) => {
                let end = (*at + frame).min(pcm.len());
                let event = TapEvent::media(Track::Mixed, timestamp_ms, &pcm[*at..end], true);
                *at = end;
                if *at >= pcm.len() {
                    self.injecting = None;
                }
                event
            }
            None => TapEvent::media(Track::Mixed, timestamp_ms, &INJECT_SILENCE[..frame], true),
        };
        self.publish(event);
    }

    pub fn published(&self) -> u64 {
        self.published
    }

    pub fn consumer_count(&self) -> usize {
        self.consumers.len()
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        self.poll_commands();
        for consumer in &self.consumers {
            consumer.shared.close();
        }
    }
}

#[derive(Clone)]
pub struct HubClient {
    commands: Arc<ArrayQueue<HubCommand>>,
    injected: Arc<ArrayQueue<Vec<i16>>>,
    next_id: Arc<AtomicU64>,
}

impl HubClient {
    pub fn inject(&self, pcm: Vec<i16>) -> bool {
        self.injected.push(pcm).is_ok()
    }

    pub fn attach(&self, capacity: usize, selection: TrackSelection) -> Option<Subscription> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let shared = Arc::new(Shared {
            frames: ArrayQueue::new(capacity.max(1)),
            dropped_oldest: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            wake: Notify::new(),
        });
        let consumer = Consumer {
            id,
            selection,
            shared: Arc::clone(&shared),
        };
        match self.commands.push(HubCommand::Attach(consumer)) {
            Ok(()) => Some(Subscription {
                id,
                shared,
                commands: Arc::clone(&self.commands),
            }),
            Err(_) => None,
        }
    }
}

pub struct Subscription {
    id: u64,
    shared: Arc<Shared>,
    commands: Arc<ArrayQueue<HubCommand>>,
}

impl Subscription {
    pub async fn next(&mut self) -> Option<TapEvent> {
        loop {
            if let Some(event) = self.shared.frames.pop() {
                return Some(event);
            }
            if self.shared.closed.load(Ordering::Acquire) {
                return self.shared.frames.pop();
            }
            self.shared.wake.notified().await;
        }
    }

    #[cfg(test)]
    pub fn try_next(&mut self) -> Option<TapEvent> {
        self.shared.frames.pop()
    }

    pub fn dropped_oldest(&self) -> u64 {
        self.shared.dropped_oldest.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn delivered(&self) -> u64 {
        self.shared.delivered.load(Ordering::Relaxed)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let _ = self.commands.push(HubCommand::Detach(self.id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(track: Track, timestamp_ms: u64) -> TapEvent {
        TapEvent::media(track, timestamp_ms, &[0i16; 160], true)
    }

    fn timestamp(event: &TapEvent) -> u64 {
        match event {
            TapEvent::Media { timestamp_ms, .. } => *timestamp_ms,
            TapEvent::Dtmf { .. } => panic!("expected media"),
        }
    }

    #[test]
    fn every_attached_consumer_receives_every_frame() {
        let (mut hub, client) = Hub::new();
        let mut first = client.attach(8, TrackSelection::All).unwrap();
        let mut second = client.attach(8, TrackSelection::All).unwrap();
        hub.poll_commands();

        for at in 0..3 {
            hub.publish(frame(Track::Customer, at * 20));
        }

        for subscription in [&mut first, &mut second] {
            let seen: Vec<u64> = std::iter::from_fn(|| subscription.try_next())
                .map(|e| timestamp(&e))
                .collect();
            assert_eq!(seen, vec![0, 20, 40]);
        }
        assert_eq!(hub.consumer_count(), 2);
        assert_eq!(hub.published(), 3);
    }

    #[test]
    fn a_full_queue_drops_the_oldest_and_counts_it() {
        let (mut hub, client) = Hub::new();
        let mut slow = client.attach(4, TrackSelection::All).unwrap();
        hub.poll_commands();

        for at in 0..6 {
            hub.publish(frame(Track::Customer, at * 20));
        }

        let seen: Vec<u64> = std::iter::from_fn(|| slow.try_next())
            .map(|e| timestamp(&e))
            .collect();
        assert_eq!(seen, vec![40, 60, 80, 100]);
        assert_eq!(slow.dropped_oldest(), 2);
        assert_eq!(slow.delivered(), 4);
    }

    #[test]
    fn track_selection_filters_at_the_hub_not_the_consumer() {
        let (mut hub, client) = Hub::new();
        let mut customer_only = client
            .attach(8, TrackSelection::Only(Track::Customer))
            .unwrap();
        hub.poll_commands();

        hub.publish(frame(Track::Customer, 0));
        hub.publish(frame(Track::Agent, 0));
        hub.publish(frame(Track::Customer, 20));

        let seen = std::iter::from_fn(|| customer_only.try_next()).count();
        assert_eq!(seen, 2);
        assert_eq!(customer_only.delivered(), 2);
    }

    #[test]
    fn attach_mid_stream_sees_only_later_frames_and_detach_stops_delivery() {
        let (mut hub, client) = Hub::new();
        hub.publish(frame(Track::Customer, 0));

        let mut late = client.attach(8, TrackSelection::All).unwrap();
        hub.poll_commands();
        hub.publish(frame(Track::Customer, 20));

        assert_eq!(timestamp(&late.try_next().unwrap()), 20);
        assert!(late.try_next().is_none());

        drop(late);
        hub.poll_commands();
        assert_eq!(hub.consumer_count(), 0);
        hub.publish(frame(Track::Customer, 40));
        assert_eq!(hub.published(), 3);
    }

    #[test]
    fn injected_audio_is_paced_out_as_the_mixed_track() {
        let (mut hub, client) = Hub::new();
        let mut listener = client.attach(16, TrackSelection::All).unwrap();
        let mut bot_only = client
            .attach(16, TrackSelection::Only(Track::Mixed))
            .unwrap();
        hub.poll_commands();

        hub.release_injected(160, 20);
        assert!(client.inject(vec![100i16; 400]));
        for _ in 0..3 {
            hub.release_injected(160, 20);
        }
        hub.release_injected(160, 20);
        let voiced = |bytes: &[u8], len: usize| bytes[..len].iter().any(|b| *b != 0xFF);

        let mut sizes = Vec::new();
        let mut stamps = Vec::new();
        let mut speech = Vec::new();
        while let Some(event) = bot_only.try_next() {
            match event {
                TapEvent::Media {
                    track,
                    timestamp_ms,
                    len,
                    bytes,
                } => {
                    assert_eq!(track, Track::Mixed);
                    stamps.push(timestamp_ms);
                    sizes.push(len);
                    speech.push(voiced(&bytes, len));
                }
                TapEvent::Dtmf { .. } => panic!("expected media"),
            }
        }
        assert_eq!(sizes, vec![160, 160, 160, 80, 160]);
        assert_eq!(stamps, vec![0, 20, 40, 60, 80]);
        assert_eq!(speech, vec![false, true, true, true, false]);
        assert_eq!(std::iter::from_fn(|| listener.try_next()).count(), 5);
    }

    #[tokio::test]
    async fn next_wakes_on_publish_and_ends_when_the_hub_dies() {
        let (mut hub, client) = Hub::new();
        let mut subscription = client.attach(8, TrackSelection::All).unwrap();
        hub.poll_commands();

        let waiter = tokio::spawn(async move {
            let mut seen = Vec::new();
            while let Some(event) = subscription.next().await {
                seen.push(timestamp(&event));
            }
            seen
        });

        let publisher = std::thread::spawn(move || {
            hub.publish(frame(Track::Customer, 0));
            hub.publish(frame(Track::Customer, 20));
            std::thread::sleep(std::time::Duration::from_millis(20));
            drop(hub);
        });

        let seen = waiter.await.unwrap();
        publisher.join().unwrap();
        assert_eq!(seen, vec![0, 20]);
    }
}
