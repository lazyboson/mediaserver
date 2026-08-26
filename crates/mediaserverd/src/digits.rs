use crossbeam_queue::ArrayQueue;
use media_core::dtmf::DigitPress;
use media_core::Track;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

const PRESS_CAPACITY: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Digit {
    pub track: Track,
    pub digit: char,
    pub duration_ms: u32,
    pub rtp_timestamp: u32,
}

impl Digit {
    pub fn pressed(track: Track, press: DigitPress) -> Digit {
        Digit {
            track,
            digit: press.digit,
            duration_ms: press.duration_ms,
            rtp_timestamp: press.rtp_timestamp,
        }
    }
}

pub struct DigitQueue {
    presses: ArrayQueue<Digit>,
    wake: Notify,
    published: AtomicU64,
    dropped: AtomicU64,
    closed: AtomicBool,
}

impl DigitQueue {
    pub fn new() -> Arc<DigitQueue> {
        Arc::new(DigitQueue {
            presses: ArrayQueue::new(PRESS_CAPACITY),
            wake: Notify::new(),
            published: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        })
    }

    pub fn publish(&self, digit: Digit) -> bool {
        let accepted = self.presses.push(digit).is_ok();
        if accepted {
            self.published.fetch_add(1, Ordering::Relaxed);
        } else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        self.wake.notify_one();
        accepted
    }

    pub async fn next(&self) -> Option<Digit> {
        loop {
            if let Some(digit) = self.presses.pop() {
                return Some(digit);
            }
            if self.closed.load(Ordering::Acquire) {
                return self.presses.pop();
            }
            self.wake.notified().await;
        }
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.wake.notify_waiters();
        self.wake.notify_one();
    }

    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn press(digit: char) -> Digit {
        Digit {
            track: Track::Customer,
            digit,
            duration_ms: 100,
            rtp_timestamp: 8000,
        }
    }

    #[tokio::test]
    async fn a_press_published_from_another_thread_arrives_in_order() {
        let queue = DigitQueue::new();
        let producer = Arc::clone(&queue);
        std::thread::spawn(move || {
            for digit in ['1', '2', '#'] {
                producer.publish(press(digit));
            }
            producer.close();
        });

        let mut seen = String::new();
        while let Some(digit) = queue.next().await {
            seen.push(digit.digit);
        }
        assert_eq!(seen, "12#");
        assert_eq!(queue.published(), 3);
        assert_eq!(queue.dropped(), 0);
    }

    #[tokio::test]
    async fn a_full_queue_counts_the_refusal_instead_of_blocking_the_media_thread() {
        let queue = DigitQueue::new();
        for _ in 0..PRESS_CAPACITY {
            assert!(queue.publish(press('7')));
        }
        assert!(!queue.publish(press('8')));
        assert_eq!(queue.dropped(), 1);
        assert_eq!(queue.published(), PRESS_CAPACITY as u64);
    }

    #[tokio::test]
    async fn closing_drains_what_is_queued_before_it_ends_the_stream() {
        let queue = DigitQueue::new();
        queue.publish(press('4'));
        queue.close();
        assert_eq!(queue.next().await.map(|d| d.digit), Some('4'));
        assert_eq!(queue.next().await, None);
    }

    #[tokio::test]
    async fn a_press_carries_the_track_and_the_timing_the_detector_measured() {
        let queue = DigitQueue::new();
        queue.publish(Digit::pressed(
            Track::Agent,
            DigitPress {
                digit: '*',
                duration_ms: 240,
                rtp_timestamp: 41_000,
            },
        ));
        queue.close();
        let digit = queue.next().await.unwrap();
        assert_eq!(digit.track, Track::Agent);
        assert_eq!(digit.digit, '*');
        assert_eq!(digit.duration_ms, 240);
        assert_eq!(digit.rtp_timestamp, 41_000);
    }
}
