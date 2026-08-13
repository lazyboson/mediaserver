//! # media-core
//!
//! Sans-IO core of the Media Streaming Service (MSS).
//!
//! Everything in this crate is a pure state machine or pure function:
//! no sockets, no threads, no async, no clocks. Callers (the real-time
//! workers in `mediaserverd`) own I/O and time and feed packets/instants in.
//! This is what makes the media path testable by replaying captured pcaps
//! byte-for-byte — the answer to "it's a media service, we can't see it".
//!
//! Design rules (enforced in review):
//! - No heap allocation on the per-packet path after session setup
//!   (buffers are caller-provided or pooled).
//! - No panics: every parse returns `Result`; malformed network input is
//!   an error value, never an abort (see mediagateway's ptime=0 panic).
//! - Time is always an explicit parameter (`Instant`/tick counts), never
//!   read from the environment.

pub mod dtmf;
pub mod frame;
pub mod g711;
pub mod jitter;
pub mod rtp;

pub use frame::{AudioFormat, Encoding, Track};
