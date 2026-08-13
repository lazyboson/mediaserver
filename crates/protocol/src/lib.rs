//! # protocol
//!
//! Serde types for the MSS's consumer-facing wire dialects.
//!
//! `twilio` mirrors what the legacy media gateway emits today (Twilio Media Streams
//! JSON), so existing bot/ASR endpoints migrate with zero changes.
//! `fork_events` mirrors the `uuid_audio_fork … send_text` JSON events
//! that the legacy stream fsm emits (`firstDtmf`, `dtmfResult`, `playbackStop`).
//!
//! The native gRPC interface lives in `proto/` at the repo root and is
//! wired up with tonic/prost in a later milestone.

pub mod fork_events;
pub mod twilio;
