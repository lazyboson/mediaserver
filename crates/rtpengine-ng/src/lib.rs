//! # rtpengine-ng
//!
//! Sans-IO client for the RTPEngine NG control protocol.
//!
//! The NG protocol is bencoded dictionaries over UDP (or WebSocket),
//! each datagram prefixed with a unique cookie:
//!
//! ```text
//! <cookie> d7:command17:subscribe request7:call-id36:...e
//! ```
//!
//! This crate builds and parses those datagrams; the caller owns the
//! socket, retries, and timeouts. Commands covered: `subscribe request`,
//! `subscribe answer`, `unsubscribe`, `ping` — the tap lifecycle from
//! docs/architecture.md §4.

pub mod bencode;
pub mod commands;

pub use commands::{NgClient, NgError, NgReply, SubscribeRequest};
