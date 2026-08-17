#![forbid(unsafe_code)]

pub mod proto {
    tonic::include_proto!("mss.v1");
}

pub mod telcompat_proto {
    tonic::include_proto!("protos");
}

pub mod controller;
pub mod convert;
pub mod server;
pub mod telcompat;

pub use controller::{EventSink, MediaPlane, MediaPlaneError, PlaybackSource, SessionController};
pub use server::{serve_on, serve_on_until};
pub use telcompat::TelCompat;
pub use tonic::async_trait;
