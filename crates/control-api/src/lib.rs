#![forbid(unsafe_code)]

pub mod proto {
    tonic::include_proto!("mss.v1");
}

pub mod telcompat_proto {
    tonic::include_proto!("protos");
}

pub mod auth;
pub mod controller;
pub mod convert;
pub mod server;
pub mod stream;
pub mod telcompat;

pub use auth::AuthPolicy;
pub use controller::{
    EventSink, InlineEgressSink, MediaPlane, MediaPlaneError, ObservationSink, OpenedSession,
    PlaybackSource, SessionController, StreamFrame,
};
pub use server::{serve_authenticated_until, serve_on, serve_on_until, serve_shared_until};
pub use stream::MediaStreamService;
pub use telcompat::TelCompat;
pub use tonic;
pub use tonic::async_trait;
