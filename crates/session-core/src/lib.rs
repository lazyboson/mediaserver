#![forbid(unsafe_code)]

pub mod attribution;
pub mod capability;
pub mod event;
pub mod ids;
pub mod mix;
pub mod registry;

pub use attribution::Attribution;
pub use capability::{Capabilities, Transport};
pub use event::{ConsumerEvent, EventKind, MediaEvent, Observation};
pub use ids::{AttachmentId, IdParseError, PlaybackId, SessionId};
pub use mix::{
    MemberControl, MemberRouteView, MemberStateView, MixRoute, MixRouteError, MixSource, MixTarget,
};
pub use registry::{
    AttachSpec, AttachmentUpdate, AttachmentView, ControlError, CreateSession, PlaybackSpec,
    SessionKind, SessionRegistry, SessionView, StoppedPlayback, TrackSelector,
    DEFAULT_MAX_ATTACHMENTS,
};
