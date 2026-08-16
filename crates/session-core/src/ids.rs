use std::fmt;
use std::str::FromStr;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("not a well-formed {kind} identifier: {value}")]
pub struct IdParseError {
    pub kind: &'static str,
    pub value: String,
}

macro_rules! wire_id {
    ($name:ident, $prefix:literal, $kind:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            pub const fn from_raw(raw: u64) -> Self {
                Self(raw)
            }

            pub const fn raw(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}{:012x}", $prefix, self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;

            fn from_str(text: &str) -> Result<Self, IdParseError> {
                text.strip_prefix($prefix)
                    .and_then(|hex| u64::from_str_radix(hex, 16).ok())
                    .map(Self)
                    .ok_or_else(|| IdParseError {
                        kind: $kind,
                        value: text.to_string(),
                    })
            }
        }
    };
}

wire_id!(SessionId, "sess-", "session");
wire_id!(AttachmentId, "att-", "attachment");
wire_id!(PlaybackId, "play-", "playback");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_survive_the_round_trip_through_the_wire_form() {
        let session = SessionId::from_raw(1);
        let attachment = AttachmentId::from_raw(0xdead_beef);
        let playback = PlaybackId::from_raw(u64::MAX);

        assert_eq!(session.to_string(), "sess-000000000001");
        assert_eq!(attachment.to_string(), "att-0000deadbeef");

        assert_eq!(SessionId::from_str(&session.to_string()), Ok(session));
        assert_eq!(
            AttachmentId::from_str(&attachment.to_string()),
            Ok(attachment)
        );
        assert_eq!(PlaybackId::from_str(&playback.to_string()), Ok(playback));
    }

    #[test]
    fn an_identifier_of_the_wrong_kind_is_rejected_rather_than_coerced() {
        let session = SessionId::from_raw(7).to_string();
        assert!(AttachmentId::from_str(&session).is_err());
        assert!(SessionId::from_str("sess-nothex").is_err());
        assert!(SessionId::from_str("").is_err());
        assert!(SessionId::from_str("000000000007").is_err());
    }
}
