use std::fmt;
use std::ops::BitOr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Capabilities(u8);

impl Capabilities {
    pub const NONE: Capabilities = Capabilities(0);
    pub const SINK: Capabilities = Capabilities(1);
    pub const EVENTS: Capabilities = Capabilities(2);
    pub const INJECT: Capabilities = Capabilities(4);

    pub fn contains(self, wanted: Capabilities) -> bool {
        self.0 & wanted.0 == wanted.0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn bits(self) -> u8 {
        self.0
    }

    pub fn missing_from(self, granted: Capabilities) -> Capabilities {
        Capabilities(self.0 & !granted.0)
    }
}

impl BitOr for Capabilities {
    type Output = Capabilities;

    fn bitor(self, other: Capabilities) -> Capabilities {
        Capabilities(self.0 | other.0)
    }
}

impl fmt::Display for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return f.write_str("NONE");
        }
        let mut first = true;
        for (bit, name) in [
            (Capabilities::SINK, "SINK"),
            (Capabilities::EVENTS, "EVENTS"),
            (Capabilities::INJECT, "INJECT"),
        ] {
            if self.contains(bit) {
                if !first {
                    f.write_str("+")?;
                }
                f.write_str(name)?;
                first = false;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Transport {
    WsTwilio,
    GrpcStream,
    FileS3,
    RtpInline,
}

impl Transport {
    pub fn carries(self) -> Capabilities {
        match self {
            Transport::FileS3 => Capabilities::SINK,
            Transport::WsTwilio | Transport::GrpcStream | Transport::RtpInline => {
                Capabilities::SINK | Capabilities::EVENTS | Capabilities::INJECT
            }
        }
    }

    pub fn has_back_channel(self) -> bool {
        self.carries().contains(Capabilities::EVENTS)
    }
}

impl fmt::Display for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Transport::WsTwilio => "ws-twilio",
            Transport::GrpcStream => "grpc-stream",
            Transport::FileS3 => "file-s3",
            Transport::RtpInline => "rtp-inline",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_capability_set_reports_only_what_it_was_granted() {
        let rtt = Capabilities::SINK | Capabilities::EVENTS;
        assert!(rtt.contains(Capabilities::SINK));
        assert!(rtt.contains(Capabilities::EVENTS));
        assert!(!rtt.contains(Capabilities::INJECT));
        assert!(!rtt.contains(Capabilities::SINK | Capabilities::INJECT));
    }

    #[test]
    fn a_file_sink_transport_offers_no_back_channel_at_all() {
        assert!(!Transport::FileS3.has_back_channel());
        assert!(!Transport::FileS3.carries().contains(Capabilities::EVENTS));
        assert!(!Transport::FileS3.carries().contains(Capabilities::INJECT));
        assert!(Transport::WsTwilio.has_back_channel());
        assert!(Transport::GrpcStream.has_back_channel());
    }

    #[test]
    fn missing_capabilities_name_themselves_for_the_error_message() {
        let granted = Capabilities::SINK;
        let wanted = Capabilities::SINK | Capabilities::INJECT;
        assert_eq!(wanted.missing_from(granted).to_string(), "INJECT");
        assert_eq!(Capabilities::NONE.to_string(), "NONE");
        assert_eq!(
            (Capabilities::SINK | Capabilities::EVENTS | Capabilities::INJECT).to_string(),
            "SINK+EVENTS+INJECT"
        );
    }
}
