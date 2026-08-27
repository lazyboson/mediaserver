use crate::{AttachmentId, Capabilities};
use std::collections::BTreeMap;
use std::time::SystemTime;

pub const MIX_TARGET_METADATA_KEY: &str = "mix_target";
pub const MIX_MONITOR_METADATA_KEY: &str = "mix_monitor";
pub const MIX_TARGET_EVERYONE: &str = "all";
pub const MIX_TARGET_OWN: &str = "own";
pub const MIX_MONITOR_INCLUDE: &str = "include";
pub const MIX_MONITOR_EXCLUDE: &str = "exclude";
pub const MIX_SOURCE_METADATA_KEY: &str = "mix_source";
pub const MIX_SOURCE_INJECT: &str = "inject";
pub const MIX_SOURCE_LEG: &str = "leg";
pub const MEMBER_MUTE_METADATA_KEY: &str = "member_mute";
pub const MEMBER_DEAF_METADATA_KEY: &str = "member_deaf";
pub const MEMBER_HOLD_METADATA_KEY: &str = "member_hold";
pub const MEMBER_STATE_TTL_METADATA_KEY: &str = "member_state_ttl_ms";
pub const MEMBER_FLAG_ON: &str = "on";
pub const MEMBER_FLAG_OFF: &str = "off";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MixSource {
    #[default]
    Inject,
    Leg,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MixTarget {
    Own,
    Member(String),
    Everyone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MixRoute {
    pub target: MixTarget,
    pub source: MixSource,
    pub monitor_audible: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemberControl {
    pub mute: Option<bool>,
    pub deaf: Option<bool>,
    pub hold: Option<bool>,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberRouteView {
    pub route: MixRoute,
    pub attachment: Option<AttachmentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberStateView {
    pub conference: String,
    pub members: Vec<String>,
    pub room_session: String,
    pub opened_at: SystemTime,
    pub seated: bool,
    pub mute: bool,
    pub deaf: bool,
    pub hold: bool,
    pub source: MixSource,
    pub routes: Vec<MemberRouteView>,
    pub mute_expires_in_ms: u64,
    pub deaf_expires_in_ms: u64,
    pub hold_expires_in_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MixRouteError {
    #[error("mix_monitor is \"include\" or \"exclude\", not {0:?}")]
    Monitor(String),
    #[error("mix_monitor routes nothing on its own; it qualifies mix_target")]
    MonitorWithoutTarget,
    #[error("mix_target routes injected audio, so the attachment must declare INJECT")]
    WithoutInject,
    #[error("mix_source is \"inject\" or \"leg\", not {0:?}")]
    Source(String),
    #[error("mix_source names where the audio comes from; it needs a mix_target to go to")]
    SourceWithoutTarget,
    #[error("{key} is \"on\" or \"off\", not {value:?}")]
    MemberFlag { key: String, value: String },
    #[error(
        "{key} is how many milliseconds a member flag holds without being refreshed, \
         so it is a whole number and 0 means no lease, not {value:?}"
    )]
    MemberStateTtl { key: String, value: String },
}

impl MixRoute {
    pub fn private() -> MixRoute {
        MixRoute {
            target: MixTarget::Own,
            source: MixSource::Inject,
            monitor_audible: false,
        }
    }

    pub fn from_metadata(
        metadata: &BTreeMap<String, String>,
    ) -> Result<Option<MixRoute>, MixRouteError> {
        let monitor = match metadata.get(MIX_MONITOR_METADATA_KEY).map(String::as_str) {
            None => None,
            Some(MIX_MONITOR_INCLUDE) => Some(true),
            Some(MIX_MONITOR_EXCLUDE) => Some(false),
            Some(other) => return Err(MixRouteError::Monitor(other.to_string())),
        };
        let source = match metadata
            .get(MIX_SOURCE_METADATA_KEY)
            .map(|value| value.trim())
        {
            None => None,
            Some(MIX_SOURCE_INJECT) => Some(MixSource::Inject),
            Some(MIX_SOURCE_LEG) => Some(MixSource::Leg),
            Some(other) => return Err(MixRouteError::Source(other.to_string())),
        };
        let Some(named) = metadata.get(MIX_TARGET_METADATA_KEY) else {
            if monitor.is_some() {
                return Err(MixRouteError::MonitorWithoutTarget);
            }
            if source.is_some() {
                return Err(MixRouteError::SourceWithoutTarget);
            }
            return Ok(None);
        };
        let target = match named.trim() {
            "" | MIX_TARGET_OWN => MixTarget::Own,
            MIX_TARGET_EVERYONE => MixTarget::Everyone,
            member => MixTarget::Member(member.to_string()),
        };
        let monitor_audible = monitor.unwrap_or(!matches!(target, MixTarget::Own));
        Ok(Some(MixRoute {
            target,
            source: source.unwrap_or_default(),
            monitor_audible,
        }))
    }

    pub fn authorize(&self, capabilities: Capabilities) -> Result<(), MixRouteError> {
        if capabilities.contains(Capabilities::INJECT) {
            Ok(())
        } else {
            Err(MixRouteError::WithoutInject)
        }
    }

    pub fn is_private(&self) -> bool {
        self.target == MixTarget::Own
    }

    pub fn is_own_leg(&self) -> bool {
        self.source == MixSource::Leg && !self.is_private()
    }

    pub fn source_name(&self) -> &'static str {
        match self.source {
            MixSource::Inject => MIX_SOURCE_INJECT,
            MixSource::Leg => MIX_SOURCE_LEG,
        }
    }

    pub fn target_name(&self) -> &str {
        match &self.target {
            MixTarget::Own => MIX_TARGET_OWN,
            MixTarget::Everyone => MIX_TARGET_EVERYONE,
            MixTarget::Member(name) => name.as_str(),
        }
    }
}

impl MemberControl {
    pub fn from_metadata(
        metadata: &BTreeMap<String, String>,
    ) -> Result<Option<MemberControl>, MixRouteError> {
        let flag = |key: &str| match metadata.get(key).map(|value| value.trim()) {
            None => Ok(None),
            Some(MEMBER_FLAG_ON) => Ok(Some(true)),
            Some(MEMBER_FLAG_OFF) => Ok(Some(false)),
            Some(other) => Err(MixRouteError::MemberFlag {
                key: key.to_string(),
                value: other.to_string(),
            }),
        };
        let ttl_ms = match metadata
            .get(MEMBER_STATE_TTL_METADATA_KEY)
            .map(|value| value.trim())
        {
            None => None,
            Some(carried) => {
                Some(
                    carried
                        .parse::<u64>()
                        .map_err(|_| MixRouteError::MemberStateTtl {
                            key: MEMBER_STATE_TTL_METADATA_KEY.to_string(),
                            value: carried.to_string(),
                        })?,
                )
            }
        };
        let control = MemberControl {
            mute: flag(MEMBER_MUTE_METADATA_KEY)?,
            deaf: flag(MEMBER_DEAF_METADATA_KEY)?,
            hold: flag(MEMBER_HOLD_METADATA_KEY)?,
            ttl_ms,
        };
        Ok((!control.is_empty()).then_some(control))
    }

    pub fn is_empty(&self) -> bool {
        self.mute.is_none() && self.deaf.is_none() && self.hold.is_none()
    }

    pub fn lease_ms(&self, default_ms: u64) -> u64 {
        self.ttl_ms.unwrap_or(default_ms)
    }

    pub fn releasing(mute: bool, deaf: bool, hold: bool) -> MemberControl {
        MemberControl {
            mute: mute.then_some(false),
            deaf: deaf.then_some(false),
            hold: hold.then_some(false),
            ttl_ms: None,
        }
    }

    pub fn muted(&self) -> bool {
        self.mute.unwrap_or(false)
    }

    pub fn deafened(&self) -> bool {
        self.deaf.unwrap_or(false)
    }

    pub fn held(&self) -> bool {
        self.hold.unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn an_attachment_that_names_no_target_declares_no_route() {
        assert_eq!(MixRoute::from_metadata(&metadata(&[])), Ok(None));
        assert_eq!(
            MixRoute::from_metadata(&metadata(&[("streamSid", "s-1")])),
            Ok(None)
        );
    }

    #[test]
    fn a_named_member_is_a_whisper_the_mixed_track_still_carries() {
        let route = MixRoute::from_metadata(&metadata(&[("mix_target", "agent-7")]))
            .expect("a member name is a valid target")
            .expect("the route is declared");
        assert_eq!(route.target, MixTarget::Member("agent-7".to_string()));
        assert!(route.monitor_audible);
        assert_eq!(route.target_name(), "agent-7");
    }

    #[test]
    fn all_is_the_barge_flip_and_own_is_the_private_playback_default() {
        let barge = MixRoute::from_metadata(&metadata(&[("mix_target", "all")]))
            .unwrap()
            .unwrap();
        assert_eq!(barge.target, MixTarget::Everyone);
        assert!(barge.monitor_audible);
        for private in ["own", "", "   "] {
            let route = MixRoute::from_metadata(&metadata(&[("mix_target", private)]))
                .unwrap()
                .unwrap();
            assert_eq!(
                route,
                MixRoute::private(),
                "{private:?} is private playback"
            );
            assert!(route.is_private());
        }
    }

    #[test]
    fn the_monitor_flag_overrides_the_default_in_both_directions() {
        let quiet = MixRoute::from_metadata(&metadata(&[
            ("mix_target", "agent-7"),
            ("mix_monitor", "exclude"),
        ]))
        .unwrap()
        .unwrap();
        assert!(!quiet.monitor_audible);
        let audited = MixRoute::from_metadata(&metadata(&[
            ("mix_target", "own"),
            ("mix_monitor", "include"),
        ]))
        .unwrap()
        .unwrap();
        assert!(audited.monitor_audible);
    }

    #[test]
    fn a_route_is_refused_by_name_when_the_metadata_makes_no_sense() {
        assert_eq!(
            MixRoute::from_metadata(&metadata(&[
                ("mix_target", "agent-7"),
                ("mix_monitor", "maybe")
            ])),
            Err(MixRouteError::Monitor("maybe".to_string()))
        );
        assert_eq!(
            MixRoute::from_metadata(&metadata(&[("mix_monitor", "include")])),
            Err(MixRouteError::MonitorWithoutTarget)
        );
    }

    #[test]
    fn a_route_may_name_the_members_own_leg_as_the_source_it_moves() {
        let coach = MixRoute::from_metadata(&metadata(&[
            ("mix_target", "agent-7"),
            ("mix_source", "leg"),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(coach.source, MixSource::Leg);
        assert_eq!(coach.source_name(), "leg");
        assert!(coach.is_own_leg());
        let injected = MixRoute::from_metadata(&metadata(&[("mix_target", "agent-7")]))
            .unwrap()
            .unwrap();
        assert_eq!(injected.source, MixSource::Inject);
        assert!(!injected.is_own_leg());
        let back_to_the_room =
            MixRoute::from_metadata(&metadata(&[("mix_target", "own"), ("mix_source", "leg")]))
                .unwrap()
                .unwrap();
        assert!(
            !back_to_the_room.is_own_leg(),
            "a leg routed to its own ear is just an ordinary member"
        );
        assert_eq!(
            MixRoute::from_metadata(&metadata(&[("mix_source", "leg")])),
            Err(MixRouteError::SourceWithoutTarget)
        );
        assert_eq!(
            MixRoute::from_metadata(&metadata(&[
                ("mix_target", "agent-7"),
                ("mix_source", "microphone")
            ])),
            Err(MixRouteError::Source("microphone".to_string()))
        );
    }

    #[test]
    fn member_flags_are_three_independent_verbs_and_absent_means_untouched() {
        assert_eq!(MemberControl::from_metadata(&metadata(&[])), Ok(None));
        assert_eq!(
            MemberControl::from_metadata(&metadata(&[("mix_target", "all")])),
            Ok(None)
        );
        let muted = MemberControl::from_metadata(&metadata(&[("member_mute", "on")]))
            .unwrap()
            .expect("a flag is declared");
        assert_eq!(
            muted,
            MemberControl {
                mute: Some(true),
                deaf: None,
                hold: None,
                ttl_ms: None
            }
        );
        assert!(muted.muted() && !muted.deafened() && !muted.held());
        let released = MemberControl::from_metadata(&metadata(&[
            ("member_hold", "off"),
            ("member_deaf", "off"),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(released.hold, Some(false));
        assert_eq!(released.deaf, Some(false));
        assert_eq!(released.mute, None);
        assert_eq!(
            MemberControl::from_metadata(&metadata(&[("member_mute", "yes")])),
            Err(MixRouteError::MemberFlag {
                key: "member_mute".to_string(),
                value: "yes".to_string()
            })
        );
    }

    #[test]
    fn a_member_flag_may_carry_a_lease_and_absent_still_means_it_holds_forever() {
        let held = MemberControl::from_metadata(&metadata(&[("member_mute", "on")]))
            .unwrap()
            .unwrap();
        assert_eq!(held.ttl_ms, None, "no key is no lease");
        assert_eq!(
            held.lease_ms(0),
            0,
            "and no deployment default leaves it held until a controller says otherwise"
        );
        assert_eq!(
            held.lease_ms(30_000),
            30_000,
            "a deployment default applies to a request that carries none"
        );
        let leased = MemberControl::from_metadata(&metadata(&[
            ("member_mute", "on"),
            ("member_deaf", "on"),
            ("member_state_ttl_ms", " 300 "),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(leased.ttl_ms, Some(300));
        assert_eq!(
            leased.lease_ms(30_000),
            300,
            "one ttl applies to every flag set on in the same request"
        );
        let unleased = MemberControl::from_metadata(&metadata(&[
            ("member_mute", "on"),
            ("member_state_ttl_ms", "0"),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(unleased.ttl_ms, Some(0));
        assert_eq!(
            unleased.lease_ms(30_000),
            0,
            "an explicit 0 means no lease, and outranks the deployment default"
        );
        assert_eq!(
            MemberControl::from_metadata(&metadata(&[("member_state_ttl_ms", "300")])),
            Ok(None),
            "a ttl on its own leases nothing, because it qualifies a flag set on"
        );
        assert_eq!(
            MemberControl::from_metadata(&metadata(&[
                ("member_mute", "on"),
                ("member_state_ttl_ms", "a while")
            ])),
            Err(MixRouteError::MemberStateTtl {
                key: "member_state_ttl_ms".to_string(),
                value: "a while".to_string()
            })
        );
        assert!(
            MemberControl::from_metadata(&metadata(&[
                ("member_mute", "on"),
                ("member_state_ttl_ms", "-1")
            ]))
            .unwrap_err()
            .to_string()
            .contains("member_state_ttl_ms"),
            "a negative lease is refused by the key's name"
        );
    }

    #[test]
    fn a_release_names_only_the_flags_whose_lease_ran_out() {
        let expired = MemberControl::releasing(true, false, true);
        assert_eq!(expired.mute, Some(false));
        assert_eq!(expired.deaf, None);
        assert_eq!(expired.hold, Some(false));
        assert_eq!(expired.ttl_ms, None, "a release leases nothing");
        assert!(!expired.is_empty());
        assert!(MemberControl::releasing(false, false, false).is_empty());
    }

    #[test]
    fn a_sink_only_attachment_may_not_whisper() {
        let route = MixRoute::from_metadata(&metadata(&[("mix_target", "agent-7")]))
            .unwrap()
            .unwrap();
        assert_eq!(
            route.authorize(Capabilities::SINK),
            Err(MixRouteError::WithoutInject)
        );
        assert_eq!(
            route.authorize(Capabilities::SINK | Capabilities::INJECT),
            Ok(())
        );
    }
}
