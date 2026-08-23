use crate::Capabilities;
use std::collections::BTreeMap;

pub const MIX_TARGET_METADATA_KEY: &str = "mix_target";
pub const MIX_MONITOR_METADATA_KEY: &str = "mix_monitor";
pub const MIX_TARGET_EVERYONE: &str = "all";
pub const MIX_TARGET_OWN: &str = "own";
pub const MIX_MONITOR_INCLUDE: &str = "include";
pub const MIX_MONITOR_EXCLUDE: &str = "exclude";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MixTarget {
    Own,
    Member(String),
    Everyone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MixRoute {
    pub target: MixTarget,
    pub monitor_audible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MixRouteError {
    #[error("mix_monitor is \"include\" or \"exclude\", not {0:?}")]
    Monitor(String),
    #[error("mix_monitor routes nothing on its own; it qualifies mix_target")]
    MonitorWithoutTarget,
    #[error("mix_target routes injected audio, so the attachment must declare INJECT")]
    WithoutInject,
}

impl MixRoute {
    pub fn private() -> MixRoute {
        MixRoute {
            target: MixTarget::Own,
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
        let Some(named) = metadata.get(MIX_TARGET_METADATA_KEY) else {
            if monitor.is_some() {
                return Err(MixRouteError::MonitorWithoutTarget);
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

    pub fn target_name(&self) -> &str {
        match &self.target {
            MixTarget::Own => MIX_TARGET_OWN,
            MixTarget::Everyone => MIX_TARGET_EVERYONE,
            MixTarget::Member(name) => name.as_str(),
        }
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
