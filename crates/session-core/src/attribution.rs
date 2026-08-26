#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Attribution {
    #[default]
    Explicit,
    Inferred,
    Unknown,
}

impl Attribution {
    pub fn as_str(&self) -> &'static str {
        match self {
            Attribution::Explicit => "explicit",
            Attribution::Inferred => "inferred",
            Attribution::Unknown => "unknown",
        }
    }

    pub fn parse(name: &str) -> Option<Attribution> {
        match name {
            "explicit" => Some(Attribution::Explicit),
            "inferred" => Some(Attribution::Inferred),
            "unknown" => Some(Attribution::Unknown),
            _ => None,
        }
    }

    pub fn names_a_direction(&self) -> bool {
        !matches!(self, Attribution::Unknown)
    }
}

impl std::fmt::Display for Attribution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_state_round_trips_through_its_wire_name() {
        for state in [
            Attribution::Explicit,
            Attribution::Inferred,
            Attribution::Unknown,
        ] {
            assert_eq!(Attribution::parse(state.as_str()), Some(state));
        }
    }

    #[test]
    fn an_unrecognised_name_is_not_silently_an_explicit_claim() {
        assert_eq!(Attribution::parse("probably"), None);
        assert_eq!(Attribution::parse(""), None);
    }

    #[test]
    fn only_unknown_refuses_to_name_a_direction() {
        assert!(Attribution::Explicit.names_a_direction());
        assert!(Attribution::Inferred.names_a_direction());
        assert!(!Attribution::Unknown.names_a_direction());
    }

    #[test]
    fn the_default_is_what_an_explicitly_tagged_session_carries() {
        assert_eq!(Attribution::default(), Attribution::Explicit);
    }
}
