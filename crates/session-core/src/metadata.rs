use std::collections::HashMap;

pub const RESERVED_METADATA_PREFIX: &str = "mss.";
pub const RESUME_MS_METADATA_KEY: &str = "mss.recording.resumeMs";
pub const SPILL_OWNER_METADATA_KEY: &str = "mss.recording.spillOwner";

pub fn reserved_metadata_key(metadata: &HashMap<String, String>) -> Option<&str> {
    metadata
        .keys()
        .filter(|key| key.starts_with(RESERVED_METADATA_PREFIX))
        .map(String::as_str)
        .min()
}

pub fn reserved_metadata_refusal(metadata: &HashMap<String, String>) -> Option<String> {
    reserved_metadata_key(metadata).map(|key| {
        format!(
            "metadata key {key} is reserved: the {RESERVED_METADATA_PREFIX} prefix is how \
             mediaserverd's own session registry passes state to itself when it rebuilds an \
             adopted attachment, and a client that could set it could pad a recording with \
             silence or take another pod's recording-group seat"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn carrying(keys: &[&str]) -> HashMap<String, String> {
        keys.iter()
            .map(|key| (key.to_string(), "value".to_string()))
            .collect()
    }

    #[test]
    fn the_two_keys_the_keeper_derives_are_under_the_reserved_prefix() {
        assert!(RESUME_MS_METADATA_KEY.starts_with(RESERVED_METADATA_PREFIX));
        assert!(SPILL_OWNER_METADATA_KEY.starts_with(RESERVED_METADATA_PREFIX));
    }

    #[test]
    fn every_client_facing_metadata_key_this_api_documents_stays_allowed() {
        let documented = carrying(&[
            "accountId",
            "streamSid",
            "callSid",
            "recordId",
            "fileFormat",
            "recordingChannels",
            "sipCallId",
            "callerTag",
            crate::mix::MIX_TARGET_METADATA_KEY,
            crate::mix::MIX_MONITOR_METADATA_KEY,
            crate::mix::MIX_SOURCE_METADATA_KEY,
            crate::mix::MEMBER_MUTE_METADATA_KEY,
            crate::mix::MEMBER_DEAF_METADATA_KEY,
            crate::mix::MEMBER_HOLD_METADATA_KEY,
        ]);
        assert_eq!(reserved_metadata_key(&documented), None);
        assert_eq!(reserved_metadata_refusal(&documented), None);
    }

    #[test]
    fn a_reserved_key_is_named_in_the_refusal_and_the_reason_is_given() {
        let refusal = reserved_metadata_refusal(&carrying(&["accountId", RESUME_MS_METADATA_KEY]))
            .expect("a reserved key must be refused");
        assert!(refusal.contains(RESUME_MS_METADATA_KEY), "{refusal}");
        assert!(refusal.contains("reserved"), "{refusal}");
    }

    #[test]
    fn the_named_key_is_stable_when_a_caller_sends_several() {
        let sent = carrying(&[
            SPILL_OWNER_METADATA_KEY,
            RESUME_MS_METADATA_KEY,
            "accountId",
        ]);
        let named = reserved_metadata_key(&sent);
        assert_eq!(
            named,
            Some(RESUME_MS_METADATA_KEY),
            "a hash map has no order, so the refusal names the lexicographically first \
             reserved key or the message is not reproducible"
        );
        let reordered = carrying(&[RESUME_MS_METADATA_KEY, SPILL_OWNER_METADATA_KEY]);
        assert_eq!(
            reserved_metadata_key(&reordered),
            named,
            "the same two keys must always produce the same message"
        );
    }

    #[test]
    fn a_key_that_merely_mentions_the_prefix_later_is_not_reserved() {
        assert_eq!(reserved_metadata_key(&carrying(&["x-mss.recording"])), None);
    }
}
