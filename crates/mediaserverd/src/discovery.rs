use crate::session_store::{RedisSessionStore, StoreError};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredNode {
    pub node: SocketAddr,
    pub from_tags: Vec<String>,
    pub caller_named: bool,
}

#[derive(Debug, Deserialize)]
struct MappedNode {
    node: String,
    #[serde(default)]
    from_tags: Vec<String>,
    #[serde(default)]
    caller_tag: Option<String>,
}

const JSON_OPENING_BRACE: char = '{';

pub fn parse_mapped_node(value: &str) -> Result<DiscoveredNode, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("the mapped value is empty".to_string());
    }
    if !trimmed.starts_with(JSON_OPENING_BRACE) {
        let node = trimmed
            .parse()
            .map_err(|_| format!("{trimmed} is not an ip:port address"))?;
        return Ok(DiscoveredNode {
            node,
            from_tags: Vec::new(),
            caller_named: false,
        });
    }
    let mapped: MappedNode =
        serde_json::from_str(trimmed).map_err(|error| format!("not a node object: {error}"))?;
    let node = mapped
        .node
        .trim()
        .parse()
        .map_err(|_| format!("node {} is not an ip:port address", mapped.node))?;
    let caller = mapped
        .caller_tag
        .as_deref()
        .map(str::trim)
        .filter(|tag| !tag.is_empty());
    let mut from_tags: Vec<String> = caller.map(str::to_string).into_iter().collect();
    for tag in mapped.from_tags {
        let tag = tag.trim();
        if tag.is_empty() || from_tags.iter().any(|held| held == tag) {
            continue;
        }
        from_tags.push(tag.to_string());
    }
    Ok(DiscoveredNode {
        node,
        caller_named: caller.is_some(),
        from_tags,
    })
}

#[control_api::async_trait]
pub trait NodeMap: Send + Sync + 'static {
    async fn read(&self, key: &str) -> Result<Option<String>, StoreError>;
}

#[control_api::async_trait]
impl NodeMap for RedisSessionStore {
    async fn read(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.read_key(key).await
    }
}

#[derive(Debug, Default)]
pub struct DiscoveryCounters {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub errors: AtomicU64,
}

pub struct NodeDiscovery {
    map: Arc<dyn NodeMap>,
    prefix: String,
    counters: Arc<DiscoveryCounters>,
}

impl NodeDiscovery {
    pub fn new(map: Arc<dyn NodeMap>, prefix: String, counters: Arc<DiscoveryCounters>) -> Self {
        NodeDiscovery {
            map,
            prefix,
            counters,
        }
    }

    pub fn key_for(&self, call_id: &str) -> String {
        format!("{}{call_id}", self.prefix)
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub async fn resolve(&self, call_id: &str) -> Option<DiscoveredNode> {
        let key = self.key_for(call_id);
        match self.map.read(&key).await {
            Ok(Some(value)) => match parse_mapped_node(&value) {
                Ok(found) => {
                    self.counters.hits.fetch_add(1, Ordering::Relaxed);
                    info!(
                        key = %key,
                        node = %found.node,
                        from_tags = ?found.from_tags,
                        caller_named = found.caller_named,
                        "the discovery map names the rtpengine node anchoring this call"
                    );
                    Some(found)
                }
                Err(error) => {
                    self.counters.errors.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        key = %key,
                        value = %value,
                        error = %error,
                        "the discovery map holds something this daemon cannot read; \
                         falling back to the default rtpengine node"
                    );
                    None
                }
            },
            Ok(None) => {
                self.counters.misses.fetch_add(1, Ordering::Relaxed);
                info!(
                    key = %key,
                    "no rtpengine node is mapped to this call; falling back to the default node"
                );
                None
            }
            Err(error) => {
                self.counters.errors.fetch_add(1, Ordering::Relaxed);
                warn!(
                    key = %key,
                    %error,
                    "the discovery map could not be read; falling back to the default \
                     rtpengine node"
                );
                None
            }
        }
    }
}

#[cfg(test)]
pub struct FixedNodeMap {
    value: Option<String>,
    broken: bool,
}

#[cfg(test)]
impl FixedNodeMap {
    pub fn holding(value: &str) -> FixedNodeMap {
        FixedNodeMap {
            value: Some(value.to_string()),
            broken: false,
        }
    }

    pub fn empty() -> FixedNodeMap {
        FixedNodeMap {
            value: None,
            broken: false,
        }
    }

    pub fn unreachable() -> FixedNodeMap {
        FixedNodeMap {
            value: None,
            broken: true,
        }
    }
}

#[cfg(test)]
#[control_api::async_trait]
impl NodeMap for FixedNodeMap {
    async fn read(&self, _key: &str) -> Result<Option<String>, StoreError> {
        if self.broken {
            return Err(StoreError::Backend("connection refused".to_string()));
        }
        Ok(self.value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn discovery(map: FixedNodeMap) -> (NodeDiscovery, Arc<DiscoveryCounters>) {
        let counters = Arc::new(DiscoveryCounters::default());
        (
            NodeDiscovery::new(
                Arc::new(map),
                "mss:node:".to_string(),
                Arc::clone(&counters),
            ),
            counters,
        )
    }

    #[test]
    fn a_bare_host_port_is_a_node_nobody_named_tags_for() {
        let found = parse_mapped_node(" 10.0.0.5:22222 ").expect("a plain address is legal");
        assert_eq!(found.node, "10.0.0.5:22222".parse::<SocketAddr>().unwrap());
        assert!(found.from_tags.is_empty());
        assert!(!found.caller_named);
    }

    #[test]
    fn a_named_caller_tag_leads_the_tag_list_and_claims_a_direction() {
        let found = parse_mapped_node(
            r#"{"node":"10.0.0.5:22222","from_tags":["callee-tag","caller-tag"],
                "caller_tag":"caller-tag"}"#,
        )
        .expect("a node object with a caller is legal");
        assert_eq!(found.from_tags, vec!["caller-tag", "callee-tag"]);
        assert!(found.caller_named);
    }

    #[test]
    fn tags_without_a_named_caller_stay_unattributed() {
        let found = parse_mapped_node(r#"{"node":"10.0.0.5:22222","from_tags":["a","b"]}"#)
            .expect("a node object without a caller is legal");
        assert_eq!(found.from_tags, vec!["a", "b"]);
        assert!(!found.caller_named);
    }

    #[test]
    fn an_empty_caller_tag_names_nobody() {
        let found = parse_mapped_node(r#"{"node":"10.0.0.5:22222","caller_tag":"  "}"#)
            .expect("an empty caller tag is not fatal");
        assert!(found.from_tags.is_empty());
        assert!(!found.caller_named);
    }

    #[test]
    fn a_value_that_is_not_an_address_is_refused() {
        for value in [
            "",
            "  ",
            "rtpengine-1",
            "10.0.0.5",
            r#"{"node":"rtpengine-1"}"#,
            r#"{"tags":["a"]}"#,
            "{not json",
        ] {
            assert!(
                parse_mapped_node(value).is_err(),
                "{value} should not parse as a node"
            );
        }
    }

    #[tokio::test]
    async fn a_mapped_call_counts_one_hit() {
        let (discovery, counters) = discovery(FixedNodeMap::holding("10.0.0.5:22222"));
        let found = discovery.resolve("call-1").await.expect("a mapped call");
        assert_eq!(found.node, "10.0.0.5:22222".parse::<SocketAddr>().unwrap());
        assert_eq!(counters.hits.load(Ordering::Relaxed), 1);
        assert_eq!(counters.misses.load(Ordering::Relaxed), 0);
        assert_eq!(counters.errors.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn an_unmapped_call_counts_one_miss_and_resolves_nothing() {
        let (discovery, counters) = discovery(FixedNodeMap::empty());
        assert!(discovery.resolve("call-1").await.is_none());
        assert_eq!(counters.misses.load(Ordering::Relaxed), 1);
        assert_eq!(counters.hits.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn an_unreadable_map_counts_an_error_and_never_fails_the_call() {
        let (discovery, counters) = discovery(FixedNodeMap::unreachable());
        assert!(discovery.resolve("call-1").await.is_none());
        assert_eq!(counters.errors.load(Ordering::Relaxed), 1);
        assert_eq!(counters.misses.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn a_malformed_value_counts_an_error_not_a_miss() {
        let (discovery, counters) = discovery(FixedNodeMap::holding("rtpengine-1"));
        assert!(discovery.resolve("call-1").await.is_none());
        assert_eq!(counters.errors.load(Ordering::Relaxed), 1);
        assert_eq!(counters.misses.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn the_key_is_the_prefix_and_the_sip_call_id() {
        let (discovery, _) = discovery(FixedNodeMap::empty());
        assert_eq!(discovery.key_for("abc@host"), "mss:node:abc@host");
    }
}
