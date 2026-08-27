use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const LEASE_TTL: Duration = Duration::from_secs(15);
pub const GROUP_RECORD_TTL: Duration = Duration::from_secs(3 * 60 * 60);
pub const RENEW_EVERY: Duration = Duration::from_secs(5);
pub const ADOPT_EVERY: Duration = Duration::from_secs(10);
pub const MAX_ADOPTIONS_PER_SWEEP: usize = 8;

pub const DEFAULT_NAMESPACE: &str = "mss";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("session store: {0}")]
    Backend(String),
    #[error("session store: {0}")]
    Encoding(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedAttachment {
    pub label: String,
    pub transport: i32,
    pub endpoint: String,
    pub capabilities: Vec<i32>,
    pub selector: Option<String>,
    pub authoritative: bool,
    pub paused: bool,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub format: Option<PersistedFormat>,
    #[serde(default)]
    pub recording: Option<PersistedRecording>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedRecording {
    pub recording_id: String,
    pub owner: String,
    pub recorded_ms: u64,
    pub spilled_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedFormat {
    pub encoding: i32,
    pub sample_rate_hz: u32,
    pub channels: u32,
    pub ptime_ms: u32,
}

pub const INLINE_SESSION_KIND: i32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedSession {
    pub external_id: String,
    pub kind: i32,
    pub call_id: String,
    pub from_tags: Vec<String>,
    pub rtpengine_node: String,
    pub owner: String,
    #[serde(default)]
    pub subscription_tag: String,
    pub attachments: Vec<PersistedAttachment>,
}

impl PersistedSession {
    pub fn is_inline(&self) -> bool {
        self.kind == INLINE_SESSION_KIND
    }

    pub fn is_rebuildable(&self) -> bool {
        !self.is_inline() && !self.call_id.is_empty() && !self.from_tags.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRecord {
    pub recording_id: String,
    pub format: String,
    pub opened_at_unix_ms: u64,
    pub created_by: String,
}

impl GroupRecord {
    pub fn opening_at(
        opened_at: SystemTime,
        recording_id: &str,
        format: &str,
        created_by: &str,
    ) -> GroupRecord {
        GroupRecord {
            recording_id: recording_id.to_string(),
            format: format.to_string(),
            opened_at_unix_ms: opened_at
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            created_by: created_by.to_string(),
        }
    }

    pub fn opened_at(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.opened_at_unix_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupJoin {
    Joined(GroupRecord),
    RecordsAnother(GroupRecord),
    ParticipantHeld { participant: String, owner: String },
}

#[control_api::async_trait]
pub trait SessionStore: Send + Sync + 'static {
    async fn ping(&self) -> Result<(), StoreError>;

    async fn upsert(&self, session: &PersistedSession) -> Result<(), StoreError>;

    async fn forget(&self, external_id: &str) -> Result<(), StoreError>;

    async fn renew(&self, external_id: &str, owner: &str) -> Result<bool, StoreError>;

    async fn release_lease(&self, external_id: &str, owner: &str) -> Result<bool, StoreError>;

    async fn claim_unleased(
        &self,
        owner: &str,
        limit: usize,
    ) -> Result<Vec<PersistedSession>, StoreError>;

    async fn open_or_join_group(
        &self,
        key: &str,
        proposed: &GroupRecord,
        participants: &[String],
        take_over: bool,
    ) -> Result<GroupJoin, StoreError>;

    async fn leave_group(&self, key: &str, participants: &[String]) -> Result<(), StoreError>;
}

pub struct RedisSessionStore {
    client: redis::Client,
    namespace: String,
}

impl RedisSessionStore {
    pub async fn connect(url: &str) -> Result<RedisSessionStore, StoreError> {
        Self::connect_in(url, DEFAULT_NAMESPACE).await
    }

    pub async fn connect_in(url: &str, namespace: &str) -> Result<RedisSessionStore, StoreError> {
        let client =
            redis::Client::open(url).map_err(|error| StoreError::Backend(error.to_string()))?;
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))?;
        redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))?;
        Ok(RedisSessionStore {
            client,
            namespace: namespace.to_string(),
        })
    }

    fn session_key(&self, external_id: &str) -> String {
        format!("{}:session:{external_id}", self.namespace)
    }

    fn lease_key(&self, external_id: &str) -> String {
        format!("{}:lease:{external_id}", self.namespace)
    }

    fn index_key(&self) -> String {
        format!("{}:sessions", self.namespace)
    }

    fn group_key(&self, group: &str) -> String {
        format!("{}:group:{group}", self.namespace)
    }

    fn group_members_key(&self, group: &str) -> String {
        format!("{}:group:{group}:members", self.namespace)
    }

    pub async fn read_key(&self, key: &str) -> Result<Option<String>, StoreError> {
        let mut connection = self.connection().await?;
        redis::cmd("GET")
            .arg(key)
            .query_async::<Option<String>>(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, StoreError> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))
    }
}

#[control_api::async_trait]
impl SessionStore for RedisSessionStore {
    async fn ping(&self) -> Result<(), StoreError> {
        let mut connection = self.connection().await?;
        redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|error| StoreError::Backend(error.to_string()))
    }

    async fn upsert(&self, session: &PersistedSession) -> Result<(), StoreError> {
        let body = serde_json::to_string(session)
            .map_err(|error| StoreError::Encoding(error.to_string()))?;
        let mut connection = self.connection().await?;
        redis::pipe()
            .atomic()
            .set(self.session_key(&session.external_id), body)
            .ignore()
            .sadd(self.index_key(), &session.external_id)
            .ignore()
            .cmd("SET")
            .arg(self.lease_key(&session.external_id))
            .arg(&session.owner)
            .arg("NX")
            .arg("EX")
            .arg(LEASE_TTL.as_secs())
            .ignore()
            .query_async::<()>(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))
    }

    async fn forget(&self, external_id: &str) -> Result<(), StoreError> {
        let mut connection = self.connection().await?;
        redis::pipe()
            .atomic()
            .del(self.session_key(external_id))
            .ignore()
            .del(self.lease_key(external_id))
            .ignore()
            .srem(self.index_key(), external_id)
            .ignore()
            .query_async::<()>(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))
    }

    async fn renew(&self, external_id: &str, owner: &str) -> Result<bool, StoreError> {
        let mut connection = self.connection().await?;
        let held: Option<String> = redis::cmd("GET")
            .arg(self.lease_key(external_id))
            .query_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))?;
        if held.as_deref() != Some(owner) {
            return Ok(false);
        }
        redis::cmd("SET")
            .arg(self.lease_key(external_id))
            .arg(owner)
            .arg("EX")
            .arg(LEASE_TTL.as_secs())
            .query_async::<()>(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))?;
        Ok(true)
    }

    async fn release_lease(&self, external_id: &str, owner: &str) -> Result<bool, StoreError> {
        let mut connection = self.connection().await?;
        let held: Option<String> = redis::cmd("GET")
            .arg(self.lease_key(external_id))
            .query_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))?;
        if held.as_deref() != Some(owner) {
            return Ok(false);
        }
        redis::cmd("DEL")
            .arg(self.lease_key(external_id))
            .query_async::<i64>(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))?;
        Ok(true)
    }

    async fn claim_unleased(
        &self,
        owner: &str,
        limit: usize,
    ) -> Result<Vec<PersistedSession>, StoreError> {
        let mut connection = self.connection().await?;
        let known: Vec<String> = redis::cmd("SMEMBERS")
            .arg(self.index_key())
            .query_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))?;

        let mut claimed = Vec::new();
        for external_id in known {
            if claimed.len() >= limit {
                break;
            }
            let won: Option<String> = redis::cmd("SET")
                .arg(self.lease_key(&external_id))
                .arg(owner)
                .arg("NX")
                .arg("EX")
                .arg(LEASE_TTL.as_secs())
                .query_async(&mut connection)
                .await
                .map_err(|error| StoreError::Backend(error.to_string()))?;
            if won.is_none() {
                continue;
            }
            let body: Option<String> = redis::cmd("GET")
                .arg(self.session_key(&external_id))
                .query_async(&mut connection)
                .await
                .map_err(|error| StoreError::Backend(error.to_string()))?;
            match body {
                Some(body) => match serde_json::from_str::<PersistedSession>(&body) {
                    Ok(mut session) => {
                        session.owner = owner.to_string();
                        claimed.push(session);
                    }
                    Err(error) => return Err(StoreError::Encoding(error.to_string())),
                },
                None => {
                    let _: Result<(), _> = redis::cmd("SREM")
                        .arg(self.index_key())
                        .arg(&external_id)
                        .query_async::<()>(&mut connection)
                        .await;
                }
            }
        }
        Ok(claimed)
    }

    async fn open_or_join_group(
        &self,
        key: &str,
        proposed: &GroupRecord,
        participants: &[String],
        take_over: bool,
    ) -> Result<GroupJoin, StoreError> {
        let body = serde_json::to_string(proposed)
            .map_err(|error| StoreError::Encoding(error.to_string()))?;
        let record_key = self.group_key(key);
        let members_key = self.group_members_key(key);
        let ttl = GROUP_RECORD_TTL.as_secs();
        let mut connection = self.connection().await?;
        let won: Option<String> = redis::cmd("SET")
            .arg(&record_key)
            .arg(&body)
            .arg("NX")
            .arg("EX")
            .arg(ttl)
            .query_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))?;
        let record = match won {
            Some(_) => proposed.clone(),
            None => {
                let held: Option<String> = redis::cmd("GET")
                    .arg(&record_key)
                    .query_async(&mut connection)
                    .await
                    .map_err(|error| StoreError::Backend(error.to_string()))?;
                match held {
                    Some(held) => serde_json::from_str::<GroupRecord>(&held)
                        .map_err(|error| StoreError::Encoding(error.to_string()))?,
                    None => {
                        redis::cmd("SET")
                            .arg(&record_key)
                            .arg(&body)
                            .arg("EX")
                            .arg(ttl)
                            .query_async::<()>(&mut connection)
                            .await
                            .map_err(|error| StoreError::Backend(error.to_string()))?;
                        proposed.clone()
                    }
                }
            }
        };
        if record.recording_id != proposed.recording_id || record.format != proposed.format {
            return Ok(GroupJoin::RecordsAnother(record));
        }
        let mut seated: Vec<&String> = Vec::new();
        for participant in participants {
            let seat_taken = if take_over {
                redis::cmd("HSET")
                    .arg(&members_key)
                    .arg(participant)
                    .arg(&proposed.created_by)
                    .query_async::<i64>(&mut connection)
                    .await
                    .map_err(|error| StoreError::Backend(error.to_string()))?;
                true
            } else {
                let added: i64 = redis::cmd("HSETNX")
                    .arg(&members_key)
                    .arg(participant)
                    .arg(&proposed.created_by)
                    .query_async(&mut connection)
                    .await
                    .map_err(|error| StoreError::Backend(error.to_string()))?;
                added == 1
            };
            if seat_taken {
                seated.push(participant);
                continue;
            }
            for taken_back in seated {
                let _: Result<i64, _> = redis::cmd("HDEL")
                    .arg(&members_key)
                    .arg(taken_back)
                    .query_async(&mut connection)
                    .await;
            }
            let owner: Option<String> = redis::cmd("HGET")
                .arg(&members_key)
                .arg(participant)
                .query_async(&mut connection)
                .await
                .map_err(|error| StoreError::Backend(error.to_string()))?;
            return Ok(GroupJoin::ParticipantHeld {
                participant: participant.clone(),
                owner: owner.unwrap_or_default(),
            });
        }
        let _: Result<(), _> = redis::pipe()
            .expire(&record_key, ttl as i64)
            .ignore()
            .expire(&members_key, ttl as i64)
            .ignore()
            .query_async::<()>(&mut connection)
            .await;
        Ok(GroupJoin::Joined(record))
    }

    async fn leave_group(&self, key: &str, participants: &[String]) -> Result<(), StoreError> {
        let record_key = self.group_key(key);
        let members_key = self.group_members_key(key);
        let mut connection = self.connection().await?;
        for participant in participants {
            redis::cmd("HDEL")
                .arg(&members_key)
                .arg(participant)
                .query_async::<i64>(&mut connection)
                .await
                .map_err(|error| StoreError::Backend(error.to_string()))?;
        }
        let remaining: i64 = redis::cmd("HLEN")
            .arg(&members_key)
            .query_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))?;
        if remaining == 0 {
            let _: Result<(), _> = redis::pipe()
                .del(&members_key)
                .ignore()
                .del(&record_key)
                .ignore()
                .query_async::<()>(&mut connection)
                .await;
        }
        Ok(())
    }
}

#[cfg(test)]
struct HeldGroup {
    record: GroupRecord,
    members: BTreeMap<String, String>,
}

#[cfg(test)]
#[derive(Default)]
pub struct MemorySessionStore {
    sessions: std::sync::Mutex<BTreeMap<String, PersistedSession>>,
    leases: std::sync::Mutex<BTreeMap<String, String>>,
    groups: std::sync::Mutex<BTreeMap<String, HeldGroup>>,
    unreachable: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl MemorySessionStore {
    pub fn set_unreachable(&self, unreachable: bool) {
        self.unreachable
            .store(unreachable, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn expire_lease(&self, external_id: &str) {
        self.leases.lock().unwrap().remove(external_id);
    }

    pub fn stored(&self) -> Vec<PersistedSession> {
        self.sessions.lock().unwrap().values().cloned().collect()
    }

    pub fn lease_holder(&self, external_id: &str) -> Option<String> {
        self.leases.lock().unwrap().get(external_id).cloned()
    }

    pub fn group_record(&self, key: &str) -> Option<GroupRecord> {
        self.groups
            .lock()
            .unwrap()
            .get(key)
            .map(|held| held.record.clone())
    }

    pub fn group_members(&self, key: &str) -> BTreeMap<String, String> {
        self.groups
            .lock()
            .unwrap()
            .get(key)
            .map(|held| held.members.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
#[control_api::async_trait]
impl SessionStore for MemorySessionStore {
    async fn ping(&self) -> Result<(), StoreError> {
        if self.unreachable.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(StoreError::Backend("connection refused".to_string()));
        }
        Ok(())
    }

    async fn upsert(&self, session: &PersistedSession) -> Result<(), StoreError> {
        self.sessions
            .lock()
            .unwrap()
            .insert(session.external_id.clone(), session.clone());
        self.leases
            .lock()
            .unwrap()
            .entry(session.external_id.clone())
            .or_insert_with(|| session.owner.clone());
        Ok(())
    }

    async fn forget(&self, external_id: &str) -> Result<(), StoreError> {
        self.sessions.lock().unwrap().remove(external_id);
        self.leases.lock().unwrap().remove(external_id);
        Ok(())
    }

    async fn renew(&self, external_id: &str, owner: &str) -> Result<bool, StoreError> {
        match self.leases.lock().unwrap().get(external_id) {
            Some(held) if held == owner => Ok(true),
            _ => Ok(false),
        }
    }

    async fn release_lease(&self, external_id: &str, owner: &str) -> Result<bool, StoreError> {
        let mut leases = self.leases.lock().unwrap();
        match leases.get(external_id) {
            Some(held) if held == owner => {
                leases.remove(external_id);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn claim_unleased(
        &self,
        owner: &str,
        limit: usize,
    ) -> Result<Vec<PersistedSession>, StoreError> {
        let sessions = self.sessions.lock().unwrap();
        let mut leases = self.leases.lock().unwrap();
        let mut claimed = Vec::new();
        for (external_id, session) in sessions.iter() {
            if claimed.len() >= limit {
                break;
            }
            if leases.contains_key(external_id) {
                continue;
            }
            leases.insert(external_id.clone(), owner.to_string());
            let mut adopted = session.clone();
            adopted.owner = owner.to_string();
            claimed.push(adopted);
        }
        Ok(claimed)
    }

    async fn open_or_join_group(
        &self,
        key: &str,
        proposed: &GroupRecord,
        participants: &[String],
        take_over: bool,
    ) -> Result<GroupJoin, StoreError> {
        if self.unreachable.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(StoreError::Backend("connection refused".to_string()));
        }
        let mut groups = self.groups.lock().unwrap();
        let held = groups.entry(key.to_string()).or_insert_with(|| HeldGroup {
            record: proposed.clone(),
            members: BTreeMap::new(),
        });
        if held.record.recording_id != proposed.recording_id
            || held.record.format != proposed.format
        {
            return Ok(GroupJoin::RecordsAnother(held.record.clone()));
        }
        for participant in participants {
            if take_over {
                continue;
            }
            if let Some(owner) = held.members.get(participant) {
                return Ok(GroupJoin::ParticipantHeld {
                    participant: participant.clone(),
                    owner: owner.clone(),
                });
            }
        }
        for participant in participants {
            held.members
                .insert(participant.clone(), proposed.created_by.clone());
        }
        Ok(GroupJoin::Joined(held.record.clone()))
    }

    async fn leave_group(&self, key: &str, participants: &[String]) -> Result<(), StoreError> {
        if self.unreachable.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(StoreError::Backend("connection refused".to_string()));
        }
        let mut groups = self.groups.lock().unwrap();
        let Some(held) = groups.get_mut(key) else {
            return Ok(());
        };
        for participant in participants {
            held.members.remove(participant);
        }
        if held.members.is_empty() {
            groups.remove(key);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(external_id: &str, owner: &str) -> PersistedSession {
        PersistedSession {
            external_id: external_id.to_string(),
            kind: 1,
            call_id: "call-abc".to_string(),
            from_tags: vec!["from-a".to_string(), "from-b".to_string()],
            rtpengine_node: "10.0.0.5:22222".to_string(),
            owner: owner.to_string(),
            subscription_tag: "tap-tag-1".to_string(),
            attachments: vec![PersistedAttachment {
                label: "rtt".to_string(),
                transport: 1,
                endpoint: "wss-endpoint".to_string(),
                capabilities: vec![1, 2],
                selector: None,
                authoritative: true,
                paused: false,
                group: String::new(),
                format: Some(PersistedFormat {
                    encoding: 3,
                    sample_rate_hz: 16_000,
                    channels: 1,
                    ptime_ms: 20,
                }),
                recording: None,
                metadata: BTreeMap::new(),
            }],
        }
    }

    #[test]
    fn a_record_written_before_groups_existed_still_decodes() {
        let stored = concat!(
            r#"{"external_id":"req-1","kind":1,"call_id":"call-abc","#,
            r#""from_tags":["from-a"],"rtpengine_node":"10.0.0.5:22222","#,
            r#""owner":"pod-a","attachments":[{"label":"rec","transport":3,"#,
            r#""endpoint":"acct/rec.wav","capabilities":[1],"selector":null,"#,
            r#""authoritative":false,"paused":false,"metadata":{}}]}"#
        );
        let decoded: PersistedSession = serde_json::from_str(stored).unwrap();
        assert_eq!(decoded.attachments[0].group, "");
        assert_eq!(
            decoded.subscription_tag, "",
            "a record written before the tap tag was persisted must still decode"
        );
        assert_eq!(
            decoded.attachments[0].format, None,
            "a record written before the negotiated format was persisted must still decode"
        );
        assert_eq!(
            decoded.attachments[0].recording, None,
            "a record written before recordings were journalled must still decode"
        );
    }

    #[test]
    fn a_recordings_spill_journal_survives_a_json_roundtrip() {
        let mut written = session("req-1", "pod-a");
        written.attachments[0].recording = Some(PersistedRecording {
            recording_id: "rec-9".to_string(),
            owner: "pod-a".to_string(),
            recorded_ms: 61_000,
            spilled_ms: 60_000,
        });
        let body = serde_json::to_string(&written).unwrap();
        let decoded: PersistedSession = serde_json::from_str(&body).unwrap();
        assert_eq!(decoded, written);
        let journal = decoded.attachments[0].recording.clone().unwrap();
        assert_eq!(journal.recorded_ms - journal.spilled_ms, 1_000);
    }

    #[test]
    fn the_negotiated_format_survives_a_json_roundtrip() {
        let written = session("req-1", "pod-a");
        let body = serde_json::to_string(&written).unwrap();
        let decoded: PersistedSession = serde_json::from_str(&body).unwrap();
        assert_eq!(decoded, written);
        assert_eq!(
            decoded.attachments[0].format,
            Some(PersistedFormat {
                encoding: 3,
                sample_rate_hz: 16_000,
                channels: 1,
                ptime_ms: 20,
            })
        );
    }

    #[tokio::test]
    async fn persisting_does_not_take_back_a_lease_another_pod_now_holds() {
        let store = MemorySessionStore::default();
        store.upsert(&session("req-1", "pod-a")).await.unwrap();
        store.expire_lease("req-1");
        let adopted = store.claim_unleased("pod-b", 8).await.unwrap();
        assert_eq!(adopted.len(), 1);

        store.upsert(&session("req-1", "pod-a")).await.unwrap();

        assert_eq!(
            store.lease_holder("req-1").as_deref(),
            Some("pod-b"),
            "a partitioned pod's persist tick stole the lease back and both pods kept tapping"
        );
    }

    #[tokio::test]
    async fn the_holder_reacquires_a_lease_that_merely_expired() {
        let store = MemorySessionStore::default();
        store.upsert(&session("req-1", "pod-a")).await.unwrap();
        store.expire_lease("req-1");

        store.upsert(&session("req-1", "pod-a")).await.unwrap();

        assert!(
            store.renew("req-1", "pod-a").await.unwrap(),
            "an unclaimed expiry must not read as split brain"
        );
    }

    #[test]
    fn the_subscription_tag_survives_a_json_roundtrip() {
        let held = session("req-1", "pod-a");
        let body = serde_json::to_string(&held).unwrap();
        let decoded: PersistedSession = serde_json::from_str(&body).unwrap();
        assert_eq!(decoded.subscription_tag, "tap-tag-1");
        assert_eq!(decoded, held);
    }

    #[tokio::test]
    async fn a_session_survives_as_everything_needed_to_rebuild_it() {
        let store = MemorySessionStore::default();
        let held = session("req-1", "pod-a");
        store.upsert(&held).await.unwrap();

        let stored = store.stored();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0], held);
        assert!(
            stored[0].is_rebuildable(),
            "without call-id and tags another pod cannot re-tap"
        );
    }

    #[tokio::test]
    async fn a_session_whose_pod_still_holds_the_lease_is_not_adoptable() {
        let store = MemorySessionStore::default();
        store.upsert(&session("req-1", "pod-a")).await.unwrap();

        let stolen = store
            .claim_unleased("pod-b", MAX_ADOPTIONS_PER_SWEEP)
            .await
            .unwrap();

        assert!(stolen.is_empty(), "a live pod's session was taken from it");
        assert_eq!(store.lease_holder("req-1").as_deref(), Some("pod-a"));
    }

    #[tokio::test]
    async fn an_expired_lease_lets_exactly_one_other_pod_adopt() {
        let store = MemorySessionStore::default();
        store.upsert(&session("req-1", "pod-a")).await.unwrap();
        store.expire_lease("req-1");

        let first = store.claim_unleased("pod-b", 8).await.unwrap();
        let second = store.claim_unleased("pod-c", 8).await.unwrap();

        assert_eq!(first.len(), 1);
        assert_eq!(first[0].owner, "pod-b", "the claim must rewrite ownership");
        assert!(second.is_empty(), "two pods adopted the same session");
        assert_eq!(store.lease_holder("req-1").as_deref(), Some("pod-b"));
    }

    #[tokio::test]
    async fn renewal_only_succeeds_for_the_pod_that_holds_it() {
        let store = MemorySessionStore::default();
        store.upsert(&session("req-1", "pod-a")).await.unwrap();

        assert!(store.renew("req-1", "pod-a").await.unwrap());
        assert!(
            !store.renew("req-1", "pod-b").await.unwrap(),
            "a pod renewed a lease it does not hold"
        );

        store.expire_lease("req-1");
        assert!(
            !store.renew("req-1", "pod-a").await.unwrap(),
            "renewal revived a lease that had already expired"
        );
    }

    #[tokio::test]
    async fn a_destroyed_session_leaves_nothing_to_adopt() {
        let store = MemorySessionStore::default();
        store.upsert(&session("req-1", "pod-a")).await.unwrap();
        store.forget("req-1").await.unwrap();

        assert!(store.stored().is_empty());
        assert!(store.claim_unleased("pod-b", 8).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_sweep_adopts_no_more_than_it_was_asked_to() {
        let store = MemorySessionStore::default();
        for index in 0..5 {
            store
                .upsert(&session(&format!("req-{index}"), "pod-a"))
                .await
                .unwrap();
            store.expire_lease(&format!("req-{index}"));
        }

        let claimed = store.claim_unleased("pod-b", 2).await.unwrap();

        assert_eq!(
            claimed.len(),
            2,
            "a pod must not adopt every orphan at once"
        );
    }

    #[test]
    fn a_session_without_call_identity_is_not_rebuildable() {
        let mut telcompat_shaped = session("req-1", "pod-a");
        telcompat_shaped.call_id = String::new();
        telcompat_shaped.from_tags.clear();
        assert!(!telcompat_shaped.is_rebuildable());
    }
}
