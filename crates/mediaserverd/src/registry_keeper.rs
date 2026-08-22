use crate::session_store::{
    PersistedAttachment, PersistedSession, SessionStore, ADOPT_EVERY, MAX_ADOPTIONS_PER_SWEEP,
    RENEW_EVERY,
};
use control_api::convert::{capabilities_wire, session_kind_wire, track_name, transport_wire};
use control_api::proto;
use control_api::proto::media_control_server::MediaControl;
use control_api::tonic::{self, Request};
use control_api::SessionController;
use session_core::{AttachmentView, SessionView, TrackSelector};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

#[derive(Default)]
pub struct KeeperCounters {
    pub persisted: AtomicU64,
    pub renewed: AtomicU64,
    pub lost: AtomicU64,
    pub adopted: AtomicU64,
    pub unrebuildable: AtomicU64,
    pub released: AtomicU64,
    pub failed: AtomicU64,
    pub grouped_not_adopted: AtomicU64,
}

pub struct RegistryKeeper {
    controller: Arc<SessionController>,
    store: Arc<dyn SessionStore>,
    owner: String,
    counters: Arc<KeeperCounters>,
    persisted_here: Mutex<BTreeSet<String>>,
}

impl RegistryKeeper {
    pub fn new(
        controller: Arc<SessionController>,
        store: Arc<dyn SessionStore>,
        owner: impl Into<String>,
    ) -> RegistryKeeper {
        RegistryKeeper {
            controller,
            store,
            owner: owner.into(),
            counters: Arc::new(KeeperCounters::default()),
            persisted_here: Mutex::new(BTreeSet::new()),
        }
    }

    pub fn counters(&self) -> Arc<KeeperCounters> {
        Arc::clone(&self.counters)
    }

    pub async fn tick(&self) {
        self.persist_and_renew().await;
        self.adopt_orphans().await;
    }

    pub async fn run(self) {
        let mut renew = tokio::time::interval(RENEW_EVERY);
        let mut adopt = tokio::time::interval(ADOPT_EVERY);
        info!(owner = %self.owner, "session registry keeper started");
        self.tick().await;
        loop {
            tokio::select! {
                _ = renew.tick() => self.persist_and_renew().await,
                _ = adopt.tick() => self.adopt_orphans().await,
            }
        }
    }

    async fn persist_and_renew(&self) {
        let mut live = BTreeSet::new();
        for (session, attachments) in self.controller.snapshot() {
            live.insert(session.external_id.clone());
            let persisted = self.persisted_from(&session, &attachments);
            let external_id = persisted.external_id.clone();
            if let Err(error) = self.store.upsert(&persisted).await {
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
                warn!(%external_id, %error, "could not persist this session");
                continue;
            }
            self.counters.persisted.fetch_add(1, Ordering::Relaxed);

            match self.store.renew(&external_id, &self.owner).await {
                Ok(true) => {
                    self.counters.renewed.fetch_add(1, Ordering::Relaxed);
                }
                Ok(false) => {
                    self.counters.lost.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        %external_id,
                        owner = %self.owner,
                        "another pod holds this session's lease; it may now be tapped twice"
                    );
                }
                Err(error) => {
                    self.counters.failed.fetch_add(1, Ordering::Relaxed);
                    warn!(%external_id, %error, "lease renewal failed");
                }
            }
        }
        self.release_gone(live).await;
    }

    async fn release_gone(&self, live: BTreeSet<String>) {
        let gone: Vec<String> = {
            let mut mine = self.persisted_here.lock().unwrap();
            let gone = mine.difference(&live).cloned().collect();
            *mine = live;
            gone
        };
        for external_id in gone {
            match self.store.forget(&external_id).await {
                Ok(()) => {
                    self.counters.released.fetch_add(1, Ordering::Relaxed);
                    info!(%external_id, "session ended here; released from the registry");
                }
                Err(error) => {
                    self.counters.failed.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        %external_id,
                        %error,
                        "could not release an ended session; another pod may adopt a call \
                         that is already over"
                    );
                }
            }
        }
    }

    async fn adopt_orphans(&self) {
        let claimed = match self
            .store
            .claim_unleased(&self.owner, MAX_ADOPTIONS_PER_SWEEP)
            .await
        {
            Ok(claimed) => claimed,
            Err(error) => {
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
                warn!(%error, "could not look for orphaned sessions");
                return;
            }
        };

        for session in claimed {
            if self.controller.holds_external_id(&session.external_id) {
                continue;
            }
            if !session.is_rebuildable() {
                self.counters.unrebuildable.fetch_add(1, Ordering::Relaxed);
                warn!(
                    external_id = %session.external_id,
                    "adopted a session with no call identity; it cannot be re-tapped, \
                     so it is being released rather than half-restored"
                );
                let _ = self.store.forget(&session.external_id).await;
                continue;
            }
            match self.rebuild(&session).await {
                Ok(()) => {
                    self.counters.adopted.fetch_add(1, Ordering::Relaxed);
                    info!(
                        external_id = %session.external_id,
                        call_id = %session.call_id,
                        attachments = session.attachments.len(),
                        "adopted an orphaned session and re-established its tap"
                    );
                }
                Err(error) => {
                    self.counters.failed.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        external_id = %session.external_id,
                        %error,
                        "could not rebuild an adopted session; releasing it for another pod"
                    );
                    let _ = self.store.forget(&session.external_id).await;
                }
            }
        }
    }

    async fn rebuild(&self, session: &PersistedSession) -> Result<(), tonic::Status> {
        self.controller
            .create_session(Request::new(proto::CreateSessionRequest {
                external_id: session.external_id.clone(),
                kind: session.kind,
                call_id: session.call_id.clone(),
                from_tags: session.from_tags.clone(),
                rtpengine_node: session.rtpengine_node.clone(),
                mix: false,
                idempotency_key: format!("adopt-{}", session.external_id),
            }))
            .await?;

        for attachment in &session.attachments {
            if !attachment.group.is_empty() {
                self.counters
                    .grouped_not_adopted
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    external_id = %session.external_id,
                    label = %attachment.label,
                    group = %attachment.group,
                    "a recording group lives in one pod's memory, so this member is not \
                     restored on the adopting pod; the group's other members keep recording \
                     where they are and this participant's file ends at the pod that died"
                );
                continue;
            }
            let restored = self
                .controller
                .attach(Request::new(proto::AttachRequest {
                    session: Some(proto::SessionRef {
                        id: Some(proto::session_ref::Id::ExternalId(
                            session.external_id.clone(),
                        )),
                    }),
                    transport: attachment.transport,
                    capabilities: attachment.capabilities.clone(),
                    selector: attachment
                        .selector
                        .as_ref()
                        .map(|only| proto::TrackSelector {
                            select: Some(proto::track_selector::Select::Only(only.clone())),
                        }),
                    format: None,
                    authoritative: attachment.authoritative,
                    label: attachment.label.clone(),
                    endpoint: attachment.endpoint.clone(),
                    group: attachment.group.clone(),
                    metadata: attachment.metadata.clone().into_iter().collect(),
                    idempotency_key: format!("adopt-{}-{}", session.external_id, attachment.label),
                }))
                .await?
                .into_inner();

            if attachment.paused {
                self.controller
                    .update_attachment(Request::new(proto::UpdateAttachmentRequest {
                        attachment_id: restored.attachment_id,
                        paused: Some(true),
                        selector: None,
                        format: None,
                        idempotency_key: String::new(),
                    }))
                    .await?;
            }
        }
        Ok(())
    }

    fn persisted_from(
        &self,
        session: &SessionView,
        attachments: &[AttachmentView],
    ) -> PersistedSession {
        PersistedSession {
            external_id: session.external_id.clone(),
            kind: session_kind_wire(session.kind),
            call_id: session.call_id.clone(),
            from_tags: session.from_tags.clone(),
            rtpengine_node: session.rtpengine_node.clone(),
            owner: self.owner.clone(),
            attachments: attachments
                .iter()
                .map(|attachment| PersistedAttachment {
                    label: attachment.label.clone(),
                    transport: transport_wire(attachment.transport),
                    endpoint: attachment.endpoint.clone(),
                    capabilities: capabilities_wire(attachment.capabilities),
                    selector: match attachment.selector {
                        TrackSelector::All => None,
                        TrackSelector::Only(track) => Some(track_name(track).to_string()),
                    },
                    authoritative: attachment.authoritative,
                    paused: attachment.paused,
                    group: attachment.group.clone(),
                    metadata: attachment.metadata.clone(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_store::MemorySessionStore;
    use control_api::{MediaPlane, MediaPlaneError, PlaybackSource};
    use session_core::{AttachmentId, PlaybackId, SessionId};
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingPlane {
        opened: Mutex<Vec<String>>,
        attached: Mutex<Vec<String>>,
    }

    #[control_api::async_trait]
    impl MediaPlane for RecordingPlane {
        async fn open_session(&self, session: SessionView) -> Result<(), MediaPlaneError> {
            self.opened.lock().unwrap().push(session.call_id.clone());
            Ok(())
        }
        async fn close_session(&self, _session: SessionId) -> Result<(), MediaPlaneError> {
            Ok(())
        }
        async fn open_attachment(&self, view: AttachmentView) -> Result<(), MediaPlaneError> {
            self.attached
                .lock()
                .unwrap()
                .push(format!("{}@{}", view.label, view.endpoint));
            Ok(())
        }
        async fn close_attachment(
            &self,
            _session: SessionId,
            _attachment: AttachmentId,
        ) -> Result<(), MediaPlaneError> {
            Ok(())
        }
        async fn send_text(
            &self,
            _attachment: AttachmentId,
            _json: String,
        ) -> Result<(), MediaPlaneError> {
            Ok(())
        }
        async fn start_playback(
            &self,
            _session: SessionId,
            _playback: PlaybackId,
            _source: PlaybackSource,
            _target_tag: Option<String>,
            _block_egress: bool,
        ) -> Result<(), MediaPlaneError> {
            Ok(())
        }
        async fn stop_playback(
            &self,
            _session: SessionId,
            _playback: PlaybackId,
        ) -> Result<(), MediaPlaneError> {
            Ok(())
        }
    }

    fn pod(name: &str) -> (Arc<SessionController>, Arc<RecordingPlane>) {
        let plane = Arc::new(RecordingPlane::default());
        let controller = Arc::new(SessionController::new(name).with_media_plane(plane.clone()));
        (controller, plane)
    }

    async fn tap_with_consumer(controller: &SessionController, external_id: &str) {
        controller
            .create_session(Request::new(proto::CreateSessionRequest {
                external_id: external_id.to_string(),
                kind: proto::SessionKind::Tap as i32,
                call_id: "call-abc".to_string(),
                from_tags: vec!["from-a".to_string(), "from-b".to_string()],
                rtpengine_node: "10.0.0.5:22222".to_string(),
                mix: false,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap();
        controller
            .attach(Request::new(proto::AttachRequest {
                session: Some(proto::SessionRef {
                    id: Some(proto::session_ref::Id::ExternalId(external_id.to_string())),
                }),
                transport: proto::Transport::WsTwilio as i32,
                capabilities: vec![
                    proto::Capability::Sink as i32,
                    proto::Capability::Events as i32,
                ],
                selector: Some(proto::TrackSelector {
                    select: Some(proto::track_selector::Select::Only("customer".to_string())),
                }),
                format: None,
                authoritative: true,
                label: "rtt".to_string(),
                endpoint: "wss-rtt-endpoint".to_string(),
                group: String::new(),
                metadata: Default::default(),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap();
    }

    async fn attach_group_member(
        controller: &SessionController,
        external_id: &str,
        label: &str,
        group: &str,
    ) {
        controller
            .attach(Request::new(proto::AttachRequest {
                session: Some(proto::SessionRef {
                    id: Some(proto::session_ref::Id::ExternalId(external_id.to_string())),
                }),
                transport: proto::Transport::FileS3 as i32,
                capabilities: vec![proto::Capability::Sink as i32],
                selector: Some(proto::TrackSelector {
                    select: Some(proto::track_selector::Select::Only("customer".to_string())),
                }),
                format: None,
                authoritative: false,
                label: label.to_string(),
                endpoint: "acct-1/rec-1.wav".to_string(),
                group: group.to_string(),
                metadata: Default::default(),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_live_session_is_persisted_with_everything_needed_to_rebuild_it() {
        let store = Arc::new(MemorySessionStore::default());
        let (controller, _) = pod("pod-a");
        tap_with_consumer(&controller, "req-1").await;

        RegistryKeeper::new(controller, store.clone(), "pod-a")
            .tick()
            .await;

        let stored = store.stored();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].call_id, "call-abc");
        assert_eq!(stored[0].from_tags, vec!["from-a", "from-b"]);
        assert_eq!(stored[0].rtpengine_node, "10.0.0.5:22222");
        assert_eq!(stored[0].owner, "pod-a");
        assert_eq!(
            stored[0].attachments[0].endpoint, "wss-rtt-endpoint",
            "without the endpoint a rebuilt consumer cannot reconnect"
        );
        assert_eq!(
            stored[0].attachments[0].selector.as_deref(),
            Some("customer")
        );
        assert!(stored[0].attachments[0].authoritative);
    }

    #[tokio::test]
    async fn another_pod_adopts_the_session_and_re_establishes_the_tap() {
        let store = Arc::new(MemorySessionStore::default());

        let (first_pod, _) = pod("pod-a");
        tap_with_consumer(&first_pod, "req-1").await;
        RegistryKeeper::new(first_pod, store.clone(), "pod-a")
            .tick()
            .await;

        store.expire_lease("req-1");

        let (second_pod, second_plane) = pod("pod-b");
        let keeper = RegistryKeeper::new(second_pod.clone(), store.clone(), "pod-b");
        keeper.tick().await;

        assert_eq!(keeper.counters().adopted.load(Ordering::Relaxed), 1);
        assert!(second_pod.holds_external_id("req-1"));
        assert_eq!(
            second_plane.opened.lock().unwrap().as_slice(),
            ["call-abc".to_string()],
            "the tap must be re-established, not merely remembered"
        );
        assert_eq!(
            second_plane.attached.lock().unwrap().as_slice(),
            ["rtt@wss-rtt-endpoint".to_string()],
            "the consumer must be reconnected to the same endpoint"
        );
        assert_eq!(store.lease_holder("req-1").as_deref(), Some("pod-b"));
    }

    #[tokio::test]
    async fn a_grouped_recording_is_persisted_but_refused_on_the_adopting_pod() {
        let store = Arc::new(MemorySessionStore::default());
        let (first_pod, _) = pod("pod-a");
        tap_with_consumer(&first_pod, "req-1").await;
        attach_group_member(&first_pod, "req-1", "alice", "conf-9").await;
        RegistryKeeper::new(first_pod, store.clone(), "pod-a")
            .tick()
            .await;

        let stored = store.stored();
        assert_eq!(stored[0].attachments[1].group, "conf-9");

        store.expire_lease("req-1");
        let (second_pod, second_plane) = pod("pod-b");
        let keeper = RegistryKeeper::new(second_pod.clone(), store.clone(), "pod-b");
        keeper.tick().await;

        assert_eq!(keeper.counters().adopted.load(Ordering::Relaxed), 1);
        assert_eq!(
            keeper
                .counters()
                .grouped_not_adopted
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            second_plane.attached.lock().unwrap().as_slice(),
            ["rtt@wss-rtt-endpoint".to_string()],
            "a recording group is per pod, so its member must not be rebuilt elsewhere"
        );
    }

    #[tokio::test]
    async fn a_session_a_living_pod_still_owns_is_never_stolen() {
        let store = Arc::new(MemorySessionStore::default());
        let (first_pod, _) = pod("pod-a");
        tap_with_consumer(&first_pod, "req-1").await;
        RegistryKeeper::new(first_pod, store.clone(), "pod-a")
            .tick()
            .await;

        let (second_pod, second_plane) = pod("pod-b");
        let keeper = RegistryKeeper::new(second_pod.clone(), store.clone(), "pod-b");
        keeper.tick().await;

        assert_eq!(keeper.counters().adopted.load(Ordering::Relaxed), 0);
        assert!(!second_pod.holds_external_id("req-1"));
        assert!(
            second_plane.opened.lock().unwrap().is_empty(),
            "a second tap on a live call would duplicate its audio"
        );
    }

    #[tokio::test]
    async fn a_session_with_no_call_identity_is_released_rather_than_half_restored() {
        let store = Arc::new(MemorySessionStore::default());
        let (first_pod, _) = pod("pod-a");
        first_pod
            .create_session(Request::new(proto::CreateSessionRequest {
                external_id: "req-telcompat".to_string(),
                kind: proto::SessionKind::Tap as i32,
                call_id: String::new(),
                from_tags: Vec::new(),
                rtpengine_node: String::new(),
                mix: false,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap();
        RegistryKeeper::new(first_pod, store.clone(), "pod-a")
            .tick()
            .await;
        store.expire_lease("req-telcompat");

        let (second_pod, second_plane) = pod("pod-b");
        let keeper = RegistryKeeper::new(second_pod.clone(), store.clone(), "pod-b");
        keeper.tick().await;

        assert_eq!(keeper.counters().unrebuildable.load(Ordering::Relaxed), 1);
        assert!(second_plane.opened.lock().unwrap().is_empty());
        assert!(
            store.stored().is_empty(),
            "an unrebuildable session must not linger and be re-adopted forever"
        );
    }

    #[tokio::test]
    async fn a_destroyed_session_stops_being_persisted() {
        let store = Arc::new(MemorySessionStore::default());
        let (controller, _) = pod("pod-a");
        tap_with_consumer(&controller, "req-1").await;
        let keeper = RegistryKeeper::new(controller.clone(), store.clone(), "pod-a");
        keeper.tick().await;
        assert_eq!(store.stored().len(), 1);

        controller
            .destroy_session(Request::new(proto::SessionRef {
                id: Some(proto::session_ref::Id::ExternalId("req-1".to_string())),
            }))
            .await
            .unwrap();
        keeper.tick().await;

        assert!(
            controller.snapshot().is_empty(),
            "the controller should no longer hold it"
        );
        assert!(
            store.stored().is_empty(),
            "an ended call left behind in the registry would be adopted as a phantom tap"
        );
        assert_eq!(keeper.counters().released.load(Ordering::Relaxed), 1);
    }
}
