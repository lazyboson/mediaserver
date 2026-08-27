use crate::recorder::{RESUME_MS_METADATA_KEY, SPILL_OWNER_METADATA_KEY};
use crate::session_store::{
    PersistedAttachment, PersistedFormat, PersistedRecording, PersistedSession, SessionStore,
    ADOPT_EVERY, MAX_ADOPTIONS_PER_SWEEP, RENEW_EVERY,
};
use control_api::convert::{
    capabilities_wire, format_wire, session_kind_wire, track_name, transport_wire,
};
use control_api::proto;
use control_api::proto::media_control_server::MediaControl;
use control_api::tonic::{self, Request};
use control_api::{MediaPlaneError, SessionController};
use media_core::AudioFormat;
use session_core::{AttachmentId, AttachmentView, SessionId, SessionView, TrackSelector};
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
    pub inline_not_adopted: AtomicU64,
    pub orphans_unsubscribed: AtomicU64,
    pub orphans_still_subscribed: AtomicU64,
    pub surrendered: AtomicU64,
    pub handed_off: AtomicU64,
}

#[control_api::async_trait]
pub trait TapSubscriptions: Send + Sync + 'static {
    fn subscription_tag(&self, session: SessionId) -> Option<String>;

    fn recording_journal(&self, attachment: AttachmentId) -> Option<PersistedRecording>;

    async fn unsubscribe_orphan(
        &self,
        node: &str,
        call_id: &str,
        to_tag: &str,
    ) -> Result<(), MediaPlaneError>;
}

pub struct RegistryKeeper {
    controller: Arc<SessionController>,
    store: Arc<dyn SessionStore>,
    owner: String,
    counters: Arc<KeeperCounters>,
    persisted_here: Mutex<BTreeSet<String>>,
    subscriptions: Option<Arc<dyn TapSubscriptions>>,
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
            subscriptions: None,
        }
    }

    pub fn with_subscriptions(
        mut self,
        subscriptions: Arc<dyn TapSubscriptions>,
    ) -> RegistryKeeper {
        self.subscriptions = Some(subscriptions);
        self
    }

    pub fn counters(&self) -> Arc<KeeperCounters> {
        Arc::clone(&self.counters)
    }

    pub async fn tick(&self) {
        self.persist_and_renew().await;
        self.adopt_orphans().await;
    }

    pub async fn hand_off_leases(&self) -> usize {
        let mine: Vec<String> = {
            let mut held = self.persisted_here.lock().unwrap();
            std::mem::take(&mut *held).into_iter().collect()
        };
        let mut handed_off = 0;
        for external_id in mine {
            match self.store.release_lease(&external_id, &self.owner).await {
                Ok(true) => {
                    self.counters.handed_off.fetch_add(1, Ordering::Relaxed);
                    handed_off += 1;
                    info!(
                        %external_id,
                        owner = %self.owner,
                        "lease released for adoption; another pod can take this session now \
                         instead of waiting for the lease to expire"
                    );
                }
                Ok(false) => info!(
                    %external_id,
                    owner = %self.owner,
                    "this pod no longer held the lease; nothing to hand off"
                ),
                Err(error) => {
                    self.counters.failed.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        %external_id,
                        %error,
                        "could not release this lease; adoption waits for it to expire"
                    );
                }
            }
        }
        handed_off
    }

    pub async fn run(self: Arc<RegistryKeeper>) {
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
        let mut surrendered = BTreeSet::new();
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
                        "another pod holds this session's lease; giving up the tap here so the \
                         call is not tapped twice"
                    );
                    surrendered.insert(external_id.clone());
                    self.surrender(&external_id).await;
                }
                Err(error) => {
                    self.counters.failed.fetch_add(1, Ordering::Relaxed);
                    warn!(%external_id, %error, "lease renewal failed");
                }
            }
        }
        for external_id in &surrendered {
            live.remove(external_id);
        }
        self.release_gone(live, &surrendered).await;
    }

    async fn surrender(&self, external_id: &str) {
        match self
            .controller
            .destroy_session(Request::new(proto::SessionRef {
                id: Some(proto::session_ref::Id::ExternalId(external_id.to_string())),
            }))
            .await
        {
            Ok(_) => {
                self.counters.surrendered.fetch_add(1, Ordering::Relaxed);
                info!(
                    %external_id,
                    "released a session whose lease another pod holds; its tap is unsubscribed here"
                );
            }
            Err(error) => {
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
                warn!(
                    %external_id,
                    %error,
                    "could not release a session whose lease another pod holds; \
                     rtpengine may now copy this call to two pods"
                );
            }
        }
    }

    async fn release_gone(&self, live: BTreeSet<String>, surrendered: &BTreeSet<String>) {
        let gone: Vec<String> = {
            let mut mine = self.persisted_here.lock().unwrap();
            let gone = mine
                .difference(&live)
                .filter(|external_id| !surrendered.contains(*external_id))
                .cloned()
                .collect();
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
            if session.is_inline() {
                self.counters
                    .inline_not_adopted
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    external_id = %session.external_id,
                    "an inline leg is an rtp endpoint on the pod that answered its offer, so \
                     no other pod can adopt it: the peer is sending to a socket that died. \
                     Releasing it; recovery is call control's job, not the registry's"
                );
                let _ = self.store.forget(&session.external_id).await;
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

    async fn drop_orphan_subscription(&self, session: &PersistedSession) {
        if session.subscription_tag.is_empty() {
            self.counters
                .orphans_still_subscribed
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                external_id = %session.external_id,
                call_id = %session.call_id,
                "this record carries no tap to-tag, so the previous owner's subscription \
                 cannot be cancelled; rtpengine copies the call to both until it ends"
            );
            return;
        }
        let Some(subscriptions) = self.subscriptions.as_ref() else {
            self.counters
                .orphans_still_subscribed
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                external_id = %session.external_id,
                "no media plane is wired to this keeper, so the previous owner's tap stays"
            );
            return;
        };
        match subscriptions
            .unsubscribe_orphan(
                &session.rtpengine_node,
                &session.call_id,
                &session.subscription_tag,
            )
            .await
        {
            Ok(()) => {
                self.counters
                    .orphans_unsubscribed
                    .fetch_add(1, Ordering::Relaxed);
                info!(
                    external_id = %session.external_id,
                    call_id = %session.call_id,
                    to_tag = %session.subscription_tag,
                    "cancelled the previous owner's tap before re-subscribing"
                );
            }
            Err(error) => {
                self.counters
                    .orphans_still_subscribed
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    external_id = %session.external_id,
                    to_tag = %session.subscription_tag,
                    %error,
                    "could not cancel the previous owner's tap; rtpengine keeps copying this \
                     call to a pod that is gone until the call ends"
                );
            }
        }
    }

    async fn rebuild(&self, session: &PersistedSession) -> Result<(), tonic::Status> {
        self.drop_orphan_subscription(session).await;
        self.controller
            .create_session(Request::new(proto::CreateSessionRequest {
                external_id: session.external_id.clone(),
                kind: session.kind,
                call_id: session.call_id.clone(),
                from_tags: session.from_tags.clone(),
                rtpengine_node: session.rtpengine_node.clone(),
                mix: false,
                idempotency_key: format!("adopt-{}", session.external_id),
                sdp_offer: String::new(),
                group: String::new(),
            }))
            .await?;

        for attachment in &session.attachments {
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
                    format: attachment.format.map(format_from_persisted),
                    authoritative: attachment.authoritative,
                    label: attachment.label.clone(),
                    endpoint: attachment.endpoint.clone(),
                    group: attachment.group.clone(),
                    metadata: resume_metadata(attachment),
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
                        metadata: std::collections::HashMap::new(),
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
            subscription_tag: self
                .subscriptions
                .as_ref()
                .and_then(|subscriptions| subscriptions.subscription_tag(session.id))
                .unwrap_or_default(),
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
                    format: Some(format_persisted(attachment.format)),
                    recording: self.subscriptions.as_ref().and_then(|subscriptions| {
                        subscriptions
                            .recording_journal(attachment.id)
                            .map(|journal| PersistedRecording {
                                owner: self.owner.clone(),
                                ..journal
                            })
                    }),
                    metadata: attachment
                        .metadata
                        .iter()
                        .filter(|(key, _)| !is_resume_key(key))
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                })
                .collect(),
        }
    }
}

fn is_resume_key(key: &str) -> bool {
    key == RESUME_MS_METADATA_KEY || key == SPILL_OWNER_METADATA_KEY
}

fn resume_metadata(attachment: &PersistedAttachment) -> std::collections::HashMap<String, String> {
    let mut metadata: std::collections::HashMap<String, String> = attachment
        .metadata
        .iter()
        .filter(|(key, _)| !is_resume_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if let Some(journal) = &attachment.recording {
        metadata.insert(
            RESUME_MS_METADATA_KEY.to_string(),
            journal.recorded_ms.to_string(),
        );
        metadata.insert(SPILL_OWNER_METADATA_KEY.to_string(), journal.owner.clone());
    }
    metadata
}

fn format_persisted(format: AudioFormat) -> PersistedFormat {
    let wire = format_wire(format);
    PersistedFormat {
        encoding: wire.encoding,
        sample_rate_hz: wire.sample_rate_hz,
        channels: wire.channels,
        ptime_ms: wire.ptime_ms,
    }
}

fn format_from_persisted(format: PersistedFormat) -> proto::AudioFormat {
    proto::AudioFormat {
        encoding: format.encoding,
        sample_rate_hz: format.sample_rate_hz,
        channels: format.channels,
        ptime_ms: format.ptime_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_store::MemorySessionStore;
    use crate::session_store::PersistedFormat;
    use control_api::{MediaPlane, MediaPlaneError, PlaybackSource};
    use media_core::Encoding;
    use session_core::{AttachmentId, PlaybackId, SessionId};
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingPlane {
        opened: Mutex<Vec<String>>,
        attached: Mutex<Vec<String>>,
        groups: Mutex<Vec<String>>,
        formats: Mutex<Vec<(String, AudioFormat)>>,
        metadata: Mutex<Vec<(String, std::collections::BTreeMap<String, String>)>>,
        journal: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Default)]
    struct FakeSubscriptions {
        tag: Option<String>,
        journal: Arc<Mutex<Vec<String>>>,
        refuse: bool,
        recording: Option<PersistedRecording>,
    }

    #[control_api::async_trait]
    impl TapSubscriptions for FakeSubscriptions {
        fn subscription_tag(&self, _session: SessionId) -> Option<String> {
            self.tag.clone()
        }

        fn recording_journal(&self, _attachment: AttachmentId) -> Option<PersistedRecording> {
            self.recording.clone()
        }

        async fn unsubscribe_orphan(
            &self,
            node: &str,
            call_id: &str,
            to_tag: &str,
        ) -> Result<(), MediaPlaneError> {
            self.journal
                .lock()
                .unwrap()
                .push(format!("unsubscribe {node} {call_id} {to_tag}"));
            if self.refuse {
                return Err(MediaPlaneError("rtpengine never answered".to_string()));
            }
            Ok(())
        }
    }

    #[control_api::async_trait]
    impl MediaPlane for RecordingPlane {
        async fn open_session(
            &self,
            session: SessionView,
        ) -> Result<control_api::OpenedSession, MediaPlaneError> {
            self.opened.lock().unwrap().push(session.call_id.clone());
            self.journal
                .lock()
                .unwrap()
                .push(format!("subscribe {}", session.call_id));
            Ok(control_api::OpenedSession::default())
        }
        async fn close_session(&self, _session: SessionId) -> Result<(), MediaPlaneError> {
            Ok(())
        }
        async fn open_attachment(&self, view: AttachmentView) -> Result<(), MediaPlaneError> {
            self.attached
                .lock()
                .unwrap()
                .push(format!("{}@{}", view.label, view.endpoint));
            if !view.group.is_empty() {
                self.groups.lock().unwrap().push(view.group.clone());
            }
            self.metadata
                .lock()
                .unwrap()
                .push((view.label.clone(), view.metadata.clone()));
            self.formats
                .lock()
                .unwrap()
                .push((view.label.clone(), view.format));
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
            _target_tag: Option<String>,
        ) -> Result<(), MediaPlaneError> {
            Ok(())
        }
    }

    fn pod(name: &str) -> (Arc<SessionController>, Arc<RecordingPlane>) {
        pod_journalling(name, Arc::new(Mutex::new(Vec::new())))
    }

    fn pod_journalling(
        name: &str,
        journal: Arc<Mutex<Vec<String>>>,
    ) -> (Arc<SessionController>, Arc<RecordingPlane>) {
        let plane = Arc::new(RecordingPlane {
            journal,
            ..RecordingPlane::default()
        });
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
                sdp_offer: String::new(),
                group: String::new(),
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

    async fn attach_recording(controller: &SessionController, external_id: &str, label: &str) {
        controller
            .attach(Request::new(proto::AttachRequest {
                session: Some(proto::SessionRef {
                    id: Some(proto::session_ref::Id::ExternalId(external_id.to_string())),
                }),
                transport: proto::Transport::FileS3 as i32,
                capabilities: vec![proto::Capability::Sink as i32],
                selector: None,
                format: None,
                authoritative: false,
                label: label.to_string(),
                endpoint: "acct-1/rec-1.wav".to_string(),
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

    async fn attach_asr_consumer(controller: &SessionController, external_id: &str) {
        controller
            .attach(Request::new(proto::AttachRequest {
                session: Some(proto::SessionRef {
                    id: Some(proto::session_ref::Id::ExternalId(external_id.to_string())),
                }),
                transport: proto::Transport::GrpcStream as i32,
                capabilities: vec![proto::Capability::Sink as i32],
                selector: None,
                format: Some(proto::AudioFormat {
                    encoding: proto::Encoding::L16 as i32,
                    sample_rate_hz: 16_000,
                    channels: 1,
                    ptime_ms: 20,
                }),
                authoritative: false,
                label: "asr".to_string(),
                endpoint: "grpc-asr".to_string(),
                group: String::new(),
                metadata: Default::default(),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn an_adopted_consumer_keeps_the_format_it_negotiated() {
        let store = Arc::new(MemorySessionStore::default());
        let (first_pod, _) = pod("pod-a");
        tap_with_consumer(&first_pod, "req-1").await;
        attach_asr_consumer(&first_pod, "req-1").await;
        RegistryKeeper::new(first_pod, store.clone(), "pod-a")
            .tick()
            .await;

        let stored = store.stored();
        assert_eq!(
            stored[0].attachments[1].format,
            Some(PersistedFormat {
                encoding: proto::Encoding::L16 as i32,
                sample_rate_hz: 16_000,
                channels: 1,
                ptime_ms: 20,
            }),
            "the negotiated format must be part of what a rebuild needs"
        );

        store.expire_lease("req-1");
        let (second_pod, second_plane) = pod("pod-b");
        RegistryKeeper::new(second_pod, store.clone(), "pod-b")
            .tick()
            .await;

        let reopened = second_plane.formats.lock().unwrap().clone();
        assert_eq!(
            reopened
                .iter()
                .find(|(label, _)| label == "asr")
                .map(|(_, format)| *format),
            Some(AudioFormat {
                encoding: Encoding::L16,
                sample_rate_hz: 16_000,
                channels: 1,
                ptime_ms: 20,
            }),
            "an ASR consumer that attached as L16/16k must not come back at the tap default"
        );
        assert_eq!(
            reopened
                .iter()
                .find(|(label, _)| label == "rtt")
                .map(|(_, format)| *format),
            Some(AudioFormat::pcmu_8k_20ms()),
            "a consumer that took the default must still get the default"
        );
    }

    #[tokio::test]
    async fn an_adopting_pod_is_told_how_much_recording_the_dead_pod_held() {
        let store = Arc::new(MemorySessionStore::default());
        let (first_pod, _) = pod("pod-a");
        tap_with_consumer(&first_pod, "req-1").await;
        attach_recording(&first_pod, "req-1", "rec").await;
        RegistryKeeper::new(first_pod, store.clone(), "pod-a")
            .with_subscriptions(Arc::new(FakeSubscriptions {
                tag: Some("tap-a".to_string()),
                journal: Arc::new(Mutex::new(Vec::new())),
                refuse: false,
                recording: Some(PersistedRecording {
                    recording_id: "rec-1".to_string(),
                    owner: String::new(),
                    recorded_ms: 61_000,
                    spilled_ms: 60_000,
                }),
            }))
            .tick()
            .await;

        let stored = store.stored();
        assert_eq!(
            stored[0].attachments[1].recording,
            Some(PersistedRecording {
                recording_id: "rec-1".to_string(),
                owner: "pod-a".to_string(),
                recorded_ms: 61_000,
                spilled_ms: 60_000,
            }),
            "the registry must name the pod whose disk holds the spilled segments"
        );

        store.expire_lease("req-1");
        let (second_pod, second_plane) = pod("pod-b");
        let keeper = RegistryKeeper::new(second_pod, store.clone(), "pod-b");
        keeper.tick().await;

        let restored = second_plane.metadata.lock().unwrap().clone();
        let carried = restored
            .iter()
            .find(|(label, _)| label == "rec")
            .map(|(_, metadata)| metadata.clone())
            .expect("the recording was not rebuilt");
        assert_eq!(
            carried.get(RESUME_MS_METADATA_KEY).map(String::as_str),
            Some("61000"),
            "the adopter must know where the recording had reached"
        );
        assert_eq!(
            carried.get(SPILL_OWNER_METADATA_KEY).map(String::as_str),
            Some("pod-a"),
            "the adopter must know whose disk holds what it cannot read"
        );

        let persisted_again = store.stored();
        assert!(
            !persisted_again[0]
                .attachments
                .iter()
                .any(|attachment| { attachment.metadata.contains_key(RESUME_MS_METADATA_KEY) }),
            "the resume hint is derived on every adoption, never accumulated in metadata"
        );
    }

    #[tokio::test]
    async fn a_grouped_recording_is_rebuilt_on_the_adopting_pod_with_its_group() {
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
            second_plane.attached.lock().unwrap().as_slice(),
            [
                "rtt@wss-rtt-endpoint".to_string(),
                "alice@acct-1/rec-1.wav".to_string()
            ],
            "a recording group lives in the session store, so its member rejoins it here"
        );
        assert_eq!(
            second_plane.groups.lock().unwrap().as_slice(),
            ["conf-9".to_string()],
            "the adopting pod must ask to rejoin the same group, not start an ungrouped file"
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
                sdp_offer: String::new(),
                group: String::new(),
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
    async fn an_orphaned_inline_leg_is_released_rather_than_adopted() {
        let store = Arc::new(MemorySessionStore::default());
        let (first_pod, _) = pod("pod-a");
        first_pod
            .create_session(Request::new(proto::CreateSessionRequest {
                external_id: "req-inline".to_string(),
                kind: proto::SessionKind::Inline as i32,
                call_id: "call-inline".to_string(),
                from_tags: vec!["from-a".to_string()],
                rtpengine_node: String::new(),
                mix: false,
                idempotency_key: String::new(),
                sdp_offer: "v=0\r\nc=IN IP4 10.9.0.4\r\nm=audio 41000 RTP/AVP 0\r\n".to_string(),
                group: String::new(),
            }))
            .await
            .unwrap();
        RegistryKeeper::new(first_pod, store.clone(), "pod-a")
            .tick()
            .await;
        store.expire_lease("req-inline");

        let (second_pod, second_plane) = pod("pod-b");
        let keeper = RegistryKeeper::new(second_pod.clone(), store.clone(), "pod-b");
        keeper.tick().await;

        assert_eq!(
            keeper.counters().inline_not_adopted.load(Ordering::Relaxed),
            1
        );
        assert_eq!(keeper.counters().adopted.load(Ordering::Relaxed), 0);
        assert!(
            second_plane.opened.lock().unwrap().is_empty(),
            "an inline leg's peer is sending to a socket on the pod that died"
        );
        assert!(
            store.stored().is_empty(),
            "an inline leg that cannot be adopted must not be re-claimed forever"
        );
    }

    #[tokio::test]
    async fn the_tap_to_tag_is_persisted_so_a_survivor_can_cancel_it() {
        let store = Arc::new(MemorySessionStore::default());
        let (controller, _) = pod("pod-a");
        tap_with_consumer(&controller, "req-1").await;

        RegistryKeeper::new(controller, store.clone(), "pod-a")
            .with_subscriptions(Arc::new(FakeSubscriptions {
                tag: Some("tap-a".to_string()),
                ..FakeSubscriptions::default()
            }))
            .tick()
            .await;

        assert_eq!(store.stored()[0].subscription_tag, "tap-a");
    }

    #[tokio::test]
    async fn the_adopter_cancels_the_dead_pods_tap_before_re_subscribing() {
        let store = Arc::new(MemorySessionStore::default());
        let (first_pod, _) = pod("pod-a");
        tap_with_consumer(&first_pod, "req-1").await;
        RegistryKeeper::new(first_pod, store.clone(), "pod-a")
            .with_subscriptions(Arc::new(FakeSubscriptions {
                tag: Some("tap-a".to_string()),
                ..FakeSubscriptions::default()
            }))
            .tick()
            .await;
        store.expire_lease("req-1");

        let journal = Arc::new(Mutex::new(Vec::new()));
        let (second_pod, _) = pod_journalling("pod-b", Arc::clone(&journal));
        let keeper = RegistryKeeper::new(second_pod.clone(), store.clone(), "pod-b")
            .with_subscriptions(Arc::new(FakeSubscriptions {
                tag: Some("tap-b".to_string()),
                journal: Arc::clone(&journal),
                refuse: false,
                recording: None,
            }));
        keeper.tick().await;

        assert_eq!(keeper.counters().adopted.load(Ordering::Relaxed), 1);
        assert_eq!(
            keeper
                .counters()
                .orphans_unsubscribed
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            journal.lock().unwrap().as_slice(),
            [
                "unsubscribe 10.0.0.5:22222 call-abc tap-a".to_string(),
                "subscribe call-abc".to_string()
            ],
            "the dead pod's tap must be cancelled before a second one is opened"
        );

        keeper.tick().await;
        assert_eq!(
            store.stored()[0].subscription_tag,
            "tap-b",
            "the new owner's to-tag must replace the stale one, or a second death leaks again"
        );
    }

    #[tokio::test]
    async fn an_adoption_that_cannot_cancel_the_old_tap_still_happens_and_is_counted() {
        let store = Arc::new(MemorySessionStore::default());
        let (first_pod, _) = pod("pod-a");
        tap_with_consumer(&first_pod, "req-1").await;
        RegistryKeeper::new(first_pod, store.clone(), "pod-a")
            .with_subscriptions(Arc::new(FakeSubscriptions {
                tag: Some("tap-a".to_string()),
                ..FakeSubscriptions::default()
            }))
            .tick()
            .await;
        store.expire_lease("req-1");

        let (second_pod, second_plane) = pod("pod-b");
        let keeper = RegistryKeeper::new(second_pod.clone(), store.clone(), "pod-b")
            .with_subscriptions(Arc::new(FakeSubscriptions {
                tag: Some("tap-b".to_string()),
                journal: Arc::new(Mutex::new(Vec::new())),
                refuse: true,
                recording: None,
            }));
        keeper.tick().await;

        assert_eq!(keeper.counters().adopted.load(Ordering::Relaxed), 1);
        assert_eq!(
            keeper
                .counters()
                .orphans_still_subscribed
                .load(Ordering::Relaxed),
            1,
            "a refused unsubscribe must be visible, not silent"
        );
        assert_eq!(
            second_plane.opened.lock().unwrap().as_slice(),
            ["call-abc".to_string()],
            "a call must still be re-tapped even when the old tap could not be cancelled"
        );
    }

    #[tokio::test]
    async fn a_record_from_before_the_tag_was_persisted_is_adopted_and_counted_as_orphaned() {
        let store = Arc::new(MemorySessionStore::default());
        let (first_pod, _) = pod("pod-a");
        tap_with_consumer(&first_pod, "req-1").await;
        RegistryKeeper::new(first_pod, store.clone(), "pod-a")
            .tick()
            .await;
        assert_eq!(store.stored()[0].subscription_tag, "");
        store.expire_lease("req-1");

        let (second_pod, _) = pod("pod-b");
        let keeper = RegistryKeeper::new(second_pod.clone(), store.clone(), "pod-b")
            .with_subscriptions(Arc::new(FakeSubscriptions {
                tag: Some("tap-b".to_string()),
                ..FakeSubscriptions::default()
            }));
        keeper.tick().await;

        assert_eq!(keeper.counters().adopted.load(Ordering::Relaxed), 1);
        assert_eq!(
            keeper
                .counters()
                .orphans_still_subscribed
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn a_pod_that_lost_the_lease_stops_tapping_and_leaves_the_record_alone() {
        let store = Arc::new(MemorySessionStore::default());
        let (controller, _) = pod("pod-a");
        tap_with_consumer(&controller, "req-1").await;
        let keeper = RegistryKeeper::new(controller.clone(), store.clone(), "pod-a")
            .with_subscriptions(Arc::new(FakeSubscriptions {
                tag: Some("tap-a".to_string()),
                ..FakeSubscriptions::default()
            }));
        keeper.tick().await;

        store.expire_lease("req-1");
        let stolen = store.claim_unleased("pod-b", 8).await.unwrap();
        assert_eq!(stolen.len(), 1);

        keeper.tick().await;

        assert_eq!(keeper.counters().lost.load(Ordering::Relaxed), 1);
        assert_eq!(keeper.counters().surrendered.load(Ordering::Relaxed), 1);
        assert!(
            !controller.holds_external_id("req-1"),
            "a pod that lost its lease must stop pumping that call"
        );
        assert_eq!(
            store.stored().len(),
            1,
            "the surrendering pod must not delete the record its successor holds"
        );
        assert_eq!(store.lease_holder("req-1").as_deref(), Some("pod-b"));
        assert_eq!(
            keeper.counters().released.load(Ordering::Relaxed),
            0,
            "surrender is not the same as an ended call"
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

    #[tokio::test]
    async fn a_draining_pod_hands_its_leases_over_without_deleting_the_records() {
        let store = Arc::new(MemorySessionStore::default());
        let (controller, _) = pod("pod-a");
        tap_with_consumer(&controller, "req-1").await;
        let keeper = RegistryKeeper::new(controller.clone(), store.clone(), "pod-a");
        keeper.tick().await;
        assert_eq!(store.lease_holder("req-1").as_deref(), Some("pod-a"));

        assert_eq!(keeper.hand_off_leases().await, 1);
        assert_eq!(keeper.counters().handed_off.load(Ordering::Relaxed), 1);
        assert_eq!(
            store.lease_holder("req-1"),
            None,
            "the lease must be free at once so an adopter does not wait for the ttl"
        );
        assert_eq!(
            store.stored().len(),
            1,
            "a drained session is adoptable, so its record must survive"
        );

        let adopted = store.claim_unleased("pod-b", 8).await.unwrap();
        assert_eq!(adopted.len(), 1);
        assert_eq!(adopted[0].external_id, "req-1");

        assert_eq!(
            keeper.hand_off_leases().await,
            0,
            "handing off twice must not touch a lease the successor now holds"
        );
        assert_eq!(store.lease_holder("req-1").as_deref(), Some("pod-b"));
    }
}
