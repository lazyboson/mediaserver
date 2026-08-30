use control_api::proto;
use control_api::proto::media_control_server::MediaControl;
use control_api::SessionController;
use rvoip_sip_core::builder::SimpleResponseBuilder;
use rvoip_sip_core::prelude::{Method, StatusCode};
use rvoip_sip_core::types::sip_request::Request as SipRequest;
use rvoip_sip_core::types::sip_response::Response as SipResponse;
use sip_uas::dialog::{
    invite_already_in_progress, negotiate_session_timer, no_such_dialog, request_out_of_order,
    with_session_timer, Classification, DialogId, Dialogs, SessionTimer, SessionTimerPolicy,
};
use sip_uas::{Action, Event, Reliability, ServerTransactions, Timings, TransactionKey};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tonic::Request as TonicRequest;
use tracing::{error, info, warn};

const DATAGRAM_CEILING: usize = 65_535;
const IDLE_TICK: Duration = Duration::from_millis(250);
const COMPLETION_QUEUE: usize = 256;
const ALLOWED_METHODS: &str = "INVITE, ACK, BYE, CANCEL, OPTIONS";

#[derive(Debug, Clone)]
pub struct FrontDoorConfig {
    pub listen: SocketAddr,
    pub advertised: SocketAddr,
    pub owner: String,
    pub session_timers: SessionTimerPolicy,
    pub retry_after: Duration,
}

impl FrontDoorConfig {
    pub fn contact_uri(&self) -> String {
        format!("sip:mss@{}", self.advertised)
    }
}

struct DialogSession {
    external_id: String,
    sdp_offer: String,
    sdp_answer: String,
}

struct PendingInvite {
    request: Box<SipRequest>,
    external_id: String,
    timer: SessionTimer,
    local_tag: String,
}

enum Completion {
    Answered {
        key: TransactionKey,
        answer: Result<String, String>,
    },
    Destroyed {
        external_id: String,
        outcome: Result<(), String>,
    },
}

pub struct FrontDoor {
    config: FrontDoorConfig,
    controller: Arc<SessionController>,
    transactions: ServerTransactions,
    dialogs: Dialogs,
    pending: HashMap<TransactionKey, PendingInvite>,
    sessions: HashMap<DialogId, DialogSession>,
    tags_issued: u64,
    answered_calls: u64,
    refused_calls: u64,
}

impl FrontDoor {
    pub fn new(config: FrontDoorConfig, controller: Arc<SessionController>) -> Self {
        let policy = config.session_timers;
        FrontDoor {
            config,
            controller,
            transactions: ServerTransactions::new(Timings::default(), Reliability::Unreliable),
            dialogs: Dialogs::new(policy),
            pending: HashMap::new(),
            sessions: HashMap::new(),
            tags_issued: 0,
            answered_calls: 0,
            refused_calls: 0,
        }
    }

    fn next_local_tag(&mut self) -> String {
        self.tags_issued += 1;
        format!("mss-{}-{}", self.config.owner, self.tags_issued)
    }

    fn external_id_for(&self, request: &SipRequest) -> String {
        let call_id = request
            .call_id()
            .map(|id| id.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        format!("sip-{call_id}")
    }

    fn answer_with_sdp(
        &self,
        request: &SipRequest,
        local_tag: &str,
        sdp_answer: &str,
        timer: SessionTimer,
    ) -> SipResponse {
        let mut builder =
            SimpleResponseBuilder::response_from_request(request, StatusCode::Ok, None);
        if let Some(to) = request.to() {
            builder = builder.to(
                to.address().display_name().unwrap_or_default(),
                &to.address().uri.to_string(),
                Some(local_tag),
            );
        }
        let response = builder
            .contact(&self.config.contact_uri(), None)
            .content_type("application/sdp")
            .header(allow_header())
            .body(sdp_answer.to_string())
            .build();
        match timer {
            SessionTimer::Negotiated { expires, refresher } => {
                with_session_timer(response, expires, refresher)
            }
            _ => response,
        }
    }

    fn acknowledged(&self, request: &SipRequest) -> SipResponse {
        SimpleResponseBuilder::response_from_request(request, StatusCode::Ok, None)
            .header(allow_header())
            .build()
    }

    fn plain(&self, request: &SipRequest, status: StatusCode, reason: &str) -> SipResponse {
        SimpleResponseBuilder::response_from_request(request, status, Some(reason))
            .header(allow_header())
            .build()
    }
}

fn allow_header() -> rvoip_sip_core::prelude::TypedHeader {
    use rvoip_sip_core::prelude::{HeaderName, HeaderValue, TypedHeader};
    TypedHeader::Other(
        HeaderName::Other("Allow".to_string()),
        HeaderValue::Raw(ALLOWED_METHODS.as_bytes().to_vec()),
    )
}

fn session_ref(external_id: &str) -> proto::SessionRef {
    proto::SessionRef {
        id: Some(proto::session_ref::Id::ExternalId(external_id.to_string())),
    }
}

pub async fn serve<Shutdown>(
    config: FrontDoorConfig,
    controller: Arc<SessionController>,
    shutdown: Shutdown,
) where
    Shutdown: std::future::Future<Output = ()> + Send,
{
    let socket = match UdpSocket::bind(config.listen).await {
        Ok(socket) => socket,
        Err(error) => {
            error!(listen = %config.listen, %error, "the sip front door could not bind");
            return;
        }
    };
    serve_on(socket, config, controller, shutdown).await;
}

pub async fn serve_on<Shutdown>(
    socket: UdpSocket,
    config: FrontDoorConfig,
    controller: Arc<SessionController>,
    shutdown: Shutdown,
) where
    Shutdown: std::future::Future<Output = ()> + Send,
{
    let socket = Arc::new(socket);
    info!(
        listen = %config.listen,
        contact = %config.contact_uri(),
        "the sip front door is answering INVITEs; the dialled user part is the group"
    );

    let mut door = FrontDoor::new(config, controller);
    let (completions, mut completed) = mpsc::channel::<Completion>(COMPLETION_QUEUE);
    let started = Instant::now();
    let mut datagram = vec![0u8; DATAGRAM_CEILING];
    tokio::pin!(shutdown);

    loop {
        let now = started.elapsed();
        let deadline = door
            .transactions
            .next_deadline()
            .into_iter()
            .chain(door.dialogs.next_deadline())
            .min();
        let delay = deadline
            .map(|due| due.saturating_sub(now))
            .unwrap_or(IDLE_TICK)
            .min(IDLE_TICK);

        tokio::select! {
            _ = &mut shutdown => {
                info!(
                    answered = door.answered_calls,
                    refused = door.refused_calls,
                    "the sip front door is closing"
                );
                return;
            }
            received = socket.recv_from(&mut datagram) => {
                match received {
                    Ok((len, from)) => {
                        let now = started.elapsed();
                        let actions = door
                            .transactions
                            .on_datagram(&datagram[..len], from, now);
                        run(&mut door, &socket, actions, &completions, now).await;
                    }
                    Err(error) => {
                        warn!(%error, "the sip front door could not read a datagram");
                    }
                }
            }
            Some(completion) = completed.recv() => {
                let now = started.elapsed();
                finish(&mut door, &socket, completion, now).await;
            }
            _ = tokio::time::sleep(delay) => {
                let now = started.elapsed();
                let actions = door.transactions.poll(now);
                run(&mut door, &socket, actions, &completions, now).await;
                for id in door.dialogs.expired(now) {
                    if let Some(session) = door.sessions.remove(&id) {
                        warn!(
                            call_id = id.call_id(),
                            external_id = session.external_id,
                            "the session timer ran out with no refresh; ending the media session"
                        );
                        destroy(&door, session.external_id, &completions);
                    }
                }
            }
        }
    }
}

async fn emit(socket: &UdpSocket, actions: &[Action]) {
    for action in actions {
        if let Action::Send { datagram, to } = action {
            if let Err(error) = socket.send_to(datagram, to).await {
                warn!(%to, %error, "the sip front door could not send a response");
            }
        }
    }
}

fn destroy(door: &FrontDoor, external_id: String, completions: &mpsc::Sender<Completion>) {
    let controller = Arc::clone(&door.controller);
    let sender = completions.clone();
    tokio::spawn(async move {
        let outcome = controller
            .destroy_session(TonicRequest::new(session_ref(&external_id)))
            .await
            .map(|_| ())
            .map_err(|status| status.message().to_string());
        let _ = sender
            .send(Completion::Destroyed {
                external_id,
                outcome,
            })
            .await;
    });
}

async fn run(
    door: &mut FrontDoor,
    socket: &UdpSocket,
    actions: Vec<Action>,
    completions: &mpsc::Sender<Completion>,
    now: Duration,
) {
    emit(socket, &actions).await;
    for action in actions {
        let Action::Deliver { key, event } = action else {
            continue;
        };
        match event {
            Event::Request(request) => {
                let replies = admit(door, key, *request, completions, now).await;
                emit(socket, &replies).await;
            }
            Event::Cancelled => {
                if let Some(pending) = door.pending.remove(&key) {
                    let cancelled = door.plain(
                        &pending.request,
                        StatusCode::RequestTerminated,
                        "the caller cancelled before we answered",
                    );
                    if let Ok(replies) = door.transactions.respond(&key, cancelled, now) {
                        emit(socket, &replies).await;
                    }
                    destroy(door, pending.external_id, completions);
                    door.refused_calls += 1;
                }
            }
            Event::AckNeverArrived => {
                let orphan = door
                    .sessions
                    .iter()
                    .find(|(_, session)| session.external_id.ends_with(key.branch()))
                    .map(|(id, _)| id.clone());
                if let Some(id) = orphan {
                    if let Some(session) = door.sessions.remove(&id) {
                        warn!(
                            transaction = %key,
                            external_id = session.external_id,
                            "no ACK ever arrived; ending the media session rather than leaking it"
                        );
                        door.dialogs.end(&id);
                        destroy(door, session.external_id, completions);
                    }
                } else if let Some(pending) = door.pending.remove(&key) {
                    warn!(
                        transaction = %key,
                        external_id = pending.external_id,
                        "no ACK ever arrived for an unanswered invite; ending the media session"
                    );
                    destroy(door, pending.external_id, completions);
                }
            }
            Event::Acknowledged | Event::Terminated => {}
        }
    }
}

async fn admit(
    door: &mut FrontDoor,
    key: TransactionKey,
    request: SipRequest,
    completions: &mpsc::Sender<Completion>,
    now: Duration,
) -> Vec<Action> {
    let classification = door.dialogs.admit(&request);
    let refusal = match classification {
        Classification::InitialInvite => {
            return open_session(door, key, request, completions, now).await;
        }
        Classification::Reinvite(id) => {
            return reanswer(door, key, request, id, now);
        }
        Classification::InDialog { id, method } => match method {
            Method::Bye => {
                if let Some(session) = door.sessions.remove(&id) {
                    destroy(door, session.external_id, completions);
                }
                door.dialogs.end(&id);
                door.acknowledged(&request)
            }
            Method::Options => door.acknowledged(&request),
            Method::Ack | Method::Cancel => return Vec::new(),
            _ => door.plain(
                &request,
                StatusCode::MethodNotAllowed,
                "a media plane answers INVITE, ACK, BYE, CANCEL and OPTIONS",
            ),
        },
        Classification::InviteAlreadyInProgress(_) => {
            invite_already_in_progress(&request, door.config.retry_after)
        }
        Classification::OutOfOrder(_) => request_out_of_order(&request),
        Classification::NoSuchDialog => no_such_dialog(&request),
        Classification::OutsideAnyDialog(Method::Options) => door.acknowledged(&request),
        Classification::OutsideAnyDialog(_) => door.plain(
            &request,
            StatusCode::MethodNotAllowed,
            "a media plane answers INVITE, ACK, BYE, CANCEL and OPTIONS",
        ),
        Classification::Unroutable => door.plain(
            &request,
            StatusCode::BadRequest,
            "this request carries no dialog identity",
        ),
    };
    door.transactions
        .respond(&key, refusal, now)
        .unwrap_or_default()
}

async fn open_session(
    door: &mut FrontDoor,
    key: TransactionKey,
    request: SipRequest,
    completions: &mpsc::Sender<Completion>,
    now: Duration,
) -> Vec<Action> {
    let timer = negotiate_session_timer(&request, door.dialogs.policy());
    if let SessionTimer::IntervalTooSmall { min_se } = timer {
        door.refused_calls += 1;
        let refusal = sip_uas::dialog::session_interval_too_small(&request, min_se);
        return door
            .transactions
            .respond(&key, refusal, now)
            .unwrap_or_default();
    }
    let group = request.uri().user.clone().unwrap_or_default();
    let offer = String::from_utf8_lossy(request.body()).to_string();
    if offer.trim().is_empty() {
        door.refused_calls += 1;
        let refusal = door.plain(
            &request,
            StatusCode::NotAcceptableHere,
            "an inline leg answers an offer; this INVITE carries none",
        );
        return door
            .transactions
            .respond(&key, refusal, now)
            .unwrap_or_default();
    }
    let external_id = door.external_id_for(&request);
    let local_tag = door.next_local_tag();
    let call_id = request
        .call_id()
        .map(|id| id.to_string())
        .unwrap_or_default();
    door.pending.insert(
        key.clone(),
        PendingInvite {
            request: Box::new(request),
            external_id: external_id.clone(),
            timer,
            local_tag,
        },
    );
    let controller = Arc::clone(&door.controller);
    let sender = completions.clone();
    let message = proto::CreateSessionRequest {
        external_id: external_id.clone(),
        kind: proto::SessionKind::Inline as i32,
        call_id,
        from_tags: Vec::new(),
        rtpengine_node: String::new(),
        mix: false,
        idempotency_key: external_id,
        sdp_offer: offer,
        group,
    };
    tokio::spawn(async move {
        let answer = controller
            .create_session(TonicRequest::new(message))
            .await
            .map(|session| session.into_inner().sdp_answer)
            .map_err(|status| status.message().to_string());
        let _ = sender.send(Completion::Answered { key, answer }).await;
    });
    Vec::new()
}

fn reanswer(
    door: &mut FrontDoor,
    key: TransactionKey,
    request: SipRequest,
    id: DialogId,
    now: Duration,
) -> Vec<Action> {
    let offer = String::from_utf8_lossy(request.body()).to_string();
    let timer = negotiate_session_timer(&request, door.dialogs.policy());
    let response = match door.sessions.get(&id) {
        None => no_such_dialog(&request),
        Some(session) if offer.trim().is_empty() || offer.trim() == session.sdp_offer.trim() => {
            let local_tag = id.local_tag().to_string();
            let answer = session.sdp_answer.clone();
            door.answer_with_sdp(&request, &local_tag, &answer, timer)
        }
        Some(_) => door.plain(
            &request,
            StatusCode::NotAcceptableHere,
            "MSS has no renegotiation path for a changed offer",
        ),
    };
    door.dialogs.answered(&id, now);
    door.transactions
        .respond(&key, response, now)
        .unwrap_or_default()
}

async fn finish(door: &mut FrontDoor, socket: &UdpSocket, completion: Completion, now: Duration) {
    match completion {
        Completion::Answered { key, answer } => {
            let Some(pending) = door.pending.remove(&key) else {
                return;
            };
            let response = match answer {
                Ok(sdp_answer) if !sdp_answer.trim().is_empty() => {
                    let response = door.answer_with_sdp(
                        &pending.request,
                        &pending.local_tag,
                        &sdp_answer,
                        pending.timer,
                    );
                    if let Some(id) = door.dialogs.establish(
                        &pending.request,
                        &pending.local_tag,
                        pending.timer,
                        now,
                    ) {
                        door.sessions.insert(
                            id,
                            DialogSession {
                                external_id: pending.external_id.clone(),
                                sdp_offer: String::from_utf8_lossy(pending.request.body())
                                    .to_string(),
                                sdp_answer,
                            },
                        );
                    }
                    door.answered_calls += 1;
                    response
                }
                Ok(_) => {
                    door.refused_calls += 1;
                    door.plain(
                        &pending.request,
                        StatusCode::ServiceUnavailable,
                        "the media plane returned no sdp answer",
                    )
                }
                Err(reason) => {
                    door.refused_calls += 1;
                    warn!(
                        external_id = pending.external_id,
                        reason, "the media plane refused the leg"
                    );
                    door.plain(&pending.request, StatusCode::ServiceUnavailable, &reason)
                }
            };
            if let Ok(actions) = door.transactions.respond(&key, response, now) {
                emit(socket, &actions).await;
            }
        }
        Completion::Destroyed {
            external_id,
            outcome,
        } => {
            if let Err(reason) = outcome {
                warn!(external_id, reason, "destroying the media session failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use control_api::controller::{MediaPlane, MediaPlaneError, OpenedSession, PlaybackSource};
    use session_core::{AttachmentId, AttachmentView, PlaybackId, SessionId, SessionView};

    const ANSWER: &str = "v=0\r\no=- 2 2 IN IP4 172.31.99.31\r\ns=mss-inline\r\n\
c=IN IP4 172.31.99.31\r\nt=0 0\r\nm=audio 40120 RTP/AVP 8\r\na=rtpmap:8 PCMA/8000\r\n\
a=ptime:20\r\na=sendrecv\r\n";

    const OFFER: &str = "v=0\r\no=- 1 1 IN IP4 172.31.99.80\r\ns=-\r\n\
c=IN IP4 172.31.99.80\r\nt=0 0\r\nm=audio 30056 RTP/AVP 8\r\na=rtpmap:8 PCMA/8000\r\n\
a=ptime:20\r\na=sendrecv\r\n";

    struct AnsweringPlane;

    #[tonic::async_trait]
    impl MediaPlane for AnsweringPlane {
        async fn open_session(
            &self,
            _session: SessionView,
        ) -> Result<OpenedSession, MediaPlaneError> {
            Ok(OpenedSession::answered(ANSWER.to_string()))
        }

        async fn close_session(&self, _session: SessionId) -> Result<(), MediaPlaneError> {
            Ok(())
        }

        async fn open_attachment(
            &self,
            _attachment: AttachmentView,
        ) -> Result<(), MediaPlaneError> {
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

    struct Caller {
        socket: UdpSocket,
        door: SocketAddr,
    }

    impl Caller {
        async fn say(&self, datagram: String) {
            self.socket
                .send_to(datagram.as_bytes(), self.door)
                .await
                .expect("the caller could send");
        }

        async fn hear(&self) -> String {
            let mut buffer = vec![0u8; DATAGRAM_CEILING];
            let read =
                tokio::time::timeout(Duration::from_secs(3), self.socket.recv_from(&mut buffer))
                    .await
                    .expect("the door answered within three seconds")
                    .expect("a datagram");
            String::from_utf8_lossy(&buffer[..read.0]).to_string()
        }

        async fn hear_final(&self) -> String {
            loop {
                let heard = self.hear().await;
                if !heard.starts_with("SIP/2.0 100") {
                    return heard;
                }
            }
        }
    }

    async fn door_and_caller() -> Caller {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("a door socket");
        let listen = socket.local_addr().expect("a bound address");
        let controller = Arc::new(
            control_api::SessionController::new("test-pod")
                .with_media_plane(Arc::new(AnsweringPlane)),
        );
        let config = FrontDoorConfig {
            listen,
            advertised: listen,
            owner: "test-pod".to_string(),
            session_timers: SessionTimerPolicy::default(),
            retry_after: Duration::from_secs(4),
        };
        tokio::spawn(serve_on(
            socket,
            config,
            controller,
            std::future::pending::<()>(),
        ));
        let caller = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("a caller socket");
        Caller {
            socket: caller,
            door: listen,
        }
    }

    fn message(
        method: &str,
        branch: &str,
        call_id: &str,
        cseq: u32,
        to_tag: Option<&str>,
        extra: &str,
        body: &str,
    ) -> String {
        let to = match to_tag {
            Some(tag) => format!("<sip:7200@127.0.0.1>;tag={tag}"),
            None => "<sip:7200@127.0.0.1>".to_string(),
        };
        format!(
            "{method} sip:7200@127.0.0.1 SIP/2.0\r\n\
Via: SIP/2.0/UDP 127.0.0.1:5060;branch={branch};rport\r\n\
From: <sip:caller@127.0.0.1>;tag=caller-tag\r\n\
To: {to}\r\n\
Call-ID: {call_id}\r\n\
CSeq: {cseq} {method}\r\n\
Max-Forwards: 70\r\n\
Contact: <sip:caller@127.0.0.1:5060>\r\n\
{extra}Content-Type: application/sdp\r\n\
Content-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn to_tag_of(response: &str) -> String {
        response
            .lines()
            .find(|line| line.to_ascii_lowercase().starts_with("to:"))
            .and_then(|line| line.split("tag=").nth(1))
            .map(|tag| tag.trim().to_string())
            .expect("the answer carries a to tag")
    }

    #[tokio::test]
    async fn an_invite_over_a_real_socket_is_answered_with_the_media_planes_sdp() {
        let caller = door_and_caller().await;
        caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let trying = caller.hear().await;
        assert!(
            trying.starts_with("SIP/2.0 100 Trying"),
            "a provisional comes first: {trying}"
        );
        let answer = caller.hear_final().await;
        assert!(answer.starts_with("SIP/2.0 200 OK"), "answered: {answer}");
        assert!(
            answer.contains("m=audio 40120 RTP/AVP 8"),
            "the media plane's own sdp: {answer}"
        );
        assert!(
            answer.contains("Contact:"),
            "a target for the ACK: {answer}"
        );
        assert!(
            answer.contains("Call-ID: call-1"),
            "the same dialog: {answer}"
        );
    }

    #[tokio::test]
    async fn two_calls_are_given_different_to_tags() {
        let caller = door_and_caller().await;
        caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let first = caller.hear_final().await;
        caller
            .say(message("INVITE", "z9hG4bK-2", "call-2", 1, None, "", OFFER))
            .await;
        let second = caller.hear_final().await;
        assert_ne!(
            to_tag_of(&first),
            to_tag_of(&second),
            "two dialogs that shared a to tag would be one dialog to the peer"
        );
    }

    #[tokio::test]
    async fn a_bye_inside_the_dialog_closes_the_leg_and_is_answered_200() {
        let caller = door_and_caller().await;
        caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let answer = caller.hear_final().await;
        let tag = to_tag_of(&answer);
        caller
            .say(message("ACK", "z9hG4bK-2", "call-1", 1, Some(&tag), "", ""))
            .await;
        caller
            .say(message("BYE", "z9hG4bK-3", "call-1", 2, Some(&tag), "", ""))
            .await;
        let closed = caller.hear_final().await;
        assert!(
            closed.starts_with("SIP/2.0 200 OK"),
            "bye answered: {closed}"
        );
    }

    #[tokio::test]
    async fn a_bye_for_a_dialog_the_door_never_opened_is_refused_481() {
        let caller = door_and_caller().await;
        caller
            .say(message(
                "BYE",
                "z9hG4bK-9",
                "never-happened",
                2,
                Some("not-our-tag"),
                "",
                "",
            ))
            .await;
        let refused = caller.hear_final().await;
        assert!(
            refused.starts_with("SIP/2.0 481"),
            "no such dialog: {refused}"
        );
    }

    #[tokio::test]
    async fn a_session_expires_below_our_floor_is_refused_422_before_any_session_is_made() {
        let caller = door_and_caller().await;
        caller
            .say(message(
                "INVITE",
                "z9hG4bK-1",
                "call-1",
                1,
                None,
                "Session-Expires: 30\r\n",
                OFFER,
            ))
            .await;
        let refused = caller.hear_final().await;
        assert!(
            refused.starts_with("SIP/2.0 422"),
            "the interval is too small: {refused}"
        );
        assert!(refused.contains("Min-SE: 90"), "our floor: {refused}");
    }

    #[tokio::test]
    async fn a_negotiated_session_timer_comes_back_on_the_answer() {
        let caller = door_and_caller().await;
        caller
            .say(message(
                "INVITE",
                "z9hG4bK-1",
                "call-1",
                1,
                None,
                "Session-Expires: 1800;refresher=uas\r\n",
                OFFER,
            ))
            .await;
        let answer = caller.hear_final().await;
        assert!(
            answer.contains("Session-Expires: 1800;refresher=uac"),
            "the caller refreshes, never the media plane: {answer}"
        );
        assert!(
            answer.contains("Require: timer"),
            "the option tag: {answer}"
        );
    }

    #[tokio::test]
    async fn a_refreshing_reinvite_is_answered_with_the_same_sdp_and_not_refused() {
        let caller = door_and_caller().await;
        caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let answer = caller.hear_final().await;
        let tag = to_tag_of(&answer);
        caller
            .say(message("ACK", "z9hG4bK-2", "call-1", 1, Some(&tag), "", ""))
            .await;
        caller
            .say(message(
                "INVITE",
                "z9hG4bK-3",
                "call-1",
                2,
                Some(&tag),
                "",
                OFFER,
            ))
            .await;
        let refreshed = caller.hear_final().await;
        assert!(
            refreshed.starts_with("SIP/2.0 200 OK"),
            "an unchanged offer is answerable: {refreshed}"
        );
        assert!(
            refreshed.contains("m=audio 40120 RTP/AVP 8"),
            "the same answer as before: {refreshed}"
        );
    }

    #[tokio::test]
    async fn a_reinvite_that_changes_the_offer_is_refused_by_name() {
        let caller = door_and_caller().await;
        caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let answer = caller.hear_final().await;
        let tag = to_tag_of(&answer);
        let moved = OFFER.replace("m=audio 30056", "m=audio 31000");
        caller
            .say(message(
                "INVITE",
                "z9hG4bK-3",
                "call-1",
                2,
                Some(&tag),
                "",
                &moved,
            ))
            .await;
        let refused = caller.hear_final().await;
        assert!(
            refused.starts_with("SIP/2.0 488"),
            "no renegotiation path yet, and it says so: {refused}"
        );
    }

    #[tokio::test]
    async fn an_options_is_answered_without_opening_anything() {
        let caller = door_and_caller().await;
        caller
            .say(message("OPTIONS", "z9hG4bK-1", "probe-1", 1, None, "", ""))
            .await;
        let answered = caller.hear_final().await;
        assert!(answered.starts_with("SIP/2.0 200"), "alive: {answered}");
    }

    #[tokio::test]
    async fn hostile_datagrams_do_not_stop_the_door_answering_the_next_real_call() {
        let caller = door_and_caller().await;
        for hostile in [
            vec![0u8; 1400],
            b"INVITE".to_vec(),
            b"INVITE sip:x SIP/2.0\r\nContent-Length: 99999\r\n\r\n".to_vec(),
            vec![0xff, 0xfe, 0xfd, 0x0d, 0x0a],
        ] {
            caller
                .socket
                .send_to(&hostile, caller.door)
                .await
                .expect("sent");
        }
        caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let answer = caller.hear_final().await;
        assert!(
            answer.starts_with("SIP/2.0 200 OK"),
            "the door survived: {answer}"
        );
    }
}
