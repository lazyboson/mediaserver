use call_events::{CallEventSink, CallIdentity, EventKind};
use control_api::controller::{SipCallControl, SipControlError};
use control_api::proto;
use control_api::proto::media_control_server::MediaControl;
use control_api::SessionController;
use rvoip_sip_core::builder::SimpleResponseBuilder;
use rvoip_sip_core::prelude::{Message, Method, StatusCode};
use rvoip_sip_core::types::sip_request::Request as SipRequest;
use rvoip_sip_core::types::sip_response::Response as SipResponse;
use session_core::{EventKind as MediaEventKind, MediaEvent};
use sip_uas::dialog::{
    invite_already_in_progress, negotiate_session_timer, no_such_dialog, request_out_of_order,
    with_session_timer, Classification, DialogId, Dialogs, SessionTimer, SessionTimerPolicy,
};
use sip_uas::{
    Action, ClientTransactions, Event, Reliability, ServerTransactions, Timings, TransactionKey,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tonic::Request as TonicRequest;
use tracing::{debug, error, info, warn};

const DATAGRAM_CEILING: usize = 65_535;
const IDLE_TICK: Duration = Duration::from_millis(250);
const COMPLETION_QUEUE: usize = 256;
const COMMAND_QUEUE: usize = 256;
const ALLOWED_METHODS: &str = "INVITE, ACK, BYE, CANCEL, OPTIONS";
const PARK_REJECTED: &str = "nobody answered the parked call in time";
const PARK_TIMEOUT_REASON: &str = "the park timeout ran out before anyone answered";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AnswerMode {
    #[default]
    Immediate,
    Parked,
}

impl AnswerMode {
    pub fn from_configured(configured: &str) -> Option<AnswerMode> {
        match configured.trim().to_ascii_lowercase().as_str() {
            "" | "immediate" => Some(AnswerMode::Immediate),
            "parked" => Some(AnswerMode::Parked),
            _ => None,
        }
    }

    pub fn as_configured(&self) -> &'static str {
        match self {
            AnswerMode::Immediate => "immediate",
            AnswerMode::Parked => "parked",
        }
    }

    fn parks(&self) -> bool {
        matches!(self, AnswerMode::Parked)
    }
}

#[derive(Debug, Clone)]
pub struct FrontDoorConfig {
    pub listen: SocketAddr,
    pub advertised: SocketAddr,
    pub owner: String,
    pub session_timers: SessionTimerPolicy,
    pub retry_after: Duration,
    pub answer_mode: AnswerMode,
    pub park_timeout: Duration,
    pub timings: Timings,
}

impl FrontDoorConfig {
    pub fn contact_uri(&self) -> String {
        format!("sip:mss@{}", self.advertised)
    }

    fn park_deadline(&self, now: Duration) -> Option<Duration> {
        (!self.park_timeout.is_zero()).then(|| now + self.park_timeout)
    }
}

struct DialogSession {
    identity: CallIdentity,
    sdp_offer: String,
    sdp_answer: String,
    peer: SocketAddr,
    end_of_interaction_published: bool,
}

struct PendingInvite {
    request: Box<SipRequest>,
    identity: CallIdentity,
    timer: SessionTimer,
    local_tag: String,
    peer: SocketAddr,
}

struct ParkedInvite {
    key: TransactionKey,
    invite: PendingInvite,
    expires_at: Option<Duration>,
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

enum DoorCommand {
    Answer {
        external_id: String,
        reply: oneshot::Sender<Result<(), SipControlError>>,
    },
    Hangup {
        external_id: String,
        reason: String,
        reply: oneshot::Sender<Result<(), SipControlError>>,
    },
}

struct DoorHandle {
    commands: mpsc::Sender<DoorCommand>,
}

impl DoorHandle {
    async fn ask<Build>(&self, build: Build) -> Result<(), SipControlError>
    where
        Build: FnOnce(oneshot::Sender<Result<(), SipControlError>>) -> DoorCommand,
    {
        let (reply, answered) = oneshot::channel();
        self.commands
            .send(build(reply))
            .await
            .map_err(|_| SipControlError::DoorClosed)?;
        answered.await.map_err(|_| SipControlError::DoorClosed)?
    }
}

#[tonic::async_trait]
impl SipCallControl for DoorHandle {
    async fn answer(&self, external_id: &str) -> Result<(), SipControlError> {
        let external_id = external_id.to_string();
        self.ask(|reply| DoorCommand::Answer { external_id, reply })
            .await
    }

    async fn hangup(&self, external_id: &str, reason: &str) -> Result<(), SipControlError> {
        let external_id = external_id.to_string();
        let reason = reason.to_string();
        self.ask(|reply| DoorCommand::Hangup {
            external_id,
            reason,
            reply,
        })
        .await
    }
}

pub struct FrontDoor {
    config: FrontDoorConfig,
    controller: Arc<SessionController>,
    transactions: ServerTransactions,
    clients: ClientTransactions,
    dialogs: Dialogs,
    pending: HashMap<TransactionKey, PendingInvite>,
    parked: HashMap<String, ParkedInvite>,
    sessions: HashMap<DialogId, DialogSession>,
    dialog_of: HashMap<String, DialogId>,
    invite_of: HashMap<TransactionKey, String>,
    byes: HashMap<TransactionKey, String>,
    terminating: HashMap<String, Duration>,
    call_events: Option<Arc<dyn CallEventSink>>,
    tags_issued: u64,
    branches_issued: u64,
    answered_calls: u64,
    refused_calls: u64,
    hangups_sent: u64,
}

impl FrontDoor {
    pub fn new(
        config: FrontDoorConfig,
        controller: Arc<SessionController>,
        call_events: Option<Arc<dyn CallEventSink>>,
    ) -> Self {
        let policy = config.session_timers;
        let timings = config.timings;
        FrontDoor {
            config,
            controller,
            transactions: ServerTransactions::new(timings, Reliability::Unreliable),
            clients: ClientTransactions::new(timings, Reliability::Unreliable),
            dialogs: Dialogs::new(policy),
            pending: HashMap::new(),
            parked: HashMap::new(),
            sessions: HashMap::new(),
            dialog_of: HashMap::new(),
            invite_of: HashMap::new(),
            byes: HashMap::new(),
            terminating: HashMap::new(),
            call_events,
            tags_issued: 0,
            branches_issued: 0,
            answered_calls: 0,
            refused_calls: 0,
            hangups_sent: 0,
        }
    }

    fn next_local_tag(&mut self) -> String {
        self.tags_issued += 1;
        format!("mss-{}-{}", self.config.owner, self.tags_issued)
    }

    fn next_branch(&mut self) -> String {
        self.branches_issued += 1;
        format!("z9hG4bK-mss-{}-{}", self.config.owner, self.branches_issued)
    }

    fn hangup_memory(&self) -> Duration {
        self.config.timings.give_up_after()
    }

    fn external_id_for(&self, request: &SipRequest) -> String {
        let call_id = request
            .call_id()
            .map(|id| id.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        format!("sip-{call_id}")
    }

    fn publish(&self, identity: &CallIdentity, kind: EventKind) {
        self.publish_because(identity, kind, String::new());
    }

    fn publish_because(&self, identity: &CallIdentity, kind: EventKind, reason: String) {
        if let Some(sink) = &self.call_events {
            sink.publish(identity.event_because(kind, reason));
        }
    }

    fn forget(&mut self, id: &DialogId) -> Option<DialogSession> {
        let session = self.sessions.remove(id)?;
        self.dialog_of.remove(&session.identity.external_id);
        self.invite_of
            .retain(|_, external_id| external_id != &session.identity.external_id);
        Some(session)
    }

    fn answer_with_sdp(
        &self,
        request: &SipRequest,
        local_tag: &str,
        sdp_answer: &str,
        timer: SessionTimer,
    ) -> SipResponse {
        let response = self
            .dialog_response(request, StatusCode::Ok, local_tag)
            .content_type("application/sdp")
            .body(sdp_answer.to_string())
            .build();
        match timer {
            SessionTimer::Negotiated { expires, refresher } => {
                with_session_timer(response, expires, refresher)
            }
            _ => response,
        }
    }

    fn ringing(&self, request: &SipRequest, local_tag: &str) -> SipResponse {
        self.dialog_response(request, StatusCode::Ringing, local_tag)
            .build()
    }

    fn dialog_response(
        &self,
        request: &SipRequest,
        status: StatusCode,
        local_tag: &str,
    ) -> SimpleResponseBuilder {
        let mut builder = SimpleResponseBuilder::response_from_request(request, status, None);
        if let Some(to) = request.to() {
            builder = builder.to(
                to.address().display_name().unwrap_or_default(),
                &to.address().uri.to_string(),
                Some(local_tag),
            );
        }
        builder
            .contact(&self.config.contact_uri(), None)
            .header(allow_header())
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

    fn next_park_deadline(&self) -> Option<Duration> {
        self.parked
            .values()
            .filter_map(|parked| parked.expires_at)
            .chain(self.terminating.values().copied())
            .min()
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

fn from_user(request: &SipRequest) -> String {
    request
        .from()
        .and_then(|from| from.address().uri.user.clone())
        .unwrap_or_default()
}

pub async fn serve<Shutdown>(
    config: FrontDoorConfig,
    controller: Arc<SessionController>,
    shutdown: Shutdown,
    call_events: Option<Arc<dyn CallEventSink>>,
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
    serve_on(socket, config, controller, shutdown, call_events).await;
}

pub async fn serve_on<Shutdown>(
    socket: UdpSocket,
    config: FrontDoorConfig,
    controller: Arc<SessionController>,
    shutdown: Shutdown,
    call_events: Option<Arc<dyn CallEventSink>>,
) where
    Shutdown: std::future::Future<Output = ()> + Send,
{
    let socket = Arc::new(socket);
    info!(
        listen = %config.listen,
        contact = %config.contact_uri(),
        answer_mode = config.answer_mode.as_configured(),
        park_timeout_ms = config.park_timeout.as_millis() as u64,
        "the sip front door is answering INVITEs; the dialled user part is the group"
    );

    let (commands, mut commanded) = mpsc::channel::<DoorCommand>(COMMAND_QUEUE);
    if !controller.attach_sip_call_control(Arc::new(DoorHandle {
        commands: commands.clone(),
    })) {
        warn!(
            "another sip front door already owns call control on this pod; AnswerSession and \
             HangupSession will reach that one"
        );
    }
    let mut reported = controller.subscribe();

    let mut door = FrontDoor::new(config, controller, call_events);
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
            .chain(door.clients.next_deadline())
            .chain(door.dialogs.next_deadline())
            .chain(door.next_park_deadline())
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
                    hangups = door.hangups_sent,
                    "the sip front door is closing"
                );
                return;
            }
            received = socket.recv_from(&mut datagram) => {
                match received {
                    Ok((len, from)) => {
                        let now = started.elapsed();
                        let actions = dispatch(&mut door, &datagram[..len], from, now);
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
            Some(command) = commanded.recv() => {
                let now = started.elapsed();
                obey(&mut door, &socket, command, &completions, now).await;
            }
            report = reported.recv() => {
                match report {
                    Ok(event) => announce_end_of_interaction(&mut door, event),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => warn!(
                        missed,
                        "the sip front door fell behind the media event bus; an \
                         end_of_interaction may not have been published"
                    ),
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
                }
            }
            _ = tokio::time::sleep(delay) => {
                let now = started.elapsed();
                let actions = door.transactions.poll(now);
                run(&mut door, &socket, actions, &completions, now).await;
                let client_actions = door.clients.poll(now);
                run(&mut door, &socket, client_actions, &completions, now).await;
                expire_parked(&mut door, &socket, &completions, now).await;
                for id in door.dialogs.expired(now) {
                    if let Some(session) = door.forget(&id) {
                        warn!(
                            call_id = id.call_id(),
                            external_id = session.identity.external_id,
                            "the session timer ran out with no refresh; ending the media session"
                        );
                        door.publish(&session.identity, EventKind::Ended);
                        destroy(&door, session.identity.external_id, &completions);
                    }
                }
            }
        }
    }
}

fn dispatch(door: &mut FrontDoor, datagram: &[u8], from: SocketAddr, now: Duration) -> Vec<Action> {
    match sip_uas::parse_without_trusting_the_network(datagram) {
        Some(Message::Request(request)) => {
            door.transactions.on_request_message(&request, from, now)
        }
        Some(Message::Response(response)) => door.clients.on_response(&response, now),
        None => Vec::new(),
    }
}

fn announce_end_of_interaction(door: &mut FrontDoor, event: MediaEvent) {
    let MediaEventKind::EndOfInteraction { reason } = event.kind else {
        return;
    };
    if !event.legacy_eligible {
        debug!(
            external_id = event.external_id,
            "a non-authoritative attachment reported end_of_interaction; only the \
             authoritative one speaks for the call"
        );
        return;
    }
    let Some(id) = door.dialog_of.get(&event.external_id).cloned() else {
        return;
    };
    let Some(session) = door.sessions.get_mut(&id) else {
        return;
    };
    if session.end_of_interaction_published {
        return;
    }
    session.end_of_interaction_published = true;
    let identity = session.identity.clone();
    door.publish_because(&identity, EventKind::EndOfInteraction, reason);
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

fn end_named(
    door: &FrontDoor,
    external_id: String,
    reason: String,
    completions: &mpsc::Sender<Completion>,
) {
    let controller = Arc::clone(&door.controller);
    let sender = completions.clone();
    tokio::spawn(async move {
        let outcome = controller
            .end_session_named(&external_id, &reason)
            .await
            .map_err(|status| status.message().to_string());
        let _ = sender
            .send(Completion::Destroyed {
                external_id,
                outcome,
            })
            .await;
    });
}

async fn obey(
    door: &mut FrontDoor,
    socket: &UdpSocket,
    command: DoorCommand,
    completions: &mpsc::Sender<Completion>,
    now: Duration,
) {
    match command {
        DoorCommand::Answer { external_id, reply } => {
            let outcome = answer_parked(door, socket, &external_id, completions, now).await;
            let _ = reply.send(outcome);
        }
        DoorCommand::Hangup {
            external_id,
            reason,
            reply,
        } => {
            let outcome = hang_up(door, socket, &external_id, reason, completions, now).await;
            let _ = reply.send(outcome);
        }
    }
}

async fn answer_parked(
    door: &mut FrontDoor,
    socket: &UdpSocket,
    external_id: &str,
    completions: &mpsc::Sender<Completion>,
    now: Duration,
) -> Result<(), SipControlError> {
    if door.dialog_of.contains_key(external_id) {
        return Ok(());
    }
    let Some(parked) = door.parked.remove(external_id) else {
        return Err(SipControlError::NotADoorSession(external_id.to_string()));
    };
    let sdp_answer = match describe_answer(door, external_id).await {
        Some(sdp_answer) => sdp_answer,
        None => {
            let refusal = door.plain(
                &parked.invite.request,
                StatusCode::ServiceUnavailable,
                "the media plane no longer holds this leg",
            );
            door.refused_calls += 1;
            if let Ok(actions) = door.transactions.respond(&parked.key, refusal, now) {
                emit(socket, &actions).await;
            }
            door.publish(&parked.invite.identity, EventKind::Ended);
            destroy(door, external_id.to_string(), completions);
            return Err(SipControlError::NotADoorSession(external_id.to_string()));
        }
    };
    let response = door.answer_with_sdp(
        &parked.invite.request,
        &parked.invite.local_tag,
        &sdp_answer,
        parked.invite.timer,
    );
    let actions = door
        .transactions
        .respond(&parked.key, response, now)
        .unwrap_or_default();
    emit(socket, &actions).await;
    settle(door, &parked.key, &parked.invite, sdp_answer, now);
    Ok(())
}

async fn describe_answer(door: &FrontDoor, external_id: &str) -> Option<String> {
    door.controller
        .describe_session(TonicRequest::new(session_ref(external_id)))
        .await
        .ok()
        .map(|session| session.into_inner().sdp_answer)
        .filter(|sdp_answer| !sdp_answer.trim().is_empty())
}

fn settle(
    door: &mut FrontDoor,
    key: &TransactionKey,
    invite: &PendingInvite,
    sdp_answer: String,
    now: Duration,
) {
    if let Some(id) = door
        .dialogs
        .establish(&invite.request, &invite.local_tag, invite.timer, now)
    {
        door.dialog_of
            .insert(invite.identity.external_id.clone(), id.clone());
        door.invite_of
            .insert(key.clone(), invite.identity.external_id.clone());
        door.sessions.insert(
            id,
            DialogSession {
                identity: invite.identity.clone(),
                sdp_offer: String::from_utf8_lossy(invite.request.body()).to_string(),
                sdp_answer,
                peer: invite.peer,
                end_of_interaction_published: false,
            },
        );
    }
    door.answered_calls += 1;
    door.publish(&invite.identity, EventKind::Answered);
}

async fn hang_up(
    door: &mut FrontDoor,
    socket: &UdpSocket,
    external_id: &str,
    reason: String,
    completions: &mpsc::Sender<Completion>,
    now: Duration,
) -> Result<(), SipControlError> {
    if door.terminating.contains_key(external_id) {
        return Ok(());
    }
    if let Some(id) = door.dialog_of.get(external_id).cloned() {
        let branch = door.next_branch();
        let via_sent_by = door.config.advertised.to_string();
        let bye = door
            .dialogs
            .in_dialog_request(&id, Method::Bye, &via_sent_by, &branch);
        let Some(session) = door.forget(&id) else {
            return Err(SipControlError::NotADoorSession(external_id.to_string()));
        };
        door.dialogs.end(&id);
        match bye {
            Some(bye) => match door.clients.begin(&bye, session.peer, now) {
                Ok((key, actions)) => {
                    emit(socket, &actions).await;
                    door.byes.insert(key, external_id.to_string());
                    door.hangups_sent += 1;
                    info!(
                        external_id,
                        peer = %session.peer,
                        reason,
                        "the media plane is hanging the call up with an in-dialog BYE"
                    );
                }
                Err(error) => warn!(
                    external_id,
                    %error,
                    "a bye for this dialog could not be started; the media session still ends"
                ),
            },
            None => warn!(
                external_id,
                "the dialog was gone before its bye could be built; the media session still ends"
            ),
        }
        door.terminating
            .insert(external_id.to_string(), now + door.hangup_memory());
        door.publish(&session.identity, EventKind::Ended);
        end_named(door, external_id.to_string(), reason, completions);
        return Ok(());
    }
    if let Some(parked) = door.parked.remove(external_id) {
        refuse_parked(door, socket, parked, reason, completions, now).await;
        door.terminating
            .insert(external_id.to_string(), now + door.hangup_memory());
        return Ok(());
    }
    Err(SipControlError::NotADoorSession(external_id.to_string()))
}

async fn refuse_parked(
    door: &mut FrontDoor,
    socket: &UdpSocket,
    parked: ParkedInvite,
    reason: String,
    completions: &mpsc::Sender<Completion>,
    now: Duration,
) {
    let refusal = door.plain(
        &parked.invite.request,
        StatusCode::TemporarilyUnavailable,
        PARK_REJECTED,
    );
    door.refused_calls += 1;
    if let Ok(actions) = door.transactions.respond(&parked.key, refusal, now) {
        emit(socket, &actions).await;
    }
    door.publish(&parked.invite.identity, EventKind::Ended);
    end_named(
        door,
        parked.invite.identity.external_id.clone(),
        reason,
        completions,
    );
}

async fn expire_parked(
    door: &mut FrontDoor,
    socket: &UdpSocket,
    completions: &mpsc::Sender<Completion>,
    now: Duration,
) {
    door.terminating.retain(|_, forget_at| now < *forget_at);
    let due: Vec<String> = door
        .parked
        .iter()
        .filter(|(_, parked)| parked.expires_at.is_some_and(|at| now >= at))
        .map(|(external_id, _)| external_id.clone())
        .collect();
    for external_id in due {
        let Some(parked) = door.parked.remove(&external_id) else {
            continue;
        };
        warn!(
            external_id,
            park_timeout_ms = door.config.park_timeout.as_millis() as u64,
            "nobody answered this parked call in time; refusing it 480 and ending the leg"
        );
        refuse_parked(
            door,
            socket,
            parked,
            PARK_TIMEOUT_REASON.to_string(),
            completions,
            now,
        )
        .await;
    }
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
            Event::Request { request, from } => {
                let replies = admit(door, key, *request, from, completions, now).await;
                emit(socket, &replies).await;
            }
            Event::Response(response) => {
                if let Some(external_id) = door.byes.remove(&key) {
                    info!(
                        external_id,
                        status = response.status_code(),
                        "the peer answered our bye; the dialog is closed on both sides"
                    );
                }
            }
            Event::TimedOut => {
                if let Some(external_id) = door.byes.remove(&key) {
                    warn!(
                        external_id,
                        "nobody ever answered our bye; giving up on the transaction. The media \
                         session was already ended when the hangup was asked for"
                    );
                }
            }
            Event::Cancelled => {
                let cancelled_parked = door
                    .parked
                    .iter()
                    .find(|(_, parked)| parked.key == key)
                    .map(|(external_id, _)| external_id.clone());
                if let Some(external_id) = cancelled_parked {
                    if let Some(parked) = door.parked.remove(&external_id) {
                        let cancelled = door.plain(
                            &parked.invite.request,
                            StatusCode::RequestTerminated,
                            "the caller cancelled before anyone answered",
                        );
                        if let Ok(replies) = door.transactions.respond(&key, cancelled, now) {
                            emit(socket, &replies).await;
                        }
                        door.publish(&parked.invite.identity, EventKind::Ended);
                        destroy(door, external_id, completions);
                        door.refused_calls += 1;
                    }
                } else if let Some(pending) = door.pending.remove(&key) {
                    let cancelled = door.plain(
                        &pending.request,
                        StatusCode::RequestTerminated,
                        "the caller cancelled before we answered",
                    );
                    if let Ok(replies) = door.transactions.respond(&key, cancelled, now) {
                        emit(socket, &replies).await;
                    }
                    destroy(door, pending.identity.external_id, completions);
                    door.refused_calls += 1;
                }
            }
            Event::AckNeverArrived => {
                let answered = door
                    .invite_of
                    .get(&key)
                    .and_then(|external_id| door.dialog_of.get(external_id))
                    .cloned();
                if let Some(id) = answered {
                    if let Some(session) = door.forget(&id) {
                        warn!(
                            transaction = %key,
                            external_id = session.identity.external_id,
                            "no ACK ever arrived; ending the media session rather than leaking it"
                        );
                        door.dialogs.end(&id);
                        door.publish(&session.identity, EventKind::Ended);
                        destroy(door, session.identity.external_id, completions);
                    }
                } else if let Some(pending) = door.pending.remove(&key) {
                    warn!(
                        transaction = %key,
                        external_id = pending.identity.external_id,
                        "no ACK ever arrived for an unanswered invite; ending the media session"
                    );
                    destroy(door, pending.identity.external_id, completions);
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
    from: SocketAddr,
    completions: &mpsc::Sender<Completion>,
    now: Duration,
) -> Vec<Action> {
    let classification = door.dialogs.admit(&request);
    let refusal = match classification {
        Classification::InitialInvite => {
            return open_session(door, key, request, from, completions, now).await;
        }
        Classification::Reinvite(id) => {
            return reanswer(door, key, request, id, now);
        }
        Classification::InDialog { id, method } => match method {
            Method::Bye => {
                if let Some(session) = door.forget(&id) {
                    door.publish(&session.identity, EventKind::Ended);
                    destroy(door, session.identity.external_id, completions);
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
    from: SocketAddr,
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
    let from_user = from_user(&request);
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
    let identity = CallIdentity {
        external_id: external_id.clone(),
        group: group.clone(),
        from: from_user,
    };
    door.pending.insert(
        key.clone(),
        PendingInvite {
            request: Box::new(request),
            identity,
            timer,
            local_tag,
            peer: from,
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
            match answer {
                Ok(sdp_answer) if !sdp_answer.trim().is_empty() => {
                    if door.config.answer_mode.parks() {
                        park(door, socket, key, pending, now).await;
                    } else {
                        accept(door, socket, key, pending, sdp_answer, now).await;
                    }
                }
                Ok(_) => {
                    door.refused_calls += 1;
                    let refusal = door.plain(
                        &pending.request,
                        StatusCode::ServiceUnavailable,
                        "the media plane returned no sdp answer",
                    );
                    if let Ok(actions) = door.transactions.respond(&key, refusal, now) {
                        emit(socket, &actions).await;
                    }
                }
                Err(reason) => {
                    door.refused_calls += 1;
                    warn!(
                        external_id = pending.identity.external_id,
                        reason, "the media plane refused the leg"
                    );
                    let refusal =
                        door.plain(&pending.request, StatusCode::ServiceUnavailable, &reason);
                    if let Ok(actions) = door.transactions.respond(&key, refusal, now) {
                        emit(socket, &actions).await;
                    }
                }
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

async fn accept(
    door: &mut FrontDoor,
    socket: &UdpSocket,
    key: TransactionKey,
    pending: PendingInvite,
    sdp_answer: String,
    now: Duration,
) {
    let response = door.answer_with_sdp(
        &pending.request,
        &pending.local_tag,
        &sdp_answer,
        pending.timer,
    );
    if let Ok(actions) = door.transactions.respond(&key, response, now) {
        emit(socket, &actions).await;
    }
    settle(door, &key, &pending, sdp_answer, now);
}

async fn park(
    door: &mut FrontDoor,
    socket: &UdpSocket,
    key: TransactionKey,
    pending: PendingInvite,
    now: Duration,
) {
    let ringing = door.ringing(&pending.request, &pending.local_tag);
    if let Ok(actions) = door.transactions.respond(&key, ringing, now) {
        emit(socket, &actions).await;
    }
    let identity = pending.identity.clone();
    let expires_at = door.config.park_deadline(now);
    door.parked.insert(
        identity.external_id.clone(),
        ParkedInvite {
            key,
            invite: pending,
            expires_at,
        },
    );
    info!(
        external_id = identity.external_id,
        group = identity.group,
        park_timeout_ms = door.config.park_timeout.as_millis() as u64,
        "this call is parked at 180 Ringing and waits for AnswerSession"
    );
    door.publish(&identity, EventKind::Invited);
}
#[cfg(test)]
mod tests {
    use super::*;
    use call_events::RecordingSink;
    use control_api::controller::{MediaPlane, MediaPlaneError, OpenedSession, PlaybackSource};
    use session_core::{AttachmentId, AttachmentView, PlaybackId, SessionId, SessionView};
    use std::str::FromStr;

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
                if heard.starts_with("SIP/2.0") && !heard.starts_with("SIP/2.0 100") {
                    return heard;
                }
            }
        }

        async fn hear_request(&self, method: &str) -> String {
            loop {
                let heard = self.hear().await;
                if heard.starts_with(method) {
                    return heard;
                }
            }
        }
    }

    fn header_of(message: &str, name: &str) -> String {
        let wanted = format!("{}:", name.to_ascii_lowercase());
        message
            .lines()
            .find(|line| line.to_ascii_lowercase().starts_with(&wanted))
            .map(|line| line.trim().to_string())
            .unwrap_or_else(|| panic!("{name} is missing from: {message}"))
    }

    fn two_hundred_for(request: &str) -> String {
        format!(
            "SIP/2.0 200 OK\r\n{}\r\n{}\r\n{}\r\n{}\r\n{}\r\nContent-Length: 0\r\n\r\n",
            header_of(request, "Via"),
            header_of(request, "From"),
            header_of(request, "To"),
            header_of(request, "Call-ID"),
            header_of(request, "CSeq"),
        )
    }

    struct Door {
        caller: Caller,
        controller: Arc<SessionController>,
        events: Arc<RecordingSink>,
    }

    impl Door {
        fn kinds(&self) -> Vec<EventKind> {
            self.events
                .snapshot()
                .into_iter()
                .map(|event| event.kind)
                .collect()
        }

        fn of_kind(&self, kind: EventKind) -> Vec<call_events::CallEvent> {
            self.events
                .snapshot()
                .into_iter()
                .filter(|event| event.kind == kind)
                .collect()
        }

        async fn settled(&self) -> Vec<EventKind> {
            for _ in 0..200 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let seen = self.kinds();
                if !seen.is_empty() {
                    return seen;
                }
            }
            self.kinds()
        }

        async fn awaiting(&self, kind: EventKind) -> Vec<call_events::CallEvent> {
            for _ in 0..1_400 {
                let seen = self.of_kind(kind);
                if !seen.is_empty() {
                    return seen;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Vec::new()
        }

        async fn answer(&self, external_id: &str) -> Result<proto::Session, tonic::Status> {
            self.controller
                .answer_session(TonicRequest::new(session_ref(external_id)))
                .await
                .map(|response| response.into_inner())
        }

        async fn hangup(&self, external_id: &str, reason: &str) -> Result<(), tonic::Status> {
            self.controller
                .hangup_session(TonicRequest::new(proto::HangupRequest {
                    session: Some(session_ref(external_id)),
                    reason: reason.to_string(),
                }))
                .await
                .map(|_| ())
        }

        async fn attach(&self, external_id: &str, authoritative: bool) -> String {
            self.controller
                .attach(TonicRequest::new(proto::AttachRequest {
                    session: Some(session_ref(external_id)),
                    transport: proto::Transport::GrpcStream as i32,
                    capabilities: vec![
                        proto::Capability::Sink as i32,
                        proto::Capability::Events as i32,
                    ],
                    selector: None,
                    format: None,
                    authoritative,
                    label: if authoritative { "bridge" } else { "analytics" }.to_string(),
                    endpoint: "grpc:".to_string(),
                    group: String::new(),
                    metadata: Default::default(),
                    idempotency_key: String::new(),
                }))
                .await
                .expect("the attachment was accepted")
                .into_inner()
                .attachment_id
        }

        fn report_end_of_interaction(&self, attachment: &str, reason: &str) {
            let attachment =
                session_core::AttachmentId::from_str(attachment).expect("an attachment id");
            self.controller
                .record_report(
                    attachment,
                    session_core::ConsumerEvent::EndOfInteraction {
                        reason: reason.to_string(),
                    },
                )
                .expect("the report was accepted");
        }
    }

    async fn door_and_caller() -> Caller {
        door_in(AnswerMode::Immediate, Duration::ZERO).await.caller
    }

    async fn door_and_caller_publishing(call_events: Option<Arc<dyn CallEventSink>>) -> Caller {
        opened(
            AnswerMode::Immediate,
            Duration::ZERO,
            call_events,
            Timings::default(),
        )
        .await
        .caller
    }

    const BRISK_T1: Duration = Duration::from_millis(50);

    async fn door_in(answer_mode: AnswerMode, park_timeout: Duration) -> Door {
        opened(answer_mode, park_timeout, None, Timings::default()).await
    }

    async fn door_timed(answer_mode: AnswerMode, t1: Duration) -> Door {
        opened(
            answer_mode,
            Duration::ZERO,
            None,
            Timings {
                t1,
                t2: t1 * 8,
                t4: t1 * 10,
            },
        )
        .await
    }

    async fn opened(
        answer_mode: AnswerMode,
        park_timeout: Duration,
        call_events: Option<Arc<dyn CallEventSink>>,
        timings: Timings,
    ) -> Door {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("a door socket");
        let listen = socket.local_addr().expect("a bound address");
        let controller = Arc::new(
            control_api::SessionController::new("test-pod")
                .with_media_plane(Arc::new(AnsweringPlane)),
        );
        let recorded = Arc::new(RecordingSink::default());
        let sink: Option<Arc<dyn CallEventSink>> =
            Some(call_events.unwrap_or_else(|| recorded.clone()));
        let config = FrontDoorConfig {
            listen,
            advertised: listen,
            owner: "test-pod".to_string(),
            session_timers: SessionTimerPolicy::default(),
            retry_after: Duration::from_secs(4),
            answer_mode,
            park_timeout,
            timings,
        };
        tokio::spawn(serve_on(
            socket,
            config,
            Arc::clone(&controller),
            std::future::pending::<()>(),
            sink,
        ));
        let caller = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("a caller socket");
        Door {
            caller: Caller {
                socket: caller,
                door: listen,
            },
            controller,
            events: recorded,
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
    async fn an_answered_invite_then_bye_is_published_on_the_call_event_sink() {
        let sink = Arc::new(RecordingSink::default());
        let caller = door_and_caller_publishing(Some(sink.clone())).await;
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
        let events = sink.snapshot();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, call_events::EventKind::Answered);
        assert_eq!(events[0].external_id, "sip-call-1");
        assert_eq!(events[0].group, "7200");
        assert_eq!(events[0].from, "caller");
        assert_eq!(events[1].kind, call_events::EventKind::Ended);
        assert_eq!(events[1].external_id, "sip-call-1");
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

    #[tokio::test]
    async fn a_parked_invite_rings_and_is_published_as_invited_before_anyone_answers() {
        let door = door_in(AnswerMode::Parked, Duration::ZERO).await;
        door.caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let trying = door.caller.hear().await;
        assert!(
            trying.starts_with("SIP/2.0 100 Trying"),
            "a provisional comes first: {trying}"
        );
        let ringing = door.caller.hear().await;
        assert!(
            ringing.starts_with("SIP/2.0 180 Ringing"),
            "a parked call rings rather than answering itself: {ringing}"
        );
        assert!(
            !to_tag_of(&ringing).is_empty(),
            "the early dialog carries the tag the 200 will reuse: {ringing}"
        );
        assert!(
            ringing.contains("Contact:"),
            "the early dialog names where in-dialog requests go: {ringing}"
        );
        let invited = door.awaiting(EventKind::Invited).await;
        assert_eq!(invited.len(), 1, "invited is published exactly once");
        assert_eq!(invited[0].external_id, "sip-call-1");
        assert_eq!(invited[0].group, "7200");
        assert_eq!(invited[0].from, "caller");
        assert_eq!(
            door.kinds(),
            vec![EventKind::Invited],
            "a parked call is not answered until someone says so"
        );
    }

    #[tokio::test]
    async fn answer_session_sends_the_two_hundred_with_the_media_planes_sdp_and_publishes_answered()
    {
        let door = door_in(AnswerMode::Parked, Duration::ZERO).await;
        door.caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let ringing = door.caller.hear_final().await;
        assert!(ringing.starts_with("SIP/2.0 180"), "parked: {ringing}");
        door.awaiting(EventKind::Invited).await;

        let session = door
            .answer("sip-call-1")
            .await
            .expect("the parked call answers");
        assert_eq!(session.external_id, "sip-call-1");
        assert!(session.sdp_answer.contains("m=audio 40120 RTP/AVP 8"));

        let answer = door.caller.hear_final().await;
        assert!(answer.starts_with("SIP/2.0 200 OK"), "answered: {answer}");
        assert!(
            answer.contains("m=audio 40120 RTP/AVP 8"),
            "the media plane's own sdp: {answer}"
        );
        assert_eq!(
            to_tag_of(&answer),
            to_tag_of(&ringing),
            "the 200 must not change the tag the 180 already gave the caller"
        );
        assert_eq!(
            door.kinds(),
            vec![EventKind::Invited, EventKind::Answered],
            "invited then answered, in that order"
        );
    }

    #[tokio::test]
    async fn a_parked_call_that_was_answered_takes_its_ack_and_its_bye_as_any_other_does() {
        let door = door_in(AnswerMode::Parked, Duration::ZERO).await;
        door.caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        door.caller.hear_final().await;
        door.awaiting(EventKind::Invited).await;
        door.answer("sip-call-1").await.expect("answered");
        let answer = door.caller.hear_final().await;
        let tag = to_tag_of(&answer);
        door.caller
            .say(message("ACK", "z9hG4bK-2", "call-1", 1, Some(&tag), "", ""))
            .await;
        door.caller
            .say(message("BYE", "z9hG4bK-3", "call-1", 2, Some(&tag), "", ""))
            .await;
        let closed = door.caller.hear_final().await;
        assert!(
            closed.starts_with("SIP/2.0 200 OK"),
            "the caller's own bye still closes the leg: {closed}"
        );
        assert_eq!(
            door.awaiting(EventKind::Ended).await.len(),
            1,
            "ended is published once"
        );
    }

    #[tokio::test]
    async fn a_park_nobody_answers_in_time_is_refused_480_and_published_as_ended() {
        let door = door_in(AnswerMode::Parked, Duration::from_millis(200)).await;
        door.caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let ringing = door.caller.hear_final().await;
        assert!(ringing.starts_with("SIP/2.0 180"), "parked: {ringing}");
        let refused = door.caller.hear_final().await;
        assert!(
            refused.starts_with("SIP/2.0 480"),
            "an unanswered park is temporarily unavailable: {refused}"
        );
        assert_eq!(
            door.kinds(),
            vec![EventKind::Invited, EventKind::Ended],
            "the park timeout ends the call it invited"
        );
        assert!(
            !door.controller.holds_external_id("sip-call-1"),
            "a timed-out park must not leave a media session behind"
        );
    }

    #[tokio::test]
    async fn a_park_timeout_of_zero_never_fires() {
        let door = door_in(AnswerMode::Parked, Duration::ZERO).await;
        door.caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        door.caller.hear_final().await;
        door.awaiting(EventKind::Invited).await;
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            door.kinds(),
            vec![EventKind::Invited],
            "0 disables the timeout, so the call is still parked"
        );
        door.answer("sip-call-1").await.expect("still answerable");
    }

    async fn answered_call(door: &Door) -> String {
        door.caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let answer = door.caller.hear_final().await;
        assert!(answer.starts_with("SIP/2.0 200 OK"), "answered: {answer}");
        let tag = to_tag_of(&answer);
        door.caller
            .say(message("ACK", "z9hG4bK-2", "call-1", 1, Some(&tag), "", ""))
            .await;
        door.awaiting(EventKind::Answered).await;
        tag
    }

    #[tokio::test]
    async fn hangup_session_sends_a_bye_carrying_this_dialogs_identifiers() {
        let door = door_in(AnswerMode::Immediate, Duration::ZERO).await;
        let tag = answered_call(&door).await;
        door.hangup("sip-call-1", "the agent said goodbye")
            .await
            .expect("the hangup was accepted");

        let bye = door.caller.hear_request("BYE").await;
        assert!(
            bye.starts_with("BYE sip:caller@127.0.0.1:5060 SIP/2.0"),
            "the bye goes to the peer's own contact: {bye}"
        );
        assert_eq!(header_of(&bye, "Call-ID"), "Call-ID: call-1");
        assert_eq!(
            header_of(&bye, "CSeq"),
            "CSeq: 2 BYE",
            "the local sequence advances past the invite: {bye}"
        );
        assert!(
            header_of(&bye, "From").contains(&format!("tag={tag}")),
            "our own tag becomes the From tag on a request we send: {bye}"
        );
        assert!(
            header_of(&bye, "To").contains("tag=caller-tag"),
            "the caller's tag becomes the To tag: {bye}"
        );
        assert!(
            header_of(&bye, "Via").contains("branch=z9hG4bK"),
            "the bye is its own transaction with its own magic-cookie branch: {bye}"
        );
        assert_eq!(
            door.kinds(),
            vec![EventKind::Answered, EventKind::Ended],
            "a hangup publishes ended just as a received bye does"
        );
    }

    #[tokio::test]
    async fn a_two_hundred_to_our_bye_ends_the_transaction_and_the_session_is_gone() {
        let door = door_in(AnswerMode::Immediate, Duration::ZERO).await;
        answered_call(&door).await;
        door.hangup("sip-call-1", "done").await.expect("accepted");
        let bye = door.caller.hear_request("BYE").await;
        door.caller.say(two_hundred_for(&bye)).await;
        for _ in 0..200 {
            if !door.controller.holds_external_id("sip-call-1") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !door.controller.holds_external_id("sip-call-1"),
            "a hung-up call must not leave a media session behind"
        );
        let quiet = tokio::time::timeout(
            Duration::from_millis(1_200),
            door.caller
                .socket
                .recv_from(&mut vec![0u8; DATAGRAM_CEILING]),
        )
        .await;
        assert!(
            quiet.is_err(),
            "an answered bye is never repeated: {quiet:?}"
        );
    }

    #[tokio::test]
    async fn a_bye_is_repeated_until_the_peer_answers_it() {
        let door = door_in(AnswerMode::Immediate, Duration::ZERO).await;
        answered_call(&door).await;
        door.hangup("sip-call-1", "done").await.expect("accepted");
        let first = door.caller.hear_request("BYE").await;
        let repeated = door.caller.hear_request("BYE").await;
        assert_eq!(
            header_of(&first, "Via"),
            header_of(&repeated, "Via"),
            "a retransmission is the same transaction, not a new one"
        );
        assert_eq!(header_of(&first, "CSeq"), header_of(&repeated, "CSeq"));
        door.caller.say(two_hundred_for(&repeated)).await;
        let quiet = tokio::time::timeout(
            Duration::from_millis(1_500),
            door.caller
                .socket
                .recv_from(&mut vec![0u8; DATAGRAM_CEILING]),
        )
        .await;
        assert!(
            quiet.is_err(),
            "the answer stops the retransmissions: {quiet:?}"
        );
    }

    #[tokio::test]
    async fn hangup_session_on_a_parked_invite_refuses_it_480_and_sends_no_bye() {
        let door = door_in(AnswerMode::Parked, Duration::ZERO).await;
        door.caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        door.caller.hear_final().await;
        door.awaiting(EventKind::Invited).await;
        door.hangup("sip-call-1", "the orchestrator declined")
            .await
            .expect("accepted");
        let refused = door.caller.hear_final().await;
        assert!(
            refused.starts_with("SIP/2.0 480"),
            "an unanswered invite is refused, never byed: {refused}"
        );
        assert_eq!(door.kinds(), vec![EventKind::Invited, EventKind::Ended]);
    }

    #[tokio::test]
    async fn answering_a_parked_call_twice_answers_it_once() {
        let door = door_in(AnswerMode::Parked, Duration::ZERO).await;
        door.caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        door.caller.hear_final().await;
        door.awaiting(EventKind::Invited).await;
        let first = door.answer("sip-call-1").await.expect("answered");
        let second = door.answer("sip-call-1").await.expect("answered again");
        assert_eq!(
            first.session_id, second.session_id,
            "the second answer returns the same session"
        );
        assert_eq!(first.sdp_answer, second.sdp_answer);
        door.caller.hear_final().await;
        assert_eq!(
            door.kinds(),
            vec![EventKind::Invited, EventKind::Answered],
            "answered is published once however many times it is asked for"
        );
    }

    #[tokio::test]
    async fn hanging_a_call_up_twice_sends_one_bye() {
        let door = door_in(AnswerMode::Immediate, Duration::ZERO).await;
        answered_call(&door).await;
        door.hangup("sip-call-1", "done").await.expect("accepted");
        let bye = door.caller.hear_request("BYE").await;
        door.hangup("sip-call-1", "done again")
            .await
            .expect("a repeat hangup is accepted, not refused");
        door.caller.say(two_hundred_for(&bye)).await;
        assert_eq!(
            door.kinds(),
            vec![EventKind::Answered, EventKind::Ended],
            "ended is published once however many times a hangup is asked for"
        );
    }

    #[tokio::test]
    async fn answer_session_for_a_session_the_door_does_not_hold_is_refused() {
        let door = door_in(AnswerMode::Parked, Duration::ZERO).await;
        let unknown = door.answer("sip-nobody-called").await.expect_err("refused");
        assert_eq!(
            unknown.code(),
            tonic::Code::NotFound,
            "a session nobody ever created: {unknown:?}"
        );
        door.controller
            .create_session(TonicRequest::new(proto::CreateSessionRequest {
                external_id: "over-grpc".to_string(),
                kind: proto::SessionKind::Tap as i32,
                call_id: "call-9".to_string(),
                from_tags: Vec::new(),
                rtpengine_node: String::new(),
                mix: false,
                idempotency_key: String::new(),
                sdp_offer: String::new(),
                group: String::new(),
            }))
            .await
            .expect("a tap session over the control api");
        let not_ours = door.answer("over-grpc").await.expect_err("refused");
        assert_eq!(
            not_ours.code(),
            tonic::Code::FailedPrecondition,
            "a session that never came in over sip has no invite to answer: {not_ours:?}"
        );
    }

    #[tokio::test]
    async fn hanging_up_a_call_nobody_holds_is_accepted_as_already_done() {
        let door = door_in(AnswerMode::Immediate, Duration::ZERO).await;
        door.hangup("sip-never-happened", "done")
            .await
            .expect("a hangup of a call that is already gone is not an error");
        assert!(door.kinds().is_empty());
    }

    #[tokio::test]
    async fn end_of_interaction_is_published_once_when_the_authoritative_attachment_reports_it() {
        let door = door_in(AnswerMode::Immediate, Duration::ZERO).await;
        answered_call(&door).await;
        let bridge = door.attach("sip-call-1", true).await;
        door.report_end_of_interaction(&bridge, "the caller said goodbye");
        door.report_end_of_interaction(&bridge, "and said it twice");
        let reported = door.awaiting(EventKind::EndOfInteraction).await;
        assert_eq!(
            reported.len(),
            1,
            "end_of_interaction is published once per session"
        );
        assert_eq!(reported[0].external_id, "sip-call-1");
        assert_eq!(reported[0].group, "7200");
        assert_eq!(reported[0].reason, "the caller said goodbye");
        assert!(
            door.controller.holds_external_id("sip-call-1"),
            "the media plane does not tear the call down itself; call control does"
        );
    }

    #[tokio::test]
    async fn a_non_authoritative_attachment_does_not_speak_for_the_call() {
        let door = door_in(AnswerMode::Immediate, Duration::ZERO).await;
        answered_call(&door).await;
        let analytics = door.attach("sip-call-1", false).await;
        door.report_end_of_interaction(&analytics, "analytics thinks it is over");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            door.kinds(),
            vec![EventKind::Answered],
            "only the authoritative attachment may end an interaction"
        );
    }

    #[tokio::test]
    async fn immediate_mode_neither_rings_nor_publishes_invited() {
        let door = door_in(AnswerMode::Immediate, Duration::ZERO).await;
        door.caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let trying = door.caller.hear().await;
        assert!(trying.starts_with("SIP/2.0 100 Trying"), "{trying}");
        let answer = door.caller.hear().await;
        assert!(
            answer.starts_with("SIP/2.0 200 OK"),
            "the default mode answers straight away, with no 180 in between: {answer}"
        );
        assert_eq!(
            door.settled().await,
            vec![EventKind::Answered],
            "immediate mode publishes what it always did and nothing more"
        );
    }

    #[tokio::test]
    async fn an_answered_call_whose_ack_never_arrives_ends_rather_than_leaking() {
        let door = door_timed(AnswerMode::Immediate, BRISK_T1).await;
        door.caller
            .say(message("INVITE", "z9hG4bK-1", "call-1", 1, None, "", OFFER))
            .await;
        let answer = door.caller.hear_final().await;
        assert!(answer.starts_with("SIP/2.0 200 OK"), "answered: {answer}");
        door.awaiting(EventKind::Answered).await;
        assert_eq!(
            door.awaiting(EventKind::Ended).await.len(),
            1,
            "at 64xT1 with no ACK the media session is ended, not held forever"
        );
        assert!(
            !door.controller.holds_external_id("sip-call-1"),
            "an unacknowledged answer must not leak a session"
        );
    }

    #[tokio::test]
    async fn a_bye_nobody_ever_answers_is_given_up_on_and_the_session_is_already_gone() {
        let door = door_timed(AnswerMode::Immediate, BRISK_T1).await;
        answered_call(&door).await;
        door.hangup("sip-call-1", "done").await.expect("accepted");
        door.caller.hear_request("BYE").await;
        for _ in 0..400 {
            if !door.controller.holds_external_id("sip-call-1") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !door.controller.holds_external_id("sip-call-1"),
            "the media session ends when the hangup is asked for, not when the peer agrees"
        );
        assert_eq!(
            door.kinds(),
            vec![EventKind::Answered, EventKind::Ended],
            "a bye timer F gives up on publishes nothing further"
        );
        door.caller
            .say(message("INVITE", "z9hG4bK-9", "call-2", 1, None, "", OFFER))
            .await;
        let next = door.caller.hear_final().await;
        assert!(
            next.starts_with("SIP/2.0 200 OK"),
            "an unanswered bye does not stop the next call: {next}"
        );
    }

    #[tokio::test]
    async fn the_answer_mode_is_named_in_the_environment_and_a_typo_is_refused() {
        assert_eq!(
            AnswerMode::from_configured("immediate"),
            Some(AnswerMode::Immediate)
        );
        assert_eq!(AnswerMode::from_configured(""), Some(AnswerMode::Immediate));
        assert_eq!(
            AnswerMode::from_configured("  PARKED "),
            Some(AnswerMode::Parked)
        );
        assert_eq!(AnswerMode::from_configured("park"), None);
        assert_eq!(AnswerMode::default(), AnswerMode::Immediate);
        assert_eq!(AnswerMode::Parked.as_configured(), "parked");
    }
}
