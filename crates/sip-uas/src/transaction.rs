use crate::message::parse_without_trusting_the_network;
use crate::timing::{Reliability, Timings};
use rvoip_sip_core::builder::SimpleResponseBuilder;
use rvoip_sip_core::prelude::{Message, Method, StatusCode};
use rvoip_sip_core::types::sip_request::Request;
use rvoip_sip_core::types::sip_response::Response;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

const BRANCH_MAGIC_COOKIE: &str = "z9hG4bK";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionKey {
    branch: String,
    sent_by: String,
    method: Method,
}

impl TransactionKey {
    pub fn branch(&self) -> &str {
        &self.branch
    }

    pub fn sent_by(&self) -> &str {
        &self.sent_by
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    fn for_invite_of(&self) -> TransactionKey {
        TransactionKey {
            branch: self.branch.clone(),
            sent_by: self.sent_by.clone(),
            method: Method::Invite,
        }
    }
}

impl fmt::Display for TransactionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} {} via {}",
            self.method, self.branch, self.sent_by
        )
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RespondError {
    #[error("no live transaction matches {0}")]
    NoSuchTransaction(TransactionKey),
    #[error("a transaction in {state} cannot send a {status} response")]
    OutOfOrder { state: &'static str, status: u16 },
}

#[derive(Debug)]
pub enum Event {
    Request {
        request: Box<Request>,
        from: SocketAddr,
    },
    Response(Box<Response>),
    Acknowledged,
    Cancelled,
    AckNeverArrived,
    TimedOut,
    Terminated,
}

#[derive(Debug)]
pub enum Action {
    Send { datagram: Vec<u8>, to: SocketAddr },
    Deliver { key: TransactionKey, event: Event },
}

pub fn key_parts(branch: String, sent_by: String, method: Method) -> Option<TransactionKey> {
    if !branch.starts_with(BRANCH_MAGIC_COOKIE) {
        return None;
    }
    Some(TransactionKey {
        branch,
        sent_by,
        method,
    })
}

pub fn transaction_key(request: &Request) -> Option<TransactionKey> {
    let via = request.first_via()?;
    let branch = via.branch()?;
    let sent_by_header = via.0.first()?;
    let host = sent_by_header.sent_by_host.to_string();
    let sent_by = match sent_by_header.sent_by_port {
        Some(port) => format!("{host}:{port}"),
        None => host,
    };
    let method = match request.method() {
        Method::Ack => Method::Invite,
        other => other,
    };
    key_parts(branch.to_string(), sent_by, method)
}

fn built_response(request: &Request, status: StatusCode) -> Vec<u8> {
    let response = SimpleResponseBuilder::response_from_request(request, status, None).build();
    Message::Response(response).to_bytes()
}

fn dialog_identity(request: &Request) -> Option<(String, u32)> {
    let call_id = request.call_id()?.to_string();
    let cseq = request.cseq()?.seq;
    Some((call_id, cseq))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InviteState {
    Proceeding,
    Completed,
    Confirmed,
    Accepted,
    Terminated,
}

impl InviteState {
    fn name(&self) -> &'static str {
        match self {
            InviteState::Proceeding => "proceeding",
            InviteState::Completed => "completed",
            InviteState::Confirmed => "confirmed",
            InviteState::Accepted => "accepted",
            InviteState::Terminated => "terminated",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlainState {
    Trying,
    Proceeding,
    Completed,
    Terminated,
}

impl PlainState {
    fn name(&self) -> &'static str {
        match self {
            PlainState::Trying => "trying",
            PlainState::Proceeding => "proceeding",
            PlainState::Completed => "completed",
            PlainState::Terminated => "terminated",
        }
    }
}

struct InviteServer {
    key: TransactionKey,
    remote: SocketAddr,
    call_id: String,
    cseq: u32,
    state: InviteState,
    provisional: Option<Vec<u8>>,
    final_response: Option<Vec<u8>>,
    retransmit_at: Option<Duration>,
    retransmit_interval: Duration,
    give_up_at: Option<Duration>,
    absorb_until: Option<Duration>,
    acknowledged: bool,
}

struct PlainServer {
    remote: SocketAddr,
    state: PlainState,
    provisional: Option<Vec<u8>>,
    final_response: Option<Vec<u8>>,
    absorb_until: Option<Duration>,
}

enum ServerTransaction {
    Invite(InviteServer),
    Plain(PlainServer),
}

impl ServerTransaction {
    fn is_terminated(&self) -> bool {
        match self {
            ServerTransaction::Invite(invite) => invite.state == InviteState::Terminated,
            ServerTransaction::Plain(plain) => plain.state == PlainState::Terminated,
        }
    }

    fn next_deadline(&self) -> Option<Duration> {
        let deadlines = match self {
            ServerTransaction::Invite(invite) => {
                [invite.retransmit_at, invite.give_up_at, invite.absorb_until]
            }
            ServerTransaction::Plain(plain) => [None, None, plain.absorb_until],
        };
        deadlines.into_iter().flatten().min()
    }
}

fn send(remote: SocketAddr, datagram: Vec<u8>) -> Action {
    Action::Send {
        datagram,
        to: remote,
    }
}

impl InviteServer {
    fn opened(key: TransactionKey, request: &Request, remote: SocketAddr) -> (Self, Vec<Action>) {
        let (call_id, cseq) = dialog_identity(request).unwrap_or_default();
        let trying = built_response(request, StatusCode::Trying);
        let invite = InviteServer {
            key: key.clone(),
            remote,
            call_id,
            cseq,
            state: InviteState::Proceeding,
            provisional: Some(trying.clone()),
            final_response: None,
            retransmit_at: None,
            retransmit_interval: Duration::ZERO,
            give_up_at: None,
            absorb_until: None,
            acknowledged: false,
        };
        let actions = vec![
            send(remote, trying),
            Action::Deliver {
                key,
                event: Event::Request {
                    request: Box::new(request.clone()),
                    from: remote,
                },
            },
        ];
        (invite, actions)
    }

    fn on_retransmitted_invite(&self) -> Vec<Action> {
        match self.state {
            InviteState::Proceeding => self
                .provisional
                .clone()
                .map(|datagram| vec![send(self.remote, datagram)])
                .unwrap_or_default(),
            InviteState::Completed | InviteState::Accepted => self
                .final_response
                .clone()
                .map(|datagram| vec![send(self.remote, datagram)])
                .unwrap_or_default(),
            InviteState::Confirmed | InviteState::Terminated => Vec::new(),
        }
    }

    fn on_ack(
        &mut self,
        now: Duration,
        timings: &Timings,
        reliability: Reliability,
    ) -> Vec<Action> {
        match self.state {
            InviteState::Completed => {
                self.state = InviteState::Confirmed;
                self.retransmit_at = None;
                self.give_up_at = None;
                let window = reliability.absorbs_retransmissions_for(timings.t4);
                if window.is_zero() {
                    self.state = InviteState::Terminated;
                } else {
                    self.absorb_until = Some(now + window);
                }
                Vec::new()
            }
            InviteState::Accepted => {
                let first_ack = !self.acknowledged;
                self.acknowledged = true;
                self.retransmit_at = None;
                if first_ack {
                    vec![Action::Deliver {
                        key: self.key.clone(),
                        event: Event::Acknowledged,
                    }]
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }

    fn respond(
        &mut self,
        response: Response,
        now: Duration,
        timings: &Timings,
        reliability: Reliability,
    ) -> Result<Vec<Action>, RespondError> {
        let status = response.status_code();
        let datagram = Message::Response(response).to_bytes();
        match (self.state, status) {
            (InviteState::Proceeding, 100..=199) => {
                self.provisional = Some(datagram.clone());
                Ok(vec![send(self.remote, datagram)])
            }
            (InviteState::Proceeding, 200..=299) => {
                self.state = InviteState::Accepted;
                self.final_response = Some(datagram.clone());
                self.retransmit_interval = timings.first_retransmit_interval();
                self.retransmit_at = reliability
                    .retransmits_responses()
                    .then(|| now + self.retransmit_interval);
                self.give_up_at = Some(now + timings.give_up_after());
                Ok(vec![send(self.remote, datagram)])
            }
            (InviteState::Proceeding, 300..=699) => {
                self.state = InviteState::Completed;
                self.final_response = Some(datagram.clone());
                self.retransmit_interval = timings.first_retransmit_interval();
                self.retransmit_at = reliability
                    .retransmits_responses()
                    .then(|| now + self.retransmit_interval);
                self.give_up_at = Some(now + timings.give_up_after());
                Ok(vec![send(self.remote, datagram)])
            }
            (InviteState::Accepted, 200..=299) => {
                self.final_response = Some(datagram.clone());
                Ok(vec![send(self.remote, datagram)])
            }
            (state, status) => Err(RespondError::OutOfOrder {
                state: state.name(),
                status,
            }),
        }
    }

    fn poll(&mut self, now: Duration, timings: &Timings) -> Vec<Action> {
        let mut actions = Vec::new();
        if let Some(due) = self.retransmit_at {
            if now >= due {
                if let Some(datagram) = self.final_response.clone() {
                    actions.push(send(self.remote, datagram));
                }
                self.retransmit_interval =
                    timings.next_retransmit_interval(self.retransmit_interval);
                self.retransmit_at = Some(now + self.retransmit_interval);
            }
        }
        if let Some(due) = self.give_up_at {
            if now >= due {
                self.retransmit_at = None;
                self.give_up_at = None;
                self.state = InviteState::Terminated;
                if !self.acknowledged {
                    actions.push(Action::Deliver {
                        key: self.key.clone(),
                        event: Event::AckNeverArrived,
                    });
                }
            }
        }
        if let Some(due) = self.absorb_until {
            if now >= due {
                self.absorb_until = None;
                self.state = InviteState::Terminated;
            }
        }
        actions
    }
}

impl PlainServer {
    fn opened(key: TransactionKey, request: &Request, remote: SocketAddr) -> (Self, Vec<Action>) {
        let plain = PlainServer {
            remote,
            state: PlainState::Trying,
            provisional: None,
            final_response: None,
            absorb_until: None,
        };
        let actions = vec![Action::Deliver {
            key,
            event: Event::Request {
                request: Box::new(request.clone()),
                from: remote,
            },
        }];
        (plain, actions)
    }

    fn on_retransmitted_request(&self) -> Vec<Action> {
        let replay = match self.state {
            PlainState::Trying => None,
            PlainState::Proceeding => self.provisional.clone(),
            PlainState::Completed => self.final_response.clone(),
            PlainState::Terminated => None,
        };
        replay
            .map(|datagram| vec![send(self.remote, datagram)])
            .unwrap_or_default()
    }

    fn respond(
        &mut self,
        response: Response,
        now: Duration,
        timings: &Timings,
        reliability: Reliability,
    ) -> Result<Vec<Action>, RespondError> {
        let status = response.status_code();
        let datagram = Message::Response(response).to_bytes();
        match (self.state, status) {
            (PlainState::Trying | PlainState::Proceeding, 100..=199) => {
                self.state = PlainState::Proceeding;
                self.provisional = Some(datagram.clone());
                Ok(vec![send(self.remote, datagram)])
            }
            (PlainState::Trying | PlainState::Proceeding, 200..=699) => {
                self.state = PlainState::Completed;
                self.final_response = Some(datagram.clone());
                let window = reliability.absorbs_retransmissions_for(timings.give_up_after());
                if window.is_zero() {
                    self.state = PlainState::Terminated;
                } else {
                    self.absorb_until = Some(now + window);
                }
                Ok(vec![send(self.remote, datagram)])
            }
            (state, status) => Err(RespondError::OutOfOrder {
                state: state.name(),
                status,
            }),
        }
    }

    fn answered_immediately(
        request: &Request,
        remote: SocketAddr,
        status: StatusCode,
        now: Duration,
        timings: &Timings,
        reliability: Reliability,
    ) -> (Self, Vec<Action>) {
        let datagram = built_response(request, status);
        let window = reliability.absorbs_retransmissions_for(timings.give_up_after());
        let plain = PlainServer {
            remote,
            state: if window.is_zero() {
                PlainState::Terminated
            } else {
                PlainState::Completed
            },
            provisional: None,
            final_response: Some(datagram.clone()),
            absorb_until: (!window.is_zero()).then(|| now + window),
        };
        (plain, vec![send(remote, datagram)])
    }

    fn poll(&mut self, now: Duration) -> Vec<Action> {
        if let Some(due) = self.absorb_until {
            if now >= due {
                self.absorb_until = None;
                self.state = PlainState::Terminated;
            }
        }
        Vec::new()
    }
}

pub struct ServerTransactions {
    timings: Timings,
    reliability: Reliability,
    live: HashMap<TransactionKey, ServerTransaction>,
}

impl ServerTransactions {
    pub fn new(timings: Timings, reliability: Reliability) -> Self {
        ServerTransactions {
            timings,
            reliability,
            live: HashMap::new(),
        }
    }

    pub fn live(&self) -> usize {
        self.live.len()
    }

    pub fn holds(&self, key: &TransactionKey) -> bool {
        self.live.contains_key(key)
    }

    pub fn on_datagram(&mut self, datagram: &[u8], from: SocketAddr, now: Duration) -> Vec<Action> {
        let Some(Message::Request(request)) = parse_without_trusting_the_network(datagram) else {
            return Vec::new();
        };
        self.on_request_message(&request, from, now)
    }

    pub fn on_request_message(
        &mut self,
        request: &Request,
        from: SocketAddr,
        now: Duration,
    ) -> Vec<Action> {
        let Some(key) = transaction_key(request) else {
            return Vec::new();
        };
        match request.method() {
            Method::Ack => self.on_ack(request, &key, now),
            Method::Cancel => self.on_cancel(request, key, from, now),
            _ => self.on_request(request, key, from),
        }
    }

    fn on_request(
        &mut self,
        request: &Request,
        key: TransactionKey,
        from: SocketAddr,
    ) -> Vec<Action> {
        if let Some(existing) = self.live.get(&key) {
            return match existing {
                ServerTransaction::Invite(invite) => invite.on_retransmitted_invite(),
                ServerTransaction::Plain(plain) => plain.on_retransmitted_request(),
            };
        }
        if request.method() == Method::Invite {
            let (invite, actions) = InviteServer::opened(key.clone(), request, from);
            self.live.insert(key, ServerTransaction::Invite(invite));
            actions
        } else {
            let (plain, actions) = PlainServer::opened(key.clone(), request, from);
            self.live.insert(key, ServerTransaction::Plain(plain));
            actions
        }
    }

    fn on_ack(&mut self, request: &Request, key: &TransactionKey, now: Duration) -> Vec<Action> {
        let timings = self.timings;
        let reliability = self.reliability;
        if let Some(ServerTransaction::Invite(invite)) = self.live.get_mut(key) {
            return invite.on_ack(now, &timings, reliability);
        }
        let Some((call_id, cseq)) = dialog_identity(request) else {
            return Vec::new();
        };
        let accepted = self
            .live
            .values_mut()
            .find_map(|transaction| match transaction {
                ServerTransaction::Invite(invite)
                    if invite.state == InviteState::Accepted
                        && invite.call_id == call_id
                        && invite.cseq == cseq =>
                {
                    Some(invite)
                }
                _ => None,
            });
        match accepted {
            Some(invite) => invite.on_ack(now, &timings, reliability),
            None => Vec::new(),
        }
    }

    fn on_cancel(
        &mut self,
        request: &Request,
        key: TransactionKey,
        from: SocketAddr,
        now: Duration,
    ) -> Vec<Action> {
        if let Some(ServerTransaction::Plain(plain)) = self.live.get(&key) {
            return plain.on_retransmitted_request();
        }
        let invite_key = key.for_invite_of();
        let cancels = matches!(
            self.live.get(&invite_key),
            Some(ServerTransaction::Invite(invite)) if invite.state == InviteState::Proceeding
        );
        let status = if cancels {
            StatusCode::Ok
        } else {
            StatusCode::CallOrTransactionDoesNotExist
        };
        let (plain, mut actions) = PlainServer::answered_immediately(
            request,
            from,
            status,
            now,
            &self.timings,
            self.reliability,
        );
        self.live.insert(key, ServerTransaction::Plain(plain));
        if cancels {
            actions.push(Action::Deliver {
                key: invite_key,
                event: Event::Cancelled,
            });
        }
        actions
    }

    pub fn respond(
        &mut self,
        key: &TransactionKey,
        response: Response,
        now: Duration,
    ) -> Result<Vec<Action>, RespondError> {
        let timings = self.timings;
        let reliability = self.reliability;
        let transaction = self
            .live
            .get_mut(key)
            .ok_or_else(|| RespondError::NoSuchTransaction(key.clone()))?;
        match transaction {
            ServerTransaction::Invite(invite) => {
                invite.respond(response, now, &timings, reliability)
            }
            ServerTransaction::Plain(plain) => plain.respond(response, now, &timings, reliability),
        }
    }

    pub fn next_deadline(&self) -> Option<Duration> {
        self.live
            .values()
            .filter_map(|transaction| transaction.next_deadline())
            .min()
    }

    pub fn poll(&mut self, now: Duration) -> Vec<Action> {
        let timings = self.timings;
        let mut actions = Vec::new();
        for transaction in self.live.values_mut() {
            match transaction {
                ServerTransaction::Invite(invite) => actions.extend(invite.poll(now, &timings)),
                ServerTransaction::Plain(plain) => actions.extend(plain.poll(now)),
            }
        }
        let finished: Vec<TransactionKey> = self
            .live
            .iter()
            .filter(|(_, transaction)| transaction.is_terminated())
            .map(|(key, _)| key.clone())
            .collect();
        for key in finished {
            self.live.remove(&key);
            actions.push(Action::Deliver {
                key,
                event: Event::Terminated,
            });
        }
        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer() -> SocketAddr {
        "172.31.99.80:5060".parse().expect("a test peer address")
    }

    fn other_peer() -> SocketAddr {
        "172.31.99.81:5060".parse().expect("a test peer address")
    }

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    fn datagram(method: &str, branch: &str, call_id: &str, cseq: u32, body: &str) -> Vec<u8> {
        format!(
            "{method} sip:7200@172.31.99.14:5080 SIP/2.0\r\n\
Via: SIP/2.0/UDP 172.31.99.80:5060;branch={branch};rport\r\n\
From: <sip:tester@172.31.99.80>;tag=caller\r\n\
To: <sip:7200@172.31.99.14>\r\n\
Call-ID: {call_id}\r\n\
CSeq: {cseq} {method}\r\n\
Max-Forwards: 70\r\n\
Contact: <sip:tester@172.31.99.80:5060>\r\n\
Content-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes()
    }

    fn invite(branch: &str) -> Vec<u8> {
        datagram("INVITE", branch, "call-one", 1, "")
    }

    fn response(request: &Request, status: StatusCode) -> Response {
        SimpleResponseBuilder::response_from_request(request, status, None).build()
    }

    fn delivered_request(actions: &[Action]) -> Request {
        actions
            .iter()
            .find_map(|action| match action {
                Action::Deliver {
                    event: Event::Request { request, .. },
                    ..
                } => Some((**request).clone()),
                _ => None,
            })
            .expect("a request delivered to the application")
    }

    fn delivered_key(actions: &[Action]) -> TransactionKey {
        actions
            .iter()
            .find_map(|action| match action {
                Action::Deliver {
                    key,
                    event: Event::Request { .. },
                } => Some(key.clone()),
                _ => None,
            })
            .expect("a key delivered to the application")
    }

    fn sent_status_lines(actions: &[Action]) -> Vec<String> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { datagram, .. } => Some(
                    String::from_utf8_lossy(datagram)
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .trim()
                        .to_string(),
                ),
                _ => None,
            })
            .collect()
    }

    fn events(actions: &[Action]) -> Vec<&'static str> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Deliver { event, .. } => Some(match event {
                    Event::Request { .. } => "request",
                    Event::Response(_) => "response",
                    Event::Acknowledged => "acknowledged",
                    Event::Cancelled => "cancelled",
                    Event::AckNeverArrived => "ack-never-arrived",
                    Event::TimedOut => "timed-out",
                    Event::Terminated => "terminated",
                }),
                _ => None,
            })
            .collect()
    }

    fn unreliable() -> ServerTransactions {
        ServerTransactions::new(Timings::default(), Reliability::Unreliable)
    }

    fn answered_invite(layer: &mut ServerTransactions) -> (TransactionKey, Request) {
        let opened = layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(0));
        let key = delivered_key(&opened);
        let request = delivered_request(&opened);
        let answered = layer
            .respond(&key, response(&request, StatusCode::Ok), ms(0))
            .expect("a 200 in proceeding");
        assert_eq!(sent_status_lines(&answered), vec!["SIP/2.0 200 OK"]);
        (key, request)
    }

    #[test]
    fn an_invite_is_answered_100_trying_before_the_application_has_said_anything() {
        let mut layer = unreliable();
        let actions = layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(0));
        assert_eq!(sent_status_lines(&actions), vec!["SIP/2.0 100 Trying"]);
        assert_eq!(events(&actions), vec!["request"]);
        assert_eq!(layer.live(), 1);
    }

    #[test]
    fn a_retransmitted_invite_replays_the_provisional_and_never_reaches_the_application_twice() {
        let mut layer = unreliable();
        layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(0));
        let again = layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(500));
        assert_eq!(sent_status_lines(&again), vec!["SIP/2.0 100 Trying"]);
        assert!(
            events(&again).is_empty(),
            "a retransmission must not seat a second session: {:?}",
            events(&again)
        );
        assert_eq!(layer.live(), 1);
    }

    #[test]
    fn a_retransmitted_invite_after_the_answer_replays_the_two_hundred() {
        let mut layer = unreliable();
        answered_invite(&mut layer);
        let again = layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(700));
        assert_eq!(sent_status_lines(&again), vec!["SIP/2.0 200 OK"]);
        assert!(events(&again).is_empty());
    }

    #[test]
    fn a_two_hundred_is_retransmitted_on_the_doubling_schedule_until_the_ack_arrives() {
        let mut layer = unreliable();
        answered_invite(&mut layer);
        let mut fired = Vec::new();
        for now in [500u64, 1500, 3500, 7500, 11500] {
            assert_eq!(
                layer.next_deadline(),
                Some(ms(now)),
                "the next retransmission is due at {now} ms"
            );
            let actions = layer.poll(ms(now));
            fired.push(sent_status_lines(&actions));
        }
        assert_eq!(
            fired,
            vec![vec!["SIP/2.0 200 OK".to_string()]; 5],
            "500 ms, 1 s, 2 s, 4 s then capped at T2"
        );
    }

    #[test]
    fn the_ack_for_a_two_hundred_arrives_on_its_own_branch_and_still_finds_its_invite() {
        let mut layer = unreliable();
        answered_invite(&mut layer);
        let ack = datagram("ACK", "z9hG4bK-a-different-branch", "call-one", 1, "");
        let actions = layer.on_datagram(&ack, peer(), ms(600));
        assert_eq!(
            events(&actions),
            vec!["acknowledged"],
            "a 2xx ACK is a new transaction, so it matches on call-id and cseq"
        );
        assert_eq!(
            layer.next_deadline(),
            Some(ms(32_000)),
            "retransmission stops, only the absorb window is left"
        );
    }

    #[test]
    fn an_invite_whose_ack_never_comes_tells_the_application_at_thirty_two_seconds() {
        let mut layer = unreliable();
        answered_invite(&mut layer);
        let quiet = layer.poll(ms(31_999));
        assert!(
            !events(&quiet).contains(&"ack-never-arrived"),
            "not before the timer is due"
        );
        let expired = layer.poll(ms(32_000));
        assert_eq!(events(&expired), vec!["ack-never-arrived", "terminated"]);
        assert_eq!(layer.live(), 0, "the transaction is not leaked");
    }

    #[test]
    fn an_acknowledged_invite_expires_without_claiming_the_ack_never_arrived() {
        let mut layer = unreliable();
        answered_invite(&mut layer);
        layer.on_datagram(
            &datagram("ACK", "z9hG4bK-new", "call-one", 1, ""),
            peer(),
            ms(600),
        );
        let expired = layer.poll(ms(32_000));
        assert_eq!(events(&expired), vec!["terminated"]);
        assert_eq!(layer.live(), 0);
    }

    #[test]
    fn a_rejected_invite_retransmits_its_final_and_absorbs_the_ack_into_confirmed() {
        let mut layer = unreliable();
        let opened = layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(0));
        let key = delivered_key(&opened);
        let request = delivered_request(&opened);
        layer
            .respond(&key, response(&request, StatusCode::BusyHere), ms(0))
            .expect("a 486 in proceeding");
        let retransmitted = layer.poll(ms(500));
        assert_eq!(
            sent_status_lines(&retransmitted),
            vec!["SIP/2.0 486 Busy Here"]
        );
        let acked = layer.on_datagram(
            &datagram("ACK", "z9hG4bK-one", "call-one", 1, ""),
            peer(),
            ms(600),
        );
        assert!(
            events(&acked).is_empty(),
            "a non-2xx ACK is absorbed, not delivered"
        );
        assert_eq!(
            layer.next_deadline(),
            Some(ms(5_600)),
            "confirmed absorbs ack retransmissions for T4"
        );
        assert!(sent_status_lines(&layer.poll(ms(5_000))).is_empty());
        assert_eq!(events(&layer.poll(ms(5_600))), vec!["terminated"]);
    }

    #[test]
    fn a_cancel_before_the_final_response_is_answered_two_hundred_and_the_invite_is_told() {
        let mut layer = unreliable();
        let opened = layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(0));
        let invite_key = delivered_key(&opened);
        let cancel = datagram("CANCEL", "z9hG4bK-one", "call-one", 1, "");
        let actions = layer.on_datagram(&cancel, peer(), ms(100));
        assert_eq!(sent_status_lines(&actions), vec!["SIP/2.0 200 OK"]);
        assert_eq!(events(&actions), vec!["cancelled"]);
        let cancelled_key = actions
            .iter()
            .find_map(|action| match action {
                Action::Deliver {
                    key,
                    event: Event::Cancelled,
                } => Some(key.clone()),
                _ => None,
            })
            .expect("a cancelled key");
        assert_eq!(
            cancelled_key, invite_key,
            "the cancel names the invite it cancels, not itself"
        );
    }

    #[test]
    fn a_cancel_for_an_invite_that_is_not_live_is_refused_481() {
        let mut layer = unreliable();
        let actions = layer.on_datagram(
            &datagram("CANCEL", "z9hG4bK-nothing", "call-one", 1, ""),
            peer(),
            ms(0),
        );
        assert_eq!(
            sent_status_lines(&actions),
            vec!["SIP/2.0 481 Call/Transaction Does Not Exist"]
        );
        assert!(events(&actions).is_empty());
    }

    #[test]
    fn a_retransmitted_cancel_replays_its_own_two_hundred_and_does_not_cancel_twice() {
        let mut layer = unreliable();
        layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(0));
        let cancel = datagram("CANCEL", "z9hG4bK-one", "call-one", 1, "");
        layer.on_datagram(&cancel, peer(), ms(100));
        let again = layer.on_datagram(&cancel, peer(), ms(600));
        assert_eq!(sent_status_lines(&again), vec!["SIP/2.0 200 OK"]);
        assert!(events(&again).is_empty());
    }

    #[test]
    fn a_bye_is_delivered_once_and_its_final_response_is_replayed_for_thirty_two_seconds() {
        let mut layer = unreliable();
        let bye = datagram("BYE", "z9hG4bK-bye", "call-one", 2, "");
        let opened = layer.on_datagram(&bye, peer(), ms(0));
        assert!(
            sent_status_lines(&opened).is_empty(),
            "a non-invite gets no automatic provisional"
        );
        assert_eq!(events(&opened), vec!["request"]);
        let key = delivered_key(&opened);
        let request = delivered_request(&opened);
        layer
            .respond(&key, response(&request, StatusCode::Ok), ms(0))
            .expect("a 200 to the bye");
        let again = layer.on_datagram(&bye, peer(), ms(1_000));
        assert_eq!(sent_status_lines(&again), vec!["SIP/2.0 200 OK"]);
        assert_eq!(layer.next_deadline(), Some(ms(32_000)));
        assert_eq!(events(&layer.poll(ms(32_000))), vec!["terminated"]);
        assert_eq!(layer.live(), 0);
    }

    #[test]
    fn a_request_retransmitted_before_any_response_is_absorbed_in_silence() {
        let mut layer = unreliable();
        let options = datagram("OPTIONS", "z9hG4bK-opt", "call-one", 3, "");
        layer.on_datagram(&options, peer(), ms(0));
        let again = layer.on_datagram(&options, peer(), ms(500));
        assert!(sent_status_lines(&again).is_empty());
        assert!(events(&again).is_empty());
    }

    #[test]
    fn a_branch_without_the_magic_cookie_is_not_matched_and_opens_nothing() {
        let mut layer = unreliable();
        let actions = layer.on_datagram(&invite("legacy-branch-1"), peer(), ms(0));
        assert!(actions.is_empty());
        assert_eq!(layer.live(), 0);
    }

    #[test]
    fn hostile_datagrams_produce_no_action_and_no_transaction() {
        let mut layer = unreliable();
        let hostile: Vec<Vec<u8>> = vec![
            vec![],
            vec![0u8; 4096],
            b"INVITE".to_vec(),
            b"INVITE sip:x SIP/2.0\r\n".to_vec(),
            b"INVITE sip:x SIP/2.0\r\nContent-Length: 99999\r\n\r\n".to_vec(),
            b"SIP/2.0 200 OK\r\n\r\n".to_vec(),
            vec![0xff, 0xfe, 0xfd, 0x0d, 0x0a, 0x0d, 0x0a],
        ];
        for datagram in hostile {
            let actions = layer.on_datagram(&datagram, peer(), ms(0));
            assert!(
                actions.is_empty(),
                "a hostile datagram must produce nothing: {datagram:?}"
            );
        }
        assert_eq!(layer.live(), 0);
        assert_eq!(layer.next_deadline(), None);
    }

    #[test]
    fn two_invites_on_different_branches_are_two_transactions() {
        let mut layer = unreliable();
        layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(0));
        layer.on_datagram(&invite("z9hG4bK-two"), other_peer(), ms(0));
        assert_eq!(layer.live(), 2);
    }

    #[test]
    fn next_deadline_is_the_earliest_pending_timer_across_every_transaction() {
        let mut layer = unreliable();
        answered_invite(&mut layer);
        let bye = datagram("BYE", "z9hG4bK-bye", "call-two", 2, "");
        let opened = layer.on_datagram(&bye, peer(), ms(10_000));
        let key = delivered_key(&opened);
        let request = delivered_request(&opened);
        layer
            .respond(&key, response(&request, StatusCode::Ok), ms(10_000))
            .expect("a 200 to the bye");
        assert_eq!(
            layer.next_deadline(),
            Some(ms(500)),
            "the invite's retransmission is due long before the bye's absorb window"
        );
    }

    #[test]
    fn a_reliable_transport_neither_retransmits_the_final_nor_waits_to_absorb() {
        let mut layer = ServerTransactions::new(Timings::default(), Reliability::Reliable);
        let bye = datagram("BYE", "z9hG4bK-bye", "call-one", 2, "");
        let opened = layer.on_datagram(&bye, peer(), ms(0));
        let key = delivered_key(&opened);
        let request = delivered_request(&opened);
        layer
            .respond(&key, response(&request, StatusCode::Ok), ms(0))
            .expect("a 200 to the bye");
        assert_eq!(events(&layer.poll(ms(0))), vec!["terminated"]);
        assert_eq!(layer.live(), 0);
    }

    #[test]
    fn an_accepted_invite_waits_for_its_ack_on_a_reliable_transport_too() {
        let mut layer = ServerTransactions::new(Timings::default(), Reliability::Reliable);
        let opened = layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(0));
        let key = delivered_key(&opened);
        let request = delivered_request(&opened);
        layer
            .respond(&key, response(&request, StatusCode::Ok), ms(0))
            .expect("a 200 in proceeding");
        assert_eq!(
            layer.next_deadline(),
            Some(ms(32_000)),
            "no retransmission is armed, but the ack is still waited for"
        );
        assert!(sent_status_lines(&layer.poll(ms(31_000))).is_empty());
    }

    #[test]
    fn a_second_final_response_on_one_transaction_is_refused_by_name() {
        let mut layer = unreliable();
        let (key, request) = answered_invite(&mut layer);
        let refused = layer
            .respond(&key, response(&request, StatusCode::BusyHere), ms(10))
            .expect_err("a second final response is a protocol error");
        assert_eq!(
            refused,
            RespondError::OutOfOrder {
                state: "accepted",
                status: 486
            }
        );
    }

    #[test]
    fn responding_to_a_transaction_that_does_not_exist_says_which_one() {
        let mut layer = unreliable();
        let opened = layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(0));
        let key = delivered_key(&opened);
        let request = delivered_request(&opened);
        layer
            .respond(&key, response(&request, StatusCode::Ok), ms(0))
            .expect("a 200");
        layer.poll(ms(32_000));
        let refused = layer
            .respond(&key, response(&request, StatusCode::Ok), ms(32_001))
            .expect_err("the transaction is gone");
        assert_eq!(refused, RespondError::NoSuchTransaction(key));
    }

    #[test]
    fn a_provisional_can_be_sent_before_the_final_and_is_what_retransmissions_replay() {
        let mut layer = unreliable();
        let opened = layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(0));
        let key = delivered_key(&opened);
        let request = delivered_request(&opened);
        let ringing = layer
            .respond(&key, response(&request, StatusCode::Ringing), ms(10))
            .expect("a 180 in proceeding");
        assert_eq!(sent_status_lines(&ringing), vec!["SIP/2.0 180 Ringing"]);
        let again = layer.on_datagram(&invite("z9hG4bK-one"), peer(), ms(600));
        assert_eq!(sent_status_lines(&again), vec!["SIP/2.0 180 Ringing"]);
    }

    #[test]
    fn a_transaction_key_names_the_branch_the_sender_and_the_method() {
        let parsed =
            parse_without_trusting_the_network(&invite("z9hG4bK-one")).expect("a parsable invite");
        let Message::Request(request) = parsed else {
            panic!("an invite is a request");
        };
        let key = transaction_key(&request).expect("a key");
        assert_eq!(key.branch(), "z9hG4bK-one");
        assert_eq!(key.sent_by(), "172.31.99.80:5060");
        assert_eq!(key.method(), &Method::Invite);
        assert_eq!(key.to_string(), "INVITE z9hG4bK-one via 172.31.99.80:5060");
    }

    #[test]
    fn an_ack_is_keyed_to_the_invite_it_acknowledges_and_not_to_itself() {
        let parsed =
            parse_without_trusting_the_network(&datagram("ACK", "z9hG4bK-one", "call-one", 1, ""))
                .expect("a parsable ack");
        let Message::Request(request) = parsed else {
            panic!("an ack is a request");
        };
        let key = transaction_key(&request).expect("a key");
        assert_eq!(key.method(), &Method::Invite);
    }
}
