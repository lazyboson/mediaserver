use crate::timing::{Reliability, Timings};
use crate::transaction::{Action, Event, TransactionKey};
use rvoip_sip_core::prelude::{Message, Method};
use rvoip_sip_core::types::sip_request::Request;
use rvoip_sip_core::types::sip_response::Response;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BeginError {
    #[error("an INVITE client transaction is not something a media plane opens")]
    InviteIsNotOurs,
    #[error("this request carries no branch parameter on its topmost Via")]
    NoBranch,
    #[error("a client transaction is already live for {0}")]
    AlreadyLive(TransactionKey),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientState {
    Trying,
    Proceeding,
    Completed,
    Terminated,
}

struct PlainClient {
    key: TransactionKey,
    remote: SocketAddr,
    datagram: Vec<u8>,
    state: ClientState,
    retransmit_at: Option<Duration>,
    retransmit_interval: Duration,
    give_up_at: Option<Duration>,
    absorb_until: Option<Duration>,
    transmissions: u32,
}

impl PlainClient {
    fn opened(
        key: TransactionKey,
        datagram: Vec<u8>,
        remote: SocketAddr,
        now: Duration,
        timings: &Timings,
        reliability: Reliability,
    ) -> (Self, Vec<Action>) {
        let retransmit_interval = timings.first_retransmit_interval();
        let client = PlainClient {
            key,
            remote,
            datagram: datagram.clone(),
            state: ClientState::Trying,
            retransmit_at: reliability
                .retransmits_responses()
                .then(|| now + retransmit_interval),
            retransmit_interval,
            give_up_at: Some(now + timings.give_up_after()),
            absorb_until: None,
            transmissions: 1,
        };
        (
            client,
            vec![Action::Send {
                datagram,
                to: remote,
            }],
        )
    }

    fn next_deadline(&self) -> Option<Duration> {
        [self.retransmit_at, self.give_up_at, self.absorb_until]
            .into_iter()
            .flatten()
            .min()
    }

    fn on_response(
        &mut self,
        response: &Response,
        now: Duration,
        timings: &Timings,
        reliability: Reliability,
    ) -> Vec<Action> {
        let status = response.status_code();
        match (self.state, status) {
            (ClientState::Trying | ClientState::Proceeding, 100..=199) => {
                self.state = ClientState::Proceeding;
                self.retransmit_interval = timings.t2;
                self.retransmit_at = reliability
                    .retransmits_responses()
                    .then(|| now + self.retransmit_interval);
                Vec::new()
            }
            (ClientState::Trying | ClientState::Proceeding, 200..=699) => {
                self.state = ClientState::Completed;
                self.retransmit_at = None;
                self.give_up_at = None;
                let window = reliability.absorbs_retransmissions_for(timings.t4);
                if window.is_zero() {
                    self.state = ClientState::Terminated;
                } else {
                    self.absorb_until = Some(now + window);
                }
                vec![Action::Deliver {
                    key: self.key.clone(),
                    event: Event::Response(Box::new(response.clone())),
                }]
            }
            _ => Vec::new(),
        }
    }

    fn poll(&mut self, now: Duration, timings: &Timings) -> Vec<Action> {
        let mut actions = Vec::new();
        if let Some(due) = self.retransmit_at {
            if now >= due {
                actions.push(Action::Send {
                    datagram: self.datagram.clone(),
                    to: self.remote,
                });
                self.transmissions += 1;
                self.retransmit_interval = match self.state {
                    ClientState::Proceeding => timings.t2,
                    _ => timings.next_retransmit_interval(self.retransmit_interval),
                };
                self.retransmit_at = Some(now + self.retransmit_interval);
            }
        }
        if let Some(due) = self.give_up_at {
            if now >= due {
                self.retransmit_at = None;
                self.give_up_at = None;
                self.state = ClientState::Terminated;
                actions.push(Action::Deliver {
                    key: self.key.clone(),
                    event: Event::TimedOut,
                });
            }
        }
        if let Some(due) = self.absorb_until {
            if now >= due {
                self.absorb_until = None;
                self.state = ClientState::Terminated;
            }
        }
        actions
    }
}

pub struct ClientTransactions {
    timings: Timings,
    reliability: Reliability,
    live: HashMap<TransactionKey, PlainClient>,
}

impl ClientTransactions {
    pub fn new(timings: Timings, reliability: Reliability) -> Self {
        ClientTransactions {
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

    pub fn transmissions(&self, key: &TransactionKey) -> Option<u32> {
        self.live.get(key).map(|client| client.transmissions)
    }

    pub fn begin(
        &mut self,
        request: &Request,
        to: SocketAddr,
        now: Duration,
    ) -> Result<(TransactionKey, Vec<Action>), BeginError> {
        if request.method() == Method::Invite {
            return Err(BeginError::InviteIsNotOurs);
        }
        let key = crate::transaction::transaction_key(request).ok_or(BeginError::NoBranch)?;
        if self.live.contains_key(&key) {
            return Err(BeginError::AlreadyLive(key));
        }
        let datagram = Message::Request(request.clone()).to_bytes();
        let (client, actions) = PlainClient::opened(
            key.clone(),
            datagram,
            to,
            now,
            &self.timings,
            self.reliability,
        );
        self.live.insert(key.clone(), client);
        Ok((key, actions))
    }

    pub fn on_response(&mut self, response: &Response, now: Duration) -> Vec<Action> {
        let Some(key) = response_transaction_key(response) else {
            return Vec::new();
        };
        let timings = self.timings;
        let reliability = self.reliability;
        match self.live.get_mut(&key) {
            Some(client) => client.on_response(response, now, &timings, reliability),
            None => Vec::new(),
        }
    }

    pub fn next_deadline(&self) -> Option<Duration> {
        self.live
            .values()
            .filter_map(|client| client.next_deadline())
            .min()
    }

    pub fn poll(&mut self, now: Duration) -> Vec<Action> {
        let timings = self.timings;
        let mut actions = Vec::new();
        for client in self.live.values_mut() {
            actions.extend(client.poll(now, &timings));
        }
        let finished: Vec<TransactionKey> = self
            .live
            .iter()
            .filter(|(_, client)| client.state == ClientState::Terminated)
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

pub fn response_transaction_key(response: &Response) -> Option<TransactionKey> {
    let branch = response.first_via_branch()?;
    let via = response.first_via()?;
    let sent_by_header = via.0.first()?;
    let host = sent_by_header.sent_by_host.to_string();
    let sent_by = match sent_by_header.sent_by_port {
        Some(port) => format!("{host}:{port}"),
        None => host,
    };
    let method = response.cseq()?.method.clone();
    crate::transaction::key_parts(branch, sent_by, method)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialog::{
        negotiate_session_timer, DialogId, Dialogs, SessionTimer, SessionTimerPolicy,
    };
    use crate::message::parse_without_trusting_the_network;

    const OUR_VIA: &str = "172.31.99.31:5080";
    const BRANCH: &str = "z9hG4bK-mss-1";

    fn peer() -> SocketAddr {
        "172.31.99.80:5060".parse().expect("a peer address")
    }

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    fn parse_request(datagram: &[u8]) -> Request {
        match parse_without_trusting_the_network(datagram).expect("a parsable request") {
            Message::Request(request) => request,
            Message::Response(_) => panic!("a request was expected"),
        }
    }

    fn invite(record_route: &str) -> Request {
        parse_request(
            format!(
                "INVITE sip:7200@172.31.99.14:5080 SIP/2.0\r\n\
Via: SIP/2.0/UDP 172.31.99.80:5060;branch=z9hG4bK-invite;rport\r\n\
From: <sip:tester@172.31.99.80>;tag=caller\r\n\
To: <sip:7200@172.31.99.14>\r\n\
Call-ID: call-one\r\n\
CSeq: 7 INVITE\r\n\
Max-Forwards: 70\r\n\
Contact: <sip:tester@172.31.99.80:5060>\r\n\
{record_route}Content-Length: 0\r\n\r\n"
            )
            .as_bytes(),
        )
    }

    fn established(record_route: &str) -> (Dialogs, DialogId) {
        let mut store = Dialogs::new(SessionTimerPolicy::default());
        let request = invite(record_route);
        store.admit(&request);
        let timer = negotiate_session_timer(&request, store.policy());
        assert_eq!(timer, SessionTimer::NotRequested);
        let id = store
            .establish(&request, "mss-tag", timer, ms(0))
            .expect("a dialog");
        (store, id)
    }

    fn bye_of(store: &mut Dialogs, id: &DialogId) -> Request {
        store
            .in_dialog_request(id, Method::Bye, OUR_VIA, BRANCH)
            .expect("a bye for a live dialog")
    }

    fn wire(request: &Request) -> String {
        String::from_utf8_lossy(&Message::Request(request.clone()).to_bytes()).to_string()
    }

    fn response_to(request: &Request, status: u16, reason: &str) -> Response {
        let cseq = request.cseq().expect("a cseq");
        let text = format!(
            "SIP/2.0 {status} {reason}\r\n\
Via: SIP/2.0/UDP {OUR_VIA};branch={BRANCH}\r\n\
From: <sip:7200@172.31.99.14>;tag=mss-tag\r\n\
To: <sip:tester@172.31.99.80>;tag=caller\r\n\
Call-ID: call-one\r\n\
CSeq: {} {}\r\n\
Content-Length: 0\r\n\r\n",
            cseq.seq, cseq.method
        );
        match parse_without_trusting_the_network(text.as_bytes()).expect("a parsable response") {
            Message::Response(response) => response,
            Message::Request(_) => panic!("a response was expected"),
        }
    }

    fn sent(actions: &[Action]) -> usize {
        actions
            .iter()
            .filter(|action| matches!(action, Action::Send { .. }))
            .count()
    }

    fn delivered(actions: Vec<Action>) -> Vec<Event> {
        actions
            .into_iter()
            .filter_map(|action| match action {
                Action::Deliver { event, .. } => Some(event),
                Action::Send { .. } => None,
            })
            .collect()
    }

    #[test]
    fn a_bye_carries_the_dialog_identifiers_the_peer_will_match_it_on() {
        let (mut store, id) = established("");
        let bye = bye_of(&mut store, &id);
        let text = wire(&bye);
        assert!(
            text.starts_with("BYE sip:tester@172.31.99.80:5060 SIP/2.0"),
            "the request uri is the peer's contact: {text}"
        );
        assert!(text.contains("Call-ID: call-one"), "same dialog: {text}");
        assert!(
            text.contains("tag=mss-tag"),
            "our tag is the From tag when we are the one sending: {text}"
        );
        assert!(
            text.contains("tag=caller"),
            "the caller's tag becomes the To tag: {text}"
        );
        assert!(
            text.contains("CSeq: 8 BYE"),
            "the local sequence advances past the invite it ends: {text}"
        );
        assert!(
            text.contains(&format!("branch={BRANCH}")),
            "our own branch: {text}"
        );
    }

    #[test]
    fn each_in_dialog_request_advances_the_local_cseq_by_one() {
        let (mut store, id) = established("");
        assert!(wire(&bye_of(&mut store, &id)).contains("CSeq: 8 BYE"));
        assert!(wire(&bye_of(&mut store, &id)).contains("CSeq: 9 BYE"));
    }

    #[test]
    fn a_loose_route_set_becomes_route_headers_and_leaves_the_request_uri_alone() {
        let (mut store, id) = established(
            "Record-Route: <sip:edge@172.31.99.5;lr>\r\nRecord-Route: <sip:core@172.31.99.6;lr>\r\n",
        );
        let text = wire(&bye_of(&mut store, &id));
        assert!(
            text.starts_with("BYE sip:tester@172.31.99.80:5060 SIP/2.0"),
            "loose routing keeps the remote target as the request uri: {text}"
        );
        let routes: Vec<&str> = text
            .lines()
            .filter(|line| line.starts_with("Route:"))
            .collect();
        assert_eq!(
            routes,
            vec![
                "Route: <sip:core@172.31.99.6;lr>",
                "Route: <sip:edge@172.31.99.5;lr>"
            ],
            "the route set is the reversed record-route, in that order: {text}"
        );
    }

    #[test]
    fn a_strict_route_set_puts_the_first_route_in_the_request_uri_and_the_target_last() {
        let (mut store, id) = established("Record-Route: <sip:legacy@172.31.99.7>\r\n");
        let text = wire(&bye_of(&mut store, &id));
        assert!(
            text.starts_with("BYE sip:legacy@172.31.99.7 SIP/2.0"),
            "a strict router expects to be the request uri: {text}"
        );
        assert!(
            text.contains("Route: <sip:tester@172.31.99.80:5060>"),
            "the remote target moves to the end of the route set: {text}"
        );
    }

    #[test]
    fn a_bye_is_retransmitted_on_the_doubling_schedule_until_a_response_arrives() {
        let (mut store, id) = established("");
        let bye = bye_of(&mut store, &id);
        let mut clients = ClientTransactions::new(Timings::default(), Reliability::Unreliable);
        let (key, opened) = clients.begin(&bye, peer(), ms(0)).expect("a transaction");
        assert_eq!(sent(&opened), 1, "the bye goes out once immediately");
        assert_eq!(clients.transmissions(&key), Some(1));

        assert_eq!(sent(&clients.poll(ms(499))), 0, "before T1 nothing repeats");
        assert_eq!(sent(&clients.poll(ms(500))), 1, "T1 is the first repeat");
        assert_eq!(sent(&clients.poll(ms(1_499))), 0);
        assert_eq!(sent(&clients.poll(ms(1_500))), 1, "then 2xT1");
        assert_eq!(sent(&clients.poll(ms(3_500))), 1, "then 4xT1");
        assert_eq!(clients.transmissions(&key), Some(4));

        let events = delivered(clients.on_response(&response_to(&bye, 200, "OK"), ms(3_600)));
        assert!(
            matches!(events.as_slice(), [Event::Response(_)]),
            "the final response reaches the application"
        );
        assert_eq!(
            sent(&clients.poll(ms(8_000))),
            0,
            "a bye that was answered is never repeated again"
        );
    }

    #[test]
    fn a_bye_nobody_ever_answers_is_given_up_on_at_sixty_four_t1() {
        let (mut store, id) = established("");
        let bye = bye_of(&mut store, &id);
        let mut clients = ClientTransactions::new(Timings::default(), Reliability::Unreliable);
        let (key, _) = clients.begin(&bye, peer(), ms(0)).expect("a transaction");
        let mut timed_out = false;
        let mut tick = 0;
        while tick <= 40_000 {
            for event in delivered(clients.poll(ms(tick))) {
                if matches!(event, Event::TimedOut) {
                    timed_out = true;
                    assert_eq!(
                        tick, 32_000,
                        "timer F is 64xT1, which the defaults make 32 seconds"
                    );
                }
            }
            tick += 100;
        }
        assert!(timed_out, "a bye with no response must not be held forever");
        assert!(
            !clients.holds(&key),
            "the transaction is dropped once it gave up"
        );
        assert_eq!(clients.live(), 0);
    }

    #[test]
    fn only_the_first_final_response_reaches_the_application() {
        let (mut store, id) = established("");
        let bye = bye_of(&mut store, &id);
        let mut clients = ClientTransactions::new(Timings::default(), Reliability::Unreliable);
        clients.begin(&bye, peer(), ms(0)).expect("a transaction");
        let answer = response_to(&bye, 200, "OK");
        assert_eq!(delivered(clients.on_response(&answer, ms(10))).len(), 1);
        assert_eq!(
            delivered(clients.on_response(&answer, ms(20))).len(),
            0,
            "a retransmitted 200 is absorbed, not delivered twice"
        );
    }

    #[test]
    fn a_provisional_response_stops_the_doubling_and_holds_the_repeat_at_t2() {
        let (mut store, id) = established("");
        let bye = bye_of(&mut store, &id);
        let mut clients = ClientTransactions::new(Timings::default(), Reliability::Unreliable);
        clients.begin(&bye, peer(), ms(0)).expect("a transaction");
        assert!(
            delivered(clients.on_response(&response_to(&bye, 100, "Trying"), ms(100))).is_empty(),
            "a provisional is not the application's business on a bye"
        );
        assert_eq!(sent(&clients.poll(ms(4_099))), 0);
        assert_eq!(
            sent(&clients.poll(ms(4_100))),
            1,
            "T2 after the provisional"
        );
    }

    #[test]
    fn a_response_for_a_transaction_we_never_opened_is_ignored() {
        let (mut store, id) = established("");
        let bye = bye_of(&mut store, &id);
        let mut clients = ClientTransactions::new(Timings::default(), Reliability::Unreliable);
        assert!(clients
            .on_response(&response_to(&bye, 200, "OK"), ms(0))
            .is_empty());
    }

    #[test]
    fn a_reliable_transport_neither_repeats_the_bye_nor_lingers_after_the_answer() {
        let (mut store, id) = established("");
        let bye = bye_of(&mut store, &id);
        let mut clients = ClientTransactions::new(Timings::default(), Reliability::Reliable);
        let (key, opened) = clients.begin(&bye, peer(), ms(0)).expect("a transaction");
        assert_eq!(sent(&opened), 1);
        assert_eq!(sent(&clients.poll(ms(1_000))), 0, "TCP repeats nothing");
        clients.on_response(&response_to(&bye, 200, "OK"), ms(1_100));
        clients.poll(ms(1_100));
        assert!(!clients.holds(&key), "there is no timer K to wait out");
    }

    #[test]
    fn the_media_plane_refuses_to_open_an_invite_client_transaction() {
        let mut clients = ClientTransactions::new(Timings::default(), Reliability::Unreliable);
        assert_eq!(
            clients.begin(&invite(""), peer(), ms(0)).err(),
            Some(BeginError::InviteIsNotOurs),
            "MSS answers INVITEs; it never sends one"
        );
    }

    #[test]
    fn the_same_bye_is_not_opened_twice() {
        let (mut store, id) = established("");
        let bye = bye_of(&mut store, &id);
        let mut clients = ClientTransactions::new(Timings::default(), Reliability::Unreliable);
        let (key, _) = clients.begin(&bye, peer(), ms(0)).expect("a transaction");
        assert_eq!(
            clients.begin(&bye, peer(), ms(0)).err(),
            Some(BeginError::AlreadyLive(key))
        );
    }

    #[test]
    fn a_bye_for_a_dialog_that_is_gone_cannot_be_built() {
        let (mut store, id) = established("");
        assert!(store.end(&id));
        assert!(store
            .in_dialog_request(&id, Method::Bye, OUR_VIA, BRANCH)
            .is_none());
    }
}
