use rvoip_sip_core::builder::{SimpleRequestBuilder, SimpleResponseBuilder};
use rvoip_sip_core::prelude::{HeaderName, HeaderValue, Method, StatusCode, TypedHeader};
use rvoip_sip_core::types::sip_request::Request;
use rvoip_sip_core::types::sip_response::Response;
use std::collections::HashMap;
use std::time::Duration;

pub const SESSION_EXPIRES: &str = "Session-Expires";
pub const MIN_SE: &str = "Min-SE";
pub const TIMER_OPTION_TAG: &str = "timer";
pub const MAX_FORWARDS: u32 = 70;
const LOOSE_ROUTE_PARAMETER: &str = "lr";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refresher {
    Uac,
    Uas,
}

impl Refresher {
    pub fn as_parameter(&self) -> &'static str {
        match self {
            Refresher::Uac => "uac",
            Refresher::Uas => "uas",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionTimerPolicy {
    pub min_se: Duration,
    pub default_expires: Duration,
}

impl Default for SessionTimerPolicy {
    fn default() -> Self {
        SessionTimerPolicy {
            min_se: Duration::from_secs(90),
            default_expires: Duration::from_secs(1800),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionTimer {
    NotRequested,
    Negotiated {
        expires: Duration,
        refresher: Refresher,
    },
    IntervalTooSmall {
        min_se: Duration,
    },
}

pub fn header_text(request: &Request, name: &str) -> Option<String> {
    request.all_headers().iter().find_map(|header| {
        let rendered = header.to_string();
        let (found, value) = rendered.split_once(':')?;
        found
            .trim()
            .eq_ignore_ascii_case(name)
            .then(|| value.trim().to_string())
    })
}

fn seconds_and_refresher(value: &str) -> (Option<u64>, Option<Refresher>) {
    let mut parts = value.split(';');
    let seconds = parts
        .next()
        .and_then(|head| head.trim().parse::<u64>().ok());
    let refresher = parts.find_map(|parameter| {
        let (name, setting) = parameter.split_once('=')?;
        if !name.trim().eq_ignore_ascii_case("refresher") {
            return None;
        }
        match setting.trim().to_ascii_lowercase().as_str() {
            "uac" => Some(Refresher::Uac),
            "uas" => Some(Refresher::Uas),
            _ => None,
        }
    });
    (seconds, refresher)
}

pub fn negotiate_session_timer(request: &Request, policy: &SessionTimerPolicy) -> SessionTimer {
    let Some(requested) = header_text(request, SESSION_EXPIRES) else {
        return SessionTimer::NotRequested;
    };
    let (seconds, _) = seconds_and_refresher(&requested);
    let Some(seconds) = seconds else {
        return SessionTimer::NotRequested;
    };
    let expires = Duration::from_secs(seconds);
    let peer_min = header_text(request, MIN_SE)
        .and_then(|text| seconds_and_refresher(&text).0)
        .map(Duration::from_secs);
    let floor = peer_min.unwrap_or(policy.min_se).max(policy.min_se);
    if expires < floor {
        return SessionTimer::IntervalTooSmall { min_se: floor };
    }
    SessionTimer::Negotiated {
        expires,
        refresher: Refresher::Uac,
    }
}

fn raw_header(name: &str, value: String) -> TypedHeader {
    TypedHeader::Other(
        HeaderName::Other(name.to_string()),
        HeaderValue::Raw(value.into_bytes()),
    )
}

pub fn with_session_timer(
    mut response: Response,
    expires: Duration,
    refresher: Refresher,
) -> Response {
    response.headers.push(raw_header(
        SESSION_EXPIRES,
        format!(
            "{};refresher={}",
            expires.as_secs(),
            refresher.as_parameter()
        ),
    ));
    response
        .headers
        .push(raw_header("Require", TIMER_OPTION_TAG.to_string()));
    response
}

pub fn session_interval_too_small(request: &Request, min_se: Duration) -> Response {
    let mut response = SimpleResponseBuilder::response_from_request(
        request,
        StatusCode::SessionIntervalTooSmall,
        Some("the session interval is below our Min-SE"),
    )
    .build();
    response
        .headers
        .push(raw_header(MIN_SE, min_se.as_secs().to_string()));
    response
}

pub fn no_such_dialog(request: &Request) -> Response {
    SimpleResponseBuilder::response_from_request(
        request,
        StatusCode::CallOrTransactionDoesNotExist,
        Some("no dialog matches this request"),
    )
    .build()
}

pub fn request_out_of_order(request: &Request) -> Response {
    SimpleResponseBuilder::response_from_request(
        request,
        StatusCode::ServerInternalError,
        Some("CSeq is not higher than the last one on this dialog"),
    )
    .build()
}

pub fn invite_already_in_progress(request: &Request, retry_after: Duration) -> Response {
    let mut response = SimpleResponseBuilder::response_from_request(
        request,
        StatusCode::ServerInternalError,
        Some("an INVITE on this dialog is still unanswered"),
    )
    .build();
    response
        .headers
        .push(raw_header("Retry-After", retry_after.as_secs().to_string()));
    response
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DialogId {
    call_id: String,
    local_tag: String,
    remote_tag: String,
}

impl DialogId {
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    pub fn local_tag(&self) -> &str {
        &self.local_tag
    }

    pub fn remote_tag(&self) -> &str {
        &self.remote_tag
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialogState {
    Early,
    Confirmed,
}

#[derive(Debug, Clone)]
pub struct Dialog {
    id: DialogId,
    state: DialogState,
    remote_cseq: u32,
    local_cseq: u32,
    local_uri: String,
    remote_uri: String,
    remote_target: Option<String>,
    route_set: Vec<String>,
    refresh_interval: Option<Duration>,
    expires_at: Option<Duration>,
    invite_in_progress: Option<u32>,
}

impl Dialog {
    pub fn id(&self) -> &DialogId {
        &self.id
    }

    pub fn state(&self) -> DialogState {
        self.state
    }

    pub fn remote_target(&self) -> Option<&str> {
        self.remote_target.as_deref()
    }

    pub fn route_set(&self) -> &[String] {
        &self.route_set
    }

    pub fn expires_at(&self) -> Option<Duration> {
        self.expires_at
    }

    pub fn local_uri(&self) -> &str {
        &self.local_uri
    }

    pub fn remote_uri(&self) -> &str {
        &self.remote_uri
    }

    pub fn local_cseq(&self) -> u32 {
        self.local_cseq
    }

    fn take_next_local_cseq(&mut self) -> u32 {
        self.local_cseq = self.local_cseq.saturating_add(1);
        self.local_cseq
    }
}

fn bare_uri(address: &str) -> String {
    let trimmed = address.trim();
    match (trimmed.find('<'), trimmed.rfind('>')) {
        (Some(open), Some(close)) if close > open => trimmed[open + 1..close].to_string(),
        _ => trimmed.to_string(),
    }
}

fn angle_bracketed(uri: &str) -> String {
    format!("<{}>", bare_uri(uri))
}

fn is_loose_route(entry: &str) -> bool {
    bare_uri(entry)
        .split(';')
        .skip(1)
        .any(|parameter| parameter.trim().eq_ignore_ascii_case(LOOSE_ROUTE_PARAMETER))
}

fn routing(remote_target: &str, route_set: &[String]) -> (String, Vec<String>) {
    match route_set.split_first() {
        None => (bare_uri(remote_target), Vec::new()),
        Some((first, _)) if is_loose_route(first) => (
            bare_uri(remote_target),
            route_set
                .iter()
                .map(|entry| angle_bracketed(entry))
                .collect(),
        ),
        Some((first, rest)) => {
            let mut routes: Vec<String> = rest.iter().map(|entry| angle_bracketed(entry)).collect();
            routes.push(angle_bracketed(remote_target));
            (bare_uri(first), routes)
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Classification {
    InitialInvite,
    Reinvite(DialogId),
    InDialog { id: DialogId, method: Method },
    InviteAlreadyInProgress(DialogId),
    OutOfOrder(DialogId),
    NoSuchDialog,
    OutsideAnyDialog(Method),
    Unroutable,
}

fn route_set_of(request: &Request) -> Vec<String> {
    let mut recorded: Vec<String> = request
        .all_headers()
        .iter()
        .filter_map(|header| match header {
            TypedHeader::RecordRoute(entries) => Some(
                entries
                    .0
                    .iter()
                    .map(|entry| entry.to_string())
                    .collect::<Vec<String>>(),
            ),
            _ => None,
        })
        .flatten()
        .collect();
    recorded.reverse();
    recorded
}

pub struct Dialogs {
    policy: SessionTimerPolicy,
    dialogs: HashMap<DialogId, Dialog>,
}

impl Dialogs {
    pub fn new(policy: SessionTimerPolicy) -> Self {
        Dialogs {
            policy,
            dialogs: HashMap::new(),
        }
    }

    pub fn policy(&self) -> &SessionTimerPolicy {
        &self.policy
    }

    pub fn live(&self) -> usize {
        self.dialogs.len()
    }

    pub fn get(&self, id: &DialogId) -> Option<&Dialog> {
        self.dialogs.get(id)
    }

    pub fn identify(&self, request: &Request) -> Option<DialogId> {
        let call_id = request.call_id()?.to_string();
        let local_tag = request.to_tag()?;
        let remote_tag = request.from_tag()?;
        Some(DialogId {
            call_id,
            local_tag,
            remote_tag,
        })
    }

    pub fn admit(&mut self, request: &Request) -> Classification {
        let method = request.method();
        let Some(call_id) = request.call_id().map(|id| id.to_string()) else {
            return Classification::Unroutable;
        };
        let Some(remote_tag) = request.from_tag() else {
            return Classification::Unroutable;
        };
        let Some(local_tag) = request.to_tag() else {
            return match method {
                Method::Invite => Classification::InitialInvite,
                other => Classification::OutsideAnyDialog(other),
            };
        };
        let id = DialogId {
            call_id,
            local_tag,
            remote_tag,
        };
        let Some(cseq) = request.cseq().map(|cseq| cseq.seq) else {
            return Classification::Unroutable;
        };
        let Some(dialog) = self.dialogs.get_mut(&id) else {
            return Classification::NoSuchDialog;
        };
        if matches!(method, Method::Ack | Method::Cancel) {
            return Classification::InDialog { id, method };
        }
        if cseq <= dialog.remote_cseq {
            return Classification::OutOfOrder(id);
        }
        if method == Method::Invite {
            if dialog.invite_in_progress.is_some() {
                return Classification::InviteAlreadyInProgress(id);
            }
            dialog.invite_in_progress = Some(cseq);
            dialog.remote_cseq = cseq;
            return Classification::Reinvite(id);
        }
        dialog.remote_cseq = cseq;
        Classification::InDialog { id, method }
    }

    pub fn establish(
        &mut self,
        request: &Request,
        local_tag: &str,
        timer: SessionTimer,
        now: Duration,
    ) -> Option<DialogId> {
        let call_id = request.call_id()?.to_string();
        let remote_tag = request.from_tag()?;
        let cseq = request.cseq().map(|cseq| cseq.seq)?;
        let id = DialogId {
            call_id,
            local_tag: local_tag.to_string(),
            remote_tag,
        };
        let (refresh_interval, expires_at) = match timer {
            SessionTimer::Negotiated { expires, .. } => (Some(expires), Some(now + expires)),
            _ => (None, None),
        };
        let dialog = Dialog {
            id: id.clone(),
            state: DialogState::Confirmed,
            remote_cseq: cseq,
            local_cseq: cseq,
            local_uri: request
                .to()
                .map(|to| to.address().uri.to_string())
                .unwrap_or_default(),
            remote_uri: request
                .from()
                .map(|from| from.address().uri.to_string())
                .unwrap_or_default(),
            remote_target: request.contact_uri(),
            route_set: route_set_of(request),
            refresh_interval,
            expires_at,
            invite_in_progress: None,
        };
        self.dialogs.insert(id.clone(), dialog);
        Some(id)
    }

    pub fn answered(&mut self, id: &DialogId, now: Duration) {
        if let Some(dialog) = self.dialogs.get_mut(id) {
            dialog.invite_in_progress = None;
            if let Some(interval) = dialog.refresh_interval {
                dialog.expires_at = Some(now + interval);
            }
        }
    }

    pub fn end(&mut self, id: &DialogId) -> bool {
        self.dialogs.remove(id).is_some()
    }

    pub fn in_dialog_request(
        &mut self,
        id: &DialogId,
        method: Method,
        via_sent_by: &str,
        branch: &str,
    ) -> Option<Request> {
        let dialog = self.dialogs.get_mut(id)?;
        let cseq = dialog.take_next_local_cseq();
        let target = dialog
            .remote_target
            .clone()
            .unwrap_or_else(|| dialog.remote_uri.clone());
        let (request_uri, routes) = routing(&target, &dialog.route_set);
        let mut builder = SimpleRequestBuilder::new(method, &request_uri)
            .ok()?
            .from("", &dialog.local_uri, Some(&dialog.id.local_tag))
            .to("", &dialog.remote_uri, Some(&dialog.id.remote_tag))
            .call_id(&dialog.id.call_id)
            .cseq(cseq)
            .via(via_sent_by, "UDP", Some(branch))
            .max_forwards(MAX_FORWARDS);
        for route in routes {
            builder = builder.header(TypedHeader::Other(
                HeaderName::Route,
                HeaderValue::Raw(route.into_bytes()),
            ));
        }
        Some(builder.build())
    }

    pub fn next_deadline(&self) -> Option<Duration> {
        self.dialogs
            .values()
            .filter_map(|dialog| dialog.expires_at)
            .min()
    }

    pub fn expired(&mut self, now: Duration) -> Vec<DialogId> {
        let due: Vec<DialogId> = self
            .dialogs
            .values()
            .filter(|dialog| dialog.expires_at.is_some_and(|at| now >= at))
            .map(|dialog| dialog.id.clone())
            .collect();
        for id in &due {
            self.dialogs.remove(id);
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::parse_without_trusting_the_network;
    use rvoip_sip_core::prelude::Message;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    fn parse(datagram: &[u8]) -> Request {
        match parse_without_trusting_the_network(datagram).expect("a parsable request") {
            Message::Request(request) => request,
            Message::Response(_) => panic!("a request was expected"),
        }
    }

    fn request(method: &str, cseq: u32, to_tag: Option<&str>, extra: &str) -> Request {
        let to = match to_tag {
            Some(tag) => format!("<sip:7200@172.31.99.14>;tag={tag}"),
            None => "<sip:7200@172.31.99.14>".to_string(),
        };
        parse(
            format!(
                "{method} sip:7200@172.31.99.14:5080 SIP/2.0\r\n\
Via: SIP/2.0/UDP 172.31.99.80:5060;branch=z9hG4bK-{cseq};rport\r\n\
From: <sip:tester@172.31.99.80>;tag=caller\r\n\
To: {to}\r\n\
Call-ID: call-one\r\n\
CSeq: {cseq} {method}\r\n\
Max-Forwards: 70\r\n\
Contact: <sip:tester@172.31.99.80:5060>\r\n\
{extra}Content-Length: 0\r\n\r\n"
            )
            .as_bytes(),
        )
    }

    fn dialogs() -> Dialogs {
        Dialogs::new(SessionTimerPolicy::default())
    }

    fn established(store: &mut Dialogs) -> DialogId {
        let invite = request("INVITE", 1, None, "");
        assert_eq!(store.admit(&invite), Classification::InitialInvite);
        store
            .establish(&invite, "mss-tag", SessionTimer::NotRequested, ms(0))
            .expect("a dialog")
    }

    #[test]
    fn an_invite_with_no_session_expires_asks_for_no_timer() {
        let invite = request("INVITE", 1, None, "");
        assert_eq!(
            negotiate_session_timer(&invite, &SessionTimerPolicy::default()),
            SessionTimer::NotRequested
        );
    }

    #[test]
    fn a_session_expires_below_our_floor_is_refused_422_carrying_that_floor() {
        let invite = request("INVITE", 1, None, "Session-Expires: 60\r\n");
        let policy = SessionTimerPolicy::default();
        assert_eq!(
            negotiate_session_timer(&invite, &policy),
            SessionTimer::IntervalTooSmall {
                min_se: Duration::from_secs(90)
            }
        );
        let refusal = session_interval_too_small(&invite, Duration::from_secs(90));
        assert_eq!(refusal.status_code(), 422);
        let wire = String::from_utf8_lossy(&Message::Response(refusal).to_bytes()).to_string();
        assert!(wire.contains("Min-SE: 90"), "the floor is stated: {wire}");
    }

    #[test]
    fn a_session_expires_above_the_floor_is_negotiated_with_the_caller_refreshing() {
        let invite = request("INVITE", 1, None, "Session-Expires: 1800;refresher=uas\r\n");
        assert_eq!(
            negotiate_session_timer(&invite, &SessionTimerPolicy::default()),
            SessionTimer::Negotiated {
                expires: Duration::from_secs(1800),
                refresher: Refresher::Uac
            },
            "an answer-only media plane never becomes the refresher"
        );
    }

    #[test]
    fn a_peer_min_se_higher_than_ours_raises_the_floor() {
        let invite = request("INVITE", 1, None, "Session-Expires: 120\r\nMin-SE: 600\r\n");
        assert_eq!(
            negotiate_session_timer(&invite, &SessionTimerPolicy::default()),
            SessionTimer::IntervalTooSmall {
                min_se: Duration::from_secs(600)
            }
        );
    }

    #[test]
    fn the_negotiated_timer_is_stamped_on_the_answer_beside_require_timer() {
        let invite = request("INVITE", 1, None, "Session-Expires: 1800\r\n");
        let answer =
            SimpleResponseBuilder::response_from_request(&invite, StatusCode::Ok, None).build();
        let stamped = with_session_timer(answer, Duration::from_secs(1800), Refresher::Uac);
        let wire = String::from_utf8_lossy(&Message::Response(stamped).to_bytes()).to_string();
        assert!(
            wire.contains("Session-Expires: 1800;refresher=uac"),
            "the negotiated interval and refresher: {wire}"
        );
        assert!(wire.contains("Require: timer"), "the option tag: {wire}");
    }

    #[test]
    fn an_initial_invite_carries_no_to_tag_and_opens_no_dialog_by_itself() {
        let mut store = dialogs();
        let invite = request("INVITE", 1, None, "");
        assert_eq!(store.admit(&invite), Classification::InitialInvite);
        assert_eq!(
            store.live(),
            0,
            "the dialog exists once we answer, not before"
        );
    }

    #[test]
    fn an_in_dialog_bye_finds_its_dialog_and_advances_the_cseq() {
        let mut store = dialogs();
        let id = established(&mut store);
        let bye = request("BYE", 2, Some("mss-tag"), "");
        assert_eq!(
            store.admit(&bye),
            Classification::InDialog {
                id: id.clone(),
                method: Method::Bye
            }
        );
        assert!(store.end(&id));
        assert_eq!(store.live(), 0);
    }

    #[test]
    fn a_bye_for_a_dialog_we_never_established_is_not_matched() {
        let mut store = dialogs();
        let bye = request("BYE", 2, Some("someone-elses-tag"), "");
        assert_eq!(store.admit(&bye), Classification::NoSuchDialog);
        let refusal = no_such_dialog(&bye);
        assert_eq!(refusal.status_code(), 481);
    }

    #[test]
    fn a_reinvite_is_recognised_as_in_dialog_and_not_as_a_new_call() {
        let mut store = dialogs();
        let id = established(&mut store);
        let reinvite = request("INVITE", 2, Some("mss-tag"), "");
        assert_eq!(store.admit(&reinvite), Classification::Reinvite(id));
    }

    #[test]
    fn a_second_reinvite_while_the_first_is_unanswered_is_refused_500_with_retry_after() {
        let mut store = dialogs();
        let id = established(&mut store);
        store.admit(&request("INVITE", 2, Some("mss-tag"), ""));
        let second = request("INVITE", 3, Some("mss-tag"), "");
        assert_eq!(
            store.admit(&second),
            Classification::InviteAlreadyInProgress(id)
        );
        let refusal = invite_already_in_progress(&second, Duration::from_secs(4));
        assert_eq!(refusal.status_code(), 500);
        let wire = String::from_utf8_lossy(&Message::Response(refusal).to_bytes()).to_string();
        assert!(wire.contains("Retry-After: 4"), "when to try again: {wire}");
    }

    #[test]
    fn answering_a_reinvite_lets_the_next_one_through() {
        let mut store = dialogs();
        let id = established(&mut store);
        store.admit(&request("INVITE", 2, Some("mss-tag"), ""));
        store.answered(&id, ms(0));
        assert_eq!(
            store.admit(&request("INVITE", 3, Some("mss-tag"), "")),
            Classification::Reinvite(id)
        );
    }

    #[test]
    fn an_in_dialog_request_whose_cseq_did_not_advance_is_refused_500() {
        let mut store = dialogs();
        let id = established(&mut store);
        let stale = request("BYE", 1, Some("mss-tag"), "");
        assert_eq!(store.admit(&stale), Classification::OutOfOrder(id));
        assert_eq!(request_out_of_order(&stale).status_code(), 500);
    }

    #[test]
    fn an_ack_is_admitted_without_cseq_policing_because_it_reuses_the_invites_number() {
        let mut store = dialogs();
        let id = established(&mut store);
        let ack = request("ACK", 1, Some("mss-tag"), "");
        assert_eq!(
            store.admit(&ack),
            Classification::InDialog {
                id,
                method: Method::Ack
            }
        );
    }

    #[test]
    fn a_request_with_no_from_tag_is_unroutable_rather_than_a_new_call() {
        let mut store = dialogs();
        let anonymous = parse(
            b"INVITE sip:7200@172.31.99.14:5080 SIP/2.0\r\n\
Via: SIP/2.0/UDP 172.31.99.80:5060;branch=z9hG4bK-1\r\n\
From: <sip:tester@172.31.99.80>\r\n\
To: <sip:7200@172.31.99.14>\r\n\
Call-ID: call-one\r\n\
CSeq: 1 INVITE\r\n\
Content-Length: 0\r\n\r\n",
        );
        assert_eq!(store.admit(&anonymous), Classification::Unroutable);
    }

    #[test]
    fn an_options_outside_a_dialog_is_named_as_such_rather_than_opening_one() {
        let mut store = dialogs();
        let options = request("OPTIONS", 1, None, "");
        assert_eq!(
            store.admit(&options),
            Classification::OutsideAnyDialog(Method::Options)
        );
    }

    #[test]
    fn the_dialog_remembers_the_remote_target_it_must_send_in_dialog_requests_to() {
        let mut store = dialogs();
        let id = established(&mut store);
        let dialog = store.get(&id).expect("the dialog");
        assert_eq!(
            dialog.remote_target(),
            Some("sip:tester@172.31.99.80:5060"),
            "the contact of the request that made the dialog"
        );
        assert_eq!(dialog.state(), DialogState::Confirmed);
    }

    #[test]
    fn a_session_that_is_never_refreshed_expires_at_the_negotiated_interval() {
        let mut store = dialogs();
        let invite = request("INVITE", 1, None, "Session-Expires: 1800\r\n");
        let timer = negotiate_session_timer(&invite, store.policy());
        let id = store
            .establish(&invite, "mss-tag", timer, ms(0))
            .expect("a dialog");
        assert_eq!(store.next_deadline(), Some(Duration::from_secs(1800)));
        assert!(store.expired(Duration::from_secs(1799)).is_empty());
        assert_eq!(store.expired(Duration::from_secs(1800)), vec![id]);
        assert_eq!(store.live(), 0, "an expired session is not left behind");
    }

    #[test]
    fn a_refresh_pushes_the_expiry_out_by_the_negotiated_interval() {
        let mut store = dialogs();
        let invite = request("INVITE", 1, None, "Session-Expires: 1800\r\n");
        let timer = negotiate_session_timer(&invite, store.policy());
        let id = store
            .establish(&invite, "mss-tag", timer, ms(0))
            .expect("a dialog");
        store.admit(&request("INVITE", 2, Some("mss-tag"), ""));
        store.answered(&id, Duration::from_secs(900));
        assert_eq!(store.next_deadline(), Some(Duration::from_secs(2700)));
        assert!(store.expired(Duration::from_secs(1800)).is_empty());
    }

    #[test]
    fn a_dialog_with_no_session_timer_never_expires_on_its_own() {
        let mut store = dialogs();
        established(&mut store);
        assert_eq!(store.next_deadline(), None);
        assert!(store.expired(Duration::from_secs(86_400)).is_empty());
        assert_eq!(store.live(), 1);
    }
}
