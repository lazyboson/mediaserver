use rvoip_sip_core::builder::SimpleResponseBuilder;
use rvoip_sip_core::prelude::{Message, Method, StatusCode};
use rvoip_sip_core::types::sip_request::Request;
use rvoip_sip_core::ParseMode;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MessageError {
    #[error("not a parsable sip message")]
    Unparsable,
    #[error("a media session is opened by INVITE, not by {method}")]
    NotAnInvite { method: String },
    #[error("this INVITE carries no sdp offer, so there is nothing to answer")]
    NoOffer,
    #[error("this INVITE's body is not utf-8")]
    BodyNotUtf8,
}

pub fn parse_without_trusting_the_network(datagram: &[u8]) -> Option<Message> {
    std::panic::catch_unwind(|| {
        rvoip_sip_core::parse_message_with_mode(datagram, ParseMode::Lenient).ok()
    })
    .ok()
    .flatten()
}

pub struct Invite {
    request: Request,
}

impl Invite {
    pub fn from_datagram(datagram: &[u8]) -> Result<Self, MessageError> {
        let message =
            parse_without_trusting_the_network(datagram).ok_or(MessageError::Unparsable)?;
        let request = match message {
            Message::Request(request) => request,
            Message::Response(_) => return Err(MessageError::Unparsable),
        };
        Self::from_request(request)
    }

    pub fn from_request(request: Request) -> Result<Self, MessageError> {
        match request.method() {
            Method::Invite => Ok(Self { request }),
            other => Err(MessageError::NotAnInvite {
                method: other.to_string(),
            }),
        }
    }

    pub fn request(&self) -> &Request {
        &self.request
    }

    pub fn call_id(&self) -> Option<String> {
        self.request.call_id().map(|id| id.to_string())
    }

    pub fn request_uri_user(&self) -> Option<String> {
        self.request.uri().user.clone()
    }

    pub fn sdp_offer(&self) -> Result<&str, MessageError> {
        let body = self.request.body();
        if body.is_empty() {
            return Err(MessageError::NoOffer);
        }
        std::str::from_utf8(body).map_err(|_| MessageError::BodyNotUtf8)
    }

    pub fn answered_with(&self, sdp_answer: &str, contact_uri: &str, local_tag: &str) -> Vec<u8> {
        let mut builder =
            SimpleResponseBuilder::response_from_request(&self.request, StatusCode::Ok, None);
        if let Some(to) = self.request.to() {
            builder = builder.to(
                to.address().display_name().unwrap_or_default(),
                &to.address().uri.to_string(),
                Some(local_tag),
            );
        }
        let response = builder
            .contact(contact_uri, None)
            .content_type("application/sdp")
            .body(sdp_answer.to_string())
            .build();
        Message::Response(response).to_bytes()
    }

    pub fn refused_with(&self, status: StatusCode, reason: &str) -> Vec<u8> {
        let response =
            SimpleResponseBuilder::response_from_request(&self.request, status, Some(reason))
                .build();
        Message::Response(response).to_bytes()
    }

    pub fn trying(&self) -> Vec<u8> {
        let response =
            SimpleResponseBuilder::response_from_request(&self.request, StatusCode::Trying, None)
                .build();
        Message::Response(response).to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OFFER: &str = "v=0\r\no=- 1 1 IN IP4 172.31.99.10\r\ns=-\r\nc=IN IP4 172.31.99.10\r\n\
t=0 0\r\nm=audio 30056 RTP/AVP 8 101\r\na=rtpmap:8 PCMA/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\na=ptime:20\r\na=sendrecv\r\n";

    const ANSWER: &str = "v=0\r\no=- 1 1 IN IP4 172.31.99.31\r\ns=mss-inline\r\n\
c=IN IP4 172.31.99.31\r\nt=0 0\r\nm=audio 40120 RTP/AVP 8 101\r\n\
a=rtpmap:8 PCMA/8000\r\na=rtpmap:101 telephone-event/8000\r\na=ptime:20\r\n\
a=sendrecv\r\n";

    fn invite_dialling(user: &str, body: &str) -> Vec<u8> {
        format!(
            "INVITE sip:{user}@172.31.99.14:5080 SIP/2.0\r\n\
Via: SIP/2.0/UDP 172.31.99.80:5060;branch=z9hG4bK-abc;rport\r\n\
From: <sip:tester@172.31.99.80>;tag=frontdoor\r\n\
To: <sip:{user}@172.31.99.14>\r\n\
Call-ID: front-door-1\r\n\
CSeq: 1 INVITE\r\n\
Max-Forwards: 70\r\n\
Contact: <sip:mod_sofia@172.31.99.80:5060>\r\n\
Content-Type: application/sdp\r\n\
Content-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes()
    }

    #[test]
    fn an_invite_yields_the_offer_and_the_user_part_it_dialled() {
        let invite =
            Invite::from_datagram(&invite_dialling("7200", OFFER)).expect("a well formed invite");
        assert_eq!(invite.call_id().as_deref(), Some("front-door-1"));
        assert_eq!(
            invite.request_uri_user().as_deref(),
            Some("7200"),
            "the dialled user part is what an application routes on"
        );
        assert!(invite
            .sdp_offer()
            .expect("an offer")
            .contains("m=audio 30056 RTP/AVP 8 101"));
    }

    #[test]
    fn the_answer_carries_the_applications_sdp_back_to_the_caller() {
        let invite = Invite::from_datagram(&invite_dialling("7200", OFFER)).expect("invite");
        let wire = invite.answered_with(ANSWER, "sip:mss@172.31.99.31:5080", "mss-tag-7");
        let text = String::from_utf8(wire).expect("a response is ascii");
        assert!(text.starts_with("SIP/2.0 200"), "200 OK expected: {text}");
        assert!(text.contains("m=audio 40120 RTP/AVP 8 101"));
        assert!(
            text.contains("tag=mss-tag-7"),
            "the caller must be given our own dialog tag, not a shared constant: {text}"
        );
        assert!(
            text.contains("Call-ID: front-door-1"),
            "same dialog: {text}"
        );
        assert!(
            text.contains("application/sdp"),
            "the answer declares its body type: {text}"
        );
    }

    #[test]
    fn a_refusal_says_why_in_the_reason_phrase_the_caller_will_log() {
        let invite = Invite::from_datagram(&invite_dialling("7200", OFFER)).expect("invite");
        let text = String::from_utf8(invite.refused_with(
            StatusCode::ServiceUnavailable,
            "an inline leg speaks pcmu or pcma",
        ))
        .expect("ascii");
        assert!(text.starts_with("SIP/2.0 503"), "503 expected: {text}");
        assert!(text.contains("an inline leg speaks pcmu or pcma"));
    }

    #[test]
    fn an_invite_with_no_body_is_refused_by_name_rather_than_answered_with_nothing() {
        let invite = Invite::from_datagram(&invite_dialling("7200", "")).expect("invite");
        assert_eq!(invite.sdp_offer(), Err(MessageError::NoOffer));
    }

    #[test]
    fn a_method_that_does_not_open_a_session_says_which_one_it_was() {
        let options = String::from_utf8(invite_dialling("7200", OFFER))
            .expect("ascii")
            .replacen("INVITE sip:", "OPTIONS sip:", 1)
            .replacen("CSeq: 1 INVITE", "CSeq: 1 OPTIONS", 1)
            .into_bytes();
        assert_eq!(
            Invite::from_datagram(&options).err(),
            Some(MessageError::NotAnInvite {
                method: "OPTIONS".to_string()
            })
        );
    }

    #[test]
    fn a_front_door_open_to_the_network_returns_errors_instead_of_panicking() {
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
            assert!(
                Invite::from_datagram(&datagram).is_err(),
                "a hostile datagram must be refused, not accepted: {datagram:?}"
            );
        }
    }
}
