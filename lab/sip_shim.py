"""A minimal SIP UAS that answers calls with an MSS inline leg.

This is the piece that takes FreeSWITCH out of the media path. OpenSIPS keeps
doing what it does today -- register the phone, anchor the media in rtpengine,
relay the INVITE -- but the INVITE is relayed here instead of to FreeSWITCH,
and what answers it is MSS: the offer goes to MediaControl.CreateSession
{kind=INLINE, sdp_offer, group}, and the SDP answer MSS returns is what this
shim puts in its 200 OK. The dialed user part of the Request-URI IS the
conference group, so two phones that dial 7001 are two inline legs seated in
one MSS conference and hear each other through the mixer. No FreeSWITCH, no
dummy leg, no mod_conference.

It is deliberately the smallest correct UAS and nothing more:

  * INVITE      -> 100 Trying, CreateSession, 200 OK with MSS's answer
  * ACK         -> absorbed
  * BYE         -> 200 OK, DestroySession
  * CANCEL      -> 200 OK to the CANCEL, 487 to the INVITE, session destroyed
  * re-INVITE   -> 488, by decision: MSS has no re-negotiation path (P3-2), so
                   an offer change is refused by name instead of half-honoured
  * OPTIONS     -> 200 OK
  * anything    -> 405

Retransmissions are idempotent because every transaction remembers its last
final response and replays it: a repeated INVITE re-sends the same 200 OK and
never creates a second session, a repeated BYE re-sends the same 200 OK and
never destroys twice. The response is always sent back to the source address of
the request, which is what symmetric-response (rport) routing asks for and the
only address a container behind docker NAT can trust.

Single-threaded on purpose. The one blocking call is CreateSession, and 100
Trying goes out before it, so a slow control plane costs a late 200 rather than
a lost retransmission -- and every retransmission that arrives meanwhile is
absorbed by the transaction table.

It is a LAB shim, not a B2BUA: no authentication, no registrar, no timers, no
session refresh, one dialog per Call-ID. Handoff H5 in docs/tasks.md is the
real thing an integrator owes.

Env: SHIM_IP, SHIM_PORT, CONTROL, STUBS, MSS_AUTH_TOKEN, EXTERNAL_ID_PREFIX,
     CONTROL_TIMEOUT.
"""

import os
import socket
import sys
import time

STUBS = os.environ.get("STUBS", "/pb")
sys.path.insert(0, STUBS)

import grpc  # noqa: E402

import mediacontrol_pb2 as pb  # noqa: E402
import mediacontrol_pb2_grpc as pb_grpc  # noqa: E402

SHIM_IP = os.environ.get("SHIM_IP", "172.31.99.14")
SHIM_PORT = int(os.environ.get("SHIM_PORT", "5080"))
CONTROL = os.environ.get("CONTROL", "172.31.99.31:50551")
TOKEN = os.environ.get("MSS_AUTH_TOKEN", "")
PREFIX = os.environ.get("EXTERNAL_ID_PREFIX", "")
CONTROL_TIMEOUT = float(os.environ.get("CONTROL_TIMEOUT", "5"))

COMPACT = {"v": "via", "f": "from", "t": "to", "i": "call-id", "m": "contact",
           "c": "content-type", "l": "content-length", "s": "subject"}
ALLOW = "INVITE, ACK, BYE, CANCEL, OPTIONS"


def log(message):
    print(f"sip-shim: {message}", flush=True)


def split(message):
    head, _, body = message.partition("\r\n\r\n")
    lines = head.split("\r\n")
    return lines[0], lines[1:], body


def named(lines, wanted):
    out = []
    for line in lines:
        name, _, value = line.partition(":")
        name = name.strip().lower()
        name = COMPACT.get(name, name)
        if name == wanted:
            out.append(value.strip())
    return out


def one(lines, wanted, default=""):
    values = named(lines, wanted)
    return values[0] if values else default


def tag_of(header):
    for part in header.split(";")[1:]:
        key, _, value = part.partition("=")
        if key.strip().lower() == "tag":
            return value.strip()
    return ""


def token(value):
    """A To-tag has to be a SIP token, and a Call-ID does not have to be one.

    MicroSIP's Call-ID carries an @host part and a browser's carries dots and
    dashes; deriving the tag from it keeps the tag deterministic (so a replayed
    200 OK is byte-identical) while this keeps it legal.
    """
    return "".join(c if c.isalnum() or c in "-._" else "-" for c in value)


def dialed_user(request_line):
    target = request_line.split(" ")[1] if " " in request_line else ""
    target = target.split(">")[0].lstrip("<")
    if target.lower().startswith(("sip:", "sips:")):
        target = target.split(":", 1)[1]
    user = target.split("@")[0] if "@" in target else ""
    return user.split(";")[0]


class Shim:
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("0.0.0.0", SHIM_PORT))
        self.socket.settimeout(1.0)
        self.channel = grpc.insecure_channel(CONTROL)
        self.control = pb_grpc.MediaControlStub(self.channel)
        self.metadata = [("authorization", f"Bearer {TOKEN}")] if TOKEN else []
        self.calls = {}
        self.answered = 0
        self.refused = 0

    def reply(self, request_line, lines, peer, code, reason, to_tag="", body="",
              contact=""):
        out = [f"SIP/2.0 {code} {reason}"]
        out += [f"Via: {value}" for value in named(lines, "via")]
        out += [f"Record-Route: {value}" for value in named(lines, "record-route")]
        out.append(f"From: {one(lines, 'from')}")
        to = one(lines, "to")
        if to_tag and not tag_of(to):
            to = f"{to};tag={to_tag}"
        out.append(f"To: {to}")
        out.append(f"Call-ID: {one(lines, 'call-id')}")
        out.append(f"CSeq: {one(lines, 'cseq')}")
        if contact:
            out.append(f"Contact: <{contact}>")
        out.append(f"Allow: {ALLOW}")
        out.append("User-Agent: mss-sip-shim")
        if body:
            out.append("Content-Type: application/sdp")
        out.append(f"Content-Length: {len(body.encode())}")
        message = "\r\n".join(out) + "\r\n\r\n" + body
        self.socket.sendto(message.encode(), peer)
        log(f"-> {code} {reason} to {peer[0]}:{peer[1]} "
            f"for {request_line.split(' ')[0]} {one(lines, 'call-id')}")
        return message

    def create(self, external_id, call_id, offer, group):
        request = pb.CreateSessionRequest(
            external_id=external_id,
            kind=pb.SESSION_KIND_INLINE,
            call_id=call_id,
            sdp_offer=offer,
            group=group,
            idempotency_key=external_id,
        )
        session = self.control.CreateSession(
            request, timeout=CONTROL_TIMEOUT, metadata=self.metadata)
        return session.sdp_answer

    def destroy(self, external_id):
        try:
            self.control.DestroySession(
                pb.SessionRef(external_id=external_id),
                timeout=CONTROL_TIMEOUT,
                metadata=self.metadata)
            log(f"destroyed mss session {external_id}")
        except grpc.RpcError as error:
            log(f"destroy {external_id} failed: {error.code()} "
                f"{error.details()}")

    def on_invite(self, request_line, lines, body, peer):
        call_id = one(lines, "call-id")
        cseq = one(lines, "cseq")
        if tag_of(one(lines, "to")):
            log(f"re-INVITE on {call_id} refused: MSS has no re-negotiation path")
            self.reply(request_line, lines, peer, 488, "Not Acceptable Here")
            return
        call = self.calls.get(call_id)
        if call and call.get("final"):
            if call.get("cseq") == cseq:
                log(f"INVITE retransmission on {call_id}; replaying the final "
                    "response")
                self.socket.sendto(call["final"].encode(), peer)
            else:
                self.reply(request_line, lines, peer, 500, "Server Internal Error")
            return
        if call and call.get("pending"):
            log(f"INVITE retransmission on {call_id} while still answering")
            self.reply(request_line, lines, peer, 100, "Trying")
            return

        group = dialed_user(request_line)
        external_id = f"{PREFIX}{call_id}"
        self.calls[call_id] = {"pending": True, "cseq": cseq,
                              "external_id": external_id,
                              "request_line": request_line, "lines": lines,
                              "peer": peer, "final": None}
        log(f"<- INVITE {call_id} from {peer[0]}:{peer[1]}, dialed '{group}', "
            f"from-tag {tag_of(one(lines, 'from'))}, "
            f"{len(body.encode())} byte offer")
        self.reply(request_line, lines, peer, 100, "Trying")
        if not group:
            self.refused += 1
            self.calls[call_id]["final"] = self.reply(
                request_line, lines, peer, 404, "Not Found")
            self.calls[call_id]["pending"] = False
            return
        if not body.strip():
            self.refused += 1
            self.calls[call_id]["final"] = self.reply(
                request_line, lines, peer, 488, "Not Acceptable Here")
            self.calls[call_id]["pending"] = False
            return
        try:
            answer = self.create(external_id, call_id, body, group)
        except grpc.RpcError as error:
            self.refused += 1
            log(f"CreateSession for {external_id} failed: {error.code()} "
                f"{error.details()}")
            self.calls[call_id]["final"] = self.reply(
                request_line, lines, peer, 503, "Service Unavailable")
            self.calls[call_id]["pending"] = False
            return
        if self.calls[call_id].get("cancelled"):
            log(f"{call_id} was cancelled while MSS was answering; "
                "destroying the leg")
            self.destroy(external_id)
            self.calls[call_id]["pending"] = False
            return
        self.answered += 1
        media = " ".join(line for line in answer.replace("\r", "").split("\n")
                         if line.startswith(("c=", "m=audio")))
        log(f"mss answered {external_id} in group '{group}': {media}")
        self.calls[call_id]["group"] = group
        self.calls[call_id]["created"] = True
        self.calls[call_id]["final"] = self.reply(
            request_line, lines, peer, 200, "OK",
            to_tag=f"mss-{token(external_id)}", body=answer,
            contact=f"sip:{group}@{SHIM_IP}:{SHIM_PORT}")
        self.calls[call_id]["pending"] = False

    def on_bye(self, request_line, lines, peer):
        call_id = one(lines, "call-id")
        call = self.calls.get(call_id)
        if not call:
            log(f"<- BYE for unknown call {call_id}")
            self.reply(request_line, lines, peer, 481,
                       "Call/Transaction Does Not Exist")
            return
        log(f"<- BYE {call_id}")
        self.reply(request_line, lines, peer, 200, "OK",
                   to_tag=f"mss-{token(call['external_id'])}")
        if call.get("created"):
            call["created"] = False
            self.destroy(call["external_id"])

    def on_cancel(self, request_line, lines, peer):
        call_id = one(lines, "call-id")
        call = self.calls.get(call_id)
        log(f"<- CANCEL {call_id}")
        if not call:
            self.reply(request_line, lines, peer, 481,
                       "Call/Transaction Does Not Exist")
            return
        self.reply(request_line, lines, peer, 200, "OK")
        call["cancelled"] = True
        if call.get("final"):
            return
        call["final"] = self.reply(call["request_line"], call["lines"],
                                   call["peer"], 487, "Request Terminated",
                                   to_tag=f"mss-{token(call['external_id'])}")
        call["pending"] = False
        if call.get("created"):
            call["created"] = False
            self.destroy(call["external_id"])

    def serve(self):
        log(f"listening on {SHIM_IP}:{SHIM_PORT}, control {CONTROL}, "
            f"the dialed user part is the conference group")
        while True:
            try:
                datagram, peer = self.socket.recvfrom(65535)
            except socket.timeout:
                continue
            message = datagram.decode("utf-8", "replace")
            if not message.strip():
                continue
            request_line, lines, body = split(message)
            if request_line.startswith("SIP/2.0"):
                log(f"<- response {request_line} on "
                    f"{one(lines, 'call-id')}, ignored")
                continue
            method = request_line.split(" ")[0].upper()
            try:
                if method == "INVITE":
                    self.on_invite(request_line, lines, body, peer)
                elif method == "ACK":
                    log(f"<- ACK {one(lines, 'call-id')}")
                elif method == "BYE":
                    self.on_bye(request_line, lines, peer)
                elif method == "CANCEL":
                    self.on_cancel(request_line, lines, peer)
                elif method == "OPTIONS":
                    self.reply(request_line, lines, peer, 200, "OK")
                else:
                    self.reply(request_line, lines, peer, 405,
                               "Method Not Allowed")
            except Exception as error:
                log(f"{method} on {one(lines, 'call-id')} blew up: "
                    f"{error!r}")


def main():
    while True:
        try:
            Shim().serve()
        except KeyboardInterrupt:
            return
        except Exception as error:
            log(f"restarting after {error!r}")
            time.sleep(1)


main()
