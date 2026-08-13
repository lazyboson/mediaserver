"""Probes a real rtpengine for the subscribe lifecycle this project depends on.

Creates a call with offer/answer, then asks for a tap and prints exactly what
rtpengine replies, so the sans-IO SDP parser can be checked against reality
instead of against the documentation.
"""

import os
import re
import socket
import sys

NODE = os.environ.get("NG_NODE", "127.0.0.1")
PORT = int(os.environ.get("NG_PORT", "22222"))
SELF_IP = os.environ.get("SELF_IP", "127.0.0.1")
CALL_ID = os.environ.get("CALL_ID", "probe-call-1")


def log(message):
    print(message, flush=True)


def bencode(value):
    if isinstance(value, int):
        return b"i%de" % value
    if isinstance(value, str):
        raw = value.encode()
        return b"%d:%s" % (len(raw), raw)
    if isinstance(value, list):
        return b"l" + b"".join(bencode(v) for v in value) + b"e"
    if isinstance(value, dict):
        out = b"d"
        for key in sorted(value):
            raw = key.encode()
            out += b"%d:%s" % (len(raw), raw) + bencode(value[key])
        return out + b"e"
    raise TypeError(type(value))


def field(body, name):
    match = re.search(rb"%d:%s(\d+):" % (len(name), name.encode()), body)
    if not match:
        return None
    length = int(match.group(1))
    start = match.end()
    return body[start:start + length].decode()


def sdp_for(port):
    return (
        "v=0\r\n"
        f"o=- 1 1 IN IP4 {SELF_IP}\r\n"
        "s=probe\r\n"
        f"c=IN IP4 {SELF_IP}\r\n"
        "t=0 0\r\n"
        f"m=audio {port} RTP/AVP 0 101\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=rtpmap:101 telephone-event/8000\r\n"
        "a=ptime:20\r\n"
        "a=sendrecv\r\n"
    )


class Ng:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(3)
        self.serial = 0

    def send(self, command):
        self.serial += 1
        cookie = f"probe-{self.serial}".encode()
        datagram = cookie + b" " + bencode(command)
        for _ in range(5):
            self.sock.sendto(datagram, (NODE, PORT))
            try:
                reply, _ = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            return reply.split(b" ", 1)[1]
        return None


def show(label, body):
    if body is None:
        log(f"{label}: NO REPLY")
        return None
    result = field(body, "result")
    if result == "error":
        log(f"{label}: ERROR -> {field(body, 'error-reason')!r}")
        return None
    log(f"{label}: result={result!r}")
    to_tag = field(body, "to-tag")
    if to_tag:
        log(f"{label}: to-tag={to_tag!r}")
    sdp = field(body, "sdp")
    if sdp:
        log(f"{label}: sdp ->")
        for line in sdp.splitlines():
            log(f"    {line}")
    return body


def main():
    ng = Ng()
    log("=== ping ===")
    if show("ping", ng.send({"command": "ping"})) is None:
        sys.exit(1)

    log("=== offer ===")
    show("offer", ng.send({
        "command": "offer",
        "call-id": CALL_ID,
        "from-tag": "tagA",
        "sdp": sdp_for(40000),
    }))

    log("=== answer ===")
    show("answer", ng.send({
        "command": "answer",
        "call-id": CALL_ID,
        "from-tag": "tagA",
        "to-tag": "tagB",
        "sdp": sdp_for(40002),
    }))

    log("=== subscribe request, both tags, accept PCMU ===")
    both = show("subscribe(both)", ng.send({
        "command": "subscribe request",
        "call-id": CALL_ID,
        "from-tags": ["tagA", "tagB"],
        "codec": {"accept": ["PCMU"]},
        "set-label": "mss-tap",
    }))

    if both is None:
        log("=== falling back: subscribe request, single tag ===")
        both = show("subscribe(single)", ng.send({
            "command": "subscribe request",
            "call-id": CALL_ID,
            "from-tags": ["tagA"],
            "codec": {"accept": ["PCMU"]},
            "set-label": "mss-tap",
        }))

    if both is not None:
        tap_tag = field(both, "to-tag")
        log("=== unsubscribe ===")
        show("unsubscribe", ng.send({
            "command": "unsubscribe",
            "call-id": CALL_ID,
            "to-tag": tap_tag,
        }))

    log("=== delete ===")
    show("delete", ng.send({"command": "delete", "call-id": CALL_ID}))


main()
