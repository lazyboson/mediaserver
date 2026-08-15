"""Finds the subscription answer rtpengine accepts when the call is A-law.

ng_answer_probe.py settled this for a PCMU call and found that dropping the
offered telephone-event is rejected. A real softphone negotiated PCMA, the tap
asked rtpengine to transcode to PCMU, and the answer was rejected again -- so
the same question has to be asked for a leg whose offer carries a codec the tap
does not want to receive.

Each variant gets a fresh subscription so a rejected answer cannot poison the
next attempt.
"""

import os
import re
import socket

NODE = os.environ.get("NG_NODE", "172.31.99.10")
PORT = int(os.environ.get("NG_PORT", "22222"))
SELF_IP = os.environ.get("SELF_IP", "172.31.99.30")
CALL_ID = os.environ.get("CALL_ID", "alaw-answer-probe")
OUR_PORTS = (40010, 40012)


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


class Ng:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(4)
        self.serial = 0

    def ok(self, command):
        self.serial += 1
        cookie = f"alaw-{self.serial}".encode()
        for _ in range(4):
            self.sock.sendto(cookie + b" " + bencode(command), (NODE, PORT))
            try:
                reply, _ = self.sock.recvfrom(131072)
            except socket.timeout:
                continue
            body = reply.split(b" ", 1)[1]
            if field(body, "result") == "error":
                return None, field(body, "error-reason")
            return body, None
        return None, "no reply"


def alaw_endpoint(port):
    return (
        "v=0\r\n"
        f"o=- 1 1 IN IP4 {SELF_IP}\r\n"
        "s=softphone\r\n"
        f"c=IN IP4 {SELF_IP}\r\n"
        "t=0 0\r\n"
        f"m=audio {port} RTP/AVP 8 101\r\n"
        "a=rtpmap:8 PCMA/8000\r\n"
        "a=rtpmap:101 telephone-event/8000\r\n"
        "a=ptime:20\r\n"
        "a=sendrecv\r\n"
    )


def answer(streams, formats, rtpmaps):
    body = ""
    for index in range(streams):
        body += f"m=audio {OUR_PORTS[min(index, len(OUR_PORTS) - 1)]} RTP/AVP {formats}\r\n"
        body += f"c=IN IP4 {SELF_IP}\r\n"
        for line in rtpmaps:
            body += f"a=rtpmap:{line}\r\n"
        body += "a=ptime:20\r\na=recvonly\r\n"
    return (
        "v=0\r\n"
        f"o=- 9 9 IN IP4 {SELF_IP}\r\n"
        "s=mss-tap\r\n"
        "t=0 0\r\n" + body
    )


PCMU = "0 PCMU/8000"
PCMA = "8 PCMA/8000"
EVENT = "101 telephone-event/8000"


def main():
    ng = Ng()
    if ng.ok({"command": "ping"})[0] is None:
        log(f"rtpengine at {NODE}:{PORT} did not answer ping")
        return

    ng.ok({"command": "offer", "call-id": CALL_ID, "from-tag": "ca",
           "sdp": alaw_endpoint(5000)})
    ng.ok({"command": "answer", "call-id": CALL_ID, "from-tag": "ca",
           "to-tag": "cb", "sdp": alaw_endpoint(5002)})

    variants = [
        ("pcmu only, what sdp.rs emits today", "0 101", [PCMU, EVENT]),
        ("pcmu first, pcma kept", "0 8 101", [PCMU, PCMA, EVENT]),
        ("offer order echoed, pcma first", "8 0 101", [PCMA, PCMU, EVENT]),
        ("pcmu only, no telephone-event", "0", [PCMU]),
    ]

    for name, formats, rtpmaps in variants:
        fresh, error = ng.ok({
            "command": "subscribe request",
            "call-id": CALL_ID,
            "from-tags": ["ca", "cb"],
            "codec": {"transcode": ["PCMU"]},
        })
        if fresh is None:
            log(f"  could not get a fresh subscription for {name}: {error}")
            continue
        tag = field(fresh, "to-tag")
        offered = field(fresh, "sdp") or ""
        streams = len(re.findall(r"m=audio \d+", offered))
        _, error = ng.ok({
            "command": "subscribe answer",
            "call-id": CALL_ID,
            "to-tag": tag,
            "sdp": answer(streams, formats, rtpmaps),
        })
        verdict = "ACCEPTED" if error is None else "REJECTED"
        log(f"  {verdict}  RTP/AVP {formats:<8} {name}" + (f"  -> {error}" if error else ""))
        ng.ok({"command": "unsubscribe", "call-id": CALL_ID, "to-tag": tag})

    ng.ok({"command": "delete", "call-id": CALL_ID})


main()
