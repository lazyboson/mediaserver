"""Finds which subscription answer SDP rtpengine actually accepts.

Each variant gets a fresh subscription, so a rejected answer cannot poison the
next attempt. Run this before changing SubscriptionAnswer::to_sdp.
"""

import os
import re
import socket

NODE = os.environ.get("NG_NODE", "172.31.98.10")
PORT = int(os.environ.get("NG_PORT", "22222"))
SELF_IP = os.environ.get("SELF_IP", "172.31.98.20")
CALL_ID = os.environ.get("CALL_ID", "answer-probe-1")
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
        self.sock.settimeout(3)
        self.serial = 0

    def send(self, command):
        self.serial += 1
        cookie = f"ap-{self.serial}".encode()
        for _ in range(5):
            self.sock.sendto(cookie + b" " + bencode(command), (NODE, PORT))
            try:
                reply, _ = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            return reply.split(b" ", 1)[1]
        return None

    def ok(self, command):
        body = self.send(command)
        if body is None:
            return None, "no reply"
        if field(body, "result") == "error":
            return None, field(body, "error-reason")
        return body, None


def endpoint_sdp(port):
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


def variant_pcmu_only():
    body = "".join(
        f"m=audio {port} RTP/AVP 0\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=ptime:20\r\n"
        "a=recvonly\r\n"
        for port in OUR_PORTS
    )
    return (
        "v=0\r\n"
        f"o=- 1 1 IN IP4 {SELF_IP}\r\n"
        "s=mss-tap\r\n"
        f"c=IN IP4 {SELF_IP}\r\n"
        "t=0 0\r\n" + body
    )


def variant_with_telephone_event():
    body = "".join(
        f"m=audio {port} RTP/AVP 0 101\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=rtpmap:101 telephone-event/8000\r\n"
        "a=ptime:20\r\n"
        "a=recvonly\r\n"
        for port in OUR_PORTS
    )
    return (
        "v=0\r\n"
        f"o=- 1 1 IN IP4 {SELF_IP}\r\n"
        "s=mss-tap\r\n"
        f"c=IN IP4 {SELF_IP}\r\n"
        "t=0 0\r\n" + body
    )


def variant_media_level_connection():
    body = "".join(
        f"m=audio {port} RTP/AVP 0 101\r\n"
        f"c=IN IP4 {SELF_IP}\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=rtpmap:101 telephone-event/8000\r\n"
        "a=ptime:20\r\n"
        "a=recvonly\r\n"
        for port in OUR_PORTS
    )
    return (
        "v=0\r\n"
        f"o=- 1 1 IN IP4 {SELF_IP}\r\n"
        "s=mss-tap\r\n"
        "t=0 0\r\n" + body
    )


def variant_echo_offer(offer_sdp):
    lines = []
    index = 0
    for line in offer_sdp.splitlines():
        if line.startswith("m=audio "):
            parts = line.split(" ")
            parts[1] = str(OUR_PORTS[min(index, len(OUR_PORTS) - 1)])
            index += 1
            lines.append(" ".join(parts))
        elif line.startswith("c=IN IP4 "):
            lines.append(f"c=IN IP4 {SELF_IP}")
        elif line.startswith("a=sendonly"):
            lines.append("a=recvonly")
        elif line.startswith("a=rtcp:"):
            continue
        else:
            lines.append(line)
    return "\r\n".join(lines) + "\r\n"


def main():
    ng = Ng()
    if ng.ok({"command": "ping"})[0] is None:
        log("rtpengine did not answer ping")
        return

    ng.ok({"command": "offer", "call-id": CALL_ID, "from-tag": "tagA",
           "sdp": endpoint_sdp(40000)})
    ng.ok({"command": "answer", "call-id": CALL_ID, "from-tag": "tagA",
           "to-tag": "tagB", "sdp": endpoint_sdp(40002)})

    offer_body, error = ng.ok({
        "command": "subscribe request",
        "call-id": CALL_ID,
        "from-tags": ["tagA", "tagB"],
        "codec": {"accept": ["PCMU"]},
    })
    if offer_body is None:
        log(f"subscribe request failed: {error}")
        return
    offered_sdp = field(offer_body, "sdp")

    variants = [
        ("pcmu only (what sdp.rs emits today)", variant_pcmu_only()),
        ("pcmu + telephone-event", variant_with_telephone_event()),
        ("media-level c=", variant_media_level_connection()),
        ("echo rtpengine's offer, ports+direction swapped",
         variant_echo_offer(offered_sdp)),
    ]

    for name, answer_sdp in variants:
        fresh, error = ng.ok({
            "command": "subscribe request",
            "call-id": CALL_ID,
            "from-tags": ["tagA", "tagB"],
            "codec": {"accept": ["PCMU"]},
        })
        if fresh is None:
            log(f"{name}: could not get a fresh subscription ({error})")
            continue
        tag = field(fresh, "to-tag")
        _, error = ng.ok({
            "command": "subscribe answer",
            "call-id": CALL_ID,
            "to-tag": tag,
            "sdp": answer_sdp,
        })
        log(f"{'ACCEPTED' if error is None else 'REJECTED'}  {name}"
            + (f"  -> {error}" if error else ""))
        ng.ok({"command": "unsubscribe", "call-id": CALL_ID, "to-tag": tag})

    ng.ok({"command": "delete", "call-id": CALL_ID})


main()
