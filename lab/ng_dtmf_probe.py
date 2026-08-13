"""Determines whether RFC 4733 survives an rtpengine subscription.

Creates a call, subscribes with and without a codec accept list, pumps audio
plus one DTMF press per leg, and tallies the payload types that actually
arrive on the subscription sockets.
"""

import os
import re
import socket
import struct
import threading
import time

NODE = os.environ.get("NG_NODE", "172.31.98.10")
PORT = int(os.environ.get("NG_PORT", "22222"))
SELF_IP = os.environ.get("SELF_IP", "172.31.98.20")

PCMU = 0
TELEPHONE_EVENT = 101
CALLER_RTP = 41000
CALLEE_RTP = 41002
TAP_PORTS = (43000, 43002)


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

    def call(self, command):
        self.serial += 1
        cookie = f"dp-{self.serial}".encode()
        for _ in range(5):
            self.sock.sendto(cookie + b" " + bencode(command), (NODE, PORT))
            try:
                reply, _ = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            body = reply.split(b" ", 1)[1]
            if field(body, "result") == "error":
                return None, field(body, "error-reason")
            return body, None
        return None, "no reply"


def endpoint_sdp(port):
    return (
        "v=0\r\n"
        f"o=- 1 1 IN IP4 {SELF_IP}\r\n"
        "s=probe\r\n"
        f"c=IN IP4 {SELF_IP}\r\n"
        "t=0 0\r\n"
        f"m=audio {port} RTP/AVP {PCMU} {TELEPHONE_EVENT}\r\n"
        f"a=rtpmap:{PCMU} PCMU/8000\r\n"
        f"a=rtpmap:{TELEPHONE_EVENT} telephone-event/8000\r\n"
        "a=ptime:20\r\n"
        "a=sendrecv\r\n"
    )


def tap_answer_sdp(stream_count):
    body = ""
    for index in range(stream_count):
        body += (
            f"m=audio {TAP_PORTS[index]} RTP/AVP {PCMU} {TELEPHONE_EVENT}\r\n"
            f"a=rtpmap:{PCMU} PCMU/8000\r\n"
            f"a=rtpmap:{TELEPHONE_EVENT} telephone-event/8000\r\n"
            "a=ptime:20\r\n"
            "a=recvonly\r\n"
        )
    return (
        "v=0\r\n"
        f"o=- 9 9 IN IP4 {SELF_IP}\r\n"
        "s=mss-tap\r\n"
        f"c=IN IP4 {SELF_IP}\r\n"
        "t=0 0\r\n" + body
    )


def pump(caller_dest, callee_dest, stop):
    caller = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    caller.bind(("0.0.0.0", CALLER_RTP))
    callee = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    callee.bind(("0.0.0.0", CALLEE_RTP))
    legs = [
        {"sock": caller, "dest": caller_dest, "seq": 100, "ts": 0, "ssrc": 0xA1, "digit": 1},
        {"sock": callee, "dest": callee_dest, "seq": 900, "ts": 0, "ssrc": 0xB2, "digit": 2},
    ]

    def emit(leg, pt, payload, ts):
        header = struct.pack("!BBHII", 0x80, pt, leg["seq"] & 0xFFFF, ts, leg["ssrc"])
        leg["sock"].sendto(header + payload, leg["dest"])
        leg["seq"] += 1

    started = time.monotonic()
    digits_done = False
    while not stop.is_set():
        if not digits_done and time.monotonic() - started > 1.5:
            for leg in legs:
                start_ts = leg["ts"]
                duration = 160
                for _ in range(4):
                    emit(leg, TELEPHONE_EVENT,
                         struct.pack("!BBH", leg["digit"], 10, duration), start_ts)
                    duration += 160
                for _ in range(3):
                    emit(leg, TELEPHONE_EVENT,
                         struct.pack("!BBH", leg["digit"], 0x80 | 10, duration), start_ts)
                leg["ts"] = start_ts + duration
            digits_done = True
            log("    pumped one digit per leg")
        for leg in legs:
            emit(leg, PCMU, bytes(160), leg["ts"])
            leg["ts"] += 160
        time.sleep(0.02)


def tally(sockets, seconds):
    seen = {}
    deadline = time.monotonic() + seconds
    for sock in sockets:
        sock.settimeout(0.2)
    while time.monotonic() < deadline:
        for index, sock in enumerate(sockets):
            try:
                datagram, _ = sock.recvfrom(4096)
            except socket.timeout:
                continue
            if len(datagram) < 12:
                continue
            pt = datagram[1] & 0x7F
            seen.setdefault(index, {}).setdefault(pt, 0)
            seen[index][pt] += 1
    return seen


def run(label, accept_codecs):
    ng = Ng()
    call_id = f"dtmf-probe-{'accept' if accept_codecs else 'noaccept'}"
    log(f"=== {label} ===")
    ng.call({"command": "delete", "call-id": call_id})
    ng.call({"command": "offer", "call-id": call_id, "from-tag": "tagA",
             "sdp": endpoint_sdp(CALLER_RTP)})
    answer, _ = ng.call({"command": "answer", "call-id": call_id, "from-tag": "tagA",
                         "to-tag": "tagB", "sdp": endpoint_sdp(CALLEE_RTP)})
    offer_body, _ = ng.call({"command": "offer", "call-id": call_id, "from-tag": "tagA",
                             "sdp": endpoint_sdp(CALLER_RTP)})

    caller_dest = int(re.findall(r"m=audio (\d+) ", field(answer, "sdp"))[0])
    callee_dest = int(re.findall(r"m=audio (\d+) ", field(offer_body, "sdp"))[0])

    subscribe = {"command": "subscribe request", "call-id": call_id,
                 "from-tags": ["tagA", "tagB"]}
    if accept_codecs:
        subscribe["codec"] = {"accept": accept_codecs}
    sub, error = ng.call(subscribe)
    if sub is None:
        log(f"    subscribe failed: {error}")
        return
    offered = field(sub, "sdp")
    log(f"    offer payload types per stream: "
        f"{re.findall(r'm=audio \\d+ RTP/AVP ([0-9 ]+)', offered)}")

    taps = []
    for port in TAP_PORTS:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", port))
        taps.append(sock)

    _, error = ng.call({"command": "subscribe answer", "call-id": call_id,
                        "to-tag": field(sub, "to-tag"),
                        "sdp": tap_answer_sdp(len(TAP_PORTS))})
    if error:
        log(f"    subscribe answer rejected: {error}")
        for sock in taps:
            sock.close()
        return

    stop = threading.Event()
    pumping = threading.Thread(target=pump, args=((NODE, caller_dest), (NODE, callee_dest), stop),
                               daemon=True)
    pumping.start()
    seen = tally(taps, 4)
    stop.set()
    pumping.join(timeout=2)

    for index in sorted(seen):
        counts = ", ".join(f"pt{pt}={n}" for pt, n in sorted(seen[index].items()))
        log(f"    tap stream {index}: {counts}")
    if not seen:
        log("    tap streams received nothing")
    got_event = any(TELEPHONE_EVENT in counts for counts in seen.values())
    log(f"    RFC 4733 on the tap: {'YES' if got_event else 'NO'}")

    for sock in taps:
        sock.close()
    ng.call({"command": "delete", "call-id": call_id})


run("subscribe WITH codec accept PCMU (what mediaserverd sends today)", ["PCMU"])
time.sleep(1)
run("subscribe WITHOUT codec accept", None)
