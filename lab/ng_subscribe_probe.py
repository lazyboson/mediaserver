"""Settles what an rtpengine subscription actually carries, and how reliably.

Three questions came out of the control-plane lab runs and none of them could
be answered one call at a time from the daemon:

  1. Does `subscribe request {from-tags: [X]}` give the media X *sends*, or
     the media X *hears*?
  2. Does `play media {from-tag: X}` put audio where X hears it, or make it
     look as though X said it? (architecture.md section 6 says the former;
     a daemon run suggested the latter, so one of them is wrong.)
  3. Why is delivery intermittent -- the same call shape gave a leg 2664
     datagrams in one run and 0 in the next.

The method makes the answers unambiguous instead of inferred. Each
participant gets its own pure tone at a distinct frequency, injected with
`play media` targeted at that participant's from-tag. Then one subscription
is made per from-tag, and every socket is scored with a Goertzel filter for
both frequencies. A tone is a fact: whichever frequency shows up on a socket
names exactly which participant's audio that subscription carries, with no
dependence on anyone speaking.

Trials repeat the whole cycle so intermittency shows up as a rate rather than
an anecdote, and SAME_SESSION_ID=1 reproduces the collision that made two
per-leg answers behave as one session.

Run it inside the lab network against a live call:

  docker run --rm --network mss-microsip_lab -v "$PWD:/lab" -w /lab \\
      python:3-slim python ng_subscribe_probe.py

Env: NG_NODE, NG_PORT, CALL_ID (required), TRIALS, LISTEN_SECONDS,
TONE_HZ (comma separated, one per participant), SAME_SESSION_ID.
"""

import math
import os
import re
import socket
import struct
import sys
import time
import wave
import io

NODE = os.environ.get("NG_NODE", "172.31.99.10")
PORT = int(os.environ.get("NG_PORT", "22222"))
CALL_ID = os.environ.get("CALL_ID", "")
TRIALS = int(os.environ.get("TRIALS", "3"))
LISTEN_SECONDS = float(os.environ.get("LISTEN_SECONDS", "4"))
TONES = [int(hz) for hz in os.environ.get("TONE_HZ", "440,1200").split(",")]
SAME_SESSION_ID = os.environ.get("SAME_SESSION_ID", "0") == "1"
SKIP_UNSUBSCRIBE = os.environ.get("SKIP_UNSUBSCRIBE", "0") == "1"
SKIP_PLAY = os.environ.get("SKIP_PLAY", "0") == "1"
ONLY_TAG_INDEX = os.environ.get("ONLY_TAG_INDEX", "")
MULTI = os.environ.get("MULTI", "0") == "1"
DUMP_QUERY = os.environ.get("DUMP_QUERY", "0") == "1"
TONE_SECONDS = float(os.environ.get("TONE_SECONDS", "3"))


def log(message):
    print(message, flush=True)


def bencode(value):
    if isinstance(value, int):
        return b"i%de" % value
    if isinstance(value, bytes):
        return b"%d:%s" % (len(value), value)
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


def bdecode(body, at=0):
    kind = body[at:at + 1]
    if kind == b"d":
        at += 1
        out = {}
        while body[at:at + 1] != b"e":
            key, at = bdecode(body, at)
            value, at = bdecode(body, at)
            out[key] = value
        return out, at + 1
    if kind == b"l":
        at += 1
        out = []
        while body[at:at + 1] != b"e":
            value, at = bdecode(body, at)
            out.append(value)
        return out, at + 1
    if kind == b"i":
        end = body.index(b"e", at)
        return int(body[at + 1:end]), end + 1
    colon = body.index(b":", at)
    length = int(body[at:colon])
    start = colon + 1
    return body[start:start + length].decode("latin-1"), start + length


class Ng:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(3)
        self.serial = 0

    def call(self, command):
        self.serial += 1
        cookie = f"subprobe-{self.serial}".encode()
        datagram = cookie + b" " + bencode(command)
        for _ in range(5):
            self.sock.sendto(datagram, (NODE, PORT))
            try:
                reply, _ = self.sock.recvfrom(200000)
            except socket.timeout:
                continue
            body = reply.split(b" ", 1)[1]
            decoded, _ = bdecode(body)
            return decoded
        return {"result": "error", "error-reason": "no reply after 5 attempts"}

    def local_address(self):
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect((NODE, PORT))
        address = probe.getsockname()[0]
        probe.close()
        return address


def tone_wav(hz, seconds, rate=8000):
    frames = bytearray()
    for n in range(int(rate * seconds)):
        frames += struct.pack("<h", int(12000 * math.sin(2 * math.pi * hz * n / rate)))
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as out:
        out.setnchannels(1)
        out.setsampwidth(2)
        out.setframerate(rate)
        out.writeframes(bytes(frames))
    return buffer.getvalue()


def answer_sdp(local_ip, port, session_id):
    return (
        "v=0\r\n"
        f"o=- {session_id} {session_id} IN IP4 {local_ip}\r\n"
        "s=mss-subscribe-probe\r\n"
        f"c=IN IP4 {local_ip}\r\n"
        "t=0 0\r\n"
        f"m=audio {port} RTP/AVP 0 8 101\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=rtpmap:8 PCMA/8000\r\n"
        "a=rtpmap:101 telephone-event/8000\r\n"
        "a=ptime:20\r\n"
        "a=recvonly\r\n"
    )


ULAW_TABLE = []
ALAW_TABLE = []


def build_tables():
    for byte in range(256):
        value = ~byte & 0xFF
        sign = value & 0x80
        exponent = (value >> 4) & 0x07
        mantissa = value & 0x0F
        sample = ((mantissa << 3) + 0x84) << exponent
        sample -= 0x84
        ULAW_TABLE.append(-sample if sign else sample)
    for byte in range(256):
        value = byte ^ 0x55
        sign = value & 0x80
        exponent = (value >> 4) & 0x07
        mantissa = value & 0x0F
        if exponent == 0:
            sample = (mantissa << 4) + 8
        else:
            sample = ((mantissa << 4) + 0x108) << (exponent - 1)
        ALAW_TABLE.append(-sample if sign else sample)


def goertzel(samples, hz, rate=8000):
    if not samples:
        return 0.0
    omega = 2 * math.pi * hz / rate
    coeff = 2 * math.cos(omega)
    s1 = s2 = 0.0
    for sample in samples:
        s0 = sample + coeff * s1 - s2
        s2, s1 = s1, s0
    power = s1 * s1 + s2 * s2 - coeff * s1 * s2
    return power / len(samples)


def multi_answer_sdp(local_ip, ports, session_id):
    head = (
        "v=0\r\n"
        f"o=- {session_id} {session_id} IN IP4 {local_ip}\r\n"
        "s=mss-subscribe-probe\r\n"
        f"c=IN IP4 {local_ip}\r\n"
        "t=0 0\r\n"
    )
    body = ""
    for port in ports:
        body += (
            f"m=audio {port} RTP/AVP 0 8 101\r\n"
            "a=rtpmap:0 PCMU/8000\r\n"
            "a=rtpmap:8 PCMA/8000\r\n"
            "a=rtpmap:101 telephone-event/8000\r\n"
            "a=ptime:20\r\n"
            "a=recvonly\r\n"
        )
    return head + body


def multi_trial(ng, local_ip, tags):
    log("\n--- one subscription carrying every tag (the safe model) ---")
    reply = ng.call({
        "command": "subscribe request",
        "call-id": CALL_ID,
        "from-tags": tags,
        "codec": {"transcode": ["PCMU"]},
    })
    if reply.get("result") != "ok":
        log(f"  REFUSED: {reply.get('error-reason')}")
        return
    to_tag = reply.get("to-tag")
    offered = reply.get("sdp", "")
    stream_ports = re.findall(r"m=audio (\d+) ", offered)
    labels = re.findall(r"a=label:(\S+)", offered)
    log(f"  offered {len(stream_ports)} streams, source ports {stream_ports}, "
        f"labels {labels or 'none'}")

    listeners = [Subscription(f"stream{i}") for i in range(len(stream_ports))]
    answer = ng.call({
        "command": "subscribe answer",
        "call-id": CALL_ID,
        "to-tag": to_tag,
        "sdp": multi_answer_sdp(local_ip, [held.port for held in listeners], 4242),
    })
    log(f"  answer: {answer.get('result')} {answer.get('error-reason') or ''}")

    deadline = time.time() + LISTEN_SECONDS
    while time.time() < deadline:
        for held in listeners:
            held.drain()

    for index, held in enumerate(listeners):
        loud = 0.0
        if held.samples:
            loud = math.sqrt(sum(x * x for x in held.samples) / len(held.samples))
        log(f"  stream {index} (source_port {stream_ports[index]}): "
            f"datagrams={held.datagrams} rms={loud:.0f} ssrcs={sorted(held.ssrcs)}")
    log(f"  call alive after listening: {alive(ng)}")
    ng.call({"command": "unsubscribe", "call-id": CALL_ID, "to-tag": to_tag})
    for held in listeners:
        held.close()


class Subscription:
    def __init__(self, tag):
        self.tag = tag
        self.to_tag = None
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", 0))
        self.sock.settimeout(0.2)
        self.port = self.sock.getsockname()[1]
        self.datagrams = 0
        self.samples = []
        self.payload_types = {}
        self.ssrcs = set()

    def drain(self):
        while True:
            try:
                packet, _ = self.sock.recvfrom(4096)
            except socket.timeout:
                return
            except OSError:
                return
            if len(packet) < 12:
                continue
            self.datagrams += 1
            self.ssrcs.add(f"{int.from_bytes(packet[8:12], 'big')}")
            payload_type = packet[1] & 0x7F
            self.payload_types[payload_type] = self.payload_types.get(payload_type, 0) + 1
            body = packet[12:]
            if payload_type == 0:
                self.samples.extend(ULAW_TABLE[b] for b in body)
            elif payload_type == 8:
                self.samples.extend(ALAW_TABLE[b] for b in body)

    def close(self):
        self.sock.close()


def alive(ng):
    return ng.call({"command": "query", "call-id": CALL_ID}).get("result") == "ok"


def trial(ng, local_ip, tags, index):
    log(f"\n--- trial {index + 1} of {TRIALS} ---")
    log(f"  call alive before this trial: {alive(ng)}")
    subscriptions = []
    for position, tag in enumerate(tags):
        reply = ng.call({
            "command": "subscribe request",
            "call-id": CALL_ID,
            "from-tags": [tag],
            "codec": {"transcode": ["PCMU"]},
        })
        if reply.get("result") != "ok":
            log(f"  subscribe for {tag}: REFUSED ({reply.get('error-reason')})")
            continue
        held = Subscription(tag)
        held.to_tag = reply.get("to-tag")
        offered = reply.get("sdp", "")
        streams = len(re.findall(r"m=audio (\d+) ", offered))
        labels = re.findall(r"a=label:(\S+)", offered)
        session_id = 1 if SAME_SESSION_ID else 1000 + position
        answer = ng.call({
            "command": "subscribe answer",
            "call-id": CALL_ID,
            "to-tag": held.to_tag,
            "sdp": answer_sdp(local_ip, held.port, session_id),
        })
        ok = answer.get("result") == "ok"
        log(f"  subscribed to {tag}: streams={streams} labels={labels or 'none'} "
            f"to-tag={held.to_tag} recv_port={held.port} answer={'ok' if ok else answer}")
        subscriptions.append(held)

    for position, tag in enumerate(tags):
        if SKIP_PLAY:
            log("  not injecting anything (SKIP_PLAY)")
            break
        hz = TONES[position % len(TONES)]
        played = ng.call({
            "command": "play media",
            "call-id": CALL_ID,
            "from-tag": tag,
            "blob": tone_wav(hz, TONE_SECONDS),
        })
        log(f"  play {hz} Hz with from-tag={tag}: {played.get('result')}")

    deadline = time.time() + LISTEN_SECONDS
    while time.time() < deadline:
        for held in subscriptions:
            held.drain()

    findings = []
    for held in subscriptions:
        scores = {hz: goertzel(held.samples, hz) for hz in TONES}
        loudest = max(scores, key=scores.get) if held.samples else None
        total = sum(scores.values()) or 1.0
        share = (scores[loudest] / total) if loudest else 0.0
        verdict = f"{loudest} Hz" if loudest and share > 0.6 else "no clear tone"
        loudness = 0.0
        if held.samples:
            loudness = math.sqrt(sum(s * s for s in held.samples) / len(held.samples))
        rate = held.datagrams / max(LISTEN_SECONDS, 0.001)
        readable = {hz: f"{value:.3g}" for hz, value in scores.items()}
        log(f"  subscription to {held.tag}: datagrams={held.datagrams} "
            f"({rate:.0f}/s) pts={held.payload_types or '{}'} rms={loudness:.0f} "
            f"carries={verdict} share={share:.2f} scores={readable}")
        findings.append((held.tag, held.datagrams, loudest if share > 0.6 else None))

    log(f"  call alive after listening: {alive(ng)}")
    if SKIP_UNSUBSCRIBE:
        log("  leaving the subscriptions in place (SKIP_UNSUBSCRIBE)")
        for held in subscriptions:
            held.close()
        return findings
    for held in subscriptions:
        reply = ng.call({"command": "unsubscribe", "call-id": CALL_ID, "to-tag": held.to_tag})
        log(f"  unsubscribe {held.tag}: {reply.get('result')} "
            f"{reply.get('error-reason') or ''}")
        held.close()
        log(f"    call alive after that unsubscribe: {alive(ng)}")
    return findings


def main():
    if not CALL_ID:
        log("CALL_ID is required; take it from /shared/call.env")
        return 2
    build_tables()
    ng = Ng()
    local_ip = ng.local_address()

    queried = ng.call({"command": "query", "call-id": CALL_ID})
    if DUMP_QUERY:
        import json as _json
        log(_json.dumps(queried, indent=1, default=str)[:6000])
    if queried.get("result") != "ok":
        log(f"rtpengine does not know call {CALL_ID}: {queried.get('error-reason')}")
        return 1
    tags = list(queried.get("tags", {}).keys())
    ssrcs = queried.get("SSRC", {})
    log(f"rtpengine reports SSRCs: {list(ssrcs.keys()) if isinstance(ssrcs, dict) else ssrcs}")
    for tag, detail in queried.get("tags", {}).items():
        if not isinstance(detail, dict):
            continue
        medias = detail.get("medias", [])
        seen = []
        for media in medias if isinstance(medias, list) else []:
            if isinstance(media, dict):
                seen.append({k: media.get(k) for k in ("type", "label", "ssrc") if k in media})
        log(f"   tag {tag}: medias={seen or len(medias)}")
    log(f"call {CALL_ID} has {len(tags)} participants: {tags}")
    log(f"probe address {local_ip}, tones {TONES} Hz, "
        f"{'ONE SHARED' if SAME_SESSION_ID else 'per-leg'} sdp session id")
    if len(tags) < 2:
        log("this probe wants a two-party call")
        return 1
    if ONLY_TAG_INDEX != "":
        chosen = tags[int(ONLY_TAG_INDEX)]
        log(f"subscribing to {chosen} ALONE, to see whether one subscription "
            f"behaves differently from two")
        tags = [chosen]

    if MULTI:
        multi_trial(ng, local_ip, tags)
        return 0

    everything = []
    for index in range(TRIALS):
        everything.append(trial(ng, local_ip, tags, index))
        time.sleep(1)

    log("\n=== summary ===")
    for position, tag in enumerate(tags):
        delivered = 0
        carried = {}
        for findings in everything:
            for found_tag, datagrams, tone in findings:
                if found_tag != tag:
                    continue
                if datagrams > 0:
                    delivered += 1
                if tone:
                    carried[tone] = carried.get(tone, 0) + 1
        own = TONES[position % len(TONES)]
        log(f"  subscription to {tag}:")
        log(f"     media arrived in {delivered}/{TRIALS} trials")
        log(f"     tones heard: {carried or 'none'} (this participant's own tone is {own} Hz)")
        for tone, count in carried.items():
            if tone == own:
                log("     -> carries the tone aimed AT this participant")
            else:
                log("     -> carries the tone aimed at the OTHER participant")
    return 0


sys.exit(main())
