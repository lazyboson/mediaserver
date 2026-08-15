"""Finds out whether rtpengine can put audio back INTO a live call.

A subscription is one-way, so bot speech needs a different mechanism. This
probe plays both call legs itself, injects audio through each candidate NG
command, and reports which leg actually heard it — the question that decides
whether interactive voice-AI needs a Phase-3 inline leg or can ride the tap.

Both legs transmit mu-law silence, so any non-silent payload a leg receives
came from the injection and nothing else.
"""

import base64
import io
import math
import os
import re
import socket
import struct
import threading
import time
import wave

NODE = os.environ.get("NG_NODE", "172.31.99.10")
PORT = int(os.environ.get("NG_PORT", "22222"))
SELF_IP = os.environ.get("SELF_IP", "172.31.99.20")
CALL_ID = os.environ.get("CALL_ID", "inject-probe-1")
FROM_TAG = "tagA"
TO_TAG = "tagB"
CALLER_RTP_PORT = 40000
CALLEE_RTP_PORT = 40002
PCMU_PAYLOAD_TYPE = 0
PCMU_SILENCE = 0xFF
LISTEN_SECONDS = 3.0


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
        cookie = f"inj-{self.serial}".encode()
        datagram = cookie + b" " + bencode(command)
        for _ in range(5):
            self.sock.sendto(datagram, (NODE, PORT))
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


def sdp_for(port):
    return (
        "v=0\r\n"
        f"o=- 1 1 IN IP4 {SELF_IP}\r\n"
        "s=inject-probe\r\n"
        f"c=IN IP4 {SELF_IP}\r\n"
        "t=0 0\r\n"
        f"m=audio {port} RTP/AVP 0 101\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=rtpmap:101 telephone-event/8000\r\n"
        "a=ptime:20\r\n"
        "a=sendrecv\r\n"
    )


def media_port(sdp):
    ports = [int(p) for p in re.findall(r"m=audio (\d+) ", sdp)]
    return ports[0] if ports else None


def tone_wav(hz=440, seconds=2.0, rate=8000):
    """A plain PCM WAV that ffmpeg can decode, built without numpy."""
    frames = bytearray()
    for n in range(int(rate * seconds)):
        sample = int(12000 * math.sin(2 * math.pi * hz * n / rate))
        frames += struct.pack("<h", sample)
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as out:
        out.setnchannels(1)
        out.setsampwidth(2)
        out.setframerate(rate)
        out.writeframes(bytes(frames))
    return buffer.getvalue()


class Leg:
    def __init__(self, name, port, dest):
        self.name = name
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        self.sock.setblocking(False)
        self.dest = dest
        self.seq = 1000
        self.ts = 0
        self.ssrc = 0x33333333 ^ port
        self.received = 0
        self.non_silent = 0
        self.payload_types = set()

    def send_silence(self):
        header = struct.pack(
            "!BBHII", 0x80, PCMU_PAYLOAD_TYPE, self.seq & 0xFFFF, self.ts, self.ssrc
        )
        self.sock.sendto(header + bytes([PCMU_SILENCE]) * 160, self.dest)
        self.seq += 1
        self.ts += 160

    def drain(self):
        while True:
            try:
                datagram, _ = self.sock.recvfrom(2048)
            except (socket.timeout, BlockingIOError):
                return
            except OSError:
                return
            if len(datagram) < 13:
                continue
            self.received += 1
            self.payload_types.add(datagram[1] & 0x7F)
            payload = datagram[12:]
            if any(byte != PCMU_SILENCE for byte in payload):
                self.non_silent += 1

    def reset(self):
        self.received = 0
        self.non_silent = 0
        self.payload_types = set()


def stream_into(port, seconds):
    """Pushes continuous non-silent PCMU at rtpengine, the way streaming TTS would."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 40010))
    seq, ts, sent = 5000, 0, 0
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        header = struct.pack("!BBHII", 0x80, PCMU_PAYLOAD_TYPE, seq & 0xFFFF, ts, 0x44444444)
        sock.sendto(header + bytes(range(160)), (NODE, port))
        seq += 1
        ts += 160
        sent += 1
        time.sleep(0.02)
    sock.close()
    log(f"      pushed {sent} rtp packets into the publish port")


def run_legs(legs, stop):
    while not stop.is_set():
        for leg in legs:
            leg.send_silence()
            leg.drain()
        time.sleep(0.02)


def attempt(ng, legs, name, command, stop_command=None):
    for leg in legs:
        leg.reset()
    body, error = ng.ok(command)
    if body is None:
        log(f"  {name}: REJECTED -> {error}")
        return
    time.sleep(LISTEN_SECONDS)
    heard = "  ".join(
        f"{leg.name}: {leg.non_silent}/{leg.received} non-silent" for leg in legs
    )
    verdict = "AUDIO REACHED THE CALL" if any(l.non_silent for l in legs) else "no audio"
    log(f"  {name}: accepted, {verdict}")
    log(f"      {heard}")
    if stop_command:
        ng.ok(stop_command)
        time.sleep(0.3)


def main():
    ng = Ng()
    if ng.ok({"command": "ping"})[0] is None:
        log(f"rtpengine at {NODE}:{PORT} did not answer ping")
        return

    offer, error = ng.ok({
        "command": "offer", "call-id": CALL_ID,
        "from-tag": FROM_TAG, "sdp": sdp_for(CALLER_RTP_PORT),
    })
    if offer is None:
        log(f"offer failed: {error}")
        return
    callee_port = media_port(field(offer, "sdp") or "")

    answer, error = ng.ok({
        "command": "answer", "call-id": CALL_ID,
        "from-tag": FROM_TAG, "to-tag": TO_TAG, "sdp": sdp_for(CALLEE_RTP_PORT),
    })
    if answer is None:
        log(f"answer failed: {error}")
        return
    caller_port = media_port(field(answer, "sdp") or "")

    legs = [
        Leg("caller/tagA", CALLER_RTP_PORT, (NODE, caller_port)),
        Leg("callee/tagB", CALLEE_RTP_PORT, (NODE, callee_port)),
    ]
    stop = threading.Event()
    pumping = threading.Thread(target=run_legs, args=(legs, stop), daemon=True)
    pumping.start()
    time.sleep(1.5)
    log(f"call is live: caller->{caller_port} callee->{callee_port}, both sending silence")

    wav = tone_wav()
    log(f"injecting a {len(wav)}-byte 440Hz wav\n")

    log("play media, targeted at from-tag tagA:")
    attempt(ng, legs, "blob64",
            {"command": "play media", "call-id": CALL_ID, "from-tag": FROM_TAG,
             "blob64": base64.b64encode(wav).decode()},
            {"command": "stop media", "call-id": CALL_ID, "from-tag": FROM_TAG})
    attempt(ng, legs, "blob (raw bytes)",
            {"command": "play media", "call-id": CALL_ID, "from-tag": FROM_TAG,
             "blob": wav},
            {"command": "stop media", "call-id": CALL_ID, "from-tag": FROM_TAG})

    log("\nplay media, targeted at every participant:")
    attempt(ng, legs, "flags=[all]",
            {"command": "play media", "call-id": CALL_ID, "from-tag": FROM_TAG,
             "blob": wav, "flags": ["all"]},
            {"command": "stop media", "call-id": CALL_ID, "from-tag": FROM_TAG,
             "flags": ["all"]})
    attempt(ng, legs, "all=all",
            {"command": "play media", "call-id": CALL_ID, "all": "all", "blob": wav},
            {"command": "stop media", "call-id": CALL_ID, "all": "all"})

    log("\npublish, the offer/answer-free injection primitive:")
    published, error = ng.ok({
        "command": "publish", "call-id": CALL_ID, "from-tag": "tagInject",
        "sdp": sdp_for(40010),
    })
    if published is None:
        log(f"  publish: REJECTED -> {error}")
    else:
        ingress = media_port(field(published, "sdp") or "")
        log(f"  publish: accepted, rtpengine receives on {ingress}")
        for leg in legs:
            leg.reset()
        stream_into(ingress, seconds=2.0)
        time.sleep(0.5)
        heard = "  ".join(
            f"{leg.name}: {leg.non_silent}/{leg.received} non-silent" for leg in legs
        )
        reached = any(leg.non_silent for leg in legs)
        log(f"  published rtp: {'REACHED THE CALL' if reached else 'did NOT reach the call legs'}")
        log(f"      {heard}")

    stop.set()
    pumping.join(timeout=2)
    ng.ok({"command": "delete", "call-id": CALL_ID})
    log("\nprobe done")


main()
