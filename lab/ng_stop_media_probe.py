"""Measures how precisely rtpengine's `stop media` can be aimed (defect D2).

MSS used to stop every playback on a call because it sent `stop media` with
`all: all`. The fix passes the from-tag the playback was started with, which
raises two questions this probe answers against a live rtpengine:

  1. Does `stop media {from-tag: X}` stop only X's player, leaving another
     participant's player running?
  2. Can one participant hold two players at once? If it cannot, then aiming
     at a participant is the same thing as aiming at a playback, and the only
     residual is a playback started with `all: all`.

The call is built with NG offer/answer, both legs transmit mu-law silence and
count non-silent payloads, so any audio a leg reports came from a player.
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
CALL_ID = os.environ.get("CALL_ID", "stop-media-probe-1")
FROM_TAG = "tagA"
TO_TAG = "tagB"
CALLER_RTP_PORT = int(os.environ.get("CALLER_RTP_PORT", "40020"))
CALLEE_RTP_PORT = int(os.environ.get("CALLEE_RTP_PORT", "40022"))
PCMU_PAYLOAD_TYPE = 0
PCMU_SILENCE = 0xFF
SETTLE = 1.5
REPEATS = 30
TAIL_PACKETS = 3


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
    """Cookies carry a per-run prefix: rtpengine caches replies per cookie, so a
    serial that restarts at 1 makes a later run read the previous run's answers
    (defect D12) and every measurement in it is a fiction."""

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(3)
        self.serial = 0
        self.prefix = f"stopm-{os.getpid()}-{int(time.time())}"

    def send(self, command):
        self.serial += 1
        cookie = f"{self.prefix}-{self.serial}".encode()
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
        "s=stop-media-probe\r\n"
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


class Leg:
    def __init__(self, name, port, dest):
        self.name = name
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        self.sock.setblocking(False)
        self.dest = dest
        self.seq = 2000
        self.ts = 0
        self.ssrc = 0x55555555 ^ port
        self.received = 0
        self.non_silent = 0

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
            except (socket.timeout, BlockingIOError, OSError):
                return
            if len(datagram) < 13:
                continue
            self.received += 1
            if any(byte != PCMU_SILENCE for byte in datagram[12:]):
                self.non_silent += 1

    def reset(self):
        self.received = 0
        self.non_silent = 0


def run_legs(legs, stop):
    while not stop.is_set():
        for leg in legs:
            leg.send_silence()
            leg.drain()
        time.sleep(0.02)


def heard(legs):
    return "  ".join(f"{leg.name}: {leg.non_silent}/{leg.received} non-silent" for leg in legs)


def observe(legs, seconds=SETTLE):
    for leg in legs:
        leg.reset()
    time.sleep(seconds)
    return {leg.name: leg.non_silent for leg in legs}


def play(ng, tag, wav):
    command = {
        "command": "play media",
        "call-id": CALL_ID,
        "blob": wav,
        "repeat-times": REPEATS,
    }
    if tag:
        command["from-tag"] = tag
    else:
        command["all"] = "all"
    return ng.ok(command)


def stop_play(ng, tag):
    command = {"command": "stop media", "call-id": CALL_ID}
    if tag:
        command["from-tag"] = tag
    else:
        command["all"] = "all"
    return ng.ok(command)


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
    halt = threading.Event()
    pumping = threading.Thread(target=run_legs, args=(legs, halt), daemon=True)
    pumping.start()
    time.sleep(1.5)
    log(f"call is live: caller->{caller_port} callee->{callee_port}, both sending silence")

    wav = tone_wav()
    log(f"each player is a {len(wav)}-byte 2 s 440 Hz wav repeated {REPEATS} times\n")

    log("1. two players, one per participant, then stop media {from-tag: tagA}")
    for tag in (FROM_TAG, TO_TAG):
        body, error = play(ng, tag, wav)
        log(f"   play media from-tag={tag}: {'accepted' if body else 'REJECTED -> ' + str(error)}")
    both = observe(legs)
    log(f"   both playing: {both}")
    body, error = stop_play(ng, FROM_TAG)
    log(f"   stop media from-tag={FROM_TAG}: {'accepted' if body else 'REJECTED -> ' + str(error)}")
    after = observe(legs)
    log(f"   after the targeted stop: {after}")
    silenced = [
        name
        for name, count in after.items()
        if count <= TAIL_PACKETS and both[name] > TAIL_PACKETS
    ]
    still = [name for name, count in after.items() if count > TAIL_PACKETS]
    log(f"   VERDICT: silenced {silenced or 'nothing'}, still playing {still or 'nothing'}")
    stop_play(ng, TO_TAG)
    stop_play(ng, None)
    time.sleep(0.5)

    log("\n2. two players on the SAME participant")
    first, error = play(ng, FROM_TAG, wav)
    log(f"   first play media from-tag={FROM_TAG}: {'accepted' if first else 'REJECTED -> ' + str(error)}")
    second, error = play(ng, FROM_TAG, wav)
    log(f"   second play media from-tag={FROM_TAG}: {'accepted' if second else 'REJECTED -> ' + str(error)}")
    during = observe(legs)
    log(f"   while both are requested: {during}")
    stop_play(ng, FROM_TAG)
    ended = observe(legs, seconds=3.0)
    log(f"   after one stop media from-tag={FROM_TAG}: {ended} (3 s window)")
    log(
        "   VERDICT: one stop cleared the participant, so a participant holds "
        "no independently stoppable second player"
        if all(count <= TAIL_PACKETS for count in ended.values())
        else "   VERDICT: audio survived the stop, so a participant held more than one player"
    )

    log("\n3. a player started with all:all, stopped with one from-tag")
    body, error = play(ng, None, wav)
    log(f"   play media all=all: {'accepted' if body else 'REJECTED -> ' + str(error)}")
    everywhere = observe(legs)
    log(f"   while playing: {everywhere}")
    stop_play(ng, FROM_TAG)
    remains = observe(legs)
    log(f"   after stop media from-tag={FROM_TAG}: {remains}")
    stop_play(ng, None)

    halt.set()
    pumping.join(timeout=2)
    ng.ok({"command": "delete", "call-id": CALL_ID})
    log("\nprobe done")


main()
