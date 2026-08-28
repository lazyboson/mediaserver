"""Plays the part of MicroSIP so the echo loop can be verified without a human.

Runs on the WSL host, not in a container, so it uses exactly the path a real
softphone uses: SIP to the published 127.0.0.1:5060, RTP to the published
30000-30099 range, PCMA like MicroSIP negotiates. It speaks a known sentence
(a wav of real speech), records everything it hears per SSRC, and hangs up.

Success is measurable afterwards: the bridge log should transcribe the spoken
sentence, and one of the recorded SSRCs should carry the TTS reply.

Concurrent instances (added for the item-19 soak suite): everything that must
be unique per caller is an env var, and the defaults are exactly the single
caller this script has always been, so every existing drill is unchanged.
Give each instance its own SIP_PORT, RTP_PORT, FROM_TAG, RTP_SSRC and CALL_ID
and N of these run side by side. CALL_ID matters most: with OpenSIPS in the
path rtpengine's call-id *is* the SIP Call-ID, so a caller that names its own
call-id tells the driver which call to tap without any discovery step.

Endpoint-side impairment (IMPAIR_LOSS, IMPAIR_REORDER, IMPAIR_DUPLICATE,
IMPAIR_JITTER_MS) damages this caller's own outbound RTP. It is not a substitute
for `tc netem` on the tap link -- it is upstream of rtpengine, which is the
*other* place testing.md's impairment matrix says to inject -- but it needs no
kernel support, which is why it exists: the WSL2 kernel this lab runs on has
CONFIG_NET_SCH_NETEM unset. Measured in the item-19 soak: rtpengine forwards the
holes into the tap, so MSS reports the loss and conceals it.

Env: SIP_PORT, RTP_PORT, CALL_ID, FROM_TAG, RTP_SSRC, EAR_PREFIX, WRITE_EARS,
SPEECH_WAV, EAR_DIR, CALL_SECONDS, TALK_EVERY, DIAL, IMPAIR_LOSS,
IMPAIR_REORDER, IMPAIR_DUPLICATE, IMPAIR_JITTER_MS, IMPAIR_SEED.
"""

import atexit
import os
import random
import re
import socket
import struct
import subprocess
import sys
import time
import wave

PROXY = ("127.0.0.1", 5060)
SIP_PORT = int(os.environ.get("SIP_PORT", "45070"))
RTP_PORT = int(os.environ.get("RTP_PORT", "45072"))
SPEECH_WAV = os.environ.get("SPEECH_WAV", "out/bridge_tts.wav")
EAR_DIR = os.environ.get("EAR_DIR", "out")
EAR_PREFIX = os.environ.get("EAR_PREFIX", "host_ear")
WRITE_EARS = os.environ.get("WRITE_EARS", "1") not in ("0", "no", "false")
CALL_SECONDS = float(os.environ.get("CALL_SECONDS", "40"))
TALK_EVERY = float(os.environ.get("TALK_EVERY", "9"))
DIAL = os.environ.get("DIAL", "9000")
FROM_TAG = os.environ.get("FROM_TAG", "hosttest")
RTP_SSRC = int(os.environ.get("RTP_SSRC", "0x77777777"), 0)
IMPAIR_LOSS = float(os.environ.get("IMPAIR_LOSS", "0"))
IMPAIR_REORDER = float(os.environ.get("IMPAIR_REORDER", "0"))
IMPAIR_JITTER_MS = float(os.environ.get("IMPAIR_JITTER_MS", "0"))
IMPAIR_DUPLICATE = float(os.environ.get("IMPAIR_DUPLICATE", "0"))
IMPAIR_SEED = os.environ.get("IMPAIR_SEED", "")

ALAW_SILENCE = 0xD5


def log(message):
    print(f"test-caller: {message}", flush=True)


def linear_to_alaw(sample):
    sign = 0x80 if sample >= 0 else 0x00
    if sample < 0:
        sample = -sample - 1
    if sample > 32635:
        sample = 32635
    if sample < 256:
        exponent = 0
        mantissa = sample >> 4
    else:
        exponent = 1
        mask = 512
        while exponent < 7 and sample >= mask:
            exponent += 1
            mask <<= 1
        exponent -= 1
        mantissa = (sample >> (exponent + 4)) & 0x0F
    return (sign | (exponent << 4) | mantissa) ^ 0x55


def alaw_to_linear(byte):
    byte ^= 0x55
    sign = byte & 0x80
    exponent = (byte >> 4) & 0x07
    mantissa = byte & 0x0F
    sample = (mantissa << 4) + 8
    if exponent > 0:
        sample = (sample + 0x100) << (exponent - 1)
    return sample if sign else -sample


def ulaw_to_linear(byte):
    byte = ~byte & 0xFF
    sign = byte & 0x80
    exponent = (byte >> 4) & 0x07
    mantissa = byte & 0x0F
    sample = ((mantissa << 3) + 0x84) << exponent
    sample -= 0x84
    return -sample if sign else sample


def ensure_speech_wav(path):
    """Returns path, generating a stand-in first if nothing is there.

    out/bridge_tts.wav is what lab/bridge_tts_dump.py captures from the real
    stream-llm-bridge, and *.wav is gitignored, so a fresh clone has no speech
    at all: every drill that dials through this script died here with
    FileNotFoundError before placing a call. The fallback is the lab's own
    generator at 8 kHz -- tones alternating with digital silence, not speech.
    That is enough for a drill asking whether audio flowed and where it went,
    and NOT enough for one judging intelligibility or sample-level parity
    against another recorder, which is why the substitution is logged.
    """
    if os.path.exists(path):
        return path
    generator = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "webrtc", "make_agent_audio.py")
    if not os.path.exists(generator):
        raise SystemExit(f"{path} is missing, and {generator} is not here to make one")
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    log(f"{path} is missing; generating a tone stand-in with make_agent_audio.py")
    subprocess.run(
        [sys.executable, generator],
        check=True,
        env=dict(os.environ, OUT=path, RATE="8000", SECONDS="9",
                 SOUND_SECONDS="2", SILENCE_SECONDS="1"),
    )
    return path


def speech_frames():
    with wave.open(ensure_speech_wav(SPEECH_WAV)) as w:
        if w.getframerate() != 8000 or w.getnchannels() != 1:
            raise SystemExit(f"{SPEECH_WAV} must be 8kHz mono")
        pcm = struct.unpack(f"<{w.getnframes()}h", w.readframes(w.getnframes()))
    alaw = bytes(linear_to_alaw(s) for s in pcm)
    padded = alaw + bytes([ALAW_SILENCE]) * ((-len(alaw)) % 160)
    return [padded[i:i + 160] for i in range(0, len(padded), 160)]


def header(message, wanted):
    for line in message.split("\r\n")[1:]:
        if not line:
            break
        name, _, value = line.partition(":")
        if name.strip().lower() == wanted.lower():
            return value.strip()
    return None


def all_headers(message, wanted):
    out = []
    for line in message.split("\r\n")[1:]:
        if not line:
            break
        name, _, value = line.partition(":")
        if name.strip().lower() == wanted.lower():
            out.append(value.strip())
    return out


def sdp_endpoint(message):
    body = message.split("\r\n\r\n", 1)[1] if "\r\n\r\n" in message else ""
    hosts = re.findall(r"c=IN IP4 ([0-9.]+)", body)
    port = re.search(r"m=audio (\d+)", body)
    if not hosts or not port:
        return None
    return (hosts[-1], int(port.group(1)))


def impairer(rtp, dest):
    """Damages this caller's outbound RTP the way netem would, in userspace.

    Returns (emit, drain, stats). `emit` is called once per paced packet and
    decides whether it goes on the wire now, late, out of order, or not at all;
    `drain` flushes whatever is still held when the call ends. A dropped packet
    leaves a hole in the sequence space, which is what makes it loss rather than
    a shorter call.
    """
    rng = random.Random(int(IMPAIR_SEED) if IMPAIR_SEED else None)
    stats = {"offered": 0, "sent": 0, "dropped": 0, "reordered": 0, "delayed": 0,
             "duplicated": 0}
    pending = []
    held = []

    def wire(packet):
        rtp.sendto(packet, dest)
        stats["sent"] += 1

    def release(packet):
        if IMPAIR_JITTER_MS > 0:
            due = time.monotonic() + rng.uniform(0.0, IMPAIR_JITTER_MS / 1000.0)
            pending.append((due, packet))
            stats["delayed"] += 1
        else:
            wire(packet)

    def emit(packet):
        stats["offered"] += 1
        if IMPAIR_LOSS > 0 and rng.random() < IMPAIR_LOSS:
            stats["dropped"] += 1
        elif IMPAIR_REORDER > 0 and not held and rng.random() < IMPAIR_REORDER:
            held.append(packet)
            stats["reordered"] += 1
        else:
            release(packet)
            if IMPAIR_DUPLICATE > 0 and rng.random() < IMPAIR_DUPLICATE:
                release(packet)
                stats["duplicated"] += 1
            if held:
                release(held.pop())
        now = time.monotonic()
        ready = sorted((due, packet) for due, packet in pending if due <= now)
        pending[:] = [(due, packet) for due, packet in pending if due > now]
        for _due, packet in ready:
            wire(packet)

    def drain():
        while held:
            wire(held.pop())
        for _due, packet in sorted(pending):
            wire(packet)
        pending.clear()

    return emit, drain, stats


def main():
    frames = speech_frames()
    log(f"speech: {len(frames)} frames ({len(frames) * 0.02:.1f}s) from {SPEECH_WAV}")

    sip = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sip.bind(("0.0.0.0", SIP_PORT))
    sip.settimeout(10)
    rtp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rtp.bind(("0.0.0.0", RTP_PORT))
    rtp.setblocking(False)

    call_id = os.environ.get("CALL_ID") or f"host-test-{int(time.time())}"
    from_tag = FROM_TAG
    sdp = ("v=0\r\n"
           f"o=- 1 1 IN IP4 127.0.0.1\r\ns=t\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\n"
           f"m=audio {RTP_PORT} RTP/AVP 8 101\r\n"
           "a=rtpmap:8 PCMA/8000\r\na=rtpmap:101 telephone-event/8000\r\n"
           "a=ptime:20\r\na=sendrecv\r\n")
    invite = (f"INVITE sip:{DIAL}@127.0.0.1 SIP/2.0\r\n"
              f"Via: SIP/2.0/UDP 127.0.0.1:{SIP_PORT};branch=z9hG4bK-{call_id};rport\r\n"
              f"From: <sip:tester@127.0.0.1>;tag={from_tag}\r\n"
              f"To: <sip:{DIAL}@127.0.0.1>\r\n"
              f"Call-ID: {call_id}\r\nCSeq: 1 INVITE\r\nMax-Forwards: 70\r\n"
              f"Contact: <sip:tester@127.0.0.1:{SIP_PORT}>\r\n"
              "Content-Type: application/sdp\r\n"
              f"Content-Length: {len(sdp)}\r\n\r\n{sdp}")
    sip.sendto(invite.encode(), PROXY)

    ok = None
    for _ in range(6):
        data, _ = sip.recvfrom(65535)
        message = data.decode("utf-8", "replace")
        status = message.split("\r\n")[0]
        log(f"<- {status}")
        if " 200 " in status:
            ok = message
            break
        if re.search(r"SIP/2.0 [456]", status):
            raise SystemExit(f"call rejected: {status}")
    if ok is None:
        raise SystemExit("no 200 OK")

    dest = sdp_endpoint(ok)
    to = header(ok, "To")
    contact = header(ok, "Contact") or ""
    contact_uri = re.search(r"<([^>]+)>", contact)
    contact_uri = contact_uri.group(1) if contact_uri else f"sip:{DIAL}@127.0.0.1"
    routes = all_headers(ok, "Record-Route")
    route_lines = "".join(f"Route: {r}\r\n" for r in routes)
    log(f"answered; sending rtp to {dest[0]}:{dest[1]}")

    def in_dialog(method, cseq):
        return (f"{method} {contact_uri} SIP/2.0\r\n"
                f"Via: SIP/2.0/UDP 127.0.0.1:{SIP_PORT};branch=z9hG4bK-{call_id}-{cseq};rport\r\n"
                f"From: <sip:tester@127.0.0.1>;tag={from_tag}\r\n"
                f"To: {to}\r\n"
                f"Call-ID: {call_id}\r\nCSeq: {cseq} {method}\r\nMax-Forwards: 70\r\n"
                f"{route_lines}"
                "Content-Length: 0\r\n\r\n").encode()

    sip.sendto(in_dialog("ACK", 1), PROXY)
    atexit.register(lambda: sip.sendto(in_dialog("BYE", 2), PROXY))

    emit, drain, impairment = impairer(rtp, dest)
    log(f"call {call_id} tag {from_tag} ssrc {RTP_SSRC:08x} sip:{SIP_PORT} rtp:{RTP_PORT} "
        f"impair loss={IMPAIR_LOSS} reorder={IMPAIR_REORDER} "
        f"duplicate={IMPAIR_DUPLICATE} jitter={IMPAIR_JITTER_MS}ms")

    heard = {}
    seq, ts = 100, 0
    started = time.monotonic()
    next_talk = started + 2.0
    talking = None
    sent = 0
    while time.monotonic() - started < CALL_SECONDS:
        if talking is None and time.monotonic() >= next_talk:
            talking = iter(frames)
            next_talk = time.monotonic() + TALK_EVERY
            log("speaking the test sentence")
        if talking is not None:
            payload = next(talking, None)
            if payload is None:
                talking = None
                payload = bytes([ALAW_SILENCE]) * 160
        else:
            payload = bytes([ALAW_SILENCE]) * 160
        head = struct.pack("!BBHII", 0x80, 8, seq & 0xFFFF, ts, RTP_SSRC)
        emit(head + payload)
        seq += 1
        ts += 160
        sent += 1
        while True:
            try:
                datagram, _ = rtp.recvfrom(2048)
            except (BlockingIOError, OSError):
                break
            if len(datagram) <= 12:
                continue
            pt = datagram[1] & 0x7F
            rts, ssrc = struct.unpack("!II", datagram[4:12])
            heard.setdefault(ssrc, {"pt": {}, "packets": []})
            heard[ssrc]["pt"][pt] = heard[ssrc]["pt"].get(pt, 0) + 1
            if pt in (0, 8):
                heard[ssrc]["packets"].append((rts, pt, datagram[12:]))
        time.sleep(0.02)

    drain()
    log(f"paced {sent} rtp packets; on the wire {impairment['sent']}, "
        f"dropped {impairment['dropped']}, reordered {impairment['reordered']}, "
        f"duplicated {impairment['duplicated']}, delayed {impairment['delayed']}; "
        "hanging up")

    if not WRITE_EARS:
        for ssrc, info in heard.items():
            log(f"ssrc {ssrc:08x}: payload types {info['pt']}, "
                f"{len(info['packets'])} audio packets (ear wav not written)")
        return

    for ssrc, info in heard.items():
        packets = info["packets"]
        log(f"ssrc {ssrc:08x}: payload types {info['pt']}")
        if not packets:
            continue
        base = packets[0][0]
        span = max(rts - base + len(p) for rts, _, p in packets)
        pcm = [0] * span
        voiced = 0
        for rts, pt, payload in packets:
            at = rts - base
            for i, byte in enumerate(payload):
                if at + i < span:
                    sample = alaw_to_linear(byte) if pt == 8 else ulaw_to_linear(byte)
                    pcm[at + i] = sample
                    if abs(sample) > 200:
                        voiced += 1
        path = os.path.join(EAR_DIR, f"{EAR_PREFIX}_{ssrc:08x}.wav")
        with wave.open(path, "wb") as out:
            out.setnchannels(1)
            out.setsampwidth(2)
            out.setframerate(8000)
            out.writeframes(b"".join(struct.pack("<h", s) for s in pcm))
        log(f"  wrote {path}: {span / 8000:.2f}s, {len(packets)} packets, {voiced} voiced samples")


main()
