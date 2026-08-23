"""A SIP-less RTP endpoint for an MSS INLINE leg: it offers, then talks and
listens.

The call_driver.py pattern with rtpengine removed. An inline leg is MSS's own
socket answering an SDP offer, so there is no relay in the path and no NG
command to send: this process writes its offer to a file, waits for the
answer file the drill produces with `mss_ctl inline`, and from then on is
simply the far end of one RTP flow.

  offer.sdp   written here, read by inline_call_drill.sh
  answer.sdp  written by the drill from mss_ctl's stdout, read here

What it records is the whole point of P3-4. Every datagram MSS sends is
appended to a jsonl with its ARRIVAL wall clock, its ssrc, sequence number,
rtp timestamp, and the energy of the injected tone in that one payload
(Goertzel over the packet, so a single 20 ms frame is classified on its own).
That timeline is what turns "Clear was sent at T" into a cut-through number:
the first packet after T whose tone energy has collapsed is the moment the
peer's ear went quiet.

It also writes one wav per ssrc on the rtp-timestamp timeline (the ear), so a
human or wav_summary.py can check what the leg actually heard.

In a conference (P4-6) one ear carries several tones at once, so the timeline
also records a Goertzel per tone named in EAR_TONES. That is the whole basis of
the conference assertions: "A's ear carries 880 and 1320 but not its own 440" is
three numbers on the same packet, and a phase window over the wall clock decides
which of them are supposed to be there.

Env: IO_DIR, PEER_IP, PEER_PORT, TONE_HZ (what we send), INJECT_HZ (what we
expect to hear), EAR_TONES (comma-separated Hz measured per packet),
RUN_SECONDS, PAYLOAD_TYPE, TONE_AMPLITUDE (a conference of four sources must
stay under full scale or the mixer clips and intermodulation lands on exactly
the harmonics the drill measures).
"""

import cmath
import json
import math
import os
import signal
import socket
import struct
import sys
import threading
import time
import wave

IO_DIR = os.environ.get("IO_DIR", "/io")
PEER_IP = os.environ.get("PEER_IP", "127.0.0.1")
PEER_PORT = int(os.environ.get("PEER_PORT", "41000"))
TONE_HZ = float(os.environ.get("TONE_HZ", "440"))
INJECT_HZ = float(os.environ.get("INJECT_HZ", "1000"))
RUN_SECONDS = float(os.environ.get("RUN_SECONDS", "300"))
PAYLOAD_TYPE = int(os.environ.get("PAYLOAD_TYPE", "0"))
ANSWER_TIMEOUT = float(os.environ.get("ANSWER_TIMEOUT", "60"))
TONE_AMPLITUDE = int(os.environ.get("TONE_AMPLITUDE", "24000"))
EAR_TONES = [
    float(hz) for hz in os.environ.get("EAR_TONES", "").split(",") if hz.strip()
]

SAMPLE_RATE = 8000
FRAME_SAMPLES = 160
PTIME = 0.02
SILENCE_BYTE = 0xFF


def log(message):
    print(f"inline-peer: {message}", flush=True)


def linear_to_ulaw(sample):
    sign = 0x80 if sample < 0 else 0
    if sample < 0:
        sample = -sample
    if sample > 32635:
        sample = 32635
    sample += 0x84
    exponent = 7
    mask = 0x4000
    while exponent > 0 and not sample & mask:
        mask >>= 1
        exponent -= 1
    mantissa = (sample >> (exponent + 3)) & 0x0F
    return ~(sign | (exponent << 4) | mantissa) & 0xFF


def ulaw_to_linear(byte):
    byte = ~byte & 0xFF
    sign = byte & 0x80
    exponent = (byte >> 4) & 0x07
    mantissa = byte & 0x0F
    sample = ((mantissa << 3) + 0x84) << exponent
    sample -= 0x84
    return -sample if sign else sample


def tone_frames(hz, count):
    """A whole number of frames of a continuous sine, phase carried across."""
    frames = []
    phase = 0
    for _ in range(count):
        samples = []
        for _ in range(FRAME_SAMPLES):
            value = int(
                TONE_AMPLITUDE * math.sin(2 * math.pi * hz * phase / SAMPLE_RATE)
            )
            samples.append(linear_to_ulaw(value))
            phase += 1
        frames.append(bytes(samples))
    return frames


def goertzel(samples, hz):
    """Magnitude of one frequency in one packet, normalised by packet energy.

    Absolute magnitude alone cannot separate "the tone stopped" from "the tone
    got quieter", and MSS's silence frame is digital silence, so the number
    reported is the plain magnitude per sample; the analyser thresholds it.
    """
    if not samples:
        return 0.0
    omega = 2 * math.pi * hz / SAMPLE_RATE
    coeff = 2 * math.cos(omega)
    s_prev = s_prev2 = 0.0
    for sample in samples:
        s = sample + coeff * s_prev - s_prev2
        s_prev2 = s_prev
        s_prev = s
    power = s_prev2 * s_prev2 + s_prev * s_prev - coeff * s_prev * s_prev2
    return math.sqrt(max(power, 0.0)) / len(samples)


def rms(samples):
    if not samples:
        return 0.0
    return math.sqrt(sum(s * s for s in samples) / len(samples))


def offer_sdp():
    return (
        "v=0\r\n"
        f"o=- 3 3 IN IP4 {PEER_IP}\r\n"
        "s=mss-inline-peer\r\n"
        f"c=IN IP4 {PEER_IP}\r\n"
        "t=0 0\r\n"
        f"m=audio {PEER_PORT} RTP/AVP 0 101\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=rtpmap:101 telephone-event/8000\r\n"
        "a=fmtp:101 0-15\r\n"
        "a=ptime:20\r\n"
        "a=sendrecv\r\n"
    )


def parse_answer(sdp):
    host = None
    port = None
    for line in sdp.replace("\r\n", "\n").split("\n"):
        if line.startswith("c=IN IP4 "):
            host = line[len("c=IN IP4 "):].strip()
        if line.startswith("m=audio "):
            port = int(line.split()[1])
    if not host or not port:
        raise SystemExit(f"the answer carries no address: {sdp!r}")
    return host, port


def receive(sock, heard, timeline, stop):
    while not stop.is_set():
        try:
            datagram, _ = sock.recvfrom(2048)
        except socket.timeout:
            continue
        except OSError:
            return
        at = time.time()
        if len(datagram) < 13:
            continue
        payload_type = datagram[1] & 0x7F
        seq = struct.unpack("!H", datagram[2:4])[0]
        timestamp, ssrc = struct.unpack("!II", datagram[4:12])
        payload = datagram[12:]
        if payload_type != PAYLOAD_TYPE:
            continue
        samples = [ulaw_to_linear(b) for b in payload]
        heard.setdefault(ssrc, []).append((timestamp, payload))
        timeline.write(json.dumps({
            "at": at,
            "ssrc": ssrc,
            "seq": seq,
            "ts": timestamp,
            "bytes": len(payload),
            "silent": all(b == SILENCE_BYTE for b in payload),
            "rms": round(rms(samples), 2),
            "tone": round(goertzel(samples, INJECT_HZ), 2),
            "tones": {
                f"{hz:g}": round(goertzel(samples, hz), 2) for hz in EAR_TONES
            },
        }) + "\n")
        timeline.flush()


def write_ears(heard):
    for ssrc, packets in sorted(heard.items()):
        base = packets[0][0]
        span = max(ts - base + len(payload) for ts, payload in packets)
        track = bytearray([SILENCE_BYTE]) * span
        for ts, payload in packets:
            at = ts - base
            if at < 0:
                continue
            track[at:at + len(payload)] = payload
        path = os.path.join(IO_DIR, f"peer_ear_{ssrc:08x}.wav")
        with wave.open(path, "wb") as out:
            out.setnchannels(1)
            out.setsampwidth(2)
            out.setframerate(SAMPLE_RATE)
            out.writeframes(
                b"".join(struct.pack("<h", ulaw_to_linear(b)) for b in track)
            )
        voiced = sum(1 for b in track if b != SILENCE_BYTE)
        log(f"wrote {path}: {len(packets)} packets, {span / SAMPLE_RATE:.2f}s, "
            f"{voiced} non-silent bytes")
    if not heard:
        log("the peer heard nothing at all")


def main():
    os.makedirs(IO_DIR, exist_ok=True)
    answer_path = os.path.join(IO_DIR, "answer.sdp")
    for stale in ("answer.sdp", "peer-timeline.jsonl"):
        path = os.path.join(IO_DIR, stale)
        if os.path.exists(path):
            os.remove(path)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", PEER_PORT))
    sock.settimeout(0.2)

    with open(os.path.join(IO_DIR, "offer.sdp"), "w") as out:
        out.write(offer_sdp())
    log(f"offered PCMU/8000 on {PEER_IP}:{PEER_PORT}; waiting for the answer")

    deadline = time.time() + ANSWER_TIMEOUT
    while time.time() < deadline and not os.path.exists(answer_path):
        time.sleep(0.1)
    if not os.path.exists(answer_path):
        raise SystemExit("no answer.sdp appeared")
    time.sleep(0.2)
    with open(answer_path) as source:
        answer = source.read()
    dest = parse_answer(answer)
    log(f"answered: MSS listens on {dest[0]}:{dest[1]}")

    heard = {}
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    timeline = open(os.path.join(IO_DIR, "peer-timeline.jsonl"), "w")
    ear = threading.Thread(
        target=receive, args=(sock, heard, timeline, stop), daemon=True
    )
    ear.start()

    with open(os.path.join(IO_DIR, "peer-ready"), "w") as ready:
        ready.write(f"{dest[0]}:{dest[1]}")

    frames = tone_frames(TONE_HZ, 50)
    seq = 5000
    timestamp = 0
    ssrc = 0x51DEB00C
    sent = 0
    started = time.monotonic()
    next_send = started
    while not stop.is_set() and time.monotonic() - started < RUN_SECONDS:
        payload = frames[sent % len(frames)]
        header = struct.pack(
            "!BBHII", 0x80, PAYLOAD_TYPE, seq & 0xFFFF, timestamp, ssrc
        )
        try:
            sock.sendto(header + payload, dest)
        except OSError as error:
            log(f"send failed: {error}")
        seq += 1
        timestamp += FRAME_SAMPLES
        sent += 1
        next_send += PTIME
        delay = next_send - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            next_send = time.monotonic()
    stop.set()
    ear.join(timeout=2)
    timeline.close()
    log(f"sent {sent} rtp packets of {TONE_HZ:g} Hz")
    write_ears(heard)
    log("done")


main()
