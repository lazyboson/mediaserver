"""Plays the part of MicroSIP so the echo loop can be verified without a human.

Runs on the WSL host, not in a container, so it uses exactly the path a real
softphone uses: SIP to the published 127.0.0.1:5060, RTP to the published
30000-30020 range, PCMA like MicroSIP negotiates. It speaks a known sentence
(a wav of real speech), records everything it hears per SSRC, and hangs up.

Success is measurable afterwards: the bridge log should transcribe the spoken
sentence, and one of the recorded SSRCs should carry the TTS reply.
"""

import atexit
import os
import re
import socket
import struct
import time
import wave

PROXY = ("127.0.0.1", 5060)
SIP_PORT = 45070
RTP_PORT = 45072
SPEECH_WAV = os.environ.get("SPEECH_WAV", "out/bridge_tts.wav")
EAR_DIR = os.environ.get("EAR_DIR", "out")
CALL_SECONDS = float(os.environ.get("CALL_SECONDS", "40"))
TALK_EVERY = float(os.environ.get("TALK_EVERY", "9"))
DIAL = os.environ.get("DIAL", "9000")

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


def speech_frames():
    with wave.open(SPEECH_WAV) as w:
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


def main():
    frames = speech_frames()
    log(f"speech: {len(frames)} frames ({len(frames) * 0.02:.1f}s) from {SPEECH_WAV}")

    sip = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sip.bind(("0.0.0.0", SIP_PORT))
    sip.settimeout(10)
    rtp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rtp.bind(("0.0.0.0", RTP_PORT))
    rtp.setblocking(False)

    call_id = f"host-test-{int(time.time())}"
    from_tag = "hosttest"
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
        head = struct.pack("!BBHII", 0x80, 8, seq & 0xFFFF, ts, 0x77777777)
        rtp.sendto(head + payload, dest)
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

    log(f"sent {sent} rtp packets; hanging up")

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
        path = os.path.join(EAR_DIR, f"host_ear_{ssrc:08x}.wav")
        with wave.open(path, "wb") as out:
            out.setnchannels(1)
            out.setsampwidth(2)
            out.setframerate(8000)
            out.writeframes(b"".join(struct.pack("<h", s) for s in pcm))
        log(f"  wrote {path}: {span / 8000:.2f}s, {len(packets)} packets, {voiced} voiced samples")


main()
