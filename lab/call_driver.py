"""Fabricates a real call through rtpengine over the NG protocol, then pumps
G.711 for both legs so mediaserverd has something to tap.

No SIP stack is involved: rtpengine accepts offer/answer directly, so this
plays the part of both endpoints and of the signalling proxy between them.
"""

import os
import re
import signal
import socket
import struct
import sys
import threading
import time

NODE = os.environ.get("NG_NODE", "127.0.0.1")
PORT = int(os.environ.get("NG_PORT", "22222"))
SELF_IP = os.environ.get("SELF_IP", "127.0.0.1")
CALL_ID = os.environ.get("CALL_ID", "lab-call-1")
FROM_TAG = os.environ.get("FROM_TAG", "tagA")
TO_TAG = os.environ.get("TO_TAG", "tagB")
PUMP_SECONDS = float(os.environ.get("PUMP_SECONDS", "60"))
READY_FILE = os.environ.get("READY_FILE", "/tmp/call-ready")
COOKIE_PREFIX = os.environ.get("COOKIE_PREFIX", "lab")

CALLER_RTP_PORT = 40000
CALLEE_RTP_PORT = 40002
PCMU_PAYLOAD_TYPE = 0
TELEPHONE_EVENT_PT = int(os.environ.get("TELEPHONE_EVENT_PT", "101"))
CALLER_DIGIT = int(os.environ.get("CALLER_DIGIT", "1"))
CALLEE_DIGIT = int(os.environ.get("CALLEE_DIGIT", "2"))
DIGIT_AFTER_SECONDS = float(os.environ.get("DIGIT_AFTER_SECONDS", "3"))
DIGIT_INTERVAL_SECONDS = float(os.environ.get("DIGIT_INTERVAL_SECONDS", "4"))
EAR_DIR = os.environ.get("EAR_DIR", "")
PUMP_SILENCE = os.environ.get("PUMP_SILENCE", "") not in ("", "0", "false")


def log(message):
    print(f"call-driver: {message}", flush=True)


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
        "s=lab\r\n"
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
        cookie = f"{COOKIE_PREFIX}-{self.serial}".encode()
        datagram = cookie + b" " + bencode(command)
        for attempt in range(1, 11):
            self.sock.sendto(datagram, (NODE, PORT))
            try:
                reply, _ = self.sock.recvfrom(65535)
            except socket.timeout:
                log(f"no reply to {command['command']!r} (attempt {attempt})")
                continue
            body = reply.split(b" ", 1)[1]
            result = field(body, "result")
            if result == "error":
                raise RuntimeError(
                    f"{command['command']!r} failed: {field(body, 'error-reason')}"
                )
            return body
        raise RuntimeError(f"rtpengine never answered {command['command']!r}")


def media_port(sdp):
    ports = [int(p) for p in re.findall(r"m=audio (\d+) ", sdp)]
    return ports[0] if ports else None


def send_rtp(leg, payload_type, payload, timestamp):
    header = struct.pack(
        "!BBHII", 0x80, payload_type, leg["seq"] & 0xFFFF, timestamp, leg["ssrc"]
    )
    leg["sock"].sendto(header + payload, leg["dest"])
    leg["seq"] += 1


def send_digit(leg, event):
    """One RFC 4733 press: repeats at a fixed timestamp, then end-bit retransmits.

    The detector must report the digit exactly once despite the retransmissions,
    which is the frozen firstDtmf/dtmfResult behaviour.
    """
    start = leg["ts"]
    duration = 160
    for _ in range(4):
        send_rtp(leg, TELEPHONE_EVENT_PT, struct.pack("!BBH", event, 10, duration), start)
        duration += 160
    for _ in range(3):
        send_rtp(leg, TELEPHONE_EVENT_PT, struct.pack("!BBH", event, 0x80 | 10, duration), start)
    leg["ts"] = start + duration
    log(f"sent digit {event} on {leg['name']}")


def ulaw_to_linear(byte):
    byte = ~byte & 0xFF
    sign = byte & 0x80
    exponent = (byte >> 4) & 0x07
    mantissa = byte & 0x0F
    sample = ((mantissa << 3) + 0x84) << exponent
    sample -= 0x84
    return -sample if sign else sample


def write_ear(leg):
    """What this endpoint HEARD, which is where injected bot speech shows up.

    A tap carries what a party sends, so audio played into the call for this
    party is only observable here, at its ear.

    Injected audio arrives as its OWN synchronisation source alongside the
    peer's, so payloads must be separated by ssrc and placed by rtp timestamp.
    Concatenating whatever turns up interleaves two streams into one buffer and
    turns intelligible speech into gibberish.
    """
    import wave

    name = leg["name"].split("/")[0]
    for ssrc, packets in sorted(leg["heard"].items()):
        base = packets[0][0]
        span = max(ts - base + len(payload) for ts, payload in packets)
        track = bytearray([0xFF]) * span
        for ts, payload in packets:
            at = ts - base
            track[at:at + len(payload)] = payload
        voiced = sum(1 for byte in track if byte != 0xFF)
        suffix = "" if len(leg["heard"]) == 1 else f"_{ssrc:08x}"
        path = os.path.join(EAR_DIR, f"{name}_ear{suffix}.wav")
        with wave.open(path, "wb") as out:
            out.setnchannels(1)
            out.setsampwidth(2)
            out.setframerate(8000)
            out.writeframes(b"".join(struct.pack("<h", ulaw_to_linear(b)) for b in track))
        log(f"wrote {path}: ssrc {ssrc:08x}, {len(packets)} packets, "
            f"{span / 8000:.2f}s, {voiced} non-silent bytes")
    if not leg["heard"]:
        log(f"{leg['name']} heard nothing at all")


def pump(caller_dest, callee_dest, stop):
    caller = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    caller.bind(("0.0.0.0", CALLER_RTP_PORT))
    callee = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    callee.bind(("0.0.0.0", CALLEE_RTP_PORT))
    caller.setblocking(False)
    callee.setblocking(False)

    legs = [
        {"name": "caller/tagA", "sock": caller, "dest": caller_dest, "seq": 1000,
         "ts": 0, "ssrc": 0x11111111, "byte": 0, "digit": CALLER_DIGIT,
         "heard": {}},
        {"name": "callee/tagB", "sock": callee, "dest": callee_dest, "seq": 9000,
         "ts": 0, "ssrc": 0x22222222, "byte": 128, "digit": CALLEE_DIGIT,
         "heard": {}},
    ]
    sent = 0
    next_digits_at = time.monotonic() + DIGIT_AFTER_SECONDS
    deadline = time.monotonic() + PUMP_SECONDS
    while not stop.is_set() and time.monotonic() < deadline:
        if time.monotonic() >= next_digits_at:
            for leg in legs:
                send_digit(leg, leg["digit"])
            next_digits_at = time.monotonic() + DIGIT_INTERVAL_SECONDS
        for leg in legs:
            if PUMP_SILENCE:
                payload = bytes([0xFF]) * 160
            else:
                payload = bytes((leg["byte"] + n) % 256 for n in range(160))
            leg["byte"] = (leg["byte"] + 160) % 256
            send_rtp(leg, PCMU_PAYLOAD_TYPE, payload, leg["ts"])
            leg["ts"] += 160
            sent += 1
            while True:
                try:
                    datagram, _ = leg["sock"].recvfrom(2048)
                except (BlockingIOError, OSError):
                    break
                if len(datagram) > 12 and datagram[1] & 0x7F == PCMU_PAYLOAD_TYPE:
                    timestamp, ssrc = struct.unpack("!II", datagram[4:12])
                    leg["heard"].setdefault(ssrc, []).append((timestamp, datagram[12:]))
        time.sleep(0.02)
    log(f"pumped {sent} rtp packets")
    if EAR_DIR:
        for leg in legs:
            write_ear(leg)


def main():
    if os.path.exists(READY_FILE):
        os.remove(READY_FILE)

    ng = Ng()
    log(f"pinging rtpengine at {NODE}:{PORT}")
    ng.send({"command": "ping"})
    log("rtpengine answered ping")

    offer_reply = ng.send({
        "command": "offer",
        "call-id": CALL_ID,
        "from-tag": FROM_TAG,
        "sdp": sdp_for(CALLER_RTP_PORT),
    })
    callee_dest_port = media_port(field(offer_reply, "sdp") or "")
    log(f"offer accepted; callee sends to {NODE}:{callee_dest_port}")

    answer_reply = ng.send({
        "command": "answer",
        "call-id": CALL_ID,
        "from-tag": FROM_TAG,
        "to-tag": TO_TAG,
        "sdp": sdp_for(CALLEE_RTP_PORT),
    })
    caller_dest_port = media_port(field(answer_reply, "sdp") or "")
    log(f"answer accepted; caller sends to {NODE}:{caller_dest_port}")

    if not callee_dest_port or not caller_dest_port:
        log("rtpengine did not return usable media ports")
        sys.exit(1)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    pumping = threading.Thread(
        target=pump,
        args=((NODE, caller_dest_port), (NODE, callee_dest_port), stop),
        daemon=True,
    )
    pumping.start()
    time.sleep(1)

    with open(READY_FILE, "w") as ready:
        ready.write(CALL_ID)
    log(f"call is live and pumping; wrote {READY_FILE}")

    pumping.join()
    log("done")


main()
