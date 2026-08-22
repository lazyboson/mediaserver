"""Fabricates a native Opus call through rtpengine and pumps a real 440 Hz tone.

call_driver.py's G.711 call can only produce Opus on the wire by asking
rtpengine to transcode to it, which is exactly the thing under suspicion in
tasks.md 16b-2: rtpengine emits ~10 packets/second when it transcodes to Opus
where the same call tapped as PCMU gives ~51. This driver removes rtpengine's
transcoder from the picture entirely -- both legs offer and answer
`opus/48000/2` at a dynamic payload type and encode with libopus themselves, so
rtpengine only relays. Tapped with MSS_TAP_TRANSCODE=off, MSS then decodes the
endpoints' own Opus and the measured packet rate is rtpengine's relay rate,
nothing else.

No SIP stack is involved: rtpengine accepts offer/answer directly, so this plays
both endpoints and the signalling proxy between them. libopus is reached through
ctypes, so the container needs `libopus0` and no pip wheel.
"""

import ctypes
import ctypes.util
import math
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
CALL_ID = os.environ.get("CALL_ID", "opus-native-1")
FROM_TAG = os.environ.get("FROM_TAG", "opusA")
TO_TAG = os.environ.get("TO_TAG", "opusB")
PUMP_SECONDS = float(os.environ.get("PUMP_SECONDS", "300"))
READY_FILE = os.environ.get("READY_FILE", "/tmp/call-ready")
OPUS_PAYLOAD_TYPE = int(os.environ.get("OPUS_PAYLOAD_TYPE", "111"))
TELEPHONE_EVENT_PT = int(os.environ.get("TELEPHONE_EVENT_PT", "101"))
PTIME_MS = int(os.environ.get("PTIME_MS", "20"))
TONE_HZ = float(os.environ.get("TONE_HZ", "440"))
TONE_AMPLITUDE = int(os.environ.get("TONE_AMPLITUDE", "12000"))
OPUS_BITRATE = int(os.environ.get("OPUS_BITRATE", "24000"))
RATE_REPORT_SECONDS = float(os.environ.get("RATE_REPORT_SECONDS", "10"))

CALLER_RTP_PORT = int(os.environ.get("CALLER_RTP_PORT", "40010"))
CALLEE_RTP_PORT = int(os.environ.get("CALLEE_RTP_PORT", "40012"))

OPUS_CLOCK_RATE_HZ = 48000
SAMPLES_PER_FRAME = OPUS_CLOCK_RATE_HZ * PTIME_MS // 1000
MAX_OPUS_PACKET = 1276

OPUS_APPLICATION_VOIP = 2048
OPUS_SET_BITRATE_REQUEST = 4002
OPUS_SET_VBR_REQUEST = 4006
OPUS_SET_DTX_REQUEST = 4016


def log(message):
    print(f"opus-call-driver: {message}", flush=True)


def load_libopus():
    candidates = ["libopus.so.0", "libopus.so"]
    found = ctypes.util.find_library("opus")
    if found:
        candidates.insert(0, found)
    for name in candidates:
        try:
            return ctypes.CDLL(name)
        except OSError:
            continue
    log("libopus is missing; install libopus0 in this container")
    sys.exit(1)


class OpusEncoder:
    def __init__(self, library):
        self.library = library
        library.opus_encoder_create.restype = ctypes.c_void_p
        library.opus_encoder_create.argtypes = [
            ctypes.c_int32, ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_int)
        ]
        library.opus_encode.restype = ctypes.c_int32
        library.opus_encode.argtypes = [
            ctypes.c_void_p, ctypes.POINTER(ctypes.c_int16), ctypes.c_int,
            ctypes.POINTER(ctypes.c_ubyte), ctypes.c_int32
        ]
        error = ctypes.c_int(0)
        self.state = library.opus_encoder_create(
            OPUS_CLOCK_RATE_HZ, 1, OPUS_APPLICATION_VOIP, ctypes.byref(error)
        )
        if not self.state or error.value != 0:
            log(f"opus_encoder_create failed with {error.value}")
            sys.exit(1)
        for request, value in (
            (OPUS_SET_BITRATE_REQUEST, OPUS_BITRATE),
            (OPUS_SET_VBR_REQUEST, 0),
            (OPUS_SET_DTX_REQUEST, 0),
        ):
            library.opus_encoder_ctl(
                ctypes.c_void_p(self.state), ctypes.c_int(request), ctypes.c_int32(value)
            )
        self.buffer = (ctypes.c_ubyte * MAX_OPUS_PACKET)()

    def encode(self, pcm):
        written = self.library.opus_encode(
            ctypes.c_void_p(self.state), pcm, SAMPLES_PER_FRAME,
            self.buffer, MAX_OPUS_PACKET
        )
        if written < 0:
            log(f"opus_encode failed with {written}")
            sys.exit(1)
        return bytes(self.buffer[:written])


def tone_frames(hertz):
    """One period-aligned cycle of frames, so the tone is continuous forever.

    A 440 Hz tone at 48 kHz repeats exactly every 1200 samples; over 20 ms
    frames of 960 samples the pattern closes after 5 frames (4800 samples,
    4 periods of 1200), which is why a fixed list of frames can be replayed
    with no phase discontinuity at the seam.
    """
    period_samples = OPUS_CLOCK_RATE_HZ / hertz
    frames_in_cycle = 1
    while frames_in_cycle < 4096:
        total = frames_in_cycle * SAMPLES_PER_FRAME
        if abs(total / period_samples - round(total / period_samples)) < 1e-9:
            break
        frames_in_cycle += 1
    frames = []
    for index in range(frames_in_cycle):
        pcm = (ctypes.c_int16 * SAMPLES_PER_FRAME)()
        for n in range(SAMPLES_PER_FRAME):
            sample = index * SAMPLES_PER_FRAME + n
            pcm[n] = int(TONE_AMPLITUDE * math.sin(2 * math.pi * hertz * sample
                                                   / OPUS_CLOCK_RATE_HZ))
        frames.append(pcm)
    log(f"built {frames_in_cycle} frames covering one whole-cycle tone period")
    return frames


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
        "s=lab-opus\r\n"
        f"c=IN IP4 {SELF_IP}\r\n"
        "t=0 0\r\n"
        f"m=audio {port} RTP/AVP {OPUS_PAYLOAD_TYPE} {TELEPHONE_EVENT_PT}\r\n"
        f"a=rtpmap:{OPUS_PAYLOAD_TYPE} opus/48000/2\r\n"
        f"a=fmtp:{OPUS_PAYLOAD_TYPE} minptime=10;useinbandfec=1;stereo=0;sprop-stereo=0\r\n"
        f"a=rtpmap:{TELEPHONE_EVENT_PT} telephone-event/8000\r\n"
        f"a=ptime:{PTIME_MS}\r\n"
        "a=sendrecv\r\n"
    )


class Ng:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(3)
        self.serial = 0

    def send(self, command):
        self.serial += 1
        cookie = f"opus-lab-{os.getpid()}-{self.serial}".encode()
        datagram = cookie + b" " + bencode(command)
        for attempt in range(1, 11):
            self.sock.sendto(datagram, (NODE, PORT))
            try:
                reply, _ = self.sock.recvfrom(65535)
            except socket.timeout:
                log(f"no reply to {command['command']!r} (attempt {attempt})")
                continue
            body = reply.split(b" ", 1)[1]
            if field(body, "result") == "error":
                raise RuntimeError(
                    f"{command['command']!r} failed: {field(body, 'error-reason')}"
                )
            return body
        raise RuntimeError(f"rtpengine never answered {command['command']!r}")


def media_port(sdp):
    ports = [int(port) for port in re.findall(r"m=audio (\d+) ", sdp)]
    return ports[0] if ports else None


def answered_payload_type(sdp):
    match = re.search(r"a=rtpmap:(\d+) opus/48000", sdp or "")
    return int(match.group(1)) if match else None


def pump(caller_dest, callee_dest, stop):
    library = load_libopus()
    frames = tone_frames(TONE_HZ)

    caller = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    caller.bind(("0.0.0.0", CALLER_RTP_PORT))
    callee = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    callee.bind(("0.0.0.0", CALLEE_RTP_PORT))
    caller.setblocking(False)
    callee.setblocking(False)

    legs = [
        {"name": "caller", "sock": caller, "dest": caller_dest, "seq": 1000,
         "ts": 0, "ssrc": 0x33333333, "encoder": OpusEncoder(library),
         "sent": 0, "bytes": 0, "heard": 0, "heard_bytes": 0, "heard_ssrcs": set()},
        {"name": "callee", "sock": callee, "dest": callee_dest, "seq": 9000,
         "ts": 0, "ssrc": 0x44444444, "encoder": OpusEncoder(library),
         "sent": 0, "bytes": 0, "heard": 0, "heard_bytes": 0, "heard_ssrcs": set()},
    ]

    ptime = PTIME_MS / 1000.0
    started = time.monotonic()
    deadline = started + PUMP_SECONDS
    next_report = started + RATE_REPORT_SECONDS
    tick = 0
    reanchors = 0

    while not stop.is_set() and time.monotonic() < deadline:
        for leg in legs:
            payload = leg["encoder"].encode(frames[tick % len(frames)])
            header = struct.pack("!BBHII", 0x80, OPUS_PAYLOAD_TYPE,
                                 leg["seq"] & 0xFFFF, leg["ts"], leg["ssrc"])
            leg["sock"].sendto(header + payload, leg["dest"])
            leg["seq"] += 1
            leg["ts"] += SAMPLES_PER_FRAME
            leg["sent"] += 1
            leg["bytes"] += len(payload)
            while True:
                try:
                    datagram, _ = leg["sock"].recvfrom(4096)
                except (BlockingIOError, OSError):
                    break
                if len(datagram) > 12:
                    leg["heard"] += 1
                    leg["heard_bytes"] += len(datagram) - 12
                    leg["heard_ssrcs"].add(struct.unpack("!I", datagram[8:12])[0])

        tick += 1
        now = time.monotonic()
        if now >= next_report:
            elapsed = now - started
            for leg in legs:
                log(f"{leg['name']}: sent {leg['sent']}"
                    f" ({leg['sent'] / elapsed:.1f}/s),"
                    f" heard {leg['heard']} ({leg['heard'] / elapsed:.1f}/s)"
                    f" from ssrcs {[format(s, '08x') for s in leg['heard_ssrcs']]}")
            next_report = now + RATE_REPORT_SECONDS

        target = started + tick * ptime
        sleep_for = target - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        elif sleep_for < -ptime:
            started = time.monotonic() - tick * ptime
            reanchors += 1

    elapsed = time.monotonic() - started
    log(f"pumped for {elapsed:.1f}s, {reanchors} pacer reanchors")
    for leg in legs:
        mean_payload = leg["bytes"] / leg["sent"] if leg["sent"] else 0
        log(f"{leg['name']} FINAL: sent {leg['sent']} packets"
            f" ({leg['sent'] / elapsed:.2f}/s), mean opus payload"
            f" {mean_payload:.1f} bytes; heard {leg['heard']} packets"
            f" ({leg['heard'] / elapsed:.2f}/s)")


def main():
    if os.path.exists(READY_FILE):
        os.remove(READY_FILE)
    if SAMPLES_PER_FRAME not in (120, 240, 480, 960, 1920, 2880):
        log(f"opus has no {PTIME_MS} ms frame at 48 kHz")
        sys.exit(1)

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
    offer_sdp = field(offer_reply, "sdp") or ""
    callee_dest_port = media_port(offer_sdp)
    log(f"offer accepted; callee sends to {NODE}:{callee_dest_port},"
        f" rtpengine kept opus at pt {answered_payload_type(offer_sdp)}")

    answer_reply = ng.send({
        "command": "answer",
        "call-id": CALL_ID,
        "from-tag": FROM_TAG,
        "to-tag": TO_TAG,
        "sdp": sdp_for(CALLEE_RTP_PORT),
    })
    answer_sdp = field(answer_reply, "sdp") or ""
    caller_dest_port = media_port(answer_sdp)
    log(f"answer accepted; caller sends to {NODE}:{caller_dest_port},"
        f" rtpengine kept opus at pt {answered_payload_type(answer_sdp)}")

    if not callee_dest_port or not caller_dest_port:
        log("rtpengine did not return usable media ports")
        sys.exit(1)
    if answered_payload_type(answer_sdp) != OPUS_PAYLOAD_TYPE:
        log("rtpengine did not keep the offered opus payload type;"
            " this call would not be a native-opus proof")
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
    log(f"native opus call is live and pumping; wrote {READY_FILE}")

    pumping.join()
    log("done")


main()
