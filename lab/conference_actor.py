"""The two gRPC actors the conference drill needs, in one file.

ROLE=monitor is the supervisor's ear: it attaches with SINK and a selector of
only=mixed, which P4-3 settled as the generic monitor verb -- the conference's
full sum is already published on every member's hub as Track::Mixed, so a
monitor needs no leg and no new noun. It writes one row per delivered frame
(wall clock, rms, a Goertzel per tone in EAR_TONES) and one wav of everything
it heard, in delivery order.

ROLE=injector is the supervisor's mouth. It attaches with SINK+INJECT and
metadata mix_target=<member external id>, which routes its audio into exactly
one listener's mix (a whisper). After WHISPER_SECONDS it calls UpdateAttachment
on ITSELF with mix_target=all -- the barge flip -- and keeps speaking for
BARGE_SECONDS. Both phase boundaries are stamped into injector-phases.jsonl on
the same clock the peers stamp their arrivals with, which is what lets the
report say "1760 Hz was in B's ear and in nobody else's during the whisper
window".

Both roles run as containers on the lab network so every stamp in the drill
comes from one kernel's clock.

Env: ROLE, CONTROL, EXTERNAL_ID, IO_DIR, NAME, STUBS, MSS_AUTH_TOKEN,
     TRACK, EAR_TONES, RUN_SECONDS,
     INJECT_HZ, MIX_TARGET, MIX_MONITOR, WHISPER_SECONDS, BARGE_SECONDS,
     LEAD_MS, TONE_AMPLITUDE.
"""

import json
import math
import os
import queue
import struct
import sys
import threading
import time
import wave

STUBS = os.environ.get("STUBS", "/pb")
sys.path.insert(0, STUBS)

import grpc  # noqa: E402

import mediacontrol_pb2 as pb  # noqa: E402
import mediacontrol_pb2_grpc as pb_grpc  # noqa: E402
import mediastream_pb2 as sb  # noqa: E402
import mediastream_pb2_grpc as sb_grpc  # noqa: E402

ROLE = os.environ.get("ROLE", "monitor")
CONTROL = os.environ.get("CONTROL", "172.31.99.31:50551")
EXTERNAL_ID = os.environ.get("EXTERNAL_ID", "conf-a")
IO_DIR = os.environ.get("IO_DIR", "/io")
NAME = os.environ.get("NAME", ROLE)
TOKEN = os.environ.get("MSS_AUTH_TOKEN", "")
TRACK = os.environ.get("TRACK", "mixed")
EAR_TONES = [
    float(hz) for hz in os.environ.get("EAR_TONES", "440,880,1320,1760").split(",")
    if hz.strip()
]
RUN_SECONDS = float(os.environ.get("RUN_SECONDS", "60"))
INJECT_HZ = float(os.environ.get("INJECT_HZ", "1760"))
MIX_TARGET = os.environ.get("MIX_TARGET", "own")
MIX_MONITOR = os.environ.get("MIX_MONITOR", "include")
WHISPER_SECONDS = float(os.environ.get("WHISPER_SECONDS", "8"))
BARGE_SECONDS = float(os.environ.get("BARGE_SECONDS", "8"))
LEAD_MS = float(os.environ.get("LEAD_MS", "200"))
TONE_AMPLITUDE = int(os.environ.get("TONE_AMPLITUDE", "6000"))

FRAME_MS = 20


def log(message):
    print(f"conf-{NAME}: {message}", flush=True)


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


def linear_to_alaw(sample):
    if sample >= 0:
        mask = 0xD5
    else:
        mask = 0x55
        sample = -sample - 1
    if sample > 32767:
        sample = 32767
    if sample >= 256:
        exponent = 7
        while exponent > 0 and not sample & (1 << (exponent + 7)):
            exponent -= 1
        mantissa = (sample >> (exponent + 3)) & 0x0F
        byte = (exponent << 4) | mantissa
    else:
        byte = sample >> 4
    return byte ^ mask


def ulaw_to_linear(byte):
    byte = ~byte & 0xFF
    sign = byte & 0x80
    exponent = (byte >> 4) & 0x07
    mantissa = byte & 0x0F
    sample = ((mantissa << 3) + 0x84) << exponent
    sample -= 0x84
    return -sample if sign else sample


def alaw_to_linear(byte):
    byte ^= 0x55
    sign = byte & 0x80
    exponent = (byte & 0x70) >> 4
    mantissa = byte & 0x0F
    sample = (mantissa << 4) + 8
    if exponent:
        sample = (sample + 256) << (exponent - 1)
    return -sample if sign else sample


def goertzel(samples, hz, rate):
    if not samples:
        return 0.0
    omega = 2 * math.pi * hz / rate
    coeff = 2 * math.cos(omega)
    s_prev = s_prev2 = 0.0
    for sample in samples:
        s = sample + coeff * s_prev - s_prev2
        s_prev2 = s_prev
        s_prev = s
    power = s_prev2 * s_prev2 + s_prev * s_prev - coeff * s_prev * s_prev2
    return math.sqrt(max(power, 0.0)) / len(samples)


class Codec:
    def __init__(self, fmt):
        self.rate = fmt.sample_rate_hz or 8000
        self.ptime = fmt.ptime_ms or FRAME_MS
        self.samples = int(self.rate * self.ptime / 1000)
        if fmt.encoding == pb.ENCODING_PCMU:
            self.name = "pcmu"
        elif fmt.encoding == pb.ENCODING_PCMA:
            self.name = "pcma"
        elif fmt.encoding == pb.ENCODING_L16:
            self.name = "l16"
        else:
            raise SystemExit(f"the attachment format is not usable here: {fmt}")

    def encode(self, linear):
        if self.name == "pcmu":
            return bytes(linear_to_ulaw(s) for s in linear)
        if self.name == "pcma":
            return bytes(linear_to_alaw(s) for s in linear)
        return b"".join(struct.pack("<h", s) for s in linear)

    def decode(self, payload):
        if self.name == "pcmu":
            return [ulaw_to_linear(b) for b in payload]
        if self.name == "pcma":
            return [alaw_to_linear(b) for b in payload]
        count = len(payload) // 2
        return list(struct.unpack("<%dh" % count, payload[:count * 2]))

    def tone(self, hz, frames):
        out = []
        phase = 0
        for _ in range(frames):
            linear = []
            for _ in range(self.samples):
                linear.append(
                    int(
                        TONE_AMPLITUDE
                        * math.sin(2 * math.pi * hz * phase / self.rate)
                    )
                )
                phase += 1
            out.append(self.encode(linear))
        return out


class Stream:
    def __init__(self, attachment, timeline):
        self.outbound = queue.Queue()
        self.channel = grpc.insecure_channel(CONTROL)
        stub = sb_grpc.MediaStreamStub(self.channel)
        self.responses = stub.Subscribe(self._requests())
        self.outbound.put(
            sb.ConsumerToServer(
                hello=sb.ConsumerHello(attachment_id=attachment, token=TOKEN)
            )
        )
        self.started = threading.Event()
        self.format = None
        self.failure = None
        self.frames = 0
        self.codec = None
        self.timeline = timeline
        self.heard = bytearray()
        threading.Thread(target=self._drain, daemon=True).start()
        if not self.started.wait(timeout=20):
            raise SystemExit(f"no StreamStart arrived for {attachment}")
        if self.failure:
            raise SystemExit(f"the stream failed at once: {self.failure}")
        self.codec = Codec(self.format)
        log(f"attached; format {self.codec.name}/{self.codec.rate}Hz/"
            f"{self.codec.ptime}ms, track {TRACK}")

    def _requests(self):
        while True:
            message = self.outbound.get()
            if message is None:
                return
            yield message

    def _drain(self):
        try:
            for message in self.responses:
                which = message.WhichOneof("msg")
                if which == "start":
                    self.format = message.start.format
                    self.started.set()
                elif which == "frame":
                    self.frames += 1
                    if self.codec:
                        self._observe(message.frame)
                elif which == "stop":
                    self.failure = (
                        f"server stopped the stream: {message.stop.reason}"
                    )
                    return
        except grpc.RpcError as error:
            self.failure = f"{error.code()}: {error.details()}"
        finally:
            self.started.set()

    def _observe(self, frame):
        linear = self.codec.decode(frame.payload)
        if not linear:
            return
        self.heard.extend(frame.payload)
        loudness = math.sqrt(sum(s * s for s in linear) / len(linear))
        self.timeline.write(json.dumps({
            "at": time.time(),
            "seq": frame.seq,
            "track": frame.track,
            "rms": round(loudness, 2),
            "tones": {
                f"{hz:g}": round(goertzel(linear, hz, self.codec.rate), 2)
                for hz in EAR_TONES
            },
        }) + "\n")

    def check(self):
        if self.failure:
            raise SystemExit(f"the stream is gone -- {self.failure}")

    def inject(self, payload, seq):
        self.outbound.put(
            sb.ConsumerToServer(
                inject=sb.AudioFrame(track="customer", seq=seq, payload=payload)
            )
        )

    def write_wav(self, path):
        with wave.open(path, "wb") as out:
            out.setnchannels(1)
            out.setsampwidth(2)
            out.setframerate(self.codec.rate)
            out.writeframes(
                b"".join(
                    struct.pack("<h", s) for s in self.codec.decode(bytes(self.heard))
                )
            )
        log(f"wrote {path}: {self.frames} frames")

    def close(self):
        self.outbound.put(None)
        self.channel.close()


def attach(stub, metadata):
    capabilities = [pb.CAPABILITY_SINK]
    request_metadata = {}
    if ROLE == "injector":
        capabilities.append(pb.CAPABILITY_INJECT)
        request_metadata = {"mix_target": MIX_TARGET, "mix_monitor": MIX_MONITOR}
    request = pb.AttachRequest(
        session=pb.SessionRef(external_id=EXTERNAL_ID),
        transport=pb.TRANSPORT_GRPC_STREAM,
        capabilities=capabilities,
        selector=pb.TrackSelector(only=TRACK),
        label=NAME,
        metadata=request_metadata,
    )
    return stub.Attach(request, metadata=metadata).attachment_id


def run_monitor(stream, stamps):
    deadline = time.monotonic() + RUN_SECONDS
    while time.monotonic() < deadline:
        stream.check()
        if os.path.exists(os.path.join(IO_DIR, "stop-monitor")):
            break
        time.sleep(0.2)
    stamps.append({"phase": "monitor", "frames": stream.frames})
    log(f"heard {stream.frames} frames on track {TRACK}")


def run_injector(stream, stamps, stub, metadata, attachment):
    codec = stream.codec
    frame_seconds = codec.ptime / 1000.0
    lead = LEAD_MS / 1000.0
    tone = codec.tone(INJECT_HZ, 50)
    seq = 0

    def talk(seconds):
        nonlocal seq
        started = time.monotonic()
        sent = 0
        while time.monotonic() - started < seconds:
            stream.check()
            ahead = sent * frame_seconds - (time.monotonic() - started)
            if ahead > lead:
                time.sleep(min(ahead - lead, 0.05))
                continue
            stream.inject(tone[sent % len(tone)], seq)
            seq += 1
            sent += 1
        return sent

    log(f"whispering {INJECT_HZ:g} Hz at {MIX_TARGET} for {WHISPER_SECONDS:g}s")
    opened = time.time()
    sent = talk(WHISPER_SECONDS)
    stamps.append({
        "phase": "whisper",
        "target": MIX_TARGET,
        "from": opened + lead,
        "to": time.time(),
        "frames": sent,
    })

    log("flipping the route to all -- the barge")
    stub.UpdateAttachment(
        pb.UpdateAttachmentRequest(
            attachment_id=attachment, metadata={"mix_target": "all"}
        ),
        metadata=metadata,
    )
    flipped = time.time()
    sent = talk(BARGE_SECONDS)
    stamps.append({
        "phase": "barge",
        "target": "all",
        "from": flipped + lead,
        "to": time.time(),
        "frames": sent,
    })
    log("detaching so the tone leaves every ear")
    stub.Detach(pb.AttachmentRef(attachment_id=attachment), metadata=metadata)


def main():
    os.makedirs(IO_DIR, exist_ok=True)
    metadata = [("authorization", f"Bearer {TOKEN}")] if TOKEN else []
    channel = grpc.insecure_channel(CONTROL)
    stub = pb_grpc.MediaControlStub(channel)
    attachment = attach(stub, metadata)
    log(f"attached {attachment} on {EXTERNAL_ID} as {ROLE}")
    with open(os.path.join(IO_DIR, f"{NAME}-attachment"), "w") as out:
        out.write(attachment)

    stamps = []
    path = os.path.join(IO_DIR, f"{NAME}-timeline.jsonl")
    with open(path, "w") as timeline:
        stream = Stream(attachment, timeline)
        try:
            if ROLE == "injector":
                run_injector(stream, stamps, stub, metadata, attachment)
            else:
                run_monitor(stream, stamps)
        finally:
            stream.close()
    if stream.heard:
        stream.write_wav(os.path.join(IO_DIR, f"{NAME}-ear.wav"))
    with open(os.path.join(IO_DIR, f"{NAME}-phases.jsonl"), "w") as out:
        for stamp in stamps:
            out.write(json.dumps(stamp) + "\n")
    log("done")


main()
