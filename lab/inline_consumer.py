"""The voice-AI half of the inline drill: a gRPC consumer that speaks into an
INLINE leg and then barges its own speech away.

It plays the role P3-3 opened up. It attaches itself with CAPABILITY_INJECT
(mss_ctl's `consume` asks for SINK+EVENTS only, and this is the one lab actor
that needs to put audio INTO a session), subscribes on MediaStream, and then:

  * streams a continuous tone as ConsumerToServer.inject frames, paced in real
    time with a deliberate LEAD_MS of audio kept in the egress queue -- the
    barge number is meaningless if there is nothing queued to throw away;
  * sends a Mark per priming round and waits for the ServerToConsumer.mark
    ack, which is the drain barrier P3-3 added -- the ack must arrive about one
    LEAD_MS after the Mark, since that is how much audio was still queued;
  * then runs ITERATIONS rounds of "talk for TONE_SECONDS, send Clear",
    stamping the wall clock at which each Clear left this process;
  * meanwhile counts what it HEARS (it holds SINK too), so the drill can
    assert the hub still taps the peer while the leg is being spoken to.

Every stamp goes to consumer-timeline.jsonl in the shared io dir, on the same
clock as inline_peer.py's arrival stamps -- both run as containers on the lab
network, so they read one kernel's clock and no skew estimate is needed.

Env: CONTROL, EXTERNAL_ID, IO_DIR, INJECT_HZ, PEER_HZ, ITERATIONS,
TONE_SECONDS, GAP_SECONDS, JITTER_MS, MARK_ROUNDS, LEAD_MS, STUBS,
MSS_AUTH_TOKEN.
"""

import json
import math
import os
import queue
import random
import struct
import sys
import threading
import time

STUBS = os.environ.get("STUBS", "/pb")
sys.path.insert(0, STUBS)

import grpc  # noqa: E402

import mediacontrol_pb2 as pb  # noqa: E402
import mediacontrol_pb2_grpc as pb_grpc  # noqa: E402
import mediastream_pb2 as sb  # noqa: E402
import mediastream_pb2_grpc as sb_grpc  # noqa: E402

CONTROL = os.environ.get("CONTROL", "172.31.99.31:50551")
EXTERNAL_ID = os.environ.get("EXTERNAL_ID", "inline-drill")
IO_DIR = os.environ.get("IO_DIR", "/io")
INJECT_HZ = float(os.environ.get("INJECT_HZ", "1000"))
PEER_HZ = float(os.environ.get("PEER_HZ", "440"))
ITERATIONS = int(os.environ.get("ITERATIONS", "20"))
TONE_SECONDS = float(os.environ.get("TONE_SECONDS", "1.5"))
GAP_SECONDS = float(os.environ.get("GAP_SECONDS", "1.0"))
# The egress emits on a 20 ms grid, so a barge period that is a whole number of
# frames (2.5 s is exactly 125 of them) samples one phase of that grid over and
# over and reports a spuriously tight distribution. Jittering the gap by up to
# one frame spreads the Clear uniformly across the grid, which is what makes the
# p95 mean anything.
JITTER_MS = float(os.environ.get("JITTER_MS", "20"))
MARK_ROUNDS = int(os.environ.get("MARK_ROUNDS", "3"))
LEAD_MS = float(os.environ.get("LEAD_MS", "400"))
TOKEN = os.environ.get("MSS_AUTH_TOKEN", "")
TRACK = os.environ.get("TRACK", "customer")

FRAME_MS = 20


def log(message):
    print(f"inline-consumer: {message}", flush=True)


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
    sign = 0x55 ^ 0xD5
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
    del sign
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
    """One AudioFormat, turned into the two things the drill needs from it:
    frames of a tone to send, and linear samples out of what arrived."""

    def __init__(self, fmt):
        self.encoding = fmt.encoding
        self.rate = fmt.sample_rate_hz or 8000
        self.ptime = fmt.ptime_ms or FRAME_MS
        self.samples = int(self.rate * self.ptime / 1000)
        if self.encoding == pb.ENCODING_PCMU:
            self.name = "pcmu"
        elif self.encoding == pb.ENCODING_PCMA:
            self.name = "pcma"
        elif self.encoding == pb.ENCODING_L16:
            self.name = "l16"
        else:
            raise SystemExit(f"the attachment format is not injectable: {fmt}")

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
                    int(24000 * math.sin(2 * math.pi * hz * phase / self.rate))
                )
                phase += 1
            out.append(self.encode(linear))
        return out


class Consumer:
    def __init__(self, attachment):
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
        self.tapped_energy = 0.0
        self.tapped_rms = 0.0
        self.marks = queue.Queue()
        self.codec = None
        threading.Thread(target=self._drain, daemon=True).start()
        if not self.started.wait(timeout=20):
            raise SystemExit(f"no StreamStart arrived for {attachment}")
        if self.failure:
            raise SystemExit(f"the stream failed at once: {self.failure}")
        self.codec = Codec(self.format)
        log(f"attached; format {self.codec.name}/{self.codec.rate}Hz/"
            f"{self.codec.ptime}ms, {self.codec.samples} samples per frame")

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
                        linear = self.codec.decode(message.frame.payload)
                        self.tapped_energy = max(
                            self.tapped_energy,
                            goertzel(linear, PEER_HZ, self.codec.rate),
                        )
                        loudness = math.sqrt(
                            sum(s * s for s in linear) / max(len(linear), 1)
                        )
                        self.tapped_rms = max(self.tapped_rms, loudness)
                elif which == "mark":
                    self.marks.put((message.mark.name, time.time()))
                elif which == "stop":
                    self.failure = f"server stopped the stream: {message.stop.reason}"
                    return
        except grpc.RpcError as error:
            self.failure = f"{error.code()}: {error.details()}"
        finally:
            self.started.set()

    def check(self):
        if self.failure:
            raise SystemExit(f"the consumer stream is gone -- {self.failure}")

    def inject(self, payload, seq):
        self.outbound.put(
            sb.ConsumerToServer(
                inject=sb.AudioFrame(track=TRACK, seq=seq, payload=payload)
            )
        )

    def mark(self, name):
        self.outbound.put(sb.ConsumerToServer(mark=sb.Mark(name=name)))

    def clear(self):
        self.outbound.put(sb.ConsumerToServer(clear=sb.Clear()))

    def close(self):
        self.outbound.put(None)
        self.channel.close()


def attach(stub, metadata):
    request = pb.AttachRequest(
        session=pb.SessionRef(external_id=EXTERNAL_ID),
        transport=pb.TRANSPORT_GRPC_STREAM,
        capabilities=[pb.CAPABILITY_SINK, pb.CAPABILITY_INJECT],
        selector=pb.TrackSelector(only=TRACK),
        label="inline-voice-ai",
    )
    return stub.Attach(request, metadata=metadata).attachment_id


def main():
    metadata = [("authorization", f"Bearer {TOKEN}")] if TOKEN else []
    channel = grpc.insecure_channel(CONTROL)
    stub = pb_grpc.MediaControlStub(channel)
    attachment = attach(stub, metadata)
    log(f"attached {attachment} with SINK+INJECT on {EXTERNAL_ID}")

    consumer = Consumer(attachment)
    codec = consumer.codec
    frame_seconds = codec.ptime / 1000.0
    lead = LEAD_MS / 1000.0
    tone = codec.tone(INJECT_HZ, 50)

    rows = []
    seq = 0

    def talk(seconds, until_clear=False):
        """Feed the egress in real time, keeping `lead` seconds queued ahead."""
        nonlocal seq
        started = time.monotonic()
        sent = 0
        while time.monotonic() - started < seconds:
            consumer.check()
            ahead = sent * frame_seconds - (time.monotonic() - started)
            if ahead > lead:
                time.sleep(min(ahead - lead, 0.05))
                continue
            consumer.inject(tone[sent % len(tone)], seq)
            seq += 1
            sent += 1
        del until_clear
        return sent

    log(f"priming the leg and testing the mark drain barrier, {MARK_ROUNDS} rounds")
    for round_index in range(MARK_ROUNDS):
        primed = talk(1.0)
        mark_sent = time.time()
        name = f"prime-{round_index}"
        consumer.mark(name)
        acked = None
        try:
            acked_name, acked = consumer.marks.get(timeout=15)
        except queue.Empty:
            log(f"mark {name!r} was never acked")
            acked_name = None
        if acked:
            log(f"mark {acked_name!r} acked {1000 * (acked - mark_sent):.0f} ms after "
                f"it was sent ({primed} frames injected, lead {LEAD_MS:.0f} ms)")
        rows.append({
            "kind": "mark",
            "name": name,
            "sent_at": mark_sent,
            "acked_at": acked,
            "ack_ms": 1000 * (acked - mark_sent) if acked else None,
            "queued_ms": primed * codec.ptime,
            "lead_ms": LEAD_MS,
        })
        time.sleep(0.3)

    log(f"running {ITERATIONS} barge iterations")
    for iteration in range(ITERATIONS):
        started = time.time()
        talk(TONE_SECONDS)
        consumer.check()
        clear_at = time.time()
        consumer.clear()
        rows.append({
            "kind": "barge",
            "iteration": iteration,
            "tone_from": started,
            "clear_at": clear_at,
            "lead_ms": LEAD_MS,
        })
        log(f"iteration {iteration}: cleared at {clear_at:.6f}")
        time.sleep(GAP_SECONDS + random.uniform(0, JITTER_MS / 1000.0))

    consumer.check()
    rows.append({
        "kind": "tap",
        "frames": consumer.frames,
        "peak_rms": round(consumer.tapped_rms, 2),
        "peak_tone": round(consumer.tapped_energy, 2),
        "tone_hz": PEER_HZ,
    })
    log(f"heard {consumer.frames} tapped frames from the peer; peak rms "
        f"{consumer.tapped_rms:.0f}, peak {PEER_HZ:g} Hz energy "
        f"{consumer.tapped_energy:.0f}")

    path = os.path.join(IO_DIR, "consumer-timeline.jsonl")
    with open(path, "w") as out:
        for row in rows:
            out.write(json.dumps(row) + "\n")
    log(f"wrote {path}")
    consumer.close()
    log("done")


main()
