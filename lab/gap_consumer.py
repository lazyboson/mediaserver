"""A WS_TWILIO consumer built to measure an outage, not to sound nice.

Item 11 (the pod-kill re-subscribe drill) needs one number: how long is a
consumer starved of audio when the pod that owns its session is killed and
another pod adopts the session? Every other consumer in this lab answers
"how did it sound"; this one answers "when did frames stop and when did they
start again".

Why a WS consumer and not mss_stream_probe: with WS_TWILIO the *pod dials
the consumer*, so when pod B adopts the session it re-dials this same
endpoint and the artifact spans the outage. A gRPC consumer dials the pod,
so it would have to discover pod B and its new attachment id by itself.

What it records:

  * every media frame's arrival wall clock, per track, appended to a jsonl
    so a run can be re-analysed without re-running it;
  * every connection open/close, so the reconnect after adoption is visible;
  * one wav per track on an *arrival* timeline -- each frame's samples are
    placed at round((arrival - t0) * 8000) and anything no frame covered is
    left as digital silence. The outage is therefore a run of samples no
    frame ever covered, and its length is the answer. Reported both ways:
    longest uncovered run, and longest gap between consecutive arrivals.

Run it on the WSL host (so it survives the containers) and give the pods an
endpoint that reaches the host from the lab network. Under Docker Desktop on
WSL2 that address is host.docker.internal (192.168.65.254): the lab bridge
gateway 172.31.99.1 belongs to the Docker VM, not to the WSL distro, and the
WSL eth0 address is not routable from the lab network at all.

  python3 lab/gap_consumer.py     # then ws://host.docker.internal:8095/ws

It writes on SIGINT/SIGTERM, which is how pod_kill_drill.sh ends it.

For the item-19 soak suite the same instrument has to serve N concurrent calls
for an hour, which the item-11 defaults cannot do: everything is keyed by track
name (so N calls would merge into one timeline) and every frame's audio is kept
in memory (so an hour of three tracks is hundreds of megabytes). Two env knobs
switch that, both defaulting to the item-11 behaviour:

  GAP_BY_CALL=1     key the report by "<callSid>/<track>" instead of "<track>",
                    so each call gets its own continuity number
  GAP_KEEP_AUDIO=0  keep only arrival stamps, no payloads and no wavs -- the
                    longest-arrival-gap survives, the longest-silent-run cannot
  GAP_JOURNAL=0     do not write the per-frame jsonl (50 lines/s/track)

Env: GAP_HOST, GAP_PORT, OUT_DIR, GAP_STAMP, GAP_RATE, GAP_BY_CALL,
GAP_KEEP_AUDIO, GAP_JOURNAL.
"""

import base64
import hashlib
import json
import os
import signal
import socket
import struct
import sys
import threading
import time
import wave

HOST = os.environ.get("GAP_HOST", "0.0.0.0")
PORT = int(os.environ.get("GAP_PORT", "8095"))
OUT_DIR = os.environ.get("OUT_DIR", os.path.join(os.path.dirname(__file__), "out"))
STAMP = os.environ.get("GAP_STAMP", str(int(time.time())))
RATE = int(os.environ.get("GAP_RATE", "8000"))
BY_CALL = os.environ.get("GAP_BY_CALL", "0") not in ("0", "no", "false")
KEEP_AUDIO = os.environ.get("GAP_KEEP_AUDIO", "1") not in ("0", "no", "false")
JOURNALLING = os.environ.get("GAP_JOURNAL", "1") not in ("0", "no", "false")
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

STATE_LOCK = threading.Lock()
ARRIVALS = {}
CONNECTIONS = []
JOURNAL = None


def log(message):
    print(f"gap-consumer: {time.strftime('%H:%M:%S')} {message}", flush=True)


def ulaw_to_linear(byte):
    byte = ~byte & 0xFF
    sign = byte & 0x80
    exponent = (byte >> 4) & 0x07
    mantissa = byte & 0x0F
    sample = ((mantissa << 3) + 0x84) << exponent
    sample -= 0x84
    return -sample if sign else sample


ULAW_TABLE = [ulaw_to_linear(b) for b in range(256)]


def handshake(conn):
    request = b""
    while b"\r\n\r\n" not in request:
        chunk = conn.recv(4096)
        if not chunk:
            return False
        request += chunk
    key = None
    for line in request.decode("latin-1").split("\r\n"):
        if line.lower().startswith("sec-websocket-key:"):
            key = line.split(":", 1)[1].strip()
    if not key:
        return False
    accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
    conn.sendall(
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
        b"Connection: Upgrade\r\nSec-WebSocket-Accept: " + accept.encode() + b"\r\n\r\n"
    )
    return True


def recv_exactly(conn, count):
    buffer = b""
    while len(buffer) < count:
        chunk = conn.recv(count - len(buffer))
        if not chunk:
            return None
        buffer += chunk
    return buffer


def read_frame(conn):
    header = recv_exactly(conn, 2)
    if not header:
        return None, None
    opcode = header[0] & 0x0F
    masked = header[1] & 0x80
    length = header[1] & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exactly(conn, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exactly(conn, 8))[0]
    mask = recv_exactly(conn, 4) if masked else b""
    payload = recv_exactly(conn, length) if length else b""
    if payload is None:
        return None, None
    if masked:
        payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
    return opcode, payload


def note(record):
    if JOURNAL is None:
        return
    JOURNAL.write(json.dumps(record) + "\n")
    JOURNAL.flush()


def note_media(record):
    """Per-frame journalling is 50 lines/s/track, which an hour-long soak does
    not want. Connection and lifecycle notes stay on regardless."""
    if JOURNALLING:
        note(record)


def serve_one(conn, peer, index):
    opened = time.time()
    with STATE_LOCK:
        CONNECTIONS.append({"index": index, "peer": peer, "opened": opened,
                            "closed": None, "media": 0, "call_sid": None})
    note({"at": opened, "event": "connected", "connection": index, "peer": peer})
    log(f"connection {index} from {peer}")
    if not handshake(conn):
        log(f"connection {index} failed the websocket handshake")
        return
    media = 0
    while True:
        opcode, payload = read_frame(conn)
        if opcode is None or opcode == 0x8:
            break
        if opcode != 0x1:
            continue
        try:
            message = json.loads(payload)
        except ValueError:
            continue
        event = message.get("event")
        at = time.time()
        if event == "start":
            start = message.get("start", {})
            with STATE_LOCK:
                CONNECTIONS[index]["call_sid"] = start.get("callSid")
            note({"at": at, "event": "start", "connection": index,
                  "call_sid": start.get("callSid"), "tracks": start.get("tracks"),
                  "media_format": start.get("mediaFormat")})
            log(f"connection {index} start: call={start.get('callSid')} "
                f"tracks={start.get('tracks')} format={start.get('mediaFormat')}")
        elif event == "media":
            packet = message.get("media", {})
            track = packet.get("track", "unknown")
            audio = base64.b64decode(packet["payload"])
            with STATE_LOCK:
                key = track
                if BY_CALL:
                    key = f"{CONNECTIONS[index]['call_sid']}/{track}"
                ARRIVALS.setdefault(key, []).append(
                    (at, index, audio if KEEP_AUDIO else len(audio))
                )
                CONNECTIONS[index]["media"] += 1
            media += 1
            if media == 1:
                log(f"connection {index} first media frame on {track}")
            note_media({"at": at, "event": "media", "connection": index, "track": track,
                        "bytes": len(audio), "timestamp": packet.get("timestamp")})
        elif event == "dtmf":
            note({"at": at, "event": "dtmf", "connection": index,
                  "digit": message.get("dtmf", {}).get("digit")})
        elif event == "stop":
            note({"at": at, "event": "stop", "connection": index})
            log(f"connection {index} stop after {media} media frames")
            break
    closed = time.time()
    with STATE_LOCK:
        CONNECTIONS[index]["closed"] = closed
    note({"at": closed, "event": "closed", "connection": index, "media": media})
    log(f"connection {index} closed after {media} media frames")


def run_guarded(conn, peer, index):
    try:
        serve_one(conn, peer, index)
    except OSError as error:
        log(f"connection {index} error: {error}")
    finally:
        try:
            conn.close()
        except OSError:
            pass


def longest_uncovered_run(covered, first, last):
    """The longest run of frame slots between the first and last arrival that
    no frame ever covered. That is the outage, in slots."""
    longest = 0
    longest_at = None
    run = 0
    for slot in range(first, last + 1):
        if slot in covered:
            if run > longest:
                longest, longest_at = run, slot - run
            run = 0
        else:
            run += 1
    if run > longest:
        longest, longest_at = run, last + 1 - run
    return longest, longest_at


def longest_arrival_gap(frames, t0):
    stamps = sorted(at for at, _i, _a in frames)
    biggest = 0.0
    biggest_at = None
    for earlier, later in zip(stamps, stamps[1:]):
        if later - earlier > biggest:
            biggest, biggest_at = later - earlier, earlier
    return stamps, biggest, biggest_at


def measure_track(track, frames, t0):
    """The stamps-only report: no payloads were kept, so the longest silent run
    (which needs to know which sample slots a frame covered) cannot be computed.
    The longest arrival gap can, and it is the continuity number."""
    stamps, biggest, biggest_at = longest_arrival_gap(frames, t0)
    return {
        "track": track,
        "path": None,
        "frames": len(frames),
        "bytes": sum(a for _at, _i, a in frames),
        "first_arrival": stamps[0] - t0,
        "last_arrival": stamps[-1] - t0,
        "longest_arrival_gap_ms": biggest * 1000.0,
        "longest_arrival_gap_at": (biggest_at - t0) if biggest_at is not None else None,
        "longest_silent_run_ms": None,
        "longest_silent_run_at": None,
    }


def write_track(track, frames, t0):
    placed = {}
    for at, _index, audio in frames:
        offset = int(round((at - t0) * RATE))
        while offset in placed:
            offset += len(audio)
        placed[offset] = audio
    slot = len(frames[0][2]) or 160
    covered = {offset // slot for offset in placed}
    span = max(offset + len(audio) for offset, audio in placed.items())
    timeline = bytearray(span * 2)
    for offset, audio in placed.items():
        pcm = b"".join(struct.pack("<h", ULAW_TABLE[b]) for b in audio)
        timeline[offset * 2:offset * 2 + len(pcm)] = pcm
    safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in track)
    path = os.path.join(OUT_DIR, f"gap-{STAMP}-{safe}.wav")
    with wave.open(path, "wb") as out:
        out.setnchannels(1)
        out.setsampwidth(2)
        out.setframerate(RATE)
        out.writeframes(bytes(timeline))

    slots = sorted(covered)
    uncovered, uncovered_at = longest_uncovered_run(covered, slots[0], slots[-1])
    stamps, biggest, biggest_at = longest_arrival_gap(frames, t0)
    return {
        "track": track,
        "path": path,
        "frames": len(frames),
        "wav_seconds": span / RATE,
        "first_arrival": stamps[0] - t0,
        "last_arrival": stamps[-1] - t0,
        "longest_arrival_gap_ms": biggest * 1000.0,
        "longest_arrival_gap_at": (biggest_at - t0) if biggest_at is not None else None,
        "longest_silent_run_ms": uncovered * slot * 1000.0 / RATE,
        "longest_silent_run_at": None if uncovered_at is None else uncovered_at * slot / RATE,
    }


def report():
    with STATE_LOCK:
        arrivals = {track: list(frames) for track, frames in ARRIVALS.items()}
        connections = [dict(c) for c in CONNECTIONS]
    if not arrivals:
        log("no media ever arrived; nothing to measure")
        return 1
    t0 = min(frames[0][0] for frames in arrivals.values() if frames)
    summary = {"stamp": STAMP, "t0": t0, "connections": [], "tracks": []}
    for connection in connections:
        entry = dict(connection)
        entry["opened_at"] = connection["opened"] - t0
        entry["closed_at"] = None if connection["closed"] is None else connection["closed"] - t0
        summary["connections"].append(entry)
        closed = entry["closed_at"]
        closed_text = "still open" if closed is None else f"{closed:.2f}s"
        log(f"connection {connection['index']}: media={connection['media']} "
            f"opened={entry['opened_at']:.2f}s closed={closed_text}")
    for track, frames in sorted(arrivals.items()):
        measured = write_track(track, frames, t0) if KEEP_AUDIO else \
            measure_track(track, frames, t0)
        summary["tracks"].append(measured)
        when = measured["longest_arrival_gap_at"]
        tail = f"longest silent run={measured['longest_silent_run_ms']:.0f}ms -> " \
               f"{measured['path']}" if KEEP_AUDIO else "no audio kept"
        span = f"wav={measured['wav_seconds']:.2f}s" if KEEP_AUDIO else \
            f"span={measured['last_arrival'] - measured['first_arrival']:.2f}s"
        log(f"track {track}: frames={measured['frames']} {span} "
            f"longest arrival gap={measured['longest_arrival_gap_ms']:.0f}ms "
            f"at {'n/a' if when is None else f'{when:.2f}s'}, {tail}")
    path = os.path.join(OUT_DIR, f"gap-{STAMP}-summary.json")
    with open(path, "w") as out:
        json.dump(summary, out, indent=1)
    log(f"summary in {path}")
    return 0


def main():
    global JOURNAL
    os.makedirs(OUT_DIR, exist_ok=True)
    JOURNAL = open(os.path.join(OUT_DIR, f"gap-{STAMP}-arrivals.jsonl"), "w")
    stopping = threading.Event()

    def stop(_signum, _frame):
        stopping.set()
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, PORT))
    listener.listen(8)
    log(f"listening on {HOST}:{PORT}, writing to {OUT_DIR}, stamp {STAMP}")
    index = 0
    try:
        while True:
            conn, peer = listener.accept()
            threading.Thread(
                target=run_guarded, args=(conn, f"{peer[0]}:{peer[1]}", index), daemon=True
            ).start()
            index += 1
    except KeyboardInterrupt:
        log("stopping; writing artifacts")
    finally:
        listener.close()
    return report()


sys.exit(main())
