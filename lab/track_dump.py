"""A diagnostic consumer that trusts nothing: one wav per track NAME.

The stereo recorder maps tracks onto channels through env vars, which made
recordings ambiguous evidence (RIGHT_TRACK=mixed skewed every verdict that
assumed right=outbound). This listener writes each track it receives to its
own file and prints per-track loudness, so "which track carries the caller"
is read directly off the track name.
"""

import base64
import hashlib
import json
import math
import os
import socket
import struct
import threading
import wave

HOST = os.environ.get("TRACK_DUMP_HOST", "0.0.0.0")
PORT = int(os.environ.get("TRACK_DUMP_PORT", "8095"))
OUT_DIR = os.environ.get("OUT_DIR", "out")
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def log(message):
    print(f"track-dump: {message}", flush=True)


def ulaw_to_linear(byte):
    byte = ~byte & 0xFF
    sign = byte & 0x80
    exponent = (byte >> 4) & 0x07
    mantissa = byte & 0x0F
    sample = ((mantissa << 3) + 0x84) << exponent
    sample -= 0x84
    return -sample if sign else sample


ULAW = [ulaw_to_linear(b) for b in range(256)]


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
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def serve_one(conn):
    if not handshake(conn):
        return
    log("attached")
    tracks = {}
    call = "call"
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
        if event == "start":
            call = message.get("start", {}).get("callSid", call)
            log(f"start: {call}")
        elif event == "media":
            media = message.get("media", {})
            track = media.get("track", "?")
            tracks.setdefault(track, bytearray()).extend(
                base64.b64decode(media["payload"])
            )
        elif event == "stop":
            break

    safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in call)[:50]
    for track, ulaw_bytes in sorted(tracks.items()):
        pcm = [ULAW[b] for b in ulaw_bytes]
        rms = math.sqrt(sum(s * s for s in pcm) / len(pcm)) if pcm else 0.0
        win = 320
        voiced = sum(
            1
            for i in range(0, len(pcm) - win, win)
            if math.sqrt(sum(s * s for s in pcm[i:i + win]) / win) > 300
        )
        total = max(len(pcm) // win, 1)
        path = os.path.join(OUT_DIR, f"track-{safe}-{track}.wav")
        with wave.open(path, "wb") as out:
            out.setnchannels(1)
            out.setsampwidth(2)
            out.setframerate(8000)
            out.writeframes(b"".join(struct.pack("<h", s) for s in pcm))
        log(
            f"track {track!r}: {len(pcm) / 8000:.1f}s rms={rms:.0f} "
            f"voiced={100 * voiced / total:.1f}% -> {path}"
        )


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, PORT))
    listener.listen(4)
    log(f"listening on {HOST}:{PORT}")
    while True:
        conn, _ = listener.accept()
        threading.Thread(target=lambda c=conn: serve_one(c), daemon=True).start()


main()
