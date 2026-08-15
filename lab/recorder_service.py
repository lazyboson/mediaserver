"""A recording consumer: takes the fan-out feed and writes a stereo wav.

Stands in for the Phase-2 recorder. It speaks the same Twilio dialect every
other consumer speaks, which is the point -- the hub does not know or care
what a subscriber does with its frames, so a recorder is just another
attachment alongside the voice-AI bridge and the RTT service.

Customer goes left, agent right, matching tap_spike's stereo convention, so a
recording and a tap artifact of the same call line up channel for channel.
"""

import base64
import hashlib
import json
import os
import socket
import struct
import threading
import time
import wave

HOST = os.environ.get("RECORDER_HOST", "0.0.0.0")
PORT = int(os.environ.get("RECORDER_PORT", "8090"))
OUT_DIR = os.environ.get("OUT_DIR", "/out")
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
LEFT_TRACK = os.environ.get("LEFT_TRACK", "inbound")
RIGHT_TRACK = os.environ.get("RIGHT_TRACK", "outbound")


def log(message):
    print(f"recorder: {message}", flush=True)


def ulaw_to_linear(byte):
    byte = ~byte & 0xFF
    sign = byte & 0x80
    exponent = (byte >> 4) & 0x07
    mantissa = byte & 0x0F
    sample = ((mantissa << 3) + 0x84) << exponent
    sample -= 0x84
    return -sample if sign else sample


ULAW_TABLE = [ulaw_to_linear(b) for b in range(256)]
ULAW_SILENCE = 0xFF


def flatten(placed):
    """Frames placed by their timestamp, gaps filled with silence, so the two
    channels of the recording stay time-aligned even when one side only
    speaks occasionally."""
    if not placed:
        return bytearray()
    span = max(at + len(chunk) for at, chunk in placed.items())
    track = bytearray([ULAW_SILENCE]) * span
    for at, chunk in placed.items():
        track[at:at + len(chunk)] = chunk
    return track


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


def write_recording(call_sid, tracks, digits):
    left = flatten(tracks.get(LEFT_TRACK, {}))
    right = flatten(tracks.get(RIGHT_TRACK, {}))
    frames = max(len(left), len(right))
    if frames == 0:
        log("nothing to write; no media arrived")
        return
    safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in call_sid)[:60]
    stamp = time.strftime("%H%M%S")
    path = os.path.join(OUT_DIR, f"recording-{safe or 'call'}-{stamp}.wav")
    while os.path.exists(path):
        path = path.replace(".wav", "x.wav")
    with wave.open(path, "wb") as out:
        out.setnchannels(2)
        out.setsampwidth(2)
        out.setframerate(8000)
        pcm = bytearray()
        for at in range(frames):
            l = ULAW_TABLE[left[at]] if at < len(left) else 0
            r = ULAW_TABLE[right[at]] if at < len(right) else 0
            pcm += struct.pack("<hh", l, r)
        out.writeframes(bytes(pcm))
    log(f"wrote {path}: {frames / 8000:.2f}s stereo, "
        f"{LEFT_TRACK} left / {RIGHT_TRACK} right, digits={digits or 'none'}")


def serve_one(conn):
    if not handshake(conn):
        return
    log("a tap attached")
    tracks = {}
    call_sid = "call"
    digits = ""
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
        if event == "start":
            start = message.get("start", {})
            call_sid = start.get("callSid") or call_sid
            log(f"start: call={call_sid} tracks={start.get('tracks')} "
                f"format={start.get('mediaFormat')}")
        elif event == "media":
            packet = message.get("media", {})
            track = packet.get("track", LEFT_TRACK)
            audio = base64.b64decode(packet["payload"])
            try:
                at = int(packet.get("timestamp", "0")) * 8
            except ValueError:
                at = 0
            tracks.setdefault(track, {})[at] = audio
            media += 1
        elif event == "dtmf":
            digit = message.get("dtmf", {}).get("digit", "")
            digits += digit
            log(f"dtmf {digit}")
        elif event == "stop":
            log("stop")
            break
    log(f"received {media} media frames across {len(tracks)} track(s)")
    write_recording(call_sid, tracks, digits)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, PORT))
    listener.listen(8)
    log(f"listening on {HOST}:{PORT}, writing to {OUT_DIR}")
    while True:
        conn, _ = listener.accept()
        threading.Thread(target=lambda c=conn: run_guarded(c), daemon=True).start()


def run_guarded(conn):
    try:
        serve_one(conn)
    except OSError as error:
        log(f"connection error: {error}")
    finally:
        conn.close()


main()
