"""A stand-in for stream-llm-bridge: hears the tap, then speaks back.

Speaks the Twilio Media Streams dialect mediagateway froze, so whatever this
accepts the real bridge should accept too. It logs the start/media/dtmf events
MSS sends, and every few seconds answers with an utterance -- a run of media
frames followed by a mark -- which is what makes MSS inject audio into the
call through rtpengine's play media.

Stdlib only, including the websocket handshake and framing, so it runs in a
bare python image next to the rest of the lab.
"""

import base64
import hashlib
import json
import math
import os
import socket
import struct
import threading
import time

HOST = os.environ.get("BRIDGE_HOST", "0.0.0.0")
PORT = int(os.environ.get("BRIDGE_PORT", "8080"))
SPEAK_AFTER_SECONDS = float(os.environ.get("SPEAK_AFTER_SECONDS", "4"))
SPEAK_EVERY_SECONDS = float(os.environ.get("SPEAK_EVERY_SECONDS", "5"))
UTTERANCE_SECONDS = float(os.environ.get("UTTERANCE_SECONDS", "1.0"))
UTTERANCE_HZ = int(os.environ.get("UTTERANCE_HZ", "660"))
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
SAMPLE_RATE = 8000
FRAME_SAMPLES = 160


def log(message):
    print(f"bridge: {message}", flush=True)


def linear_to_ulaw(sample):
    BIAS = 0x84
    CLIP = 32635
    sign = 0x80 if sample < 0 else 0x00
    if sample < 0:
        sample = -sample
    if sample > CLIP:
        sample = CLIP
    sample += BIAS
    exponent = 7
    mask = 0x4000
    while exponent > 0 and not sample & mask:
        exponent -= 1
        mask >>= 1
    mantissa = (sample >> (exponent + 3)) & 0x0F
    return ~(sign | (exponent << 4) | mantissa) & 0xFF


def utterance_frames():
    """A tone, chunked into 20 ms mu-law frames the way a TTS stream arrives."""
    total = int(SAMPLE_RATE * UTTERANCE_SECONDS)
    ulaw = bytes(
        linear_to_ulaw(int(11000 * math.sin(2 * math.pi * UTTERANCE_HZ * n / SAMPLE_RATE)))
        for n in range(total)
    )
    return [ulaw[at:at + FRAME_SAMPLES] for at in range(0, len(ulaw), FRAME_SAMPLES)]


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
        b"HTTP/1.1 101 Switching Protocols\r\n"
        b"Upgrade: websocket\r\n"
        b"Connection: Upgrade\r\n"
        b"Sec-WebSocket-Accept: " + accept.encode() + b"\r\n\r\n"
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


def send_text(conn, text):
    payload = text.encode()
    header = bytearray([0x81])
    length = len(payload)
    if length < 126:
        header.append(length)
    elif length < 65536:
        header.append(126)
        header += struct.pack("!H", length)
    else:
        header.append(127)
        header += struct.pack("!Q", length)
    conn.sendall(bytes(header) + payload)


def speaker(conn, state, stop):
    frames = utterance_frames()
    time.sleep(SPEAK_AFTER_SECONDS)
    spoken = 0
    while not stop.is_set():
        stream_sid = state.get("stream_sid")
        if not stream_sid:
            time.sleep(0.2)
            continue
        spoken += 1
        try:
            for frame in frames:
                send_text(conn, json.dumps({
                    "event": "media",
                    "streamSid": stream_sid,
                    "media": {"payload": base64.b64encode(frame).decode()},
                }))
            send_text(conn, json.dumps({
                "event": "mark",
                "streamSid": stream_sid,
                "mark": {"name": f"utterance-{spoken}"},
            }))
        except OSError:
            return
        log(f"spoke utterance {spoken} ({len(frames)} frames, {UTTERANCE_HZ}Hz)")
        stop.wait(SPEAK_EVERY_SECONDS)


def serve_one(conn):
    if not handshake(conn):
        log("handshake failed")
        return
    log("mss connected")
    state = {}
    stop = threading.Event()
    talking = threading.Thread(target=speaker, args=(conn, state, stop), daemon=True)
    talking.start()

    counts = {"media": 0, "dtmf": 0}
    tracks = {}
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
            state["stream_sid"] = message.get("streamSid")
            start = message.get("start", {})
            log(f"start: call={start.get('callSid')} tracks={start.get('tracks')} "
                f"format={start.get('mediaFormat')}")
        elif event == "media":
            counts["media"] += 1
            track = message["media"].get("track")
            tracks[track] = tracks.get(track, 0) + 1
        elif event == "dtmf":
            counts["dtmf"] += 1
            log(f"dtmf {message['dtmf'].get('digit')} on {message['dtmf'].get('track')}")
        elif event == "stop":
            log("stop")
            break

    stop.set()
    log(f"received {counts['media']} media frames {tracks}, {counts['dtmf']} dtmf events")


def main():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, PORT))
    listener.listen(4)
    log(f"listening on {HOST}:{PORT}")
    while True:
        conn, peer = listener.accept()
        log(f"connection from {peer[0]}:{peer[1]}")
        try:
            serve_one(conn)
        except OSError as error:
            log(f"connection error: {error}")
        finally:
            conn.close()


main()
