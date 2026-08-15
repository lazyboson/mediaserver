"""A real-time-transcription consumer: fan-out feed in, transcript file out.

Stands in for the RTT consumer Phase 1 exists to serve. It attaches to the
same hub feed as the voice-AI bridge and the recorder, opens one Deepgram
stream per track so speaker attribution survives, and appends every final
transcript to a text file as it arrives.

Without DEEPGRAM_API_KEY it still runs, logging frame counts and writing a
mock transcript line per track, so the fan-out can be demonstrated with no
external dependency at all.
"""

import base64
import hashlib
import json
import os
import socket
import ssl
import struct
import threading
import time

HOST = os.environ.get("RTT_HOST", "0.0.0.0")
PORT = int(os.environ.get("RTT_PORT", "8091"))
OUT_DIR = os.environ.get("OUT_DIR", "/out")
DEEPGRAM_KEY = os.environ.get("DEEPGRAM_API_KEY", "")
DEEPGRAM_MODEL = os.environ.get("DEEPGRAM_MODEL", "nova-3")
DEEPGRAM_HOST = "api.deepgram.com"
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def log(message):
    print(f"rtt: {message}", flush=True)


def mask_frame(opcode, data):
    mask = os.urandom(4)
    header = bytearray([0x80 | opcode])
    if len(data) < 126:
        header.append(0x80 | len(data))
    elif len(data) < 65536:
        header.append(0x80 | 126)
        header += struct.pack("!H", len(data))
    else:
        header.append(0x80 | 127)
        header += struct.pack("!Q", len(data))
    header += mask
    return bytes(header) + bytes(b ^ mask[i % 4] for i, b in enumerate(data))


def server_frame(opcode, data):
    header = bytearray([0x80 | opcode])
    if len(data) < 126:
        header.append(len(data))
    elif len(data) < 65536:
        header.append(126)
        header += struct.pack("!H", len(data))
    else:
        header.append(127)
        header += struct.pack("!Q", len(data))
    return bytes(header) + data


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


class DeepgramStream:
    """One Deepgram live connection, so each track keeps its own speaker."""

    def __init__(self, track, on_transcript):
        self.track = track
        self.on_transcript = on_transcript
        self.sock = None
        self.alive = False
        if not DEEPGRAM_KEY:
            return
        try:
            self.connect()
            self.alive = True
            threading.Thread(target=self.read_loop, daemon=True).start()
        except (OSError, ssl.SSLError, RuntimeError) as error:
            log(f"[{track}] deepgram unavailable ({error}); falling back to counting")

    def connect(self):
        query = (
            f"/v1/listen?encoding=mulaw&sample_rate=8000&channels=1"
            f"&model={DEEPGRAM_MODEL}&punctuate=true&interim_results=false"
        )
        raw = socket.create_connection((DEEPGRAM_HOST, 443), timeout=10)
        context = ssl.create_default_context()
        self.sock = context.wrap_socket(raw, server_hostname=DEEPGRAM_HOST)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall(
            f"GET {query} HTTP/1.1\r\nHost: {DEEPGRAM_HOST}\r\n"
            f"Authorization: Token {DEEPGRAM_KEY}\r\n"
            f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode()
        )
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise RuntimeError("deepgram closed during handshake")
            response += chunk
        if b" 101 " not in response.split(b"\r\n")[0]:
            raise RuntimeError(response.split(b"\r\n")[0].decode("latin-1"))
        log(f"[{self.track}] deepgram stream open")

    def send(self, audio):
        if not self.alive:
            return
        try:
            self.sock.sendall(mask_frame(0x2, audio))
        except OSError:
            self.alive = False

    def read_loop(self):
        while self.alive:
            try:
                opcode, payload = read_frame(self.sock)
            except OSError:
                break
            if opcode is None or opcode == 0x8:
                break
            if opcode != 0x1:
                continue
            try:
                message = json.loads(payload)
            except ValueError:
                continue
            for alternative in message.get("channel", {}).get("alternatives", []):
                text = alternative.get("transcript", "").strip()
                if text and message.get("is_final"):
                    self.on_transcript(self.track, text)
        self.alive = False

    def close(self):
        if self.sock:
            try:
                self.sock.sendall(mask_frame(0x1, b'{"type":"CloseStream"}'))
                self.sock.close()
            except OSError:
                pass
        self.alive = False


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


def serve_one(conn):
    if not handshake(conn):
        return
    log("a tap attached")
    call_sid = "call"
    streams = {}
    counts = {}
    started = time.monotonic()
    transcript_path = [None]
    lock = threading.Lock()

    def record(track, text):
        line = f"[{time.monotonic() - started:7.2f}s] {track:8s} {text}"
        log(line)
        with lock:
            if transcript_path[0]:
                with open(transcript_path[0], "a") as out:
                    out.write(line + "\n")

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
            safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in call_sid)[:60]
            transcript_path[0] = os.path.join(OUT_DIR, f"transcript-{safe or 'call'}.txt")
            fresh = not os.path.exists(transcript_path[0])
            with open(transcript_path[0], "a") as out:
                if fresh:
                    out.write(f"# real-time transcript for call {call_sid}\n")
                    out.write(f"# deepgram={'live' if DEEPGRAM_KEY else 'mocked'} model={DEEPGRAM_MODEL}\n")
                else:
                    out.write("# --- tap reattached; transcript continues ---\n")
            log(f"start: call={call_sid} -> {transcript_path[0]}")
        elif event == "media":
            packet = message.get("media", {})
            track = packet.get("track", "inbound")
            audio = base64.b64decode(packet["payload"])
            counts[track] = counts.get(track, 0) + 1
            if track not in streams:
                streams[track] = DeepgramStream(track, record)
            streams[track].send(audio)
        elif event == "dtmf":
            record(message.get("dtmf", {}).get("track", "?"),
                   f"<dtmf {message.get('dtmf', {}).get('digit', '')}>")
        elif event == "stop":
            break

    for stream in streams.values():
        stream.close()
    time.sleep(0.5)
    for track, count in counts.items():
        if not streams[track].alive and not DEEPGRAM_KEY:
            record(track, f"<mock transcript: {count} frames, {count * 0.02:.1f}s of audio>")
    log(f"finished: {counts}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, PORT))
    listener.listen(8)
    log(f"listening on {HOST}:{PORT}, deepgram={'live' if DEEPGRAM_KEY else 'mocked'}")
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
