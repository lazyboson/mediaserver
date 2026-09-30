"""Asks the real stream-llm-bridge which wire shape it accepts.

crates/protocol/src/twilio.rs claims to be compatible with the legacy media gateway, and its
serialization tests are treated as the spec. This checks that claim against
the service that has to parse it, the same way ng_answer_probe.py checked
rtpengine instead of trusting the NG documentation.
"""

import base64
import hashlib
import json
import os
import socket
import struct
import time

HOST = os.environ.get("BRIDGE_HOST", "127.0.0.1")
PORT = int(os.environ.get("BRIDGE_PORT", "18080"))
PATH = os.environ.get("BRIDGE_PATH", "/ws")
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def log(message):
    print(message, flush=True)


def connect():
    sock = socket.create_connection((HOST, PORT), timeout=5)
    key = base64.b64encode(os.urandom(16)).decode()
    sock.sendall(
        f"GET {PATH} HTTP/1.1\r\nHost: {HOST}:{PORT}\r\nUpgrade: websocket\r\n"
        f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n\r\n".encode()
    )
    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("bridge closed during handshake")
        response += chunk
    status = response.split(b"\r\n")[0].decode()
    expected = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
    if expected.encode() not in response:
        raise RuntimeError(f"bad handshake: {status}")
    return sock


def send_text(sock, payload):
    data = payload.encode()
    mask = os.urandom(4)
    header = bytearray([0x81])
    length = len(data)
    if length < 126:
        header.append(0x80 | length)
    elif length < 65536:
        header.append(0x80 | 126)
        header += struct.pack("!H", length)
    else:
        header.append(0x80 | 127)
        header += struct.pack("!Q", length)
    header += mask
    masked = bytes(byte ^ mask[i % 4] for i, byte in enumerate(data))
    sock.sendall(bytes(header) + masked)


def recv_messages(sock, seconds):
    sock.settimeout(seconds)
    out = []
    deadline = time.monotonic() + seconds
    buffer = b""
    while time.monotonic() < deadline:
        try:
            chunk = sock.recv(65535)
        except (socket.timeout, TimeoutError):
            break
        if not chunk:
            break
        buffer += chunk
        while len(buffer) >= 2:
            opcode = buffer[0] & 0x0F
            length = buffer[1] & 0x7F
            at = 2
            if length == 126:
                if len(buffer) < 4:
                    break
                length = struct.unpack("!H", buffer[2:4])[0]
                at = 4
            elif length == 127:
                if len(buffer) < 10:
                    break
                length = struct.unpack("!Q", buffer[2:10])[0]
                at = 10
            if len(buffer) < at + length:
                break
            payload = buffer[at:at + length]
            buffer = buffer[at + length:]
            if opcode == 0x1:
                out.append(payload.decode("utf-8", "replace"))
            elif opcode == 0x8:
                out.append("<close frame>")
                return out
    return out


def attempt(name, start, media):
    log(f"\n--- {name}")
    try:
        sock = connect()
    except (OSError, RuntimeError) as error:
        log(f"  connect failed: {error}")
        return
    log(f"  start: {start}")
    send_text(sock, start)
    for _ in range(5):
        send_text(sock, media)
    replies = recv_messages(sock, 3)
    if not replies:
        log("  bridge said nothing")
    for reply in replies[:6]:
        log(f"  <- {reply[:240]}")
    sock.close()


def main():
    silence = base64.b64encode(bytes([0xFF] * 160)).decode()

    ours_start = json.dumps({
        "event": "start", "sequenceNumber": "1", "streamSid": "MZ-1",
        "start": {
            "accountId": "acct-1", "streamSid": "MZ-1", "callSid": "call-1",
            "tracks": ["inbound", "outbound"],
            "mediaFormat": {"encoding": "audio/x-mulaw", "sampleRate": 8000, "channels": 1},
        },
    })
    ours_media = json.dumps({
        "event": "media", "sequenceNumber": "2", "streamSid": "MZ-1",
        "media": {"track": "inbound", "timestamp": 20, "payload": silence},
    })

    theirs_start = json.dumps({
        "event": "start", "sequenceNumber": "1", "streamSid": "MZ-1",
        "start": {
            "accountId": "acct-1", "streamSid": "MZ-1", "callSid": "call-1",
            "tracks": ["inbound", "outbound"],
            "mediaFormat": {"encoding": "PCMU", "sampleRate": 8000, "channels": 1},
        },
    })
    theirs_media = json.dumps({
        "event": "media", "sequenceNumber": "2", "streamSid": "MZ-1",
        "media": {"track": "inbound", "timestamp": "20", "payload": silence},
    })

    attempt("what twilio.rs emits today (audio/x-mulaw, numeric timestamp)",
            ours_start, ours_media)
    attempt("what the legacy media gateway emits (PCMU, string timestamp)",
            theirs_start, theirs_media)
    attempt("mixed: PCMU start, numeric timestamp media",
            theirs_start, ours_media)


if __name__ == "__main__":
    main()
