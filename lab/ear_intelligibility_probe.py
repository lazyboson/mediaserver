"""Measures whether injected audio is intelligible, using the ASR as the judge.

Feeds a recorded ear wav (what the caller actually heard) back through the
bridge as if it were a tap. If Deepgram transcribes the reply text correctly,
the injection path is objectively intelligible; a human complaint about
quality then points at the codec ceiling or the softphone, not this pipeline.

Run inside the lab network with the wav mounted:
  docker run --rm --network mss-microsip_lab -v $PWD:/lab -w /lab \
    python:3-slim python3 ear_intelligibility_probe.py out/host_ear_<ssrc>.wav
"""

import base64
import hashlib
import json
import os
import socket
import struct
import sys
import time
import wave

HOST = os.environ.get("BRIDGE_HOST", "172.31.99.50")
PORT = int(os.environ.get("BRIDGE_PORT", "8080"))
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def log(message):
    print(f"intel-probe: {message}", flush=True)


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


def connect():
    sock = socket.create_connection((HOST, PORT), timeout=5)
    key = base64.b64encode(os.urandom(16)).decode()
    sock.sendall(
        f"GET /ws HTTP/1.1\r\nHost: {HOST}:{PORT}\r\nUpgrade: websocket\r\n"
        f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n\r\n".encode()
    )
    response = b""
    while b"\r\n\r\n" not in response:
        response += sock.recv(4096)
    expected = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
    if expected.encode() not in response:
        raise SystemExit("handshake failed")
    return sock


def send_text(sock, payload):
    data = payload.encode()
    mask = os.urandom(4)
    header = bytearray([0x81])
    if len(data) < 126:
        header.append(0x80 | len(data))
    else:
        header.append(0x80 | 126)
        header += struct.pack("!H", len(data))
    header += mask
    sock.sendall(bytes(header) + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))


def downsample(pcm, rate):
    """The bridge speaks 8 kHz mu-law, so anything wider is averaged down.

    Averaging each group is a crude low-pass, which is enough to keep a 16 kHz
    L16 tap artifact (what mss_stream_probe writes) intelligible to the ASR.
    """
    if rate == 8000:
        return pcm
    factor, remainder = divmod(rate, 8000)
    if remainder or factor < 1:
        raise SystemExit(f"{rate} Hz is not a whole multiple of 8000")
    return [
        sum(pcm[i:i + factor]) // factor
        for i in range(0, len(pcm) - factor + 1, factor)
    ]


def main():
    path = sys.argv[1]
    with wave.open(path) as w:
        pcm = struct.unpack(f"<{w.getnframes()}h", w.readframes(w.getnframes()))
        rate = w.getframerate()
    pcm = downsample(list(pcm), rate)
    log(f"{path} is {rate} Hz; feeding {len(pcm) / 8000:.2f}s at 8 kHz")
    ulaw = bytes(linear_to_ulaw(s) for s in pcm)
    frames = [ulaw[i:i + 160] for i in range(0, len(ulaw) - 159, 160)]
    log(f"replaying {len(frames)} frames ({len(frames) * 0.02:.1f}s) of {path}")

    sock = connect()
    send_text(sock, json.dumps({
        "event": "start", "sequenceNumber": "1", "streamSid": "MZ-intel",
        "start": {
            "accountId": "intel", "streamSid": "MZ-intel", "callSid": "intel-1",
            "tracks": ["inbound"],
            "mediaFormat": {"encoding": "PCMU", "sampleRate": 8000, "channels": 1},
        },
    }))
    sock.setblocking(False)
    for index, frame in enumerate(frames):
        send_text(sock, json.dumps({
            "event": "media", "sequenceNumber": str(index + 2), "streamSid": "MZ-intel",
            "media": {"track": "inbound", "timestamp": str(index * 20),
                      "payload": base64.b64encode(frame).decode()},
        }))
        try:
            while sock.recv(65535):
                pass
        except (BlockingIOError, OSError):
            pass
        time.sleep(0.02)
    time.sleep(4)
    sock.close()
    log("done; the verdict is in the bridge log as 'Processing complete utterance'")


main()
