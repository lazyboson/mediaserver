"""Captures the bridge's TTS audio straight off the websocket, before MSS touches it.

If this file sounds good and caller_ear.wav does not, the damage is in our
injection path. If this file already sounds bad, the bridge or its ElevenLabs
settings are the cause. Either way it separates the two.
"""

import base64
import json
import os
import struct
import wave

from bridge_probe import connect, recv_messages, send_text

OUT = os.environ.get("OUT", "out/bridge_tts.wav")
LISTEN_SECONDS = float(os.environ.get("LISTEN_SECONDS", "12"))


def ulaw_to_linear(byte):
    byte = ~byte & 0xFF
    sign = byte & 0x80
    exponent = (byte >> 4) & 0x07
    mantissa = byte & 0x0F
    sample = ((mantissa << 3) + 0x84) << exponent
    sample -= 0x84
    return -sample if sign else sample


def main():
    sock = connect()
    silence = base64.b64encode(bytes([0xFF] * 160)).decode()
    send_text(sock, json.dumps({
        "event": "start", "sequenceNumber": "1", "streamSid": "MZ-dump",
        "start": {
            "accountId": "acct", "streamSid": "MZ-dump", "callSid": "call-dump",
            "tracks": ["inbound", "outbound"],
            "mediaFormat": {"encoding": "PCMU", "sampleRate": 8000, "channels": 1},
        },
    }))
    send_text(sock, json.dumps({
        "event": "media", "sequenceNumber": "2", "streamSid": "MZ-dump",
        "media": {"track": "inbound", "timestamp": "20", "payload": silence},
    }))

    messages = recv_messages(sock, LISTEN_SECONDS)
    sock.close()

    audio = bytearray()
    rates = set()
    encodings = set()
    chunks = 0
    for raw in messages:
        try:
            message = json.loads(raw)
        except ValueError:
            continue
        if message.get("event") != "media":
            print(f"  non-media event: {raw[:160]}")
            continue
        media = message.get("media", {})
        rates.add(media.get("sampleRate"))
        encodings.add(media.get("encoding"))
        payload = base64.b64decode(media["payload"])
        chunks += 1
        print(f"  chunk {chunks}: {len(payload)} bytes, first 8 = {payload[:8].hex()}")
        audio += payload

    print(f"\nchunks={chunks} bytes={len(audio)} sampleRates={rates} encodings={encodings}")
    if audio[:4] == b"RIFF":
        print("!! the payload starts with a RIFF header, so it is a container, not raw audio")

    with wave.open(OUT, "wb") as out:
        out.setnchannels(1)
        out.setsampwidth(2)
        out.setframerate(8000)
        out.writeframes(b"".join(struct.pack("<h", ulaw_to_linear(b)) for b in audio))
    print(f"wrote {OUT}: {len(audio) / 8000:.2f}s if this really is 8kHz mu-law")


main()
