"""Fabricates a real call through rtpengine over the NG protocol, then pumps
G.711 for both legs so mediaserverd has something to tap.

No SIP stack is involved: rtpengine accepts offer/answer directly, so this
plays the part of both endpoints and of the signalling proxy between them.
"""

import os
import re
import socket
import struct
import sys
import threading
import time

NODE = os.environ.get("NG_NODE", "127.0.0.1")
PORT = int(os.environ.get("NG_PORT", "22222"))
SELF_IP = os.environ.get("SELF_IP", "127.0.0.1")
CALL_ID = os.environ.get("CALL_ID", "lab-call-1")
FROM_TAG = os.environ.get("FROM_TAG", "tagA")
TO_TAG = os.environ.get("TO_TAG", "tagB")
PUMP_SECONDS = float(os.environ.get("PUMP_SECONDS", "60"))
READY_FILE = os.environ.get("READY_FILE", "/tmp/call-ready")

CALLER_RTP_PORT = 40000
CALLEE_RTP_PORT = 40002


def log(message):
    print(f"call-driver: {message}", flush=True)


def bencode(value):
    if isinstance(value, int):
        return b"i%de" % value
    if isinstance(value, str):
        raw = value.encode()
        return b"%d:%s" % (len(raw), raw)
    if isinstance(value, list):
        return b"l" + b"".join(bencode(v) for v in value) + b"e"
    if isinstance(value, dict):
        out = b"d"
        for key in sorted(value):
            raw = key.encode()
            out += b"%d:%s" % (len(raw), raw) + bencode(value[key])
        return out + b"e"
    raise TypeError(type(value))


def field(body, name):
    match = re.search(rb"%d:%s(\d+):" % (len(name), name.encode()), body)
    if not match:
        return None
    length = int(match.group(1))
    start = match.end()
    return body[start:start + length].decode()


def sdp_for(port):
    return (
        "v=0\r\n"
        f"o=- 1 1 IN IP4 {SELF_IP}\r\n"
        "s=lab\r\n"
        f"c=IN IP4 {SELF_IP}\r\n"
        "t=0 0\r\n"
        f"m=audio {port} RTP/AVP 0 101\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=rtpmap:101 telephone-event/8000\r\n"
        "a=ptime:20\r\n"
        "a=sendrecv\r\n"
    )


class Ng:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(3)
        self.serial = 0

    def send(self, command):
        self.serial += 1
        cookie = f"lab-{self.serial}".encode()
        datagram = cookie + b" " + bencode(command)
        for attempt in range(1, 11):
            self.sock.sendto(datagram, (NODE, PORT))
            try:
                reply, _ = self.sock.recvfrom(65535)
            except socket.timeout:
                log(f"no reply to {command['command']!r} (attempt {attempt})")
                continue
            body = reply.split(b" ", 1)[1]
            result = field(body, "result")
            if result == "error":
                raise RuntimeError(
                    f"{command['command']!r} failed: {field(body, 'error-reason')}"
                )
            return body
        raise RuntimeError(f"rtpengine never answered {command['command']!r}")


def media_port(sdp):
    ports = [int(p) for p in re.findall(r"m=audio (\d+) ", sdp)]
    return ports[0] if ports else None


def pump(caller_dest, callee_dest, stop):
    caller = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    caller.bind(("0.0.0.0", CALLER_RTP_PORT))
    callee = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    callee.bind(("0.0.0.0", CALLEE_RTP_PORT))

    legs = [
        {"sock": caller, "dest": caller_dest, "seq": 1000, "ts": 0, "ssrc": 0x11111111, "byte": 0},
        {"sock": callee, "dest": callee_dest, "seq": 9000, "ts": 0, "ssrc": 0x22222222, "byte": 128},
    ]
    sent = 0
    deadline = time.monotonic() + PUMP_SECONDS
    while not stop.is_set() and time.monotonic() < deadline:
        for leg in legs:
            payload = bytes((leg["byte"] + n) % 256 for n in range(160))
            leg["byte"] = (leg["byte"] + 160) % 256
            header = struct.pack(
                "!BBHII", 0x80, 0, leg["seq"] & 0xFFFF, leg["ts"], leg["ssrc"]
            )
            leg["sock"].sendto(header + payload, leg["dest"])
            leg["seq"] += 1
            leg["ts"] += 160
            sent += 1
        time.sleep(0.02)
    log(f"pumped {sent} rtp packets")


def main():
    if os.path.exists(READY_FILE):
        os.remove(READY_FILE)

    ng = Ng()
    log(f"pinging rtpengine at {NODE}:{PORT}")
    ng.send({"command": "ping"})
    log("rtpengine answered ping")

    offer_reply = ng.send({
        "command": "offer",
        "call-id": CALL_ID,
        "from-tag": FROM_TAG,
        "sdp": sdp_for(CALLER_RTP_PORT),
    })
    callee_dest_port = media_port(field(offer_reply, "sdp") or "")
    log(f"offer accepted; callee sends to {NODE}:{callee_dest_port}")

    answer_reply = ng.send({
        "command": "answer",
        "call-id": CALL_ID,
        "from-tag": FROM_TAG,
        "to-tag": TO_TAG,
        "sdp": sdp_for(CALLEE_RTP_PORT),
    })
    caller_dest_port = media_port(field(answer_reply, "sdp") or "")
    log(f"answer accepted; caller sends to {NODE}:{caller_dest_port}")

    if not callee_dest_port or not caller_dest_port:
        log("rtpengine did not return usable media ports")
        sys.exit(1)

    stop = threading.Event()
    pumping = threading.Thread(
        target=pump,
        args=((NODE, caller_dest_port), (NODE, callee_dest_port), stop),
        daemon=True,
    )
    pumping.start()
    time.sleep(1)

    with open(READY_FILE, "w") as ready:
        ready.write(CALL_ID)
    log(f"call is live and pumping; wrote {READY_FILE}")

    pumping.join()
    log("done")


main()
