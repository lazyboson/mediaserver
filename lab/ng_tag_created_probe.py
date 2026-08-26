"""Asks rtpengine which fields of a `query` reply carry a participant's
creation time, and whether the reply's tag order matches that time.

D17 (leg labels invert without an explicit from-tag) turns on one question we
must not answer from documentation: given a call rtpengine anchors, can MSS
tell the caller from the callee without being told? This probe fabricates a
call with a deliberate gap between the caller's offer and the callee's answer,
then dumps every scalar field of each tag entry, so the field names in
docs/implementation-notes.md are measured rather than assumed.

  docker run --rm --network mss-microsip_lab -v "$PWD/lab:/lab" -w /lab \
      -e NG_NODE=172.31.99.10 -e ANSWER_DELAY=3 python:3-slim \
      python ng_tag_created_probe.py

Env: NG_NODE, NG_PORT, CALL_ID, FROM_TAG, TO_TAG, ANSWER_DELAY (seconds
between the offer and the answer), REVERSE (=1 asks the answer first, i.e.
creates the callee tag before the caller tag), COOKIE_PREFIX (defaults to a
random one -- rtpengine replays a cached reply for a repeated cookie, so a
fixed prefix makes consecutive runs answer with the previous run's call).
"""

import json
import os
import random
import re
import socket
import time

NODE = os.environ.get("NG_NODE", "127.0.0.1")
PORT = int(os.environ.get("NG_PORT", "22222"))
SELF_IP = os.environ.get("SELF_IP", "127.0.0.1")
CALL_ID = os.environ.get("CALL_ID", "tag-created-probe")
FROM_TAG = os.environ.get("FROM_TAG", "caller-tag")
TO_TAG = os.environ.get("TO_TAG", "callee-tag")
ANSWER_DELAY = float(os.environ.get("ANSWER_DELAY", "3"))
REVERSE = os.environ.get("REVERSE", "0") == "1"
COOKIE_PREFIX = os.environ.get(
    "COOKIE_PREFIX", "created-%d" % random.randint(0, 1 << 30))


def log(message):
    print(message, flush=True)


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


def bdecode(body, at=0):
    kind = body[at:at + 1]
    if kind == b"d":
        at += 1
        out = {}
        while body[at:at + 1] != b"e":
            key, at = bdecode(body, at)
            value, at = bdecode(body, at)
            out[key] = value
        return out, at + 1
    if kind == b"l":
        at += 1
        out = []
        while body[at:at + 1] != b"e":
            value, at = bdecode(body, at)
            out.append(value)
        return out, at + 1
    if kind == b"i":
        end = body.index(b"e", at)
        return int(body[at + 1:end]), end + 1
    colon = body.index(b":", at)
    length = int(body[at:colon])
    start = colon + 1
    return body[start:start + length].decode("latin-1"), start + length


class Ng:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(3)
        self.serial = 0

    def send(self, command):
        self.serial += 1
        cookie = b"%s-%d " % (COOKIE_PREFIX.encode(), self.serial)
        datagram = cookie + bencode(command)
        for _ in range(4):
            self.sock.sendto(datagram, (NODE, PORT))
            try:
                reply, _ = self.sock.recvfrom(400000)
            except socket.timeout:
                continue
            decoded, _ = bdecode(reply.split(b" ", 1)[1])
            return decoded
        raise RuntimeError(f"rtpengine never answered {command['command']!r}")


def sdp_for(port):
    return (
        "v=0\r\n"
        f"o=- 1 1 IN IP4 {SELF_IP}\r\n"
        "s=probe\r\n"
        f"c=IN IP4 {SELF_IP}\r\n"
        "t=0 0\r\n"
        f"m=audio {port} RTP/AVP 0\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=sendrecv\r\n"
    )


def scalars(prefix, value, into):
    if isinstance(value, dict):
        for key, inner in value.items():
            scalars(f"{prefix}.{key}" if prefix else str(key), inner, into)
    elif isinstance(value, list):
        for index, inner in enumerate(value):
            scalars(f"{prefix}[{index}]", inner, into)
    else:
        into[prefix] = value


def main():
    ng = Ng()
    ng.send({"command": "ping"})
    log(f"rtpengine at {NODE}:{PORT} answered ping")

    first_tag, second_tag = (TO_TAG, FROM_TAG) if REVERSE else (FROM_TAG, TO_TAG)
    log(f"creating tag {first_tag!r} first, then {second_tag!r} after "
        f"{ANSWER_DELAY}s (reverse={REVERSE})")

    started = int(time.time())
    ng.send({
        "command": "offer",
        "call-id": CALL_ID,
        "from-tag": first_tag,
        "sdp": sdp_for(41000),
    })
    log(f"offer accepted at t={int(time.time()) - started}s")
    time.sleep(ANSWER_DELAY)
    ng.send({
        "command": "answer",
        "call-id": CALL_ID,
        "from-tag": first_tag,
        "to-tag": second_tag,
        "sdp": sdp_for(41002),
    })
    log(f"answer accepted at t={int(time.time()) - started}s")

    reply = ng.send({"command": "query", "call-id": CALL_ID})
    top = {k: v for k, v in reply.items() if not isinstance(v, (dict, list))}
    log(f"query top-level scalars: {json.dumps(top, sort_keys=True)}")

    tags = reply.get("tags", {})
    log(f"tag order as the reply lists it: {list(tags)}")
    for tag, details in tags.items():
        if not isinstance(details, dict):
            continue
        flat = {}
        scalars("", details, flat)
        timeish = {
            key: value for key, value in flat.items()
            if re.search(r"creat|time|last|since|age", key, re.I)
        }
        own = {k: v for k, v in details.items()
               if not isinstance(v, (dict, list))}
        log(f"--- tag {tag!r}")
        log(f"    own scalar fields: {json.dumps(own, sort_keys=True)}")
        log(f"    time-ish anywhere: {json.dumps(timeish, sort_keys=True)}")

    ng.send({"command": "delete", "call-id": CALL_ID})
    log("call deleted")


if __name__ == "__main__":
    main()
