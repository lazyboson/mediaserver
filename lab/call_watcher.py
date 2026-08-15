"""Discovers the live call so mediaserverd can tap it.

With OpenSIPS in the path the rtpengine call-id is the softphone's random SIP
Call-ID, so MSS_TAP_CALL_ID cannot be set before the call exists. roadmap.md
Phase 0 settles this properly by having OpenSIPS write call-id, node and tags
to Redis at setup. This is the lab stand-in: poll rtpengine's own `list`, ask
`query` for the tags, and write an env file mediaserverd sources.

Tags are ordered by creation so the caller comes first, which is what makes
stream 0 the customer and points injection at the softphone rather than at
FreeSWITCH.
"""

import os
import socket
import time

NODE = os.environ.get("NG_NODE", "172.31.99.10")
NG_PORT = int(os.environ.get("NG_PORT", "22222"))
OUT = os.environ.get("CALL_ENV_FILE", "/shared/call.env")
POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "1"))
CALLEE_MEDIA_IP = os.environ.get("CALLEE_MEDIA_IP", "172.31.99.80")


def log(message):
    print(f"call-watcher: {message}", flush=True)


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


def bdecode(data, at=0):
    token = data[at:at + 1]
    if token == b"i":
        end = data.index(b"e", at)
        return int(data[at + 1:end]), end + 1
    if token == b"l":
        at += 1
        items = []
        while data[at:at + 1] != b"e":
            item, at = bdecode(data, at)
            items.append(item)
        return items, at + 1
    if token == b"d":
        at += 1
        out = {}
        while data[at:at + 1] != b"e":
            key, at = bdecode(data, at)
            value, at = bdecode(data, at)
            out[key.decode("utf-8", "replace") if isinstance(key, bytes) else key] = value
        return out, at + 1
    colon = data.index(b":", at)
    length = int(data[at:colon])
    start = colon + 1
    return data[start:start + length], start + length


class Ng:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(3)
        self.serial = 0

    def send(self, command):
        self.serial += 1
        cookie = f"watch-{self.serial}".encode()
        self.sock.sendto(cookie + b" " + bencode(command), (NODE, NG_PORT))
        try:
            reply, _ = self.sock.recvfrom(262144)
        except socket.timeout:
            return None
        body = reply.split(b" ", 1)[1]
        decoded, _ = bdecode(body)
        return decoded


def text(value):
    return value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)


def endpoint_address(details):
    for media in details.get("medias", []) or []:
        for stream in media.get("streams", []) or []:
            endpoint = stream.get("endpoint", {}) or {}
            address = endpoint.get("address")
            if address:
                return text(address)
    return ""


def tags_of(query):
    """Caller first, which is what makes stream 0 the customer.

    `created` is second-resolution and ties on a fast answer, so ordering by it
    flips at random. The peer endpoint address does not: the callee leg points
    at FreeSWITCH, and whatever is left is the softphone.
    """
    tags = query.get("tags", {})
    caller, callee = [], []
    for tag, details in tags.items():
        if not isinstance(details, dict):
            continue
        name = text(tag)
        if not name:
            continue
        if endpoint_address(details) == CALLEE_MEDIA_IP:
            callee.append(name)
        else:
            caller.append(name)
    return caller + callee


def main():
    ng = Ng()
    log(f"watching {NODE}:{NG_PORT} for a call")
    announced = None
    while True:
        listing = ng.send({"command": "list"})
        calls = [text(c) for c in (listing or {}).get("calls", [])]
        if not calls:
            if announced:
                log(f"call {announced} is gone")
                announced = None
                if os.path.exists(OUT):
                    os.remove(OUT)
            time.sleep(POLL_SECONDS)
            continue

        call_id = calls[0]
        if call_id == announced:
            time.sleep(POLL_SECONDS)
            continue

        query = ng.send({"command": "query", "call-id": call_id})
        tags = tags_of(query or {})
        if len(tags) < 2:
            log(f"call {call_id} has {len(tags)} tag(s) so far, waiting for the answer")
            time.sleep(POLL_SECONDS)
            continue

        with open(OUT, "w") as out:
            out.write(f"MSS_TAP_CALL_ID={call_id}\n")
            out.write(f"MSS_TAP_FROM_TAGS={','.join(tags)}\n")
        announced = call_id
        log(f"call {call_id} is up with tags {tags}; wrote {OUT}")
        time.sleep(POLL_SECONDS)


main()
