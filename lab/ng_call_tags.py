"""Asks rtpengine who is attached to a call, and which of them are taps.

The pod-kill drill (item 11) has to prove whether a re-subscribe leaves an
orphan behind: the pod that died cannot send `unsubscribe`, so if nothing
else does, rtpengine keeps copying media to a dead socket for the rest of the
call. Asking rtpengine is the only honest way to find out -- our own logs
report what we *asked* for, not what the vendor kept.

Where a subscription shows up, measured on rtpengine 14.1.1.8 rather than
assumed, including one surprise:

  * a tap appears inside each tapped leg as an entry in that leg's
    `subscribers` list with `"type": "pub/sub"` (the `offer/answer` entry
    there is the other call leg), and also as a tag of its own;
  * **but a lone subscription is invisible.** With exactly one tap on the
    call, 20 s of delivered media in, `query` still reported only the two
    call legs and no pub/sub subscriber. Both taps appeared the moment a
    second `subscribe` touched the call. So a 0 from this script means
    "0 or 1", and only counts above 1 are trustworthy;
  * and the per-stream numbers in a query reply are not live at all: on a
    call carrying 50 packets/s, both `stats_out` and `last packet` came back
    unchanged from two queries 15-20 s apart. Treat them as a stale snapshot,
    not as evidence that media is or is not flowing.

For a real audit of what a call was fed, read rtpengine's own "Final packet
stats" block at teardown: it names every subscription monologue (ours carry
the label `mss-tap`), the address it was sent to, and the packet count. The
drill prints it for exactly that reason.

Pass the legs in LEGS (comma separated, exactly what MSS_TAP_FROM_TAGS holds)
so a tag that is not a call leg is labelled as a tap.

  docker run --rm --network mss-microsip_lab -v "$PWD/lab:/lab" -w /lab \\
      -e CALL_ID=... -e LEGS=... python:3-slim python ng_call_tags.py

Env: NG_NODE, NG_PORT, CALL_ID (required), LEGS, LABEL, DUMP.
Exit code is the number of subscriptions, so a shell can assert on it.
"""

import json
import os
import socket
import sys

NODE = os.environ.get("NG_NODE", "172.31.99.10")
PORT = int(os.environ.get("NG_PORT", "22222"))
CALL_ID = os.environ.get("CALL_ID", "")
LEGS = [tag for tag in os.environ.get("LEGS", "").split(",") if tag]
LABEL = os.environ.get("LABEL", "now")
DUMP = os.environ.get("DUMP", "0") == "1"


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


def query():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(3)
    datagram = b"tags-1 " + bencode({"command": "query", "call-id": CALL_ID})
    for _ in range(4):
        sock.sendto(datagram, (NODE, PORT))
        try:
            reply, _ = sock.recvfrom(400000)
        except socket.timeout:
            continue
        decoded, _ = bdecode(reply.split(b" ", 1)[1])
        return decoded
    return {"result": "error", "error-reason": "no reply"}


def local_ports(details):
    ports = []
    for media in details.get("medias", []) or []:
        for stream in media.get("streams", []) or []:
            port = stream.get("local port") or stream.get("local_port")
            if port:
                ports.append(port)
    return ports


def last_packet(details):
    """The newest `last packet` across the tag's streams -- reported for
    completeness, but see the module docstring: it does not advance between
    queries on a live call, so it cannot be used to argue that media is still
    flowing. `created` is the field that reliably separates an older orphan
    from the tap that replaced it."""
    newest = 0
    for media in details.get("medias", []) or []:
        for stream in media.get("streams", []) or []:
            seen = stream.get("last packet", 0)
            if isinstance(seen, int) and seen > newest:
                newest = seen
    return newest


def main():
    if not CALL_ID:
        log("CALL_ID is required")
        return -1
    answer = query()
    if DUMP:
        log(json.dumps(answer, indent=1, default=str)[:8000])
    if answer.get("result") != "ok":
        log(f"[{LABEL}] rtpengine does not know call {CALL_ID}: "
            f"{answer.get('error-reason')}")
        return 0
    tags = answer.get("tags", {})
    taps = {}
    for tag, details in tags.items():
        if not isinstance(details, dict):
            continue
        for subscriber in details.get("subscribers", []) or []:
            if not isinstance(subscriber, dict):
                continue
            if subscriber.get("type") != "pub/sub":
                continue
            taps.setdefault(subscriber.get("tag", "?"), []).append(tag)

    log(f"[{LABEL}] call {CALL_ID}: {len(tags)} tag(s), {len(taps)} tap(s)")
    for tag, details in tags.items():
        if not isinstance(details, dict):
            continue
        kind = "leg" if not LEGS or tag in LEGS else "tap"
        log(f"  {kind} {tag[:24]} ports={local_ports(details)} "
            f"created={details.get('created')} last_packet={last_packet(details)}")
    for tag, carried in sorted(taps.items()):
        log(f"  subscriber {tag[:24]} carries {sorted(carried)}")
    return len(taps)


sys.exit(main())
