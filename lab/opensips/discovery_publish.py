#!/usr/bin/env python3
"""Publishes the lab's call-id -> rtpengine node map into Redis for MSS.

This is the lab's stand-in for OpenSIPS's own cachedb_redis `cache_store`.
The reason it exists is a packaging fact, not a design one: the pinned
opensips/opensips:3.4 image ships cachedb_local.so and cachedb_sql.so but NOT
cachedb_redis.so, and apt.opensips.org no longer carries a 3.4 component for
bullseye, so the module cannot be installed into that image. The image does
ship exec.so and python3, so the lab proxy calls this script where an
integrator's proxy calls cache_store/cache_remove -- the key and the value are
byte-for-byte what docs/deploy.md tells an integrator to write.

  discovery_publish.py store <key> <ttl-seconds> <host:port> [caller-tag] [callee-tag]
  discovery_publish.py remove <key>

The value is the JSON form MSS reads:

  {"node":"172.31.99.10:22222","caller_tag":"abc","from_tags":["abc","def"]}

caller_tag is what makes the tap's attribution `explicit`: it says which of
the two tags called. Without it MSS still taps both legs but names them
leg_a / leg_b (D17). A bare `host:port` value is legal too and is the minimum
an integrator has to write.

Env: DISCOVERY_REDIS (host:port, default 172.31.99.61:6379), DISCOVERY_TIMEOUT
(seconds, default 0.5 -- this runs on an OpenSIPS SIP worker, so it must never
hang one).

Exits 0 even when Redis is unreachable: a proxy that cannot publish the map
must still complete the call, exactly as MSS must still tap it.
"""

import json
import os
import socket
import sys

REDIS = os.environ.get("DISCOVERY_REDIS", "172.31.99.61:6379")
TIMEOUT = float(os.environ.get("DISCOVERY_TIMEOUT", "0.5"))


def encode(args):
    out = [b"*%d\r\n" % len(args)]
    for arg in args:
        raw = arg.encode()
        out.append(b"$%d\r\n%s\r\n" % (len(raw), raw))
    return b"".join(out)


def talk(args):
    host, _, port = REDIS.partition(":")
    with socket.create_connection((host, int(port or "6379")), TIMEOUT) as link:
        link.settimeout(TIMEOUT)
        link.sendall(encode(args))
        return link.recv(4096).decode(errors="replace").strip()


def main():
    argv = sys.argv[1:]
    if len(argv) < 2:
        sys.stderr.write(__doc__)
        return 2
    action, key = argv[0], argv[1]
    if action == "store":
        if len(argv) < 4:
            sys.stderr.write("store needs <key> <ttl> <host:port>\n")
            return 2
        ttl, node = argv[2], argv[3]
        caller = argv[4] if len(argv) > 4 else ""
        callee = argv[5] if len(argv) > 5 else ""
        value = {"node": node}
        if caller:
            value["caller_tag"] = caller
        tags = [tag for tag in (caller, callee) if tag]
        if tags:
            value["from_tags"] = tags
        args = ("SET", key, json.dumps(value, separators=(",", ":")), "EX", ttl)
    elif action == "remove":
        args = ("DEL", key)
    else:
        sys.stderr.write("store or remove\n")
        return 2
    try:
        print(talk(args))
    except (OSError, ValueError) as error:
        sys.stderr.write("discovery map %s failed: %s\n" % (action, error))
    return 0


if __name__ == "__main__":
    sys.exit(main())
