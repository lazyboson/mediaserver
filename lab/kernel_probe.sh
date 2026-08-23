#!/bin/sh
# Item 23: says whether an rtpengine's kernel module (xt_RTPENGINE) is in play,
# using only the NG control protocol, so it runs against a container, a staging
# node or a production box unchanged.
#
#   ./lab/kernel_probe.sh [host] [port]           # defaults 127.0.0.1 22222
#
# In this lab the NG port is not published to the WSL host, so reach it from
# inside the lab network:
#
#   DOCKER_API_VERSION=1.43 docker run --rm --network mss-microsip_lab \
#     -v "$PWD/lab":/lab:ro python:3-slim sh /lab/kernel_probe.sh 172.31.99.10 22222
#
# The NG evidence is rtpengine's own accounting: `statistics` splits both the
# lifetime relay totals (relayedpackets_kernel vs _user) and the live rates
# (packetrate_kernel, media_kernel, media_mixed) between the kernel module and
# userspace. A node with the module loaded and forwarding shows non-zero kernel
# counters; a node running --table=-1, or one whose every session is
# transcoding, shows all of it in userspace.
#
# When run ON the rtpengine host it also reports the local evidence
# (/proc/rtpengine, lsmod), which the NG protocol cannot expose. That half is
# skipped, not guessed, anywhere else.
#
# Exit codes: 0 kernel module in play, 1 userspace only, 2 cannot tell,
# 3 rtpengine unreachable or python3 missing.
set -eu

HOST=${1:-${NG_NODE:-127.0.0.1}}
PORT=${2:-${NG_PORT:-22222}}

echo "kernel_probe: asking $HOST:$PORT for statistics over NG"

echo "kernel_probe: --- local evidence (only meaningful on the rtpengine host) ---"
if [ -d /proc/rtpengine ]; then
  echo "kernel_probe: /proc/rtpengine exists; tables:"
  for table in /proc/rtpengine/*; do
    [ -d "$table" ] || continue
    entries=$(wc -l <"$table/list" 2>/dev/null || echo "?")
    echo "kernel_probe:   $(basename "$table") -> $entries forwarding entries"
  done
else
  echo "kernel_probe: no /proc/rtpengine (module not loaded here, or not this host)"
fi
if command -v lsmod >/dev/null 2>&1; then
  if lsmod | grep -q xt_RTPENGINE; then
    echo "kernel_probe: lsmod shows xt_RTPENGINE loaded"
  else
    echo "kernel_probe: lsmod does not show xt_RTPENGINE"
  fi
else
  echo "kernel_probe: no lsmod on this host"
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "kernel_probe: python3 is required to speak bencode over UDP" >&2
  exit 3
fi

echo "kernel_probe: --- NG evidence ---"
NG_HOST=$HOST NG_PORT=$PORT python3 - <<'PY'
import os
import socket
import sys

HOST = os.environ["NG_HOST"]
PORT = int(os.environ["NG_PORT"])


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


def bdecode(data, i=0):
    head = data[i:i + 1]
    if head == b"i":
        end = data.index(b"e", i)
        return int(data[i + 1:end]), end + 1
    if head == b"l":
        i += 1
        out = []
        while data[i:i + 1] != b"e":
            value, i = bdecode(data, i)
            out.append(value)
        return out, i + 1
    if head == b"d":
        i += 1
        out = {}
        while data[i:i + 1] != b"e":
            key, i = bdecode(data, i)
            value, i = bdecode(data, i)
            out[key if isinstance(key, str) else repr(key)] = value
        return out, i + 1
    colon = data.index(b":", i)
    length = int(data[i:colon])
    start = colon + 1
    raw = data[start:start + length]
    try:
        return raw.decode(), start + length
    except UnicodeDecodeError:
        return raw.hex(), start + length


SERIAL = [0]


def call(sock, command):
    """One cookie per command: rtpengine caches a reply against its cookie and
    replays it, so reusing one makes every later command answer the first."""
    SERIAL[0] += 1
    cookie = b"kernel-probe-%d-%d" % (os.getpid(), SERIAL[0])
    datagram = cookie + b" " + bencode({"command": command})
    for _ in range(4):
        sock.sendto(datagram, (HOST, PORT))
        try:
            reply, _ = sock.recvfrom(1 << 20)
        except socket.timeout:
            continue
        return bdecode(reply.split(b" ", 1)[1])[0]
    return None


def number(holder, key):
    """rtpengine returns some of these as bencode strings, not integers."""
    raw = (holder or {}).get(key)
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        try:
            return int(float(raw))
        except ValueError:
            return None
    return None


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(3)

if call(sock, "ping") is None:
    print(f"kernel_probe: no reply from {HOST}:{PORT}; is this an NG port?")
    sys.exit(3)

reply = call(sock, "statistics")
if reply is None:
    print("kernel_probe: rtpengine answered ping but not statistics")
    sys.exit(3)
if reply.get("result") == "error":
    print(f"kernel_probe: statistics refused: {reply.get('error-reason')!r}")
    sys.exit(3)

stats = reply.get("statistics") or {}
totals = stats.get("totalstatistics") or {}
current = stats.get("currentstatistics") or {}

version = call(sock, "version")
if isinstance(version, dict) and version.get("result") != "error":
    print(f"kernel_probe: version -> {version.get('version', version)!r}")
else:
    reason = (version or {}).get("error-reason", "no reply")
    print(f"kernel_probe: this rtpengine has no NG version command ({reason!r});"
          " read the version from the process or the package on the host")

relayed = number(totals, "relayedpackets")
kernel_total = number(totals, "relayedpackets_kernel")
user_total = number(totals, "relayedpackets_user")
kernel_rate = number(current, "packetrate_kernel") or 0
user_rate = number(current, "packetrate_user") or 0
media_kernel = number(current, "media_kernel") or 0
media_user = number(current, "media_userspace") or 0
media_mixed = number(current, "media_mixed") or 0
transcoding = number(current, "transcodedmedia") or 0
sessions = number(current, "sessionstotal") or 0
uptime = number(totals, "uptime")

print(f"kernel_probe: uptime {uptime}s, sessions now {sessions},"
      f" transcoding media now {transcoding}")
print(f"kernel_probe: relayed packets total={relayed} kernel={kernel_total}"
      f" userspace={user_total}")
print(f"kernel_probe: packets/s now kernel={kernel_rate} userspace={user_rate};"
      f" media now kernel={media_kernel} userspace={media_user}"
      f" mixed={media_mixed}")

for chain in stats.get("transcoders") or []:
    if isinstance(chain, dict) and (number(chain, "packets") or 0) > 0:
        print(f"kernel_probe: transcoding {chain.get('chain')!r},"
              f" {number(chain, 'packets')} packets"
              " (a transcoded stream cannot be kernel-forwarded)")

if kernel_total is None and "packetrate_kernel" not in current:
    print("kernel_probe: VERDICT cannot tell -- this rtpengine's statistics"
          " reply carries no kernel/userspace split")
    sys.exit(2)

if kernel_rate > 0 or media_kernel > 0 or media_mixed > 0:
    print("kernel_probe: VERDICT the kernel module IS forwarding media right now")
    sys.exit(0)

if (kernel_total or 0) > 0:
    print("kernel_probe: VERDICT the kernel module HAS forwarded media on this"
          " node, but nothing is in the kernel at this instant")
    sys.exit(0)

if not relayed and not user_total:
    print("kernel_probe: VERDICT cannot tell -- this node has not relayed a"
          " single packet yet; put a call through it and probe again")
    sys.exit(2)

print("kernel_probe: VERDICT the kernel module is NOT in play -- every packet"
      " this node relayed went through userspace")
print("kernel_probe: check for --table=-1, a module that never loaded, or"
      " sessions that all transcode")
sys.exit(1)
PY
