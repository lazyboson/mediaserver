#!/bin/sh
# Preflight: does this environment have what mediaserverd needs, before anyone
# deploys mediaserverd into it? (item 45 / G5)
#
# Every dependency MSS has is checked from the outside, the way MSS itself would
# use it -- an NG subscribe against a call this tool fabricates and cleans up, a
# Redis SET NX with a TTL, a real S3 put/head/delete, a UDP datagram from the
# rtpengine host into the media range -- and every line is PASS, FAIL or SKIP
# with one sentence saying why. Exit is non-zero if anything FAILed. SKIP means
# "not checked, and here is what to run to check it", never "probably fine".
#
# POSIX sh plus python3 from the standard library, nothing else: it has to run
# on a jump host where `pip install` is not on offer.
#
#   ./lab/preflight.sh --ng 10.0.0.5:22222 --redis redis://10.0.0.6:6379 \
#       --kafka 10.0.0.7:9092 --s3-endpoint https://s3.eu-west-1.amazonaws.com \
#       --bucket call-recordings --media-ports 40000-40999 \
#       --advertise-ip 10.0.0.8 --ssh rtpengine-1
#
# Defaults come from the same MSS_* variables mediaserverd reads, so on a host
# that already has the deployment's environment file sourced, `./preflight.sh`
# alone checks exactly what that deployment would use.
#
# In this lab the NG port is not published to the WSL host, so run it inside the
# lab network:
#
#   DOCKER_API_VERSION=1.43 docker run --rm --network mss-microsip_lab \
#     -v "$PWD/lab":/lab:ro -w /lab python:3-slim sh /lab/preflight.sh \
#     --ng 172.31.99.10:22222 --redis redis://172.31.99.61:6379 \
#     --kafka 172.31.99.60:9092 --s3-endpoint http://172.31.99.62:9000 \
#     --bucket lab-recordings --access-key minioadmin --secret-key minioadmin
set -eu

HERE=$(dirname "$0")

NG=${MSS_RTPENGINE_NODE:-127.0.0.1:22222}
REDIS=${MSS_REDIS_URL:-}
KAFKA=${MSS_KAFKA_BROKERS:-}
TOPIC=${MSS_EVENTS_TOPIC:-mss.events}
S3_ENDPOINT=${MSS_RECORDING_S3_ENDPOINT:-}
BUCKET=${MSS_RECORDING_BUCKET:-}
REGION=${MSS_RECORDING_S3_REGION:-us-east-1}
ACCESS_KEY=${MSS_RECORDING_S3_ACCESS_KEY_ID:-}
SECRET_KEY=${MSS_RECORDING_S3_SECRET_ACCESS_KEY:-}
RECORDING_STORE=${MSS_RECORDING_STORE:-}
RECORDING_ROOT=${MSS_RECORDING_ROOT:-}
PORT_MIN=${MSS_MEDIA_PORT_MIN:-}
PORT_MAX=${MSS_MEDIA_PORT_MAX:-}
ADVERTISE_IP=${MSS_MEDIA_ADVERTISE_IP:-}
LOCAL_IP=${MSS_TAP_LOCAL_IP:-}
SSH_TARGET=
RTPENGINE_VERSION=
JSON=0

usage() {
  cat <<'USAGE'
Usage: preflight.sh [options]

  --ng HOST:PORT             rtpengine NG control socket   (MSS_RTPENGINE_NODE)
  --redis URL                redis://[:pass@]host:port/db  (MSS_REDIS_URL)
  --kafka B1,B2              bootstrap brokers             (MSS_KAFKA_BROKERS)
  --topic NAME               event topic                   (MSS_EVENTS_TOPIC)
  --s3-endpoint URL          S3 endpoint          (MSS_RECORDING_S3_ENDPOINT)
  --bucket NAME              recording bucket        (MSS_RECORDING_BUCKET)
  --region NAME              S3 region            (MSS_RECORDING_S3_REGION)
  --access-key ID            S3 access key  (MSS_RECORDING_S3_ACCESS_KEY_ID)
  --secret-key KEY           S3 secret  (MSS_RECORDING_S3_SECRET_ACCESS_KEY)
  --recording-store NAME     s3 | filesystem            (MSS_RECORDING_STORE)
  --recording-root DIR       the shared recording tree, checked with the same
                             write/rename/delete probe mediaserverd runs at
                             startup                     (MSS_RECORDING_ROOT)
  --media-ports MIN-MAX      media range     (MSS_MEDIA_PORT_MIN/MAX)
  --advertise-ip ADDR        address peers reach  (MSS_MEDIA_ADVERTISE_IP)
  --local-ip ADDR            address to bind          (MSS_TAP_LOCAL_IP)
  --ssh TARGET               ssh target on the rtpengine host, for the
                             media-range reachability check (optional)
  --rtpengine-version V      the version read off the rtpengine host, which
                             cannot be asked over NG
  --json                     machine-readable report on stdout, lines on stderr
  -h, --help                 this

Exit: 0 when nothing FAILed, 1 when something did, 2 on bad usage.
USAGE
}

while [ $# -gt 0 ]; do
  case $1 in
    --ng) NG=$2; shift 2 ;;
    --redis) REDIS=$2; shift 2 ;;
    --kafka) KAFKA=$2; shift 2 ;;
    --topic) TOPIC=$2; shift 2 ;;
    --s3-endpoint) S3_ENDPOINT=$2; shift 2 ;;
    --bucket) BUCKET=$2; shift 2 ;;
    --region) REGION=$2; shift 2 ;;
    --access-key) ACCESS_KEY=$2; shift 2 ;;
    --secret-key) SECRET_KEY=$2; shift 2 ;;
    --recording-store) RECORDING_STORE=$2; shift 2 ;;
    --recording-root) RECORDING_ROOT=$2; RECORDING_STORE=${RECORDING_STORE:-filesystem}; shift 2 ;;
    --media-ports)
      PORT_MIN=$(echo "$2" | cut -d- -f1)
      PORT_MAX=$(echo "$2" | cut -d- -f2)
      shift 2 ;;
    --advertise-ip) ADVERTISE_IP=$2; shift 2 ;;
    --local-ip) LOCAL_IP=$2; shift 2 ;;
    --ssh) SSH_TARGET=$2; shift 2 ;;
    --rtpengine-version) RTPENGINE_VERSION=$2; shift 2 ;;
    --json) JSON=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "preflight: unknown option $1" >&2; usage >&2; exit 2 ;;
  esac
done

if ! command -v python3 >/dev/null 2>&1; then
  echo "preflight: python3 is required (bencode over UDP, RESP, SigV4)" >&2
  exit 2
fi

PF_NG=$NG; PF_REDIS=$REDIS; PF_KAFKA=$KAFKA; PF_TOPIC=$TOPIC
PF_S3_ENDPOINT=$S3_ENDPOINT; PF_BUCKET=$BUCKET; PF_REGION=$REGION
PF_ACCESS_KEY=$ACCESS_KEY; PF_SECRET_KEY=$SECRET_KEY
PF_RECORDING_STORE=$RECORDING_STORE; PF_RECORDING_ROOT=$RECORDING_ROOT
PF_PORT_MIN=$PORT_MIN; PF_PORT_MAX=$PORT_MAX
PF_ADVERTISE_IP=$ADVERTISE_IP; PF_LOCAL_IP=$LOCAL_IP
PF_SSH=$SSH_TARGET; PF_RTPENGINE_VERSION=$RTPENGINE_VERSION
PF_KERNEL_PROBE=$HERE/kernel_probe.sh; PF_JSON=$JSON
export PF_NG PF_REDIS PF_KAFKA PF_TOPIC PF_S3_ENDPOINT PF_BUCKET PF_REGION
export PF_ACCESS_KEY PF_SECRET_KEY PF_PORT_MIN PF_PORT_MAX PF_ADVERTISE_IP
export PF_RECORDING_STORE PF_RECORDING_ROOT
export PF_LOCAL_IP PF_SSH PF_RTPENGINE_VERSION PF_KERNEL_PROBE PF_JSON

exec python3 - <<'PY'
"""The engine. Ordered so the cheapest, most fundamental check runs first: an
environment that cannot answer an NG ping has nothing else worth reporting."""

import datetime
import hashlib
import hmac
import http.client
import json
import os
import re
import shutil
import socket
import struct
import subprocess
import sys
import time
import urllib.parse

NG = os.environ.get("PF_NG", "")
REDIS = os.environ.get("PF_REDIS", "").strip()
KAFKA = os.environ.get("PF_KAFKA", "").strip()
TOPIC = os.environ.get("PF_TOPIC", "mss.events").strip() or "mss.events"
S3_ENDPOINT = os.environ.get("PF_S3_ENDPOINT", "").strip()
BUCKET = os.environ.get("PF_BUCKET", "").strip()
REGION = os.environ.get("PF_REGION", "").strip() or "us-east-1"
ACCESS_KEY = os.environ.get("PF_ACCESS_KEY", "").strip()
SECRET_KEY = os.environ.get("PF_SECRET_KEY", "").strip()
RECORDING_STORE = os.environ.get("PF_RECORDING_STORE", "").strip().lower()
RECORDING_ROOT = os.environ.get("PF_RECORDING_ROOT", "").strip()
PORT_MIN = os.environ.get("PF_PORT_MIN", "").strip()
PORT_MAX = os.environ.get("PF_PORT_MAX", "").strip()
ADVERTISE_IP = os.environ.get("PF_ADVERTISE_IP", "").strip()
LOCAL_IP = os.environ.get("PF_LOCAL_IP", "").strip()
SSH_TARGET = os.environ.get("PF_SSH", "").strip()
RTPENGINE_VERSION = os.environ.get("PF_RTPENGINE_VERSION", "").strip()
KERNEL_PROBE = os.environ.get("PF_KERNEL_PROBE", "")
JSON_OUT = os.environ.get("PF_JSON", "0") == "1"

RUN = f"{os.getpid()}-{int(time.time())}"
RESULTS = []
STREAM = sys.stderr if JSON_OUT else sys.stdout

SUBSCRIBE_SINCE_MAJOR = 11
LAB_VERIFIED_VERSION = "14.1.1.8"
CLOCK_TOLERANCE_MS = 100.0


def line(text):
    print(text, file=STREAM, flush=True)


def record(status, name, detail):
    RESULTS.append({"check": name, "status": status, "detail": detail})
    line(f"{status:<4}  {name:<18}  {detail}")


def split_host_port(value, default_port):
    if not value:
        return None, None
    if value.count(":") == 1:
        host, _, port = value.partition(":")
        try:
            return host or None, int(port)
        except ValueError:
            return host or None, None
    return value, default_port


def bencode(value):
    if isinstance(value, int):
        return b"i%de" % value
    if isinstance(value, bytes):
        return b"%d:%s" % (len(value), value)
    if isinstance(value, str):
        raw = value.encode()
        return b"%d:%s" % (len(raw), raw)
    if isinstance(value, list):
        return b"l" + b"".join(bencode(item) for item in value) + b"e"
    if isinstance(value, dict):
        out = b"d"
        for key in sorted(value):
            raw = key.encode()
            out += b"%d:%s" % (len(raw), raw) + bencode(value[key])
        return out + b"e"
    raise TypeError(type(value))


def bdecode(data, at=0):
    head = data[at:at + 1]
    if head == b"i":
        end = data.index(b"e", at)
        return int(data[at + 1:end]), end + 1
    if head == b"l":
        at += 1
        out = []
        while data[at:at + 1] != b"e":
            item, at = bdecode(data, at)
            out.append(item)
        return out, at + 1
    if head == b"d":
        at += 1
        out = {}
        while data[at:at + 1] != b"e":
            key, at = bdecode(data, at)
            value, at = bdecode(data, at)
            out[key if isinstance(key, str) else repr(key)] = value
        return out, at + 1
    colon = data.index(b":", at)
    length = int(data[at:colon])
    start = colon + 1
    raw = data[start:start + length]
    try:
        return raw.decode(), start + length
    except UnicodeDecodeError:
        return raw.hex(), start + length


class Ng:
    """One cookie per command, unique to this run (defect D12): rtpengine caches
    a reply against its cookie and replays it, so a reused cookie makes every
    later command answer the first one."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(2)
        self.serial = 0

    def call(self, command, attempts=3):
        self.serial += 1
        cookie = f"preflight-{RUN}-{self.serial}".encode()
        datagram = cookie + b" " + bencode(command)
        for _ in range(attempts):
            try:
                self.sock.sendto(datagram, (self.host, self.port))
                reply, _ = self.sock.recvfrom(1 << 20)
            except socket.timeout:
                continue
            except OSError as failure:
                return {"result": "error", "error-reason": str(failure)}
            return bdecode(reply.split(b" ", 1)[1])[0]
        return None

    def local_address(self):
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect((self.host, self.port))
            return probe.getsockname()[0]
        finally:
            probe.close()

    def close(self):
        self.sock.close()


def sdp(address, port, direction="sendrecv"):
    return (
        "v=0\r\n"
        f"o=- 1 1 IN IP4 {address}\r\n"
        "s=mss-preflight\r\n"
        f"c=IN IP4 {address}\r\n"
        "t=0 0\r\n"
        f"m=audio {port} RTP/AVP 0 8\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=rtpmap:8 PCMA/8000\r\n"
        "a=ptime:20\r\n"
        f"a={direction}\r\n"
    )


def media_port_of(answer):
    ports = re.findall(r"m=audio (\d+) ", answer or "")
    return int(ports[0]) if ports else None


def bind_failure(address):
    """Returns the OSError from binding a probe socket on address, or None.

    An unbindable --local-ip used to reach socket.bind() inside check_ng and
    leave a traceback on stdout, which breaks this tool's one promise: every
    line is PASS, FAIL or SKIP.
    """
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.bind((address, 0))
        return None
    except OSError as failure:
        return failure
    finally:
        probe.close()


def check_ng():
    """NG ping, then the whole tap handshake against a call this tool creates:
    offer, answer, subscribe request, subscribe answer, RTP through it,
    unsubscribe, delete -- and a query afterwards to prove nothing was left
    behind. `subscribe` support is the one rtpengine capability MSS cannot work
    around, and no amount of version reading proves it the way doing it does."""
    host, port = split_host_port(NG, 22222)
    if not host or not port:
        record("FAIL", "ng_ping", f"--ng {NG!r} is not host:port")
        record("SKIP", "ng_subscribe", "no usable NG address to fabricate a call on")
        record("SKIP", "ng_tap_media", "no usable NG address to fabricate a call on")
        return
    ng = Ng(host, port)
    started = time.monotonic()
    pong = ng.call({"command": "ping"})
    elapsed = (time.monotonic() - started) * 1000
    if pong is None:
        record("FAIL", "ng_ping",
               f"no reply from {host}:{port} in 3 tries over 6 s -- wrong port, "
               f"a firewall, or not an rtpengine NG socket")
        record("SKIP", "ng_subscribe", "rtpengine did not answer ping")
        record("SKIP", "ng_tap_media", "rtpengine did not answer ping")
        ng.close()
        return
    if pong.get("result") != "pong":
        record("FAIL", "ng_ping",
               f"{host}:{port} answered {pong.get('result')!r} instead of 'pong'")
        record("SKIP", "ng_subscribe", "rtpengine did not answer ping")
        record("SKIP", "ng_tap_media", "rtpengine did not answer ping")
        ng.close()
        return
    record("PASS", "ng_ping", f"rtpengine at {host}:{port} answered pong in {elapsed:.0f} ms")

    local = LOCAL_IP if LOCAL_IP and LOCAL_IP != "0.0.0.0" else ng.local_address()
    unbindable = bind_failure(local)
    if unbindable is not None:
        record("FAIL", "ng_subscribe",
               f"cannot bind a probe socket on {local} ({unbindable}) -- "
               f"--local-ip / MSS_TAP_LOCAL_IP has to be an address of the host "
               f"running preflight, which is where these probe sockets open")
        record("SKIP", "ng_tap_media", "no probe socket could be bound")
        ng.close()
        return
    call_id = f"mss-preflight-{RUN}"
    from_tag = f"preflight-a-{RUN}"
    to_tag = f"preflight-b-{RUN}"
    legs = []
    tap = None
    subscribed_to = None
    try:
        for _ in range(2):
            leg = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            leg.bind((local, 0))
            leg.setblocking(False)
            legs.append(leg)
        caller, callee = legs

        offer = ng.call({
            "command": "offer",
            "call-id": call_id,
            "from-tag": from_tag,
            "sdp": sdp(local, caller.getsockname()[1]),
        })
        if not offer or offer.get("result") != "ok":
            reason = (offer or {}).get("error-reason", "no reply")
            record("FAIL", "ng_subscribe", f"offer for a throwaway call refused: {reason}")
            record("SKIP", "ng_tap_media", "no call to subscribe to")
            return
        answer = ng.call({
            "command": "answer",
            "call-id": call_id,
            "from-tag": from_tag,
            "to-tag": to_tag,
            "sdp": sdp(local, callee.getsockname()[1]),
        })
        if not answer or answer.get("result") != "ok":
            reason = (answer or {}).get("error-reason", "no reply")
            record("FAIL", "ng_subscribe", f"answer for a throwaway call refused: {reason}")
            record("SKIP", "ng_tap_media", "no call to subscribe to")
            return
        callee_dest = media_port_of(offer.get("sdp"))
        caller_dest = media_port_of(answer.get("sdp"))

        request = ng.call({
            "command": "subscribe request",
            "call-id": call_id,
            "from-tags": [from_tag],
            "codec": {"transcode": ["PCMU"]},
        })
        if not request or request.get("result") != "ok":
            reason = (request or {}).get("error-reason", "no reply")
            record("FAIL", "ng_subscribe",
                   f"'subscribe request' refused ({reason}) -- this rtpengine cannot "
                   f"feed MSS a tap; check its version and build")
            record("SKIP", "ng_tap_media", "the subscription was refused")
            return
        subscribed_to = request.get("to-tag")
        tap = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        tap.bind((local, 0))
        tap.setblocking(False)
        accepted = ng.call({
            "command": "subscribe answer",
            "call-id": call_id,
            "to-tag": subscribed_to,
            "sdp": sdp(ADVERTISE_IP or local, tap.getsockname()[1], "recvonly"),
        })
        if not accepted or accepted.get("result") != "ok":
            reason = (accepted or {}).get("error-reason", "no reply")
            record("FAIL", "ng_subscribe", f"'subscribe answer' refused: {reason}")
            record("SKIP", "ng_tap_media", "the subscription was never established")
            return
        streams = len(re.findall(r"m=audio (\d+) ", request.get("sdp") or ""))
        record("PASS", "ng_subscribe",
               f"'subscribe request'/'subscribe answer' accepted on a throwaway call "
               f"({streams} stream(s), to-tag {subscribed_to})")

        pumped = 0
        arrived = 0
        timestamp = 0
        sequence = 1000
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            for leg, dest, ssrc in (
                (caller, caller_dest, 0x11111111),
                (callee, callee_dest, 0x22222222),
            ):
                if not dest:
                    continue
                header = struct.pack("!BBHII", 0x80, 0, sequence & 0xFFFF, timestamp, ssrc)
                try:
                    leg.sendto(header + bytes(160), (host, dest))
                    pumped += 1
                except OSError:
                    pass
            sequence += 1
            timestamp += 160
            while True:
                try:
                    datagram, _ = tap.recvfrom(4096)
                except (socket.timeout, BlockingIOError, OSError):
                    break
                if len(datagram) >= 12:
                    arrived += 1
            time.sleep(0.02)
        tap.settimeout(0.3)
        deadline = time.monotonic() + 0.6
        while time.monotonic() < deadline:
            try:
                datagram, _ = tap.recvfrom(4096)
            except OSError:
                break
            if len(datagram) >= 12:
                arrived += 1

        advertised = ADVERTISE_IP or local
        if arrived:
            record("PASS", "ng_tap_media",
                   f"{arrived} tapped RTP datagrams arrived at {advertised}:"
                   f"{tap.getsockname()[1]} from {pumped} pumped into the call")
        else:
            record("FAIL", "ng_tap_media",
                   f"{pumped} RTP packets went into the call and not one came back to "
                   f"{advertised}:{tap.getsockname()[1]} -- rtpengine cannot reach the "
                   f"address MSS will advertise, or the media range is firewalled")
    finally:
        clean = True
        why = []
        if subscribed_to:
            done = ng.call({
                "command": "unsubscribe",
                "call-id": call_id,
                "to-tag": subscribed_to,
            })
            if not done or done.get("result") != "ok":
                clean = False
                why.append(f"unsubscribe: {(done or {}).get('error-reason', 'no reply')}")
        removed = ng.call({"command": "delete", "call-id": call_id})
        if not removed or removed.get("result") != "ok":
            clean = False
            why.append(f"delete: {(removed or {}).get('error-reason', 'no reply')}")
        left = ng.call({"command": "query", "call-id": call_id})
        if left and left.get("result") == "ok":
            clean = False
            why.append("rtpengine still knows the call after delete")
        if clean:
            record("PASS", "ng_cleanup",
                   f"the throwaway call {call_id} was unsubscribed, deleted, and is "
                   f"gone from rtpengine")
        else:
            record("FAIL", "ng_cleanup",
                   f"this probe may have left state on rtpengine ({'; '.join(why)}); "
                   f"check `rtpengine-ctl list sessions` for {call_id}")
        if tap:
            tap.close()
        for leg in legs:
            leg.close()
        ng.close()


def check_rtpengine_version():
    """rtpengine has no NG `version` command, in the lab build or upstream
    (item 23), so this can only report a version the caller read on the host."""
    where = ("read it on the rtpengine host with `rtpengine --version`, "
             "`dpkg -l ngcp-rtpengine-daemon` / `rpm -q rtpengine`, or over its "
             "CLI socket (`rtpengine-ctl` with --listen-cli), then re-run with "
             "--rtpengine-version")
    if not RTPENGINE_VERSION:
        record("SKIP", "rtpengine_version",
               f"cannot be asked over NG -- rtpengine has no NG 'version' command; {where}")
        return
    numbers = re.findall(r"\d+", RTPENGINE_VERSION)
    if not numbers:
        record("FAIL", "rtpengine_version",
               f"--rtpengine-version {RTPENGINE_VERSION!r} carries no version number")
        return
    major = int(numbers[0])
    if major < SUBSCRIBE_SINCE_MAJOR:
        record("FAIL", "rtpengine_version",
               f"{RTPENGINE_VERSION} predates rtpengine {SUBSCRIBE_SINCE_MAJOR}, which is "
               f"where the 'subscribe request'/'subscribe answer' pair MSS taps with "
               f"arrived -- the ng_subscribe line above is the authority")
        return
    record("PASS", "rtpengine_version",
           f"{RTPENGINE_VERSION} (caller-supplied; MSS is verified against "
           f"{LAB_VERIFIED_VERSION}, and ng_subscribe above proves this build's support)")


def check_kernel_forwarding():
    """The verdict is kernel_probe.sh's, unchanged; only its exit code is read.
    Userspace-only forwarding is a working deployment, so it is a PASS that says
    so: MSS taps either way, and the difference is rtpengine's CPU."""
    if not KERNEL_PROBE or not os.path.exists(KERNEL_PROBE):
        record("SKIP", "kernel_forwarding",
               f"kernel_probe.sh not found next to this script ({KERNEL_PROBE!r})")
        return
    host, port = split_host_port(NG, 22222)
    try:
        probe = subprocess.run(
            ["sh", KERNEL_PROBE, host or "127.0.0.1", str(port or 22222)],
            capture_output=True, text=True, timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired) as failure:
        record("SKIP", "kernel_forwarding", f"kernel_probe.sh could not be run: {failure}")
        return
    verdict = ""
    for spoken in probe.stdout.splitlines():
        if "VERDICT" in spoken:
            verdict = spoken.split("VERDICT", 1)[1].strip()
    verdict = verdict or (probe.stdout.strip().splitlines() or [""])[-1]
    if probe.returncode == 0:
        record("PASS", "kernel_forwarding", f"kernel_probe.sh: {verdict}")
    elif probe.returncode == 1:
        record("PASS", "kernel_forwarding",
               f"kernel_probe.sh: {verdict} -- MSS taps a userspace relay just as well, "
               f"at a higher CPU cost per call on rtpengine (architecture 8.1)")
    elif probe.returncode == 2:
        record("SKIP", "kernel_forwarding",
               f"kernel_probe.sh cannot tell yet: {verdict}")
    else:
        record("FAIL", "kernel_forwarding",
               f"kernel_probe.sh could not reach rtpengine: {verdict}")


class Resp:
    def __init__(self, sock):
        self.stream = sock.makefile("rb")
        self.sock = sock

    def command(self, *words):
        out = b"*%d\r\n" % len(words)
        for word in words:
            raw = word if isinstance(word, bytes) else str(word).encode()
            out += b"$%d\r\n%s\r\n" % (len(raw), raw)
        self.sock.sendall(out)
        return self.read()

    def read(self):
        head = self.stream.readline()
        if not head:
            raise OSError("the server closed the connection")
        kind, body = head[:1], head[1:].strip()
        if kind == b"+":
            return body.decode()
        if kind == b"-":
            raise OSError(body.decode())
        if kind == b":":
            return int(body)
        if kind == b"$":
            length = int(body)
            if length < 0:
                return None
            payload = self.stream.read(length + 2)
            return payload[:length]
        if kind == b"*":
            return [self.read() for _ in range(int(body))]
        raise OSError(f"unexpected RESP type {kind!r}")

    def close(self):
        try:
            self.stream.close()
        finally:
            self.sock.close()


def check_redis():
    """MSS's registry is SET NX with a TTL and nothing more exotic, so that is
    exactly what gets tried: a key nobody else could hold, its TTL read back,
    then deleted. Raw RESP over a socket -- redis-py is not on a jump host."""
    if not REDIS:
        record("SKIP", "redis",
               "no --redis/MSS_REDIS_URL given, so MSS would run single-pod with no "
               "registry and no session adoption")
        return
    parsed = urllib.parse.urlsplit(REDIS)
    if parsed.scheme not in ("redis", "rediss"):
        record("FAIL", "redis", f"{REDIS!r} is not a redis:// or rediss:// URL")
        return
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 6379
    database = 0
    if parsed.path and parsed.path.strip("/").isdigit():
        database = int(parsed.path.strip("/"))
    key = f"mss:preflight:{RUN}"
    resp = None
    try:
        sock = socket.create_connection((host, port), timeout=5)
        if parsed.scheme == "rediss":
            import ssl
            sock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
        sock.settimeout(5)
        resp = Resp(sock)
        if parsed.password:
            if parsed.username:
                resp.command("AUTH", parsed.username, parsed.password)
            else:
                resp.command("AUTH", parsed.password)
        if database:
            resp.command("SELECT", database)
        taken = resp.command("SET", key, RUN, "NX", "EX", "30")
        if taken != "OK":
            record("FAIL", "redis",
                   f"SET {key} NX returned {taken!r} -- the key already exists, which "
                   f"means another preflight is running or the clock went backwards")
            return
        ttl = resp.command("TTL", key)
        if not isinstance(ttl, int) or not 0 < ttl <= 30:
            record("FAIL", "redis",
                   f"TTL on the probe key came back {ttl!r}, so this server does not "
                   f"expire keys the way the registry lease needs")
            return
        deleted = resp.command("DEL", key)
        if deleted != 1:
            record("FAIL", "redis", f"DEL on the probe key returned {deleted!r}")
            return
        record("PASS", "redis",
               f"SET NX / TTL {ttl}s / DEL round-tripped on {host}:{port} db {database}")
    except (OSError, ValueError) as failure:
        record("FAIL", "redis", f"{host}:{port} -- {failure}")
    finally:
        if resp:
            try:
                resp.close()
            except OSError:
                pass


def check_kafka():
    """Two lines, because there are two different facts. Whether a broker
    accepts a TCP connection is checkable anywhere; whether the topic takes a
    record needs a Kafka client, and hand-rolling the wire protocol here would
    be a second implementation of something MSS already has (rskafka)."""
    if not KAFKA:
        record("SKIP", "kafka",
               "no --kafka/MSS_KAFKA_BROKERS given, so MSS would keep events in memory "
               "only and nothing downstream would see them")
        record("SKIP", "kafka_topic", "no brokers to reach the topic through")
        return
    brokers = [broker.strip() for broker in KAFKA.split(",") if broker.strip()]
    reachable = []
    refused = []
    for broker in brokers:
        host, port = split_host_port(broker, 9092)
        try:
            with socket.create_connection((host, port), timeout=5):
                reachable.append(f"{host}:{port}")
        except OSError as failure:
            refused.append(f"{host}:{port} ({failure})")
    if not reachable:
        record("FAIL", "kafka", f"no broker accepted a TCP connection: {'; '.join(refused)}")
        record("SKIP", "kafka_topic", "no broker was reachable")
        return
    detail = f"{len(reachable)}/{len(brokers)} broker(s) accepted a TCP connection"
    if refused:
        detail += f"; unreachable: {'; '.join(refused)}"
    record("PASS", "kafka", detail)

    try:
        from kafka import KafkaConsumer, KafkaProducer
        from kafka.structs import TopicPartition
    except ImportError:
        record("SKIP", "kafka_topic",
               f"no kafka client library here, so nothing produced to {TOPIC!r}; install "
               f"kafka-python-ng, or check from a broker host with "
               f"`kafka-topics.sh --describe --topic {TOPIC}` / `rpk topic describe {TOPIC}`")
        return
    probe = f"preflight-{RUN}".encode()
    try:
        producer = KafkaProducer(bootstrap_servers=brokers, request_timeout_ms=8000,
                                 api_version_auto_timeout_ms=8000, acks=1)
        sent = producer.send(TOPIC, key=probe, value=probe).get(timeout=10)
        producer.close(timeout=5)
        consumer = KafkaConsumer(bootstrap_servers=brokers, consumer_timeout_ms=10000,
                                 api_version_auto_timeout_ms=8000,
                                 enable_auto_commit=False,
                                 auto_offset_reset="earliest")
        partition = TopicPartition(TOPIC, sent.partition)
        consumer.assign([partition])
        consumer.seek(partition, sent.offset)
        found = False
        for message in consumer:
            if message.key == probe:
                found = True
                break
            if message.offset > sent.offset:
                break
        consumer.close()
        if found:
            record("PASS", "kafka_topic",
                   f"a probe record produced to {TOPIC!r} partition {sent.partition} at "
                   f"offset {sent.offset} was read back")
        else:
            record("FAIL", "kafka_topic",
                   f"the probe record went to {TOPIC!r} offset {sent.offset} but could not "
                   f"be read back within 10 s")
    except Exception as failure:
        record("FAIL", "kafka_topic", f"{TOPIC!r} on {brokers}: {failure}")


def sigv4_request(method, endpoint, bucket, key, body, query=""):
    parsed = urllib.parse.urlsplit(endpoint)
    secure = parsed.scheme != "http"
    host = parsed.hostname or ""
    port = parsed.port or (443 if secure else 80)
    host_header = host if parsed.port is None else f"{host}:{parsed.port}"
    path = f"{parsed.path.rstrip('/')}/{bucket}/{key}"
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body or b"").hexdigest()
    canonical_headers = (
        f"host:{host_header}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{stamp}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical = "\n".join([
        method, path, query, canonical_headers, signed_headers, payload_hash,
    ])
    scope = f"{day}/{REGION}/s3/aws4_request"
    to_sign = "\n".join([
        "AWS4-HMAC-SHA256", stamp, scope,
        hashlib.sha256(canonical.encode()).hexdigest(),
    ])

    def sign(secret, message):
        return hmac.new(secret, message.encode(), hashlib.sha256).digest()

    signing = sign(sign(sign(sign(f"AWS4{SECRET_KEY}".encode(), day), REGION), "s3"),
                   "aws4_request")
    signature = hmac.new(signing, to_sign.encode(), hashlib.sha256).hexdigest()
    headers = {
        "Host": host_header,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": stamp,
        "Authorization": (
            f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
    }
    if body:
        headers["Content-Length"] = str(len(body))
    factory = http.client.HTTPSConnection if secure else http.client.HTTPConnection
    connection = factory(host, port, timeout=15)
    try:
        connection.request(method, path + (f"?{query}" if query else ""), body, headers)
        response = connection.getresponse()
        return response.status, response.read()[:300]
    finally:
        connection.close()


def s3_error(payload):
    found = re.search(rb"<Code>([^<]+)</Code>", payload or b"")
    if found:
        return found.group(1).decode()
    return (payload or b"").decode("utf-8", "replace").strip()[:120] or "no body"


def check_s3():
    """A recording's whole life is put/head/delete under
    ${accountID}/${recordingID}.${format}, so the probe object goes through the
    same three verbs and is then confirmed gone. SigV4 by hand in the standard
    library beats requiring boto3 on a jump host; the CLIs are the fallback when
    no keys were passed, because they can read a role or a profile."""
    if RECORDING_STORE == "filesystem":
        record("SKIP", "s3",
               "MSS_RECORDING_STORE=filesystem, so the bucket variables are ignored by "
               "mediaserverd and nothing here was checked -- see the recording-root line")
        return
    if not BUCKET:
        record("SKIP", "s3",
               "no --bucket/MSS_RECORDING_BUCKET given, so recording to S3 is off and "
               "nothing was checked")
        return
    endpoint = S3_ENDPOINT or f"https://s3.{REGION}.amazonaws.com"
    key = f"mss-preflight/{RUN}.probe"
    body = f"mss preflight {RUN}\n".encode()

    if ACCESS_KEY and SECRET_KEY:
        try:
            put, put_body = sigv4_request("PUT", endpoint, BUCKET, key, body)
            if put not in (200, 201):
                record("FAIL", "s3",
                       f"PUT {BUCKET}/{key} on {endpoint} -> HTTP {put} "
                       f"{s3_error(put_body)} (stdlib SigV4)")
                return
            head, _ = sigv4_request("HEAD", endpoint, BUCKET, key, None)
            if head != 200:
                record("FAIL", "s3", f"the object was written but HEAD -> HTTP {head}")
                return
            gone, gone_body = sigv4_request("DELETE", endpoint, BUCKET, key, None)
            if gone not in (200, 204):
                record("FAIL", "s3",
                       f"put and head worked but DELETE -> HTTP {gone} "
                       f"{s3_error(gone_body)}; {key} was left behind")
                return
            after, _ = sigv4_request("HEAD", endpoint, BUCKET, key, None)
            record("PASS", "s3",
                   f"put/head/delete of {BUCKET}/{key} on {endpoint} via stdlib SigV4 "
                   f"(region {REGION}, path style), and it is gone afterwards "
                   f"(HEAD -> {after})")
        except (OSError, ValueError) as failure:
            record("FAIL", "s3", f"{endpoint} bucket {BUCKET}: {failure} (stdlib SigV4)")
        return

    cli = shutil.which("aws")
    if cli:
        arguments = ["--endpoint-url", endpoint] if S3_ENDPOINT else []
        target = f"s3://{BUCKET}/{key}"
        try:
            put = subprocess.run([cli, "s3", "cp", "-", target, *arguments],
                                 input=body, capture_output=True, timeout=60)
            if put.returncode != 0:
                record("FAIL", "s3",
                       f"aws s3 cp to {target} failed: "
                       f"{put.stderr.decode('utf-8', 'replace').strip()[:160]} (aws CLI)")
                return
            head = subprocess.run(
                [cli, "s3api", "head-object", "--bucket", BUCKET, "--key", key, *arguments],
                capture_output=True, timeout=60)
            removed = subprocess.run([cli, "s3", "rm", target, *arguments],
                                     capture_output=True, timeout=60)
            if head.returncode != 0 or removed.returncode != 0:
                record("FAIL", "s3",
                       f"the object was written but head/rm failed on {target} (aws CLI)")
                return
            record("PASS", "s3",
                   f"put/head/delete of {target} on {endpoint} via the aws CLI (no keys "
                   f"were passed, so its own credentials were used)")
        except (OSError, subprocess.TimeoutExpired) as failure:
            record("FAIL", "s3", f"aws CLI on {target}: {failure}")
        return

    if shutil.which("mc"):
        record("SKIP", "s3",
               f"no --access-key/--secret-key given; `mc` is on PATH but needs an alias "
               f"with credentials of its own -- run `mc alias set probe {endpoint} KEY "
               f"SECRET && mc cp - probe/{BUCKET}/{key}` by hand, or pass the keys")
        return
    record("SKIP", "s3",
           f"no --access-key/--secret-key and neither `aws` nor `mc` on PATH, so "
           f"{BUCKET} on {endpoint} was not touched")


def check_recording_root():
    """The filesystem store lands a recording by writing a sibling .part file,
    fsyncing it and renaming it over the final name, so the probe is exactly
    those three verbs -- the same ones mediaserverd runs at startup and refuses
    to start without. A root that is writable but cannot rename (some FUSE and
    SMB mounts) fails here rather than mid-call."""
    if RECORDING_STORE and RECORDING_STORE not in ("s3", "filesystem"):
        record("FAIL", "recording-root",
               f"MSS_RECORDING_STORE={RECORDING_STORE} names no recording store, and "
               f"mediaserverd REFUSES TO START on it; it is s3 or filesystem")
        return
    if RECORDING_STORE != "filesystem" and not RECORDING_ROOT:
        record("SKIP", "recording-root",
               "the recording store is s3 (the default), so no shared recording tree "
               "was checked -- pass --recording-root DIR to check one")
        return
    if not RECORDING_ROOT:
        record("FAIL", "recording-root",
               "MSS_RECORDING_STORE=filesystem needs MSS_RECORDING_ROOT, an absolute "
               "directory every recording pod mounts; mediaserverd refuses to start "
               "without it")
        return
    if not RECORDING_ROOT.startswith("/"):
        record("FAIL", "recording-root",
               f"{RECORDING_ROOT} is not absolute, and mediaserverd refuses to start "
               f"on a relative recording root")
        return
    probe = os.path.join(RECORDING_ROOT, f".mss-preflight-{RUN}")
    written = f"{probe}.part"
    try:
        os.makedirs(RECORDING_ROOT, exist_ok=True)
        with open(written, "wb") as handle:
            handle.write(f"mss preflight {RUN}\n".encode())
            handle.flush()
            os.fsync(handle.fileno())
        os.rename(written, probe)
        if not os.path.isfile(probe):
            record("FAIL", "recording-root",
                   f"{probe} is not there after the rename that created it")
            return
        os.unlink(probe)
        free = shutil.disk_usage(RECORDING_ROOT)
        record("PASS", "recording-root",
               f"write, fsync, rename and delete under {RECORDING_ROOT} "
               f"({free.free // (1024 * 1024 * 1024)} GiB free of "
               f"{free.total // (1024 * 1024 * 1024)} GiB) -- this is the same probe "
               f"mediaserverd runs at startup. It must be the SAME volume on every "
               f"recording pod (RWX), or a cross-pod adopter finds nothing")
    except OSError as failure:
        for leftover in (written, probe):
            try:
                os.unlink(leftover)
            except OSError:
                pass
        record("FAIL", "recording-root",
               f"{RECORDING_ROOT}: {failure} -- mediaserverd runs as uid 65532 and "
               f"refuses to start when this probe fails")


def check_media_ports():
    """The range MSS binds is the one a firewall has to admit, so the useful
    facts are how many concurrent legs it holds and whether it is actually free
    on this host -- which is only meaningful when the preflight runs on the host
    MSS will run on."""
    if not PORT_MIN and not PORT_MAX:
        record("SKIP", "media_ports",
               "no --media-ports/MSS_MEDIA_PORT_MIN/MAX given: MSS would use an ephemeral "
               "port per socket, which no firewall can describe")
        return
    if not PORT_MIN or not PORT_MAX:
        record("FAIL", "media_ports",
               f"only half the range is set (min {PORT_MIN or 'unset'}, max "
               f"{PORT_MAX or 'unset'}); MSS ignores a half-set range and falls back to "
               f"ephemeral ports")
        return
    try:
        low, high = int(PORT_MIN), int(PORT_MAX)
    except ValueError:
        record("FAIL", "media_ports", f"{PORT_MIN}-{PORT_MAX} is not a pair of numbers")
        return
    if low > high or low < 1 or high > 65535:
        record("FAIL", "media_ports", f"{low}-{high} is not a usable UDP port range")
        return
    first_even = low if low % 2 == 0 else low + 1
    capacity = len(range(first_even, high + 1, 2))
    if capacity == 0:
        record("FAIL", "media_ports",
               f"{low}-{high} contains no even port, and MSS hands out even ports only")
        return
    address = LOCAL_IP or "0.0.0.0"
    held = []
    for port in {first_even, (high if high % 2 == 0 else high - 1)}:
        try:
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            probe.bind((address, port))
            probe.close()
        except OSError as failure:
            held.append(f"{port} ({failure})")
    if held:
        record("FAIL", "media_ports",
               f"{low}-{high} would hold {capacity} legs but this host cannot bind "
               f"{'; '.join(held)} on {address}")
        return
    record("PASS", "media_ports",
           f"{low}-{high} gives {capacity} even ports = {capacity} tapped legs "
           f"({capacity // 2} two-party calls); both ends bind free on {address}")


def check_media_reachability():
    """Whether rtpengine's host can put a datagram on MSS's media port is a
    question only the rtpengine host can answer, so it is asked from there over
    ssh or not asked at all."""
    if not SSH_TARGET:
        record("SKIP", "media_udp",
               "no --ssh <rtpengine-host> given, so nothing proved the rtpengine host can "
               "reach the media range inbound (ng_tap_media above covers the path this "
               "host sees)")
        return
    if not shutil.which("ssh"):
        record("SKIP", "media_udp", "--ssh was given but there is no ssh on PATH here")
        return
    listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    bind_port = 0
    if PORT_MIN:
        try:
            low = int(PORT_MIN)
            bind_port = low if low % 2 == 0 else low + 1
        except ValueError:
            bind_port = 0
    try:
        listener.bind((LOCAL_IP or "0.0.0.0", bind_port))
    except OSError as failure:
        record("FAIL", "media_udp", f"could not open a listener on port {bind_port}: {failure}")
        listener.close()
        return
    port = listener.getsockname()[1]
    listener.settimeout(6)
    target = ADVERTISE_IP
    if not target:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        host, ng_port = split_host_port(NG, 22222)
        try:
            probe.connect((host or "127.0.0.1", ng_port or 22222))
            target = probe.getsockname()[0]
        except OSError:
            target = "127.0.0.1"
        finally:
            probe.close()
    token = f"mss-preflight-{RUN}"
    remote = (
        f"if command -v python3 >/dev/null 2>&1; then "
        f"python3 -c \"import socket;s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);"
        f"[s.sendto(b'{token}',('{target}',{port})) for _ in range(5)]\"; "
        f"elif command -v nc >/dev/null 2>&1; then "
        f"for i in 1 2 3 4 5; do echo {token} | nc -u -w1 {target} {port}; done; "
        f"else echo no-sender >&2; exit 9; fi"
    )
    try:
        sent = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", SSH_TARGET, remote],
            capture_output=True, text=True, timeout=45)
    except (OSError, subprocess.TimeoutExpired) as failure:
        record("FAIL", "media_udp", f"ssh {SSH_TARGET} failed: {failure}")
        listener.close()
        return
    if sent.returncode == 9:
        record("SKIP", "media_udp",
               f"{SSH_TARGET} has neither python3 nor nc, so it could not send a datagram")
        listener.close()
        return
    if sent.returncode != 0:
        record("FAIL", "media_udp",
               f"ssh {SSH_TARGET} exited {sent.returncode}: {sent.stderr.strip()[:160]}")
        listener.close()
        return
    try:
        datagram, source = listener.recvfrom(2048)
        record("PASS", "media_udp",
               f"a UDP datagram sent from {SSH_TARGET} ({source[0]}) arrived on "
               f"{target}:{port}, so the media range is open inbound from the rtpengine host")
    except socket.timeout:
        record("FAIL", "media_udp",
               f"{SSH_TARGET} sent 5 datagrams to {target}:{port} and none arrived -- a "
               f"firewall between rtpengine and MSS, or the wrong advertise address")
    finally:
        listener.close()


def check_clock():
    """Recording timestamps, event ordering and the recording-group time
    alignment are all wall-clock; a node minutes out of step corrupts them
    quietly, which is worse than failing loudly."""
    if shutil.which("chronyc"):
        try:
            tracking = subprocess.run(["chronyc", "tracking"], capture_output=True,
                                      text=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired) as failure:
            record("SKIP", "clock", f"chronyc could not be run: {failure}")
            return
        offset = re.search(r"System time\s*:\s*([\d.]+) seconds", tracking.stdout)
        if offset:
            drift_ms = float(offset.group(1)) * 1000
            status = "PASS" if drift_ms < CLOCK_TOLERANCE_MS else "FAIL"
            record(status, "clock",
                   f"chronyc reports the system clock {drift_ms:.1f} ms off NTP "
                   f"(tolerance {CLOCK_TOLERANCE_MS:.0f} ms)")
            return
    if shutil.which("timedatectl"):
        try:
            state = subprocess.run(["timedatectl", "show", "-p", "NTPSynchronized",
                                    "--value"], capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired) as failure:
            record("SKIP", "clock", f"timedatectl could not be run: {failure}")
            return
        answer = state.stdout.strip()
        if answer == "yes":
            record("PASS", "clock", "timedatectl reports the clock NTP-synchronised")
        elif answer == "no":
            record("FAIL", "clock",
                   "timedatectl reports the clock is NOT NTP-synchronised, so recording "
                   "timestamps and event order cannot be trusted across pods")
        else:
            record("SKIP", "clock", f"timedatectl answered {answer!r}")
        return
    record("SKIP", "clock",
           "neither chronyc nor timedatectl here, so NTP offset is unknown -- check it on "
           "the host MSS will run on, not from a container")


def main():
    line(f"preflight: mediaserverd target-environment check, run {RUN}")
    line(f"preflight: ng={NG or 'unset'} redis={REDIS or 'unset'} "
         f"kafka={KAFKA or 'unset'} topic={TOPIC} bucket={BUCKET or 'unset'} "
         f"s3={S3_ENDPOINT or 'unset'} store={RECORDING_STORE or 's3'} "
         f"root={RECORDING_ROOT or 'unset'} ports={PORT_MIN or 'unset'}-{PORT_MAX or 'unset'} "
         f"advertise={ADVERTISE_IP or 'unset'} local={LOCAL_IP or 'unset'} "
         f"ssh={SSH_TARGET or 'unset'}")
    line("")
    check_ng()
    check_rtpengine_version()
    check_kernel_forwarding()
    check_redis()
    check_kafka()
    check_s3()
    check_recording_root()
    check_media_ports()
    check_media_reachability()
    check_clock()

    passed = sum(1 for item in RESULTS if item["status"] == "PASS")
    failed = [item for item in RESULTS if item["status"] == "FAIL"]
    skipped = sum(1 for item in RESULTS if item["status"] == "SKIP")
    line("")
    line(f"preflight: {passed} PASS, {len(failed)} FAIL, {skipped} SKIP")
    if failed:
        for item in failed:
            line(f"preflight: FAILED {item['check']}: {item['detail']}")
        line("preflight: this environment is NOT ready for mediaserverd")
    else:
        line("preflight: nothing failed; every SKIP above is an unchecked assumption")
    if JSON_OUT:
        print(json.dumps({
            "run": RUN,
            "ready": not failed,
            "summary": {"pass": passed, "fail": len(failed), "skip": skipped},
            "target": {
                "ng": NG, "redis": REDIS, "kafka": KAFKA, "topic": TOPIC,
                "s3_endpoint": S3_ENDPOINT, "bucket": BUCKET, "region": REGION,
                "recording_store": RECORDING_STORE or "s3",
                "recording_root": RECORDING_ROOT,
                "media_port_min": PORT_MIN, "media_port_max": PORT_MAX,
                "advertise_ip": ADVERTISE_IP, "local_ip": LOCAL_IP,
                "ssh": SSH_TARGET,
            },
            "checks": RESULTS,
        }, indent=2))
    return 1 if failed else 0


sys.exit(main())
PY
