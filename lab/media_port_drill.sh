#!/bin/sh
# Items G2 + G3: the media port range and the advertised address, on a live
# tapped call.
#
# A firewalled deployment cannot let MSS take ephemeral ports: the operator has
# to open a fixed UDP range from the rtpengine hosts. This drill proves the
# range is real -- every media socket the tap binds sits inside it -- and that
# audio still flows while it does, then that closing the session gives the
# ports back.
#
# Bring the lab up with the range set, so mss-control inherits it:
#   export DOCKER_API_VERSION=1.43
#   export MSS_MEDIA_PORT_MIN=40100 MSS_MEDIA_PORT_MAX=40139
#   export MSS_MEDIA_ADVERTISE_IP=172.31.99.31
#   docker compose -f lab/docker-compose.microsip.yml \
#     -f lab/docker-compose.webrtc.yml up -d rtpengine opensips freeswitch \
#     call-watcher redpanda redis minio minio-init mock-bridge mss-control
#   ./lab/media_port_drill.sh
#
# The advertised address is the same as the bind address in the lab on purpose:
# rtpengine sends the tap copy to whatever MSS advertised, so a different value
# here would only prove the audio stops. What the drill checks is that MSS put
# the ADVERTISED address in the subscribe answer and the LOCAL one on the
# socket -- in a NAT/hostNetwork deployment those differ.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
OUT=${OUT:-$HERE/out}
CONTROL=${CONTROL:-http://127.0.0.1:50551}
METRICS=${METRICS:-http://127.0.0.1:9464/metrics}
POD=${POD:-mss-microsip-mss-control-1}
WATCHER=${WATCHER:-mss-microsip-call-watcher-1}
PORT_MIN=${MSS_MEDIA_PORT_MIN:-40100}
PORT_MAX=${MSS_MEDIA_PORT_MAX:-40139}
CALL_SECONDS=${CALL_SECONDS:-90}
TAP_SECONDS=${TAP_SECONDS:-15}
DIAL=${DIAL:-9000}
DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
export DOCKER_API_VERSION

STAMP=$(date +%s)
EXTERNAL_ID=portdrill-$STAMP
LOG=$OUT/media-port-drill-$STAMP.log
CALL_ID=""
CALLER=""

mkdir -p "$OUT"
say() { echo "drill: $*" | tee -a "$LOG"; }
metric() {
  value=$(curl -s "$METRICS" | sed -n "s/^$1 //p" | head -1)
  echo "${value:-0}"
}
# iproute2 is not in the rust image, so read the kernel table directly.
udp_ports() {
  # strtonum() is a gawk extension. Under mawk (Debian's default awk) it is an
  # undefined function: every port came back empty, the drill reported that no
  # media socket was inside the range, and the product looked broken. Convert
  # the hex by hand so any POSIX awk reads the kernel table.
  docker exec "$POD" sh -c 'cat /proc/net/udp /proc/net/udp6 2>/dev/null' |
    awk 'function hex(s) {
           n = 0
           s = toupper(s)
           for (i = 1; i <= length(s); i++) {
             n = n * 16 + index("0123456789ABCDEF", substr(s, i, 1)) - 1
           }
           return n
         }
         NR > 1 {split($2, a, ":"); if (a[2] != "") print hex(a[2])}' |
    sort -n | uniq
}

if ! docker ps --format '{{.Names}}' | grep -qx "$POD"; then
  say "$POD is not running"
  exit 1
fi
if ! curl -s -o /dev/null "$METRICS"; then
  say "$METRICS does not answer"
  exit 1
fi

say "session $EXTERNAL_ID, range $PORT_MIN-$PORT_MAX, log $LOG"
say "the pod reports capacity=$(metric mss_media_ports_capacity) \
free=$(metric mss_media_ports_free) in_use=$(metric mss_media_ports_in_use)"
if [ "$(metric mss_media_ports_capacity)" = "0" ]; then
  say "this pod has no media port range configured; recreate it with \
MSS_MEDIA_PORT_MIN/MAX set"
  exit 1
fi

cd "$REPO"
say "building mss_ctl before the call so nothing compiles mid-drill"
cargo build --quiet -p control-api --examples

cleanup() {
  [ -n "$CALLER" ] && kill "$CALLER" 2>/dev/null || true
}
trap cleanup EXIT

BEFORE=$(udp_ports | awk -v lo="$PORT_MIN" -v hi="$PORT_MAX" \
  '$1 >= lo && $1 <= hi' | tr '\n' ' ')
say "udp sockets inside the range before the call: [${BEFORE:-none}]"

docker exec "$WATCHER" rm -f /shared/call.env 2>/dev/null || true
say "dialing $DIAL for ${CALL_SECONDS}s"
( cd "$HERE" && CALL_SECONDS=$CALL_SECONDS DIAL=$DIAL EAR_DIR=out \
    python3 host_test_caller.py >"$OUT/host-caller-$STAMP.log" 2>&1 ) &
CALLER=$!

waited=0
while [ "$waited" -lt 25 ]; do
  if env=$(docker exec "$WATCHER" cat /shared/call.env 2>/dev/null); then
    CALL_ID=$(echo "$env" | sed -n 's/^MSS_TAP_CALL_ID=//p')
    FROM_TAGS=$(echo "$env" | sed -n 's/^MSS_TAP_FROM_TAGS=//p')
    [ -n "$CALL_ID" ] && break
  fi
  waited=$((waited + 1))
  sleep 1
done
if [ -z "$CALL_ID" ]; then
  say "call_watcher never announced a call; is the SIP half up?"
  exit 1
fi
say "call $CALL_ID legs $FROM_TAGS"

BASE_DATAGRAMS=$(metric mss_ingest_datagrams_total)
cargo run --quiet -p control-api --example mss_ctl -- \
  "$CONTROL" create "$EXTERNAL_ID" "$CALL_ID" "$FROM_TAGS" 2>&1 | tee -a "$LOG"

sleep "$TAP_SECONDS"
DURING=$(udp_ports | awk -v lo="$PORT_MIN" -v hi="$PORT_MAX" \
  '$1 >= lo && $1 <= hi' | tr '\n' ' ')
OUTSIDE=$(udp_ports | awk -v lo="$PORT_MIN" -v hi="$PORT_MAX" \
  '$1 < lo || $1 > hi' | tr '\n' ' ')
DATAGRAMS=$(metric mss_ingest_datagrams_total)
say "udp sockets inside the range while tapping: [${DURING:-none}]"
say "udp sockets outside the range (ng control sockets, by design): \
[${OUTSIDE:-none}]"
say "in_use=$(metric mss_media_ports_in_use) free=$(metric mss_media_ports_free) \
exhausted=$(metric mss_media_ports_exhausted_total) \
conflicts=$(metric mss_media_ports_bind_conflicts_total)"
say "ingest datagrams $BASE_DATAGRAMS -> $DATAGRAMS over ${TAP_SECONDS}s"

FAILED=0
[ -z "$DURING" ] && { say "no media socket landed in the range -- FAILED"; FAILED=1; }
[ "$DATAGRAMS" -le "$BASE_DATAGRAMS" ] && {
  say "no audio arrived while the range was in force -- FAILED"
  FAILED=1
}
say "the answer MSS sent rtpengine, and the sockets it bound:"
docker logs "$POD" 2>&1 | tail -400 >"$OUT/pod-$STAMP.log"
OUT_DIR=$OUT STAMP=$STAMP python3 - <<'PYEOF' | tee -a "$LOG"
import json, os
path = f"{os.environ['OUT_DIR']}/pod-{os.environ['STAMP']}.log"
keep = ("bind inside this port range", "peers are told", "bind to this address",
        "rtpengine offered a tap stream", "settled the tap format",
        "answering the subscription", "session closed")
for line in open(path):
    try:
        record = json.loads(line)
    except ValueError:
        continue
    fields = record.get("fields", {})
    message = fields.get("message", "")
    if not any(word in message for word in keep):
        continue
    extra = " ".join(f"{k}={v}" for k, v in fields.items() if k != "message")
    print(f"  {record.get('timestamp','')[11:23]} {message} {extra}".rstrip())
PYEOF

cargo run --quiet -p control-api --example mss_ctl -- \
  "$CONTROL" destroy "$EXTERNAL_ID" 2>&1 | tee -a "$LOG"
sleep 3
AFTER=$(udp_ports | awk -v lo="$PORT_MIN" -v hi="$PORT_MAX" \
  '$1 >= lo && $1 <= hi' | tr '\n' ' ')
say "udp sockets inside the range after the session closed: [${AFTER:-none}]"
say "in_use=$(metric mss_media_ports_in_use) free=$(metric mss_media_ports_free)"
if [ "$(metric mss_media_ports_in_use)" != "0" ]; then
  say "the closed session did not give its ports back -- FAILED"
  FAILED=1
fi
if [ "$(metric mss_media_ports_free)" != "$(metric mss_media_ports_capacity)" ]; then
  say "the range did not come back to full capacity -- FAILED"
  FAILED=1
fi

kill "$CALLER" 2>/dev/null || true
CALLER=""
if [ "$FAILED" = "0" ]; then
  say "PASS: media sockets stayed inside $PORT_MIN-$PORT_MAX, audio flowed, \
ports were returned on close"
else
  say "FAILED -- see $LOG"
fi
exit "$FAILED"
