#!/bin/sh
# G11: proves that a CreateSession which names NO rtpengine node still lands on
# the rtpengine anchoring the call, because the proxy published call-id -> node
# into Redis and MSS looked it up.
#
# The proof rests on a deliberately WRONG default node: mss-control is started
# with MSS_RTPENGINE_NODE pointing at a black hole (nothing listens there), so
# audio can only arrive if the node came from the discovery map. The second
# half asks for a call-id nobody mapped -- that one must fall back to the black
# hole, count a miss, and fail the create rather than guessing.
#
#   export DOCKER_API_VERSION=1.43 DISCOVERY=on
#   export MSS_DISCOVERY_REDIS_KEY_PREFIX=mss:call-node:
#   export MSS_RTPENGINE_NODE=172.31.99.199:22222
#   docker compose -f lab/docker-compose.microsip.yml \
#     -f lab/docker-compose.webrtc.yml up -d --force-recreate \
#     rtpengine opensips freeswitch redis redpanda minio minio-init \
#     call-watcher mss-control
#   ./lab/node_discovery_drill.sh
#
# Afterwards put the lab back: unset DISCOVERY MSS_DISCOVERY_REDIS_KEY_PREFIX
# MSS_RTPENGINE_NODE and recreate opensips + mss-control.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
OUT=${OUT:-$HERE/out}
CONTROL=${CONTROL:-http://127.0.0.1:50551}
METRICS=${METRICS:-http://127.0.0.1:9464/metrics}
POD=${POD:-mss-microsip-mss-control-1}
PROXY=${PROXY:-mss-microsip-opensips-1}
REDIS_POD=${REDIS_POD:-mss-microsip-redis-1}
WATCHER=${WATCHER:-mss-microsip-call-watcher-1}
PREFIX=${MSS_DISCOVERY_REDIS_KEY_PREFIX:-mss:call-node:}
ANCHOR=${ANCHOR:-172.31.99.10:22222}
CALL_SECONDS=${CALL_SECONDS:-90}
TAP_SECONDS=${TAP_SECONDS:-12}
DIAL=${DIAL:-9000}
DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
export DOCKER_API_VERSION

STAMP=$(date +%s)
LOG=$OUT/node-discovery-drill-$STAMP.log
MAPPED_ID=disc-mapped-$STAMP
UNMAPPED_ID=disc-unmapped-$STAMP
CALL_ID=""
CALLER=""
FAILED=0

mkdir -p "$OUT"
say() { echo "discovery-drill: $*" | tee -a "$LOG"; }
metric() {
  value=$(curl -s "$METRICS" | sed -n "s/^$1 //p" | head -1)
  echo "${value:-0}"
}
mapped_value() {
  docker exec "$REDIS_POD" redis-cli --no-raw GET "$PREFIX$1" 2>/dev/null |
    tr -d '\r'
}

cleanup() {
  [ -n "$CALLER" ] && kill "$CALLER" 2>/dev/null || true
}
trap cleanup EXIT

if ! docker ps --format '{{.Names}}' | grep -qx "$POD"; then
  say "$POD is not running"
  exit 1
fi
if [ "$(curl -s "$METRICS" | grep -c '^mss_discovery_')" -eq 0 ]; then
  say "this pod exposes no mss_discovery_ counters, so it has no discovery map \
configured; recreate it with MSS_DISCOVERY_REDIS_KEY_PREFIX set"
  exit 1
fi
if ! docker exec "$PROXY" sh -c 'grep -q "discovery_publish.py store" /tmp/opensips.cfg &&
    grep -q "on\" == \"on\"\|discovery) == \"on\"" /tmp/opensips.cfg'; then
  say "the proxy config has no discovery block; recreate opensips with DISCOVERY=on"
  exit 1
fi
DEFAULT_NODE=$(docker inspect --format \
  '{{range .Config.Env}}{{println .}}{{end}}' "$POD" |
  sed -n 's/^MSS_RTPENGINE_NODE=//p')
say "pod default node is ${DEFAULT_NODE:-unset}; the call anchors on $ANCHOR"
if [ "$DEFAULT_NODE" = "$ANCHOR" ]; then
  say "the default node IS the anchoring node, so this drill cannot tell the \
map apart from the fallback; recreate mss-control with \
MSS_RTPENGINE_NODE=172.31.99.199:22222"
  exit 1
fi

cd "$REPO"
say "building mss_ctl before the call so nothing compiles mid-drill"
cargo build --quiet -p control-api --examples
CTL="$REPO/target/debug/examples/mss_ctl"

HITS=$(metric mss_discovery_hits_total)
MISSES=$(metric mss_discovery_misses_total)
ERRORS=$(metric mss_discovery_errors_total)
say "counters before: hits=$HITS misses=$MISSES errors=$ERRORS"

docker exec "$WATCHER" rm -f /shared/call.env 2>/dev/null || true
say "dialing $DIAL through OpenSIPS for ${CALL_SECONDS}s"
( cd "$HERE" && CALL_SECONDS=$CALL_SECONDS DIAL=$DIAL EAR_DIR=out \
    python3 host_test_caller.py >"$OUT/host-caller-$STAMP.log" 2>&1 ) &
CALLER=$!

waited=0
while [ "$waited" -lt 25 ]; do
  if env=$(docker exec "$WATCHER" cat /shared/call.env 2>/dev/null); then
    CALL_ID=$(echo "$env" | sed -n 's/^MSS_TAP_CALL_ID=//p')
    [ -n "$CALL_ID" ] && break
  fi
  waited=$((waited + 1))
  sleep 1
done
[ -n "$CALL_ID" ] || { say "call_watcher never announced a call"; exit 1; }
say "call $CALL_ID is up"

VALUE=""
waited=0
while [ "$waited" -lt 15 ]; do
  VALUE=$(mapped_value "$CALL_ID")
  case "$VALUE" in *node*) break ;; esac
  waited=$((waited + 1))
  sleep 1
done
say "redis GET $PREFIX$CALL_ID -> ${VALUE:-(nil)}"
case "$VALUE" in
  *"$ANCHOR"*) say "the proxy published the anchoring node" ;;
  *) say "the proxy published no usable node -- FAILED"; FAILED=1 ;;
esac
case "$VALUE" in
  *caller_tag*) say "and it marked which tag called" ;;
  *) say "it named no caller tag, so this tap would be leg_a/leg_b" ;;
esac

BASE_DATAGRAMS=$(metric mss_ingest_datagrams_total)
say "--- case mapped: create $MAPPED_ID with NO node and NO from-tags"
if created=$("$CTL" "$CONTROL" create "$MAPPED_ID" "$CALL_ID" - 2>&1); then
  echo "$created" >>"$LOG"
  say "created without naming a node"
else
  say "the create was refused: $created -- FAILED"
  FAILED=1
fi
sleep "$TAP_SECONDS"
DATAGRAMS=$(metric mss_ingest_datagrams_total)
say "ingest datagrams $BASE_DATAGRAMS -> $DATAGRAMS over ${TAP_SECONDS}s"
[ "$DATAGRAMS" -le "$BASE_DATAGRAMS" ] && {
  say "no audio arrived, so nothing tapped the anchoring node -- FAILED"
  FAILED=1
}
described=$("$CTL" "$CONTROL" describe "$MAPPED_ID" 2>&1 || true)
echo "$described" >>"$LOG"
ATTRIBUTION=$(echo "$described" | sed -n 's/.*attribution: "\([^"]*\)".*/\1/p' | head -1)
say "describe reports attribution=${ATTRIBUTION:-none}"
HITS_NOW=$(metric mss_discovery_hits_total)
say "hits $HITS -> $HITS_NOW (expected one more)"
[ "$HITS_NOW" -eq $((HITS + 1)) ] || { say "hit not counted -- FAILED"; FAILED=1; }
"$CTL" "$CONTROL" destroy "$MAPPED_ID" 2>&1 | tee -a "$LOG" || true

say "--- case unmapped: a call-id nobody published"
if refused=$("$CTL" "$CONTROL" create "$UNMAPPED_ID" "no-such-call-$STAMP@lab" - \
    2>&1); then
  say "a session on the black-hole default node was created: $refused -- FAILED"
  FAILED=1
else
  say "refused, as it must be: $refused"
fi
MISSES_NOW=$(metric mss_discovery_misses_total)
say "misses $MISSES -> $MISSES_NOW (expected one more)"
[ "$MISSES_NOW" -eq $((MISSES + 1)) ] || { say "miss not counted -- FAILED"; FAILED=1; }

say "hanging up so the proxy removes the key (SIGINT, so the caller sends BYE)"
kill -INT "$CALLER" 2>/dev/null || true
wait "$CALLER" 2>/dev/null || true
CALLER=""
sleep 3
say "redis GET after the caller went away -> $(mapped_value "$CALL_ID")"

say "errors $ERRORS -> $(metric mss_discovery_errors_total)"
say "what the daemon logged about discovery:"
docker logs --since 5m "$POD" 2>&1 |
  grep -i "discovery map\|mapped to this call\|discovery" | tail -8 | tee -a "$LOG" || true

if [ "$FAILED" -ne 0 ]; then
  say "FAILED -- transcript in $LOG"
  exit 1
fi
say "PASS -- transcript in $LOG"
say "restore the lab: unset DISCOVERY MSS_DISCOVERY_REDIS_KEY_PREFIX \
MSS_RTPENGINE_NODE and recreate opensips + mss-control"
