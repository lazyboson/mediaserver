#!/bin/sh
# Item G1: the SIGTERM drain drill -- what a Kubernetes rollout does to a pod
# that is tapping a live call.
#
# Pod A taps a live SIP call with a WS consumer attached; pod B idles beside it
# sharing the same Redis. Then `docker stop -t 60` sends pod A the signal
# Kubernetes sends (SIGTERM, not SIGINT) and the drill checks:
#
#   1. every drain step appears in pod A's log, in order
#   2. the container exits 0 -- not 137, which is what SIGKILL after the
#      grace period looks like when SIGTERM was ignored
#   3. it exits well inside the 60 s stop timeout (the drain window is 30 s)
#   4. pod B adopts the session quickly, because the lease was handed off
#      rather than left to expire
#   5. the consumer's audio gap, for comparison with the 14.41 s that
#      pod_kill_drill.sh (SIGKILL, no drain) measured
#
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
#     -f lab/docker-compose.webrtc.yml up -d rtpengine opensips freeswitch \
#     call-watcher redpanda redis minio minio-init mock-bridge mss-control \
#     mss-control-b
#   ./lab/drain_drill.sh
#
# Pod A is stopped, not destroyed; bring it back with
#   docker compose -f lab/docker-compose.microsip.yml up -d mss-control
#
# The drill asserts pod A's PID 1 is mediaserverd itself: `cargo run`
# exec-replaces itself on Unix, and if that ever stops being true the signal
# would land on cargo and this whole measurement would be meaningless.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
OUT=${OUT:-$HERE/out}
CONTROL_A=${CONTROL_A:-http://127.0.0.1:50551}
CONTROL_B=${CONTROL_B:-http://127.0.0.1:50552}
METRICS_A=${METRICS_A:-http://127.0.0.1:9464/metrics}
METRICS_B=${METRICS_B:-http://127.0.0.1:9465/metrics}
POD_A=${POD_A:-mss-microsip-mss-control-1}
POD_B=${POD_B:-mss-microsip-mss-control-b-1}
SPIKE=${SPIKE:-mss-microsip-mediaserverd-1}
REDIS=${REDIS:-mss-microsip-redis-1}
WATCHER=${WATCHER:-mss-microsip-call-watcher-1}
CONSUMER_HOST=${CONSUMER_HOST:-host.docker.internal}
CONSUMER_PORT=${CONSUMER_PORT:-8096}
CALL_SECONDS=${CALL_SECONDS:-150}
TAP_SECONDS=${TAP_SECONDS:-20}
STOP_TIMEOUT=${STOP_TIMEOUT:-60}
ADOPT_TIMEOUT=${ADOPT_TIMEOUT:-60}
DIAL=${DIAL:-9000}
DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
export DOCKER_API_VERSION

STAMP=$(date +%s)
EXTERNAL_ID=draindrill-$STAMP
LOG=$OUT/drain-drill-$STAMP.log
CONSUMER=ws://$CONSUMER_HOST:$CONSUMER_PORT/ws
CALL_ID=""
FROM_TAGS=""
GAP=""
CALLER=""

mkdir -p "$OUT"
say() { echo "drill: $*" | tee -a "$LOG"; }
metric() {
  value=$(curl -s "$1" | sed -n "s/^$2 //p" | head -1)
  echo "${value:-0}"
}

echo "drill: session $EXTERNAL_ID, consumer $CONSUMER, log $LOG"

if docker ps --format '{{.Names}}' | grep -qx "$SPIKE"; then
  say "the Phase-0 spike container $SPIKE is running; it would tap the same call"
  exit 1
fi
for pod in "$POD_A" "$POD_B"; do
  if ! docker ps --format '{{.Names}}' | grep -qx "$pod"; then
    say "$pod is not running; bring up mss-control and mss-control-b first"
    exit 1
  fi
done
for endpoint in "$METRICS_A" "$METRICS_B"; do
  if ! curl -s -o /dev/null "$endpoint"; then
    say "$endpoint does not answer; is that pod's MSS_METRICS_LISTEN published?"
    exit 1
  fi
done
PID1=$(docker exec "$POD_A" cat /proc/1/comm 2>/dev/null || echo unknown)
say "pod A PID 1 is '$PID1' (must be mediaserverd for the signal to reach it)"
if [ "$PID1" != "mediaserverd" ]; then
  say "the signal would land on '$PID1', not on the daemon -- FAILED"
  exit 1
fi
say "pod A reports mss_draining=$(metric "$METRICS_A" mss_draining) before the drain"
BASE_ADOPTED_B=$(metric "$METRICS_B" mss_registry_adopted_total)

cd "$REPO"
say "building mss_ctl before the call so nothing compiles mid-drill"
cargo build --quiet -p control-api --examples

cleanup() {
  [ -n "$GAP" ] && kill "$GAP" 2>/dev/null || true
  [ -n "$CALLER" ] && kill "$CALLER" 2>/dev/null || true
}
trap cleanup EXIT

GAP_STAMP=$STAMP OUT_DIR=$OUT GAP_PORT=$CONSUMER_PORT \
  python3 "$HERE/gap_consumer.py" >"$OUT/gap-consumer-$STAMP.log" 2>&1 &
GAP=$!
sleep 1
if ! kill -0 "$GAP" 2>/dev/null; then
  say "the gap consumer did not start; see $OUT/gap-consumer-$STAMP.log"
  exit 1
fi

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

cargo run --quiet -p control-api --example mss_ctl -- \
    "$CONTROL_A" create "$EXTERNAL_ID" "$CALL_ID" "$FROM_TAGS" 2>&1 | tee -a "$LOG"
cargo run --quiet -p control-api --example mss_ctl -- \
    "$CONTROL_A" attach "$EXTERNAL_ID" "$CONSUMER" gapmeter authoritative 2>&1 | tee -a "$LOG"

say "letting pod A's tap run for ${TAP_SECONDS}s"
sleep "$TAP_SECONDS"
say "pod A delivered $(metric "$METRICS_A" mss_consumer_delivered_total) frames, \
sessions_live=$(metric "$METRICS_A" mss_sessions_live)"
say "lease before the stop: owner=$(docker exec "$REDIS" redis-cli --raw \
  get "mss:lease:$EXTERNAL_ID") ttl=$(docker exec "$REDIS" redis-cli --raw \
  ttl "mss:lease:$EXTERNAL_ID")"

# --- the signal Kubernetes sends, with a grace period longer than the drain --
STOPPED_AT=$(date +%s.%N)
say "docker stop -t $STOP_TIMEOUT $POD_A (SIGTERM, then SIGKILL after ${STOP_TIMEOUT}s)"
docker stop -t "$STOP_TIMEOUT" "$POD_A" >/dev/null
EXITED_AT=$(date +%s.%N)
EXIT_CODE=$(docker inspect --format '{{.State.ExitCode}}' "$POD_A")
DRAIN_SECONDS=$(echo "$EXITED_AT $STOPPED_AT" | awk '{printf "%.2f", $1 - $2}')
say "pod A exited with code $EXIT_CODE after ${DRAIN_SECONDS}s"
docker logs "$POD_A" >"$OUT/pod-a-$STAMP.log" 2>&1 || true

say "the drain sequence pod A logged:"
OUT_DIR=$OUT STAMP=$STAMP python3 - <<'PYEOF' | tee -a "$LOG"
import json, os
path = f"{os.environ['OUT_DIR']}/pod-a-{os.environ['STAMP']}.log"
keep = ("shutdown signal", "readiness is off", "lease released for adoption",
        "session closed for shutdown", "drain step", "drain complete",
        "drain incomplete", "control plane listener", "totals at shutdown",
        "mediaserverd stopped")
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
    stamp = record.get("timestamp", "")[11:23]
    print(f"  {stamp} {record.get('level','')} {message} {extra}".rstrip())
PYEOF

# --- pod B should adopt fast, because the lease was handed off, not expired ---
adopted_b=$BASE_ADOPTED_B
waited=0
while [ "$waited" -lt "$ADOPT_TIMEOUT" ]; do
  adopted_b=$(metric "$METRICS_B" mss_registry_adopted_total)
  [ "$adopted_b" != "$BASE_ADOPTED_B" ] && break
  waited=$((waited + 1))
  sleep 1
done
ADOPTED_AT=$(date +%s.%N)
say "pod B adopted=$BASE_ADOPTED_B -> $adopted_b, \
$(echo "$ADOPTED_AT $EXITED_AT" | awk '{printf "%.1f", $1 - $2}')s after pod A exited \
($(echo "$ADOPTED_AT $STOPPED_AT" | awk '{printf "%.1f", $1 - $2}')s after the signal)"
say "lease now: owner=$(docker exec "$REDIS" redis-cli --raw \
  get "mss:lease:$EXTERNAL_ID")"

wait "$CALLER" 2>/dev/null || true
CALLER=""
cargo run --quiet -p control-api --example mss_ctl -- \
    "$CONTROL_B" destroy "$EXTERNAL_ID" 2>&1 | tee -a "$LOG" || true

kill -TERM "$GAP" 2>/dev/null || true
wait "$GAP" 2>/dev/null || true
GAP=""
say "the consumer's measurement (compare with 14.41 s for SIGKILL):"
grep -E 'track |connection ' "$OUT/gap-consumer-$STAMP.log" | tee -a "$LOG" || true

say "=== the assertions ==="
say "1. exit code: $EXIT_CODE (0 is a clean drain; 137 means SIGKILL after the grace period)"
say "2. drained in ${DRAIN_SECONDS}s inside a ${STOP_TIMEOUT}s stop timeout"
say "3. pod B adopted: $BASE_ADOPTED_B -> $adopted_b"
say "done. artifacts in $OUT: gap-$STAMP-*.wav, pod-a-$STAMP.log, $LOG"
say "restart pod A with: docker compose -f $HERE/docker-compose.microsip.yml \
up -d mss-control"
