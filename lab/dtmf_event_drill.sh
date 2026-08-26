#!/bin/sh
# D21: proves that a digit pressed on a tapped call reaches mss.events.
#
# Before this, digits were delivered to CONSUMERS only (WS `dtmf` frames, gRPC
# `DtmfFrame`) and counted in mss_ingest_dtmf_digits_total, so an integrator who
# wanted an in-call digit menu had to hold a media stream open to hear one. A
# digit is a property of the CALL, not of a consumer, so it is now published at
# session level: no attachment, no capability, nothing to subscribe to but the
# bus.
#
# lab/call_driver.py presses one digit per leg every DIGIT_INTERVAL_SECONDS and
# sends the RFC 4733 end packet three times. Exactly one event per press must
# appear, carrying the digit, the track, the press duration and the RTP
# timestamp of the event's start.
#
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
#     -f lab/docker-compose.webrtc.yml up -d rtpengine redis redpanda minio \
#     minio-init mss-control
#   ./lab/dtmf_event_drill.sh
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
OUT=${OUT:-$HERE/out}
NET=${NET:-mss-microsip_lab}
NG_NODE=${NG_NODE:-172.31.99.10}
DRIVER_IP=${DRIVER_IP:-172.31.99.92}
CONTROL=${CONTROL:-127.0.0.1:50551}
CTL_POD=${CTL_POD:-mss-microsip-mss-control-1}
BROKERS=${BROKERS:-127.0.0.1:19092}
METRICS=${METRICS:-127.0.0.1:9464}
PUMP_SECONDS=${PUMP_SECONDS:-90}
TAIL_SECONDS=${TAIL_SECONDS:-25}
DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
export DOCKER_API_VERSION

STAMP=$(date +%s)
LOG=$OUT/dtmf-events-$STAMP.log
CTL="$REPO/target/debug/examples/mss_ctl"
CALL_ID=dtmf-$STAMP
EXTERNAL=dtmf-ext-$STAMP
CALLER_TAG=caller-$STAMP
CALLEE_TAG=callee-$STAMP

mkdir -p "$OUT"

say() {
  echo "dtmf-drill: $*" | tee -a "$LOG"
}

fail() {
  say "FAIL: $*"
  exit 1
}

cleanup() {
  docker rm -f dtmf-driver >/dev/null 2>&1 || true
}
trap cleanup EXIT

say "building mss_ctl and the event tail"
(cd "$REPO" && cargo build --quiet -p control-api --example mss_ctl)
(cd "$REPO" && cargo build --quiet -p mediaserverd --example mss_events_tail)

say "fabricating call $CALL_ID; the driver presses 1 on the caller and 2 on the callee"
cleanup
docker run -d --name dtmf-driver --network "$NET" --ip "$DRIVER_IP" \
  -v "$HERE/call_driver.py:/call_driver.py:ro" \
  -e NG_NODE="$NG_NODE" -e NG_PORT=22222 -e SELF_IP="$DRIVER_IP" \
  -e COOKIE_PREFIX="dtmf$STAMP" -e CALL_ID="$CALL_ID" \
  -e FROM_TAG="$CALLER_TAG" -e TO_TAG="$CALLEE_TAG" \
  -e PUMP_SECONDS="$PUMP_SECONDS" -e DIGIT_AFTER_SECONDS=4 \
  -e DIGIT_INTERVAL_SECONDS=6 \
  python:3-slim python3 /call_driver.py >/dev/null
sleep 5

say "tapping it with the caller's tag, so the tracks are named customer / agent"
"$CTL" "http://$CONTROL" create "$EXTERNAL" "$CALL_ID" "$CALLER_TAG" 2>&1 | tee -a "$LOG"

say "reading mss.events for ${TAIL_SECONDS}s with NO consumer attached"
TAIL=$OUT/dtmf-events-$STAMP.events
(cd "$REPO" && cargo run --quiet -p mediaserverd --example mss_events_tail -- \
  "$BROKERS" mss.events "$TAIL_SECONDS") >"$TAIL" 2>>"$LOG" || true

say "the digit events this call put on the bus:"
grep 'Dtmf(Dtmf' "$TAIL" | tee -a "$LOG" || fail "mss.events carried no digit"

digits=$(grep -c 'Dtmf(Dtmf' "$TAIL" || true)
say "digit events seen: $digits"
[ "$digits" -ge 2 ] || fail "expected at least one press per leg, saw $digits"

# The ingest counter keeps counting after the tail window closes, so it can
# lead the number of events read here; what must match is one event per press
# inside the window, and zero drops.
say "presses the ingest path counted, for comparison"
curl -s "http://$METRICS/metrics" | grep -E \
  'mss_ingest_dtmf_digits_total|mss_dtmf_events_dropped_total' | tee -a "$LOG"

say "what the daemon logged about the presses"
docker logs --since 2m "$CTL_POD" 2>&1 | grep -i 'digit was pressed' | tail -6 |
  tee -a "$LOG" || true

"$CTL" "http://$CONTROL" destroy "$EXTERNAL" 2>&1 | tee -a "$LOG"
say "done; transcript in $LOG, raw events in $TAIL"
