#!/bin/sh
# Item 10: proves the gRPC data plane on a live tapped call, with no human
# dialing. lab/host_test_caller.py plays the softphone from the WSL host,
# call_watcher finds the call, mss_ctl creates the session against the running
# mss-control daemon, and mss_stream_probe attaches a GRPC_STREAM consumer at
# L16/16k and writes what it hears.
#
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
#     up -d rtpengine opensips freeswitch call-watcher redpanda redis \
#           minio minio-init mock-bridge mss-control
#   ./lab/grpc_stream_drill.sh
#
# RECORD=1 also attaches a FILE_S3 recording to the same session (item 15's
# live-call half), pausing and resuming it mid-call so the recordPause callback
# is exercised too. Everything lands in lab/out/.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
OUT=${OUT:-$HERE/out}
CONTROL=${CONTROL:-http://127.0.0.1:50551}
METRICS=${METRICS:-http://127.0.0.1:9464/metrics}
CALL_SECONDS=${CALL_SECONDS:-45}
PROBE_SECONDS=${PROBE_SECONDS:-30}
DIAL=${DIAL:-9000}
RECORD=${RECORD:-0}
ACCOUNT=${ACCOUNT:-acct-grpc}
WATCHER=${WATCHER:-mss-microsip-call-watcher-1}
DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
export DOCKER_API_VERSION

STAMP=$(date +%s)
EXTERNAL_ID=grpc-drill-$STAMP
WAV=$OUT/grpc-probe-$STAMP.wav
LOG=$OUT/grpc-stream-drill-$STAMP.log
RECORDING=$ACCOUNT/rec-$STAMP.wav

mkdir -p "$OUT"
echo "drill: session $EXTERNAL_ID, wav $WAV, log $LOG"

echo "drill: building the probe before the call so it starts immediately"
cargo build --quiet -p control-api --examples

echo "drill: no call should be live yet"
docker exec "$WATCHER" rm -f /shared/call.env 2>/dev/null || true

echo "drill: dialing $DIAL for ${CALL_SECONDS}s"
( cd "$HERE" && CALL_SECONDS=$CALL_SECONDS DIAL=$DIAL EAR_DIR=out \
    python3 host_test_caller.py >"$OUT/host-caller-$STAMP.log" 2>&1 ) &
CALLER=$!

CALL_ID=""
FROM_TAGS=""
waited=0
while [ "$waited" -lt 20 ]; do
  if env=$(docker exec "$WATCHER" cat /shared/call.env 2>/dev/null); then
    CALL_ID=$(echo "$env" | sed -n 's/^MSS_TAP_CALL_ID=//p')
    FROM_TAGS=$(echo "$env" | sed -n 's/^MSS_TAP_FROM_TAGS=//p')
    if [ -n "$CALL_ID" ]; then
      break
    fi
  fi
  waited=$((waited + 1))
  sleep 1
done
if [ -z "$CALL_ID" ]; then
  echo "drill: call_watcher never announced a call; is the SIP half up?" >&2
  kill "$CALLER" 2>/dev/null || true
  exit 1
fi
echo "drill: call $CALL_ID tags $FROM_TAGS"

cd "$REPO"
{
  echo "drill: call=$CALL_ID tags=$FROM_TAGS external=$EXTERNAL_ID"
  cargo run --quiet -p control-api --example mss_ctl -- \
      "$CONTROL" create "$EXTERNAL_ID" "$CALL_ID" "$FROM_TAGS"
} 2>&1 | tee -a "$LOG"

RECORDER=""
if [ "$RECORD" = "1" ]; then
  RECORDER=$(cargo run --quiet -p control-api --example mss_ctl -- \
      "$CONTROL" record "$EXTERNAL_ID" "$RECORDING" recorder |
      sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
  echo "drill: recording $RECORDING as attachment $RECORDER" | tee -a "$LOG"
fi

MSS_PROBE_TRACKS=${MSS_PROBE_TRACKS:-all} \
MSS_PROBE_ENCODING=${MSS_PROBE_ENCODING:-l16} \
MSS_PROBE_RATE=${MSS_PROBE_RATE:-16000} \
  cargo run --quiet -p control-api --example mss_stream_probe -- \
      "$CONTROL" "$EXTERNAL_ID" "$WAV" "$PROBE_SECONDS" 2>&1 | tee -a "$LOG" &
PROBE=$!

if [ -n "$RECORDER" ]; then
  sleep 8
  cargo run --quiet -p control-api --example mss_ctl -- \
      "$CONTROL" pause "$RECORDER" true 2>&1 | tee -a "$LOG"
  sleep 4
  cargo run --quiet -p control-api --example mss_ctl -- \
      "$CONTROL" pause "$RECORDER" false 2>&1 | tee -a "$LOG"
else
  sleep 8
fi

echo "drill: metrics mid-call" | tee -a "$LOG"
curl -s "$METRICS" |
  grep -E '^mss_(consumer|legs|taps|sessions|recording)' | tee -a "$LOG" || true

wait "$PROBE" || true
wait "$CALLER" 2>/dev/null || true

echo "drill: metrics after the probe" | tee -a "$LOG"
curl -s "$METRICS" |
  grep -E '^mss_(consumer|legs|taps|sessions|recording)' | tee -a "$LOG" || true

cargo run --quiet -p control-api --example mss_ctl -- \
    "$CONTROL" destroy "$EXTERNAL_ID" 2>&1 | tee -a "$LOG"

if [ -n "$RECORDER" ]; then
  sleep 5
  echo "drill: what landed in the bucket" | tee -a "$LOG"
  docker exec mss-microsip-minio-1 sh -c \
      "mc alias set lab http://127.0.0.1:9000 \
         \${MINIO_ROOT_USER:-minioadmin} \${MINIO_ROOT_PASSWORD:-minioadmin} >/dev/null &&
       mc stat lab/\${MSS_RECORDING_BUCKET:-lab-recordings}/$RECORDING" 2>&1 |
      tee -a "$LOG" || true
fi

echo "drill: done; artifacts in $OUT, transcript of the run in $LOG"
