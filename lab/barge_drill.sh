#!/bin/sh
# Item 5/12: measures the half of barge-in cut-through that MSS owns --
# MSS publishes a MediaEvent -> Kafka `mss.events` -> a mock translator
# consumes it -> the translator calls StopPlayback -> MSS acks the stop.
#
# It runs BESIDE the live lab and adds only one container: a fabricated call
# (lab/call_driver.py, no SIP) on a free IP, tapped by the lab's own
# mss-control pod, so the events travel the real event_pump into the real
# Redpanda. The translator (lab/barge_translator.py) runs on the host.
#
# What it does NOT measure, and why, is in barge_translator.py's header: the
# consumer -> MSS speech-report hop has no wire today (Registry::report is
# test-only), so the trigger event is PlaybackStarted and the consumer's own
# detection latency is out of frame.
#
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
#     up -d rtpengine redpanda redis mss-control
#   pip install --user grpcio grpcio-tools kafka-python-ng
#   ./lab/barge_drill.sh
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
NET=${NET:-mss-microsip_lab}
NG_NODE=${NG_NODE:-172.31.99.10}
DRIVER_IP=${DRIVER_IP:-172.31.99.123}
CONTROL=${CONTROL:-127.0.0.1:50551}
BROKERS=${BROKERS:-127.0.0.1:19092}
TOPIC=${TOPIC:-mss.events}
ITERATIONS=${ITERATIONS:-25}
STAMP=$(date +%s)
EXTERNAL_ID=${EXTERNAL_ID:-barge-$STAMP}
CALL_ID=${CALL_ID:-barge-call-$STAMP}
OUT=${OUT:-$REPO/lab/out/barge-drill-$STAMP.jsonl}

export DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
CTL="$REPO/target/debug/examples/mss_ctl"

say() { printf 'barge-drill: %s\n' "$*"; }

cleanup() {
  say "cleaning up"
  [ -n "${SESSION:-}" ] && "$CTL" "http://$CONTROL" destroy "$SESSION" >/dev/null 2>&1 || true
  docker rm -f barge-driver >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$REPO/lab/out/pb"
say "generating python stubs for MediaControl"
python3 -m grpc_tools.protoc -I"$REPO/proto" \
  --python_out="$REPO/lab/out/pb" --grpc_python_out="$REPO/lab/out/pb" \
  "$REPO/proto/mediacontrol.proto"

say "building mss_ctl"
(cd "$REPO" && cargo build --quiet -p control-api --example mss_ctl)

# One cookie prefix per driver: rtpengine caches replies per cookie, so two
# drivers sharing one prefix read each other's answers (the D12 shape).
say "fabricating a call $CALL_ID on $NG_NODE"
docker rm -f barge-driver >/dev/null 2>&1 || true
docker run -d --name barge-driver --network "$NET" --ip "$DRIVER_IP" \
  -v "$HERE/call_driver.py:/call_driver.py:ro" \
  -e NG_NODE="$NG_NODE" -e NG_PORT=22222 -e SELF_IP="$DRIVER_IP" \
  -e COOKIE_PREFIX=barge -e CALL_ID="$CALL_ID" \
  -e FROM_TAG=bgA -e TO_TAG=bgB -e PUMP_SECONDS=900 \
  python:3-slim python3 /call_driver.py >/dev/null
sleep 6
docker logs barge-driver 2>&1 | tail -2

say "tapping it as $EXTERNAL_ID"
SESSION=$("$CTL" "http://$CONTROL" create "$EXTERNAL_ID" "$CALL_ID" bgA,bgB |
  sed -n 's/.*session_id: "\([^"]*\)".*/\1/p')
[ -n "$SESSION" ] || { say "no session was created"; exit 1; }
say "session $SESSION"

say "running the mock translator for $ITERATIONS iterations"
BROKERS="$BROKERS" TOPIC="$TOPIC" CONTROL="$CONTROL" \
  EXTERNAL_ID="$EXTERNAL_ID" ITERATIONS="$ITERATIONS" \
  STUBS="$REPO/lab/out/pb" OUT="$OUT" \
  python3 "$HERE/barge_translator.py" "$SESSION"

say "rows in $OUT"
say "done"
