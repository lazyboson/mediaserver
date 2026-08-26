#!/bin/sh
# D17: proves that leg labels no longer depend on the order rtpengine's
# `query` happens to return participants in.
#
# The two-node drill (tasks item 24) created its session with from-tags "-" and
# got customer/agent swapped, because MSS labelled the legs in the order the
# reply listed them -- and that order is the bencode dict's, i.e. lexicographic
# by tag. This drill reproduces exactly that condition: the CALLEE's tag sorts
# first, so the old code would have called the callee the customer.
#
# lab/ng_tag_created_probe.py measured what rtpengine 14.1.1.8 actually exposes
# per participant -- `created`, one second resolution, stamped per DIALOGUE and
# therefore identical for the two legs of one call. So creation order cannot
# separate them, and the honest outcome is attribution=unknown with the tracks
# named leg_a / leg_b. The same drill run WITH the caller's tag must still
# produce explicit attribution and customer / agent.
#
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
#     -f lab/docker-compose.webrtc.yml up -d rtpengine redis redpanda minio \
#     minio-init mss-control
#   ./lab/leg_attribution_drill.sh
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
OUT=${OUT:-$HERE/out}
NET=${NET:-mss-microsip_lab}
NG_NODE=${NG_NODE:-172.31.99.10}
DRIVER_IP=${DRIVER_IP:-172.31.99.91}
CONTROL=${CONTROL:-127.0.0.1:50551}
CTL_POD=${CTL_POD:-mss-microsip-mss-control-1}
BUCKET=${BUCKET:-lab-recordings}
PUMP_SECONDS=${PUMP_SECONDS:-180}
PROBE_SECONDS=${PROBE_SECONDS:-8}
DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
export DOCKER_API_VERSION

STAMP=$(date +%s)
LOG=$OUT/leg-attribution-$STAMP.log
CTL="$REPO/target/debug/examples/mss_ctl"

mkdir -p "$OUT"

say() {
  echo "attribution-drill: $*" | tee -a "$LOG"
}

fail() {
  say "FAIL: $*"
  exit 1
}

cleanup() {
  docker rm -f attribution-driver >/dev/null 2>&1 || true
}
trap cleanup EXIT

say "building mss_ctl and the stream probe"
(cd "$REPO" && cargo build --quiet -p control-api --examples)

# The callee's tag sorts BEFORE the caller's, which is the inversion the
# two-node drill hit: whoever labels by reply order calls the callee "customer".
CALLER_TAG=zz-caller
CALLEE_TAG=aa-callee
CALL_ID=attribution-$STAMP

say "fabricating call $CALL_ID: caller $CALLER_TAG, callee $CALLEE_TAG"
say "the callee's tag sorts first, so reply order alone would invert the legs"
cleanup
docker run -d --name attribution-driver --network "$NET" --ip "$DRIVER_IP" \
  -v "$HERE/call_driver.py:/call_driver.py:ro" \
  -e NG_NODE="$NG_NODE" -e NG_PORT=22222 -e SELF_IP="$DRIVER_IP" \
  -e COOKIE_PREFIX="attr$STAMP" -e CALL_ID="$CALL_ID" \
  -e FROM_TAG="$CALLER_TAG" -e TO_TAG="$CALLEE_TAG" \
  -e PUMP_SECONDS="$PUMP_SECONDS" \
  python:3-slim python3 /call_driver.py >/dev/null
sleep 6
docker logs attribution-driver 2>&1 | tail -3 | tee -a "$LOG"

say "what rtpengine says about this call's participants"
docker run --rm --network "$NET" -v "$HERE:/lab:ro" -w /lab \
  -e NG_NODE="$NG_NODE" -e CALL_ID="$CALL_ID" -e LEGS="$CALLER_TAG,$CALLEE_TAG" \
  python:3-slim python ng_call_tags.py 2>&1 | tee -a "$LOG" || true

run_case() {
  case_name=$1
  tags=$2
  expect_attribution=$3
  expect_tracks=$4
  external=attr-$case_name-$STAMP
  recording=acct-attr/rec-$case_name-$STAMP.wav

  say "--- case $case_name: create with from-tags '$tags'"
  "$CTL" "http://$CONTROL" create "$external" "$CALL_ID" "$tags" 2>&1 | tee -a "$LOG"

  described=$("$CTL" "http://$CONTROL" describe "$external" 2>&1)
  echo "$described" >>"$LOG"
  got=$(echo "$described" | sed -n 's/.*attribution: "\([^"]*\)".*/\1/p' | head -1)
  say "describe reports attribution=$got (expected $expect_attribution)"
  [ "$got" = "$expect_attribution" ] || fail "attribution $got, expected $expect_attribution"

  say "recording this session as a group member so the object keys are named"
  "$CTL" "http://$CONTROL" record "$external" "$recording" alice grp-$case_name all \
    2>&1 | tee -a "$LOG"

  say "attaching a grpc consumer and reading its start frame"
  MSS_PROBE_TRACKS=all MSS_PROBE_ENCODING=l16 MSS_PROBE_RATE=16000 \
    "$REPO/target/debug/examples/mss_stream_probe" "http://$CONTROL" "$external" \
    "$OUT/attr-$case_name-$STAMP.wav" "$PROBE_SECONDS" 2>&1 | tee -a "$LOG" || true
  started=$(grep -o 'tracks=\[[^]]*\]' "$LOG" | tail -1)
  say "start frame $started (expected $expect_tracks)"
  echo "$started" | grep -q "$expect_tracks" || fail "start frame said $started"

  "$CTL" "http://$CONTROL" destroy "$external" 2>&1 | tee -a "$LOG"
  sleep 4
  say "objects this case left in the bucket"
  docker exec mss-microsip-minio-1 sh -c \
    "mc alias set lab http://127.0.0.1:9000 \
       \${MINIO_ROOT_USER:-minioadmin} \${MINIO_ROOT_PASSWORD:-minioadmin} >/dev/null &&
     mc ls --recursive lab/$BUCKET/acct-attr" 2>&1 |
    grep "rec-$case_name-$STAMP" | tee -a "$LOG" || say "nothing landed"
}

run_case nobody-said - unknown '"leg_a", "leg_b"'
run_case caller-named "$CALLER_TAG" explicit '"customer", "agent"'

say "what the daemon logged about attribution"
docker logs --since 5m "$CTL_POD" 2>&1 |
  grep -i "attribut\|claim a direction\|creation times" | tail -6 | tee -a "$LOG" || true

say "done; transcript in $LOG"
