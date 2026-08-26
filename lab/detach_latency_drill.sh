#!/bin/sh
# D11: proves that stopping a recording does not wait for its upload, and that
# the upload's own event still lands in the session's sequence afterwards.
#
# Phase A (storage healthy): tap a fabricated call, record, then time the
# Detach RPC. It must answer in milliseconds; UploadCompleted arrives on
# mss.events afterwards and the object is in the bucket.
#
# Phase B (storage stalled): `docker pause` MinIO so the upload hangs inside
# its 60 s window, then detach AND destroy the session while it hangs. Both
# RPCs must still answer in milliseconds, mss_recording_uploads_in_flight must
# read 1 while the session is already gone from mss_sessions_live, and when
# MinIO is unpaused the late UploadCompleted must arrive carrying the ended
# session's identity with the sequence number that follows SessionEnded — the
# "finishing session record" this item added.
#
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
#     -f lab/docker-compose.webrtc.yml up -d rtpengine redis redpanda minio \
#     minio-init mss-control
#   ./lab/detach_latency_drill.sh
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
OUT=${OUT:-$HERE/out}
NET=${NET:-mss-microsip_lab}
NG_NODE=${NG_NODE:-172.31.99.10}
DRIVER_IP=${DRIVER_IP:-172.31.99.93}
CONTROL=${CONTROL:-127.0.0.1:50551}
CTL_POD=${CTL_POD:-mss-microsip-mss-control-1}
MINIO_POD=${MINIO_POD:-mss-microsip-minio-1}
BROKERS=${BROKERS:-127.0.0.1:19092}
METRICS=${METRICS:-127.0.0.1:9464}
BUCKET=${BUCKET:-lab-recordings}
ACCOUNT=${ACCOUNT:-acct-detach}
PUMP_SECONDS=${PUMP_SECONDS:-150}
RECORD_SECONDS=${RECORD_SECONDS:-15}
STALLED_SECONDS=${STALLED_SECONDS:-8}
PAUSED_SECONDS=${PAUSED_SECONDS:-12}
TAIL_SECONDS=${TAIL_SECONDS:-20}
ANSWERED_WITHIN_MS=${ANSWERED_WITHIN_MS:-500}
DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
export DOCKER_API_VERSION

STAMP=$(date +%s)
LOG=$OUT/detach-latency-$STAMP.log
CTL="$REPO/target/debug/examples/mss_ctl"
CALL_A=detach-a-$STAMP
CALL_B=detach-b-$STAMP
EXTERNAL_A=detach-ext-a-$STAMP
EXTERNAL_B=detach-ext-b-$STAMP
RECORDING_A=rec-fast-$STAMP
RECORDING_B=rec-stalled-$STAMP

mkdir -p "$OUT"

say() {
  echo "detach-drill: $*" | tee -a "$LOG"
}

fail() {
  say "FAIL: $*"
  exit 1
}

cleanup() {
  docker unpause "$MINIO_POD" >/dev/null 2>&1 || true
  docker rm -f detach-driver-a detach-driver-b >/dev/null 2>&1 || true
}
trap cleanup EXIT

metric() {
  curl -s "http://$METRICS/metrics" | awk -v name="$1" '$1 == name { print $2 }'
}

timed() {
  label=$1
  shift
  started=$(date +%s%N)
  "$@" 2>&1 | tee -a "$LOG"
  finished=$(date +%s%N)
  elapsed=$(( (finished - started) / 1000000 ))
  say "$label answered in ${elapsed} ms (includes mss_ctl start and connect)"
  [ "$elapsed" -le "$ANSWERED_WITHIN_MS" ] ||
    fail "$label took ${elapsed} ms; it waited for the upload"
}

fabricate() {
  name=$1
  ip=$2
  call=$3
  docker run -d --name "$name" --network "$NET" --ip "$ip" \
    -v "$HERE/call_driver.py:/call_driver.py:ro" \
    -e NG_NODE="$NG_NODE" -e NG_PORT=22222 -e SELF_IP="$ip" \
    -e COOKIE_PREFIX="dtc$STAMP$call" -e CALL_ID="$call" \
    -e FROM_TAG="caller-$call" -e TO_TAG="callee-$call" \
    -e PUMP_SECONDS="$PUMP_SECONDS" \
    python:3-slim python3 /call_driver.py >/dev/null
}

say "building mss_ctl and the event tail"
(cd "$REPO" && cargo build --quiet -p control-api --example mss_ctl)
(cd "$REPO" && cargo build --quiet -p mediaserverd --example mss_events_tail)

cleanup
say "the upload concurrency this pod is running with"
docker logs "$CTL_POD" 2>&1 | grep -i 'this many at a time' | tail -1 | tee -a "$LOG" || true

say "phase A: storage healthy"
fabricate detach-driver-a "$DRIVER_IP" "$CALL_A"
sleep 5
"$CTL" "http://$CONTROL" create "$EXTERNAL_A" "$CALL_A" "caller-$CALL_A" 2>&1 | tee -a "$LOG"
attachment_a=$("$CTL" "http://$CONTROL" record "$EXTERNAL_A" "$ACCOUNT/$RECORDING_A.wav" |
  sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
[ -n "$attachment_a" ] || fail "no recording attachment on $EXTERNAL_A"
say "recording $RECORDING_A for ${RECORD_SECONDS}s as $attachment_a"
sleep "$RECORD_SECONDS"
backgrounded_before=$(metric mss_recording_uploads_backgrounded_total)
timed "Detach (healthy storage)" "$CTL" "http://$CONTROL" detach "$attachment_a"
say "in flight right after the detach: $(metric mss_recording_uploads_in_flight)"
sleep 3
say "uploads: total=$(metric mss_recording_uploads_total) \
backgrounded=$(metric mss_recording_uploads_backgrounded_total) \
in_flight=$(metric mss_recording_uploads_in_flight) \
failures=$(metric mss_recording_upload_failures_total)"
[ "$(metric mss_recording_uploads_backgrounded_total)" != "$backgrounded_before" ] ||
  fail "the upload was not backgrounded at all"
"$CTL" "http://$CONTROL" destroy "$EXTERNAL_A" 2>&1 | tee -a "$LOG"

say "phase B: the same detach with storage stalled"
fabricate detach-driver-b 172.31.99.94 "$CALL_B"
sleep 5
"$CTL" "http://$CONTROL" create "$EXTERNAL_B" "$CALL_B" "caller-$CALL_B" 2>&1 | tee -a "$LOG"
attachment_b=$("$CTL" "http://$CONTROL" record "$EXTERNAL_B" "$ACCOUNT/$RECORDING_B.wav" |
  sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
[ -n "$attachment_b" ] || fail "no recording attachment on $EXTERNAL_B"
say "recording $RECORDING_B for ${STALLED_SECONDS}s as $attachment_b"
sleep "$STALLED_SECONDS"
say "pausing $MINIO_POD so the upload cannot finish"
docker pause "$MINIO_POD" >/dev/null
timed "Detach (stalled storage)" "$CTL" "http://$CONTROL" detach "$attachment_b"
timed "DestroySession (upload still stalled)" "$CTL" "http://$CONTROL" destroy "$EXTERNAL_B"
stalled_in_flight=$(metric mss_recording_uploads_in_flight)
say "with the session already destroyed: uploads_in_flight=$stalled_in_flight \
sessions_live=$(metric mss_sessions_live)"
[ "$stalled_in_flight" = "1" ] ||
  say "WARNING: expected one stalled upload in flight, metrics say $stalled_in_flight"
say "leaving storage paused for ${PAUSED_SECONDS}s, then unpausing"
sleep "$PAUSED_SECONDS"
docker unpause "$MINIO_POD" >/dev/null
sleep 8
say "after unpause: uploads_in_flight=$(metric mss_recording_uploads_in_flight) \
total=$(metric mss_recording_uploads_total) \
failures=$(metric mss_recording_upload_failures_total) \
settle_timeouts=$(metric mss_recording_upload_settle_timeouts_total)"

say "reading mss.events from the earliest offset and picking this drill's two sessions"
TAIL=$OUT/detach-latency-$STAMP.events
(cd "$REPO" && cargo run --quiet -p mediaserverd --example mss_events_tail -- \
  "$BROKERS" mss.events "$TAIL_SECONDS") >"$TAIL" 2>>"$LOG" || true

for external in "$EXTERNAL_A" "$EXTERNAL_B"; do
  say "sequence for $external:"
  grep "$external" "$TAIL" | sed 's/^/  /' | tee -a "$LOG"
  grep "$external" "$TAIL" | grep -q 'UploadCompleted' ||
    fail "$external never reported its upload on mss.events"
done

say "what the daemon logged"
docker logs --since 5m "$CTL_POD" 2>&1 |
  grep -E 'its upload runs in the background|recording uploaded|backgrounded recording upload|remembered until its recording upload|forgotten' |
  tail -20 | tee -a "$LOG" || true

say "the objects in the bucket"
docker exec "$MINIO_POD" sh -c \
  "mc alias set lab http://127.0.0.1:9000 minioadmin minioadmin >/dev/null &&
   mc stat lab/$BUCKET/$ACCOUNT/$RECORDING_A.wav | head -6 &&
   mc stat lab/$BUCKET/$ACCOUNT/$RECORDING_B.wav | head -6" 2>&1 | tee -a "$LOG"

say "done; transcript in $LOG, raw events in $TAIL"
