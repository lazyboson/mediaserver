#!/bin/sh
# Item 11: the pod-kill re-subscribe drill, a Phase-1 exit criterion.
#
# Three mediaserverd pods share one Redis. A live SIP call is tapped by pod A
# with a WS consumer attached; pod A is then killed the way a node loss kills
# it (SIGKILL, no shutdown, no lease release). One of the two survivors' keepers
# must notice the expired lease, adopt the session, re-subscribe to rtpengine
# and re-dial the same consumer endpoint -- and the other must not. The drill
# measures how long the consumer went without audio, and checks three things:
#
#   1. exactly one adopter        -- mss_registry_adopted_total, as a delta
#   2. no lease was ever lost     -- mss_registry_lost_total stays 0
#   3. no orphan subscription     -- NG query before / tapped / after
#
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
#     up -d rtpengine opensips freeswitch call-watcher redpanda redis \
#           minio minio-init llm-bridge mss-control mss-control-b mss-control-c
#   ./lab/pod_kill_drill.sh
#
# The compose "mediaserverd" service (the Phase-0 spike) must stay DOWN: it
# taps whatever call it finds from its own process.
#
# With RECORD=1 (item 53, defect D9) the call also carries a FILE_S3 recording,
# so the drill measures what a pod death costs a recording: pod A spills its
# closed segments into the recording bucket under the reserved _spill/ prefix,
# the adopter reads them back, and the final object must be within one spill
# interval plus the adoption gap of the tapped length. That needs the pods
# started with MSS_RECORDING_SPILL_TO=s3 (and a short
# MSS_RECORDING_SPILL_SECONDS, or nothing spills inside the drill's window):
#
#   printf 'MSS_RECORDING_SPILL_TO=s3\nMSS_RECORDING_SPILL_SECONDS=5\n' >>lab/.env
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
#     up -d --force-recreate mss-control mss-control-b mss-control-c
#   RECORD=1 ./lab/pod_kill_drill.sh
#
# Mind the lab's own shortcut: all three pods mount the same ./out, so
# MSS_RECORDING_SPILL_DIR is cross-pod readable *here* and a disk spill would
# look like it works. The preflight below refuses RECORD=1 unless the pod's own
# log says the journal lives in the bucket.
#
# lab/gap_consumer.py runs on the host, not in a container, because it has to
# outlive pod A and be dialable by both pods. The pods reach it at
# host.docker.internal (CONSUMER_HOST): under Docker Desktop on WSL2 the lab
# bridge gateway 172.31.99.1 belongs to the Docker VM, not to the WSL distro
# this script runs in, so the gateway address is refused and the WSL eth0
# address times out. Everything lands in lab/out/.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
OUT=${OUT:-$HERE/out}
CONTROL_A=${CONTROL_A:-http://127.0.0.1:50551}
CONTROL_B=${CONTROL_B:-http://127.0.0.1:50552}
METRICS_A=${METRICS_A:-http://127.0.0.1:9464/metrics}
METRICS_B=${METRICS_B:-http://127.0.0.1:9465/metrics}
CONTROL_C=${CONTROL_C:-http://127.0.0.1:50553}
METRICS_C=${METRICS_C:-http://127.0.0.1:9466/metrics}
POD_A=${POD_A:-mss-microsip-mss-control-1}
POD_B=${POD_B:-mss-microsip-mss-control-b-1}
POD_C=${POD_C:-mss-microsip-mss-control-c-1}
SPIKE=${SPIKE:-mss-microsip-mediaserverd-1}
REDIS=${REDIS:-mss-microsip-redis-1}
WATCHER=${WATCHER:-mss-microsip-call-watcher-1}
RTPENGINE=${RTPENGINE:-mss-microsip-rtpengine-1}
LAB_NETWORK=${LAB_NETWORK:-mss-microsip_lab}
CONSUMER_HOST=${CONSUMER_HOST:-host.docker.internal}
CONSUMER_PORT=${CONSUMER_PORT:-8095}
RECORD=${RECORD:-0}
if [ "$RECORD" = 1 ]; then
  CALL_SECONDS=${CALL_SECONDS:-190}
  TAP_SECONDS=${TAP_SECONDS:-30}
else
  CALL_SECONDS=${CALL_SECONDS:-150}
  TAP_SECONDS=${TAP_SECONDS:-20}
fi
ADOPT_TIMEOUT=${ADOPT_TIMEOUT:-60}
SETTLE_SECONDS=${SETTLE_SECONDS:-25}
DIAL=${DIAL:-9000}
SPILL_SECONDS=${SPILL_SECONDS:-5}
SPILL_PREFIX=${SPILL_PREFIX:-_spill/}
RECORD_ACCOUNT=${RECORD_ACCOUNT:-acct-kill}
RECORD_RATE=${RECORD_RATE:-8000}
RECORD_CHANNELS=${RECORD_CHANNELS:-2}
BUCKET=${BUCKET:-lab-recordings}
DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
export DOCKER_API_VERSION

STAMP=$(date +%s)
EXTERNAL_ID=killdrill-$STAMP
LOG=$OUT/pod-kill-drill-$STAMP.log
CONSUMER=ws://$CONSUMER_HOST:$CONSUMER_PORT/ws
CALL_ID=""
FROM_TAGS=""
GAP=""
CALLER=""
RECORDER=""
RECORDING=${RECORDING:-rec-$STAMP}
ENDPOINT="$RECORD_ACCOUNT/$RECORDING.wav"
TAP_STARTED_AT=""
KILLED_AT=""
ADOPTED_AT=""
DESTROYED_AT=""

mkdir -p "$OUT"
say() { echo "drill: $*" | tee -a "$LOG"; }
metric() {
  value=$(curl -s "$1" | sed -n "s/^$2 //p" | head -1)
  echo "${value:-0}"
}
# ng_call_tags.py exits with the number of taps it can see on the call, so the
# assertion is the exit code and the log is the evidence. Mind its blind spot,
# documented there: a *lone* subscription is invisible to NG query.
tags() {
  set +e
  docker run --rm --network "$LAB_NETWORK" -v "$HERE:/lab" -w /lab \
    -e CALL_ID="$CALL_ID" -e LEGS="$FROM_TAGS" -e LABEL="$1" \
    python:3-slim python ng_call_tags.py >"$OUT/ng-$1-$STAMP.txt" 2>&1
  found=$?
  set -e
  cat "$OUT/ng-$1-$STAMP.txt" | tee -a "$LOG"
  SUBSCRIPTIONS=$found
}

echo "drill: session $EXTERNAL_ID, consumer $CONSUMER, log $LOG"

# --- preflight: the shape of the lab is half of this drill's validity -------
if docker ps --format '{{.Names}}' | grep -qx "$SPIKE"; then
  say "the Phase-0 spike container $SPIKE is running; it would tap the same call"
  exit 1
fi
# Pod A is the one that dies, so any run -- including one that ends early --
# leaves it dead. Bring it back before the next: docker compose up -d mss-control.
for pod in "$POD_A" "$POD_B"; do
  if ! docker ps --format '{{.Names}}' | grep -qx "$pod"; then
    say "$pod is not running; bring up mss-control and mss-control-b first"
    exit 1
  fi
done
if ! docker ps --format '{{.Names}}' | grep -qx "$POD_C"; then
  say "$POD_C is not running; the drill will run with one survivor, which makes \
'exactly one adopter' a tautology. Bring up mss-control-c to race two."
  CONTROL_C=""
  METRICS_C=""
fi
for endpoint in "$METRICS_A" "$METRICS_B" $METRICS_C; do
  if ! curl -s -o /dev/null "$endpoint"; then
    say "$endpoint does not answer; is that pod's MSS_METRICS_LISTEN published?"
    exit 1
  fi
done
MINIO=${MINIO:-$(docker ps --filter name=minio --format '{{.Names}}' | head -1)}
if [ "$RECORD" = 1 ]; then
  if ! docker logs "$POD_A" 2>&1 |
       grep -q 'spill into the recording bucket itself'; then
    say "RECORD=1 needs the pods started with MSS_RECORDING_SPILL_TO=s3, or a \
cross-pod adopter reads nothing (and this lab's shared ./out mount would hide \
that). See the header of this script."
    exit 1
  fi
  if [ -z "$MINIO" ]; then
    say "RECORD=1 needs minio up to read the object back"
    exit 1
  fi
  say "recording this call to $ENDPOINT, spill prefix $SPILL_PREFIX in bucket $BUCKET"
fi
BASE_ADOPTED_B=$(metric "$METRICS_B" mss_registry_adopted_total)
BASE_LOST_B=$(metric "$METRICS_B" mss_registry_lost_total)
BASE_ADOPTED_C=0
BASE_LOST_C=0
if [ -n "$CONTROL_C" ]; then
  BASE_ADOPTED_C=$(metric "$METRICS_C" mss_registry_adopted_total)
  BASE_LOST_C=$(metric "$METRICS_C" mss_registry_lost_total)
fi
say "baseline: pod B adopted=$BASE_ADOPTED_B lost=$BASE_LOST_B, \
pod C adopted=$BASE_ADOPTED_C lost=$BASE_LOST_C, \
pod A lost=$(metric "$METRICS_A" mss_registry_lost_total)"
say "sessions already in the registry: \
$(docker exec "$REDIS" redis-cli --raw smembers mss:sessions | tr '\n' ' ')"

cd "$REPO"
say "building mss_ctl before the call so nothing compiles mid-drill"
cargo build --quiet -p control-api --examples

cleanup() {
  [ -n "$GAP" ] && kill "$GAP" 2>/dev/null || true
  [ -n "$CALLER" ] && kill "$CALLER" 2>/dev/null || true
}
trap cleanup EXIT

# --- the consumer, on the host so it outlives pod A ------------------------
GAP_STAMP=$STAMP OUT_DIR=$OUT GAP_PORT=$CONSUMER_PORT \
  python3 "$HERE/gap_consumer.py" >"$OUT/gap-consumer-$STAMP.log" 2>&1 &
GAP=$!
sleep 1
if ! kill -0 "$GAP" 2>/dev/null; then
  say "the gap consumer did not start; see $OUT/gap-consumer-$STAMP.log"
  exit 1
fi

# --- a live call ----------------------------------------------------------
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

tags before-any-tap
BEFORE=$SUBSCRIPTIONS

# --- pod A taps it, with a WS consumer ------------------------------------
cargo run --quiet -p control-api --example mss_ctl -- \
    "$CONTROL_A" create "$EXTERNAL_ID" "$CALL_ID" "$FROM_TAGS" 2>&1 | tee -a "$LOG"
cargo run --quiet -p control-api --example mss_ctl -- \
    "$CONTROL_A" attach "$EXTERNAL_ID" "$CONSUMER" gapmeter authoritative 2>&1 | tee -a "$LOG"
if [ "$RECORD" = 1 ]; then
  RECORDER=$(cargo run --quiet -p control-api --example mss_ctl -- \
    "$CONTROL_A" record "$EXTERNAL_ID" "$ENDPOINT" 2>&1 | tee -a "$LOG" |
    sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
  say "recorder attachment $RECORDER on pod A"
fi
TAP_STARTED_AT=$(date +%s.%N)

say "letting pod A's tap run for ${TAP_SECONDS}s"
sleep "$TAP_SECONDS"
tags tapped-by-pod-A
TAPPED=$SUBSCRIPTIONS
say "lease before the kill: owner=$(docker exec "$REDIS" redis-cli --raw \
  get "mss:lease:$EXTERNAL_ID") ttl=$(docker exec "$REDIS" redis-cli --raw \
  ttl "mss:lease:$EXTERNAL_ID")"
say "pod A delivered $(metric "$METRICS_A" mss_consumer_delivered_total) frames, \
sessions_live=$(metric "$METRICS_A" mss_sessions_live) \
legs_live=$(metric "$METRICS_A" mss_legs_live)"
BASE_SEGMENTS_A=0
if [ "$RECORD" = 1 ]; then
  BASE_SEGMENTS_A=$(metric "$METRICS_A" mss_recording_spill_segments_total)
  say "pod A spilled $BASE_SEGMENTS_A closed segments before the kill, \
recordings_live=$(metric "$METRICS_A" mss_recordings_live)"
  if [ "$BASE_SEGMENTS_A" = "0" ]; then
    say "nothing had spilled yet: raise TAP_SECONDS or lower \
MSS_RECORDING_SPILL_SECONDS, or this drill measures nothing about D9"
  fi
fi

# --- the kill: SIGKILL to the container, which is a node loss -------------
docker logs "$POD_A" >"$OUT/pod-a-$STAMP.log" 2>&1 || true
KILLED_AT=$(date +%s.%N)
docker kill --signal=KILL "$POD_A" >/dev/null
say "killed $POD_A at $KILLED_AT (SIGKILL, no lease release)"

# --- B and C race for it; exactly one must win -----------------------------
adopted_b=$BASE_ADOPTED_B
adopted_c=$BASE_ADOPTED_C
waited=0
while [ "$waited" -lt "$ADOPT_TIMEOUT" ]; do
  adopted_b=$(metric "$METRICS_B" mss_registry_adopted_total)
  [ -n "$CONTROL_C" ] && adopted_c=$(metric "$METRICS_C" mss_registry_adopted_total)
  if [ "$adopted_b" != "$BASE_ADOPTED_B" ] || [ "$adopted_c" != "$BASE_ADOPTED_C" ]; then
    break
  fi
  waited=$((waited + 1))
  sleep 1
done
ADOPTED_AT=$(date +%s.%N)
ADOPTERS=$(echo "$adopted_b $BASE_ADOPTED_B $adopted_c $BASE_ADOPTED_C" |
  awk '{print ($1 - $2) + ($3 - $4)}')
if [ "$ADOPTERS" = "0" ]; then
  say "neither survivor adopted the session within ${ADOPT_TIMEOUT}s -- FAILED"
else
  say "adopted $ADOPTERS session(s) \
$(echo "$ADOPTED_AT $KILLED_AT" | awk '{printf "%.1f", $1 - $2}')s after the kill \
(pod B $BASE_ADOPTED_B -> $adopted_b, pod C $BASE_ADOPTED_C -> $adopted_c)"
fi

sleep "$SETTLE_SECONDS"
tags after-adoption
AFTER=$SUBSCRIPTIONS
OWNER=$(docker exec "$REDIS" redis-cli --raw get "mss:lease:$EXTERNAL_ID")
say "lease after the adoption: owner=$OWNER"
registry() {
  say "$1: adopted=$(metric "$2" mss_registry_adopted_total) \
lost=$(metric "$2" mss_registry_lost_total) \
unrebuildable=$(metric "$2" mss_registry_unrebuildable_total) \
failed=$(metric "$2" mss_registry_failed_total) \
sessions_live=$(metric "$2" mss_sessions_live) \
legs_live=$(metric "$2" mss_legs_live) \
delivered=$(metric "$2" mss_consumer_delivered_total)"
}
registry "pod B" "$METRICS_B"
[ -n "$CONTROL_C" ] && registry "pod C" "$METRICS_C"
WINNER=$CONTROL_B
WINNER_METRICS=$METRICS_B
if [ "$OWNER" = "lab-control-c" ]; then
  WINNER=$CONTROL_C
  WINNER_METRICS=$METRICS_C
fi
say "the surviving owner is $OWNER at $WINNER"
curl -s "$WINNER_METRICS" |
  grep -E '^mss_(registry|consumer|legs|sessions|taps|ingest)' | tee -a "$LOG"

say "rtpengine's own view of this call's control traffic \
(two subscribe requests from two pod addresses, and no unsubscribe for the \
dead one, is what an orphan looks like):"
docker logs --since 10m "$RTPENGINE" 2>&1 | grep -F "$CALL_ID" |
  grep -iE "'(un)?subscribe" | tail -30 | tee -a "$LOG" || true

# --- wind down ------------------------------------------------------------
wait "$CALLER" 2>/dev/null || true
CALLER=""
cargo run --quiet -p control-api --example mss_ctl -- \
    "$WINNER" destroy "$EXTERNAL_ID" 2>&1 | tee -a "$LOG" || true
DESTROYED_AT=$(date +%s.%N)
tags after-destroy

# rtpengine's teardown summary is the instrument that settles the orphan
# question, because `query` cannot: on 14.1.1.8 a *lone* subscription is
# invisible in `query` (it shows up as a tag, and in each leg's `subscribers`
# list, only once a second subscribe touches the call), and `stats_out` in a
# query reply is not live. The "Final packet stats" block, by contrast, names
# every mss-tap monologue with the address it was fed and the packets it was
# sent -- so a tap pointing at the dead pod's address with a large `out` count
# is the orphan, measured.
say "rtpengine's final packet stats for this call (look for two 'mss-tap' \
tags; the one pointing at the dead pod's address is the orphan):"
docker logs --since 10m "$RTPENGINE" 2>&1 | grep -F "$CALL_ID" |
  grep -E "Final packet stats|--- Tag |--------- Port" | tail -40 | tee -a "$LOG" || true

kill -TERM "$GAP" 2>/dev/null || true
wait "$GAP" 2>/dev/null || true
GAP=""
say "the consumer's measurement:"
grep -E 'track |connection ' "$OUT/gap-consumer-$STAMP.log" | tee -a "$LOG" || true
docker logs "$POD_B" >"$OUT/pod-b-$STAMP.log" 2>&1 || true
[ -n "$CONTROL_C" ] && { docker logs "$POD_C" >"$OUT/pod-c-$STAMP.log" 2>&1 || true; }
grep -h 'tap leg finished' "$OUT/pod-b-$STAMP.log" "$OUT/pod-c-$STAMP.log" 2>/dev/null |
  tail -4 | tee -a "$LOG" || true

RECORD_VERDICT=""
if [ "$RECORD" = 1 ]; then
  say "waiting for the adopter's upload to settle"
  waited=0
  while [ "$waited" -lt 60 ]; do
    if docker exec "$MINIO" sh -c \
        "mc alias set lab http://127.0.0.1:9000 minioadmin minioadmin >/dev/null &&
         mc stat lab/$BUCKET/$ENDPOINT" >"$OUT/rec-stat-$STAMP.txt" 2>&1; then
      break
    fi
    waited=$((waited + 1))
    sleep 1
  done
  cat "$OUT/rec-stat-$STAMP.txt" | tee -a "$LOG"
  SIZE=$(sed -n 's/^Size *: *//p' "$OUT/rec-stat-$STAMP.txt" | head -1)
  BYTES=$(docker exec "$MINIO" sh -c \
    "mc alias set lab http://127.0.0.1:9000 minioadmin minioadmin >/dev/null &&
     mc ls --json lab/$BUCKET/$ENDPOINT" 2>/dev/null |
    sed -n 's/.*"size":\([0-9]*\).*/\1/p' | head -1)
  BYTES=${BYTES:-0}
  RECORDED=$(echo "$BYTES $RECORD_RATE $RECORD_CHANNELS" |
    awk '{ if ($1 > 44) printf "%.2f", ($1 - 44) / (2 * $3 * $2); else print "0" }')
  TAPPED=$(echo "$DESTROYED_AT $TAP_STARTED_AT" | awk '{printf "%.2f", $1 - $2}')
  GAP_SECONDS=$(echo "$ADOPTED_AT $KILLED_AT" | awk '{printf "%.2f", $1 - $2}')
  ALLOWED=$(echo "$SPILL_SECONDS $GAP_SECONDS" | awk '{printf "%.2f", $1 + $2 + 2}')
  MISSING=$(echo "$TAPPED $RECORDED" | awk '{printf "%.2f", $1 - $2}')
  say "recording $ENDPOINT: size=${SIZE:-$BYTES bytes} -> ${RECORDED}s of audio \
against ${TAPPED}s tapped; missing ${MISSING}s, allowed ${ALLOWED}s \
(one ${SPILL_SECONDS}s spill interval + the ${GAP_SECONDS}s adoption gap + 2s slack)"
  say "the adopter's recording counters: \
frames_lost_on_adopt=$(metric "$WINNER_METRICS" mss_recording_frames_lost_on_adopt_total) \
spill_segments=$(metric "$WINNER_METRICS" mss_recording_spill_segments_total) \
spill_lost_ownership=$(metric "$WINNER_METRICS" mss_recording_spill_lost_ownership_total) \
spill_foreign_manifests=$(metric "$WINNER_METRICS" mss_recording_spill_foreign_manifests) \
uploads=$(metric "$WINNER_METRICS" mss_recording_uploads_total) \
upload_failures=$(metric "$WINNER_METRICS" mss_recording_upload_failures_total)"
  say "what is left under the reserved spill namespace (it must be empty, \
because the adopter finished the object and discarded the journal):"
  docker exec "$MINIO" sh -c \
    "mc alias set lab http://127.0.0.1:9000 minioadmin minioadmin >/dev/null &&
     mc ls -r lab/$BUCKET/$SPILL_PREFIX" 2>&1 | tee -a "$LOG" || true
  RECORD_VERDICT=$(echo "$MISSING $ALLOWED" |
    awk '{ if ($1 <= $2) print "PASS"; else print "FAILED" }')
fi

say "=== the three assertions ==="
say "1. exactly one adopter: $ADOPTERS (pod B $BASE_ADOPTED_B -> \
$(metric "$METRICS_B" mss_registry_adopted_total), pod C $BASE_ADOPTED_C -> \
$(metric "$METRICS_C" mss_registry_adopted_total))"
say "2. no lease lost: pod B $BASE_LOST_B -> \
$(metric "$METRICS_B" mss_registry_lost_total), pod C $BASE_LOST_C -> \
$(metric "$METRICS_C" mss_registry_lost_total)"
say "3. taps visible in NG query: before=$BEFORE tapped-by-A=$TAPPED \
after-adoption=$AFTER after-destroy=$SUBSCRIPTIONS. after-adoption 2 means \
pod A's subscription was left behind; and tapped-by-A reads 0 because a lone \
subscription is invisible to query (see the final-packet-stats block above, \
which is the honest instrument)"
if [ "$RECORD" = 1 ]; then
  say "4. the recording survived the kill: $RECORD_VERDICT -- ${RECORDED}s of \
${TAPPED}s, missing ${MISSING}s against an allowance of ${ALLOWED}s. Put this \
number in lab.md; it is the live pod-kill-with-a-recorder drill D9 has owed \
since item 30"
fi
say "done. artifacts in $OUT: gap-$STAMP-*.wav, gap-$STAMP-summary.json, $LOG"
say "restart pod A with: docker compose -f $HERE/docker-compose.microsip.yml \
up -d mss-control"
