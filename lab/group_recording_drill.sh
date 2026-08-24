#!/bin/sh
# Item 21 (+ P2-1): proves a recording group across two calls, the second
# member joining JOIN_STAGGER_SECONDS late so the group time anchor is
# exercised: both participant objects must come back the same length. Two
# fabricated calls
# (lab/call_driver.py, no SIP) become two MSS sessions on one pod, each
# attaching a FILE_S3 recording with the same `group`, so the conference is
# one recording with one object per participant.
#
# It runs BESIDE a live lab: it adds its own containers on free IPs
# (172.31.99.120-122) and its own daemon, so nothing existing is touched.
# The lab needs rtpengine and minio up:
#
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
#     up -d rtpengine minio minio-init
#   ./lab/group_recording_drill.sh
#
# The mediaserverd binary is built in mss-lab-rust:1.95 (libopus needs cmake,
# make and g++) into its own target volume, so a running lab pod's artifacts
# are never overwritten. mss_ctl runs from the host against the published
# control port.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
NET=${NET:-mss-microsip_lab}
NG_NODE=${NG_NODE:-172.31.99.10}
MINIO=${MINIO:-172.31.99.62:9000}
BUCKET=${BUCKET:-lab-recordings}
IMAGE=${IMAGE:-mss-lab-rust:1.95}
TARGET_VOLUME=${TARGET_VOLUME:-mss-group-target}
REGISTRY_VOLUME=${REGISTRY_VOLUME:-${NET}-registry}
CONTROL_PORT=${CONTROL_PORT:-19090}
METRICS_PORT=${METRICS_PORT:-19091}
RECORD_SECONDS=${RECORD_SECONDS:-20}
JOIN_STAGGER_SECONDS=${JOIN_STAGGER_SECONDS:-5}
ACCOUNT=${ACCOUNT:-acct-conf}
GROUP=${GROUP:-conf-drill}
STAMP=$(date +%s)
RECORDING=${RECORDING:-rec-$STAMP}
ENDPOINT="$ACCOUNT/$RECORDING.wav"

export DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
CONTROL=http://127.0.0.1:$CONTROL_PORT
METRICS=http://127.0.0.1:$METRICS_PORT/metrics
CTL="$REPO/target/debug/examples/mss_ctl"

say() { printf 'group-drill: %s\n' "$*"; }

DRILL_REACHED_END=0
cleanup() {
  if [ "$DRILL_REACHED_END" = 0 ]; then
    say "the drill did not finish; the pod's own last words follow"
    docker logs --tail 40 mss-group-pod 2>&1 | sed 's/^/pod: /' || true
  fi
  say "cleaning up"
  docker rm -f mss-group-pod group-driver-a group-driver-b >/dev/null 2>&1 || true
  docker volume rm "$TARGET_VOLUME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

say "building mediaserverd in $IMAGE (own target volume $TARGET_VOLUME)"
docker volume create "$TARGET_VOLUME" >/dev/null
docker run --rm --network "$NET" \
  -v "$REPO:/build:ro" -v "$TARGET_VOLUME:/target" -v "$REGISTRY_VOLUME:/usr/local/cargo/registry" \
  -w /build -e CARGO_TARGET_DIR=/target \
  "$IMAGE" cargo build --quiet -p mediaserverd
say "building mss_ctl on the host"
(cd "$REPO" && cargo build --quiet -p control-api --example mss_ctl)

# One cookie prefix per driver: rtpengine caches replies per cookie, so two
# drivers both starting at "lab-1" get each other's answers (the D12 shape).
say "fabricating two calls"
for leg in a b; do
  case $leg in
    a) ip=172.31.99.120; tags="gaA gaB" ;;
    b) ip=172.31.99.121; tags="gbA gbB" ;;
  esac
  set -- $tags
  docker run -d --name "group-driver-$leg" --network "$NET" --ip "$ip" \
    -v "$HERE/call_driver.py:/call_driver.py:ro" \
    -e NG_NODE="$NG_NODE" -e NG_PORT=22222 -e SELF_IP="$ip" \
    -e COOKIE_PREFIX="grp$leg" -e CALL_ID="group-call-$leg" \
    -e FROM_TAG="$1" -e TO_TAG="$2" -e PUMP_SECONDS=420 \
    python:3-slim python3 /call_driver.py >/dev/null
done
sleep 5
docker logs group-driver-a 2>&1 | tail -2
docker logs group-driver-b 2>&1 | tail -2

say "starting a pod of its own at 172.31.99.122"
docker run -d --name mss-group-pod --network "$NET" --ip 172.31.99.122 \
  -p "127.0.0.1:$CONTROL_PORT:9090" -p "127.0.0.1:$METRICS_PORT:9091" \
  -v "$REPO:/build:ro" -v "$TARGET_VOLUME:/target" -v "$REGISTRY_VOLUME:/usr/local/cargo/registry" \
  -w /build -e CARGO_TARGET_DIR=/target -e RUST_LOG=info \
  -e MSS_RTPENGINE_NODE="$NG_NODE:22222" -e MSS_TAP_LOCAL_IP=172.31.99.122 \
  -e MSS_CONTROL_LISTEN=0.0.0.0:9090 -e MSS_METRICS_LISTEN=0.0.0.0:9091 \
  -e MSS_POD_NAME=pod-group \
  -e MSS_RECORDING_BUCKET="$BUCKET" -e MSS_RECORDING_S3_ENDPOINT="http://$MINIO" \
  -e MSS_RECORDING_S3_REGION=us-east-1 -e MSS_RECORDING_S3_ACCESS_KEY_ID=minioadmin \
  -e MSS_RECORDING_S3_SECRET_ACCESS_KEY=minioadmin \
  "$IMAGE" cargo run --quiet -p mediaserverd >/dev/null
until curl -sf "$METRICS" >/dev/null 2>&1; do sleep 1; done

say "tapping both calls and joining them to group $GROUP as $ENDPOINT"
"$CTL" "$CONTROL" create conf-alice group-call-a gaA,gaB
"$CTL" "$CONTROL" create conf-bob group-call-b gbA,gbB
alice=$("$CTL" "$CONTROL" record conf-alice "$ENDPOINT" alice "$GROUP" customer |
  sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
say "staggering bob's join by ${JOIN_STAGGER_SECONDS}s: P2-1 pads bob's file back to the group anchor"
sleep "$JOIN_STAGGER_SECONDS"
bob=$("$CTL" "$CONTROL" record conf-bob "$ENDPOINT" bob "$GROUP" customer |
  sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
say "members: alice=$alice bob=$bob"

say "a reused label and a second recording id must both be refused"
"$CTL" "$CONTROL" record conf-bob "$ENDPOINT" alice "$GROUP" customer || true
"$CTL" "$CONTROL" record conf-bob "$ACCOUNT/rec-other.wav" carol "$GROUP" customer || true

curl -s "$METRICS" | grep -E '^mss_recording_group|^mss_recordings_live|^mss_sessions_live'
say "recording for $RECORD_SECONDS s"
sleep "$RECORD_SECONDS"

say "detaching both members, which uploads both participant files"
"$CTL" "$CONTROL" detach "$alice"
"$CTL" "$CONTROL" detach "$bob"
curl -s "$METRICS" | grep -E '^mss_recording_group|^mss_recording_uploads_total|^mss_recordings_live'

say "what landed in the bucket"
docker exec "$(docker ps --filter name=minio --format '{{.Names}}' | head -1)" sh -c \
  "mc alias set lab http://127.0.0.1:9000 minioadmin minioadmin >/dev/null &&
   mc ls -r lab/$BUCKET/$ACCOUNT/$RECORDING/ &&
   mc stat lab/$BUCKET/$ACCOUNT/$RECORDING/alice.wav &&
   mc stat lab/$BUCKET/$ACCOUNT/$RECORDING/bob.wav"
say "both members must be the same length: a staggered join is padded, not shifted"
say "the frozen two-leg identity must NOT exist for a grouped recording"
docker exec "$(docker ps --filter name=minio --format '{{.Names}}' | head -1)" sh -c \
  "mc stat lab/$BUCKET/$ENDPOINT" && say "UNEXPECTED: $ENDPOINT exists" || say "absent, as it should be"

docker logs mss-group-pod 2>&1 | grep -E 'recording group|recording this call|recording uploaded|opens with silence'
DRILL_REACHED_END=1
say "done"
