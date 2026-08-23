#!/bin/sh
# Items P3-4 + P3-5: the inline leg, end to end, with no human and no SIP.
#
# A plain RTP peer (lab/inline_peer.py) offers PCMU; MSS answers it with
# `mss_ctl inline`, so the media anchors on MSS's own socket and nothing else
# is in the path -- no rtpengine, no FreeSWITCH. A gRPC consumer
# (lab/inline_consumer.py) attaches with SINK+INJECT and speaks a 1000 Hz tone
# into the leg while the peer speaks 440 Hz back, then barges its own speech
# away with Clear, twelve times over. lab/inline_barge_report.py reads the
# peer's arrival timeline and prints the cut-through distribution.
#
# Both python actors run as containers on the lab network so their timestamps
# come from one kernel clock: the barge number is a difference between a stamp
# taken in the consumer and a stamp taken in the peer, and a host-to-VM clock
# skew of a few milliseconds would be a large error against a 20 ms target.
#
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
#     up -d rtpengine redpanda redis minio minio-init mss-control
#   ./lab/inline_call_drill.sh
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
NET=${NET:-mss-microsip_lab}
CONTROL=${CONTROL:-127.0.0.1:50551}
CONTROL_IN_NET=${CONTROL_IN_NET:-172.31.99.31:50551}
METRICS=${METRICS:-127.0.0.1:9464}
PEER_IP=${PEER_IP:-172.31.99.124}
PEER_PORT=${PEER_PORT:-41000}
TONE_HZ=${TONE_HZ:-440}
INJECT_HZ=${INJECT_HZ:-1000}
ITERATIONS=${ITERATIONS:-20}
TONE_SECONDS=${TONE_SECONDS:-1.5}
GAP_SECONDS=${GAP_SECONDS:-1.0}
LEAD_MS=${LEAD_MS:-400}
STAMP=$(date +%s)
EXTERNAL_ID=${EXTERNAL_ID:-inline-$STAMP}
CALL_ID=${CALL_ID:-inline-call-$STAMP}
IO_DIR=${IO_DIR:-$REPO/lab/out/inline-$STAMP}

export DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
CTL="$REPO/target/debug/examples/mss_ctl"

say() { printf 'inline-drill: %s\n' "$*"; }

cleanup() {
  say "cleaning up"
  docker stop -t 3 inline-peer >/dev/null 2>&1 || true
  docker logs inline-peer 2>&1 | tail -6 || true
  docker rm -f inline-peer inline-consumer >/dev/null 2>&1 || true
  [ -n "${SESSION:-}" ] && "$CTL" "http://$CONTROL" destroy "$EXTERNAL_ID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$IO_DIR" "$REPO/lab/out/pb"
chmod 777 "$IO_DIR"

say "generating python stubs"
python3 -m grpc_tools.protoc -I"$REPO/proto" \
  --python_out="$REPO/lab/out/pb" --grpc_python_out="$REPO/lab/out/pb" \
  "$REPO/proto/mediacontrol.proto" "$REPO/proto/mediastream.proto"

say "building mss_ctl"
(cd "$REPO" && cargo build --quiet -p control-api --example mss_ctl)

say "starting the rtp peer at $PEER_IP:$PEER_PORT ($TONE_HZ Hz)"
docker rm -f inline-peer >/dev/null 2>&1 || true
docker run -d --name inline-peer --network "$NET" --ip "$PEER_IP" \
  -v "$HERE/inline_peer.py:/inline_peer.py:ro" -v "$IO_DIR:/io" \
  -e IO_DIR=/io -e PEER_IP="$PEER_IP" -e PEER_PORT="$PEER_PORT" \
  -e TONE_HZ="$TONE_HZ" -e INJECT_HZ="$INJECT_HZ" -e RUN_SECONDS=300 \
  python:3-slim python3 /inline_peer.py >/dev/null

say "waiting for the peer's offer"
i=0
while [ ! -f "$IO_DIR/offer.sdp" ]; do
  i=$((i + 1))
  [ "$i" -gt 60 ] && { say "the peer never wrote an offer"; docker logs inline-peer; exit 1; }
  sleep 1
done

say "creating the INLINE session $EXTERNAL_ID from that offer"
ANSWER=$("$CTL" "http://$CONTROL" inline "$EXTERNAL_ID" "$CALL_ID" "$IO_DIR/offer.sdp" |
  sed '1s/^ok: //')
case "$ANSWER" in
  v=0*) ;;
  *) say "mss_ctl did not answer the offer: $ANSWER"; exit 1 ;;
esac
printf '%s\n' "$ANSWER" > "$IO_DIR/answer.sdp"
SESSION=$EXTERNAL_ID
say "MSS answered: $(printf '%s' "$ANSWER" | tr -d '\r' | sed -n 's/^m=audio /m=audio /p')"
say "$(printf '%s' "$ANSWER" | tr -d '\r' | sed -n 's/^c=/c=/p')"

say "waiting for the peer to start pumping"
i=0
while [ ! -f "$IO_DIR/peer-ready" ]; do
  i=$((i + 1))
  [ "$i" -gt 30 ] && { say "the peer never became ready"; docker logs inline-peer; exit 1; }
  sleep 1
done
sleep 2

say "attaching the injecting consumer ($INJECT_HZ Hz, $ITERATIONS barges)"
docker rm -f inline-consumer >/dev/null 2>&1 || true
docker run -d --name inline-consumer --network "$NET" \
  -v "$HERE/inline_consumer.py:/inline_consumer.py:ro" \
  -v "$REPO/lab/out/pb:/pb:ro" -v "$IO_DIR:/io" \
  -e IO_DIR=/io -e STUBS=/pb -e CONTROL="$CONTROL_IN_NET" \
  -e EXTERNAL_ID="$EXTERNAL_ID" -e INJECT_HZ="$INJECT_HZ" -e PEER_HZ="$TONE_HZ" \
  -e ITERATIONS="$ITERATIONS" -e TONE_SECONDS="$TONE_SECONDS" \
  -e GAP_SECONDS="$GAP_SECONDS" -e LEAD_MS="$LEAD_MS" \
  -e MSS_AUTH_TOKEN="${MSS_AUTH_TOKEN:-}" \
  -e PIP_DISABLE_PIP_VERSION_CHECK=1 \
  python:3-slim sh -c 'pip install --quiet grpcio protobuf && python3 /inline_consumer.py' \
  >/dev/null

say "waiting for the consumer to finish"
STATUS=$(docker wait inline-consumer)
docker logs inline-consumer 2>&1 | tail -25
[ "$STATUS" = "0" ] || { say "the consumer exited $STATUS"; exit 1; }

say "stopping the peer so it writes its ear"
docker stop -t 5 inline-peer >/dev/null 2>&1 || true
docker logs inline-peer 2>&1 | tail -6

say "inline egress counters on the pod"
curl -s "http://$METRICS/metrics" | grep -E '^mss_inline' || say "no inline metrics"

say "analysing $IO_DIR"
python3 "$HERE/inline_barge_report.py" "$IO_DIR"
say "artifacts in $IO_DIR"
say "done"
