#!/bin/sh
# Item P4-6: the whole Phase-4 conference, on real sockets, with no human.
#
# Three lab/inline_peer.py containers offer PCMU and are joined into ONE
# conference by `mss_ctl inline <id> <call> <offer> <group>` -- three INLINE
# legs sharing one MixMatrix in the capture world, with FreeSWITCH nowhere in
# the path and rtpengine nowhere either. Each peer sends its own tone
# (440/880/1320 Hz) and records a Goertzel per tone per arriving packet, so
# "A hears the other two and not itself" is three numbers on one packet.
#
# Then, in phases stamped on one kernel clock:
#
#   pair     A and B alone, so a two-party mix is judged before a three-party
#   three    C joins late -- also the recording group's anchor-pad evidence
#   whisper  an INJECT consumer with mix_target=B speaks a 4th tone (1760 Hz)
#   barge    the same consumer flips its route to all
#   mute     member A is muted by metadata on one of A's attachments
#   unmute   and restored
#
# A monitor consumer (SINK, selector only=mixed) listens to the room the whole
# way through, and two recordings run at once: the room as ONE mixed object,
# and a recording group with one object per participant.
#
# lab/conference_report.py judges the ear timelines against the phase windows,
# and the pulled recordings against their own expectations. Nothing here is a
# human listening to anything.
#
# The lab needs the compose stack up:
#   DOCKER_API_VERSION=1.43 ELEVENLABS_API_KEY=x DEEPGRAM_API_KEY=x \
#     docker compose -f lab/docker-compose.microsip.yml \
#     up -d redpanda redis minio minio-init mss-control
#   ./lab/conference_drill.sh
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
NET=${NET:-mss-microsip_lab}
CONTROL=${CONTROL:-127.0.0.1:50551}
CONTROL_IN_NET=${CONTROL_IN_NET:-172.31.99.31:50551}
METRICS=${METRICS:-127.0.0.1:9464}
MINIO=${MINIO:-172.31.99.62:9000}
BUCKET=${BUCKET:-lab-recordings}
PHASE_SECONDS=${PHASE_SECONDS:-8}
LENGTH_TOLERANCE_MS=${LENGTH_TOLERANCE_MS:-600}
WHISPER_SECONDS=${WHISPER_SECONDS:-8}
BARGE_SECONDS=${BARGE_SECONDS:-8}
EAR_TONES=${EAR_TONES:-440,880,1320,1760}
# Four sources at once must stay under full scale: the mixer has no AGC, and a
# clipped sum intermodulates onto exactly the harmonics being measured.
TONE_AMPLITUDE=${TONE_AMPLITUDE:-6000}
WHISPER_HZ=${WHISPER_HZ:-1760}
STAMP=$(date +%s)
GROUP=${GROUP:-conf-$STAMP}
ACCOUNT=${ACCOUNT:-acct-conf}
ROOM_KEY="$ACCOUNT/room-$STAMP.wav"
PARTY_KEY="$ACCOUNT/party-$STAMP.wav"
IO_DIR=${IO_DIR:-$REPO/lab/out/conference-$STAMP}

export DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
CTL="$REPO/target/debug/examples/mss_ctl"
MINIO_CTR=$(docker ps --filter name=minio --format '{{.Names}}' | head -1)

say() { printf 'conference-drill: %s\n' "$*"; }
now() { date +%s.%N; }
attachment_of() { sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p'; }

PEERS="a b c"
peer_ip() {
  case $1 in
    a) echo 172.31.99.131 ;;
    b) echo 172.31.99.132 ;;
    c) echo 172.31.99.133 ;;
  esac
}
peer_port() {
  case $1 in
    a) echo 41100 ;;
    b) echo 41200 ;;
    c) echo 41300 ;;
  esac
}
peer_hz() {
  case $1 in
    a) echo 440 ;;
    b) echo 880 ;;
    c) echo 1320 ;;
  esac
}

REACHED_END=0
cleanup() {
  if [ "$REACHED_END" = 0 ]; then
    say "the drill did not finish; the pod's own last words follow"
    docker logs --tail 40 mss-microsip-mss-control-1 2>&1 | sed 's/^/pod: /' || true
  fi
  say "cleaning up"
  for peer in $PEERS; do
    docker rm -f "conf-peer-$peer" >/dev/null 2>&1 || true
    "$CTL" "http://$CONTROL" destroy "conf-$STAMP-$peer" >/dev/null 2>&1 || true
  done
  docker rm -f conf-monitor conf-injector >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$IO_DIR" "$REPO/lab/out/pb"
chmod 777 "$IO_DIR"
for peer in $PEERS; do
  mkdir -p "$IO_DIR/peer-$peer"
  chmod 777 "$IO_DIR/peer-$peer"
done

say "generating python stubs"
python3 -m grpc_tools.protoc -I"$REPO/proto" \
  --python_out="$REPO/lab/out/pb" --grpc_python_out="$REPO/lab/out/pb" \
  "$REPO/proto/mediacontrol.proto" "$REPO/proto/mediastream.proto"

say "building mss_ctl"
(cd "$REPO" && cargo build --quiet -p control-api --example mss_ctl)

start_peer() {
  peer=$1
  ip=$(peer_ip "$peer")
  port=$(peer_port "$peer")
  hz=$(peer_hz "$peer")
  say "peer $peer: $hz Hz at $ip:$port"
  docker rm -f "conf-peer-$peer" >/dev/null 2>&1 || true
  docker run -d --name "conf-peer-$peer" --network "$NET" --ip "$ip" \
    -v "$HERE/inline_peer.py:/inline_peer.py:ro" -v "$IO_DIR/peer-$peer:/io" \
    -e IO_DIR=/io -e PEER_IP="$ip" -e PEER_PORT="$port" \
    -e TONE_HZ="$hz" -e INJECT_HZ="$WHISPER_HZ" -e EAR_TONES="$EAR_TONES" \
    -e TONE_AMPLITUDE="$TONE_AMPLITUDE" \
    -e RUN_SECONDS=600 \
    python:3-slim python3 /inline_peer.py >/dev/null
  i=0
  while [ ! -f "$IO_DIR/peer-$peer/offer.sdp" ]; do
    i=$((i + 1))
    [ "$i" -gt 60 ] && {
      say "peer $peer never offered"; docker logs "conf-peer-$peer"; exit 1
    }
    sleep 1
  done
}

seat_peer() {
  peer=$1
  external="conf-$STAMP-$peer"
  answer=$("$CTL" "http://$CONTROL" inline "$external" "conf-call-$peer" \
    "$IO_DIR/peer-$peer/offer.sdp" "$GROUP" | sed '1s/^ok: //')
  case "$answer" in
    v=0*) ;;
    *) say "MSS did not answer peer $peer: $answer"; exit 1 ;;
  esac
  printf '%s\n' "$answer" > "$IO_DIR/peer-$peer/answer.sdp"
  say "seated $external in conference $GROUP on $(printf '%s' "$answer" |
    tr -d '\r' | sed -n 's/^m=audio \([0-9]*\).*/port \1/p')"
  i=0
  while [ ! -f "$IO_DIR/peer-$peer/peer-ready" ]; do
    i=$((i + 1))
    [ "$i" -gt 30 ] && { say "peer $peer never armed"; exit 1; }
    sleep 1
  done
}

for peer in a b; do start_peer "$peer"; done
for peer in a b; do seat_peer "$peer"; done

say "recording the room as ONE mixed object at $ROOM_KEY"
ROOM=$("$CTL" "http://$CONTROL" record "conf-$STAMP-a" "$ROOM_KEY" room "" mixed |
  attachment_of)
[ -n "$ROOM" ] || { say "the room recorder did not attach"; exit 1; }

say "recording each participant into group $GROUP at $PARTY_KEY"
PARTY_A=$("$CTL" "http://$CONTROL" record "conf-$STAMP-a" "$PARTY_KEY" a "$GROUP" \
  customer | attachment_of)
PARTY_B=$("$CTL" "http://$CONTROL" record "conf-$STAMP-b" "$PARTY_KEY" b "$GROUP" \
  customer | attachment_of)
say "room=$ROOM party a=$PARTY_A b=$PARTY_B"

PAIR_FROM=$(now)
say "phase pair: A and B alone for ${PHASE_SECONDS}s"
sleep "$PHASE_SECONDS"
PAIR_TO=$(now)

say "peer c joins late -- the group anchor must pad its participant file"
start_peer c
seat_peer c
PARTY_C=$("$CTL" "http://$CONTROL" record "conf-$STAMP-c" "$PARTY_KEY" c "$GROUP" \
  customer | attachment_of)

say "attaching the monitor consumer (SINK, only=mixed)"
docker rm -f conf-monitor >/dev/null 2>&1 || true
docker run -d --name conf-monitor --network "$NET" \
  -v "$HERE/conference_actor.py:/conference_actor.py:ro" \
  -v "$REPO/lab/out/pb:/pb:ro" -v "$IO_DIR:/io" \
  -e ROLE=monitor -e NAME=monitor -e TRACK=mixed -e IO_DIR=/io -e STUBS=/pb \
  -e CONTROL="$CONTROL_IN_NET" -e EXTERNAL_ID="conf-$STAMP-a" \
  -e EAR_TONES="$EAR_TONES" -e RUN_SECONDS=600 \
  -e MSS_AUTH_TOKEN="${MSS_AUTH_TOKEN:-}" -e PIP_DISABLE_PIP_VERSION_CHECK=1 \
  python:3-slim sh -c 'pip install --quiet grpcio protobuf && python3 /conference_actor.py' \
  >/dev/null

i=0
while [ ! -f "$IO_DIR/monitor-attachment" ]; do
  i=$((i + 1))
  [ "$i" -gt 90 ] && { say "the monitor never attached"; docker logs conf-monitor; exit 1; }
  sleep 1
done
say "monitor attachment $(cat "$IO_DIR/monitor-attachment")"

THREE_FROM=$(now)
say "phase three: all three for ${PHASE_SECONDS}s"
sleep "$PHASE_SECONDS"
THREE_TO=$(now)

say "whisper then barge: an INJECT consumer on A's session at $WHISPER_HZ Hz"
docker rm -f conf-injector >/dev/null 2>&1 || true
docker run -d --name conf-injector --network "$NET" \
  -v "$HERE/conference_actor.py:/conference_actor.py:ro" \
  -v "$REPO/lab/out/pb:/pb:ro" -v "$IO_DIR:/io" \
  -e ROLE=injector -e NAME=injector -e TRACK=customer -e IO_DIR=/io -e STUBS=/pb \
  -e CONTROL="$CONTROL_IN_NET" -e EXTERNAL_ID="conf-$STAMP-a" \
  -e MIX_TARGET="conf-$STAMP-b" -e MIX_MONITOR=include \
  -e INJECT_HZ="$WHISPER_HZ" -e EAR_TONES="$EAR_TONES" \
  -e WHISPER_SECONDS="$WHISPER_SECONDS" -e BARGE_SECONDS="$BARGE_SECONDS" \
  -e TONE_AMPLITUDE="$TONE_AMPLITUDE" \
  -e MSS_AUTH_TOKEN="${MSS_AUTH_TOKEN:-}" -e PIP_DISABLE_PIP_VERSION_CHECK=1 \
  python:3-slim sh -c 'pip install --quiet grpcio protobuf && python3 /conference_actor.py' \
  >/dev/null

STATUS=$(docker wait conf-injector)
docker logs conf-injector 2>&1 | tail -12
[ "$STATUS" = "0" ] || { say "the injector exited $STATUS"; exit 1; }
curl -s "http://$METRICS/metrics" | grep -E '^mss_conference_(whispers|route)' || true

say "letting the whisper drain out of the egress before muting"
sleep 2

say "phase mute: silencing member A everywhere by metadata on $PARTY_A"
"$CTL" "http://$CONTROL" member "$PARTY_A" mute on
MUTE_FROM=$(now)
sleep "$PHASE_SECONDS"
MUTE_TO=$(now)
curl -s "http://$METRICS/metrics" | grep -E '^mss_conference_(muted|deaf|held|member)' || true

say "phase unmute: A comes back"
"$CTL" "http://$CONTROL" member "$PARTY_A" mute off
UNMUTE_FROM=$(now)
sleep "$PHASE_SECONDS"
UNMUTE_TO=$(now)

say "conference metrics"
curl -s "http://$METRICS/metrics" |
  grep -E '^mss_conference|^mss_inline_legs_live|^mss_sessions_live' || true

say "stopping the monitor and uploading every recording"
touch "$IO_DIR/stop-monitor"
docker wait conf-monitor >/dev/null 2>&1 || true
docker logs conf-monitor 2>&1 | tail -6
for attachment in "$ROOM" "$PARTY_A" "$PARTY_B" "$PARTY_C"; do
  "$CTL" "http://$CONTROL" detach "$attachment" >/dev/null
done
sleep 3
curl -s "http://$METRICS/metrics" |
  grep -E '^mss_recording_uploads_total|^mss_recordings_live|^mss_recording_group' || true

say "stopping the peers so each writes its ear"
for peer in $PEERS; do
  docker stop -t 6 "conf-peer-$peer" >/dev/null 2>&1 || true
  docker logs "conf-peer-$peer" 2>&1 | tail -3 | sed "s/^/peer-$peer: /"
done

say "writing the phase windows and the expectations"
python3 - "$IO_DIR" <<PYEOF
import json, os, sys
io_dir = sys.argv[1]
phases = [
    {"phase": "pair", "from": $PAIR_FROM, "to": $PAIR_TO},
    {"phase": "three", "from": $THREE_FROM, "to": $THREE_TO},
    {"phase": "mute", "from": $MUTE_FROM, "to": $MUTE_TO},
    {"phase": "unmute", "from": $UNMUTE_FROM, "to": $UNMUTE_TO},
]
ears = []
for name, own in (("a", 440), ("b", 880), ("c", 1320)):
    ears.append({
        "name": name,
        "own": own,
        "timeline": f"peer-{name}/peer-timeline.jsonl",
    })
ears.append({"name": "monitor", "own": None, "timeline": "monitor-timeline.jsonl"})
expect = [
    {"phase": "pair", "ear": "a", "present": [880], "absent": [440, 1320, 1760]},
    {"phase": "pair", "ear": "b", "present": [440], "absent": [880, 1320, 1760]},
    {"phase": "three", "ear": "a", "present": [880, 1320], "absent": [440, 1760]},
    {"phase": "three", "ear": "b", "present": [440, 1320], "absent": [880, 1760]},
    {"phase": "three", "ear": "c", "present": [440, 880], "absent": [1320, 1760]},
    {"phase": "three", "ear": "monitor",
     "present": [440, 880, 1320], "absent": [1760]},
    {"phase": "whisper", "ear": "b", "present": [440, 1320, 1760], "absent": []},
    {"phase": "whisper", "ear": "a", "present": [880, 1320], "absent": [1760]},
    {"phase": "whisper", "ear": "c", "present": [440, 880], "absent": [1760]},
    {"phase": "whisper", "ear": "monitor",
     "present": [440, 880, 1320, 1760], "absent": []},
    {"phase": "barge", "ear": "b", "present": [440, 1320, 1760], "absent": []},
    {"phase": "barge", "ear": "c", "present": [440, 880, 1760], "absent": []},
    {"phase": "barge", "ear": "a", "present": [880, 1320, 1760], "absent": []},
    {"phase": "barge", "ear": "monitor",
     "present": [440, 880, 1320, 1760], "absent": []},
    {"phase": "mute", "ear": "b", "present": [1320], "absent": [440, 880, 1760]},
    {"phase": "mute", "ear": "c", "present": [880], "absent": [440, 1320, 1760]},
    {"phase": "mute", "ear": "monitor", "present": [880, 1320], "absent": [440]},
    {"phase": "unmute", "ear": "b", "present": [440, 1320], "absent": [880, 1760]},
    {"phase": "unmute", "ear": "c", "present": [440, 880], "absent": [1320, 1760]},
    {"phase": "unmute", "ear": "monitor",
     "present": [440, 880, 1320], "absent": [1760]},
]
manifest = {
    "phases": phases,
    "phase_files": ["injector-phases.jsonl"],
    "ears": ears,
    "expect": expect,
}
with open(os.path.join(io_dir, "manifest.json"), "w") as out:
    json.dump(manifest, out, indent=2)
print(f"wrote {len(phases)} stamped phases and {len(expect)} expectations")
PYEOF

say "pulling the recordings out of minio"
docker exec "$MINIO_CTR" sh -c \
  "mc alias set lab http://127.0.0.1:9000 minioadmin minioadmin >/dev/null &&
   mc ls -r lab/$BUCKET/$ACCOUNT/" | grep -E "room-$STAMP|party-$STAMP"
docker exec "$MINIO_CTR" sh -c \
  "mc alias set lab http://127.0.0.1:9000 minioadmin minioadmin >/dev/null &&
   mc cat lab/$BUCKET/$ROOM_KEY" > "$IO_DIR/room.wav"
for peer in $PEERS; do
  docker exec "$MINIO_CTR" sh -c \
    "mc alias set lab http://127.0.0.1:9000 minioadmin minioadmin >/dev/null &&
     mc cat lab/$BUCKET/$ACCOUNT/party-$STAMP/$peer.wav" \
    > "$IO_DIR/party-$peer.wav"
done

cat > "$IO_DIR/recordings.json" <<JSONEOF
[
  {"path": "$IO_DIR/room.wav", "label": "room(mixed)",
   "present": [440, 880, 1320], "absent": []},
  {"path": "$IO_DIR/party-a.wav", "label": "party-a(own 440)",
   "present": [440], "absent": [880, 1320, 1760]},
  {"path": "$IO_DIR/party-b.wav", "label": "party-b(own 880)",
   "present": [880], "absent": [440, 1320, 1760]},
  {"path": "$IO_DIR/party-c.wav", "label": "party-c(own 1320)",
   "present": [1320], "absent": [440, 880, 1760]}
]
JSONEOF

say "=== ears, per phase ==="
EARS_STATUS=0
python3 "$HERE/conference_report.py" ears "$IO_DIR" || EARS_STATUS=$?

say "=== recordings ==="
WAVS_STATUS=0
python3 "$HERE/conference_report.py" wavs "$IO_DIR/recordings.json" || WAVS_STATUS=$?

say "=== the recording group's files must all be one length ==="
LENGTH_STATUS=0
python3 "$HERE/conference_report.py" lengths "$LENGTH_TOLERANCE_MS" \
  "$IO_DIR"/party-a.wav "$IO_DIR"/party-b.wav "$IO_DIR"/party-c.wav ||
  LENGTH_STATUS=$?

say "=== wav_summary of every artifact ==="
TONE_CANDIDATES="$EAR_TONES" python3 "$HERE/webrtc/wav_summary.py" \
  "$IO_DIR"/room.wav "$IO_DIR"/party-*.wav "$IO_DIR"/monitor-ear.wav \
  "$IO_DIR"/peer-*/peer_ear_*.wav || true

say "artifacts in $IO_DIR"
REACHED_END=1
[ "$EARS_STATUS" = 0 ] && [ "$WAVS_STATUS" = 0 ] && [ "$LENGTH_STATUS" = 0 ] || {
  say "FAILED: ears=$EARS_STATUS wavs=$WAVS_STATUS lengths=$LENGTH_STATUS"
  exit 1
}
say "done"
