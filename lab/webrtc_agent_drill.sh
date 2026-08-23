#!/bin/sh
# Rung 2 of the two-node testing plan: one lab call whose two legs are anchored
# in two different rtpengine nodes, the agent leg being a real browser sending
# real WebRTC media.
#
#   MicroSIP-shaped caller -> OpenSIPS -> rtpengine (RE1) -> FreeSWITCH
#   browser (WebRTC)       -> opensips-agent (ws) -> rtpengine-agent (RE2) -> FreeSWITCH
#   both legs meet in one FreeSWITCH conference
#   MSS taps each leg at its own node: two sessions, one recording group
#
# Why it exists: production anchors the customer leg at the carrier
# interconnect and the agent leg on the agent side, and its WebRTC customers
# send Opus. Every Opus packet this project has decoded so far came from
# lab/opus_call_driver.py, which sends CBR, DTX off, inband FEC off, one 20 ms
# frame per packet, no RED. A browser defaults to none of that. This drill is
# where the difference stops being a guess.
#
# One profile per run, one variable changed per run (session-playbook.md):
#
#   PROFILE=control   PCMU end to end, nothing transcodes. Run this first.
#   PROFILE=opus      Opus 48 kHz, VBR, inband FEC. The production shape.
#   PROFILE=dtx       Opus with usedtx=1. Does a DTX gap read as packet loss?
#   PROFILE=red       Opus wrapped in RFC 2198 redundancy (Chrome's PT 63).
#   PROFILE=ptime60   Opus with a 60 ms offer: three frames per packet.
#   PROFILE=cbr       Opus with cbr=1, the shape opus_call_driver.py sends.
#   PROFILE=stereo    Opus stereo, two channels where the tap expects one.
#   PROFILE=dsp       Opus with Chrome's own AEC/NS/AGC left on.
#
# AGENT=headless (default) places the call with a containerised Chrome and a
# wav file for a microphone, so no human is needed. AGENT=browser prints a URL
# and waits for you to click dial, which is the run to use when you want to
# hear it yourself.
#
#   ./lab/webrtc_agent_drill.sh
#   PROFILE=dtx ./lab/webrtc_agent_drill.sh
#   PROFILE=opus AGENT=browser ./lab/webrtc_agent_drill.sh
#
# Needs the lab's usual prerequisites: Docker Desktop running on WSL2 and
# DOCKER_API_VERSION pinned (the daemon here is older than the CLI).
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)

PROFILE=${PROFILE:-opus}
AGENT=${AGENT:-headless}
CALL_SECONDS=${CALL_SECONDS:-60}
RECORD_SECONDS=${RECORD_SECONDS:-35}
ACCOUNT=${ACCOUNT:-acct-webrtc}
STAMP=$(date +%s)
RECORDING=${RECORDING:-rec-$STAMP}
GROUP_RECORDING=${GROUP_RECORDING:-grp-$STAMP}
GROUP=${GROUP:-webrtc-$STAMP}
BUCKET=${BUCKET:-lab-recordings}
CONTROL=${CONTROL:-http://127.0.0.1:50551}
METRICS=${METRICS:-http://127.0.0.1:9464/metrics}
CUSTOMER_CALL_ID=${CUSTOMER_CALL_ID:-webrtc-cust-$STAMP}
CUSTOMER_TAG=${CUSTOMER_TAG:-custtag$STAMP}
OUT=$HERE/out

export DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
# The base compose file demands these for llm-bridge, which this drill never
# starts -- but compose interpolates the whole file before it starts anything.
export ELEVENLABS_API_KEY=${ELEVENLABS_API_KEY:-unused-by-this-drill}
export DEEPGRAM_API_KEY=${DEEPGRAM_API_KEY:-unused-by-this-drill}

# The profile is nothing but a set of AGENT_* variables; docker-compose.webrtc.yml
# passes them into the page's query string, so the browser offer is what changes
# and everything else in the lab stays identical between runs.
export AGENT_CODEC=opus AGENT_RED=0 AGENT_DTX=0 AGENT_FEC=1 AGENT_CBR=0
export AGENT_STEREO=0 AGENT_PTIME= AGENT_BITRATE= AGENT_DSP=0
export AGENT_DIAL=4048
case "$PROFILE" in
  control)  AGENT_CODEC=pcmu; AGENT_DIAL=4100 ;;
  opus)     ;;
  dtx)      AGENT_DTX=1 ;;
  red)      AGENT_RED=1 ;;
  ptime60)  AGENT_PTIME=60 ;;
  cbr)      AGENT_CBR=1; AGENT_FEC=0 ;;
  stereo)   AGENT_STEREO=1 ;;
  dsp)      AGENT_DSP=1 ;;
  *) echo "unknown PROFILE=$PROFILE" >&2; exit 2 ;;
esac
export AGENT_CALL_SECONDS=$CALL_SECONDS
export AGENT_MIC_WAV=agent_mic.wav
# The headless browser lives inside the docker network, a real one does not, and
# rtpengine has to be told which of its interfaces to advertise to it.
if [ "$AGENT" = headless ]; then
  export AGENT_RTPE_OFFER_DIR="internal internal"
  export AGENT_RTPE_ANSWER_DIR="internal internal"
else
  export AGENT_RTPE_OFFER_DIR="external internal"
  export AGENT_RTPE_ANSWER_DIR="internal external"
fi

COMPOSE="docker compose -f $HERE/docker-compose.microsip.yml -f $HERE/docker-compose.webrtc.yml"
CTL="$REPO/target/debug/examples/mss_ctl"
SERVICES="rtpengine rtpengine-agent opensips opensips-agent freeswitch redis redpanda minio minio-init mss-control webrtc-page call-watcher-agent"

say() { printf '\nwebrtc-drill: %s\n' "$*"; }
minio_container() { docker ps --filter name=minio --format '{{.Names}}' | grep -v init | head -1; }

cleanup() {
  say "cleaning up"
  [ -n "${CALLER_PID:-}" ] && kill "$CALLER_PID" 2>/dev/null || true
  $COMPOSE --profile headless rm -sf chrome-agent >/dev/null 2>&1 || true
}
trap cleanup EXIT

say "profile $PROFILE: codec=$AGENT_CODEC dial=$AGENT_DIAL red=$AGENT_RED dtx=$AGENT_DTX fec=$AGENT_FEC cbr=$AGENT_CBR stereo=$AGENT_STEREO ptime=${AGENT_PTIME:-default} dsp=$AGENT_DSP agent=$AGENT"

if ! docker info >/dev/null 2>&1; then
  echo "the docker daemon is not reachable. On WSL2 this lab needs Docker" >&2
  echo "Desktop started, and DOCKER_API_VERSION pinned (1.43 works here)." >&2
  exit 1
fi

say "building the microphone Chrome will play"
SPEECH=""
[ -f "$OUT/bridge_tts.wav" ] && SPEECH="$OUT/bridge_tts.wav"
OUT="$OUT/agent_mic.wav" SECONDS="$CALL_SECONDS" SPEECH_WAV="$SPEECH" \
  python3 "$HERE/webrtc/make_agent_audio.py"
OUT=$HERE/out

say "building mss_ctl on the host"
(cd "$REPO" && cargo build --quiet -p control-api --example mss_ctl)

say "starting the lab (both rtpengine nodes, both proxies, one MSS pod)"
# shellcheck disable=SC2086
$COMPOSE up -d $SERVICES
say "waiting for the control plane"
until curl -sf "$METRICS" >/dev/null 2>&1; do sleep 2; done
$COMPOSE logs mss-control 2>&1 | grep -iE 'kernel|capabilit|tap decodes|transcod' | tail -5 || true

say "placing the customer leg: PCMA through OpenSIPS and RE1, dialling $AGENT_DIAL"
(cd "$HERE" && CALL_ID="$CUSTOMER_CALL_ID" FROM_TAG="$CUSTOMER_TAG" DIAL="$AGENT_DIAL" \
  CALL_SECONDS="$CALL_SECONDS" EAR_PREFIX="webrtc_cust_$STAMP" \
  python3 host_test_caller.py >"$OUT/webrtc-drill-caller.log" 2>&1) &
CALLER_PID=$!
sleep 6

if [ "$AGENT" = headless ]; then
  say "placing the agent leg: headless Chrome, real WebRTC, fake microphone"
  $COMPOSE --profile headless up -d chrome-agent
else
  say "open http://127.0.0.1:8081/?target=$AGENT_DIAL&codec=$AGENT_CODEC&red=$AGENT_RED&dtx=$AGENT_DTX&fec=$AGENT_FEC&cbr=$AGENT_CBR&stereo=$AGENT_STEREO&ptime=$AGENT_PTIME&dsp=$AGENT_DSP"
  say "click dial, then come back here. Waiting up to 120 s."
fi

say "waiting for the agent call to appear on RE2"
AGENT_ENV=""
attempt=0
while [ "$attempt" -lt 60 ]; do
  AGENT_ENV=$($COMPOSE exec -T call-watcher-agent cat /shared/call-agent.env 2>/dev/null || true)
  case "$AGENT_ENV" in *MSS_TAP_CALL_ID=?*) break ;; esac
  attempt=$((attempt + 1))
  sleep 2
done
case "$AGENT_ENV" in
  *MSS_TAP_CALL_ID=?*) ;;
  *)
    say "no call on RE2. What the pieces say:"
    $COMPOSE logs --tail 30 opensips-agent 2>&1 | tail -30
    [ "$AGENT" = headless ] && $COMPOSE logs --tail 30 chrome-agent 2>&1 | tail -30
    $COMPOSE logs --tail 20 webrtc-page 2>&1 | tail -20
    exit 1
    ;;
esac
AGENT_CALL_ID=$(printf '%s' "$AGENT_ENV" | sed -n 's/^MSS_TAP_CALL_ID=//p' | tr -d '\r')
AGENT_TAGS=$(printf '%s' "$AGENT_ENV" | sed -n 's/^MSS_TAP_FROM_TAGS=//p' | tr -d '\r')
BROWSER_TAG=$(printf '%s' "$AGENT_TAGS" | cut -d, -f1)
say "agent call $AGENT_CALL_ID, tags $AGENT_TAGS, browser is $BROWSER_TAG"

# Two sessions, because one session carries one rtpengine node.
#
# The customer session names no tags: MSS resolves the call's participants from
# rtpengine itself, gets both, and the tap is stereo -- which in a two-party
# conference means one file holding both sides, since what FreeSWITCH sends
# toward the caller is the mix of everyone else, i.e. the agent.
#
# The agent session names ONE tag on purpose. Both sides of RE2 carry the same
# codec only if FreeSWITCH negotiated Opus with the browser; if it answered
# PCMU instead, rtpengine transcodes and the two directions differ, which
# offered_tap_format refuses by name ("a tap decodes one format for every leg").
# Subscribing to the browser alone is one stream, always legal, and is exactly
# the per-participant feed a recording group wants.
say "creating the two sessions"
"$CTL" "$CONTROL" create webrtc-cust "$CUSTOMER_CALL_ID" - 172.31.99.10:22222
"$CTL" "$CONTROL" create webrtc-agent "$AGENT_CALL_ID" "$BROWSER_TAG" 172.31.99.11:22222

say "what MSS settled on for each tap"
$COMPOSE logs mss-control 2>&1 | grep -E 'offered a tap stream|settled the tap format' | tail -6

say "recording: one stereo file of the whole call, plus a cross-node group"
stereo=$("$CTL" "$CONTROL" record webrtc-cust "$ACCOUNT/$RECORDING.wav" stereo "" all |
  sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
member_c=$("$CTL" "$CONTROL" record webrtc-cust "$ACCOUNT/$GROUP_RECORDING.wav" customer "$GROUP" customer |
  sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
member_a=$("$CTL" "$CONTROL" record webrtc-agent "$ACCOUNT/$GROUP_RECORDING.wav" agent "$GROUP" agent |
  sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
say "attachments: stereo=$stereo customer=$member_c agent=$member_a"

curl -s "$METRICS" | grep -E '^mss_(sessions_live|legs_live|recordings_live|recording_group)' || true
say "recording for $RECORD_SECONDS s"
sleep "$RECORD_SECONDS"

say "ingest counters, both legs, at the end of the run"
curl -s "$METRICS" | grep -E '^mss_(ingest|jitter|legs)_' | sort

say "detaching, which uploads"
"$CTL" "$CONTROL" detach "$stereo"
"$CTL" "$CONTROL" detach "$member_c"
"$CTL" "$CONTROL" detach "$member_a"
curl -s "$METRICS" | grep -E '^mss_recording_(uploads_total|upload_failures_total|bytes_uploaded_total)' || true
"$CTL" "$CONTROL" destroy webrtc-cust || true
"$CTL" "$CONTROL" destroy webrtc-agent || true

say "what the browser itself reported (its own getStats, not our counters)"
$COMPOSE logs webrtc-page 2>&1 | grep 'lab-report' | tail -3 || say "no report line: the page never finished a call"

say "what landed in the bucket"
MC=$(minio_container)
docker exec "$MC" sh -c \
  "mc alias set lab http://127.0.0.1:9000 minioadmin minioadmin >/dev/null &&
   mc ls -r lab/$BUCKET/$ACCOUNT/ | tail -20"

say "pulling the artifacts back for a look"
for object in "$RECORDING.wav" "$GROUP_RECORDING/customer.wav" "$GROUP_RECORDING/agent.wav"; do
  name=$(printf '%s' "$object" | tr '/' '-')
  docker exec "$MC" sh -c "mc cp lab/$BUCKET/$ACCOUNT/$object /tmp/$name" >/dev/null 2>&1 || {
    say "absent: $ACCOUNT/$object"
    continue
  }
  docker cp "$MC:/tmp/$name" "$OUT/webrtc-$STAMP-$name" >/dev/null
done
python3 "$HERE/webrtc/wav_summary.py" "$OUT"/webrtc-"$STAMP"-*.wav || true

say "read this before believing the run"
cat <<'NOTES'
  * The stereo file is the whole call: caller left, conference mix right. With
    two members that mix IS the agent, so it is a both-sides recording. With a
    third member it stops being one -- that is Phase 4's problem, not a bug.
  * The group directory has one mono file per participant, written by two
    sessions on two rtpengine nodes. That is the cross-node claim.
  * DTX and loss look identical in a wav and are told apart by counters:
    mss_jitter_silence_gaps_total is a gap the sender chose, mss_jitter_lost_total
    is a gap the network made. On PROFILE=dtx the first should move and the
    second should not.
  * The conference beeps are landmarks for when each leg joined, not artefacts.
NOTES
say "done. Artifacts in lab/out/webrtc-$STAMP-*.wav, caller log in lab/out/webrtc-drill-caller.log"
