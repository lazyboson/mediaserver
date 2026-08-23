#!/bin/sh
# Waits for a two-node lab call to exist, then taps both legs and records them.
#
# It is the manual companion to webrtc_agent_drill.sh: you place the call
# yourself (MicroSIP into 4001, a browser registered as the agent), and this
# attaches MSS to whatever appears. Both call-ids are discovered rather than
# chosen, because a softphone and a browser both generate their own.
#
#   ./lab/webrtc_record_live.sh                 # waits, records, stops on Ctrl-C
#   RECORD_SECONDS=60 ./lab/webrtc_record_live.sh
#
# What it produces:
#   acct-webrtc/<rec>.wav              the customer leg, stereo: caller on one
#                                      channel, what FreeSWITCH sends back on
#                                      the other, so one file holds both sides
#   acct-webrtc/<grp>/customer.wav     the same call as a group member
#   acct-webrtc/<grp>/agent.wav        the browser's own audio, tapped at RE2
#
# The group is the cross-node claim: two sessions on two rtpengine nodes, one
# recording, one file per participant.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)

RECORD_SECONDS=${RECORD_SECONDS:-0}
ACCOUNT=${ACCOUNT:-acct-webrtc}
STAMP=$(date +%s)
RECORDING=${RECORDING:-rec-$STAMP}
GROUP_RECORDING=${GROUP_RECORDING:-grp-$STAMP}
GROUP=${GROUP:-webrtc-$STAMP}
BUCKET=${BUCKET:-lab-recordings}
CONTROL=${CONTROL:-http://127.0.0.1:50551}
METRICS=${METRICS:-http://127.0.0.1:9464/metrics}
WAIT_SECONDS=${WAIT_SECONDS:-180}
RE1=${RE1:-172.31.99.10:22222}
RE2=${RE2:-172.31.99.11:22222}

export DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
CTL="$REPO/target/debug/examples/mss_ctl"
OUT=$HERE/out

say() { printf '\nrecord-live: %s\n' "$*"; }
watcher_env() { docker exec "$1" cat "$2" 2>/dev/null || true; }
field() { printf '%s' "$2" | sed -n "s/^$1=//p" | tr -d '\r'; }

stereo="" ; member_c="" ; member_a=""
finish() {
  say "detaching and uploading"
  for id in $stereo $member_c $member_a; do "$CTL" "$CONTROL" detach "$id" || true; done
  "$CTL" "$CONTROL" destroy webrtc-cust 2>/dev/null || true
  "$CTL" "$CONTROL" destroy webrtc-agent 2>/dev/null || true

  say "recording counters"
  curl -s "$METRICS" | grep -E '^mss_recording_(uploads_total|upload_failures_total|bytes_uploaded_total|seconds_total)' || true

  say "in the bucket"
  MC=$(docker ps --filter name=minio --format '{{.Names}}' | grep -v init | head -1)
  docker exec "$MC" sh -c \
    "mc alias set lab http://127.0.0.1:9000 minioadmin minioadmin >/dev/null &&
     mc ls -r lab/$BUCKET/$ACCOUNT/ | tail -10" || true

  for object in "$RECORDING.wav" "$GROUP_RECORDING/customer.wav" "$GROUP_RECORDING/agent.agent.wav" "$GROUP_RECORDING/agent.customer.wav"; do
    name=$(printf '%s' "$object" | tr '/' '-')
    docker exec "$MC" sh -c "mc cp lab/$BUCKET/$ACCOUNT/$object /tmp/$name" >/dev/null 2>&1 || continue
    docker cp "$MC:/tmp/$name" "$OUT/webrtc-$STAMP-$name" >/dev/null 2>&1 || true
  done
  python3 "$HERE/webrtc/wav_summary.py" "$OUT"/webrtc-"$STAMP"-*.wav 2>/dev/null || true
  say "artifacts: lab/out/webrtc-$STAMP-*.wav"
}
trap finish EXIT INT TERM

say "building mss_ctl"
(cd "$REPO" && cargo build --quiet -p control-api --example mss_ctl)

say "waiting up to ${WAIT_SECONDS}s for a call on both rtpengine nodes"
say "  customer leg: dial 4001 from MicroSIP"
say "  agent leg:    http://127.0.0.1:8081/ registered as 'agent', it auto-answers"
waited=0
while [ "$waited" -lt "$WAIT_SECONDS" ]; do
  CUST=$(watcher_env mss-microsip-call-watcher-1 /shared/call.env)
  AGENT=$(watcher_env mss-microsip-call-watcher-agent-1 /shared/call-agent.env)
  cust_id=$(field MSS_TAP_CALL_ID "$CUST")
  agent_id=$(field MSS_TAP_CALL_ID "$AGENT")
  if [ -n "$cust_id" ] && [ -n "$agent_id" ]; then break; fi
  waited=$((waited + 2))
  sleep 2
done

if [ -z "${cust_id:-}" ] || [ -z "${agent_id:-}" ]; then
  say "gave up. customer='${cust_id:-none}' agent='${agent_id:-none}'"
  say "if the agent leg is missing, check that the browser says 'registered':"
  docker logs --tail 15 mss-microsip-opensips-agent-1 2>&1 | tail -15
  exit 1
fi

cust_tags=$(field MSS_TAP_FROM_TAGS "$CUST")
agent_tags=$(field MSS_TAP_FROM_TAGS "$AGENT")
# The LAST tag, not the first. call_watcher.py orders tags caller-first, and on
# the agent leg the caller is FreeSWITCH -- it originated toward the browser. A
# measured run proved the cost of getting this backwards: the "agent" track came
# back carrying the customer's voice relayed toward the browser, byte-identical
# to the customer leg's own track, so the group held one side twice.
browser_tag=$(printf '%s' "$agent_tags" | rev | cut -d, -f1 | rev)
say "customer call $cust_id tags [$cust_tags] on RE1"
say "agent call    $agent_id tags [$agent_tags] on RE2, browser is $browser_tag"

# The customer session names no tags: MSS asks rtpengine for the call's
# participants itself and taps both, which makes the recording stereo and
# therefore a both-sides file on its own.
#
# The agent session names one tag. Both sides of RE2 carry the same codec only
# if nothing transcoded in between; subscribing to the browser alone is always
# one stream and one format, and is exactly the per-participant feed a
# recording group wants.
say "creating both sessions"
"$CTL" "$CONTROL" create webrtc-cust "$cust_id" - "$RE1"
"$CTL" "$CONTROL" create webrtc-agent "$agent_id" "$browser_tag" "$RE2"

say "what each tap settled on"
docker logs --since 60s mss-microsip-mss-control-1 2>&1 |
  grep -E 'offered a tap stream|settled the tap format|named the leg|leg named' | tail -8 || true

say "recording"
stereo=$("$CTL" "$CONTROL" record webrtc-cust "$ACCOUNT/$RECORDING.wav" stereo "" all |
  sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
member_c=$("$CTL" "$CONTROL" record webrtc-cust "$ACCOUNT/$GROUP_RECORDING.wav" customer "$GROUP" customer |
  sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
member_a=$("$CTL" "$CONTROL" record webrtc-agent "$ACCOUNT/$GROUP_RECORDING.wav" agent "$GROUP" all |
  sed -n 's/.*attachment_id: "\([^"]*\)".*/\1/p')
say "stereo=$stereo customer=$member_c agent=$member_a"

if [ "$RECORD_SECONDS" -gt 0 ]; then
  say "recording for ${RECORD_SECONDS}s, then stopping"
  sleep "$RECORD_SECONDS"
else
  say "recording until you press Ctrl-C (talk on both ends now)"
  while :; do
    sleep 10
    curl -s "$METRICS" | grep -E '^mss_ingest_datagrams_total|^mss_jitter_lost_total|^mss_legs_live' |
      tr '\n' ' '
    printf '\n'
  done
fi
