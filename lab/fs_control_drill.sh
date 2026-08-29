#!/bin/sh
# Handoff H5, the other half: FreeSWITCH answers the call and carries none of it.
#
# lab/fsless_call_drill.sh proves MSS can carry a call with FreeSWITCH STOPPED.
# That is the end state, and it is not the state anybody migrates into. This
# drill proves the shape a real deployment moves through first: FreeSWITCH is
# running and still owns the call -- it answers, it decides which conference the
# caller belongs to, it decides when to hang up -- while every byte of audio
# belongs to MSS.
#
# Two softphone-shaped callers (lab/host_test_caller.py) dial 7200. The dialplan
# sets bypass_media BEFORE bridging, so FreeSWITCH negotiates the two SDPs
# against each other and then drops out of the RTP path; the far leg is
# lab/sip_shim.py, which turns the offer into CreateSession{kind=INLINE,
# group=7200}. So each caller is an inline leg in one MSS mix, and FreeSWITCH
# holds two signalling channels that carry no media at all.
#
# What is asserted, all from measurements:
#   * the freeswitch container is RUNNING for the whole drill (the opposite of
#     the fsless drill, and the whole point of this one)
#   * FreeSWITCH holds 2 channels per caller and every one of them reports
#     bypass_media=true on the inbound leg and _undef_ RTP byte counters, with
#     signal_bridge as the application: it negotiated media and forwarded none
#   * the pod reports one live conference with two members and two inline legs
#   * both sessions describe as INLINE in group 7200
#   * caller A's ear carries B's tone and not A's own, and the mirror image
#     (lab/conference_report.py, Goertzel per block, minus-self)
#
# With ORCHESTRATOR_LOG pointed at the log of a controller holding one inbound
# event socket -- https://github.com/lazyboson/fs-orchestrator is the one these
# numbers were taken against -- it also drives the three lifecycle outcomes and
# asserts the controller's own account of each: the last party alone gets hung
# up after the grace period, a rejoin inside the window cancels that, and a
# lonely party that hangs up by itself is not chased. Without it those rounds
# are skipped and said to be skipped -- the media claims above stand alone.
#
#   ./lab/fs_control_drill.sh
#   ORCHESTRATOR_LOG=/var/log/fs-orchestrator.log ./lab/fs_control_drill.sh
#
# Do NOT run this with docker-compose.public.yml applied: the callers live on
# this host and reach OpenSIPS at 127.0.0.1, so an SDP advertising a public
# address sends their RTP somewhere the host cannot hear.
#
# EXTRA_COMPOSE passes further -f overlays through to every compose call this
# drill makes, which matters on a host whose lab is not the plain one: without
# it, "up -d" recreates services from the base spec and silently undoes an
# overlay the host depends on (the rig FreeSWITCH image, a memory-tuned broker).
#
#   EXTRA_COMPOSE="-f lab/docker-compose.rig.yml" ./lab/fs_control_drill.sh
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
CONTROL=${CONTROL:-127.0.0.1:50551}
METRICS=${METRICS:-127.0.0.1:9464}
FS_CONTAINER=${FS_CONTAINER:-mss-microsip-freeswitch-1}
DIAL=${DIAL:-7200}
TONE_A=${TONE_A:-440}
TONE_B=${TONE_B:-1000}
AMPLITUDE=${AMPLITUDE:-6000}
CALL_SECONDS=${CALL_SECONDS:-30}
JOIN_STAGGER=${JOIN_STAGGER:-4}
ORCHESTRATOR_LOG=${ORCHESTRATOR_LOG:-}
STAMP=$(date +%s)
IO_DIR=${IO_DIR:-$REPO/lab/out/fs-control-$STAMP}

export DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
COMPOSE="docker compose -f $HERE/docker-compose.microsip.yml -f $HERE/docker-compose.shim.yml${EXTRA_COMPOSE:+ $EXTRA_COMPOSE}"
CTL="$REPO/target/debug/examples/mss_ctl"
SERVICES="rtpengine opensips redis redpanda minio minio-init mss-control sip-shim freeswitch"
FAILURES=0

say() { printf 'fs-control-drill: %s\n' "$*"; }
fail() { say "FAIL: $*"; FAILURES=$((FAILURES + 1)); }

ensure_ctl() {
  if command -v cargo >/dev/null 2>&1; then
    say "building mss_ctl"
    (cd "$REPO" && cargo build --quiet -p control-api --example mss_ctl)
    return
  fi
  # A lab host need not carry a Rust toolchain: this repo's own images build the
  # workspace in a container, and one such host has 3.6 GB of RAM where a cargo
  # build is what wedges it. A prebuilt binary is a perfectly good mss_ctl.
  [ -x "$CTL" ] || {
    say "no cargo on this host and no prebuilt binary at $CTL"
    exit 1
  }
  say "using the prebuilt mss_ctl (no cargo on this host)"
}

fs() { docker exec "$FS_CONTAINER" fs_cli -x "$1" 2>/dev/null | tr -d '\r'; }
channels() { fs "show channels count" | sed -n 's/^\([0-9]*\) total.*/\1/p' | head -1; }

cleanup() {
  kill ${PIDS:-} 2>/dev/null || true
  for c in ${SESSIONS:-}; do "$CTL" "http://$CONTROL" destroy "$c" >/dev/null 2>&1 || true; done
}
trap cleanup EXIT

mkdir -p "$IO_DIR"
PIDS=""
SESSIONS=""

say "bringing up the stack, FreeSWITCH included"
$COMPOSE up -d $SERVICES >/dev/null

ensure_ctl

say "waiting for the shim, the pod and a sofia profile"
i=0
while :; do
  ready=0
  $COMPOSE logs sip-shim 2>&1 | grep -q "listening on" && ready=$((ready + 1))
  curl -sf -o /dev/null "http://$METRICS/metrics" && ready=$((ready + 1))
  fs "sofia status" | grep -q RUNNING && ready=$((ready + 1))
  [ "$ready" -eq 3 ] && break
  i=$((i + 1))
  [ "$i" -gt 60 ] && { say "the shim, the pod or FreeSWITCH never came up"; exit 1; }
  sleep 3
done

fs "xml_locate dialplan" | grep -q "$DIAL" ||
  { say "FreeSWITCH has no $DIAL extension loaded; reloadxml or recreate it"; exit 1; }
say "FreeSWITCH is up with the $DIAL extension loaded"

say "tones: caller A speaks $TONE_A Hz, caller B speaks $TONE_B Hz"
python3 - "$IO_DIR" "$TONE_A" "$TONE_B" "$AMPLITUDE" <<'PY'
import math, struct, sys, wave
io_dir, hz_a, hz_b, amplitude = sys.argv[1], float(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4])
for hz, name in ((hz_a, "a"), (hz_b, "b")):
    with wave.open(f"{io_dir}/tone_{name}.wav", "wb") as out:
        out.setnchannels(1)
        out.setsampwidth(2)
        out.setframerate(8000)
        out.writeframes(b"".join(
            struct.pack("<h", int(amplitude * math.sin(2 * math.pi * hz * n / 8000)))
            for n in range(8000 * 3)))
PY

CALL_A="fsctl-$STAMP-a"
CALL_B="fsctl-$STAMP-b"
say "caller A dials $DIAL"
(cd "$HERE" && CALL_ID="$CALL_A" FROM_TAG="fsctl${STAMP}a" DIAL="$DIAL" \
  SIP_PORT=45080 RTP_PORT=45082 RTP_SSRC=0x7C000001 \
  SPEECH_WAV="$IO_DIR/tone_a.wav" EAR_DIR="$IO_DIR" EAR_PREFIX=ear_a \
  CALL_SECONDS="$CALL_SECONDS" TALK_EVERY=1 \
  python3 host_test_caller.py > "$IO_DIR/caller-a.log" 2>&1) &
PID_A=$!
PIDS="$PID_A"

sleep "$JOIN_STAGGER"
say "caller B dials $DIAL"
(cd "$HERE" && CALL_ID="$CALL_B" FROM_TAG="fsctl${STAMP}b" DIAL="$DIAL" \
  SIP_PORT=45084 RTP_PORT=45086 RTP_SSRC=0x7C000002 \
  SPEECH_WAV="$IO_DIR/tone_b.wav" EAR_DIR="$IO_DIR" EAR_PREFIX=ear_b \
  CALL_SECONDS=$((CALL_SECONDS - JOIN_STAGGER)) TALK_EVERY=1 \
  python3 host_test_caller.py > "$IO_DIR/caller-b.log" 2>&1) &
PID_B=$!
PIDS="$PID_A $PID_B"

sleep 8
LIVE=$(channels)
say "FreeSWITCH holds $LIVE channels: one inbound leg and one leg to the shim, per caller"
[ "$LIVE" = "4" ] || fail "expected 4 FreeSWITCH channels mid-call, it holds '$LIVE'"

say "what FreeSWITCH did with the media on each of them:"
fs "show channels as xml" > "$IO_DIR/fs-channels.xml"
UUIDS=$(grep -oE '<uuid>[^<]+' "$IO_DIR/fs-channels.xml" | cut -c7-)
MEDIA_CARRIED=0
BYPASSED=0
for u in $UUIDS; do
  bypass=$(fs "uuid_getvar $u bypass_media")
  app=$(fs "uuid_getvar $u current_application")
  inb=$(fs "uuid_getvar $u rtp_audio_in_raw_bytes")
  outb=$(fs "uuid_getvar $u rtp_audio_out_raw_bytes")
  say "  $u  bypass_media=$bypass  application=$app  rtp_in=$inb  rtp_out=$outb"
  [ "$bypass" = "true" ] && BYPASSED=$((BYPASSED + 1))
  case "$inb$outb" in
    _undef__undef_) ;;
    *) MEDIA_CARRIED=$((MEDIA_CARRIED + 1)) ;;
  esac
done
[ "$MEDIA_CARRIED" -eq 0 ] ||
  fail "$MEDIA_CARRIED FreeSWITCH channel(s) counted RTP bytes; FreeSWITCH is in the media path"
[ "$BYPASSED" -ge 2 ] ||
  fail "expected bypass_media=true on both inbound legs, saw $BYPASSED"
say "no FreeSWITCH channel counted a single RTP byte"

say "mid-call, from the pod's own metrics:"
curl -s "http://$METRICS/metrics" | grep -E '^mss_(conferences_live|conference_members_live|inline_legs_live|conference_mixed_frames_total)' |
  tee "$IO_DIR/metrics-midcall.txt" | sed 's/^/  /'
MEMBERS=$(sed -n 's/^mss_conference_members_live //p' "$IO_DIR/metrics-midcall.txt")
CONFS=$(sed -n 's/^mss_conferences_live //p' "$IO_DIR/metrics-midcall.txt")
LEGS=$(sed -n 's/^mss_inline_legs_live //p' "$IO_DIR/metrics-midcall.txt")
[ "$MEMBERS" = "2" ] || fail "expected 2 conference members mid-call, metrics say '$MEMBERS'"
[ "$CONFS" = "1" ] || fail "expected 1 live conference mid-call, metrics say '$CONFS'"
[ "$LEGS" = "2" ] || fail "expected 2 live inline legs mid-call, metrics say '$LEGS'"

say "waiting for both callers to hang up"
wait "$PID_A" || fail "caller A exited non-zero"
wait "$PID_B" || fail "caller B exited non-zero"
PIDS=""
tail -2 "$IO_DIR/caller-a.log" | sed 's/^/  a| /'
tail -2 "$IO_DIR/caller-b.log" | sed 's/^/  b| /'

docker inspect -f '{{.State.Status}}' "$FS_CONTAINER" | grep -q running ||
  fail "the freeswitch container did not stay running"
say "the freeswitch container ran throughout"

say "judging the ears"
python3 - "$IO_DIR" "$TONE_A" "$TONE_B" <<'PY' > "$IO_DIR/ears.json"
import glob, json, os, sys
io_dir, hz_a, hz_b = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
spec = []
for prefix, own, other in (("ear_a", hz_a, hz_b), ("ear_b", hz_b, hz_a)):
    found = sorted(glob.glob(os.path.join(io_dir, f"{prefix}_*.wav")))
    if len(found) != 1:
        raise SystemExit(f"{prefix}: expected exactly one ear wav, found {found}")
    spec.append({"path": found[0], "label": f"{prefix} hears the other leg through MSS",
                 "present": [other], "absent": [own]})
print(json.dumps(spec))
PY
python3 "$HERE/conference_report.py" wavs "$IO_DIR/ears.json" || fail "ear assertions failed"

if [ -z "$ORCHESTRATOR_LOG" ]; then
  say "lifecycle rounds SKIPPED: set ORCHESTRATOR_LOG to a controller's log to run them"
else
  [ -r "$ORCHESTRATOR_LOG" ] || { fail "ORCHESTRATOR_LOG is not readable: $ORCHESTRATOR_LOG"; ORCHESTRATOR_LOG=""; }
fi

if [ -n "$ORCHESTRATOR_LOG" ]; then
  GRACE=$(grep -o '"hang_up_last_party_after":"[0-9]*s"' "$ORCHESTRATOR_LOG" | tail -1 |
          sed 's/.*:"\([0-9]*\)s"/\1/')
  GRACE=${GRACE:-20}
  # The media phase above left a party alone when its partner hung up first, so
  # a grace timer from it is still armed. Let it expire before marking the log,
  # or its (correct) line lands in the middle of round 1 and reads as round 1's.
  say "letting the media phase's own grace timer expire before the rounds start"
  sleep $((GRACE + 4))
  say "the controller says its grace period is ${GRACE}s; driving the three outcomes"
  MARK=$(wc -l < "$ORCHESTRATOR_LOG")

  round() { # label sip_port callid ssrc seconds
    (cd "$HERE" && CALL_ID="$3" FROM_TAG="$3" DIAL="$DIAL" WRITE_EARS=0 \
      SIP_PORT="$2" RTP_PORT=$(($2 + 2)) RTP_SSRC="$4" CALL_SECONDS="$5" \
      SPEECH_WAV="$IO_DIR/tone_a.wav" \
      python3 host_test_caller.py > "$IO_DIR/$3.log" 2>&1) &
    say "  $1 dials $DIAL, hangs up after ${5}s"
  }

  say "round 1: the last party alone must be hung up after the grace period"
  round A 45100 "l1a-$STAMP" 0x7C010001 15
  sleep 5
  round B 45110 "l1b-$STAMP" 0x7C010002 $((25 + GRACE * 2))
  sleep $((15 + GRACE + 8))
  L1=$(channels)
  [ "$L1" = "0" ] || fail "round 1: the lonely party was not hung up, $L1 channels remain"
  say "round 1: FreeSWITCH is back to $L1 channels"

  say "round 2: a rejoin inside the window must cancel that hangup"
  round C 45120 "l2c-$STAMP" 0x7C020001 15
  sleep 5
  round D 45130 "l2d-$STAMP" 0x7C020002 $((30 + GRACE * 2))
  sleep 12
  round E 45140 "l2e-$STAMP" 0x7C020003 $((20 + GRACE * 2))
  sleep $((GRACE + 6))
  L2=$(channels)
  [ "$L2" = "4" ] || fail "round 2: expected the two remaining parties (4 channels), saw $L2"
  say "round 2: both remaining parties survived the grace timer ($L2 channels)"
  pkill -f "CALL_ID=l2" 2>/dev/null || true
  fs "hupall NORMAL_CLEARING" >/dev/null
  sleep 4

  say "round 3: a lonely party that hangs up by itself must not be chased"
  round F 45150 "l3f-$STAMP" 0x7C030001 10
  sleep 5
  round G 45160 "l3g-$STAMP" 0x7C030002 17
  sleep $((17 + GRACE + 8))
  L3=$(channels)
  [ "$L3" = "0" ] || fail "round 3: $L3 channels remain"

  wait 2>/dev/null || true
  say "the controller's account of all three rounds:"
  tail -n +$((MARK + 1)) "$ORCHESTRATOR_LOG" > "$IO_DIR/orchestrator.log"
  python3 - "$IO_DIR/orchestrator.log" <<'PY'
import json, sys
for line in open(sys.argv[1]):
    try:
        d = json.loads(line)
    except ValueError:
        continue
    when = d.pop("time", "")[11:19]
    d.pop("level", None)
    msg = d.pop("msg", "")
    rest = "  ".join(f"{k}={v}" for k, v in d.items() if k not in ("arrived_via", "caller"))
    print(f"  {when}  {msg:64s} {rest}")
PY
  for want in "hanging up the last party in an emptied conference" \
              "somebody joined during the grace period, so the last party stays" \
              "the last party hung up during the grace period"; do
    grep -q "$want" "$IO_DIR/orchestrator.log" ||
      fail "the controller never logged: $want"
  done
  say "all three lifecycle outcomes appear in the controller's log"
fi

say "artifacts in $IO_DIR"
if [ "$FAILURES" -eq 0 ]; then
  say "done: FreeSWITCH answered and decided, MSS carried every byte"
else
  say "done with $FAILURES failure(s)"
  exit 1
fi
