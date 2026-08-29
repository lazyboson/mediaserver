#!/bin/sh
# Handoff H5, prototyped: a SIP call whose media never touches FreeSWITCH.
#
# Two softphone-shaped callers (lab/host_test_caller.py, the same script that
# stands in for MicroSIP everywhere else) dial 7001 through OpenSIPS. OpenSIPS
# anchors each call in rtpengine exactly as it does today and relays the INVITE
# to lab/sip_shim.py instead of to FreeSWITCH; the shim turns each offer into
# CreateSession{kind=INLINE, group=7001} and answers with MSS's SDP. So the two
# callers are two inline legs seated in one MSS conference, and each one's ear
# must carry the other's tone and not its own.
#
# The FreeSWITCH container is STOPPED for the whole run and started again
# afterwards if it was up. That is the point of the drill: a call that carries
# audio while FreeSWITCH is not running cannot be using FreeSWITCH.
#
# What is asserted, all from measurements:
#   * both callers got a 200 OK with an MSS answer and RTP flowed both ways
#   * caller A's ear carries B's tone and not A's own, and the mirror image
#     (lab/conference_report.py wavs, Goertzel per block, minus-self)
#   * the pod reports one live conference with two members while they talk
#   * both sessions are INLINE, in group 7001, on this pod
#   * the freeswitch container was in state "exited" throughout
#
#   ./lab/fsless_call_drill.sh
#
# Everything it needs is brought up by the drill itself. Artifacts land in
# lab/out/fsless-<stamp>/.
#
# EXTRA_COMPOSE passes further -f overlays through to every compose call this
# drill makes, which matters on a host whose lab is not the plain one: without
# it, "up -d" recreates services from the base spec and silently undoes an
# overlay the host depends on (the rig FreeSWITCH image, a memory-tuned broker).
#
#   EXTRA_COMPOSE="-f lab/docker-compose.rig.yml" ./lab/fsless_call_drill.sh
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
CONTROL=${CONTROL:-127.0.0.1:50551}
METRICS=${METRICS:-127.0.0.1:9464}
DIAL=${DIAL:-7001}
TONE_A=${TONE_A:-440}
TONE_B=${TONE_B:-1000}
AMPLITUDE=${AMPLITUDE:-6000}
CALL_SECONDS=${CALL_SECONDS:-30}
JOIN_STAGGER=${JOIN_STAGGER:-4}
STAMP=$(date +%s)
IO_DIR=${IO_DIR:-$REPO/lab/out/fsless-$STAMP}
CALL_A=${CALL_A:-fsless-$STAMP-a}
CALL_B=${CALL_B:-fsless-$STAMP-b}

export DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
COMPOSE="docker compose -f $HERE/docker-compose.microsip.yml -f $HERE/docker-compose.shim.yml${EXTRA_COMPOSE:+ $EXTRA_COMPOSE}"
CTL="$REPO/target/debug/examples/mss_ctl"
SERVICES="rtpengine opensips redis redpanda minio minio-init mss-control sip-shim"
FS_WAS_RUNNING=no
FAILURES=0

say() { printf 'fsless-drill: %s\n' "$*"; }
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


cleanup() {
  kill "${PID_A:-}" "${PID_B:-}" 2>/dev/null || true
  "$CTL" "http://$CONTROL" destroy "$CALL_A" >/dev/null 2>&1 || true
  "$CTL" "http://$CONTROL" destroy "$CALL_B" >/dev/null 2>&1 || true
  if [ "$FS_WAS_RUNNING" = yes ]; then
    say "starting freeswitch again"
    $COMPOSE start freeswitch >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

mkdir -p "$IO_DIR"

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

say "bringing up the fsless stack (opensips relays to the shim, not to FS)"
$COMPOSE up -d $SERVICES >/dev/null

ensure_ctl

say "waiting for the shim and the pod"
i=0
while :; do
  ready=0
  $COMPOSE logs sip-shim 2>&1 | grep -q "listening on" && ready=$((ready + 1))
  curl -sf -o /dev/null "http://$METRICS/metrics" && ready=$((ready + 1))
  [ "$ready" -eq 2 ] && break
  i=$((i + 1))
  [ "$i" -gt 60 ] && { say "the shim or the pod never came up"; $COMPOSE logs --tail 20 sip-shim; exit 1; }
  sleep 3
done

if docker inspect -f '{{.State.Running}}' mss-microsip-freeswitch-1 2>/dev/null | grep -q true; then
  FS_WAS_RUNNING=yes
fi
say "stopping freeswitch for the whole run (it was running: $FS_WAS_RUNNING)"
$COMPOSE stop freeswitch >/dev/null 2>&1 || true
FS_STATE_BEFORE=$(docker inspect -f '{{.State.Status}}' mss-microsip-freeswitch-1 2>/dev/null || echo absent)
say "freeswitch is now '$FS_STATE_BEFORE'"

say "caller A dials $DIAL as $CALL_A"
(cd "$HERE" && CALL_ID="$CALL_A" FROM_TAG="fsless${STAMP}a" DIAL="$DIAL" \
  SIP_PORT=45080 RTP_PORT=45082 RTP_SSRC=0x51510001 \
  SPEECH_WAV="$IO_DIR/tone_a.wav" EAR_DIR="$IO_DIR" EAR_PREFIX=ear_a \
  CALL_SECONDS="$CALL_SECONDS" TALK_EVERY=1 \
  python3 host_test_caller.py > "$IO_DIR/caller-a.log" 2>&1) &
PID_A=$!

sleep "$JOIN_STAGGER"
say "caller B dials $DIAL as $CALL_B"
(cd "$HERE" && CALL_ID="$CALL_B" FROM_TAG="fsless${STAMP}b" DIAL="$DIAL" \
  SIP_PORT=45084 RTP_PORT=45086 RTP_SSRC=0x51510002 \
  SPEECH_WAV="$IO_DIR/tone_b.wav" EAR_DIR="$IO_DIR" EAR_PREFIX=ear_b \
  CALL_SECONDS=$((CALL_SECONDS - JOIN_STAGGER)) TALK_EVERY=1 \
  python3 host_test_caller.py > "$IO_DIR/caller-b.log" 2>&1) &
PID_B=$!

sleep 8
say "mid-call, from the pod's own metrics:"
curl -s "http://$METRICS/metrics" | grep -E '^mss_(conferences_live|conference_members_live|inline_legs_live|conference_mixed_frames_total|conference_clipped_samples_total)' |
  tee "$IO_DIR/metrics-midcall.txt" | sed 's/^/  /'
MEMBERS=$(sed -n 's/^mss_conference_members_live //p' "$IO_DIR/metrics-midcall.txt")
CONFS=$(sed -n 's/^mss_conferences_live //p' "$IO_DIR/metrics-midcall.txt")
LEGS=$(sed -n 's/^mss_inline_legs_live //p' "$IO_DIR/metrics-midcall.txt")
[ "$MEMBERS" = "2" ] || fail "expected 2 conference members mid-call, metrics say '$MEMBERS'"
[ "$CONFS" = "1" ] || fail "expected 1 live conference mid-call, metrics say '$CONFS'"
[ "$LEGS" = "2" ] || fail "expected 2 live inline legs mid-call, metrics say '$LEGS'"

for call in "$CALL_A" "$CALL_B"; do
  described=$("$CTL" "http://$CONTROL" describe "$call" 2>&1 || true)
  printf '%s\n' "$described" > "$IO_DIR/describe-$call.txt"
  case "$described" in
    *"group: \"$DIAL\""*) ;;
    *) fail "$call is not in group $DIAL: $described" ;;
  esac
  case "$described" in
    *Inline*|*"kind: 2"*) ;;
    *) fail "$call is not an INLINE session: $described" ;;
  esac
done
say "both sessions describe as INLINE in group $DIAL"

say "waiting for both callers to hang up"
wait "$PID_A" || fail "caller A exited non-zero"
wait "$PID_B" || fail "caller B exited non-zero"
PID_A=""; PID_B=""
tail -3 "$IO_DIR/caller-a.log" | sed 's/^/  a| /'
tail -3 "$IO_DIR/caller-b.log" | sed 's/^/  b| /'

FS_STATE_AFTER=$(docker inspect -f '{{.State.Status}}' mss-microsip-freeswitch-1 2>/dev/null || echo absent)
say "freeswitch state after the call: '$FS_STATE_AFTER'"
case "$FS_STATE_BEFORE:$FS_STATE_AFTER" in
  exited:exited|absent:absent) say "FreeSWITCH was down for the whole call" ;;
  *) fail "freeswitch was not stopped throughout ($FS_STATE_BEFORE -> $FS_STATE_AFTER)" ;;
esac

say "judging the ears"
python3 - "$IO_DIR" "$TONE_A" "$TONE_B" <<'PY' > "$IO_DIR/ears.json"
import glob, json, os, sys
io_dir, hz_a, hz_b = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
spec = []
for prefix, own, other in (("ear_a", hz_a, hz_b), ("ear_b", hz_b, hz_a)):
    found = sorted(glob.glob(os.path.join(io_dir, f"{prefix}_*.wav")))
    if len(found) != 1:
        raise SystemExit(f"{prefix}: expected exactly one ear wav, found {found}")
    spec.append({"path": found[0], "label": f"{prefix} hears the other leg",
                 "present": [other], "absent": [own]})
print(json.dumps(spec))
PY
python3 "$HERE/conference_report.py" wavs "$IO_DIR/ears.json" || fail "ear assertions failed"

say "the shim's transaction log"
$COMPOSE logs sip-shim 2>&1 | grep "sip-shim:" | grep -E "$CALL_A|$CALL_B" | sed 's/^.*sip-shim: /  /'
ANSWERED=$($COMPOSE logs sip-shim 2>&1 | grep -c "mss answered" || true)
[ "$ANSWERED" -ge 2 ] || fail "the shim answered $ANSWERED calls, expected at least 2"

say "artifacts in $IO_DIR"
if [ "$FAILURES" -eq 0 ]; then
  say "done: FreeSWITCH was stopped and two SIP phones heard each other through MSS"
else
  say "done with $FAILURES failure(s)"
  exit 1
fi
