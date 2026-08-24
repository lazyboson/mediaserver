#!/bin/sh
# Item P2-3 / Phase-2 exit criterion: record ONE live call both ways -- with
# FreeSWITCH's own uuid_record (RECORD_STEREO, the shape MSS copies) and with
# an MSS FILE_S3 recording attachment -- pull both wavs and run
# lab/recording_parity.py over them. Until this drill existed the harness had
# never seen a real FreeSWITCH recording.
#
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
#     up -d rtpengine opensips freeswitch call-watcher redpanda redis \
#           minio minio-init llm-bridge mss-control
#   ./lab/fs_parity_drill.sh
#
# The lab's FS image has mod_dptools (record_session, uuid_record) and
# mod_sndfile (wav), and /var/lib/freeswitch/recordings is writable, so the FS
# half is real. What the lab canNOT give is a two-party call: extension 9000
# answers and plays silence_stream://-1, so FS's write side is silence and only
# the customer channel carries audio. Set DIAL to a bridging extension on a
# richer rig for a two-voice comparison.
#
# The mediaserverd compose service must stay down -- it would tap the same call.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
OUT=${OUT:-$HERE/out}
CONTROL=${CONTROL:-http://127.0.0.1:50551}
CALL_SECONDS=${CALL_SECONDS:-40}
RECORD_SECONDS=${RECORD_SECONDS:-25}
DIAL=${DIAL:-9000}
ACCOUNT=${ACCOUNT:-acct-parity}
WATCHER=${WATCHER:-mss-microsip-call-watcher-1}
FS=${FS:-mss-microsip-freeswitch-1}
MC=${MC:-mss-microsip-minio-1}
BUCKET=${BUCKET:-lab-recordings}
FS_DIR=${FS_DIR:-/var/lib/freeswitch/recordings}
DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
export DOCKER_API_VERSION

STAMP=$(date +%s)
EXTERNAL_ID=parity-drill-$STAMP
LOG=$OUT/fs-parity-drill-$STAMP.log
RECORDING=$ACCOUNT/rec-$STAMP.wav
MSS_WAV=$OUT/parity-$STAMP-mss.wav
FS_WAV=$OUT/parity-$STAMP-fs.wav
FS_PATH=$FS_DIR/parity-$STAMP.wav

mkdir -p "$OUT"
echo "drill: session $EXTERNAL_ID, log $LOG" | tee "$LOG"

echo "drill: what the FS image can record" | tee -a "$LOG"
docker exec "$FS" fs_cli -x "show application" 2>&1 |
  grep -E '^(record|record_session|stop_record_session)' | tee -a "$LOG"
docker exec "$FS" fs_cli -x "show api" 2>&1 |
  grep -E '^uuid_record' | tee -a "$LOG"

echo "drill: building mss_ctl before the call so it starts immediately" | tee -a "$LOG"
( cd "$REPO" && cargo build --quiet -p control-api --example mss_ctl )

docker exec "$WATCHER" rm -f /shared/call.env 2>/dev/null || true

echo "drill: dialing $DIAL for ${CALL_SECONDS}s" | tee -a "$LOG"
( cd "$HERE" && CALL_SECONDS=$CALL_SECONDS DIAL=$DIAL EAR_DIR=out \
    python3 host_test_caller.py >"$OUT/host-caller-$STAMP.log" 2>&1 ) &
CALLER=$!

CALL_ID=""
FROM_TAGS=""
waited=0
while [ "$waited" -lt 20 ]; do
  if env=$(docker exec "$WATCHER" cat /shared/call.env 2>/dev/null); then
    CALL_ID=$(echo "$env" | sed -n 's/^MSS_TAP_CALL_ID=//p')
    FROM_TAGS=$(echo "$env" | sed -n 's/^MSS_TAP_FROM_TAGS=//p')
    if [ -n "$CALL_ID" ]; then
      break
    fi
  fi
  waited=$((waited + 1))
  sleep 1
done
if [ -z "$CALL_ID" ]; then
  echo "drill: call_watcher never announced a call; is the SIP half up?" >&2
  kill "$CALLER" 2>/dev/null || true
  exit 1
fi
echo "drill: call $CALL_ID tags $FROM_TAGS" | tee -a "$LOG"

UUID=""
waited=0
while [ "$waited" -lt 10 ]; do
  for candidate in $(docker exec "$FS" fs_cli -x "show channels as delim |" 2>/dev/null |
                       sed -n 's/^\([0-9a-f-]\{36\}\)|.*/\1/p'); do
    got=$(docker exec "$FS" fs_cli -x "uuid_getvar $candidate sip_call_id" 2>/dev/null |
            tr -d '\r\n')
    if [ "$got" = "$CALL_ID" ]; then
      UUID=$candidate
      break
    fi
  done
  [ -n "$UUID" ] && break
  waited=$((waited + 1))
  sleep 1
done
if [ -z "$UUID" ]; then
  echo "drill: no FS channel carries Call-ID $CALL_ID; FS is not in the path" >&2
  kill "$CALLER" 2>/dev/null || true
  exit 1
fi
echo "drill: FS channel $UUID" | tee -a "$LOG"

cd "$REPO"
cargo run --quiet -p control-api --example mss_ctl -- \
    "$CONTROL" create "$EXTERNAL_ID" "$CALL_ID" "$FROM_TAGS" 2>&1 | tee -a "$LOG"

cargo run --quiet -p control-api --example mss_ctl -- \
    "$CONTROL" record "$EXTERNAL_ID" "$RECORDING" parity 2>&1 | tee -a "$LOG"

echo "drill: FS record_session (RECORD_STEREO) to $FS_PATH" | tee -a "$LOG"
docker exec "$FS" fs_cli -x "uuid_setvar $UUID RECORD_STEREO true" 2>&1 | tee -a "$LOG"
docker exec "$FS" fs_cli -x "uuid_record $UUID start $FS_PATH" 2>&1 | tee -a "$LOG"

sleep "$RECORD_SECONDS"

docker exec "$FS" fs_cli -x "uuid_record $UUID stop $FS_PATH" 2>&1 | tee -a "$LOG"
cargo run --quiet -p control-api --example mss_ctl -- \
    "$CONTROL" destroy "$EXTERNAL_ID" 2>&1 | tee -a "$LOG"
wait "$CALLER" 2>/dev/null || true

sleep 5
docker cp "$FS:$FS_PATH" "$FS_WAV" 2>&1 | tee -a "$LOG" || true
docker exec "$MC" sh -c \
    "mc alias set lab http://127.0.0.1:9000 \
       \${MINIO_ROOT_USER:-minioadmin} \${MINIO_ROOT_PASSWORD:-minioadmin} >/dev/null &&
     mc cp lab/$BUCKET/$RECORDING /tmp/parity-$STAMP-mss.wav" 2>&1 | tee -a "$LOG" || true
docker cp "$MC:/tmp/parity-$STAMP-mss.wav" "$MSS_WAV" 2>&1 | tee -a "$LOG" || true

if [ ! -s "$FS_WAV" ] || [ ! -s "$MSS_WAV" ]; then
  echo "drill: missing one of the two recordings; nothing to compare" >&2
  ls -l "$FS_WAV" "$MSS_WAV" 2>&1 | tee -a "$LOG" || true
  exit 1
fi

echo "drill: parity" | tee -a "$LOG"
python3 "$HERE/recording_parity.py" --mss "$MSS_WAV" --fs "$FS_WAV" 2>&1 | tee -a "$LOG" || true

echo "drill: done; wavs $MSS_WAV and $FS_WAV, transcript in $LOG"
