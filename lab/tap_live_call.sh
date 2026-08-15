#!/bin/sh
# Waits for call-watcher to find a live call, then taps it -- re-arming
# correctly when calls overlap or outlive the tap window.
#
# The env file always describes the CURRENT live call, so "wait until the file
# disappears" is wrong the moment a new call arrives while the previous tap is
# still finishing: the file then belongs to the new call, and waiting on it
# blocks until that caller gives up. Instead: tap whatever the file names, stop
# the tap the moment the file stops naming that call (SIGINT, which
# mediaserverd turns into a clean stop that still writes every artifact), and
# re-arm immediately. A call that outlives one tap window simply gets tapped
# again -- re-subscribing to live calls is the design's recovery story.
set -eu

CALL_ENV=${CALL_ENV_FILE:-/shared/call.env}

echo "tap-live-call: building mediaserverd so the tap can start immediately"
cargo build --quiet -p mediaserverd

while true; do
  echo "tap-live-call: waiting for a call (dial 9000 from MicroSIP)"
  while [ ! -f "$CALL_ENV" ]; do
    sleep 1
  done
  . "$CALL_ENV"
  tapped=$MSS_TAP_CALL_ID
  export MSS_TAP_CALL_ID MSS_TAP_FROM_TAGS

  echo "tap-live-call: tapping $tapped tags=$MSS_TAP_FROM_TAGS"
  cargo run --quiet -p mediaserverd &
  tap_pid=$!

  while kill -0 "$tap_pid" 2>/dev/null; do
    current=""
    if [ -f "$CALL_ENV" ]; then
      . "$CALL_ENV"
      current=$MSS_TAP_CALL_ID
    fi
    if [ "$current" != "$tapped" ]; then
      echo "tap-live-call: call $tapped is gone; stopping its tap"
      kill -INT "$tap_pid" 2>/dev/null || true
      break
    fi
    sleep 1
  done
  wait "$tap_pid" || true
  sleep 1
done
