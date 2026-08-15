#!/bin/sh
# Waits for call-watcher to find a live call, then taps it.
#
# mediaserverd reads MSS_TAP_CALL_ID at startup, and with OpenSIPS in the path
# that id is not known until the softphone dials. Building first means the tap
# starts within a second of the call rather than after a cold cargo build.
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
  export MSS_TAP_CALL_ID MSS_TAP_FROM_TAGS

  echo "tap-live-call: tapping $MSS_TAP_CALL_ID tags=$MSS_TAP_FROM_TAGS"
  cargo run --quiet -p mediaserverd || true

  echo "tap-live-call: tap finished; waiting for this call to end before the next one"
  while [ -f "$CALL_ENV" ]; do
    sleep 1
  done
done
