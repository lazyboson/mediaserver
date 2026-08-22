#!/usr/bin/env bash
# Defect D5 / task 13 drill: prove the event pump loses nothing across a
# broker outage. Sends a steady stream of MediaEvents through the real
# RskafkaTransport while Redpanda is stopped for a window in the middle, then
# asserts the per-key seq run on the topic is complete and in order.
#
# Prerequisites: the lab Redpanda is up and published on the host
#   DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml up -d redpanda
set -uo pipefail

BROKERS=${BROKERS:-127.0.0.1:19092}
TOPIC=${TOPIC:-mss.events.drill}
CONTAINER=${CONTAINER:-mss-microsip-redpanda-1}
COUNT=${COUNT:-60}
INTERVAL_MS=${INTERVAL_MS:-1000}
QUIET_BEFORE=${QUIET_BEFORE:-15}
OUTAGE_SECONDS=${OUTAGE_SECONDS:-30}
export DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
log="$root/lab/out/event-outage-drill.log"
mkdir -p "$root/lab/out"

echo "building the drill test"
cargo test --manifest-path "$root/Cargo.toml" -p mediaserverd --test kafka_outage --no-run
if [ $? -ne 0 ]; then
    echo "the drill test does not build"
    exit 1
fi

echo "drill: $COUNT events every ${INTERVAL_MS}ms to $TOPIC on $BROKERS"
MSS_TEST_KAFKA_BROKERS="$BROKERS" \
MSS_TEST_EVENTS_TOPIC="$TOPIC" \
MSS_TEST_EVENT_COUNT="$COUNT" \
MSS_TEST_EVENT_INTERVAL_MS="$INTERVAL_MS" \
    cargo test --manifest-path "$root/Cargo.toml" -p mediaserverd --test kafka_outage \
    -- --nocapture >"$log" 2>&1 &
drill=$!

sleep "$QUIET_BEFORE"
echo "stopping $CONTAINER for ${OUTAGE_SECONDS}s"
docker stop -t 5 "$CONTAINER" >/dev/null
sleep "$OUTAGE_SECONDS"
echo "starting $CONTAINER"
docker start "$CONTAINER" >/dev/null

wait "$drill"
status=$?
cat "$log"
if [ "$status" -eq 0 ]; then
    echo "DRILL PASSED: no event was lost across a ${OUTAGE_SECONDS}s outage"
else
    echo "DRILL FAILED: see $log"
fi
exit "$status"
