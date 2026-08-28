#!/bin/sh
# tc netem on the tap link, which testing.md calls "the single highest-value
# item" on the lab list: damage that only the MSS pods see, so the jitter buffer
# is tested rather than the call.
#
#   ./lab/netem.sh probe            # can this kernel do netem at all?
#   ./lab/netem.sh apply loss5      # impair rtpengine -> MSS only
#   ./lab/netem.sh show
#   ./lab/netem.sh clear
#
# How it attaches: rtpengine's own network namespace, entered by a sidecar
# container with --net container: and CAP_NET_ADMIN, because `docker exec`
# cannot add a capability to an already-running container. The sidecar reuses
# the rtpengine image, which already carries iproute2.
#
# Why a prio qdisc and not netem at the root: netem at the root would impair
# *every* packet rtpengine sends, including the two call legs, and then loss
# reported by MSS could not be attributed. Instead all traffic defaults to band
# 1:1 (priomap is all zeros) and u32 filters steer only packets addressed to the
# MSS pods into 1:3, where the netem lives. That is testing.md's first injection
# point -- "on the tap link (rtpengine -> MSS)" -- exactly.
#
# Profiles are the impairment matrix's rows, verbatim.
set -eu

RTPENGINE=${RTPENGINE:-mss-microsip-rtpengine-1}
NETEM_IMAGE=${NETEM_IMAGE:-jambonz/rtpengine}
DEV=${DEV:-eth0}
TAP_TARGETS=${TAP_TARGETS:-172.31.99.31 172.31.99.32 172.31.99.33}
# rtpengine answers NG on this port, so its replies to a pod leave with this as
# their source port. They must stay OUT of the impaired band -- see below.
NG_PORT=${NG_PORT:-22222}
DOCKER_API_VERSION=${DOCKER_API_VERSION:-1.43}
export DOCKER_API_VERSION

profile_spec() {
  case "$1" in
    clean|none) echo "" ;;
    loss1) echo "loss 1%" ;;
    loss5) echo "loss 5%" ;;
    # NOT "loss 10% 50%". That is netem's legacy correlation form, and the
    # correlation collapses the effective rate: the qdisc's own counter showed
    # 12 packets dropped out of 10,548 -- 0.11%, not 10% -- and MSS duly
    # reported exactly 12. The profile measured nothing it claimed to.
    # gemodel is the Gilbert-Elliott burst model: p is good->bad, r is
    # bad->good, so the steady-state loss is p/(p+r) = 10% and a burst runs
    # 1/r = about 3.3 packets.
    burst) echo "loss gemodel 3.3% 30%" ;;
    reorder) echo "delay 30ms 20ms reorder 25% 50%" ;;
    reorder-far) echo "delay 120ms 60ms reorder 25% 50%" ;;
    duplicate) echo "duplicate 1%" ;;
    jitter) echo "delay 20ms 15ms distribution normal" ;;
    *) echo "unknown profile: $1" >&2; exit 2 ;;
  esac
}

in_netns() {
  docker run --rm --net "container:$RTPENGINE" --cap-add NET_ADMIN --user 0 \
    --entrypoint sh "$NETEM_IMAGE" -c "$1"
}

case "${1:-probe}" in
  probe)
    # A kernel without CONFIG_NET_SCH_NETEM answers "Specified qdisc kind is
    # unknown", which is the whole verdict. Leave nothing behind either way.
    if in_netns "tc qdisc add dev $DEV root netem loss 0.01% >/dev/null 2>&1 && \
        tc qdisc del dev $DEV root >/dev/null 2>&1"; then
      echo "netem: available on $DEV in $RTPENGINE"
      exit 0
    fi
    echo "netem: NOT available -- this kernel has no sch_netem"
    in_netns "tc qdisc add dev $DEV root netem loss 0.01%" 2>&1 | head -2 || true
    exit 1
    ;;
  apply)
    spec=$(profile_spec "${2:?usage: netem.sh apply <profile>}")
    if [ -z "$spec" ]; then
      exec "$0" clear
    fi
    # Steering on destination address alone caught rtpengine's NG *control*
    # replies as well as the tap's RTP, because both are addressed to the pod.
    # A loss profile then timed out subscribe commands -- "ng node did not
    # reply, attempts 3" -- so the pod never got a tap at all and the phase
    # measured a broken subscription instead of the jitter buffer, which is the
    # opposite of what this tool is for. Control traffic is pinned to the clean
    # band first, at a higher filter priority, and only what is left of the
    # pod-bound traffic goes into the netem band.
    filters="tc filter add dev $DEV protocol ip parent 1: prio 1 u32 \
match ip sport $NG_PORT 0xffff flowid 1:1;"
    for target in $TAP_TARGETS; do
      filters="$filters tc filter add dev $DEV protocol ip parent 1: prio 2 u32 \
match ip dst $target/32 flowid 1:3;"
    done
    in_netns "set -e
      tc qdisc del dev $DEV root 2>/dev/null || true
      tc qdisc add dev $DEV root handle 1: prio bands 3 \
        priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
      tc qdisc add dev $DEV parent 1:3 handle 30: netem $spec
      $filters
      tc qdisc show dev $DEV"
    echo "netem: applied '$2' ($spec) to $TAP_TARGETS on $DEV in $RTPENGINE,"
  echo "netem: sparing NG control replies from source port $NG_PORT"
    ;;
  show)
    in_netns "tc qdisc show dev $DEV; tc filter show dev $DEV"
    ;;
  clear)
    in_netns "tc qdisc del dev $DEV root 2>/dev/null || true; tc qdisc show dev $DEV"
    echo "netem: cleared on $DEV in $RTPENGINE"
    ;;
  *)
    echo "usage: $0 probe|apply <profile>|show|clear" >&2
    exit 2
    ;;
esac
