"""Turns the inline drill's two timelines into the P3-4 number.

Reads the peer's per-packet arrival timeline and the consumer's Clear stamps
(same kernel clock, both written by containers on the lab network) and answers
four questions:

  egress pacing    packets per second, sequence continuity, rtp timestamp step
  the ear          did the injected tone reach the peer at all
  cut-through      Clear sent -> first arrived packet whose tone has collapsed
  the tap          left to the consumer's own row (it holds SINK)

The cut-through number is an ARRIVAL measurement, so it contains everything
between the Clear and the ear: the gRPC hop, the egress pump's 5 ms tick, the
packet already on the wire, and the lab network. It is an upper bound on what
MSS itself costs, never a lower one -- which is the honest direction for a
target of "at most one ptime".

An iteration is only counted if the packets just before its Clear were tone;
otherwise there was nothing to cut and the round says nothing.
"""

import json
import os
import sys

TONE_FLOOR = float(os.environ.get("TONE_FLOOR", "1500"))
PRE_WINDOW = float(os.environ.get("PRE_WINDOW", "0.2"))
POST_WINDOW = float(os.environ.get("POST_WINDOW", "2.0"))
MARK_SLACK_MS = float(os.environ.get("MARK_SLACK_MS", "150"))


def log(message):
    print(f"inline-report: {message}", flush=True)


def rows(path):
    with open(path) as source:
        return [json.loads(line) for line in source if line.strip()]


def percentile(values, fraction):
    if not values:
        return float("nan")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(fraction * (len(ordered) - 1))))
    return ordered[index]


def main():
    io_dir = sys.argv[1] if len(sys.argv) > 1 else "lab/out/inline"
    peer = rows(os.path.join(io_dir, "peer-timeline.jsonl"))
    consumer = rows(os.path.join(io_dir, "consumer-timeline.jsonl"))
    if not peer:
        raise SystemExit("the peer heard no packets at all")

    failures = []

    by_ssrc = {}
    for packet in peer:
        by_ssrc.setdefault(packet["ssrc"], []).append(packet)
    log(f"the peer's ear carried {len(by_ssrc)} ssrc(s): "
        + ", ".join(f"{ssrc:08x} ({len(p)} packets)" for ssrc, p in by_ssrc.items()))
    ssrc, packets = max(by_ssrc.items(), key=lambda item: len(item[1]))
    packets.sort(key=lambda packet: packet["at"])

    span = packets[-1]["at"] - packets[0]["at"]
    rate = (len(packets) - 1) / span if span > 0 else 0.0
    steps = {}
    seq_breaks = 0
    for before, after in zip(packets, packets[1:]):
        steps[after["ts"] - before["ts"]] = steps.get(after["ts"] - before["ts"], 0) + 1
        if (after["seq"] - before["seq"]) & 0xFFFF != 1:
            seq_breaks += 1
    log(f"egress ssrc {ssrc:08x}: {len(packets)} packets over {span:.1f}s = "
        f"{rate:.2f} pkt/s, {seq_breaks} sequence breaks, ts steps {steps}")
    if not 47.0 <= rate <= 53.0:
        failures.append(f"egress pacing is {rate:.2f} pkt/s, not ~50")
    if seq_breaks:
        failures.append(f"{seq_breaks} sequence discontinuities at the peer's ear")
    if set(steps) != {160}:
        failures.append(f"rtp timestamp steps are not all 160: {steps}")

    voiced = [packet for packet in packets if packet["tone"] >= TONE_FLOOR]
    peak = max(packet["tone"] for packet in packets)
    log(f"the injected tone reached the ear in {len(voiced)}/{len(packets)} "
        f"packets, peak energy {peak:.0f} (floor {TONE_FLOOR:.0f})")
    if not voiced:
        failures.append("the peer never heard the injected tone")

    tap = next((row for row in consumer if row.get("kind") == "tap"), None)
    if tap:
        log(f"the hub tapped {tap['frames']} frames of the peer's own audio: "
            f"peak rms {tap['peak_rms']}, peak {tap['tone_hz']:g} Hz energy "
            f"{tap['peak_tone']}")
        if tap["frames"] < 100 or tap["peak_rms"] < 100:
            failures.append("the tap side carried no real audio while injecting")
    else:
        failures.append("the consumer wrote no tap row")

    marks = [row for row in consumer if row.get("kind") == "mark"]
    acks = [row["ack_ms"] for row in marks if row.get("acked_at")]
    if marks:
        log("mark drain barrier: {}/{} acked, ms {}".format(
            len(acks), len(marks), [round(value) for value in acks]))
    if len(acks) != len(marks):
        failures.append(f"{len(marks) - len(acks)} mark(s) were never acked")
    for row, ack in zip([m for m in marks if m.get("acked_at")], acks):
        budget = row.get("lead_ms", 400) + MARK_SLACK_MS
        if ack > budget:
            failures.append(
                f"mark {row['name']} acked in {ack:.0f} ms, more than the "
                f"{row.get('lead_ms', 400):.0f} ms it had queued plus "
                f"{MARK_SLACK_MS:.0f} ms slack")

    cut = []
    for row in consumer:
        if row.get("kind") != "barge":
            continue
        clear_at = row["clear_at"]
        before = [
            packet for packet in packets
            if clear_at - PRE_WINDOW <= packet["at"] < clear_at
        ]
        if not before or not all(packet["tone"] >= TONE_FLOOR for packet in before):
            log(f"iteration {row['iteration']}: not counted, the ear was not "
                f"already hearing tone ({len(before)} packets before the clear)")
            continue
        after = [packet for packet in packets
                 if clear_at <= packet["at"] <= clear_at + POST_WINDOW]
        quiet = next((p for p in after if p["tone"] < TONE_FLOOR), None)
        if not quiet:
            failures.append(f"iteration {row['iteration']} never went quiet")
            continue
        millis = 1000 * (quiet["at"] - clear_at)
        cut.append(millis)
        log(f"iteration {row['iteration']}: cut through in {millis:.1f} ms "
            f"(seq {quiet['seq']}, silent={quiet['silent']})")

    if len(cut) < 10:
        failures.append(f"only {len(cut)} usable barge iterations")
    if cut:
        log("cut-through ms: n={} p50={:.1f} p95={:.1f} max={:.1f} min={:.1f}".format(
            len(cut), percentile(cut, 0.5), percentile(cut, 0.95), max(cut), min(cut)))
        with open(os.path.join(io_dir, "barge-summary.json"), "w") as out:
            json.dump({
                "iterations": len(cut),
                "p50_ms": percentile(cut, 0.5),
                "p95_ms": percentile(cut, 0.95),
                "max_ms": max(cut),
                "min_ms": min(cut),
                "samples_ms": [round(value, 1) for value in cut],
                "egress_pkt_per_second": round(rate, 2),
                "sequence_breaks": seq_breaks,
            }, out, indent=2)

    if failures:
        for failure in failures:
            log(f"FAILED: {failure}")
        sys.exit(1)
    log("all assertions passed")


main()
