"""Judges the conference drill, from the numbers alone.

Every claim P4-6 makes is of one shape: during phase P, ear E carried tones X
and Y and did not carry Z. The peers and the monitor stamp a Goertzel per tone
per delivered frame with the wall clock it arrived at; the drill and the
injector stamp the phase boundaries on the same clock. This script joins the
two and decides.

A tone is judged relative to the loudest tone in that same ear during that same
phase, not against an absolute level: the mix sums whatever is speaking, so the
absolute level of "present" moves with the member count, while the ratio does
not. Spectral leakage sets the floor -- 440/880/1320/1760 are harmonics, a
20 ms window is 160 samples, and a pure sine one harmonic away still reads a
few percent in its neighbour's bin -- so present means >= PRESENT_RATIO of the
loudest and absent means <= ABSENT_RATIO of it, with a wide gap between them.

Both edges of every phase window are trimmed by SETTLE_MS, because a route
change is an async command to the conference thread and the egress carries a
queue of already-paced audio.

  conference_report.py ears <io-dir>      reads <io-dir>/manifest.json
  conference_report.py wavs <spec.json>   [{path, label, present, absent}]
  conference_report.py lengths <ms> <wav>...  every file the same length

Env: PRESENT_RATIO (0.20), ABSENT_RATIO (0.10), PRESENT_FLOOR (150),
     SETTLE_MS (400).
"""

import json
import math
import os
import struct
import sys
import wave

PRESENT_RATIO = float(os.environ.get("PRESENT_RATIO", "0.20"))
ABSENT_RATIO = float(os.environ.get("ABSENT_RATIO", "0.10"))
PRESENT_FLOOR = float(os.environ.get("PRESENT_FLOOR", "150"))
SETTLE_MS = float(os.environ.get("SETTLE_MS", "400"))

BLOCK = 8000


def goertzel(samples, hz, rate):
    if not samples:
        return 0.0
    omega = 2 * math.pi * hz / rate
    coeff = 2 * math.cos(omega)
    s_prev = s_prev2 = 0.0
    for sample in samples:
        s = sample + coeff * s_prev - s_prev2
        s_prev2 = s_prev
        s_prev = s
    power = s_prev2 * s_prev2 + s_prev * s_prev - coeff * s_prev * s_prev2
    return math.sqrt(max(power, 0.0)) / len(samples)


def rows(path):
    if not os.path.exists(path):
        return []
    out = []
    with open(path) as source:
        for line in source:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def key(hz):
    return f"{float(hz):g}"


def judge(name, levels, present, absent, failures, note=""):
    reference = max(levels.values()) if levels else 0.0
    verdicts = []
    for hz in present:
        level = levels.get(key(hz), 0.0)
        ok = level >= max(PRESENT_RATIO * reference, PRESENT_FLOOR)
        verdicts.append(("+", hz, level, ok))
        if not ok:
            failures.append(f"{name}: {key(hz)} Hz should be audible, read {level:.0f}")
    for hz in absent:
        level = levels.get(key(hz), 0.0)
        ok = level <= ABSENT_RATIO * reference or level < PRESENT_FLOOR
        verdicts.append(("-", hz, level, ok))
        if not ok:
            failures.append(f"{name}: {key(hz)} Hz should be silent, read {level:.0f}")
    shown = "  ".join(
        f"{sign}{key(hz)}={level:7.0f}{'' if ok else ' FAIL'}"
        for sign, hz, level, ok in verdicts
    )
    print(f"  {name:34s} {shown}{note}")


def ears(io_dir):
    with open(os.path.join(io_dir, "manifest.json")) as source:
        manifest = json.load(source)

    phases = {}
    for phase in manifest["phases"]:
        phases[phase["phase"]] = (phase["from"], phase["to"])
    for extra in manifest.get("phase_files", []):
        for phase in rows(os.path.join(io_dir, extra)):
            if "from" in phase and "to" in phase:
                phases[phase["phase"]] = (phase["from"], phase["to"])

    timelines = {}
    for ear in manifest["ears"]:
        timelines[ear["name"]] = rows(os.path.join(io_dir, ear["timeline"]))
        print(f"{ear['name']}: {len(timelines[ear['name']])} frames "
              f"from {ear['timeline']}")

    failures = []
    settle = SETTLE_MS / 1000.0
    for expectation in manifest["expect"]:
        phase = expectation["phase"]
        if phase not in phases:
            failures.append(f"{phase}: no window was stamped for this phase")
            continue
        start, end = phases[phase]
        start += settle
        end -= settle
        if end <= start:
            failures.append(f"{phase}: the window is shorter than the settle time")
            continue
        name = expectation["ear"]
        window = [
            row for row in timelines.get(name, [])
            if start <= row["at"] <= end
        ]
        if not window:
            failures.append(f"{phase}/{name}: no frames arrived in the window")
            print(f"  {phase}/{name:26s} NO FRAMES IN WINDOW")
            continue
        levels = {}
        for hz in window[0].get("tones", {}):
            levels[hz] = sum(row["tones"].get(hz, 0.0) for row in window) / len(window)
        judge(
            f"{phase}/{name}",
            levels,
            expectation.get("present", []),
            expectation.get("absent", []),
            failures,
            f"  ({len(window)} frames, {end - start:.1f}s)",
        )
    return failures


def wavs(spec_path):
    with open(spec_path) as source:
        spec = json.load(source)
    failures = []
    for entry in spec:
        path = entry["path"]
        label = entry.get("label", os.path.basename(path))
        if not os.path.exists(path):
            failures.append(f"{label}: {path} is not there")
            print(f"  {label:34s} MISSING")
            continue
        with wave.open(path, "rb") as source:
            channels = source.getnchannels()
            width = source.getsampwidth()
            rate = source.getframerate()
            count = source.getnframes()
            frames = source.readframes(count)
        if width != 2:
            failures.append(f"{label}: {width * 8}-bit, expected 16")
            continue
        total = len(frames) // 2
        flat = struct.unpack(f"<{total}h", frames[: total * 2])
        tones = set()
        for hz in entry.get("present", []) + entry.get("absent", []):
            tones.add(float(hz))
        for channel in range(channels):
            samples = flat[channel::channels]
            levels = {}
            blocks = max(1, len(samples) // BLOCK)
            for hz in sorted(tones):
                total_level = 0.0
                for index in range(blocks):
                    block = samples[index * BLOCK:(index + 1) * BLOCK]
                    total_level += goertzel(block, hz, rate)
                levels[key(hz)] = total_level / blocks
            name = label if channels == 1 else f"{label}#ch{channel}"
            judge(
                name,
                levels,
                entry.get("present", []),
                entry.get("absent", []),
                failures,
                f"  ({channels}ch {rate}Hz {count / rate:.2f}s)",
            )
    return failures


def lengths(tolerance_ms, paths):
    failures = []
    measured = []
    for path in paths:
        if not os.path.exists(path):
            failures.append(f"{os.path.basename(path)} is not there")
            continue
        with wave.open(path, "rb") as source:
            rate = source.getframerate()
            count = source.getnframes()
        measured.append((path, 1000.0 * count / rate))
        print(f"  {os.path.basename(path):24s} {1000.0 * count / rate:9.0f} ms")
    if len(measured) > 1:
        shortest = min(length for _, length in measured)
        longest = max(length for _, length in measured)
        print(f"  spread {longest - shortest:.0f} ms against a {tolerance_ms:.0f} ms bar")
        if longest - shortest > tolerance_ms:
            failures.append(
                f"the group's files differ by {longest - shortest:.0f} ms, over "
                f"{tolerance_ms:.0f}: a member of one group is padded to a shared t=0, "
                "so equal length is the criterion"
            )
    return failures


def main():
    if len(sys.argv) < 3:
        raise SystemExit(__doc__)
    mode = sys.argv[1]
    if mode == "ears":
        failures = ears(sys.argv[2])
    elif mode == "wavs":
        failures = wavs(sys.argv[2])
    elif mode == "lengths":
        failures = lengths(float(sys.argv[2]), sys.argv[3:])
    else:
        raise SystemExit(__doc__)
    print()
    if failures:
        for failure in failures:
            print(f"FAIL {failure}")
        raise SystemExit(f"{len(failures)} assertion(s) failed")
    print("all assertions passed")


main()
