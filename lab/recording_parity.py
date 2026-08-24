#!/usr/bin/env python3
"""Compare an MSS recording against a FreeSWITCH RECORD_STEREO recording.

This is the harness the Phase-2 exit criterion asks for ("byte-comparable
recordings vs FS output"). It does not sign parity off -- a human still has to
listen -- but it turns "sounds the same" into numbers, and it fails loudly when
the two files disagree in a way a downstream consumer would notice.

    python3 recording_parity.py --mss out/mss.wav --fs out/fs.wav

Getting the two files:

  MSS   the object is in the bucket under ${accountID}/${recordingID}.wav
        docker exec mss-microsip-minio-1 \
            mc cp lab/lab-recordings/acct/rec.wav /tmp/mss.wav
        docker cp mss-microsip-minio-1:/tmp/mss.wav out/mss.wav
        (or click it out of the console on http://127.0.0.1:9001)

  FS    record the same call with the legacy controller's recorder, or by hand:
        uuid_record <uuid> start /recordings/fs.wav
        with RECORD_STEREO=true set on the channel, so the customer is left
        and the agent is right -- the convention MSS follows.

What it checks, in the order a mismatch matters:

  1. container agreement: channels, sample rate, sample width. A difference
     here means downstream tooling that hardcodes 8 kHz 16-bit stereo breaks,
     and nothing below is meaningful.
  2. duration: the pause contract says a paused interval is absent from the
     audio, so a recording with pauses is SHORTER than wall clock and both
     files must be short by the same amount.
  3. alignment: FS starts recording when it is asked, MSS when its attachment
     opens, so a constant offset is expected. The harness finds the offset by
     maximizing correlation over a window and reports it; a large offset is a
     finding, not an error.
  4. sample agreement after alignment, per channel: identical-sample ratio,
     mean absolute difference, and worst-case difference. Exact equality is
     not expected -- the two paths conceal loss differently (MSS does G.711
     Appendix I PLC since item 17) -- so the bar is a tolerance, and the
     tolerance is printed with the verdict rather than hidden in the code.

Exit code 0 means every check passed at the given tolerances.
"""

import argparse
import sys
import wave


def read_wav(path):
    with wave.open(path, "rb") as handle:
        channels = handle.getnchannels()
        width = handle.getsampwidth()
        rate = handle.getframerate()
        frames = handle.getnframes()
        raw = handle.readframes(frames)
    if width != 2:
        raise SystemExit(f"{path}: {width * 8}-bit samples; this harness reads 16-bit")
    samples = memoryview(raw).cast("h")
    tracks = [samples[channel::channels] for channel in range(channels)]
    return {
        "path": path,
        "channels": channels,
        "rate": rate,
        "width": width,
        "frames": frames,
        "tracks": [list(track) for track in tracks],
    }


def rms(track):
    if not track:
        return 0.0
    total = sum(float(sample) * float(sample) for sample in track)
    return (total / len(track)) ** 0.5


def offsets_by_distance(search):
    """0, -1, +1, -2, +2 ... so a tie on a periodic tone keeps the smaller shift."""
    yield 0
    for distance in range(1, search + 1):
        yield -distance
        yield distance


def best_offset(left, right, window, search):
    """Offset (in frames) to add to `right` so it lines up with `left`."""
    if not left or not right:
        return 0, 0.0
    best, score = 0, None
    for offset in offsets_by_distance(search):
        total = 0.0
        counted = 0
        for index in range(window):
            a = index
            b = index + offset
            if a >= len(left) or b < 0 or b >= len(right):
                continue
            total += float(left[a]) * float(right[b])
            counted += 1
        if counted == 0:
            continue
        normalized = total / counted
        if score is None or normalized > score:
            best, score = offset, normalized
    return best, score or 0.0


def compare_track(name, mine, theirs, offset, tolerance):
    compared = 0
    identical = 0
    worst = 0
    total_difference = 0
    first_divergence = None
    for index in range(len(mine)):
        other = index + offset
        if other < 0 or other >= len(theirs):
            continue
        difference = abs(int(mine[index]) - int(theirs[other]))
        compared += 1
        total_difference += difference
        if difference == 0:
            identical += 1
        if difference > worst:
            worst = difference
        if difference > tolerance and first_divergence is None:
            first_divergence = index
    if compared == 0:
        return {"channel": name, "compared": 0, "verdict": "no overlap"}
    return {
        "channel": name,
        "compared": compared,
        "identical_ratio": identical / compared,
        "mean_difference": total_difference / compared,
        "worst_difference": worst,
        "first_divergence_frame": first_divergence,
        "mss_rms": rms(mine),
        "fs_rms": rms(theirs),
    }


def windowed_agreement(mine, theirs, rate, window_seconds, search, tolerance, stride):
    """Re-align every window instead of once for the whole file.

    A single global offset assumes the two recorders keep one sample grid for
    the whole call. They do not: MSS and FreeSWITCH have independent jitter
    buffers and conceal loss independently, so the offset between the files
    wanders. This reports the offset and agreement per window, which separates
    "the audio transform differs" (agreement poor in every window) from "only
    the timing differs" (agreement near-perfect in the windows that lock).
    """
    window = max(int(rate * window_seconds), 1)
    rows = []
    for start in range(0, max(len(theirs) - window, 0), window):
        segment = theirs[start : start + window]
        offset, _ = best_offset(mine[start:], segment, min(window, len(segment)), search)
        compared = 0
        identical = 0
        total_difference = 0
        for index in range(0, len(segment), stride):
            other = start + index + offset
            if other < 0 or other >= len(mine):
                continue
            difference = abs(int(mine[other]) - int(segment[index]))
            compared += 1
            total_difference += difference
            if difference <= tolerance:
                identical += 1
        if compared == 0:
            continue
        rows.append(
            {
                "at_seconds": start / rate,
                "offset": offset,
                "agreeing_ratio": identical / compared,
                "mean_difference": total_difference / compared,
            }
        )
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mss", required=True, help="the recording MSS uploaded")
    parser.add_argument("--fs", required=True, help="the FreeSWITCH RECORD_STEREO file")
    parser.add_argument(
        "--duration-tolerance-ms",
        type=int,
        default=200,
        help="how much shorter or longer the two files may be (default 200)",
    )
    parser.add_argument(
        "--sample-tolerance",
        type=int,
        default=64,
        help="per-sample difference treated as agreement (default 64 of 32768)",
    )
    parser.add_argument(
        "--mean-tolerance",
        type=float,
        default=200.0,
        help="maximum mean absolute per-sample difference allowed (default 200)",
    )
    parser.add_argument(
        "--identical-ratio",
        type=float,
        default=0.0,
        help="minimum ratio of bit-identical samples to demand (default 0: report only)",
    )
    parser.add_argument(
        "--align-window",
        type=int,
        default=8000,
        help="frames used to find the alignment offset (default 8000)",
    )
    parser.add_argument(
        "--drift-window",
        type=float,
        default=0.0,
        help="seconds per window for the windowed re-alignment report (0 = off)",
    )
    parser.add_argument(
        "--drift-stride",
        type=int,
        default=1,
        help="sample stride inside a drift window, to trade accuracy for time",
    )
    parser.add_argument(
        "--align-search",
        type=int,
        default=1600,
        help="maximum offset searched, in frames (default 1600 = 200 ms at 8k)",
    )
    arguments = parser.parse_args()

    mine = read_wav(arguments.mss)
    theirs = read_wav(arguments.fs)
    failures = []

    print(f"mss: {mine['path']}")
    print(
        f"  {mine['channels']}ch {mine['rate']}Hz {mine['width'] * 8}bit "
        f"{mine['frames']} frames ({mine['frames'] * 1000 // max(mine['rate'], 1)} ms)"
    )
    print(f"fs:  {theirs['path']}")
    print(
        f"  {theirs['channels']}ch {theirs['rate']}Hz {theirs['width'] * 8}bit "
        f"{theirs['frames']} frames ({theirs['frames'] * 1000 // max(theirs['rate'], 1)} ms)"
    )

    for field in ("channels", "rate", "width"):
        if mine[field] != theirs[field]:
            failures.append(f"{field}: mss {mine[field]} vs fs {theirs[field]}")
    if failures:
        for failure in failures:
            print(f"FAIL container {failure}")
        return 1

    rate = mine["rate"]
    mss_ms = mine["frames"] * 1000 // max(rate, 1)
    fs_ms = theirs["frames"] * 1000 // max(rate, 1)
    drift = abs(mss_ms - fs_ms)
    print(f"duration difference: {drift} ms (tolerance {arguments.duration_tolerance_ms})")
    if drift > arguments.duration_tolerance_ms:
        failures.append(f"duration differs by {drift} ms")

    names = ["customer-left", "agent-right", "channel-2", "channel-3"]
    offset, score = best_offset(
        mine["tracks"][0],
        theirs["tracks"][0],
        arguments.align_window,
        arguments.align_search,
    )
    print(
        f"alignment: fs is offset by {offset} frames "
        f"({offset * 1000 // max(rate, 1)} ms), correlation {score:.1f}"
    )

    for index, name in zip(range(mine["channels"]), names):
        report = compare_track(
            name,
            mine["tracks"][index],
            theirs["tracks"][index],
            offset,
            arguments.sample_tolerance,
        )
        if report.get("compared", 0) == 0:
            failures.append(f"{name}: the two files do not overlap")
            print(f"FAIL {name}: no overlap")
            continue
        print(
            f"{name}: compared={report['compared']} "
            f"identical={report['identical_ratio']:.4f} "
            f"mean_diff={report['mean_difference']:.1f} "
            f"worst_diff={report['worst_difference']} "
            f"rms mss={report['mss_rms']:.0f} fs={report['fs_rms']:.0f} "
            f"first_divergence={report['first_divergence_frame']}"
        )
        if report["identical_ratio"] < arguments.identical_ratio:
            failures.append(
                f"{name}: only {report['identical_ratio']:.4f} of samples are identical"
            )
        if report["mean_difference"] > arguments.mean_tolerance:
            failures.append(
                f"{name}: mean difference {report['mean_difference']:.1f} "
                f"exceeds {arguments.mean_tolerance:.1f}"
            )

    if arguments.drift_window > 0:
        rows = windowed_agreement(
            mine["tracks"][0],
            theirs["tracks"][0],
            rate,
            arguments.drift_window,
            arguments.align_search,
            arguments.sample_tolerance,
            max(arguments.drift_stride, 1),
        )
        print(
            f"windowed re-alignment ({arguments.drift_window:g}s windows, "
            f"stride {max(arguments.drift_stride, 1)}):"
        )
        for row in rows:
            print(
                f"  t={row['at_seconds']:6.1f}s offset={row['offset']:+6d} "
                f"agreeing={row['agreeing_ratio']:.4f} "
                f"mean_diff={row['mean_difference']:7.1f}"
            )
        if rows:
            best = max(rows, key=lambda row: row["agreeing_ratio"])
            spread = max(row["offset"] for row in rows) - min(
                row["offset"] for row in rows
            )
            print(
                f"  best window t={best['at_seconds']:.1f}s "
                f"agreeing={best['agreeing_ratio']:.4f} "
                f"mean_diff={best['mean_difference']:.1f}; "
                f"offset wanders over {spread} frames "
                f"({spread * 1000 // max(rate, 1)} ms)"
            )

    if failures:
        for failure in failures:
            print(f"FAIL {failure}")
        return 1
    print("PASS both recordings agree within the given tolerances")
    return 0


if __name__ == "__main__":
    sys.exit(main())
