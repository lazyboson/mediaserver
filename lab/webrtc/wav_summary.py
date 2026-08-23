"""Summarises a recorded wav the way this drill needs to judge one.

Per channel: rms, peak, how much of it is digital silence, and the longest
unbroken silent run. That last number is the one that matters for a WebRTC
agent leg -- it is where DTX, concealment and a genuinely lost tap all show up,
and telling them apart is the point of the exercise:

  a DTX gap        the encoder chose not to send; the decoder filled it
  a concealed gap  packets really were lost; libopus invented the audio
  a dead tap       nothing arrived and nothing was filled

The wav alone cannot separate those three -- MSS's own counters do that
(mss_jitter_silence_gaps_total against mss_jitter_lost_total against
mss_ingest_datagrams_total). What the wav can say is whether the silence is
where the source put it, which is why the fake microphone has a known pattern.

Also reports the dominant frequency per channel by a plain Goertzel sweep, so a
tone-driven run can assert the pitch survived resampling and the conference.

Usage: python3 wav_summary.py <file.wav> [more.wav ...]
Env: SILENCE_LEVEL (absolute sample value counted as silence, default 64),
     TONE_CANDIDATES (comma-separated Hz to test, default 440,1000)
"""

import math
import os
import struct
import sys
import wave

SILENCE_LEVEL = int(os.environ.get("SILENCE_LEVEL", "64"))
TONE_CANDIDATES = [
    float(hz) for hz in os.environ.get("TONE_CANDIDATES", "440,1000").split(",") if hz
]


def goertzel(samples, rate, frequency):
    """Energy at one frequency, without pulling in a DFT library."""
    if not samples:
        return 0.0
    step = 2.0 * math.cos(2.0 * math.pi * frequency / rate)
    first = 0.0
    second = 0.0
    for sample in samples:
        current = sample + step * first - second
        second = first
        first = current
    return math.sqrt(max(first * first + second * second - step * first * second, 0.0))


def summarise(path):
    with wave.open(path, "rb") as source:
        channels = source.getnchannels()
        width = source.getsampwidth()
        rate = source.getframerate()
        count = source.getnframes()
        frames = source.readframes(count)

    if width != 2:
        print(f"{path}: {width * 8}-bit, only 16-bit is summarised")
        return

    total = len(frames) // 2
    flat = struct.unpack(f"<{total}h", frames[: total * 2])
    duration = count / rate if rate else 0.0

    print(f"{path}")
    print(f"  {channels} ch  {rate} Hz  {count} frames  {duration:.2f} s")

    for channel in range(channels):
        samples = flat[channel::channels]
        if not samples:
            continue
        peak = max(max(samples), -min(samples))
        energy = math.sqrt(sum(float(s) * s for s in samples) / len(samples))

        silent = 0
        longest = 0
        run = 0
        for sample in samples:
            if abs(sample) <= SILENCE_LEVEL:
                silent += 1
                run += 1
                longest = max(longest, run)
            else:
                run = 0

        tones = []
        window = samples[: min(len(samples), rate)]
        for candidate in TONE_CANDIDATES:
            tones.append((candidate, goertzel(window, rate, candidate)))
        loudest = max(tones, key=lambda pair: pair[1]) if tones else None

        share = 100.0 * silent / len(samples)
        line = (
            f"  ch{channel}: rms {energy:8.1f}  peak {peak:6d}  "
            f"silence {share:5.1f}%  longest silent run {longest / rate:6.3f} s"
        )
        if loudest and loudest[1] > 0:
            line += f"  loudest of {','.join(str(int(t)) for t, _ in tones)} Hz: {int(loudest[0])}"
        print(line)


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    for path in sys.argv[1:]:
        summarise(path)


if __name__ == "__main__":
    main()
