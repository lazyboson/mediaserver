"""Builds the fake microphone Chrome feeds the agent leg.

Chrome's --use-file-for-fake-audio-capture takes a 16-bit PCM wav and plays it
as the microphone, which is what makes a WebRTC leg machine-drivable: no human,
no headset, and a known signal on the wire so the tap can be judged instead of
listened to.

The pattern alternates sound and *digital silence* on purpose. Continuous audio
would hide the one Opus behaviour that most looks like a bug downstream: with
usedtx=1 the encoder stops sending during silence, so the RTP stream develops
gaps -- timestamps keep advancing while sequence numbers do not -- and a naive
receiver reports them as packet loss. lab/opus_call_driver.py cannot produce
that shape at all (it sends CBR, DTX off, one frame per packet, forever), so
until now nothing in this lab has ever tapped it.

Content is either a tone pair or a real speech wav (SPEECH_WAV), upsampled to
48 kHz because that is what a browser captures at. Speech is the better input
when the run ends in ear_intelligibility_probe.py; tones are the better input
when the run ends in a spectrum assertion.

Env: OUT, SECONDS, SOUND_SECONDS, SILENCE_SECONDS, SPEECH_WAV, TONE_HZ,
     TONE2_HZ, AMPLITUDE, RATE.
"""

import math
import os
import struct
import wave

OUT = os.environ.get("OUT", "out/agent_mic.wav")
SECONDS = float(os.environ.get("SECONDS", "60"))
SOUND_SECONDS = float(os.environ.get("SOUND_SECONDS", "3"))
SILENCE_SECONDS = float(os.environ.get("SILENCE_SECONDS", "2"))
SPEECH_WAV = os.environ.get("SPEECH_WAV", "")
TONE_HZ = float(os.environ.get("TONE_HZ", "440"))
TONE2_HZ = float(os.environ.get("TONE2_HZ", "1000"))
AMPLITUDE = float(os.environ.get("AMPLITUDE", "0.35"))
RATE = int(os.environ.get("RATE", "48000"))


def log(message):
    print(f"make-agent-audio: {message}", flush=True)


def read_speech(path):
    """Returns mono 16-bit samples at RATE, resampled by linear interpolation.

    Linear interpolation is not a good resampler. It does not need to be: this
    is the *source* signal for a codec test, and any aliasing it introduces is
    present identically in every run, which is what comparability needs.
    """
    with wave.open(path, "rb") as source:
        channels = source.getnchannels()
        width = source.getsampwidth()
        rate = source.getframerate()
        frames = source.readframes(source.getnframes())
    if width != 2:
        raise SystemExit(f"{path}: only 16-bit wav is supported, this is {width * 8}-bit")

    count = len(frames) // (2 * channels)
    samples = struct.unpack(f"<{count * channels}h", frames[: count * channels * 2])
    if channels > 1:
        samples = [
            sum(samples[i * channels:(i + 1) * channels]) // channels for i in range(count)
        ]
    else:
        samples = list(samples)

    if rate == RATE:
        return samples

    ratio = RATE / rate
    out_count = int(len(samples) * ratio)
    resampled = []
    for i in range(out_count):
        position = i / ratio
        left = int(position)
        right = min(left + 1, len(samples) - 1)
        weight = position - left
        resampled.append(int(samples[left] * (1 - weight) + samples[right] * weight))
    log(f"{path}: {len(samples)} samples at {rate} Hz -> {out_count} at {RATE} Hz")
    return resampled


def tone(count, start_phase):
    """Two tones summed, phase-continuous across calls so joins do not click."""
    peak = int(32767 * AMPLITUDE / 2)
    step1 = 2 * math.pi * TONE_HZ / RATE
    step2 = 2 * math.pi * TONE2_HZ / RATE
    return (
        [
            int(peak * math.sin(step1 * (start_phase + i)))
            + int(peak * math.sin(step2 * (start_phase + i)))
            for i in range(count)
        ],
        start_phase + count,
    )


def main():
    total = int(SECONDS * RATE)
    sound = int(SOUND_SECONDS * RATE)
    silence = int(SILENCE_SECONDS * RATE)

    speech = read_speech(SPEECH_WAV) if SPEECH_WAV else None
    samples = []
    phase = 0
    speech_at = 0
    cycles = 0

    while len(samples) < total:
        if speech:
            chunk = []
            while len(chunk) < sound:
                take = min(sound - len(chunk), len(speech) - speech_at)
                chunk.extend(speech[speech_at:speech_at + take])
                speech_at = (speech_at + take) % len(speech)
            samples.extend(chunk)
        else:
            chunk, phase = tone(sound, phase)
            samples.extend(chunk)
        samples.extend([0] * silence)
        cycles += 1

    samples = samples[:total]

    directory = os.path.dirname(OUT)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with wave.open(OUT, "wb") as out:
        out.setnchannels(1)
        out.setsampwidth(2)
        out.setframerate(RATE)
        out.writeframes(struct.pack(f"<{len(samples)}h", *samples))

    source = f"speech from {SPEECH_WAV}" if speech else f"tones {TONE_HZ}+{TONE2_HZ} Hz"
    log(
        f"wrote {OUT}: {SECONDS:.0f}s at {RATE} Hz, {cycles} cycles of "
        f"{SOUND_SECONDS:.1f}s {source} + {SILENCE_SECONDS:.1f}s digital silence"
    )
    log("the silence is the point: it is where DTX stops sending and the tap sees a gap")


if __name__ == "__main__":
    main()
