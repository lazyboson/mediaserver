# The mediaserver Constitution

This document is the highest authority in this repository. Code, reviews,
and roadmap decisions are judged against it. It changes only by deliberate
amendment (see Article X), never by drift.

## Purpose

mediaserver is the upstream platform's centralized media plane: it taps per-call audio
from RTPEngine and fans it out to RTT, ASR, recording, and voice-AI
consumers, and — in later phases — carries inline interactive media and
mixing, removing all media workloads from FreeSWITCH. It exists because
media must scale independently of call control.

## Article I — The media path is sacred

Audio frames meet their deadlines before everything else. No feature,
metric, log line, or refactor may introduce blocking I/O, unbounded work,
or shared-lock contention on the packet path. Anything that can be slow
happens somewhere else.

## Article II — Two worlds, one boundary

The control world (async, Tokio) manages sessions, signaling, registries,
and consumer connections. The media world (dedicated OS threads) owns
sockets, jitter buffers, codecs, and pacing. They communicate only through
bounded, lock-free queues. The media world never waits on the control
world. Code that blurs this boundary is rejected regardless of how
convenient it is.

## Article III — Sans-IO cores

Protocol and DSP logic is written as pure state machines: packets in,
values out, time as an explicit parameter. No sockets, no clocks, no async
in core crates. Every media bug must be reproducible by replaying captured
packets in a unit test, because in production nobody can hear the audio —
we can only replay it.

## Article IV — Malformed input is a value, not a crash

Network input is hostile. Every parser returns `Result` or `Option`.
A packet that cannot be parsed is counted, logged at debug, and dropped.
`panic!`, `unwrap`, and `expect` on data that crossed a network or process
boundary are forbidden. One bad packet must never end one call; one bad
session must never end a process.

## Article V — No allocation on the hot path

After session setup, the per-packet path performs zero heap allocations.
Buffers are pooled or caller-provided. A PR that adds an allocation per
packet must prove it cannot be avoided, and will usually lose.

## Article VI — Every session is supervised

A dead task must never mean a silently dead call. Every session carries an
audio-flow watchdog; every spawned task is joined or monitored; stalls are
events, not mysteries. Recovery is designed in: passive taps re-subscribe,
they do not linger broken.

## Article VII — Wire compatibility is a contract

Consumers speaking the Twilio Media Streams dialect, the audio_fork
send_text events, and the recording identity scheme
(`${accountID}/${recordingID}.${format}`) migrated to this service on the
promise that nothing changes for them. Field names, shapes, and semantics
of compatibility surfaces are frozen; changes require a new versioned
surface, never mutation of the old one.

## Article VIII — Measured, then merged

Performance claims require benchmarks; capacity claims require load tests.
The Phase-0 numbers (per-tap cost, per-pod session ceiling) are re-measured
whenever the pipeline changes. Silent capability caps (drops, truncations,
sampling) must be surfaced as first-class metrics with alerts.

## Article IX — Scope moves in phases

Phase 1: passive fan-out. Phase 2: recording. Phase 3: inline legs and
injection. Phase 4: mixing. A phase begins only when the previous one is
boringly stable in production. Features from a future phase do not leak
into the current one "while we're in there."

## Article X — Amendments

Any article may be amended by a PR that changes this file, approved by the
project owners, with the reasoning recorded in the PR description. An
amendment that weakens Articles I–VI requires a working demonstration that
the weakened rule cannot cause audible harm.

## Article XI — Code speaks for itself

Source files contain no comments of any kind. Intent lives in names, types,
tests, and `docs/`. If code needs a comment to be understood, the code is
rewritten until it does not. The full policy is
[docs/rust-guidelines.md](docs/rust-guidelines.md), and CI enforces it.
