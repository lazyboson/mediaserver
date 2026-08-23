# CLAUDE.md — context for AI-assisted sessions

Read this first. It exists so any future session (or new engineer) can
pick up this project without re-deriving its context.

## What this project is

**mediaserver** (MSS, Media Streaming Service) is an open-source
centralized media plane for SIP infrastructures built on rtpengine. Its
mission, in one sentence: **remove every media workload from FreeSWITCH,
phase by phase, until FreeSWITCH does only IVR and call control — or
nothing at all.**

It taps per-call audio directly from RTPEngine (NG protocol `subscribe
request/answer` — the SIPREC mechanism) and fans it out to consumers:
real-time transcription (RTT) over gRPC, ASR over WebSocket, recording to
S3, and voice-AI agents. Later phases add inline media (bot speech
injection) and finally conference mixing.

## The reference deployment it replaces parts of

The project grew out of one production contact-center stack; that stack is
the reference deployment, and its internal component names appear
throughout the docs as concrete stand-ins for generic roles (full glossary
in the [README](README.md#provenance-and-glossary)):

- **Carrier → OpenSIPS → RTPEngine (kernel module) → FreeSWITCH** is the
  customer leg; agents connect via a registrar/OpenSIPS gateway + RTPEngine
  into the same FreeSWITCH.
- **the legacy controller** (Go) is the legacy telephony controller: it drives FreeSWITCH
  over ESL — IVR, the `<Stream>` verb state machine (`the legacy stream fsm`),
  conference-based monitor/whisper, recording. Its `the legacy verb API` gRPC API is
  the control surface our `TelCompat` façade mirrors byte-for-byte
  (StartStream→CreateSession/Attach etc.).
- **the legacy media gateway** (Go) is the legacy gateway this service supersedes: a
  per-call RTP↔WebSocket pump behind an OpenSIPS B2B dummy leg +
  FreeSWITCH conference. Its audit produced our requirements delta
  (architecture doc §7.1) — every design rule here traces to a defect
  found there (no jitter buffer, hardcoded 20 ms pacing, blocking I/O on
  the pacer, silent queue drops, per-pod pinned state).
- Today that FreeSWITCH carries the fork load we are removing:
  `uuid_audio_fork` (a fork of mod_audio_fork with extra positional args),
  `uuid_google_transcribe2`, `record_session` media bugs, and one dummy
  leg + one conference per voice-AI interaction.

None of the core depends on that platform: any deployment whose media
anchors in rtpengine can run MSS, and the compatibility surfaces
(Twilio-Media-Streams WS dialect, mod_audio_fork event names, the the legacy verb API
façade) are optional adapters.

## Binding documents (in order of authority)

1. [CONSTITUTION.md](CONSTITUTION.md) — twelve articles; reviews are
   judged against it.
2. [docs/rust-guidelines.md](docs/rust-guidelines.md) — coding rules.
   **The one that surprises people: strictly no comments in Rust sources**
   (no `//`, `///`, `//!`, `/* */`; CI fails on them). Intent goes in
   names, types, tests, docs/, and commit messages.
3. [docs/architecture.md](docs/architecture.md) — the accepted design:
   RTPEngine tap ingest, Rust decision record (§7.2), scaling model,
   ecosystem map (Appendix A).
4. [docs/roadmap.md](docs/roadmap.md) — the phased execution plan with
   exit criteria and current status. **Update it when a milestone lands.**
   [docs/tasks.md](docs/tasks.md) is the ordered next-up list, open defects
   and what is blocked on other people — **read it first to know what to
   pick up, and update it in the same PR that changes an item's state.**
5. [docs/implementation-notes.md](docs/implementation-notes.md) — per-module
   scaffold status, known limitations, and pending work. Since source files
   carry no comments, this file is where that context lives — **update it
   in the same PR that changes a module.**
6. [docs/testing.md](docs/testing.md) — the three test altitudes (replay,
   lab, benchmark), the impairment matrix, and the M2 benchmark method.
   [docs/lab.md](docs/lab.md) documents the lab that exists today.
7. [docs/session-playbook.md](docs/session-playbook.md) — **read before
   changing code.** Working rules distilled from real incidents in the
   sessions that built Phases 0–1: probe vendors before trusting docs,
   machine-verify before human tests, read artifacts before touching code,
   verify scripted edits landed, never chain `&&` through the gate.

## Architecture in three sentences

Two worlds: a Tokio control plane (session API, NG client, Redis/Kafka,
consumer I/O) and dedicated real-time OS threads for the packet path
(recv → jitter buffer → decode → resample → fan-out → paced send),
joined only by bounded lock-free queues — the media world never waits on
the control world. All protocol/DSP logic is sans-IO (pure state machines,
time as a parameter) so every media bug is reproducible by packet replay
in a unit test. Taps are pull-initiated (MSS asks rtpengine to send it a
copy), which makes passive sessions re-subscribable after a pod loss and
placement a scheduling decision instead of an SDP-routing problem.

## Key invariants a session must never break

- Taps are ears; inline legs are mouths. A subscription cannot inject
  audio; interactive voice-AI needs the Phase-3 inline leg.
- Wire compatibility is frozen: the Twilio Media Streams dialect
  (`crates/protocol/src/twilio.rs`), the the legacy stream fsm send_text events
  (`fork_events.rs`), and the recording identity scheme
  `${accountID}/${recordingID}.${format}` are contracts (Constitution,
  Article VII). Their serialization tests are the spec.
- No comments in `.rs` files. No `unsafe` outside dedicated FFI wrapper
  crates — codec/DSP math is adopted from proven C libraries, never
  reimplemented (Article XI). No panics on network input. No allocation
  per packet. No async in `media-core`/`rtpengine-ng`.

## Working on this repo

Building needs **cmake, make and g++** on the host — `opus-ffi` compiles a
vendored libopus. `apt install cmake make g++` on Debian/Ubuntu.

```sh
cargo test --workspace
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
grep -rn '//' crates --include='*.rs'   # must output nothing
```

CI mirrors exactly these plus `cargo deny check all`, which covers the
RustSec advisory database as well as licenses, bans and sources. The release
pipeline triggers on `v*` tags.

## Where work continues

Read [docs/tasks.md](docs/tasks.md) — it holds the milestone state and the
ordered next-up list with a definition of done for each item.

Short version as of 2026-08-23: M1–M3 are done, **M4 (control plane) is code
complete (~95%)** and **M5 (recording to S3) is code complete** — the recorder
is a hub consumer that segments on pause, keeps the frozen
`${accountID}/${recordingID}.${format}` identity, and uploads through
`object_store` (verified against a real MinIO **and on a live tapped call**,
2026-08-22; FS byte-parity is still owed). On M4: `MediaControl`, `TelCompat`
and the gRPC `MediaStream` data plane are served on one port — the last one
now proved on a live call too (bearer auth via
`MSS_AUTH_TOKEN`), taps and consumers are driven by the API, events publish
to Kafka `mss.events`, the Redis registry re-subscribes on pod loss, leg
identity is solved by SSRC correlation, and Prometheus metrics are exported
on `MSS_METRICS_LISTEN` with alert rules in `deploy/`. What still gates the
tenant pilot: reviewing and merging the the legacy controller event translator (written, on
the legacy controller branch `feature/legacy-translator`) and the barge-in cut-through
measurement. The **pod-kill re-subscribe drill is done** (2026-08-22): a real
`kill -9` on the owning pod mid-call cost the consumer a **14.41 s** audio gap
before another pod adopted the session and re-subscribed — and showed that the
dead pod's subscription is never torn down (D14). **Opus ingest landed
2026-08-23** — libopus via the `opus-ffi` crate, decoding at 8/12/16/24/48 kHz
with libopus's own concealment, proven on a live call by asking rtpengine to
`transcode: [opus]` (`MSS_TAP_FORMAT=opus`). Building now needs cmake, make and
g++. **Kernel-module readiness landed 2026-08-23** (item 23): NG `statistics`
and a kernel-forwarding verdict in `rtpengine-ng`, a per-node capability log in
mediaserverd, `lab/kernel_probe.sh` (machine-verified on the lab's no-module
path), and the on-metal checklist in architecture §8.1 — plus the verdict that
rtpengine's *transcoder*, not its relay, was under-producing Opus: a native
Opus call taps at **50.0 pkt/s** against 3.8 transcoded. Two Phase-0 items
remain blocked on other people: the production rtpengine version check — which
**cannot** be asked over NG, since rtpengine has no NG `version` command — and
the rtpengine-side per-tap cost.
