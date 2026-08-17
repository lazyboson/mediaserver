# CLAUDE.md — context for AI-assisted sessions

Read this first. It exists so any future session (or new engineer) can
pick up this project without re-deriving its context.

## What this project is

**mediaserver** is 3CLogic's centralized media plane (internally: MSS,
Media Streaming Service). Its mission, in one sentence: **remove every
media workload from FreeSWITCH, phase by phase, until FreeSWITCH does
only IVR and call control — or nothing at all.**

It taps per-call audio directly from RTPEngine (NG protocol `subscribe
request/answer` — the SIPREC mechanism) and fans it out to consumers:
real-time transcription (RTT) over gRPC, ASR over WebSocket, recording to
S3, and voice-AI agents. Later phases add inline media (bot speech
injection) and finally conference mixing.

## The production stack this replaces parts of

- **Carrier → OpenSIPS → RTPEngine (kernel module) → FreeSWITCH** is the
  customer leg; agents connect via a registrar/OpenSIPS gateway + RTPEngine
  into the same FreeSWITCH.
- **cigol** (Go, `Telephony/cigol` repo) drives FreeSWITCH over ESL: IVR,
  the `<Stream>` verb state machine (`streamfsm`), conference-based
  monitor/whisper, recording. Its `telservice` gRPC API is the control
  surface our `MediaControl` API mirrors (StartStream→StartTap etc.).
- **mediagateway** (Go, separate repo) is the predecessor this service
  supersedes: a per-call RTP↔WebSocket pump behind an OpenSIPS B2B dummy
  leg + FreeSWITCH conference. Its audit produced our requirements delta
  (architecture doc §7.1) — every design rule here traces to a defect
  found there (no jitter buffer, hardcoded 20 ms pacing, blocking I/O on
  the pacer, silent queue drops, per-pod pinned state).
- Today FreeSWITCH carries the fork load we are removing:
  `uuid_audio_fork` (a 3CLogic-forked mod_audio_fork with extra positional
  args), `uuid_google_transcribe2`, `record_session` media bugs, and one
  dummy leg + one conference per voice-AI interaction.

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
  (`crates/protocol/src/twilio.rs`), the streamfsm send_text events
  (`fork_events.rs`), and the recording identity scheme
  `${accountID}/${recordingID}.${format}` are contracts (Constitution,
  Article VII). Their serialization tests are the spec.
- No comments in `.rs` files. No `unsafe` outside dedicated FFI wrapper
  crates — codec/DSP math is adopted from proven C libraries, never
  reimplemented (Article XI). No panics on network input. No allocation
  per packet. No async in `media-core`/`rtpengine-ng`.

## Working on this repo

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

Short version as of 2026-08-17: M1–M3 are done and **M4 (control plane) is
about two thirds there** — `MediaControl` is served over gRPC and wired into
the daemon, taps and consumers are driven by the API, events publish to Kafka
`mss.events`, and leg identity is solved by SSRC correlation. Next up: the
event translator in cigol (which gates flipping any tenant), the `TelCompat`
façade, the Redis registry with re-subscribe on pod loss, and exported
metrics. Three Phase-0 items remain blocked on other people: the production
rtpengine version check, the rtpengine-side per-tap cost, and the
OpenSIPS→Redis call→node discovery design.
