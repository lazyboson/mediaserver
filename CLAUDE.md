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
- **cigol** (Go) is the legacy telephony controller: it drives FreeSWITCH
  over ESL — IVR, the `<Stream>` verb state machine (`streamfsm`),
  conference-based monitor/whisper, recording. Its `telservice` gRPC API is
  the control surface our `TelCompat` façade mirrors byte-for-byte
  (StartStream→CreateSession/Attach etc.).
- **mediagateway** (Go) is the legacy gateway this service supersedes: a
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
(Twilio-Media-Streams WS dialect, mod_audio_fork event names, the telsvc
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
  (`crates/protocol/src/twilio.rs`), the streamfsm send_text events
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

Short version as of 2026-08-24: **M1–M5 are done and M6 is code complete** —
FreeSWITCH is out of the media path for every workload this repository set out
to take from it. Phase 1 (passive fan-out) closed out: `MediaControl`,
`TelCompat` and the gRPC `MediaStream` data plane are served on one port with
bearer auth (`MSS_AUTH_TOKEN`), taps and consumers are driven by the API, events
publish to Kafka `mss.events` with an at-least-once backlog, the Redis registry
re-subscribes on pod loss (a real `kill -9` mid-call cost **14.41 s** of
consumer audio, and the orphaned subscription it exposed — D14 — is fixed), leg
identity is solved by SSRC correlation, and metrics are exported on
`MSS_METRICS_LISTEN` with alert rules in `deploy/`. The barge-in cut-through is
now **measured for every hop MSS owns**: a real consumer `SpeechReport` to an
acked `StopPlayback` at **p50 3.5 ms** over the bus, and an inline `Clear` to the
first silent packet at a real peer's ear at **p50 12.2 ms** — one ptime, as
designed. Phase 2 (recording to S3) keeps the frozen
`${accountID}/${recordingID}.${format}` identity, segments on pause, spills
closed segments to disk so a pod death costs the spill interval rather than the
call, and time-aligns recording-group members on the group's open instant; FS
parity was measured against a real FreeSWITCH recording (container, layout and
rms exact; a re-aligned window agreeing 1.0000 at mean diff 0.6/32768) and that
measurement retired byte-parity-at-a-fixed-offset as an achievable bar.
**Phase 3 (inline legs) and Phase 4 (conferences) are code complete and
lab-verified 2026-08-24**: `CreateSession{kind=INLINE, sdp_offer}` answers an
offer and speaks on real sockets through a sans-IO playout pacer, an INJECT
attachment streams into it full duplex with `Mark`/`Clear` as the barge seam,
and legs sharing a `group` share one mix — with monitor, whisper, barge,
member mute/deaf/hold and room prompts all expressed as **cells of one
`MixMatrix`** named by metadata verbs on the existing nouns, not as new RPCs. A
conference records both ways at once (one mono object for the room, one per
participant). The three-peer conference drill is green on real sockets: twenty
tone-per-phase assertions at a ≥30:1 margin. **Opus ingest** landed via the
`opus-ffi` crate (so the build needs cmake, make and g++), and
kernel-module readiness ships with `lab/kernel_probe.sh` plus the on-metal
checklist in architecture §8.1 — including the verdict that rtpengine's
*transcoder*, not its relay, under-produces Opus (50.0 pkt/s native against
3.8 transcoded).

**The media plane answers SIP itself (2026-08-30, [item 59](docs/tasks.md)).**
`crates/sip-uas` grew from a parser into a **UAS** — RFC 3261 §17 transactions
with RFC 6026's `Accepted` state, RFC 3261 §12 dialogs, RFC 4028 session
timers — written rather than adopted, for the reasons architecture §7 records.
`MSS_SIP_LISTEN` puts it on a socket, calling the same `SessionController` the
gRPC service does, so it is a second entrance onto the existing nouns and never
a parallel implementation. Answer-only is unchanged: no registrar, no routing,
no forking, no UAC. `lab/sip_shim.py` is what this replaces, and it keeps its
job until the lab drills are pointed at the new port.

Still true: **no datagram has yet come from a real FreeSWITCH, OpenSIPS or
softphone** — the front door's tests construct their own, over real sockets
against the real controller — and no measurement in this repository has been
judged by a human ear. A re-INVITE whose offer *changes* is refused 488, because
`session-core` has no renegotiation path; that is P3-2's media half and the only
part of re-INVITE still open. What remains is otherwise deployment work rather
than engineering: see **Integration handoffs (deployment-gated)** in
[docs/tasks.md](docs/tasks.md) for the eight, and the open defects **D6** (`play
media` from-tag semantics unmeasured) and **D8** (no real pod placement, which
is what an inline leg's room affinity needs) for what to watch in a pilot —
D11, D16, D17, D20, D21 and D22 all closed in items 47–57. The Phase 3/4 work is
on branch **`feat/m6-autonomous`** and the SIP work on **`feat/sip-uas`**, both
pending review.
