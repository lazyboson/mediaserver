# mediaserver

The **Media Streaming Service (MSS)** — 3CLogic's centralized media plane.

Pulls per-call audio taps from RTPEngine (NG `subscribe`), fans them out to
RTT (gRPC), ASR/transcription (WebSocket, Twilio-Media-Streams-compatible),
recording, and voice-AI consumers — with no FreeSWITCH media bugs, dummy
legs, or forking conferences. Full design: [docs/architecture.md](docs/architecture.md).

**Language:** Rust (decision record in architecture doc §7.2).
**Ingest:** RTPEngine tap (`subscribe request/answer` / `unsubscribe`).
**Roadmap:** Phase 1 passive fan-out → Phase 2 recording → Phase 3 inline
legs + injection (voice AI) → Phase 4 mixing/conferencing.

## Workspace layout

| Crate | What it is |
| --- | --- |
| `crates/media-core` | **Sans-IO** media pipeline core: RTP parse/serialize, G.711, RFC 2833 DTMF, jitter buffer, frame/format types. No sockets, no clocks, no async — testable by pcap replay. |
| `crates/rtpengine-ng` | **Sans-IO** RTPEngine NG protocol client: bencode + `subscribe request/answer`, `unsubscribe`, `ping` datagram builders and reply parsing. |
| `crates/protocol` | Consumer wire dialects: Twilio Media Streams JSON (mediagateway-compatible) and `audio_fork send_text` control events (streamfsm-compatible). |
| `crates/mediaserverd` | The daemon: Tokio control plane + dedicated real-time media worker threads (the **two-world** architecture), session supervision / audio-flow watchdog. |
| `proto/` | gRPC contracts for `MediaControl` / `MediaStream` (wired with tonic in milestone 2). |
| `docs/` | Architecture proposal & decision records. |

## Governance & docs

- [CONSTITUTION.md](CONSTITUTION.md) — the project's binding principles;
  everything below derives from it.
- [docs/rust-guidelines.md](docs/rust-guidelines.md) — coding rules,
  including the **strictly-no-comments policy** (CI-enforced): intent lives
  in names, types, tests, and `docs/`, never in source comments.
- [docs/architecture.md](docs/architecture.md) — the accepted design:
  current production state, RTPEngine tap ingest, Rust decision record,
  scaling model, ecosystem map.
- [docs/roadmap.md](docs/roadmap.md) — the phased plan from first tap to
  a FreeSWITCH with no media, with exit criteria and live status.
- [CLAUDE.md](CLAUDE.md) — orientation for AI-assisted sessions and new
  engineers: stack context, binding rules, where work continues.

## Architecture rules (enforced in review)

1. **Two worlds.** Tokio owns everything latency-tolerant (session API, NG
   client, Redis/Kafka, consumer I/O). Dedicated OS threads own the packet
   path (recv → jitter → decode → fan-out → paced send). Bounded lock-free
   queues between them; the media world never blocks on the control world.
2. **Sans-IO cores.** Protocol/DSP logic takes packets and instants as
   parameters and returns values. If a module in `media-core` or
   `rtpengine-ng` grows a socket, a clock, or an `async fn`, it's wrong.
3. **No per-packet allocation** on the hot path after session setup.
4. **No panics on network input.** Malformed RTP/bencode/JSON is an error
   value. (mediagateway crashed on `a=ptime:0`; we return `None`.)
5. **Supervised sessions.** Every session has an audio-flow watchdog; a
   dead task must never be a silently dead call.
6. **Zero `unsafe`** outside dedicated FFI wrapper crates.

## Development

```sh
cargo test          # unit tests (all sans-IO cores are tested here)
cargo clippy --all-targets -- -D warnings
cargo fmt --check
cargo run -p mediaserverd   # starts control plane + media workers (skeleton)
```

Toolchain is pinned in `rust-toolchain.toml`.

## Milestones

- [x] **M1 — scaffold**: workspace, sans-IO cores (RTP, G.711, DTMF, jitter,
  NG bencode, consumer dialects), two-world daemon skeleton, watchdog.
- [ ] **M2 — Phase-0 spike**: NG client over real UDP against a lab
  rtpengine; tap one call, jitter-buffer it, dump both legs to WAV;
  `recvmmsg` ingest benchmark. *Exit: measured per-tap cost.*
- [ ] **M3 — fan-out hub**: per-session pub/sub, consumer bridges
  (WS Twilio dialect first), pause/resume/send_text parity with telservice.
- [ ] **M4 — control plane**: tonic `MediaControl`, Redis session registry
  with ownership leases, Kafka billing/lifecycle events, pilot behind a
  tenant feature flag.
