# mediaserver

**MSS (Media Streaming Service)** — an open-source, Rust-built centralized
media plane for SIP infrastructures whose media anchors in
[rtpengine](https://github.com/sipwise/rtpengine).

If your calls flow through rtpengine (OpenSIPS/Kamailio in front, FreeSWITCH
or anything else behind), MSS can **tap any call's audio without touching the
call**: it asks rtpengine for a copy of each participant's media (NG protocol
`subscribe request/answer` — the SIPREC mechanism) and fans it out to as many
consumers as you attach — real-time transcription, ASR, recording, voice-AI
bridges. No media bugs, no dummy legs, no conference tricks, and taps are
re-creatable, so a lost pod re-subscribes instead of losing the call.

The longer mission (see [docs/roadmap.md](docs/roadmap.md)): remove every
media workload from FreeSWITCH phase by phase — passive fan-out, then
recording, then inline interactive media (voice-AI speech in), then mixing —
until the softswitch does only call control, or nothing at all.

## What works today

- **Tap ingest** from rtpengine ≥ mr14 (`subscribe`/`unsubscribe`), with
  per-leg jitter buffering, G.711 decode (µ-law and A-law, whatever
  arrives), RFC 4733 DTMF, and speaker attribution by SSRC correlation.
- **Fan-out hub**: N consumers per call, attach/detach mid-call, bounded
  per-consumer queues with counted drop-oldest — a slow consumer only ever
  hurts itself.
- **Consumer transports**: a WebSocket adapter speaking the Twilio Media
  Streams dialect (drop-in for tooling that already consumes it), and a
  native gRPC bidirectional stream (`proto/mediastream.proto`) with binary
  frames, capability-checked audio injection, and mark/clear barge-in
  semantics.
- **Control plane**: the `MediaControl` gRPC API (`proto/mediacontrol.proto`)
  over sessions / attachments / playbacks, typed events onto Kafka
  (`mss.events`, gapless per-session sequence), a Redis session registry
  with ownership leases and automatic re-subscribe after pod loss, optional
  bearer-token auth, and Prometheus metrics with shipped alert rules
  ([deploy/prometheus-alerts.yaml](deploy/prometheus-alerts.yaml)).
- **Audio injection into tapped calls** via rtpengine `play media`
  (utterance-shaped; streaming TTS arrives with Phase 3 inline legs).
- **A legacy façade** (`TelCompat`): serves a FreeSWITCH-controller-style
  `the legacy verb API` verb API (`StartStream`/`StartRecording`/…) byte-compatibly on
  the same port, so an existing controller can be pointed at MSS by config
  flag and rolled back the same way. Optional — skip it if you have no such
  controller.

## Quick start

```sh
cargo run -p mediaserverd            # needs only Rust; no protoc, no cmake
```

Environment (all optional except the listen address):

| Variable | What it does |
| --- | --- |
| `MSS_CONTROL_LISTEN` | `ip:port` for the gRPC control+data plane |
| `MSS_RTPENGINE_NODE` | default rtpengine NG address (`ip:port`) |
| `MSS_TAP_LOCAL_IP` | address rtpengine sends tap media to (must be routable from rtpengine) |
| `MSS_KAFKA_BROKERS` | comma list; unset = events stay in-process |
| `MSS_REDIS_URL` | session registry; unset = sessions die with the pod |
| `MSS_METRICS_LISTEN` | `ip:port` for Prometheus `/metrics` |
| `MSS_AUTH_TOKEN` | shared bearer secret; unset = open (lab mode) |
| `MSS_POD_NAME` | this pod's identity in the registry |

Then drive it:

```sh
cargo run -p control-api --example mss_ctl -- http://127.0.0.1:50551 \
    create req-1 <sip-call-id> <caller-from-tag> <rtpengine-ip:port>
cargo run -p control-api --example mss_ctl -- http://127.0.0.1:50551 \
    attach req-1 ws://your-consumer/ws
```

A full docker-compose lab (OpenSIPS + FreeSWITCH + rtpengine + Redpanda +
synthetic callers and mock consumers) lives in [`lab/`](docs/lab.md).

## Workspace layout

| Crate | What it is |
| --- | --- |
| `crates/media-core` | **Sans-IO** media pipeline core: RTP parse/serialize, G.711, RFC 4733 DTMF, jitter buffer, frame/format types, packet-replay harness. No sockets, no clocks, no async. |
| `crates/rtpengine-ng` | **Sans-IO** rtpengine NG protocol client: bencode, `subscribe request/answer`, `unsubscribe`, `play media`/`stop media`, `query`, subscription SDP. |
| `crates/protocol` | Frozen consumer wire dialects: Twilio Media Streams JSON and `audio_fork` send_text control events. The serialization tests are the spec. |
| `crates/session-core` | **Sans-IO** control-plane state machine: sessions, attachments, playbacks, events — capability authorization, one authoritative attachment per session, idempotent retries. |
| `crates/control-api` | The network surface: `MediaControl` + `MediaStream` + `TelCompat` gRPC services over `session-core`. Pure-Rust protobuf build (no `protoc`). |
| `crates/mediaserverd` | The daemon: Tokio control plane + dedicated real-time media threads (the **two-world** architecture), fan-out hub, consumer bridges, Kafka event pump, Redis registry keeper, metrics. |

## Architecture rules (enforced in review)

1. **Two worlds.** Tokio owns everything latency-tolerant. Dedicated OS
   threads own the packet path (recv → jitter → decode → fan-out → paced
   send). Bounded lock-free queues between them; the media world never
   blocks on the control world.
2. **Sans-IO cores.** Protocol/DSP logic takes packets and instants as
   parameters and returns values, so every media bug is reproducible by
   packet replay in a unit test.
3. **No per-packet allocation** on the hot path after session setup.
4. **No panics on network input.** Malformed RTP/bencode/JSON is an error
   value.
5. **Supervised sessions.** Every tap leg has an audio-flow watchdog; a dead
   task must never be a silently dead call.
6. **Zero `unsafe`** outside dedicated FFI wrapper crates; codec/DSP math is
   adopted from proven libraries, never reimplemented.

The binding version of these rules is [CONSTITUTION.md](CONSTITUTION.md);
coding style (including the **strictly-no-comments policy** — intent lives in
names, types, tests and `docs/`, enforced by CI) is
[docs/rust-guidelines.md](docs/rust-guidelines.md).

## Documentation

- [docs/architecture.md](docs/architecture.md) — the accepted design and its
  decision records.
- [docs/roadmap.md](docs/roadmap.md) — the phased plan with exit criteria
  and live status; [docs/tasks.md](docs/tasks.md) is the ordered next-up
  list with a definition of done per item.
- [docs/implementation-notes.md](docs/implementation-notes.md) — per-module
  status and known limitations (sources carry no comments; this is where
  that context lives).
- [docs/testing.md](docs/testing.md) and [docs/lab.md](docs/lab.md) — the
  three test altitudes and the lab that exists today.
- [CLAUDE.md](CLAUDE.md) — orientation for AI-assisted sessions and new
  engineers.

## Provenance and glossary

MSS grew out of one production contact-center platform, and the design docs
keep that history verbatim — it is where the requirements came from, and
every design rule traces to a measured defect or probe. Internal component
names you will meet in `docs/` map to generic roles:

| Name in docs | Generic role |
| --- | --- |
| `the legacy controller` | the legacy telephony controller: drives FreeSWITCH over ESL, exposes the `the legacy verb API` gRPC API that `TelCompat` mirrors |
| `the legacy verb API` / `the legacy verb API` | that controller's gRPC API (`proto/telcompat.proto` copies its shapes) |
| `the legacy stream fsm` / `the application server` | the state machine and application server consuming the legacy fork events |
| `the legacy media gateway` | the legacy per-call RTP↔WebSocket gateway MSS supersedes; source of the frozen Twilio-dialect bytes |
| `the voice-AI orchestrator` | the voice-AI orchestrator that will switch from the legacy gateway to MSS in Phase 3 |
| `uuid_audio_fork` | a fork of mod_audio_fork whose event vocabulary (`mod_audio_fork::*`) the event contract preserves |

Nothing in the core depends on that platform. The compatibility surfaces
(Twilio-dialect WS, `mod_audio_fork` event names, the `the legacy verb API` façade) are
optional adapters: use them if you are migrating off a similar stack, ignore
them if you are not.

## Development

```sh
cargo test --workspace
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
grep -rn '//' crates --include='*.rs'   # must output nothing
cargo deny check all
```

Toolchain is pinned in `rust-toolchain.toml`. CI runs exactly the gate above.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at
your option. Contributions are accepted under the same terms.
