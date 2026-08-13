# Implementation notes — scaffold status per module

Source files carry no comments (rust-guidelines §1), so the state of each
module, its known limitations, and its pending work live here. Update this
file in the same PR that changes the module. Items marked **M2/M3/M4**
map to the milestones in [roadmap.md](roadmap.md) and README.

## crates/media-core

### rtp.rs — complete for current needs
- Parses version, marker, PT, seq, timestamp, SSRC; skips CSRCs and
  extension headers; honors padding; rejects malformed input as errors.
- `serialize` emits only the 12-byte header form (no CSRC/extension) —
  intentional, that is all the MSS sends. `RtpError::Unsupported` is
  reserved for when a caller ever requests more.

### g711.rs — working, needs reference verification before GA
- Segment-based µ-law/A-law, verified by full 16-bit-sweep roundtrip,
  monotonicity, and code-word-stability tests.
- **Before GA:** diff the full sweep against a reference implementation
  (spandsp tables or ITU-T G.711 test vectors) and freeze the 256-entry
  decode tables in-tree. µ-law has two zero codes (0xFF canonical, 0x7F);
  our encoder canonicalizes to 0xFF.

### jitter.rs — scaffold; the M2/M3 hardening list
Current: fixed target depth, sequence-only ordering, 64-slot ring,
G.711-sized slots (480 B), duplicate/late/loss/reset accounting,
wraparound-safe.

Playout semantics as decided (each pinned by a test named after it):
- `depth()` is occupancy of the `play_seq..=max_seq` span and returns **0**
  when the buffer is drained or has never started — not 1. The earlier
  `+1`-always form made a drained buffer at `target_depth == 1` fail the
  `depth < target` gate, so every `pop()` walked into an empty slot and
  counted a phantom `Lost` forever on an idle stream
  (`drained_stream_waits_instead_of_counting_loss`).
- A drained buffer returns `Waiting`. `Lost` means an observed gap *below*
  `max_seq` — a packet that can no longer arrive usefully — never "the
  network has not delivered the next packet yet".
- Priming is real, not nominal: `pop()` holds `Waiting` until depth first
  reaches `target_depth`, then playout begins and the pre-existing reorder
  grace (wait while `depth < target_depth` and the head slot is empty)
  applies from then on (`primes_before_playout`).
- A `Reset` (discontinuity ≥ `CAPACITY`) clears the primed flag, so the
  cushion is rebuilt for what is effectively a new stream
  (`reset_reprimes_before_playout_resumes`). A transient drain does **not**
  re-prime: re-priming after an underrun on a continuous sequence would
  ratchet playout delay upward for the rest of a live call, which is worse
  than the single gap it would paper over.
- **M2:** adaptive target depth driven by observed inter-arrival jitter.
- **M2:** timestamp-aware gap handling so silence-suppression gaps
  (marker bit, big TS jump, small seq jump) are not misread as loss.
- **M3:** PLC hook on `PopOutcome::Lost` (G.711 Appendix I style repeat/
  attenuate; Opus PLC comes free with the decoder later).
- **M3:** slot sizing revisit when Opus lands (payloads up to ~1275 B).

### dtmf.rs — complete for RFC 4733 digit reporting
- Reports once per press on end-bit, deduped by (digit, RTP timestamp);
  events ≥16 (flash-hook etc.) deliberately ignored.

### frame.rs — complete
- `samples_per_packet` returns `None` on zero ptime/rate rather than
  dividing by zero (the legacy media gateway crashed on `a=ptime:0`).

## crates/rtpengine-ng — sans-IO layer complete; transport is M2
- bencode encode/decode with malformed-input tests; command builders for
  `ping`, `subscribe request` (from-tags, mix flag, codec accept list,
  set-label), `subscribe answer`, `unsubscribe`; reply parsing with
  cookie extraction and `result=error` surfacing.
- **M2:** async UDP transport in mediaserverd (cookie correlation map,
  timeout, retry, node health), SDP answer construction for the
  subscription leg, and re-subscribe orchestration on pod recovery.
- Unlike the legacy media gateway's fire-and-forget MI client, every request must
  await its correlated reply.

## crates/protocol — frozen wire contracts
- `twilio.rs` and `fork_events.rs` serialization tests are the contract
  (Constitution VII). Do not change shapes; add new versioned surfaces.
- Every `Outbound` variant now has a byte-exact expected-JSON test
  (`start/media/dtmf/mark/stop_matches_the legacy media gateway_bytes`), matching the
  `fork_events` style. Previously only `Media` was covered, and only by
  field spot-checks — a frozen contract with four untested shapes.
- **`mark` carries no `sequenceNumber`; `start`/`media`/`dtmf`/`stop` all
  do.** Confirmed against the the legacy media gateway source: its mark echo is
  `{"event":"mark","streamSid":...,"mark":{"name":...}}`. The asymmetry is
  the contract, not an oversight, and
  `mark_omits_sequence_number_while_start_media_dtmf_stop_carry_it` exists
  to fail on anyone "fixing" it.
- `customParameters` is omitted entirely when empty and emitted when
  present; byte-exact tests cover both. Multi-entry maps have no
  deterministic order, so byte-exact tests use at most one entry.
- **M3:** the WS consumer bridge must also replicate the the upstream platform-forked
  mod_audio_fork dialect. **Open item:** the fork's exact wire format is
  specified only in the fork's C source (not in this repo, not in the legacy controller);
  pull it and write the byte-exact tests before the first ASR consumer
  migrates (architecture.md risk #4).

## crates/mediaserverd

### media_rt.rs — thread/tick skeleton real; session work is M2/M3
The worker loop currently only ticks and counts. Per-iteration plan:
1. drain control-plane commands (add/remove session, pause/resume),
2. `recvmmsg` on owned sockets → `RtpPacket::parse` → per-session
   `JitterBuffer::push` / `DtmfDetector::push`,
3. release frames whose wall-clock deadline (`t0 + n·ptime`) has passed,
   encode per consumer format, enqueue to consumer bridges
   (drop-oldest on full, counted),
4. publish per-session heartbeats for the audio-flow watchdog.
Deadline anchoring already implemented: late wakeups shorten the next
sleep; a badly-behind loop re-anchors instead of bursting.

### supervisor.rs — watchdog logic done, unwired
`AudioFlowWatchdog` (touch/check, sticky stall origin) has tests but no
caller. **M3:** wire into the worker loop per session; stall events feed
metrics + tap re-subscribe.

### main.rs — boots both worlds; control plane is M4
**M4:** tonic `MediaControl` (proto/mediastream.proto), NG client task,
Redis session registry with ownership leases, Kafka producers (reuse
`LEGACY_MEDIA_GATEWAY_BILLING_TOPIC` / `KAFKA_VOICE_AI_AGENT_TOPIC` schemas),
health endpoint, graceful drain.

## Cross-cutting decisions already made (do not relitigate casually)
- Codec interchange is L16 internally; one decode per ingest stream,
  N encodes shared per consumer format.
- Consumers get raw bytes on gRPC (no base64); base64/JSON only on the
  WS-compat adapter.
- `rust-toolchain.toml` pins 1.95.0, and both workflows now request
  `dtolnay/rust-toolchain@1.95.0` explicitly. They previously said
  `@stable`; the toolchain file still won in practice, so the workflow
  read as if the pin did not exist. Bumping the pin now means editing the
  toolchain file *and* the workflow refs in the same PR.
- Workspace crates are `publish = false`; cargo-deny ignores private
  crates for licensing and allows wildcard *path* deps only.
- Dockerfile builds only `mediaserverd` and ships distroless nonroot. It
  copies `Cargo.lock` and builds `--locked`, so the image ships exactly
  the dependency set cargo-deny and the RustSec audit ran against;
  without it the image re-resolved dependencies and could ship versions
  CI never saw.
