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

### pipeline.rs — one tapped stream, end to end, sans-IO
`StreamPipeline` is the whole per-stream ingest path as a state machine:
`ingest(datagram)` classifies and buffers, `release()` emits one frame of
PCM when the caller's pacing deadline says so. It owns the jitter buffer,
the DTMF detector and the G.711 decode, and allocates nothing per packet
(one reusable `[i16; MAX_PAYLOAD]`).
- **Telephone-event packets never enter the jitter buffer.** They are
  routed to `DtmfDetector` by payload type and counted separately;
  decoding RFC 4733 payloads as G.711 audio would emit noise into every
  consumer. `telephone_events_never_reach_the_audio_path` is the guard.
- An unexpected payload type is counted and dropped, never decoded:
  the legacy media gateway's codec mismatch silently passed garbage bytes through.
- Loss is `Playout::Concealed` filled with silence, counted separately
  from `Playout::Pcm`, so a caller writing a recording keeps timing while
  the metric still says "this was a gap, not audio". **M3:** replace the
  silence fill with real PLC — the variant is the seam for it.
- Known limitation, pinned by
  `reorder_before_playout_starts_strands_the_earlier_packet`: playout
  anchors on the first packet that arrives, so if the first two packets of
  a stream arrive swapped, the earlier one is a late drop. **M3
  candidate:** allow `play_seq` to rewind while still priming, which would
  make start-of-stream reorder recoverable. Deferred because it changes
  jitter admission on the sacred path and wants its own benchmark.

### replay.rs — the packet-replay harness (Article III)
- `G711StreamGenerator` emits deterministic G.711 streams whose payload
  bytes are a counter, so decoded output is exactly assertable; it can
  also emit telephone-event packets and `skip_one` to leave a real
  sequence gap.
- `disturb(datagrams, script)` applies `Deliver / Drop / DeliverTwice /
  DelayOne` to model loss, duplication and reorder in one reusable place.
- `DatagramLog` reads a length-prefixed (`u32` big-endian) log of raw
  datagrams from a byte slice, with malformed logs surfaced as
  `ReplayError`, not panics. It takes a slice rather than a path because
  `media-core` stays sans-IO — the caller does the `fs::read`. Converting
  a real pcap into this format is the intended path once we have
  production captures; the format is deliberately trivial so the
  converter is throwaway.

### dtmf.rs — complete for RFC 4733 digit reporting
- Reports once per press on end-bit, deduped by (digit, RTP timestamp);
  events ≥16 (flash-hook etc.) deliberately ignored.

### frame.rs — complete
- `samples_per_packet` returns `None` on zero ptime/rate rather than
  dividing by zero (the legacy media gateway crashed on `a=ptime:0`).

## crates/rtpengine-ng — sans-IO layer complete, SDP included
- bencode encode/decode with malformed-input tests; command builders for
  `ping`, `subscribe request` (from-tags, mix flag, codec accept list,
  set-label), `subscribe answer`, `unsubscribe`; reply parsing with
  cookie extraction and `result=error` surfacing.
- `split_cookie` exists because `parse_reply` returns `Err` for
  `result=error` replies and therefore cannot tell a caller *which*
  request failed. The transport splits the cookie first, then parses, so
  an error reply is delivered to the waiter that asked for it instead of
  being dropped into a timeout.
- Unlike the legacy media gateway's fire-and-forget MI client, every request must
  await its correlated reply.

### sdp.rs — subscription-leg offer/answer
- Parses rtpengine's subscribe offer: per-stream ports, payload-type
  lists, `a=ptime`, `a=label`, and session- vs media-level `c=` lines
  (`stream_address` resolves media-level over session-level, which is
  what a two-leg tap offer needs).
- Builds the `recvonly` answer with one local receive port per offered
  stream; the count must match or it is an error, since an answer with a
  different number of m= sections is not a legal answer.
- Deliberate limits, each with a test: audio-only (a non-audio m= line is
  a loud error, not a silently skipped section), static payload types
  only (L16/Opus answers need dynamic rtpmap — `NoStaticPayloadType`),
  mono only, and a zero ptime/sample rate is rejected via the same
  `samples_per_packet` guard `frame.rs` uses.
- Depends on `media-core` for `AudioFormat`/`Encoding` rather than
  restating the codec table; `Encoding::rtpmap_name` was added there so
  the vocabulary lives in one crate.
- **M3:** dynamic rtpmap so L16/Opus can be signalled on the tap leg.

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

### ng_transport.rs — async NG transport, tested against a fake node
Control-world only (Tokio): one UDP socket per rtpengine node, a reader
task that dispatches each datagram to the waiter registered under its
cookie, per-request `oneshot`, timeout, retry, and node health counters.
- **Retries reuse the same cookie.** rtpengine caches replies by cookie,
  so a retransmit is idempotent and a slow-but-alive node returns the
  cached reply instead of executing the command twice. Pinned by
  `retransmits_the_same_cookie_until_the_node_answers`.
- Cookies are `{prefix:x}-{serial:x}` with the prefix supplied by the
  caller (`main` derives it from the wall clock), so a restarted pod does
  not collide with its own pre-restart cookies in rtpengine's reply
  cache. `CookieSequence` itself takes no clock, keeping it testable.
- A `PendingGuard` removes the waiter on every exit path, so a timed-out
  request cannot leak an entry into the correlation map
  (`pending_waiters_are_released_when_a_request_ends`).
- Stray datagrams (unknown cookie, no separator) are counted at debug and
  dropped; they must not disturb an in-flight request (Article IV).
- The `Mutex` around the correlation map is legal because this is the
  control world; nothing here touches a packet deadline (Article II).
- Tests run against a fake rtpengine on `127.0.0.1:0` that can be told to
  swallow datagrams, reply with errors, or reply out of order — no lab
  needed for the protocol behavior.
- Known limits: one node per transport (no pool or node registry yet), no
  re-subscribe orchestration, and `#[allow(dead_code)]` on the module
  until the control plane calls the subscribe verbs — same pattern as
  `supervisor.rs`.
- **M2 remaining:** real lab validation against rtpengine.
  **M4:** node registry keyed by call→node discovery, re-subscribe on pod
  loss.

### tap_spike.rs — the Phase-0 capture (media world)
Owns the sockets and the pacing for one tap: drain both legs, release one
frame per leg every `ptime` on a wall-clock-anchored deadline, accumulate
PCM, and write the WAV once capture ends.
- **No file I/O on the media thread.** PCM accumulates into per-leg buffers
  preallocated from `max_capture`, and `write_wav` runs afterwards on the
  caller's thread. Writing a WAV frame-by-frame from the release loop would
  be exactly the blocking I/O Article I forbids. The cost is that capture
  length is bounded by memory: 8 kHz × 2 B × 2 legs ≈ 32 KB/s, so a
  10-minute tap is ~19 MB.
- Every cap is surfaced rather than silent (Article VIII): `capture_full`
  when the preallocated buffer is reached, `drain_batches_filled` when a
  drain hits `MAX_DATAGRAMS_PER_DRAIN` (64, which bounds how long a flood
  can starve the pacer), `recv_errors`, and `reanchors` when the release
  deadline falls more than one `ptime` behind and re-anchors instead of
  bursting.
- `Playout::Waiting` appends a frame of silence and counts an underrun, so
  both legs stay sample-aligned and the stereo file keeps real time. That
  is why a source slower than the pacer shows up as underruns plus silence
  rather than as a shortened file.
- Stereo mapping is Customer left, Agent right (matching FS
  `RECORD_STEREO`); one leg writes mono; duplicate tracks and more than two
  legs are errors.

### tap_session.rs — the Phase-0 orchestration (control world)
Drives the whole subscribe lifecycle and is env-configured, so the spike
needs no CLI or config plumbing: `MSS_TAP_CALL_ID` (presence selects spike
mode instead of the daemon loop), `MSS_RTPENGINE_NODE`, `MSS_TAP_LOCAL_IP`
(must be routable *from* rtpengine — it goes into the answer SDP),
`MSS_TAP_FROM_TAGS`, `MSS_TAP_OUTPUT`, `MSS_TAP_SECONDS`.
- **Order matters:** subscribe request first, *then* bind sockets, then
  answer. The offer is what tells us how many streams rtpengine will send,
  so binding first would mean guessing the count.
- Track assignment follows offer order (stream 0 = customer), and each
  offered stream's `a=label` and source address are logged so an operator
  can confirm the mapping instead of trusting it. Label-based assignment is
  a later refinement.
- `unsubscribe` failure is a warning, not an error return: the WAV artifact
  is the point of the exercise and must not be lost because teardown
  failed. rtpengine expires the subscription on its own.
- Known limits: at most 2 streams (`TooManyStreams`), PCMU/8k/20ms fixed
  (we request PCMU in `accept_codecs` and let rtpengine transcode), and the
  process exits after the spike rather than continuing as a daemon.

### Verified end to end without a lab (2026-08-14)
The spike was run against a scripted fake rtpengine on loopback that
answers `subscribe request` with a two-stream sendonly offer, reads our
answer SDP for the receive ports, streams G.711 to them, and answers
`unsubscribe`. Result: both labels parsed, ports bound and advertised, 160
datagrams per leg received with 0 unparsable and 0 recv errors, 159 frames
played + 40 underruns = 199 releases on both legs, 0 reanchors, both legs
sample-aligned at 31,840 samples, and a 2-channel 8 kHz WAV of 3.98 s with
distinct content per channel. The underruns are the fake sending at ~40
packets/s against a 50/s pacer — the expected shape, and useful proof that
underrun accounting and silence-fill alignment work.
**This does not close M2:** it proves our side of the protocol. The exit
criteria still need a real rtpengine (version support, re-INVITE/hold
behavior, transcoding at the tap) and the rtpengine-host CPU measurement.

### main.rs — optional NG reachability probe
`MSS_RTPENGINE_NODE=ip:port` makes the daemon ping that node at startup
and log the result, which is how the Phase-0 "does our deployed rtpengine
answer NG at all" question gets answered without any other wiring. Unset
skips the probe; an unparseable value logs an error and the daemon still
starts (a diagnostic must not be able to stop the service).

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

## Phase-0 measurements so far (Article VIII)

`cargo bench -p media-core` (criterion, `benches/ingest_pipeline.rs`),
first run 2026-08-14 on an **Apple M2, 8 cores, arm64, rustc 1.93.0** —
note that is *not* the pinned 1.95.0 (see the toolchain note below), and
not the production Linux/x86 target, so treat these as order-of-magnitude:

| Benchmark | Per 1000 packets | Per packet |
| --- | --- | --- |
| `ingest_only_per_packet` (parse + jitter push) | 10.89 µs | **10.9 ns** |
| `parse_jitter_decode_per_packet` (+ pop + G.711 decode) | 164.9 µs | **165 ns** |

Reading: decode dominates the sans-IO path (~154 of the 165 ns). One
G.711/20 ms leg is 50 packets/s, so a leg costs ~8.3 µs of CPU per second
and a two-leg passive session ~16.5 µs/s — about 0.002% of one core.

**What this does and does not price.** It prices exactly the sans-IO
pipeline on a clean single stream with a hot cache. It excludes the socket
syscalls (the thing `recvmmsg` would address), per-consumer encode,
resampling to 16 kHz, fan-out queueing, and everything on the rtpengine
host. The Phase-0 exit criteria still need the real tap. The useful
conclusion for now is a negative one: the decode/jitter path is nowhere
near the constraint, so the per-tap ceiling will be set by syscalls and
fan-out, which is where the next measurement should go.

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
- **The pin only binds rustup-managed toolchains.** A Homebrew-installed
  `rustc` ignores `rust-toolchain.toml` entirely, so a developer can run
  the whole local gate on a different compiler than CI uses (observed:
  1.93.0 locally vs 1.95.0 in CI). If local and CI results ever disagree,
  check `rustc --version` first. Installing via rustup is what makes the
  pin real on a workstation.
- `criterion` is a dev-dependency with `default-features = false`, which
  drops plotters/rayon and keeps the added third-party crate count and
  license surface small. All 77 third-party crates resolve to a license in
  the `deny.toml` allowlist (several use the legacy `MIT/Apache-2.0`
  spelling, which cargo-deny normalizes).
- Workspace crates are `publish = false`; cargo-deny ignores private
  crates for licensing and allows wildcard *path* deps only.
- Dockerfile builds only `mediaserverd` and ships distroless nonroot. It
  copies `Cargo.lock` and builds `--locked`, so the image ships exactly
  the dependency set cargo-deny and the RustSec audit ran against;
  without it the image re-resolved dependencies and could ship versions
  CI never saw.
