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
- Telephone-event sequence numbers are *accounted*, not lost:
  `account(seq)` occupies the slot without audio, `pop()` returns
  `Accounted` for it, and `lost` counts only real gaps. This closed the
  IVR-inflates-loss finding from the lab; the DTMF-on-a-clean-link
  acceptance test lives in pipeline.rs.
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
- **Telephone-event payloads never reach the audio path**, but their
  sequence numbers are accounted to the jitter buffer, so a DTMF press is
  neither decoded as noise nor miscounted as loss. Playout emits
  `Playout::Suppressed` silence for those slots, counted as
  `frames_suppressed` — endpoints suppress audio during a press, and the
  recording keeps wall-clock timing.
  `telephone_events_never_reach_the_audio_path` is the guard.
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

### encode.rs — per-consumer output formats (landed 2026-08-22)

`ConsumerEncoder` turns the tap's PCM into what one consumer asked for:
G.711 µ/A passthrough at the tap rate, L16 at the tap rate, or resampled
L16 (verified at 16k and 48k). One encoder instance per consumer **per
track** — a resampler is stateful, so interleaving two tracks through one
instance would corrupt its filter history; the gRPC pump keys encoders by
track.

- **L16 is little-endian** ("linear16" as ASR vendors consume it), not the
  network-order RTP L16. Pinned by
  `l16_at_the_tap_rate_is_little_endian_identity` and noted in
  mediastream.proto.
- Resampling is `rubato` 5 (`Fft`, `FixedSync::Input`) — Article XI:
  adopted, pure Rust, no new system dependency. Chunk size is the tap's
  samples-per-packet, so every call emits exactly one output frame
  (320 samples at 16k). The FFT resampler carries a small group delay
  (`output_delay`), which is why the replay test asserts rms over the
  steady-state half rather than sample-exact equality.
- **Short frames are zero-padded to a full chunk** rather than producing a
  short output — streams must stay continuous for downstream ASR
  (session-playbook §8); the injected-utterance tail is the case that hits
  this.
- Refusals are named, never silent: Opus (tasks item 9), G.711 at a
  non-tap rate, stereo, mismatched ptime.

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
- `SubscribeRequest` carries both `accept_codecs` and `transcode_codecs`,
  and the tap sends **transcode**. `accept` only means "use this codec if
  the leg already has it", so against an A-law call rtpengine offered PCMA
  alone and rejected the PCMU answer. `transcode` makes it offer both and
  convert, which keeps the whole pipeline PCMU whatever the carrier picked.
  A real softphone found this; every synthetic lab call had been PCMU.
- `play media` / `stop media` builders exist so audio can be pushed *into*
  a tapped call without an inline leg. `PlayTarget` is named for what the
  lab measured rather than for the wire key: `HeardBy(tag)` emits
  `from-tag` and only that participant hears the audio, `HeardByEveryone`
  emits `all: "all"` and both do. `PlaySource::Blob` carries the audio as
  raw `Value::Bytes`; `blob64` is **not** supported by rtpengine 14.1.1.8
  (it answers `No media file specified`), so base64 is not an option.
- These are utterance-shaped, not a stream: rtpengine plays a complete
  ffmpeg-decodable file or blob. Streaming TTS with barge-in still needs
  the Phase-3 inline leg. `stop media` is the barge-in primitive and its
  cut-through latency has not been measured yet.
- `PlayMedia.block_egress` emits `flags: [block-egress]`, and injection
  always sets it. Without it the listener receives the peer's stream and
  the player concurrently — two SSRCs and two sequence spaces through one
  softphone jitter buffer, which is inaudible mush. With it rtpengine
  pauses the peer for the playback and resumes after, measured in
  `lab/host_test_caller.py` runs as complementary packet counts.

### sdp.rs — subscription-leg offer/answer
- Parses rtpengine's subscribe offer: per-stream ports, payload-type
  lists, `a=ptime`, `a=label`, and session- vs media-level `c=` lines
  (`stream_address` resolves media-level over session-level, which is
  what a two-leg tap offer needs).
- Builds the `recvonly` answer with one local receive port per offered
  stream; the count must match or it is an error, since an answer with a
  different number of m= sections is not a legal answer.
- **The answer may not drop any payload type the offer carried.** It lists
  our codec first and then echoes every other offered payload type with its
  rtpmap. The telephone-event rule found earlier was a special case of this;
  an A-law call from a real softphone exposed the general one. Listing our
  codec first is what makes rtpengine transcode to it, measured as PCMA in,
  payload type 0 out.
- **The answer must echo the offered `telephone-event` payload type.** Real
  rtpengine 14.1.1.8 rejects a subscription answer that drops it with
  `Failed to process subscription answer` — proven by `lab/ng_answer_probe.py`
  (see [lab.md](lab.md)). `a=rtpmap:` is parsed per stream so the answer can
  echo the payload type and clock rate rtpengine actually offered, and so the
  negotiated telephone-event type feeds `StreamPipeline` instead of a hardcoded
  101. Without this there would be no RFC 4733 on any tap, silently breaking
  the frozen `firstDtmf`/`dtmfResult` contracts.
- The real rtpengine offer is pinned as a fixture
  (`parses_a_real_rtpengine_14_subscribe_offer`). It has **no session-level
  `c=` line** — only media-level — so media-level precedence is the only
  source of the source address, not a refinement.
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
- **M3:** the WS consumer bridge must also replicate the reference
  deployment's forked mod_audio_fork dialect. **Open item:** the fork's exact wire format is
  specified only in the fork's C source (not in this repo, not in the legacy controller);
  pull it and write the byte-exact tests before the first ASR consumer
  migrates (architecture.md risk #4).

## crates/session-core — the control-plane state machine (M4), sans-IO

`SessionRegistry` is the MediaControl API of architecture.md §5.1 as a pure
state machine: no sockets, no async, no clock of its own. The tonic service
and the Redis registry are shells around it, which is what makes every rule
below testable without a lab (Constitution, Article III).

The four nouns are `SessionKind` (Tap/Inline/Mix), `AttachSpec`,
`PlaybackSpec` and `MediaEvent`. Phase 3/4 add no operations — an inline leg
is `SessionKind::Inline` and a conference is `SessionKind::Mix`, both already
accepted by the same calls.

### The invariants it enforces, and where they come from

- **Capability is an authorization boundary (§5.2), checked twice.** Once
  against the transport (`Transport::carries` — `FileS3` carries `SINK`
  only, so a recorder cannot even be *granted* `INJECT`), and once against
  the attachment at use time (`authorize_inject`, `report`). A recorder that
  tries to inject and an analytics sink that tries to emit
  `end_of_interaction` both fail structurally rather than by convention.
- **Exactly one authoritative attachment per session (§5.3).** A second
  authoritative attach is `AuthoritativeAlreadyBound`, never silently
  resolved. `legacy_eligible` on each event is that attachment's flag, and
  `MediaEvent::legacy_name()` returns `None` for everything else — so an RTT
  service and a voice-AI bridge can both return transcripts while only one
  drives `the legacy stream fsm`. Detaching frees the role for a successor.
- **A consumer cannot forge event identity (§5.5).** This is why
  `ConsumerEvent` and `EventKind` are separate types rather than one: a far
  end can *claim* speech results, but `session`, `external_id`,
  `attachment`, `seq`, `legacy_eligible` and `first_final` are all supplied
  by the registry. There is no field a hostile or buggy consumer can set to
  misattribute an event, because the type it sends has no such field.
- **`first_final` is tracked per attachment**, not per session: it means
  "the first final transcript of this fork", which is what
  `mod_audio_fork::first_transcript` meant. Two attachments each get one.
- **Idempotency (§5.1).** Every mutating spec carries an optional key. A
  replay returns the original id; the same key with a *different* request
  fingerprint is `IdempotencyConflict` rather than silently handing back the
  wrong resource. Fingerprints deliberately exclude the key itself.

### Bounds, because nothing here may grow without one

- `max_attachments` per session (default 16) → `TooManyAttachments`.
- Outbox `OUTBOX_CAPACITY` (1024) is drop-oldest with an `events_dropped()`
  counter, the same policy the hub uses — a dropped event must be a metric,
  never a silence. Sequence numbers are per session and gapless, so a
  consumer can *detect* a drop rather than infer it. The shell is expected
  to `drain_events()` every tick, which keeps this far from the cap.
- Idempotency cache `IDEMPOTENCY_CAPACITY` (4096) with FIFO eviction. A
  sans-IO core has no clock, so it cannot expire by TTL; if the shell wants
  time-based expiry it must drive it.

### Known gaps (M4)

- **Not yet wired to tonic.** `proto/mediacontrol.proto` is the contract;
  the prost/tonic build and the service impl are the next increment, along
  with mapping `ControlError` onto gRPC status codes (`CapabilityDenied` →
  `PERMISSION_DENIED`, `AuthoritativeAlreadyBound` → `FAILED_PRECONDITION`,
  `Unknown*` → `NOT_FOUND`, `IdempotencyConflict` → `ABORTED`).
- **Not yet wired to the hub.** `TrackSelector` is the control-world twin of
  `hub::TrackSelection`; the shell converts. They are deliberately separate
  types — the media world must not depend on control-plane vocabulary — but
  if a third copy ever appears, that is the signal to promote one.
- **No persistence.** Redis session registry with ownership leases and
  re-subscribe on pod loss is M4; the registry is per-process today, and
  `SessionId`/`AttachmentId` counters restart with the process (the wire
  form is prefixed and parse-checked, so a stale id from another pod is
  rejected as unknown rather than aliased onto a live session).
- `Observation` covers what MSS witnesses itself (DTMF from the pipeline,
  recording lifecycle). Playback events are emitted by the registry.
  Recording is a Phase-2 consumer, so those variants exist ahead of the
  sink that will raise them.

## crates/control-api — the Session Controller's network surface (M4)

The `MediaControl` gRPC service (`proto/mediacontrol.proto`) over the
sans-IO `session-core` state machine. The crate exists so the generated
protobuf code and the gRPC dependency tree stay out of `mediaserverd`, and
so the service is testable without booting the daemon.

**On the name:** gRPC is served by `tonic`, but that word appears nowhere in
our vocabulary — the type is `SessionController`, after architecture.md
§3.1. tonic is a dependency, not a concept.

### Build: no protoc, deliberately

`build.rs` compiles the protos with **`protox`** (a pure-Rust protobuf
compiler) and hands the descriptors to `tonic-prost-build` via
`compile_fds`. The obvious route — `tonic_prost_build::compile_protos` —
shells out to a `protoc` binary, which GitHub's runners do not ship and the
distroless Docker build does not have. Going through protox keeps the build
hermetic: CI and the Dockerfile needed no changes at all. Generated code
lands in `OUT_DIR`, which also keeps it clear of the CI comment scan (it is
full of doc comments lifted from the `.proto` files).

### Where the design shows up on the wire

- `ControlError` maps to a status a caller can act on, not a generic
  failure: `CapabilityDenied` → `PERMISSION_DENIED`,
  `AuthoritativeAlreadyBound` / `TransportCannotCarry` →
  `FAILED_PRECONDITION`, `ExternalIdInUse` → `ALREADY_EXISTS`,
  `IdempotencyConflict` → `ABORTED`, `TooManyAttachments` →
  `RESOURCE_EXHAUSTED`, `Unknown*` → `NOT_FOUND`, malformed ids and
  unspecified enums → `INVALID_ARGUMENT`.
- **Unspecified enum values are refused, never defaulted.** proto3 cannot
  distinguish "absent" from "zero", so accepting `SESSION_KIND_UNSPECIFIED`
  would silently create a TAP session for a caller who meant INLINE.
- **Nothing is silently ignored.** A request field this layer cannot honour
  yet fails loudly: `mix: true` returns `UNIMPLEMENTED` because mixed
  subscriptions are not wired to rtpengine, and a `StartPlayback` with no
  source is `INVALID_ARGUMENT` rather than a no-op.
- `MediaPlane` is the seam to the media world, and it covers the whole
  lifecycle: `open_session` / `close_session`, `open_attachment` /
  `close_attachment`, `send_text`, `start_playback` / `stop_playback`, and
  `open_stream` (a bounded channel of `StreamFrame`s for a grpc-stream
  attachment; defaulted to a refusal so registry-only planes need not care).
  It is async because opening a tap means an NG round trip to rtpengine.
  **Every open is rolled back if the media world refuses it** — a session
  rtpengine will not tap is destroyed again before the RPC returns, and an
  attachment whose consumer cannot be reached is detached, so a failed call
  never leaves a half-built session behind. Teardown runs the other way:
  the registry is authoritative and media cleanup is best-effort with a
  warning, because a caller that asked to destroy should not be refused
  because cleanup hiccuped.
- With **no** media plane installed the controller is a registry only:
  lifecycle calls are skipped and `send_text`/playback return `UNAVAILABLE`.
  That is the mode the unit tests use; `mediaserverd` always installs one.

### The drain bug this layer already had, and its fix

The first wire test hung forever. tonic's graceful shutdown waits for
in-flight requests, and a `WatchEvents` stream is in-flight until it ends —
so **one debug watcher would have pinned a pod open through its entire
drain**. Two changes, both regression-tested
(`a_watcher_that_never_reads_does_not_pin_the_server_open_on_drain`,
`a_watch_on_one_session_ends_when_that_session_does`):

- A session-scoped watch **ends when that session ends**, which is the
  correct semantics anyway.
- The controller carries a drain signal (`begin_drain`, wired by
  `serve_on_until`) that ends every watcher, including watch-everything
  streams that have no natural end.

Watchers are served by a spawned task feeding a bounded `mpsc`, so a
subscriber that stops reading applies backpressure to itself rather than to
the controller; broadcast lag is logged with the missed count.

### stream.rs — the `MediaStream` data plane (M4, landed 2026-08-20)

`MediaStreamService` implements the generated `MediaStream::Subscribe`. The
shape inverts the WS adapter: MSS does not dial the consumer, the consumer
dials MSS after `Attach{GRPC_STREAM}`, presenting
`ConsumerHello{attachment_id, token}`. The service validates hello (token,
attachment exists, transport is grpc-stream, requested format equals the
attachment's — the format was chosen at Attach and per-hello renegotiation
is deliberately not a thing, so a mismatch is `UNIMPLEMENTED`,
never silent), then asks the media plane for frames through the new
`MediaPlane::open_stream` seam and serves them as binary `AudioFrame`s
(raw payload, no base64), `DtmfFrame`s, and `TextFrame`s for
`SendToAttachment` passthrough.

- **Frame vocabulary is native** (`customer`/`agent`/`mixed`), not the
  Twilio `inbound`/`outbound` — this is the native surface, the WS adapter
  is the compatibility one.
- **Inject is authorized at the first frame**, not at flush:
  `authorize_inject` maps `CapabilityDenied` to `PERMISSION_DENIED` and the
  stream ends — the proto calls unprivileged inject a protocol violation,
  not a no-op. An authorized utterance accumulates decoded PCM, `Mark`
  flushes it as a WAV blob through the controller's own `StartPlayback`
  (requested_by = the attachment, block_egress = true), so the playback is
  registry-tracked, evented onto `mss.events` and attributed; `Clear`
  discards the buffer and stops the last playback — the barge shape, with
  `NOT_FOUND` on the stop tolerated because the playback may have ended.
- **Utterances are capped at one playback datagram** (~3.7 s at 8 kHz,
  `MAX_UTTERANCE_SAMPLES`); over the cap is `RESOURCE_EXHAUSTED` naming
  chunked playback as unimplemented. The WS bridge keeps the piece-paced
  path for long utterances.
- **The drain lesson applies here too**: every subscribe task watches the
  controller's drain signal and ends with a `StreamStop{draining}`, pinned
  by `a_subscribe_stream_ends_when_the_pod_drains` — the same class of bug
  the first `WatchEvents` had.
- Stream end semantics: media-plane channel closing (detach, session end)
  sends `StreamStop{"the tap ended"}`; the consumer hanging up just ends the
  task, which drops the frame receiver and detaches from the hub. The
  attachment survives its consumer, so a reconnect is a fresh `Subscribe` —
  `TapPlane` refuses a second concurrent consumer per attachment.

### auth.rs — the shared-secret policy (M4, landed 2026-08-20)

`AuthPolicy` carries an optional shared secret (`MSS_AUTH_TOKEN` in the
daemon). As a tonic interceptor it guards `MediaControl` (`authorization:
Bearer …`, constant-time comparison, `UNAUTHENTICATED` otherwise); the same
policy checks `ConsumerHello.token` on the data plane. Unset means open —
the lab mode — and the daemon logs that loudly at startup.

**`TelCompat` is deliberately not intercepted.** Its whole contract is that
an unmodified the legacy controller client works byte-for-byte; the legacy controller sends no auth
metadata, so intercepting it would break the no-client-change property.
The pilot fronts that surface with network policy; if the legacy controller ever grows an
outbound interceptor, wiring the same `AuthPolicy` there is one line.
A shared static secret is the v1: per-attachment minted tokens (so a
data-plane consumer never holds the control-plane credential) are the
natural next step and would ride the provisional `Attachment` proto.

### Known gaps (M4)

- `WatchEvents` is the debug path only; the production event path is Kafka
  `mss.events` via the daemon's event pump.
- `owner_pod` is whatever string the controller was constructed with; real
  placement is still ahead (the Redis lease keeper supplies recovery, not
  placement).
- Auth is a single shared secret per deployment; per-attachment tokens are
  future work (see auth.rs above).

## crates/mediaserverd

### metrics.rs — the Prometheus endpoint (M4, landed 2026-08-20)

`MSS_METRICS_LISTEN=ip:port` serves the Prometheus text format. There is
deliberately **no metrics crate and no HTTP framework**: the exposition
format is plain text and the handler answers any complete HTTP/1.1 request
with the one page, `Connection: close`. Fewer dependencies to audit, and
the format is asserted by tests (every series declares HELP/TYPE; every
drop counter the alert rules reference is present).

How the numbers get out of the media world without locks or allocation:

- Each `TapLeg` gets an `Arc<SharedLegStats>` (a struct of `AtomicU64`s).
  The capture thread `store`s its counters into it once per release tick
  (~17 relaxed stores per leg at 50 Hz — noise), plus once at capture end.
- `TapPlaneMetrics` holds the live map plus **retired totals**: when a
  session closes, its final leg values are folded into the retired
  accumulator *after* the capture thread joins (so the values are final),
  and a scrape sums retired + live under one lock. That keeps every
  counter monotonic across session churn, which `rate()` requires.
- Consumers are the same shape: `hub::SubscriptionMetrics` exposes queue
  depth, `dropped_oldest` and `delivered` from the hub's own atomics;
  closing or replacing a consumer folds its totals into retired.
- Per-call label cardinality is deliberately avoided: totals are per pod,
  gauges cover live state (`mss_sessions_live`, `mss_legs_stalled`,
  queue depth sum/max). The per-call detail already exists in the
  `tap leg finished` log line; a label per call is a cardinality bomb at
  the intended scale.
- Event pump, registry keeper and controller outbox counters are read
  directly from their existing `Arc`s; absent sources (no Kafka, no Redis)
  leave their series out entirely rather than reporting zeros that look
  like health.

`deploy/prometheus-alerts.yaml` carries the alert rules: every drop counter
(consumer frames, event queue, publish failures, outbox), watchdog stalls,
jitter loss ratio, and the split-brain signal `mss_registry_lost_total`.

### supervisor.rs — the audio-flow watchdog, now wired (2026-08-20)

`AudioFlowWatchdog` finally has a caller: each `TapLeg` with shared stats
owns one. The capture loop touches it when datagrams arrive and checks it
each release tick; the stall flag and transition count are exported
(`mss_legs_stalled`, `mss_ingest_stalls_total`). `STALL_AFTER` is 10 s —
generous on purpose, because a silence-suppressing caller (MicroSIP
between utterances) legitimately stops sending RTP; the alert adds its own
`for:` window on top. Stall-triggered re-subscribe remains future,
incident-driven work — the metric comes first so we learn real stall
shapes before automating a reaction.

Until now a tap lived and died with its pod. The registry makes a session
recoverable, which is the Phase-1 exit criterion about re-subscribe recovery.

**What is stored** (`mss:` namespace, configurable — a shared Redis serves
several environments):

- `mss:session:{external_id}` — the whole `PersistedSession`: kind, call-id,
  from-tags, rtpengine node, owner, and every attachment with its **endpoint,
  capabilities, selector, authoritative flag, paused state and metadata**.
  The test that matters asserts the endpoint survives: without it a rebuilt
  consumer has nowhere to reconnect, and the first draft dropped it because
  the proto `Attachment` response does not carry endpoint or metadata. The
  keeper now reads `SessionController::snapshot()` — the lossless view — not
  the wire types.
- `mss:lease:{external_id}` — the owner pod, **with a TTL** (15 s, renewed
  every 5 s). The lease is the whole HA mechanism: a pod that dies stops
  renewing, the key expires, and the session becomes adoptable.
- `mss:sessions` — a set, so a sweep never needs `KEYS`.

**How adoption works.** Every 10 s a pod runs `claim_unleased`, which for each
indexed session attempts `SET lease NX EX 15`. `NX` is what makes the claim
atomic: **exactly one** pod wins, verified against real Redis with six
concurrent contenders racing for one orphan. The winner rebuilds through the
controller's own API — `CreateSession` then `Attach` per attachment, replaying
`paused` — so every invariant (capability checks, one authoritative
attachment, idempotency) applies to a rebuilt session exactly as to a new one,
and `TapPlane` re-establishes the rtpengine subscription as a side effect.
`MAX_ADOPTIONS_PER_SWEEP` (8) stops one pod inhaling every orphan at once.

**Two failure modes it refuses to paper over:**

- A session with no call-id or from-tags **cannot be re-tapped**, so it is
  released rather than half-restored and counted as `unrebuildable`. This is
  exactly the shape a TelCompat-created session has today, which is another
  reason the discovery map matters.
- An **ended** session must stop being persisted, or another pod adopts a call
  that is already over and taps a dead call forever. The keeper tracks what it
  persisted and calls `forget` for anything that has vanished from the
  controller; `released` counts it. The first draft missed this and the test
  for it was the one that caught it.

Counters (`persisted`, `renewed`, `lost`, `adopted`, `unrebuildable`,
`released`, `failed`) are logged at shutdown and are the natural next metrics.
A rising `lost` means two pods believe they own one session — the split-brain
signal worth alerting on.

Config: `MSS_REDIS_URL`; unset means sessions live and die with the pod
(logged), and a configured-but-unreachable Redis **refuses to start** rather
than running with no recovery. Verified in the lab on a live call: the session
appeared in Redis with its call-id, tags and consumer endpoint, the lease
counted down from 15, and the index was empty again after `DestroySession`.

`redis` 1.6 is pure Rust, so this added no system dependency — but it pulls
`xxhash-rust` under **BSL-1.0** (Boost), now allowed in `deny.toml`:
permissive, OSI-approved, no attribution burden in binaries.

Gaps: leases are renewed per session per tick with one round trip each (fine
at hundreds, revisit at thousands); the discovery map (call-id → node + tags)
is a separate, still-unbuilt concern; and `SessionStore` is a `mediaserverd`
module rather than a crate, so the integration test re-includes it by path.

### event_pump.rs — events onto Kafka `mss.events` (M4)

The other half of architecture.md §5.4: commands arrive over gRPC, events
leave on the bus. `SessionController` gained an `EventSink` seam
(`with_event_sink`), fed **inside the registry lock** right after
`drain_events()` so per-session order survives concurrent RPCs; `accept` is
contractually non-blocking (`try_send` into a bounded queue). The pump
worker encodes each `session_core::MediaEvent` to the typed protobuf
`mss.v1.MediaEvent` (`convert::event_bytes` / `event_from_bytes`, pure and
round-trip-tested) and produces it keyed by `external_id`.

- **Partitioning**: FNV-1a over `external_id` modulo the topic's partition
  count — deterministic across restarts, so a session's events stay on one
  partition and per-call ordering holds. Verified off the wire: a full
  lifecycle (2×AttachmentUp, PlaybackStarted, 2×AttachmentDown,
  SessionEnded) landed on one partition with gapless seq 0-5 and
  `legacy_eligible` true only for the authoritative attachment's events.
- **Bounds and honesty**: queue of 1024, counted drops with a warning;
  broker failures are counted per event and logged, never fatal
  (at-most-once for now — durable retry is future work and the gapless seq
  makes gaps detectable downstream). Totals logged at shutdown.
- **Topic bootstrap**: `RskafkaTransport::connect` lists topics and creates
  `mss.events` (default 4 partitions, RF 1) when absent, then holds one
  `PartitionClient` per partition. Config: `MSS_KAFKA_BROKERS` (comma
  list; unset = events stay in-process, logged), `MSS_EVENTS_TOPIC`,
  `MSS_EVENTS_PARTITIONS`. A configured-but-unreachable broker **refuses to
  start** rather than silently running eventless.
- **Crate map deviation, measured not preferred**: the architecture listed
  `rdkafka`, but its vendored librdkafka 2.12 build hard-requires libcurl
  headers plus cmake/g++, which would have to be added to CI, the
  Dockerfile and the lab image (probe: `rdkafka_conf.c:60 fatal error:
  curl/curl.h`). `rskafka` (InfluxData, MIT/Apache) is pure Rust and
  builds hermetically — the same trade already made with protox over
  protoc. Producer-only use fits it; revisit only if consumer groups or
  transactions are ever needed on the MSS side.
- `examples/mss_events_tail.rs` consumes and decodes the topic — the lab
  verification tool and the reference for the legacy controller's translator.
- Fixed in passing: the Dockerfile never copied `proto/`, so the image
  build had been broken since control-api landed.

### Resolving a call's participants without the discovery map (2026-08-17)

The blocker on any the legacy controller integration was that a TelCompat caller knows only
the FreeSWITCH channel uuid, while a tap needs the SIP call-id and the
participants' from-tags — filed for months as "waiting on OpenSIPS to write a
discovery map to Redis".

It turns out not to need one. the legacy controller already has both facts on the channel:
`Variable_sip_call_id`, and the caller's tag inside `Variable_sip_full_from`.
And rtpengine will name a call's participants itself — `query` returns
`tags`, which is what `lab/call_watcher.py` has been doing all along. So:

- `TelCompat` reads `sipCallId` and `callerFromTag` out of `StreamRequest`'s
  metadata map (no proto change, so clients stay wire-compatible) and puts
  them on the session.
- `TapPlane::complete_from_tags` asks rtpengine for the rest before
  subscribing. One `query`, no new dependency, no OpenSIPS config change.

**The caller hint is not optional in practice.** Measured on two live calls:

| | resolved order | the caller's voice landed on |
| --- | --- | --- |
| no hint | `[freeswitch-tag, hosttest]` | `outbound` — **wrong** (rms 488) |
| with hint | `[hosttest, freeswitch-tag]` | `inbound` — correct (rms 543) |

rtpengine's reply is a bencode dict, so tags arrive in key order, not creation
order; `created` is second-resolution and ties on a fast answer (the lab
watcher hit the same wall and resorted to a topology heuristic — which leg
faces FreeSWITCH — that MSS should not copy). Without the hint, customer and
agent are a coin flip on tag sort, so an unnamed caller now logs a **warning**
naming the metadata key that fixes it, rather than silently mislabelling a
recording. SSRC correlation still pins *which stream is which speaker*; it
cannot know which speaker is the customer, and that is what the hint supplies.

Consequence for the roadmap: the OpenSIPS→Redis discovery map is no longer on
the critical path for a pilot. It is still the better long-term answer — it
avoids a `query` per tap and works when MSS never sees the channel — but it is
now an optimisation rather than a blocker.

### telcompat.rs — the legacy controller's verbs, served by MSS (§5.6)

The migration switch. `proto/telcompat.proto` declares
**`package protos; service TelService`** deliberately: gRPC routes on the
fully-qualified method path, so `/protos.TelService/StartStream` is
byte-identical to what the legacy controller already calls, and a per-tenant flag can point a
client at `MSS` instead of `the legacy gRPC server` with **no client change** and roll
back by pointing it back. Message shapes and field numbers are copied verbatim
from the legacy controller's `the legacy verb API.proto`.

Only the media subset is declared — stream, transcription, recording, playback
stop. Originate, answer, hangup, bridge, conferences and IVR prompting stay on
FreeSWITCH, and a client calling one of those here gets `UNIMPLEMENTED`, which
is the honest answer rather than a silent success.

The mapping (one test per row in `tests/telcompat.rs`):

| the legacy verb API verb | MSS nouns |
| --- | --- |
| `StartStream` | `CreateSession{TAP}` if absent + `Attach{WS_TWILIO, SINK+EVENTS+INJECT, authoritative}` |
| `StopStream` | `Detach`, and `DestroySession` when it was the last attachment |
| `StreamPause` / `StreamResume` | `UpdateAttachment{paused}` |
| `StreamSendText` | `SendToAttachment` |
| `StreamPlayFile` | `StartPlayback{file, requested_by: the fork}` |
| `StartRecording` | `Attach{FILE_S3, SINK}`, endpoint `${accountID}/${recordingID}.${format}` |
| `StopRecording` | `Detach` (+ `DestroySession` if last) |
| `StopPlayback` | `StopPlayback` — the barge-in primitive |
| `StartCallTranscription` | **`UNIMPLEMENTED`**, deliberately: since the Deepgram move transcription *is* the fork, and the tenant's ASR endpoint is not in `PlayAndGatherRequest`. Callers use `StartStream` with `ws_url` until that config is plumbed. Inventing an endpoint here would fail at connect time instead of at the call. |

Two things the tests caught, both worth keeping:

- **Authoritative follows the session's purpose, not arrival order.** The first
  version claimed `authoritative: true` for every attachment, so a recorder
  joining a streamed call was refused with `AuthoritativeAlreadyBound`. The
  fork produces the speech events the legacy stream fsm runs on; a recorder is a `SINK`
  with no back-channel and must never claim them.
- **One session serves both.** `StartStream` + `StartRecording` on the same
  channel produce one tap with two attachments, which is the whole point:
  today those are two FreeSWITCH mechanisms.

Both surfaces share one controller and one port (`server.rs`): the packages
differ (`mss.v1` vs `protos`) so the method paths cannot collide, and
`over_the_wire.rs` proves an unmodified the legacy controller client and the native API drive
the same session over one socket. `SessionController` implements `MediaControl`
for `Arc<Self>` so both services can hold it.

Gap: session creation passes an empty `call_id`/`from_tags`, because the legacy verb API
callers only know the channel uuid — the OpenSIPS→Redis discovery map (M2, open)
is what resolves those, and until it exists a TelCompat-created session cannot
actually tap.

### tap_plane.rs — the control plane's hands in the media world

`TapPlane` implements `control_api::MediaPlane` over the machinery the
Phase-0 spike proved, which is what turns `MediaControl` from a registry
into something that actually taps calls.

- `open_session` does the real NG dance — bind transport, `subscribe
  request`, parse the offer, bind one UDP socket per stream, `subscribe
  answer` — then builds a `TapLeg` per stream, starts a `Hub`, and spawns
  the capture thread. The session is only recorded once all of that
  succeeded, which is what makes the controller's rollback meaningful.
- **Managed taps retain no local audio.** `TapLeg` sizes its capture buffer
  from the duration it is given, so the spike's WAV-shaped sizing would
  allocate hundreds of megabytes per leg for a call-length session. Managed
  sessions pass `RETAIN_NO_LOCAL_AUDIO` (zero), which floors at one second
  and then reports `capture_full` — audio goes to consumers through the hub,
  not into a buffer nobody reads.
- The capture loop is bounded by `MAX_SESSION_DURATION` (8 h) as well as by
  its stop flag, so a session whose `DestroySession` never arrives cannot
  pin a thread forever.
- `open_attachment` serves `WS_TWILIO` and (since 2026-08-20) `GRPC_STREAM`;
  `FILE_S3` and `RTP_INLINE` are refused **by name** rather than silently
  accepted and ignored. Metadata carries `accountId`/`streamSid` through to
  the Twilio `start` frame, and the `TrackSelector` becomes both the hub's
  `TrackSelection` and the `tracks` list the consumer is told about.
- **A grpc-stream attachment is two-phase**: `Attach` records it (validating
  the session is tapped here and `ConsumerEncoder` serves the format —
  g711 at the tap rate or L16 at 8k/16k/48k since 2026-08-22, Opus refused
  by name), and the hub subscription only happens when the consumer's
  `ConsumerHello` arrives and `open_stream` runs. A pump task converts
  `TapEvent`s into `StreamFrame`s over a bounded channel; either side going
  away ends the pump, which drops the hub subscription. One connected
  consumer per attachment at a time — a second `Subscribe` while one is live
  is refused, and a reconnect after it drops is allowed, which is what makes
  a consumer restart survivable without re-attaching. `send_text` reaches a
  connected grpc consumer as a `TextFrame`.
- `send_text` reaches the far end through a bounded channel added to
  `consumer_ws::run`; blob playback is refused above
  `MAX_PLAYBACK_BLOB_BYTES` because one NG datagram cannot carry it (the
  lab's `EMSGSIZE` finding), and streaming playback names itself as
  Phase-3 work.
- **Leg identity: SOLVED by SSRC correlation plus elimination (2026-08-17),
  verified per-track on live calls.** The chain of evidence, including two
  wrong turns, is worth keeping:
  1. Stream order in a multi-tag subscription genuinely swaps between calls
     (measured: the same speaker arrived on stream 0 in one run and stream 1
     in the next), and 14.1.1.8 returns no `a=label`. Positional naming is a
     coin flip.
  2. Per-tag subscriptions were tried and reverted; the probe that condemned
     them answered with a payload type never offered, so its call-death and
     flood findings are contaminated — but per-tag remains unnecessary.
  3. **A subscription leg carries what the participant SENDS, stamped with
     the sender's SSRC** (fresh direct measurement: the speech-carrying leg
     bore the synthetic caller's hardcoded SSRC). An earlier "carries what
     the participant hears" conclusion — and a day of confusion — traced to
     the lab recorder's `RIGHT_TRACK: mixed` env (left over from the
     voice-AI demo), which silently mapped the right channel of every
     analysis to the injection track. Channel-based verdicts through that
     recorder are void; `lab/track_dump.py` now exists to analyse by track
     name so a mock's env can never skew evidence again.
  4. `query` returns `tags[].medias[].streams[].SSRC`, and in every daemon
     run it matched what arrived on one of the legs (`unknown_ssrc=None`
     throughout) — even when transcoding re-stamped a leg with a generated
     SSRC, query reported the generated one.
  So: `speaker_ssrcs` (tap_plane) maps each from-tag's SSRC to that
  participant's own track (`from_tags[0]` = Customer); `TapLeg` resolves on
  the first accepted audio packet; and `settle_by_elimination` names the
  second leg of a two-leg tap once the first is recognized, covering the
  transcode-SSRC case. Unmatched SSRCs are counted (`unknown_ssrc`) and the
  leg keeps its positional default. Acceptance: two consecutive live calls,
  caller's voice on `inbound` (rms 564) and silence on `outbound` (rms 6),
  independent of stream order. Remaining soft spot: a mid-call SSRC change
  (re-INVITE, transfer) re-resolves nothing yet — `ssrcs_seen` in the leg
  stats makes it visible when it happens.
- **Known gaps:** no Redis registry, so a tap lives and dies with its pod;
  `stop_playback` stops everything on the call rather than one playback,
  because rtpengine's `stop media` targets a participant, not a playback id;
  `close_attachment` aborts the consumer task rather than closing the
  websocket politely.

### main.rs — how the daemon chooses what to be

Three modes, in priority order: the Phase-0 tap spike when its env vars are
set (unchanged scaffolding), the **control plane** when
`MSS_CONTROL_LISTEN` is an `ip:port`, and otherwise an idle process that
waits for a signal. `MSS_RTPENGINE_NODE` becomes the default node for
sessions that do not name one, `MSS_TAP_LOCAL_IP` the media address, and
`MSS_POD_NAME` the `owner_pod` reported by `DescribeSession`.

Verified against the running binary, not just in tests: `CreateSession`
toward an unreachable rtpengine returns
`Unavailable: subscribe request: no reply from rtpengine ... after 3
attempts`, and the follow-up `DescribeSession` returns `NotFound` — the
rollback works in the daemon, not only against the test fake.
`crates/control-api/examples/mss_ctl.rs` is the small client used for that
and is the quickest way to poke a running control plane by hand.

### hub.rs — the fan-out core (M3), first increment
The per-session pub/sub the roadmap calls the fan-out hub. Two-worlds
shape: the capture thread owns the consumer list and is the only thing
that touches it; attach/detach arrive over a bounded lock-free command
queue (`crossbeam` `ArrayQueue`) polled once per tick, so the media world
never takes a lock and never waits on the control world.
- Each consumer gets its own bounded frame ring with **drop-oldest**
  semantics (`ArrayQueue::force_push`) — a slow consumer loses its own
  oldest frames, never anyone else's, and every displaced frame increments
  a per-consumer `dropped_oldest` counter (Article VIII: drops are
  first-class, never silent). `delivered` counts what actually queued.
- `TrackSelection` filters at the hub, so a Customer-only consumer costs
  nothing on the Agent leg. The single-track default and
  `MSS_CONSUMER_TRACKS=both` behavior carried over unchanged.
- `Subscription::next()` is async and cancel-safe (pop-then-wait against a
  `Notify`; a permit stored by a racing publish is consumed on the next
  poll). Hub drop or detach closes the subscription: `next()` drains what
  is queued, then returns `None`, which is what tells the WS consumer to
  send `stop`.
- Attach is command-queue-bounded; a full queue refuses the attach rather
  than blocking anyone. `crossbeam-queue` is the one new dependency
  (Article XI: adopted lock-free structure, not hand-rolled).
- **Injected bot speech is a hub track.** rtpengine cannot tap the media
  player (measured: an `egress` subscription mirrors the peer stream but
  not the player), so the injection path publishes the exact PCM it hands
  rtpengine. `HubClient::inject` queues an utterance; the capture loop
  releases it one ptime frame per tick as `Track::Mixed`, with the frame
  clock advancing every tick whether or not audio is pending, so timestamps
  are wall-aligned and a recorder placing frames by timestamp gets both
  sides of the conversation in real time. Consumers opt in by track:
  the ASR-feeding bridge subscription stays Customer-only, which is also
  what keeps bot speech out of its own transcription loop.
- `MSS_LISTENERS=name=url,name=url` attaches N listen-only Twilio-dialect
  consumers per tap; the lab wires an RTT service and a recorder this way
  alongside the interactive bridge.
- ✅ Per-consumer codec/resample pipelines landed 2026-08-22: `TapEvent`
  now carries **PCM samples**, not µ-law bytes, which is what makes the
  "L16 interchange, one decode per ingest, N encodes per consumer"
  cross-cutting decision physically true instead of aspirational. The WS
  bridge µ-law-encodes at its edge (the frozen dialect's byte-exact tests
  did not move); the gRPC pump encodes per attachment format with a
  `ConsumerEncoder` per track (media-core encode.rs). The gRPC
  `MediaStream` adapter and exported hub metrics landed 2026-08-20 —
  `SubscriptionMetrics` is the cloneable handle over a subscription's
  queue depth and drop/delivery counters that the metrics endpoint reads.
  Opus output remains open (tasks item 9).
- **The hub is what architecture.md §5 calls an Attachment set**, and the
  spike already prefigures two of its rules: capability is structural (the
  voice-AI consumer is constructed with a command channel, listeners with
  `None`, so only it can inject) and track selection happens at the hub.
  What the M4 control plane must add on top: an `attachment_id` on every
  event, exactly one `authoritative` attachment per session (§5.3), and
  `external_id` carried from `CreateSession` onto every event so Kafka can
  key by the legacy controller's `request_uuid`.

### consumer_ws.rs — the first consumer bridge, and the speech path back
- WebSocket client speaking the frozen Twilio dialect from
  `protocol::twilio`, so a the legacy media gateway-compatible endpoint accepts it
  unchanged. `MSS_CONSUMER_URL` turns it on; without it the tap behaves
  exactly as before. Since the hub landed it is just another subscriber:
  it consumes a `hub::Subscription` and reports the hub's per-consumer
  `dropped_oldest` as `media_dropped`.
- Inbound audio accumulates until the utterance ends, then goes out as
  `BridgeCommand::Speak`; `clear` discards the buffer and raises
  `BridgeCommand::Barge`. `tap_session::inject_bridge_speech` wraps the
  utterance as a WAV blob and plays it with `play media`, targeted by
  default at the first from-tag so only the customer hears the agent.
  `MSS_INJECT_TARGET=everyone` widens it.
- **The end of an utterance is inferred, not signalled.** stream-llm-bridge
  streams TTS as a run of `media` events and never sends `mark`, so the
  consumer flushes after `UTTERANCE_IDLE` (700 ms) without inbound audio.
  `mark` and `endOfInteraction` still flush immediately when they do arrive.
- **Underruns feed the consumer silence, not nothing.** A
  silence-suppressing caller (MicroSIP) stops sending RTP between
  utterances; skipping those frames made Deepgram time out and close. The
  Twilio dialect implies a continuous stream.
- **Only the Customer track streams by default.** stream-llm-bridge feeds
  every media packet to the ASR regardless of `track`, so sending both legs
  interleaves two sources at double rate and transcribes as nothing.
  `MSS_CONSUMER_TRACKS=both` restores dual-track; the `start` event's
  `tracks` list reflects what is actually sent.
- **`play media` blobs are capped by the UDP datagram size.** NG is UDP, so
  anything over ~64 KB fails with `EMSGSIZE`; that is roughly 4s of 16-bit
  8 kHz WAV. Utterances are split into `BLOB_SAMPLES_PER_DATAGRAM` pieces
  played back to back, sleeping each piece's duration so a piece does not
  truncate the one before it. `play media {file}` avoids the cap but needs
  storage shared with the rtpengine host.
- The dialect values are the ones the real bridge accepts, verified with
  `lab/bridge_probe.py`: `encoding` is `PCMU` (not `audio/x-mulaw`) and
  `media.timestamp` is a **string**. A numeric timestamp makes the bridge
  answer `invalid_json` and close the connection.
- Utterance-shaped, so latency is one whole utterance. Barge-in is a
  `stop media`, and its cut-through time is still unmeasured — but a barge
  arriving mid-playback no longer waits out the piece pacing sleep:
  `wait_out_piece` keeps receiving commands while a piece plays, stops the
  playback immediately on `Barge`, and queues anything else. Artifact
  writes (WAV, datagram logs) run under `block_in_place` so the control
  runtime is never blocked on the filesystem.
- **Bot speech is not observable in the tap**, because a subscription
  carries what a party sends and injection reaches what it hears. Verify at
  the endpoint (`lab/out/caller_ear.wav`), not in `tap.wav`.
- Plain `ws` only: `tokio-tungstenite` is built without TLS. A `wss`
  endpoint needs a TLS feature and a rustls review against `cargo deny`.

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

### supervisor.rs — watchdog logic, wired 2026-08-20
`AudioFlowWatchdog` (touch/check, sticky stall origin) is driven per tap
leg from the capture loop and exported through the metrics endpoint (see
the metrics.rs section above). Stall-triggered re-subscribe remains
future work.

### main.rs — boots both worlds
The control-plane pieces are all wired: `MediaControl` + `TelCompat` +
`MediaStream` on `MSS_CONTROL_LISTEN` (auth via `MSS_AUTH_TOKEN`), the
Kafka event pump on `MSS_KAFKA_BROKERS`, the Redis registry keeper on
`MSS_REDIS_URL`, the metrics endpoint on `MSS_METRICS_LISTEN`, graceful
drain on ctrl-c. Still open: billing-topic producers for phase 3
(`LEGACY_MEDIA_GATEWAY_BILLING_TOPIC` / `KAFKA_VOICE_AI_AGENT_TOPIC` schemas).

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
  N encodes per consumer format. Realized 2026-08-22: the hub publishes
  PCM and every consumer bridge encodes its own output (encode.rs).
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
- **Dependabot merges can silently revert workflow edits.** The
  `@1.95.0` pin was dropped once already: a dependabot PR branched from an
  older `main` bumped `actions/checkout` on adjacent lines, and its merge
  kept its own copy of `ci.yml`. After any dependabot merge that touches a
  workflow, `grep -n 'rust-toolchain@' .github/workflows/*.yml` is worth
  one second of checking.
- **Dependabot no longer proposes `dtolnay/rust-toolchain` bumps**
  (`.github/dependabot.yml` ignore rule, plus an `@dependabot ignore this
  dependency` on PR #14). For that action the tag is the Rust version, so
  the bump is a toolchain upgrade wearing an action-update costume — and
  PR #14 proposed 1.100.0 while static.rust-lang.org still 404s it (the
  action's tags run ahead of actual Rust releases), so every toolchain job
  failed at setup. Toolchain bumps stay manual and move
  `rust-toolchain.toml` and the workflow refs together.
- **There is deliberately no `cargo audit` job.** `cargo deny check all`
  already reads the same RustSec advisory database, so the second tool
  added no coverage — but it did break repeatedly for a reason unrelated to
  our dependencies: `rustsec/audit-check` runs `cargo install cargo-audit`
  *without* `--locked`, which resolves the tool's own deps to their newest
  versions, and one of them (`kstring 2.0.4`) requires rustc 1.96.0 while
  this repo pins 1.95.0. The install aborted before checking a single
  advisory. If cargo-audit is ever wanted back, it needs `--locked` and a
  toolchain decoupled from the repo pin — the tool's build compiler has
  nothing to do with our MSRV, since auditing only reads `Cargo.lock`.
- `criterion` is a dev-dependency with `default-features = false`, which
  drops plotters/rayon and keeps the added third-party crate count and
  license surface small. All 77 third-party crates resolve to a license in
  the `deny.toml` allowlist (several use the legacy `MIT/Apache-2.0`
  spelling, which cargo-deny normalizes).
- Workspace crates are `publish = false`; cargo-deny ignores private
  crates for licensing and allows wildcard *path* deps only.
- Dockerfile builds only `mediaserverd` and ships distroless nonroot. It
  copies `Cargo.lock` and builds `--locked`, so the image ships exactly
  the dependency set cargo-deny checked, advisories included;
  without it the image re-resolved dependencies and could ship versions
  CI never saw.
