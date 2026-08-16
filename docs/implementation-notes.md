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
- **M3:** the WS consumer bridge must also replicate the the upstream platform-forked
  mod_audio_fork dialect. **Open item:** the fork's exact wire format is
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
  `close_attachment`, `send_text`, `start_playback` / `stop_playback`. It is
  async because opening a tap means an NG round trip to rtpengine.
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

### Known gaps (M4)

- `WatchEvents` is the debug path only. The production event path is Kafka
  `mss.events` (§5.4) and has not been built yet — `SessionController`
  currently publishes events only to its in-process broadcast.
- `owner_pod` is whatever string the controller was constructed with; real
  placement and Redis ownership leases are still ahead.
- No auth interceptor yet. `ConsumerHello.token` exists in the data-plane
  proto and nothing verifies it.
- The `MediaStream` data-plane service is generated but not implemented;
  only `MediaControl` is served.

## crates/mediaserverd

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
- `open_attachment` serves `WS_TWILIO` only; every other transport is
  refused **by name** rather than silently accepted and ignored. Metadata
  carries `accountId`/`streamSid` through to the Twilio `start` frame, and
  the `TrackSelector` becomes both the hub's `TrackSelection` and the
  `tracks` list the consumer is told about.
- `send_text` reaches the far end through a bounded channel added to
  `consumer_ws::run`; blob playback is refused above
  `MAX_PLAYBACK_BLOB_BYTES` because one NG datagram cannot carry it (the
  lab's `EMSGSIZE` finding), and streaming playback names itself as
  Phase-3 work.
- **Leg identity: one subscription per participant (2026-08-16/17).** The
  first version asked for both `from-tags` in a single `subscribe request`
  and named the resulting streams by position. Against 14.1.1.8 that is not
  sound: rtpengine returns **no `a=label`** on the streams and the order does
  not follow the order of the tags requested. `subscribe_one_leg` now makes
  one subscription per tag, so the tag we asked for is the only thing on the
  socket and identity is never inferred from ordering. If a later leg fails,
  the earlier ones are unsubscribed before the error returns.
- **Two lab findings came out of that, both measured, one of them ours:**
  1. *Every answer needs its own SDP session id.* The per-leg answers
     initially reused `sdp_session_id`, and rtpengine then delivered roughly
     twice the datagrams to one leg and silence to the other — consistent
     with it treating identical `o=` lines as one session. Each leg now
     answers with `sdp_session_id + index`, and the two legs immediately
     came back with matched counts (2609 vs 2664).
  2. *A subscription carries what that participant **hears**, not what it
     says.* With the counts even, the subscription made with the caller's
     tag was silent while the one made with FreeSWITCH's tag carried the
     caller's voice, across three runs. FreeSWITCH was genuinely silent
     (the caller's own ear recording has zero voiced samples), so both sides
     agree. `voice_the_participant_hears` encodes this: the leg subscribed
     with participant *i*'s tag is named for the **other** party's voice.
     This contradicts the older architecture.md §6 note that `play media`'s
     `from-tag` picks who *hears* the audio — an injection aimed at the
     caller showed up on the FreeSWITCH-tag subscription — so one of the two
     readings is wrong and §6 should not be trusted until re-probed.
- **Not yet proven end to end, and it must be before Phase 2 trusts stereo.**
  Media delivery on per-tag subscriptions is **intermittent**: the same code
  and the same call shape produced 2664 datagrams of speech on the second
  leg in one run and 0 in the next, and a single-tag session delivered
  nothing at all. Until that is understood, the naming above rests on the
  runs where media flowed. The next step is a dedicated probe in the
  `lab/ng_*_probe.py` style — subscribe per tag repeatedly against one call,
  with both parties producing distinguishable audio — rather than more
  guessing from the daemon.
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
- Still to come in M3: per-consumer codec/resample pipelines (G.711 →
  L16 → 8k/16k), the gRPC `MediaStream` adapter, and hub metrics exported
  rather than logged.
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
- **Dependabot merges can silently revert workflow edits.** The
  `@1.95.0` pin was dropped once already: a dependabot PR branched from an
  older `main` bumped `actions/checkout` on adjacent lines, and its merge
  kept its own copy of `ci.yml`. After any dependabot merge that touches a
  workflow, `grep -n 'rust-toolchain@' .github/workflows/*.yml` is worth
  one second of checking.
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
