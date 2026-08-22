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

### jitter.rs — hardened 2026-08-22 (tasks item 17, defect D7)
Current: adaptive target depth from observed inter-arrival jitter,
timestamp-aware gap classification, explicit stream restart, 64-slot ring,
G.711-sized slots (480 B), duplicate/late/loss/reset/silence accounting,
wraparound-safe.

Two constructors, deliberately:
- `JitterBuffer::new(target)` is the **fixed-depth, timing-agnostic** form
  (`timestamp_increment: 0`, ceiling == floor). Adaptation and gap
  classification are inert, which is why every playout-semantics test below
  still reads exactly as it did before the hardening.
- `JitterBuffer::with_config(JitterConfig)` is what `StreamPipeline` builds:
  floor = the caller's `target_depth_packets`, ceiling = 4× that (hard-capped
  at `CAPACITY / 2` so the reorder grace can never deadlock playout), and
  `timestamp_increment` = samples per packet.

Timing is a **parameter, never a clock** (Article II): `push_timed` takes
`Timing { timestamp, arrival_ticks, marker }`, where `arrival_ticks` is a
monotonic count in the stream's own sample-clock units. `StreamPipeline`
converts the caller's arrival micros; `pipeline.ingest` without an arrival
passes `arrival_ticks = timestamp`, which is exactly the statement "this
replay models a perfectly paced network" — one code path, zero jitter
measured, gap classification still live.

- **Adaptive depth.** RFC 3550's interarrival estimate
  (`J += (|D| - J)/16`, `D` = arrival delta minus timestamp delta) is kept
  in `Stats.jitter_ticks`. The target is
  `floor + ceil(3·J / timestamp_increment)`, clamped to the ceiling. It
  **rises immediately and shrinks one packet at a time** after 250
  consecutive packets that wanted less (`the_target_depth_shrinks_only_after_a_long_quiet_spell`);
  raising the target never re-primes, so growing the cushion costs an
  underrun, not a re-priming gap. Only packets that advance `max_seq` feed
  the estimate: reordered arrivals would inflate it, and telephone-event
  packets freeze the RTP timestamp during a press, which would blow it up.
- **Timestamp-aware gaps.** When a sequence gap opens, the buffer asks
  whether more time elapsed than the missing packets could carry:
  `unexplained = (ts - last_ts) - span·increment`. If `unexplained` is a
  whole packet or more (or the marker bit says talkspurt-start and any time
  is unexplained), the skipped sequence numbers are filled as silence,
  counted in `Stats.silence_gaps`, and `push_timed` returns
  `BufferedAfterSenderSilence`; loss is untouched. Otherwise the gap is
  loss, exactly as before. Pinned by
  `a_gap_the_timestamps_explain_is_silence_not_loss` and
  `a_gap_the_timestamps_do_not_explain_is_still_loss`.
- **`restart()`** is the explicit stream discontinuity, used by the pipeline
  on an SSRC change. It re-anchors on the next packet, so a new sender at a
  *nearby* sequence number no longer produces a run of `TooLate` drops (the
  defect item 14 handed over); it counts a `reset`, so
  `mss_jitter_resets_total` and `mss_legs_ssrc_changes_total` keep moving
  together. The cost is the same as the old sequence-jump reset: audio
  already buffered for the old sender is dropped
  (`a_new_ssrc_at_a_nearby_sequence_restarts_instead_of_dropping_late`
  pins the count).

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
- ~~**M2:** adaptive target depth~~ — landed 2026-08-22.
- ~~**M2:** timestamp-aware gap handling~~ — landed 2026-08-22.
- ~~**M3:** PLC hook on `PopOutcome::Lost`~~ — landed 2026-08-22 (plc.rs).
- **M3:** slot sizing revisit when Opus lands (payloads up to ~1275 B).
- **Still open:** the adaptive ceiling is a multiple of the configured floor
  rather than a millisecond budget, and nothing yet *reports* the chosen
  depth per leg beyond `Stats.target_depth` (exported only in the
  `tap leg finished` line, not as a gauge).

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
  mediagateway's codec mismatch silently passed garbage bytes through.
- Loss is `Playout::Concealed`, counted separately from `Playout::Pcm`, so a
  caller writing a recording keeps timing while the metric still says "this
  was a gap, not audio". Since 2026-08-22 the fill is **real PLC** (plc.rs),
  not silence — `conceals_a_dropped_packet_with_plc_audio_and_counts_it`.
- **Comfort noise (payload type 13, RFC 3389/3551 static) is accounted, not
  decoded**: the sequence number is occupied and played out as
  `Playout::Suppressed` silence, so a caller that suppresses audio during a
  silence period neither inflates `jitter_lost` nor collapses the recording's
  wall-clock timing. Counted as `PipelineStats.comfort_noise`. This is the
  same trick that closed the DTMF-inflates-loss finding, applied to the other
  in-band non-audio payload we actually see.
- **Arrival time comes from the caller.** `ingest_at(datagram, micros)` is
  the production entry point (the tap leg passes monotonic micros since the
  leg's epoch, read once per datagram); `ingest(datagram)` is the replay
  entry point and means "arrived perfectly paced". The pipeline converts
  micros to the stream's sample-clock ticks — media-core still owns no clock.
- **An SSRC change restarts the buffer and forgets the PLC history.**
  `last_audio_ssrc` is still assigned **before** the push (item 14's
  invariant: leg identity survives a restart), and the first packet of the
  new sender reports `IngestOutcome::Resynchronized` whatever its sequence
  number. Two interleaved SSRCs on one leg will therefore restart per packet
  — that pathology already produced garbage before this change (see the
  dual-SSRC note in lab.md); it is not made worse, and the leg-renaming
  hysteresis lives in `tap_spike.rs`, not here.
- A talkspurt that resumes after sender silence also forgets the PLC
  history, so concealment can never resurrect audio from before a pause.
- Known limitation, pinned by
  `reorder_before_playout_starts_strands_the_earlier_packet`: playout
  anchors on the first packet that arrives, so if the first two packets of
  a stream arrive swapped, the earlier one is a late drop. **M3
  candidate:** allow `play_seq` to rewind while still priming, which would
  make start-of-stream reorder recoverable. Deferred because it changes
  jitter admission on the sacred path and wants its own benchmark.

### plc.rs — packet loss concealment, G.711 Appendix I shape (landed 2026-08-22)

`PacketLossConcealer` is the concealment G.711 Appendix I describes, adopted
as an algorithm rather than invented: keep a history of recent output, find
its pitch period, repeat that period for as long as the gap lasts, hold the
first 10 ms unattenuated, fade linearly to silence by 60 ms, and cross-fade
back into the first real frame. Article XI says adopt DSP rather than
reimplement it; this is arithmetic (AMDF search, integer gain ramp, linear
cross-fade) with no codec table in it, so it is implemented in-tree from the
published algorithm's shape — the same shape spandsp's `plc.c` carries.

- **Fixed buffers, no allocation on the packet path**: 100 ms history ring
  (800 samples), a 320-sample linear search window, a 160-sample period
  buffer. Pitch search runs **only on the first frame of a gap**: AMDF over
  a 20 ms span against lags of 40–160 samples (200 Hz–50 Hz). The window is
  copied out of the ring first so the inner loop indexes linearly instead of
  paying a modulo per sample.
- Pitch bounds and the fade points are derived from the format's sample rate
  and clamped into the fixed arrays, so an odd rate degrades rather than
  panicking (`unusual_sample_rates_stay_inside_the_fixed_buffers`). G.711 is
  8 kHz by definition; the constants are chosen for it.
- Concealed output is fed back into the history (spandsp does the same), so a
  long burst keeps a coherent signal to repeat and the fade is monotone.
- `forget()` is how the pipeline says "the signal before this point is not
  continuous with what comes next": comfort noise, a DTMF suppression frame,
  a talkspurt resuming after silence, a reset, an SSRC change.
- Tests pin each clause of the algorithm:
  `conceals_by_repeating_the_detected_pitch_period` (unattenuated head is a
  sample-exact repeat), `a_burst_fades_to_silence_by_sixty_milliseconds`,
  `the_first_frame_after_a_gap_is_crossfaded_not_spliced`,
  `with_no_history_concealment_is_silence`.
- **Not done:** no listening test yet, and no comparison against a reference
  Appendix I implementation's output. What is measured is the algorithm's
  structure, not its perceptual quality; the ASR-as-judge probe
  (`lab/ear_intelligibility_probe.py`) under `tc netem` burst loss is the
  cheap next step.

### replay.rs — the packet-replay harness (Article III)
- `G711StreamGenerator` emits deterministic G.711 streams whose payload
  bytes are a counter, so decoded output is exactly assertable; it can
  also emit telephone-event packets and `skip_one` to leave a real
  sequence gap.
- `disturb(datagrams, script)` applies `Deliver / Drop / DeliverTwice /
  DelayOne` to model loss, duplication and reorder in one reusable place.
- Added for the jitter hardening (item 17): `suppress_silence(packets)`
  advances the RTP timestamp without the sequence number and sets the marker
  bit on the next packet — a sender going quiet and starting a new talkspurt;
  `next_comfort_noise_datagram(level)` emits a payload-type-13 packet.
  `disturb` deliberately still models *order and multiplicity only* — arrival
  time is a per-packet argument to `ingest_at`, so a test that needs modelled
  jitter supplies its own arrival schedule (see
  `arrival_jitter_without_loss_grows_the_cushion_and_plays_everything`).
- `pipeline.rs`'s `impairment_matrix` test module drives these against a
  **lag-based pacer**: playout trails the arrival clock by the target depth,
  which is what a wall-clock pacer does. A one-release-per-datagram loop is
  *not* equivalent — it gives a duplicate its own release slot and drains the
  cushion, which made duplication look like late arrival. If a new impairment
  test reports impossible counters, check the pacer model first.
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
  dividing by zero (mediagateway crashed on `a=ptime:0`).

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
- Unlike mediagateway's fire-and-forget MI client, every request must
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
  (`start/media/dtmf/mark/stop_matches_mediagateway_bytes`), matching the
  `fork_events` style. Previously only `Media` was covered, and only by
  field spot-checks — a frozen contract with four untested shapes.
- **`mark` carries no `sequenceNumber`; `start`/`media`/`dtmf`/`stop` all
  do.** Confirmed against the mediagateway source: its mark echo is
  `{"event":"mark","streamSid":...,"mark":{"name":...}}`. The asymmetry is
  the contract, not an oversight, and
  `mark_omits_sequence_number_while_start_media_dtmf_stop_carry_it` exists
  to fail on anyone "fixing" it.
- `customParameters` is omitted entirely when empty and emitted when
  present; byte-exact tests cover both. Multi-entry maps have no
  deterministic order, so byte-exact tests use at most one entry.
- **M3:** the WS consumer bridge must also replicate the reference
  deployment's forked mod_audio_fork dialect. **Open item:** the fork's exact wire format is
  specified only in the fork's C source (not in this repo, not in cigol);
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
  drives `streamfsm`. Detaching frees the role for a successor.
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
  **The recording variants have a raiser since 2026-08-22** (M5): the
  recorder reaches `observe` through `control_api::ObservationSink`, and
  `RecordingPaused{paused, duration_ms}` was added for the frozen
  `recordPause` callback. `Observation::Dtmf` is still unraised — the
  pipeline reports digits into the hub as `TapEvent::Dtmf`, and nothing
  turns those into a `MediaEvent` yet.

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
- **Proved on a live call 2026-08-22** (tasks item 10), not only against the
  fake plane: `crates/control-api/examples/mss_stream_probe.rs` is the
  reference consumer — attach, `ConsumerHello`, decode, one wav per track —
  and `lab/grpc_stream_drill.sh` runs it against a real tapped SIP call. The
  customer track came out at rms 614.5 and Deepgram transcribed it verbatim.
- **Known wart found by that run (D13):** `start_message` derives its
  `tracks` list from the selector, so `TrackSelector::All` advertises
  `["customer","agent"]` — but the hub also delivers `Track::Mixed` (the
  injection feed, silence-filled every tick so it stays gap-free), so a
  consumer receives a track it was never told about and 50% more bytes than
  the start frame implies. The same `tracks_of` shape feeds the frozen Twilio
  `start` frame, which is why it was left alone here rather than "fixed" in
  passing.

### auth.rs — the shared-secret policy (M4, landed 2026-08-20)

`AuthPolicy` carries an optional shared secret (`MSS_AUTH_TOKEN` in the
daemon). As a tonic interceptor it guards `MediaControl` (`authorization:
Bearer …`, constant-time comparison, `UNAUTHENTICATED` otherwise); the same
policy checks `ConsumerHello.token` on the data plane. Unset means open —
the lab mode — and the daemon logs that loudly at startup.

**`TelCompat` is deliberately not intercepted.** Its whole contract is that
an unmodified cigol client works byte-for-byte; cigol sends no auth
metadata, so intercepting it would break the no-client-change property.
The pilot fronts that surface with network policy; if cigol ever grows an
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

Since the jitter hardening (item 17) there is also
`mss_jitter_silence_gaps_total` — sequence numbers a sender's silence
explains. It has no alert rule on purpose: it is normal traffic on any leg
whose endpoint does silence suppression. Its value is that
`MssJitterLossHigh` got *quieter and more honest* — those gaps used to land
in `mss_jitter_lost_total`.

The SSRC re-resolution series (2026-08-22) are meant to be read as one
sequence: `mss_legs_ssrc_changes_total` (a leg's sender changed),
`mss_ssrc_requeries_total` (the control world asked rtpengine about it),
`mss_legs_ssrc_reresolved_total` (a refreshed map renamed a leg), and the
gauge `mss_legs_unknown_ssrc`, which is the one that must come back **down**.
`MssLegSpeakerUnresolved` therefore alerts on the gauge holding for 2 m —
the change itself is normal, a change that never recovers is not.

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
- **Bounds and honesty**: handoff queue of 1024, counted drops with a
  warning; broker failures are counted and logged, never fatal. Totals
  logged at shutdown. Delivery durability is described below.
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
  verification tool and the reference for cigol's translator.
- Fixed in passing: the Dockerfile never copied `proto/`, so the image
  build had been broken since control-api landed.

#### Delivery durability — defect D5 closed, semantics now at-least-once (2026-08-22)

The pump used to publish each event once and count the failure; a broker
blip was a permanent, silent-except-for-a-counter hole in a session's event
stream. It now keeps a **single FIFO backlog** in the worker and retries the
head until it lands:

- **Order first.** The head is retried before any newer event is attempted,
  so nothing is ever reordered — globally, therefore per session too. The
  cost is head-of-line blocking across sessions during an outage, accepted
  deliberately: the alternative (per-session queues) buys throughput that
  low-rate lifecycle events do not need.
- **Backoff** doubles from 100 ms to a 5 s cap. While waiting, the worker
  keeps draining the handoff queue in the same `select!`, so `accept` stays
  non-blocking and the drop decision belongs to the backlog, not the
  channel.
- **Bounded, counted, oldest-first.** The backlog caps at 8192 events;
  beyond that the **oldest unsent** event is evicted and counted
  (`dropped_oldest`). Evicting the head resets the attempt counter. Loss is
  still possible under a long enough outage — but it is bounded, counted,
  alerted, and visible downstream as a seq gap.
- **A send that never answers** is a failure: each attempt is wrapped in a
  5 s timeout, because rskafka's produce can otherwise park the worker for
  as long as the broker's TCP stack allows.
- **Shutdown** is bounded too: `main` waits up to 10 s
  (`await_empty_backlog`) for the backlog to drain before aborting the
  worker, and the worker itself gives up after `shutdown_attempts` (3)
  failures once the inbox has closed, counting the remainder as
  `abandoned`. Without that bound a permanently dead broker would keep the
  worker alive forever.
- **Tuning** lives in `PumpTuning` (`start_tuned`) so tests can use
  millisecond backoffs and a 4-event cap; production uses `Default`.

**Semantics shipped: at-least-once.** A retry after an ambiguous failure
(timeout, connection reset after the broker committed) can duplicate a
record. **The downstream translator must treat `(external_id, seq)` as
idempotent** — it does not dedupe today; it relies on the gapless seq, which
is exactly the key it needs. Duplicates were not observed in the lab drill
(0 of 60), but the guarantee is at-least-once, not exactly-once, and a
consumer that acts twice on one `PlaybackFinished` would be acting on our
guarantee, not on chance.

New counters, all exported by `metrics.rs` and alerted in
`deploy/prometheus-alerts.yaml`: `mss_events_retried_total`,
`mss_events_dropped_oldest_total`, `mss_events_abandoned_total`, and the
gauge `mss_events_retry_depth`. `mss_events_failed_total` changed meaning:
it now counts failed **attempts** (each retried), not lost events.

Verified against a real broker: `lab/event_outage_drill.sh` sends 60 events
at 1/s through the production `RskafkaTransport` while Redpanda is
`docker stop`ped for 30 s in the middle. Result (2026-08-22):
`accepted=60 published=60 failed=6 retried=6 dropped=0 dropped_oldest=0
unsent=0`, and 60 distinct seqs 0-59 on one partition at contiguous offsets
— confirmed independently with `mss_events_tail`. Those 6 failed attempts
are exactly what the old code would have lost. The drill is
`crates/mediaserverd/tests/kafka_outage.rs`, skipped unless
`MSS_TEST_KAFKA_BROKERS` is set (same pattern as the Redis integration
test). Unit tests cover what the lab cannot schedule: an outage that
outlasts the cap (drop-oldest keeps the newest survivors in order), a
transient refusal retried without a duplicate landing, a send that never
answers, and the shutdown give-up path.

What is still not durable: events dropped by the controller's outbox or by
the handoff queue never reach the backlog, and the backlog is in memory
only — a pod that dies with a backlog loses it. Disk-backed spooling was
not built; the pilot's bar is surviving a broker restart, not a pod loss
with the broker down.

### Resolving a call's participants without the discovery map (2026-08-17)

The blocker on any cigol integration was that a TelCompat caller knows only
the FreeSWITCH channel uuid, while a tap needs the SIP call-id and the
participants' from-tags — filed for months as "waiting on OpenSIPS to write a
discovery map to Redis".

It turns out not to need one. cigol already has both facts on the channel:
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

### telcompat.rs — cigol's verbs, served by MSS (§5.6)

The migration switch. `proto/telcompat.proto` declares
**`package protos; service TelService`** deliberately: gRPC routes on the
fully-qualified method path, so `/protos.TelService/StartStream` is
byte-identical to what cigol already calls, and a per-tenant flag can point a
client at `mssServer` instead of `telServer` with **no client change** and roll
back by pointing it back. Message shapes and field numbers are copied verbatim
from cigol's `telsvc.proto`.

Only the media subset is declared — stream, transcription, recording, playback
stop. Originate, answer, hangup, bridge, conferences and IVR prompting stay on
FreeSWITCH, and a client calling one of those here gets `UNIMPLEMENTED`, which
is the honest answer rather than a silent success.

The mapping (one test per row in `tests/telcompat.rs`):

| telsvc verb | MSS nouns |
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
  fork produces the speech events streamfsm runs on; a recorder is a `SINK`
  with no back-channel and must never claim them.
- **One session serves both.** `StartStream` + `StartRecording` on the same
  channel produce one tap with two attachments, which is the whole point:
  today those are two FreeSWITCH mechanisms.

Both surfaces share one controller and one port (`server.rs`): the packages
differ (`mss.v1` vs `protos`) so the method paths cannot collide, and
`over_the_wire.rs` proves an unmodified cigol client and the native API drive
the same session over one socket. `SessionController` implements `MediaControl`
for `Arc<Self>` so both services can hold it.

Gap: session creation passes an empty `call_id`/`from_tags`, because telsvc
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
- `open_attachment` serves `WS_TWILIO`, `GRPC_STREAM` (2026-08-20) and
  `FILE_S3` (2026-08-22, the recorder — see its own section below);
  `RTP_INLINE` is refused **by name** rather than silently accepted and
  ignored. Metadata carries `accountId`/`streamSid` through to
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
  independent of stream order.
- **Mid-call SSRC re-resolution (D1, 2026-08-22).** A leg no longer decides
  its speaker once and for all. `reresolve_speakers` is the control-world
  half: a per-session task that wakes every `SSRC_WATCH_INTERVAL` (500 ms),
  reads each leg's unresolved SSRC out of `SharedLegStats`, and — the first
  time it sees one it has not already asked about — re-runs `query` +
  `speaker_ssrcs` and publishes the refreshed map to every leg. `SsrcRequeries`
  is the rate limit and the reason this cannot become an NG flood: **one query
  per distinct unknown SSRC**, remembering 16 and forgetting the oldest. It is
  spawned only when the initial map is non-empty (an empty map means positional
  naming, which re-resolution cannot improve) and aborted in `close_session`
  alongside the capture thread's stop flag.
  Two limits worth knowing before trusting it on a transfer: `from_tags` is
  frozen at subscribe time, so a transfer that introduces a **new tag** yields
  an SSRC no map can name — that needs a re-subscribe, not a re-resolve — and
  the whole loop rests on rtpengine's `query` reporting the *current* SSRC for
  a tag after a change, which is measured at subscribe time but **not yet
  measured mid-call**. Probe it before claiming transfer support.
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
and is the quickest way to poke a running control plane by hand; it covers
`create`, `describe`, `attach` (ws), `record` (the `FILE_S3` identity),
`pause`, `detach`, `play` and `destroy`.
`crates/control-api/examples/mss_stream_probe.rs` is its data-plane sibling:
it attaches a `GRPC_STREAM` consumer, subscribes, and writes what it hears
as a wav per track with rms and peak — the tool the item-10 lab proof used.

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
  key by cigol's `request_uuid`.

### consumer_ws.rs — the first consumer bridge, and the speech path back
- WebSocket client speaking the frozen Twilio dialect from
  `protocol::twilio`, so a mediagateway-compatible endpoint accepts it
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
- **The serial is process-wide, not per-`CookieSequence` (2026-08-22, defect
  D12).** It used to restart at 0 in every instance, and `TapPlane` binds a
  **new transport per session** — so every session's first command carried
  cookie `<prefix>-0`. The same reply cache that makes a retransmit
  idempotent then made a *second session* idempotent with the first: two
  sessions inside rtpengine's cache window received the same cached
  `subscribe answer`, and the second tap listened to a subscription that no
  longer existed — `datagrams: 0` on every leg, looking for all the world
  like a network fault. A static `AtomicU64` shared by all instances makes a
  cookie unique per process by construction; the prefix still separates pods
  and restarts. Pinned by `two_transports_on_one_pod_never_share_a_cookie`
  and measured both ways in the lab (see lab.md, the gRPC drill).
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
- **Leg identity is now a small state machine (2026-08-22, D1).** Three
  fields carry it: `observed_ssrc` (the SSRC the current name came from),
  `candidate_ssrc` + `candidate_packets` (the challenger), and `ssrc_tracks`
  (a `SsrcTracks` — a fixed `[Option<(u32, Track)>; MAX_SSRC_TRACKS]`, `Copy`,
  so nothing allocates on the media thread). Rules, in order:
  1. The **first** SSRC on a leg resolves it on its first packet, as before.
  2. A **different** SSRC must arrive `SSRC_CHANGE_CONFIRMATIONS` (3) times
     consecutively to take over; any packet of the incumbent forgets the
     challenger. This is the anti-flap guard: two SSRCs interleaved on one
     leg — the dual-SSRC fault the echo-loop sessions found — would otherwise
     rename the leg per packet and scramble the hub's track fan-out.
  3. A confirmed SSRC the map does not name puts the leg back into the
     unresolved state (`unknown_ssrc = Some(new)`, `resolved_track() == None`)
     but **keeps the name it already had**. A stale-but-stable label beats a
     flapping one, elimination can still name it from the other leg, and the
     unresolved SSRC is what the control world reads to know it should
     re-query.
  4. A refreshed map arrives over a per-leg bounded `ArrayQueue<SsrcTracks>`
     (`ssrc_track_publisher()` for the control side, `poll_ssrc_tracks()`
     once per capture tick for the media side — the hub's command pattern,
     capacity 4, **drop-oldest** because a newer map supersedes an older one).
     Applying it re-runs the naming, which is how a leg recovers.
  `settle_by_elimination` was left alone deliberately: an eliminated leg keeps
  `unknown_ssrc` set and therefore never reports itself resolved, so the pass
  re-runs every tick and a re-resolution that flips one leg re-derives the
  other on the next one.
- The counters the recovery story is read through: `ssrc_changes` (confirmed
  takeovers), `reresolutions` (times a pushed map renamed the leg), and the
  existing `unknown_ssrc`, which now **returns to zero** when a leg recovers.
  `SharedLegStats` also publishes the unresolved SSRC's value
  (`unresolved_ssrc()`, sentinel `NO_SSRC` when resolved) — that is the only
  new thing the control world reads out of the media world, and it reads it
  the same way as every other counter: relaxed atomics, no lock.

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
(`MEDIAGATEWAY_BILLING_TOPIC` / `KAFKA_VOICE_AI_AGENT_TOPIC` schemas).

## Phase-0 measurements so far (Article VIII)

`cargo bench -p media-core` (criterion, `benches/ingest_pipeline.rs`),
first run 2026-08-14 on an **Apple M2, 8 cores, arm64, rustc 1.93.0** —
note that is *not* the pinned 1.95.0 (see the toolchain note below), and
not the production Linux/x86 target, so treat these as order-of-magnitude:

| Benchmark | Per 1000 packets | Per packet |
| --- | --- | --- |
| `ingest_only_per_packet` (parse + jitter push) | 10.89 µs | **10.9 ns** |
| `parse_jitter_decode_per_packet` (+ pop + G.711 decode) | 164.9 µs | **165 ns** |

Re-measured 2026-08-22 for the jitter hardening (tasks item 17) on the
development machine — **WSL2 on Windows, 11th-gen Intel i7-1165G7, 8 cores
allotted, x86_64, rustc 1.95.0 (the pinned toolchain)**. Both columns come
from the same machine and the same criterion invocation, minutes apart, so
the delta is meaningful even though neither number is comparable to the M2
row above:

| Benchmark (per packet) | Before item 17 | After item 17 | Delta |
| --- | --- | --- | --- |
| `ingest_only_per_packet` | 16.3 ns | **25.6 ns** | +52% |
| `parse_jitter_decode_per_packet` | 240.9 ns | **269.8 ns** | +10% |

The Article-VIII bar is 2× per packet; the full path moved 10%. The ingest
half costs ~9 ns more because every admitted packet now updates the RFC 3550
estimate and recomputes the target depth (that recompute carries an integer
division). It was left per-packet rather than batched: at 50 packets/s per
leg, 9 ns is 0.45 µs of CPU per second per 1000 legs, and a per-packet target
is easier to reason about than a periodically-refreshed one. The decode path
grew ~29 ns, mostly the PLC history memcpy (320 bytes per frame) — the pitch
search does not run on a clean stream. Neither benchmark exercises loss, so
concealment cost is unpriced; the AMDF search is ~19k integer ops once per
gap, which is tens of microseconds against a 20 ms pacing deadline.

Reading of the original M2 run: decode dominates the sans-IO path (~154 of
the 165 ns). One
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

### recorder.rs — the stereo segmenter and the S3 sink (M5, landed 2026-08-22)

The Phase-2 recorder is **a hub consumer in the control world**, exactly like
`consumer_ws`: it owns a `hub::Subscription`, a tokio task, and no part of the
media thread. Nothing in this module runs on the capture thread — the WAV is
built and the upload is made after the audio is already in memory.

- **The identity is the contract.** `RecordingIdentity::parse` accepts exactly
  `${accountID}/${recordingID}.${format}` and refuses everything else *by
  name*: no separator, a nested prefix (more than one separator), an empty
  account or recording id, a relative segment (`.`/`..`), no extension,
  whitespace or control characters, or a format other than `wav`.
  `object_key()` rebuilds the string byte for byte, and the round-trip is a
  test (`the_frozen_identity_round_trips_byte_exact`). Checked in
  `object_store`'s source rather than assumed: `Path::parse` rejects only
  `.`/`..`, ASCII control characters and an embedded `/`, all of which this
  parser already refuses — so a key we accept is always a key the store can
  address, and `UploadError::Key` is a defensive path rather than a reachable
  one. That is also why there is no separate key-validation hook on the sink
  trait: it would be dead code. `TelCompat`
  already composed this endpoint from `acc_id`/`record_id`/`file_format`, so
  `StartRecording` reaches the parser unchanged.
- **The segmenter is sans-IO and pause is its only interesting state.** Three
  mono buffers (customer, agent, mixed) plus an `anchor_ms` and a
  `segment_start`; a frame lands at `segment_start + (timestamp_ms - anchor) *
  rate / 1000`, so a leg that falls silent is zero-filled and both legs stay
  wall-aligned. `finish()` interleaves customer **left**, agent **right** —
  the `write_wav` convention from `tap_spike.rs`, which is FS
  `RECORD_STEREO`'s — and injected bot speech (`Track::Mixed`) is
  **saturating-summed into the right channel**, because a tapped voice-AI call
  has no agent leg and a recording without the bot side would be useless.
  Separate buffers rather than one interleaved buffer is what makes that sum
  independent of the order the hub publishes a tick's frames.
- **pause = segment + defer + accumulate, and that is three mechanisms.**
  `pause()` sets `segment_start` to the current end and clears the anchor;
  frames arriving while paused are dropped and counted
  (`frames_while_paused`); `resume()` re-anchors on the first frame after it,
  so the next segment is written directly after the last one. The audio of a
  paused interval is therefore absent, the segments are joined end to end with
  no silence between them, and `duration_ms` (frames / rate) is the
  accumulated recorded duration rather than wall clock. Nothing is uploaded on
  pause — that is the "defer".
- **Command ordering is deliberate.** The task's `select!` is `biased` with
  the media branch first, so audio that arrived before a pause command is
  absorbed before the pause takes effect, and audio that arrives during a
  pause is dropped even if a resume is already queued. At 50 Hz the queue
  empties between frames, so a command waits at most one ptime; the payoff is
  that the segment boundary is where the caller asked for it and the tests are
  deterministic without sleeps.
- **Events, in the frozen order.** `RecordingStarted` is raised by `TapPlane`
  when the attachment opens (so it is synchronous with `Attach`);
  `RecordingPaused{paused, duration_ms}` on both pause edges;
  `RecordingStopped{duration_ms}` **before** the upload;
  `UploadCompleted{uri}` only when the object actually landed. All four go
  through `SessionRegistry::observe` via the new `ObservationSink`, which
  means they are sequenced, keyed by `external_id` and published to
  `mss.events` like every other event. `RecordingPaused` is new in
  `session-core`, `EventKind` and `proto` (field 24) — the legacy contract has
  a `recordPause` callback and MSS had no way to express it; the resume edge
  reuses the same variant with `paused: false`, which is our extension and is
  documented in the proto.
- **Uploads use `object_store` (`AmazonS3`), never a hand-rolled S3 client.**
  Configured from `MSS_RECORDING_BUCKET` (presence enables recording),
  `MSS_RECORDING_S3_ENDPOINT` (set for MinIO; plain http is allowed only for a
  non-https endpoint, and path-style addressing is forced), region, key id and
  secret. `put_opts` sets `Content-Type: audio/wav` — verified on the object
  in MinIO. Retries are `object_store`'s own (3, bounded by
  `UPLOAD_TIMEOUT`), with a tokio timeout as the outer bound, and the whole
  finish path is bounded by `FINISH_TIMEOUT` (90 s) so `DestroySession` cannot
  hang on a dead bucket.
- **A failed upload spills instead of vanishing.** With
  `MSS_RECORDING_SPILL_DIR` set, the WAV is written to `dir/<identity>` and
  counted (`mss_recording_spills_total`, alerted). Without it, a failed upload
  loses the audio — that is the honest state, and the alert says so.
- **Bounded, like everything else.** The whole recording is buffered in
  memory: 8 kHz stereo is ~32 KB/s, so `MAX_RECORDING` (2 h) caps one
  recording at ~230 MB and further frames are counted
  (`frames_beyond_cap`, `mss_recordings_truncated_total`) rather than
  silently dropped. Streaming multipart upload is the fix when calls longer
  than that matter; it is not built.
- **`StopRecording` waits for the upload, deliberately.** `Detach` (and
  `DestroySession`) await the recorder's finish, so a cigol `StopRecording`
  blocks for as long as the upload takes — bounded by `UPLOAD_TIMEOUT` (60 s)
  and `FINISH_TIMEOUT` (90 s). The alternative, backgrounding the upload,
  cannot work today: `observe` refuses a session the registry has forgotten,
  so `UploadCompleted` would be lost exactly when the call has ended, which is
  every time. If a pilot finds the added `StopRecording` latency unacceptable,
  the fix is a session-independent event path (an event that carries
  `external_id` without needing a live session), not a silent background
  upload.
- **Known gaps:** no multipart/streaming upload (hence the cap and the memory
  cost); `recordingChannels=mono` metadata is not honoured (a single-track
  *selector* gives a mono file, a mono *mix* of both parties does not exist);
  no re-upload of spilled files (an operator job today); and a recording is
  per pod, so a pod that dies mid-call loses the audio it had buffered even
  though the session itself is adopted elsewhere (the adopted session
  re-taps, but the recording restarts).

### The rustls/ring dependency this added, and why

An S3 client needs TLS, and TLS in Rust needs a crypto provider. The repo's
hermetic pure-Rust build (protox over protoc, rskafka over rdkafka) survives
this addition only because of a specific feature selection, so do not
"simplify" it:

- `object_store` with `default-features = false` and features
  `aws-base, reqwest, ring`. The `aws` umbrella feature pulls **aws-lc-rs**,
  which builds C with **cmake** — not available in `rust:1.95-slim-bookworm`,
  so it would break the Dockerfile. `ring` builds C/asm with `cc`, which that
  image has.
- `reqwest` with `rustls-no-provider` (reqwest 0.13's `rustls` feature also
  means aws-lc-rs) plus `rustls` with `ring`, and
  `recorder::install_crypto_provider()` installs the ring provider once
  before the first client is built. Without that install, building a client
  **panics at runtime** — which is exactly how the first drill failed.
- `cargo deny check all` then needed one allowance:
  **CDLA-Permissive-2.0** for `webpki-root-certs`, the Mozilla CA root bundle
  rustls verifies against. It is a data licence on certificates, not code,
  and every Rust HTTPS client lands on a Mozilla-derived bundle under either
  it or MPL-2.0. The reasoning is in `deny.toml` next to the entry.

### What the recorder changed in the modules around it

- **`control-api/controller.rs`** grew three things: the `ObservationSink`
  trait (implemented by `SessionController`, so the media world can raise
  `Observation`s through the registry rather than inventing its own events —
  `observe` had no caller at all before this), `MediaPlane::update_attachment`
  (default `Ok(())`), and a reordering of `DestroySession`: **the media plane
  is now closed before the registry forgets the session.** That reorder is
  load-bearing — `observe` refuses an unknown session, so a recorder finishing
  during teardown could not have published `RecordingStopped`/
  `UploadCompleted` at all. Test:
  `a_recording_closed_by_a_hangup_still_gets_its_callbacks_before_the_session_ends`
  asserts the event order is AttachmentUp, RecordingStopped, UploadCompleted,
  AttachmentDown, SessionEnded.
- `UpdateAttachment` now reaches the media plane, and **a refused update is
  rolled back** in the registry (paused/selector/format restored) rather than
  left claiming a state the media world never accepted.
- **`tap_plane.rs`** serves `FILE_S3`: parse identity → require a configured
  sink → require the session → hub-subscribe (all tracks, or one if the
  selector names one) → spawn the recorder → register it as a consumer for
  metrics → raise `RecordingStarted`. `update_attachment` forwards pause to
  the recorder and, for WS/gRPC, logs that pause is still control-plane state
  only (their media keeps flowing — an unchanged, and now explicit, gap).
  `close_attachment` **awaits** the recorder's finish so the callbacks land
  before the caller's `Detach` returns, and `close_session` now sweeps *all*
  of a session's attachments (previously it left WS tasks and map entries
  behind on `DestroySession`) finishing recordings first.
  `TapPlane::observe_through` takes a `Weak<dyn ObservationSink>` — weak, so
  the controller↔plane cycle cannot leak the controller.
- **`hub.rs`**: `Subscription::try_next` is no longer test-only. The recorder
  drains what is already queued after it is told to finish, so a stop does not
  discard up to 200 buffered frames (4 s) of audio.
- **`metrics.rs`**: ten new series
  (`mss_recordings_started_total`, `_stopped_total`,
  `mss_recording_pauses_total`, `_uploads_total`, `_upload_failures_total`,
  `_spills_total`, `mss_recordings_truncated_total`,
  `mss_recording_bytes_uploaded_total`, `mss_recording_seconds_total`, gauge
  `mss_recordings_live`), read out of one `Arc<RecorderCounters>` shared with
  `TapPlaneMetrics`. Upload failures, spills and truncation are alerted in
  `deploy/prometheus-alerts.yaml`.

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
