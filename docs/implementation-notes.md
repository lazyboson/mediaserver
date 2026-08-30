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
- ~~**M3:** slot sizing revisit when Opus lands~~ — landed 2026-08-23.
  `MAX_PAYLOAD` is **1276** (RFC 6716's maximum Opus packet) and sizes the wire
  slot only; the new `MAX_FRAME_SAMPLES` (960) sizes the PCM scratch. The old
  single constant did both jobs, which is why widening it needed care.
- **Still open:** the adaptive ceiling is a multiple of the configured floor
  rather than a millisecond budget, and nothing yet *reports* the chosen
  depth per leg beyond `Stats.target_depth` (exported only in the
  `tap leg finished` line, not as a gauge).

### pipeline.rs — one tapped stream, end to end, sans-IO
`StreamPipeline` is the whole per-stream ingest path as a state machine:
`ingest(datagram)` classifies and buffers, `release()` emits one frame of
PCM when the caller's pacing deadline says so. It owns the jitter buffer,
the DTMF detector and the decoder, and allocates nothing per packet
(reusable fixed arrays only).
- **Two ways to build one, and the difference matters.**
  `StreamPipeline::new(format, ..)` is for a codec whose payload type and clock
  rate both follow from the static RTP table — G.711. `with_config(PipelineConfig)`
  is for everything else, because it takes the wire facts (`audio_payload_type`,
  `clock_rate_hz`) separately from the decode target (`decode: AudioFormat`).
  Opus needs this: its payload type is dynamic, its RTP clock is always 48000
  (RFC 7587) whatever rate it decodes to, so a 16 kHz Opus tap advances the
  jitter buffer by **960** ticks per packet while emitting **320** samples.
  Conflating those two numbers is the bug this split exists to prevent.
- **Concealment is per codec, and they are not interchangeable.** G.711 loss
  is filled by plc.rs; Opus loss is filled by **libopus itself** (a NULL packet
  handed to `opus_decode`), because Opus carries the decoder state that makes
  its own extrapolation better than anything generic.
- **A decoded frame longer than one packet is carried, not truncated.** Opus
  senders may use 40 or 60 ms frames — legal, and common for WebRTC on a bad
  network. An early cut normalised every decoded frame to `samples_per_packet`,
  which silently discarded two thirds of a 60 ms sender's audio. The surplus
  now goes into a fixed `Carry` (no allocation, capped at libopus's 60 ms at
  48 kHz) and is released over the following frames; `release()` serves the
  carry before touching the jitter buffer. `frame_size_mismatch` and
  `carry_overflow_samples` count the two ways this can bite, and both are
  logged per leg — the point is that truncation can never again be invisible.
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
G.711 µ/A at 8 kHz from **any** tap rate, L16 at the tap rate, or resampled
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
- **G.711 output resamples first (landed 2026-08-23).** The refusal used to be
  "g711 output only exists at the tap rate", which meant a 16 kHz Opus tap
  could not feed the frozen PCMU-8k WebSocket bridge. The encode now happens
  after the resample, so the only remaining constraint is the codec's own:
  G.711 is defined at 8 kHz. Pinned by
  `a_wideband_tap_still_feeds_the_frozen_pcmu_bridge`, which checks the
  downsampled tone's rms rather than its length.
- Refusals are named, never silent: Opus **output** (tasks item 16d — ingest
  landed), G.711 at anything but 8 kHz, stereo, mismatched ptime.

### pacer.rs — the egress playout pacer (Phase 3 groundwork, landed 2026-08-23)

The first piece of the inline leg, and the mirror image of `pipeline.rs`: where
the pipeline turns arriving RTP into PCM, `PlayoutPacer` turns queued PCM into
departing RTP. Sans-IO in the strict sense — no sockets, no threads, no clock:
`tick(now: Duration)` takes elapsed monotonic time as a parameter and returns
`Option<PacedPacket>`, whose `datagram` is a complete RTP packet the caller can
hand to `send_to` unchanged.

Structure:

- `SampleQueue` is a fixed `Vec<i16>` ring (capacity = `queue_frames` × source
  frame). `push` copies in with at most two `copy_from_slice` calls and, when
  full, advances the read cursor — drop-oldest, returning how many samples it
  dropped. `take_into` fills the frame scratch buffer and zero-pads the tail, so
  a half-frame of TTS still leaves on time.
- Encoding is `ConsumerEncoder`, unchanged and reused per tick: it clears and
  refills its own byte buffer, so nothing allocates at steady state. That is
  also why the pacer inherits its rules — mono only, source and wire ptime must
  match, no Opus output.
- The datagram buffer is `12 + (wire frame + 8 samples) × bytes-per-sample`; the
  slack covers the FFT resampler's output rounding. `RtpPacket::serialize`
  writes into it in place.
- State is exactly what an RTP sender owes: `sequence`, `timestamp`, `ssrc`,
  `payload_type`, `in_silence`, `started`, plus `next_deadline`.

Decisions worth knowing before extending it:

- **Two underrun policies, `Silence` (default) and `Suppress`.** Silence keeps
  the far end's jitter buffer fed and is what P3-2 should use for a plain SIP
  peer. Suppress advances the timestamp but consumes no sequence number, which
  is the honest wire shape for a DTX/comfort-noise peer; it is untested against
  a real endpoint.
- **Catch-up is one packet per call**, never a burst inside one tick, so a late
  media thread cannot dump five packets into the network in one wakeup; the
  missed deadlines show up as `late_ticks` in `PacerStats` and should be
  exported as a metric when the pacer is wired into mediaserverd.
- **`clear()` is the barge-in seam.** Flushing the queue makes the next tick a
  silence frame (or a suppression), which is the ≤ one-ptime cut-through P3-4
  wants to measure. It is deliberately a queue operation, not a session verb.
- **Failures are counted, not fatal.** An encoder or serializer error increments
  `encode_errors`, still advances sequence and timestamp (the far end reads one
  lost packet, which is what happened) and returns `None`.

Not done here: nothing called it yet at this commit. **Item 33 wired it**
(`inline_leg.rs` below — UDP socket pair, SDP answer, and `InlineEgress::pump`
driving `tick` from the capture loop), item 34 fed it from a consumer's inject
stream, and item 37 drives one per conference member. Opus egress is still absent
everywhere in the crate.

### mixer.rs — the N-way mix matrix (Phase 4 core, landed 2026-08-23)

Phase 4's product is this file: `MixMatrix` is the conference engine, sans-IO
and thread-free like the rest of the crate. It holds **N contributors x M
listeners** of `Gain`, mixes one frame per tick, and knows nothing about
sockets, sessions or rtpengine — P4-2 wires it between the per-leg ingest
pipelines and one `pacer.rs` per listener.

Contract, decided here so P4-2/P4-3/P4-5 do not re-litigate it:

- **Frame-synchronous, one frame per contributor per tick.** The caller pushes
  each contributor's frame for tick T (`push` — exactly `frame_samples`, mono,
  already decoded and resampled to the conference rate), then calls `mix()`
  once, which returns a `MixOutput` borrow with one frame per occupied
  listener. A frame is consumed by exactly one tick: a contributor that pushes
  nothing is silence for that tick and is counted (`absent_frames`), and a
  second push inside one tick replaces the first (`PushOutcome::Replaced`,
  counted) rather than summing — two frames in one tick is a caller bug, not a
  mixing decision.
- **Contributors and listeners are separate memberships**, which is what makes
  the three Phase-4 features fall out of the same matrix instead of needing
  special cases: `join_party()` takes one of each and links them (**minus-self**
  = that one pair defaults to `Gain::MUTED`, every other pair to unity);
  `join_listener()` alone is a **monitor** (hears everyone, contributes
  nothing — no leg at all, matching architecture §Phase 4); `join_contributor()`
  alone is an injector (a prompt player, or a **whisper** source once
  `route_only(contributor, listener)` mutes its row everywhere else).
  `route_to_all` is the **barge** flip. Mute is a zeroed row
  (`mute_contributor`), deaf is a zeroed column (`deafen_listener`), and their
  inverses restore the *defaults*, minus-self included — P4-5's member controls
  are these four calls plus a control-plane verb.
- **Identity is index + generation.** `ContributorId`/`ListenerId` carry the
  slot's generation, so a stale handle from a party that left is refused
  (`MixError::Unknown{Contributor,Listener}`) instead of silently addressing the
  next occupant. Leaving a slot clears its pending frame, its speech state, its
  self-link and its gain row/column back to defaults, and zeroes the listener's
  output region — a slot reused by the next joiner cannot leak the previous
  party's routing or a tail of their audio. Both directions are reset on *join*
  as well, because a slot that was never occupied has no history either.
- **Accumulate in i32, saturate to i16.** Per pair, unity gains add the raw
  sample and non-unity gains add `(sample * gain_q12) >> 12` (Q12 fixed point,
  `Gain::MAX_Q12` = 8x, so no product or sum can overflow i32 at conference
  scale; the accumulator uses `saturating_add` anyway — nothing here panics).
  Clamping to `i16::MIN/MAX` counts `clipped_samples`, which is the metric that
  says a conference needs AGC. There is no AGC and no DC filter yet; the
  roadmap's "sum/saturate DSP, active-speaker, AGC" line is two thirds done.
- **Active-speaker flags are per contributor, and hysteretic.** Frame energy is
  the mean square computed once at push time; `SpeechGate { rms_threshold,
  attack_frames, hangover_frames }` (default 300 / 2 / 12 = 240 ms of hangover
  at 20 ms frames) needs `attack_frames` consecutive loud frames to raise the
  flag and more than `hangover_frames` quiet ones to drop it, so alternating
  loud/quiet frames never flap it (a test asserts exactly that: zero onsets).
  Absent frames count as quiet. Flags never affect routing — `speaking()`,
  `level()` (rms) and `active_speakers()` are for events and for the
  loudest-talker UX, and the mix is unconditional.
- **No allocation per frame.** The gain matrix (row stride = listener capacity),
  the i32 accumulator, the per-listener output block and each contributor's
  frame buffer are sized at construction and grow **only** on membership change
  — a new contributor extends the matrix by one row, and a listener past the
  current stride doubles the stride and re-strides the matrix once. A test
  drives 10 000 ticks of an 8-party conference plus a monitor (with drops,
  clipping and speaker transitions) and asserts every buffer's length *and*
  capacity is unchanged at the end.

Mixing is the straightforward O(contributors x listeners) accumulate with muted
pairs and absent contributors skipped. The obvious optimization for large
conferences — sum every contributor once, then subtract each listener's own
contribution — is deliberately *not* here: it is only valid while the matrix is
the minus-self default, and whisper/mute/per-pair gain (the reason this is a
matrix) break it. If conference fan-in ever needs it, gate it on a
"matrix is default" flag rather than on N.

Multi-rate conferences are out of scope — every contributor must arrive at the
conference's rate and frame size. Opus egress is still absent crate-wide, so a
conference of Opus legs transcodes to G.711/L16 on the way out.

Since item 37 the caller is `mediaserverd`'s `conference.rs`, which drives one
matrix from one capture thread. One sharp edge that only shows up there and is
worth repeating: **`join_listener` resets its whole column to the defaults**, so
a new member's arrival restores unity from every contributor into it —
including contributors whose row was deliberately narrowed by `route_only`.
Any non-default routing must therefore be re-applied after every membership
change, which is exactly what `conference.rs`'s `apply_matrix` does (named
`reroute_injectors` when item 37 introduced it). The
mixer test suite could not have caught it: it never re-checks an old route
after a later join, and a conference test did.

## crates/opus-ffi — libopus, and the only place unsafe lives

WebRTC legs are Opus, so a tap that cannot decode Opus either depends on
rtpengine transcoding it — expensive, and the thing that keeps the tap out of
the kernel path (tasks item 20) — or cannot serve those calls at all.

**The codec is libopus.** Article XI's preference order is explicit: vetted
bindings to proven C libraries, *the same code FreeSWITCH wraps*. libopus has
shipped in every browser, WhatsApp, Zoom, Discord, Signal, FreeSWITCH and
Asterisk since 2012. Nothing else on offer is in that category, and this crate
exists so its `unsafe` has exactly one home (`media-core` and every other logic
crate keep `#![forbid(unsafe_code)]`; this one deliberately does not).

**A pure-Rust port was tried first and rejected on evidence.** `opus-rs` builds
with no cmake at all, which was attractive, but: first release six months ago,
**30 releases** in that window, **no external crates depend on it**, docs
coverage 1.49%, and its own changelog records a table-transcription bug that
made stereo above ~160 kbps decode to garbage against libopus, fixed days
before it was considered. For a codec on the ingest path carrying real customer
audio, that is not a defensible trade against a convenience property, and it
inverts the Constitution's own ordering. Recorded here because the reasoning
matters more than the outcome.

**The binding is `opusic-sys` 0.7.5, not the more popular `opus` crate**, and
the reason is concrete rather than aesthetic: `opus` 0.3.1 (57 dependents)
pulls `audiopus_sys` 0.2.2, unmaintained since 2021, which vendors libopus 1.3
whose `CMakeLists.txt` declares a `cmake_minimum_required` below 3.5 —
**CMake 4.x has removed that compatibility, so it does not build at all**
(measured: `CMake Error ... Compatibility with CMake < 3.5 has been removed`).
`opusic-sys` is actively maintained, vendors current libopus, and builds under
the pinned 1.95 toolchain in ~14 s. Its licence is BSD-3-Clause, already
allowlisted, so `cargo deny` needed **no new allowance**.

What the wrapper guarantees, so callers never touch a raw pointer:

- Mono only and one of libopus's five legal rates (8/12/16/24/48 kHz), both
  refused by name before libopus is asked.
- `frame_samples` parses the packet's TOC to learn the frame length, so a
  buffer too small is `FrameTooLong` naming both sizes **before** the decode
  rather than an opaque libopus error code.
- Every libopus error code is turned into its `opus_strerror` text, so a log
  line says what libopus actually objected to.
- `conceal()` passes a NULL packet, which is how libopus is told a frame was
  lost — so **Opus's own concealment is used for Opus loss**, rather than the
  G.711 Appendix I concealer, which is right for G.711 and merely adequate
  here. Pinned by `libopus_conceals_a_lost_frame_itself`.
- An empty packet is `EmptyPacket` rather than silently meaning loss: loss goes
  through `conceal`, so the two cases cannot be confused at a call site.
- `Drop` destroys the decoder and encoder; `Send` is asserted because a
  decoder is owned by one session and may move between threads, never shared.
- The encoder is public because the speech probe needs it and Opus output
  (tasks 16d) will; it is not on any production path yet.

**The build cost, paid in three places and measured.** libopus is vendored and
compiled, so `cmake`, `make` and `g++` are needed at build time — **not at
run time**, because it links statically. The Dockerfile builder stage installs
them in one cached layer and the shipped distroless image is unchanged
(verified: the image builds, the binary starts, 36.6 MB). The lab pods run
`cargo run` from a mounted tree, so they now build from
`lab/Dockerfile.rust` instead of the bare `rust:slim` image. CI needs no
change: it runs on `ubuntu-latest` runners, which carry all three. The `make`
requirement was found by building the image rather than assuming — cmake alone
fails with `CMAKE_MAKE_PROGRAM is not set`.

**Verified**: a tone round-trips at all five rates with exact frame counts,
silence stays quiet, libopus conceals a lost frame, malformed packets are
errors and never crashes, a decoder survives moving between threads, and — the
bar that matters for a speech codec — real lab speech round-tripped through
libopus at 8 kHz was transcribed **verbatim by Deepgram** ("Hello.", "This is
the media server speaking through your bridge.") via
`ear_intelligibility_probe.py`, at 9.8 kbps. `examples/opus_speech_probe.rs`
produces the artifact.

### media-core/opus.rs — the AudioFormat adapter

A thin, safe shim: it validates that the format really is Opus and hands the
rate and channel count to `opus-ffi`, so the pipeline has one call and
`media-core` keeps owning the `AudioFormat` vocabulary without gaining any
`unsafe`. Rate is the **caller's** choice, deliberately — Opus decodes to any
of five rates and which one a tap should use is a negotiation question, not a
decoder question.

**Wired into `StreamPipeline` since 2026-08-23** (tasks 16b-2; see the
pipeline.rs section above — `PipelineConfig` carries the negotiated payload type
and `Decoder::Opus`). It was deliberately left unwired at *this* commit because
the pipeline derived its audio payload type from
`Encoding::static_payload_type`, and Opus has no static type — it is always
dynamically negotiated (typically 111), so the plumbing belonged with the SDP
work rather than being half-wired here.

### frame.rs — the codec table, now readable in both directions

`Encoding::static_payload_type` had no inverse, so nothing could answer "what
codec is payload type 8?". `from_static_payload_type` and
`static_clock_rate_hz` close that, and a round-trip test pins them against
each other so the two directions cannot drift. Only PT 0 (PCMU) and 8 (PCMA)
map — deliberately, because those are exactly the encodings
`StreamPipeline` can decode. L16's static types (10/11) are **not** listed:
claiming them would let a tap accept a stream the pipeline would then refuse.
That is what makes `offered_format` in `rtpengine-ng` a safe question to ask.

### dtmf.rs — complete for RFC 4733 digit reporting
- Reports once per press on end-bit, deduped by (digit, RTP timestamp);
  events ≥16 (flash-hook etc.) deliberately ignored. That dedupe is what makes
  RFC 4733's three end retransmissions **one** press, and since item 48 it has a
  test of its own rather than being an implicit property.
- `push` returns a `DigitPress { digit, duration_ms, rtp_timestamp }`, not a bare
  `char` (item 48, D21): the bus needs the press length and a timestamp that
  lines a digit up with recorded audio. **`duration_ms` is converted through the
  detector's own clock rate**, which `StreamPipeline` gives it from
  `PipelineConfig::clock_rate_hz` — the *negotiated* RTP clock, not a hardcoded
  8000. RFC 4733 §2.4.1 has the event stream share the audio stream's clock, so
  an 800-tick press reads 100 ms on a G.711 call and 16 ms on a 48 kHz Opus one.
  A zero clock rate yields 0 ms rather than dividing by zero.

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

### stats.rs — `version`, `statistics`, and the kernel-forwarding verdict (2026-08-23, item 23)
- `NgClient::statistics` and `NgClient::version` are one-key requests;
  `RtpengineStatistics::from_reply` shapes the reply, and
  `KernelForwarding::from_statistics` turns it into a verdict. All sans-IO, all
  unit-tested against bencode captured from the live lab node.
- **There is no NG `version` command.** rtpengine 14.1.1.8-jambonz11 answers
  `Unrecognized command`, and the upstream protocol documentation's command list
  does not contain one either — brute-forcing 37 candidate names against the
  live node found only `ping`, `list`, `statistics`, `transform` and the
  call-scoped verbs. The builder ships anyway as a *probe*: it costs one
  datagram at startup, a future build that grows the command is picked up for
  free, and `mediaserverd`'s `VersionReport::NoVersionCommandOnThisNode` makes
  the universal outcome a first-class state instead of an error. The version
  itself has to come from the process, the package or `--listen-cli`.
- The shape was probed before it was typed, and the probe changed the types.
  Measured facts now pinned by tests: `totalstatistics.uptime` is a bencode
  **string** (`"23129"`), not an integer, and so are the duration fields — hence
  `number()` accepts `Value::Int` and numeric `Value::Bytes` and truncates at
  the decimal point. `currentstatistics` carries `packetrate_kernel` /
  `packetrate_user` / `media_kernel` / `media_userspace` / `media_mixed` /
  `transcodedmedia`; `totalstatistics` carries `relayedpackets` and
  `relayedbytes` each split `_kernel` / `_user`; `transcoders` is a list of
  `{chain, packets, bytes, samples, num}` where `chain` reads
  `"PCMU/8000 -> opus/48000/2"`.
- The verdict ladder, in order: no kernel/userspace split in the reply at all →
  `Undetermined(StatisticsWithoutKernelCounters)`; live kernel packet rate or
  kernel/mixed media now → `ForwardingInKernelNow`; a non-zero kernel lifetime
  total → `ForwardedInKernelEarlier`; nothing relayed at all →
  `Undetermined(NoMediaRelayedYet)`; otherwise → `RelayingEntirelyInUserspace`.
  `media_mixed` counts as kernel because part of it is. `module_in_play()`
  returns `Option<bool>` so "cannot tell" cannot be mistaken for "no".
- Deliberately **not** modelled: the `controlstatistics.proxies` array (one
  entry per NG peer with per-command counts and durations) and the per-interface
  ingress/egress/ports/voip_metrics blocks. They are large and nothing needs
  them yet; `lab/kernel_probe.sh` reads whatever it likes straight off the wire.
- Known limit: a `statistics` reply grows with the number of NG peers and
  interfaces. It arrives as one UDP datagram and the transport's buffer is
  65535 bytes, so a node with hundreds of proxies could in principle truncate.
  Not observed — the lab's reply with six proxies is a few KB.

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
- **`offered_format` reads the tap's codec out of the offer** (2026-08-22).
  It walks the offered payload types **in offer order** and returns the first
  one `media-core` maps to a static encoding, taking the clock rate from that
  PT's `a=rtpmap` when present and the ptime from the stream (falling back to
  the caller's). Offer order matters: it means the answer's first payload type
  is one rtpengine actually offered, which is what makes a no-transcode
  subscription legal. An offer of nothing decodable is
  `NoStaticCodecOffered(offered)` — named, with the list, so the log says what
  the call was using.
  The companion test is the one worth keeping: with our *configured* PCMU
  format, the answer to an A-law-only offer is `RTP/AVP 0 8 101` — it **adds**
  payload type 0. That addition is precisely the mechanism that asks rtpengine
  to transcode, and it is now pinned
  (`our_configured_codec_would_have_added_an_unoffered_payload_type`)
  next to the case that must not add anything
  (`the_answer_to_an_alaw_only_offer_adds_no_codec_the_offer_did_not_carry`).
  `to_sdp` itself needed **no change**: it already puts `self.format` first and
  echoes the rest, so handing it the offered format is the whole fix.
- ~~**M3:** dynamic rtpmap~~ — **landed 2026-08-22.** `NegotiatedCodec
  {payload_type, encoding, clock_rate_hz}` is now what an answer is built from,
  and it comes from one of two places, never a silent default:
  - `OfferedStream::negotiate()` — what the offer carries. Static payload types
    resolve through `media-core`'s table; a **dynamic** type is matched by its
    `a=rtpmap` encoding name, which is the only way Opus can be recognised
    because RFC 7587 says its payload type "is to be assigned dynamically".
  - `NegotiatedCodec::from_static_format(format)` — our own configured codec,
    which is the *transcode* shape: it puts a payload type into the answer that
    the offer may not have carried, and that addition is what asks rtpengine to
    convert.
  `SubscriptionAnswer.answer_with` is **required**, not optional. An earlier
  draft defaulted it to "negotiate from the offer", which silently broke the
  transcode path — the caller knows which mode it is in, so it says.
- **Three RFC 7587 rules the answer obeys, verified against the RFC rather than
  assumed**, because §4's history is a list of answers rtpengine rejected:
  the rtpmap clock rate **must** be 48000 and the channel count **must be 2**
  *even for a mono stream* (mono is signalled in-band, not in the rtpmap); the
  RTP timestamp clock is 48000 Hz "for all modes of Opus and all sampling
  rates", which is why `NegotiatedCodec::samples_per_packet` is computed from
  the **clock rate** and not from the audio rate we decode to; and the payload
  type is dynamic, so the answer echoes the offer's rather than choosing one.
  An offer advertising opus at any other clock rate is `OpusClockRate`.
- A dynamic payload type whose rtpmap names a codec we do not decode (EVS,
  G.722) is **skipped, not guessed**, and if nothing is left the error names
  every offered type.

#### The inline leg's offer/answer (item 33, 2026-08-23)

`InlineOffer` / `InlineAnswer` live in this same module, and the reason is a
dependency direction, not laziness. This file is already a plain sans-IO SDP
implementation whose only dependency is `media-core`; the alternative home
(`media-core/src/sdp.rs`) would have duplicated the line splitter, the rtpmap
parser and the codec-negotiation types, because `media-core` cannot depend on
`rtpengine-ng`. The crate's *name* is now narrower than this module — if a third
SDP dialect ever appears, split the module into its own crate rather than
copying it. Nothing in `InlineOffer` talks to rtpengine.

- `InlineOffer::parse(sdp, ptime_fallback_ms)` reuses `SubscriptionOffer::parse`
  and then enforces what an inline leg needs: **exactly one** `m=audio`
  (`InlineStreamCount`), a non-zero port (`NoPeerPort` — port 0 is held media),
  an address from the media- or session-level `c=` (`NoPeerAddress`), a codec
  the leg can both hear *and speak*, and a paceable ptime (`UnusablePtime`,
  capped at `MAX_INLINE_PTIME_MS` = 120).
- `negotiate_inline` walks the offer's payload types **in the offer's own
  order** and takes the first PCMU or PCMA (`INLINE_CODECS`). Opus is refused
  even though the tap path decodes it: `ConsumerEncoder` has no Opus *encoder*,
  so an Opus inline leg could listen and never speak. Refusal names what was
  offered — `NoInlineCodecOffered(vec!["opus", "G722", …])`, from the rtpmaps
  when present, from the static table otherwise, and `payload type 9` when
  neither knows it.
- The answer is one `m=audio <our port> RTP/AVP <pt> [te]`, the chosen rtpmap,
  the offer's own telephone-event payload type with `a=fmtp:<te> 0-15`, the
  ptime, and `a=sendrecv`. It deliberately does **not** echo the offer's other
  payload types — the opposite of the subscription answer above, where echoing
  everything is what makes rtpengine transcode. Here MSS *is* the endpoint, so
  the answer is a narrowing, and an offer whose only codecs we refuse never
  gets an answer at all.
- The answer bytes are pinned by a test, and a second test re-parses our own
  answer with `InlineOffer::parse` — the cheapest available proof that what we
  emit is legal SDP by our own reading of it. No real SIP peer has parsed it
  yet (item 33 is replay + fake socket only).

### Leg attribution: what a `query` reply does and does not carry (item 47)

`NgReply::tags_created()` returns each participant's `created` alongside its
tag, preserving the reply's own order. What that field is, **measured** with
`lab/ng_tag_created_probe.py` against the lab's rtpengine 14.1.1.8 rather than
read from documentation:

- a tag entry's only scalar fields are `tag` and **`created`** — an integer of
  whole **seconds**. The sub-second companions `created_ts` (microseconds since
  epoch) and `created_us` exist **only at the top level** of the reply, next to
  the call's own `created`; there is no per-tag equivalent;
- `created` is stamped **per dialogue**, not per participant. An offer and an
  answer 4 s apart both came back `1787737315`. A second offer/answer pair on
  the *same* call-id 12 s later came back `1787737395` for both of its tags
  (legA/legB `…383`, legC/legD `…395`);
- therefore creation time can separate B2B dialogues sharing one call-id and
  **can never** separate the two legs of one dialogue. `MSS`'s
  `order_participants()` returns `Attribution::Inferred` only when the first two
  participants carry strictly different seconds, which for a two-party call on
  this version never happens;
- the reply's own tag order is **not** rtpengine's insertion order by the time
  MSS sees it: `bencode::Value::Dict` is a `BTreeMap`, so `tags()` is
  lexicographic by tag bytes. That, not any creation order, is what inverted the
  labels in the two-node drill (D17);
- and the probe itself must randomise its cookie prefix: rtpengine replays a
  cached reply for a repeated cookie, so a fixed prefix makes consecutive runs
  answer with the *previous* run's call (the D12 shape, seen again here).

### DTMF digits on the event bus, and why they need no capability (item 48, D21)

`crates/mediaserverd/src/digits.rs` is the whole bridge: a bounded
`ArrayQueue<Digit>` plus a `Notify`. `TapLeg::drain` — on the real-time capture
thread — calls `publish`, which pushes or counts a refusal and returns; it never
blocks, never allocates and never touches the registry. One Tokio task per
session drains the queue and calls `ObservationSink::observe`, which is the same
`Weak<dyn ObservationSink>` the recorder uses. `close_session` closes the queue,
the publisher drains what is left and ends on its own — it is deliberately **not**
aborted like the SSRC watcher, because a digit pressed just before the hangup is
still a fact about the call.

**The gating decision, and why it differs from `SpeechReport`.** A digit is
published at **session level**, whenever the session exists: no attachment, no
`CAPABILITY_EVENTS`, no consumer needed. MSS decoded it from the call's own RTP,
so there is nothing to authenticate — the same footing as `RecordingStarted` and
`LegsAttributed`, which also go through `Registry::observe`. `SpeechReport`
(item 28, D19) is gated because it is the mirror image: a *consumer* claiming
something it inferred, where the attachment is the claimant and must be
privileged to speak for the call. Reading the attachment-level EVENTS capability
here would mean a call with no consumer produced no digits, which is exactly the
integration this closes — a recording-only or bus-only integrator building a digit
menu. Do not "harmonise" the two paths; the asymmetry is the point.

- capacity is 64 presses. Digits are rare (item 40 chose not to rate-limit
  them), so a drop means something is wrong, and it is visible:
  `mss_dtmf_events_dropped_total`, accumulated per session at close;
- **both** tap legs and inline legs carry the sink, so digits pressed into a bot
  leg reach the bus too;
- the payload's `track` goes through `convert::track_name_under`, so an
  `attribution=unknown` session's digits arrive on `leg_a`/`leg_b` — item 47's
  rule, unchanged, applied to digits;
- `proto.Dtmf` gained `duration_ms = 3` and `rtp_timestamp = 4`. Additive: the
  `MediaEvent` payload oneof tag stays **14**, and an old consumer decoding the
  message simply sees the two new fields defaulted.

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

`attribution.rs` holds `Attribution` (`Explicit` / `Inferred` / `Unknown`,
default `Explicit`) — how a session's track names were arrived at (item 47,
D17). `SessionRecord` carries it, `SessionView` exposes it, and **every**
`MediaEvent` is stamped with the record's value at `push_event` time, so a
consumer of `mss.events` can tell a named direction from a guess without asking
the API. It is set at `create_session` (a TAP with no `from_tags` is `Unknown`;
everything else, including every INLINE leg, is `Explicit` — MSS answered an
inline leg itself, so its capture is unambiguous) and rewritten by
`Observation::LegsAttributed` from the media plane, which is also the event that
reaches the bus. `record_attribution()` is the direct setter for callers that
have no observation to make. It is deliberately **not** persisted in
`PersistedSession`: a session with no `from_tags` is not rebuildable, and one
with them re-derives `Explicit` on the adopting pod.

`SessionRegistry` is the MediaControl API of architecture.md §5.1 as a pure
state machine: no sockets, no async, no clock of its own. The tonic service
and the Redis registry are shells around it, which is what makes every rule
below testable without a lab (Constitution, Article III).

The four nouns are `SessionKind` (Tap/Inline/Mix), `AttachSpec`,
`PlaybackSpec` and `MediaEvent`. Phase 3/4 add no operations — an inline leg
is `SessionKind::Inline` and **a conference is inline legs sharing a `group`**
(item 37), so both ride the same calls. Since item 55 `SessionKind::Mix` is the
**room session**: a session with no leg whose `group` names the conference it
*is*. `create_session` validates that shape in one place
(`room_session_shape`): a non-empty `group` and no `call_id`, `from_tags` or
`sdp_offer`, refused otherwise as `ControlError::RoomSessionShape { reason }`
(→ `INVALID_ARGUMENT`), which is the only kind-specific rule that lives in
session-core rather than in the controller — a room's shape is a property of the
record, not of the wire.

`SessionRecord` also stamps `opened_at: SystemTime` at create and `SessionView`
exposes it, which the controller renders as `Session.opened_at_unix_ms`
(field 15). It is **not** persisted: an adopted session is re-created on the
adopting pod and takes that pod's stamp, and the only session whose open must
be exact — the room — is never adopted.

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

- **A playback remembers who it was played to (2026-08-23, item 27, defect
  D2).** `PlaybackRecord` keeps the `target_tag` from its `PlaybackSpec`, and
  `stop_playback` returns it as `StoppedPlayback { session, target_tag }` so
  the shell can aim rtpengine's `stop media` at the same participant instead
  of stopping every player on the call. The registry is the only place that
  knows this, because the media plane sees a `PlaybackId` and nothing else.

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

### Known gaps (M4) — all three closed, kept for the reasoning

- ~~**Not yet wired to tonic.**~~ **Wired since 2026-08-20**: `crates/control-api`
  serves `MediaControl` over tonic and `ControlError` maps onto the status codes
  planned here (`CapabilityDenied` → `PERMISSION_DENIED`,
  `AuthoritativeAlreadyBound` → `FAILED_PRECONDITION`, `Unknown*` →
  `NOT_FOUND`, `IdempotencyConflict` → `ABORTED`), with `MixRoute` →
  `INVALID_ARGUMENT` added by item 38.
- ~~**Not yet wired to the hub.**~~ **Wired**: `tap_plane` converts
  `TrackSelector` into `hub::TrackSelection` (which item 27 split into `All` and
  `Speakers` for D13). The two types stay deliberately separate — the media
  world must not depend on control-plane vocabulary — and the rule still holds:
  if a third copy ever appears, that is the signal to promote one.
- ~~**No persistence.**~~ **Landed 2026-08-17**: the Redis session registry
  carries ownership leases and re-subscribes on pod loss (proved on a live
  `kill -9`, tasks item 11; the orphaned subscription it exposed is fixed in
  item 25). `SessionId`/`AttachmentId` counters still restart with the process,
  which is safe by construction: the wire form is prefixed and parse-checked, so
  a stale id from another pod is rejected as unknown rather than aliased onto a
  live session. An INLINE or grouped session is deliberately **not** adoptable
  (items 33 and 37).
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

### server.rs — `WireFacing`, because the keeper and a client share one `attach`

`RegistryKeeper` rebuilds an adopted attachment by calling the very same
`SessionController::attach(Request<AttachRequest>)` the gRPC server serves, and
it passes its own state through the attachment metadata:
`mss.recording.resumeMs` (item 30) and `mss.recording.spillOwner` (items 53 and
54). Nothing refused those keys on the wire, so any authenticated client could
send them — silence-padding a recording, and since item 54 setting `take_over`
and claiming another pod's recording-group seat. The bearer token is
cluster-internal, so this was low severity, but it was the only door into
`take_over`.

`WireFacing(Arc<SessionController>)` closes it. It implements `MediaControl` by
delegation and, in `attach` and `update_attachment` only, refuses
`INVALID_ARGUMENT` naming any metadata key that starts with
`RESERVED_METADATA_PREFIX` (`mss.`). `serve_authenticated_until` — and through
it every other `serve_*` — hands `MediaControlServer` a `WireFacing`; **the
keeper and every other in-process caller keep the bare
`Arc<SessionController>`**, which is exactly why the keeper's rebuild tests
carry those keys and stay green. That contrast is the proof the guard is at the
right layer.

The prefix and the two key constants live in `session-core/src/metadata.rs`
(with `reserved_metadata_key` / `reserved_metadata_refusal`, both pure and unit
tested there) because control-api cannot see `mediaserverd`; `recorder.rs`
re-exports the two names it already used, so `recorder::RESUME_MS_METADATA_KEY`
and `registry_keeper`'s `is_resume_key` did not move. `mss.` rather than
`mss.recording.` was safe to reserve: a session-core test pins every
client-facing key this API documents — the eight telcompat/Twilio ones and the
six `mix_*`/`member_*` verbs — as still allowed, so the wider prefix costs
nothing and leaves room for the next internal key.

**TelCompat needed the same guard, and it is not `WireFacing`'s job.** The
façade is served over the same socket but reaches the controller *in-process*,
and `stream_metadata` copies `request.metadata.clone()` — caller-supplied —
into the attach. So `TelCompat::attach_sink`, the single funnel for every
façade attach, calls `reserved_metadata_refusal` itself.

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
  `RESOURCE_EXHAUSTED`, `Unknown*` → `NOT_FOUND`, malformed ids,
  `RoomSessionShape` and unspecified enums → `INVALID_ARGUMENT`.
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
  Two synchronous readers hang off it — `inline_egress_sink` and
  `member_state`, both defaulted — and one **reverse** call goes the other way:
  `ObservationSink::session_finished(session, reason)`, added by item 55 so the
  auto-ending room session can end its own control-plane record (the controller
  implements it as `destroy_session`, so `SessionEnded` reaches the bus in that
  session's own sequence). It is defaulted to a no-op; nothing else uses it, and
  the media plane still never reaches into the registry itself.
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
  is the compatibility one. Under `TrackSelector::All` the tracks
  advertised in `StreamStart` are customer and agent, and since item 27 that
  is exactly what arrives — `mixed` is delivered only to a consumer that
  named it (defect D13).
- **`StreamFrame::Stop { reason }` is how the media plane says goodbye**
  (2026-08-23, item 27): it becomes a `StreamStop` carrying the reason and
  the server ends the stream. Before it existed, a detached attachment
  reached the consumer as nothing more than a closed channel, which is
  indistinguishable from a crash.
- **Inject is authorized at the first frame**, not at flush:
  `authorize_inject` maps `CapabilityDenied` to `PERMISSION_DENIED` and the
  stream ends — the proto calls unprivileged inject a protocol violation,
  not a no-op. An authorized utterance accumulates decoded PCM, `Mark`
  flushes it as a WAV blob through the controller's own `StartPlayback`
  (requested_by = the attachment, block_egress = true), so the playback is
  registry-tracked, evented onto `mss.events` and attributed; `Clear`
  discards the buffer and stops the last playback — the barge shape, with
  `NOT_FOUND` on the stop tolerated because the playback may have ended.
- **On an INLINE session the inject path is continuous** (2026-08-23, item 34).
  The stream resolves `SessionController::inline_egress_sink(session)` **once**,
  at subscribe, when `session.kind == INLINE`; if it comes back `Some`, every
  `Inject` frame is decoded to PCM and pushed straight into the leg's egress
  queue, `Clear` calls `flush()`, and `Mark` becomes a drain barrier instead of
  a playback. If it comes back `None` — every TAP session, and any plane with no
  inline legs — the accumulate-`Mark`-`StartPlayback` path above runs unchanged,
  cap included. Three things a future editor should not "simplify":
  `authorize_inject_once` also carries the format check (the leg's rate vs the
  attachment's declared rate; mismatch is `FAILED_PRECONDITION` naming both,
  because there is no streaming resampler on this path), so it must stay the
  single entry point for `Inject`/`Mark`/`Clear` on an inline session; the mark
  poll branch reads `marks_pending` computed **before** `select!` so the async
  block does not borrow `inject` across the await; and a full egress queue logs
  and drops one chunk rather than ending the stream (≈6.4 s of backlog means a
  misbehaving consumer, but killing a live voice-AI stream is worse).
- **`ServerToConsumer.mark = 6`** exists only for that ack (`Mark` had no
  server→consumer wire before item 34). It is sent when
  `drained_watermark() >= ` the watermark the `Mark` recorded, polled every
  20 ms while any mark is outstanding — so an ack is at most one ptime late and
  never early. A tap's `Mark` is never acked: it starts an rtpengine playback,
  which reports no completion.
- **That poll is one pinned `tokio::time::interval`, never a `sleep` inside
  `select!`** (2026-08-23, item 35 — this was a live defect, not a style rule).
  Both loops (`control-api/src/stream.rs`, `mediaserverd/src/consumer_ws.rs`)
  originally wrote the branch as `tokio::time::sleep(MARK_POLL_INTERVAL)` inside
  the `select!`, which recreates — and therefore **resets** — the timer on every
  loop iteration. A subscribed consumer wakes that loop every 20 ms with a
  tapped frame, and `select!` picks randomly among ready branches, so the sleep
  was routinely cancelled before it elapsed: the inline drill measured a mark
  acked **8221 ms** after its audio had already drained on time at 400 ms. The
  fix is one `interval` (with `MissedTickBehavior::Delay`) constructed **outside**
  the loop and a guarded branch, `_ = mark_poll.tick(), if marks_pending`, which
  costs nothing while no mark is outstanding because a disabled branch is not
  polled. Measured after the fix: 403–410 ms against a 400 ms lead. Any future
  periodic work in either loop must follow the same shape.
- **`SpeechReport` is the consumer's only way to report speech** (2026-08-23,
  item 28, defect D19). `ConsumerToServer.report` carries a kind
  (`STARTED`/`PARTIAL`/`FINAL`/`END_OF_UTTERANCE`/`END_OF_INTERACTION`), track,
  text, confidence and the consumer's own `observed_at`;
  `convert::speech_report` turns it into a `ConsumerEvent` and
  `SessionController::record_report` commits it through the registry, so it
  publishes on `mss.events` like any other event and MSS — not the consumer —
  still decides `first_final`. `Registry::report` requires
  `Capabilities::EVENTS`, so a `SINK`-only consumer that reports gets
  `PERMISSION_DENIED` and the stream ends: the same protocol-violation shape as
  an unprivileged inject, deliberately, because a consumer whose barge trigger
  is being silently discarded needs to know. Two choices to keep in mind when
  extending this: an unspecified or unknown `kind` is `INVALID_ARGUMENT` rather
  than a default, and `observed_at` is **logged as a lag, never published** —
  the bus event's `at` is MSS's own clock, because a consumer's clock cannot be
  reconciled with it downstream. **The WS-Twilio dialect has no equivalent and
  will not get one** (frozen bytes): a WS consumer cannot report speech, so
  interactive voice-AI belongs on the gRPC transport. Measured live — cut-through
  from `SpeechReport` to `StopPlayback` acked, p50 3.54–3.98 ms (lab.md).
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
- **Known wart found by that run (D13) — closed in item 27:** `start_message`
  derives its `tracks` list from the selector, so `TrackSelector::All`
  advertises `["customer","agent"]`, while the hub also delivered
  `Track::Mixed` (the injection feed, silence-filled every tick so it stays
  gap-free) — a track the consumer was never told about and 50% more bytes than
  the start frame implied. Fixed by narrowing the *delivery* to the
  advertisement (hub `Speakers`), not by touching the frozen Twilio `start`
  frame.

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
- `mss_registry_grouped_not_adopted_total` was **removed** by item 54: a
  grouped recording is now rebuilt on the adopting pod, so there is nothing
  left to count. The recording-group series that remain
  (`mss_recording_groups_live`, `mss_recording_group_members_live`,
  `mss_recording_group_joins_refused_total`) are **per pod** by design — a
  group whose members sit on two pods is counted once on each — and the
  refusal counter gained a fourth reason, a session registry this pod could
  not read.

`deploy/prometheus-alerts.yaml` carries the alert rules: every drop counter
(consumer frames, event queue, publish failures, outbox), watchdog stalls,
jitter loss ratio, and the split-brain signal `mss_registry_lost_total`.

Since the jitter hardening (item 17) there is also
`mss_jitter_silence_gaps_total` — sequence numbers a sender's silence
explains. It has no alert rule on purpose: it is normal traffic on any leg
whose endpoint does silence suppression. Its value is that
`MssJitterLossHigh` got *quieter and more honest* — those gaps used to land
in `mss_jitter_lost_total`.

The rtpengine node series (item 57, 2026-08-26) are the one place where
`metrics.rs` reports numbers that are **not MSS's own**: they are the last NG
`statistics` sample each health probe took, exported per node in the
`mss_dependency_ready{…}` labelled style —
`mss_rtpengine_tap_kernel_verdict{node,verdict}` at 1 for the verdict that held,
then `mss_rtpengine_{relayed_packets_kernel,relayed_packets_user,media_kernel,
media_userspace,media_mixed,transcoded_media,sessions_live}{node}` and
`mss_rtpengine_sample_age_seconds{node}`. Cardinality is bounded by the number
of rtpengine nodes a pod has probed, not by calls, and a pod that has probed
nothing emits none of them (the same "absent source leaves its series out"
rule as Kafka and Redis). The age gauge is the honesty check: `statistics`
failing leaves the last sample in place, and the age is what says so.
`MssTapsFellOutOfKernel` alerts on the transcoding verdict or on userspace
media rising for 10 min while kernel media stays flat, with architecture §8.1
as its runbook. Two pods tapping one node report the same counters, so
aggregate with `max by (node)`, never `sum`.

The recording spill series gained two counters with item 53:
`mss_recording_spill_lost_ownership_total` (closed segments this pod did not
spill because another pod had adopted the journal — a partition signal, not a
storage signal) and `mss_recording_spill_foreign_manifests` (journals this pod's
startup salvage left alone because their manifest names another owner). Both sit
beside `mss_recording_spill_segments_total` and
`mss_recording_spill_failures_total`, and neither has an alert rule yet: a
non-zero `lost_ownership` is the interesting one, and what a good threshold is
will not be known until a pilot has produced a baseline.

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
  Since 2026-08-23 it also carries `subscription_tag` — the `to-tag` rtpengine
  answered the `subscribe request` with — because that string is the only
  handle that can cancel the tap, and only the pod that created it held one.
  The field is `#[serde(default)]`, so a record written before it existed
  still decodes (asserted in unit tests and against real Redis); an empty tag
  means "unknown", and an adoption that finds one counts
  `orphans_still_subscribed` instead of pretending it cleaned up.
- `mss:lease:{external_id}` — the owner pod, **with a TTL** (15 s, renewed
  every 5 s). The lease is the whole HA mechanism: a pod that dies stops
  renewing, the key expires, and the session becomes adoptable.
  `upsert` writes this key with **`NX`**, not a bare `SET`. It used to
  overwrite it every persist tick, which quietly made the lease unloseable:
  a pod that had already been adopted away took ownership back 5 s later and
  both pods tapped the call. With `NX` the incumbent still re-acquires a lease
  that merely **expired** unclaimed (so a Redis restart does not read as split
  brain), but a lease another pod holds stays held, and `renew` returning
  `false` now means exactly one thing — another pod owns this session.
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

**What the keeper does for a recording (item 53, 2026-08-26).** Nothing changed
in `registry_keeper.rs` for the cross-pod spill, and that is the point:
`persisted_from` already stamps `PersistedRecording{recording_id, owner,
recorded_ms, spilled_ms}` on every tick and `rebuild` already derives
`mss.recording.resumeMs` / `mss.recording.spillOwner` into the rebuilt
attachment's metadata. What changed is what those two mean downstream —
`resumeMs` is still where the recording had reached, but `spillOwner` no longer
predicts what the adopter can read: with `MSS_RECORDING_SPILL_TO=s3` the
adopter reads the dead pod's segments out of the recording bucket and pads only
the unspilled tail, so a foreign `spillOwner` is now a log field, not a loss.

**Cancelling the previous owner's tap (D14, fixed 2026-08-23).** `rebuild`
first calls `TapSubscriptions::unsubscribe_orphan(node, call_id, to_tag)` —
a small trait in `registry_keeper.rs` that `TapPlane` implements by binding a
throwaway `NgTransport` to the session's node and sending NG `unsubscribe` —
and only then re-subscribes. The order is asserted in a test through a shared
journal (`unsubscribe …` must precede `subscribe …`), and the wire bytes are
asserted in `tap_plane.rs` against a fake rtpengine socket. Two decisions
worth knowing:

- **Only after winning the claim.** The unsubscribe happens inside `rebuild`,
  which runs after `claim_unleased`'s atomic `SET NX` — so a pod can never
  cancel a tap it did not just win the right to own. That is also why the NX
  change above matters: without it, "won the claim" was not durable.
- **A refused unsubscribe does not block adoption.** The call is re-tapped
  anyway and `orphans_still_subscribed` counts the leak, because a re-tapped
  call with a leaked copy is strictly better than a call nobody taps.

**An inline leg is not adoptable, and the registry refuses honestly (item 33,
2026-08-23).** A tap is re-creatable from any pod because MSS *asks* rtpengine
for the copy — that is the HA advantage architecture §7 claims for pull-initiated
taps, and this is where its limit is written down. An inline leg **is** the RTP
destination: the peer is sending to an ip:port on the pod that answered the
offer, and no other pod can inherit that socket or that SDP. So
`PersistedSession::is_rebuildable()` returns false for `kind == INLINE`, and
`adopt_orphans` checks `is_inline()` *before* the unsubscribe/rebuild path,
releases the record, counts `mss_registry_inline_not_adopted_total` and logs that
recovery belongs to call control (a re-INVITE to a live pod), not to the
registry. The SDP is deliberately not persisted: storing it would invite exactly
the dishonest rebuild this check exists to prevent.

**Losing the lease is fatal for the session (the partitioned-owner half).**
A pod whose `renew` returns `false` now destroys the session locally through
its own `DestroySession` — which closes the tap, and `close_session` already
sends `unsubscribe` for its own `to-tag`, so a partitioned-but-alive owner
cleans up after itself instead of double-tapping until the call ends.
`surrendered` counts it. The surrendering pod must **not** `forget` the
registry record — its successor owns that record now — so `release_gone`
takes the surrendered set and skips exactly those ids while still dropping
them from `persisted_here`. `released` therefore stays a count of *ended*
calls only. The trade-off accepted: a pod stalled past the 15 s lease (a long
GC pause, a frozen host) drops a call it could still have served. It is the
right side to err on — the adopter has already re-established that tap, and
the alternative is the D14 cost forever.

**What a rebuilt attachment restores (D15, fixed 2026-08-23).** The replayed
`AttachRequest` carries the attachment's **negotiated format**, not `None`.
`PersistedAttachment.format` is an `Option<PersistedFormat>` — encoding as the
`proto.Encoding` number plus `sample_rate_hz` / `channels` / `ptime_ms`, the
same wire shape `kind`, `transport` and `capabilities` are already persisted
in, deliberately, so there is no second encoding table to drift from the
proto. `#[serde(default)]` makes a record written before the field existed
decode to `None`, and `None` is read by `convert::format` as
`AudioFormat::pcmu_8k_20ms()` — which is what such a record meant anyway.
Before this, an ASR consumer that attached as L16/16 kHz came back from an
adoption as g711/8 kHz on the same stream: no error, just a wrong sample rate.
Two things are **not** restored and stay listed under D16/D9: a recording
group's membership, and a recording's buffered audio.

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
`released`, `failed`, `inline_not_adopted`, `orphans_unsubscribed`,
`orphans_still_subscribed`, `surrendered`) are exported as
`mss_registry_*_total`. A rising `lost`/`surrendered` means two pods believed
they owned one session and one gave up; a rising `orphans_still_subscribed`
means rtpengine is copying a call to a pod that is gone — the two worth
alerting on.

The resume metadata (`mss.recording.resumeMs`, `mss.recording.spillOwner`) is
**derived on every adoption and never accumulated**: `resume_metadata` strips
both keys off the persisted record before re-adding them from the recording
journal, so a session adopted three times does not carry three generations of
hints. Since the 2026-08-27 hardening those two keys are also **refused on the
wire** — the `mss.` prefix is reserved, see `server.rs` — `WireFacing` above.
The keeper is unaffected because it calls the controller in-process, which is
what its rebuild tests assert by still carrying them.

Config: `MSS_REDIS_URL`; unset means sessions live and die with the pod
(logged), and a configured-but-unreachable Redis **refuses to start** rather
than running with no recovery. Verified in the lab on a live call: the session
appeared in Redis with its call-id, tags and consumer endpoint, the lease
counted down from 15, and the index was empty again after `DestroySession`.

`redis` 1.6 is pure Rust, so this added no system dependency — but it pulls
`xxhash-rust` under **BSL-1.0** (Boost), now allowed in `deny.toml`:
permissive, OSI-approved, no attribution burden in binaries.

**Observed on a real pod kill (2026-08-22, tasks item 11).** Three pods on one
Redis, `kill -9` on the owner of a live tapped call: one survivor adopted
14.6 s later and the consumer's audio resumed after a **14.41 s** gap, which is
what the constants predict (lease 15 s renewed every 5 s + a 10 s sweep = 25 s
worst case). Adoption rebuilt the session through the controller, `TapPlane`
re-subscribed, the WS consumer was re-dialed at its persisted endpoint, and the
rebuilt legs were named and clean (`jitter_lost: 0`, `recv_errors: 0`).

Gaps: leases are renewed per session per tick with one round trip each (fine
at hundreds, revisit at thousands); the discovery map (call-id → node + tags)
is a separate, still-unbuilt concern; `SessionStore` is a `mediaserverd`
module rather than a crate, so the integration test re-includes it by path;
and D14's fix has **not** been re-measured on a live pod
kill — the drill needs a live SIP call and a rebuilt pod image, so what is
proven today is the seam and the ordering (unit tests, a fake rtpengine
socket, and the store paths against the lab's real Redis), not another
teardown block showing zero orphaned packets.

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
callers only know the channel uuid. **Resolved since 2026-08-17** (see
"Resolving a call's participants without the discovery map" above): the caller
passes the SIP call-id and the caller's from-tag as session metadata
(`sipCallId` / `callerFromTag`, both already on the FreeSWITCH channel) and
`TapPlane` asks rtpengine's `query` for the rest, so a TelCompat-created session
taps without the OpenSIPS→Redis discovery map.

### inline_leg.rs — the egress pump (item 33, Phase 3, 2026-08-23)

The socket half of `pacer.rs`, and the only place in the daemon where the media
thread *sends*. Two objects with a deliberate split:

- `InlineEgress` lives on the capture thread. It owns a `try_clone`d handle of
  the leg's receive socket — so MSS sends **from the port it answered on**,
  which is what symmetric-RTP peers and NAT expect — plus the `PlayoutPacer`
  and the consumer end of the queue. `pump(now)` is called from
  `capture_with_egress` on every loop iteration (roughly every ptime/4): it
  honours a pending flush, tops the pacer up, and calls `tick`, which self-paces
  and returns at most one datagram per ptime. `send_to` on a non-blocking socket
  cannot stall the media thread; a refusal counts `send_errors`.
- `InlineEgressHandle` is the control-world side: `push(Vec<i16>) -> bool` into
  a bounded `crossbeam_queue::ArrayQueue` (the same shape `hub.rs`'s inject path
  already uses) and `clear()`, which sets an `AtomicBool` the media thread
  swaps. Nothing in the control world can block the media world and nothing in
  the media world waits on a lock.

Sizing rule worth keeping: the **ArrayQueue is the buffer** (64 chunks; a
`StartPlayback` chunks at 100 ms, so ≈ 6.4 s) and the **pacer ring is only a
prebuffer** — `pump` stops popping once the pacer holds two frames. Without
that rule a 5 s prompt pushed at once would overflow the pacer's 500 ms ring
and be dropped oldest-first, i.e. the caller would hear the *end* of the prompt.
A push into a full queue is refused and counted rather than dropped silently.

Counters are published through `InlineEgressShared` (atomics, relaxed) and
summed into `IngestSnapshot.inline` by `TapPlaneMetrics`, which is how
`mss_inline_egress_*` reach Prometheus — including `late_ticks`, the metric
`pacer.rs` asked for when it was written. Deallocation of the popped `Vec`
still happens on the media thread; that is the pre-existing hub-inject shape,
and the honest residual until a sample-ring handoff replaces both.

`InlineEgressHandle` also carries the leg's `AudioFormat` and two watermarks
added by item 34, and implements `control_api::InlineEgressSink` so the control
plane can reach it through `MediaPlane::inline_egress_sink` without knowing this
type. The trait names are deliberately distinct from the inherent ones
(`push_pcm`/`flush`/`egress_format` vs `push`/`clear`/`format`) so no call site
can silently resolve to the wrong one. The drain accounting is one identity,
recomputed in `publish` every pump:

    drained_samples = (samples a flush discarded straight out of the ArrayQueue)
                    + pacer.stats().pushed_samples
                    - pacer.queued_samples()

It is exact, not an estimate: every sample the control world queues is either
still in the ArrayQueue (uncounted), discarded from it by a flush, or pushed
into the pacer, where it has either left (paced out, dropped by ring overflow,
or flushed) or is still queued. `pushed_samples` counts everything *offered* to
the pacer, which is why a ring overflow needs no separate term. A `Mark` ack is
`drained_samples >= watermark`, and the tick after a `clear()` is silence, so a
drained mark means the peer really has heard it.

Item 37 split `pump` into the four steps a conference needs to drive
separately — `take_flush`, `pop_chunk`, `queue_frame`, `send_tick` — and `pump`
is now their composition, so a plain two-party inline leg behaves exactly as
before. A conferenced leg never calls `pump`: its pacer is fed one **mixed**
frame per tick by `queue_frame`, and the ArrayQueue's chunks are pulled out with
`pop_chunk` and handed to the mix matrix as that leg's private injector instead.
That moves the drain identity, so `account_mixed_in` / `discard_pending` exist to
keep it exact in conference mode:

    drained_samples = (samples a flush discarded, queue or partial chunk)
                    + (samples the mixer consumed from the injector)

`mixed_in_samples: Option<u64>` is what selects between the two identities —
`None` (never accounted) keeps the tap/two-party formula untouched. A `Mark` on
a conferenced leg is therefore acked when the marked audio has been *mixed*,
one frame before it is paced out, and a `Clear` still flushes both the queue and
the pacer, which now discards mixed audio too (counted in `cleared_samples`).

`egress_ssrc(session, salt)` derives a non-zero SSRC (and the initial sequence
and timestamp) from the session id and the pod's SDP session id rather than
adding an RNG dependency; distinctness across sessions and pods is what matters,
not unpredictability, and a test pins that plus never-zero.

### conference.rs — one clock, one matrix, N legs (item 37, Phase 4, 2026-08-24)

A conference is a set of inline legs that named the same `group` on
`CreateSession`, and this module is the thread they share.
`CreateSession{kind=INLINE, group="standup"}` mirrors the recording group on
`Attach` deliberately: no new proto noun, and the same mental model (N sessions,
one shared thing). A `group` on a TAP is refused in `control-api` by name — a
recording group is named on `Attach`, a conference on the session.

**The threading decision, and why.** Before this item each inline session owned
its own capture thread. A conference cannot: mixing is frame-synchronous, so all
its legs must be released, mixed and paced on **one** clock, and a matrix shared
between capture threads would need a lock in the middle of the packet path —
exactly what the two-world rule forbids. The design chosen is therefore a
**conference-owner thread the legs migrate onto**: `open_inline_session` builds
the leg's socket, pipeline, hub and `InlineEgress` in the control world exactly
as it does for a two-party leg, and then hands the whole bundle
(`ConferenceMember`) to the conference thread over a bounded `ArrayQueue`
instead of spawning a thread for it. The alternative — one thread per leg with a
shared matrix — was rejected for the lock; the third option, migrating *sockets*
(fd handoff) rather than whole legs, buys nothing here because the pipeline and
jitter buffer would have to move with them anyway.

**Who decides membership: the control world, under one lock.** `TapPlane`
holds `conferences: Mutex<HashMap<String, Conference>>` beside the recording
`groups` table. `Conference` is the control-world half — command queue, stop
flag, shared counters, the member list and the `JoinHandle`. Because both the
"is there room / does the format fit" check and the member-list edit happen
under that lock, a join racing the last leave cannot resurrect a dying mix. The
thread never removes itself from the table: `unseat` decides, and when it drops
the last member it sets the stop flag and **returns the `JoinHandle`**, which
`close_session` joins in `spawn_blocking` the same way it joins a per-session
capture thread. A conferenced session's `LiveSession.capture` is therefore
`None` and its `conference` is `Some(name)`; everything else about it — hub,
egress handle, metrics registration, polite consumer shutdown — is unchanged.

**One tick.** Poll commands, then per member: hub commands, ssrc map, socket
drain, and a pending `Clear`. Every member's pacer gets a `send_tick` on every
loop iteration (the pacer self-paces, as it does for a plain inline leg). When
the release deadline arrives:

1. each leg releases its jitter-buffered frame, which goes to that leg's hub as
   the `customer` track (unchanged) **and** into the matrix as that leg's
   contributor — `TapLeg::release_frame_with` takes the sink, so the frame is
   never copied for the mixer;
2. each member's queued inject/playback audio is cut into exact frames by
   `InjectFeed` and pushed as that member's **injector** contributor;
3. `mix()` once;
4. each member's minus-self ear goes to its own `InlineEgress::queue_frame`, and
   the **monitor** listener's frame — the sum of everyone, self included — is
   published on *every* member's hub as `Track::Mixed`.

A leg that releases nothing pushes nothing and is mixed as silence (the matrix
counts `absent_frames`); a leg that leaves is a command processed at the top of
a tick, so **leaving cannot stall the mix** — there is no per-leg wait anywhere
in the loop.

**Decisions worth not re-litigating:**

- **The mixed track is published on every member session, not one designated
  one.** A monitor or recorder can then attach to whichever member session it
  already knows with the verbs that exist (`selector: only=mixed`), and no
  session becomes load-bearing for the conference's output. It costs one extra
  `TapEvent` per member per tick, which is the same fan-out cost the hub already
  pays for the customer track.
- **The mixed track is the full sum, not the listener's ear.** A recording of a
  conference should contain everybody; minus-self is an ear, not a record. That
  is what the matrix's `join_listener`-only monitor is for.
- **Injected audio is private to the leg it was played into, unless a route
  says otherwise (item 38).** Each member gets an injector contributor whose
  default route is `route_only` into its own listener, so `StartPlayback` and
  INJECT frames on a conferenced leg behave as they did before the leg joined —
  only that participant hears them, and the mixed track does not. Naming
  another member is the whisper; naming everybody is the barge. See
  *conference.rs — monitor, whisper and barge* below.
- **Multi-rate conferences are refused by name.** `Conference::accepts`
  requires the joining leg's sample rate, ptime and channel count to equal the
  conference's (set by its first leg) and says so in the error; the encoding may
  differ, because each leg owns its own encoder on the way out. Per-leg
  resampling is the residual, and it is a real one: a conference whose first leg
  is 8 kHz cannot admit a 16 kHz leg today.

Sizes: command queue 64, `MAX_CONFERENCE_MEMBERS` 32 (the matrix starts at 8
contributor/listener slots and grows on membership change only), one `SpeechGate`
default. New metrics: `mss_conferences_live`, `mss_conference_members_live`,
and counters for joins, leaves, mixed frames, clipped samples, absent frames,
clock re-anchors and matrix-refused frames.

Verified in-process over real UDP sockets (five tests in `tap_plane.rs`): three
fake peers on one group each hear the sum of the *other two* and never their own
tone (1000/2000/4000 in, ears of 6000/5000/3000 out, decoded off the wire); a
monitor on one member's hub sees the mixed track carrying all three (7000); a
leg closed mid-mix leaves the survivors reading exactly each other and the
conference still live, and the last leg out closes it (`conferences_live` back to
0); a prompt played into one leg is in that leg's ear and not the other's; and a
40 ms-ptime leg is refused from a 20 ms conference by name.

Not done here: cross-pod conferences (a group is one pod's table — a leg
answered by another pod cannot join it, and an inline leg is not adoptable
anyway), per-leg resampling, conference recording as one mixed file (P4-4),
AGC, and a **SIP** peer — none has been in a conference yet. Monitor, whisper and
barge landed as item 38, conference recording as item 39, the member-control
verbs as item 40 and the live three-peer drill as item 41, all below.
A conference group name is also **global to the pod**: there is no tenant scope
on a session the way `accountId` scopes a recording group, so two tenants
choosing the same group name would share a mix. Prefixing the name is the
integrator's job until sessions carry a tenant.

### conference.rs — monitor, whisper and barge (item 38, Phase 4, 2026-08-24)

Three conference features, one mechanism: **an attachment names where its
injected audio lands, and the matrix does the rest**. No new RPC, no new noun.

`session-core/src/mix.rs` is the single parser and the vocabulary:
`mix_target` = `own` (or empty — private playback, the item-37 default) |
`<member external id>` (whisper) | `all` (barge), and `mix_monitor` =
`include` | `exclude`. `MixRoute::from_metadata` is the only place those strings
are interpreted, `MixRoute::authorize` requires `INJECT`, and both refusals plus
`mix_monitor` without `mix_target` are typed errors that reach the API as
`invalid_argument` (`ControlError::MixRoute`). The literal `all` is reserved, so
a member whose external id is `all` cannot be whispered to by name.

The verbs:

- **Monitor** needed no code. `Attach{GRPC_STREAM|WS_TWILIO, SINK,
  selector.only="mixed"}` on any member session is it: the conference's full sum
  is on every member's hub as `Track::Mixed`, and a consumer that selects that
  one track hears the whole conference, itself included. A monitor is a record,
  not an ear, so minus-self does not apply to it.
- **Whisper** is `mix_target=<member>` on an INJECT attachment. The plane
  resolves nothing: it hands `ConferenceCommand::Route{session, route}` to the
  mix thread, which resolves the target **by external id, every time it
  reroutes**. That is what makes churn safe.
- **Barge** is the same key flipped to `all`, carried on `UpdateAttachment`,
  which gained `map<string,string> metadata = 6` — merged into the attachment's
  metadata (named keys overwritten, the rest left alone). Reusing the RPC that
  already carries pause/resume was the cheapest additive path, and it makes
  every future metadata-carried verb free. Re-applying an unchanged route is
  idempotent (and counted), because every pause on a whisperer re-sends it.

**The mixed track hears whispers by default.** `monitor_audible` defaults to
true for a whisper and a barge and false for private playback, and
`mix_monitor` overrides it in both directions. The reasoning is that `mixed` is
the recording feed as much as the monitor feed, and a recording that omits what
an agent was told mid-call misrepresents the call; a deployment that treats
supervisor coaching as off-record sets `mix_monitor=exclude`. Implementation is
one extra cell: `route_only`/`route_to_all` for the destination, then
`set_gain(injector, monitor, UNITY|MUTED)`.

`reroute_injectors` is now the whole routing decision and runs after **every**
join, leave and route change (item 40 renamed it `apply_matrix` and added the
member-control passes; it is still the only writer of non-default cells) (item 37's sharp edge: `join_listener` resets its
column). Two behaviors fall out of resolving by name each time: a whisper to
somebody who is not in the conference is **muted, not broadcast** — including
its monitor cell, so audio nobody heard never reaches the record — and it
becomes audible the moment that member joins.

Route ownership lives on the leg, not the attachment: `LiveSession.mix_route`
remembers which attachment moved the leg's injector off private, and
`close_attachment` puts it back (`revert_injection`). An attach whose transport
setup then fails reverts the same way. Two INJECT attachments on one leg share
one injector — last writer wins, documented, not enforced.

Events: `EventKind::MixRouted{target, monitor_audible}` →
`MediaEvent.mix_routed` (oneof tag 25; item 40 then took 26 and item 47's
`legs_attributed` took 27, so **the next free payload tag is 28** — item 48 added
fields to the existing `Dtmf` message rather than a new payload) on the
attach that declares a route and on every change, so an integrator can audit who
whispered to whom and whether it was on the record. `mss_ctl mix <attachment-id>
<own|all|member-id> [include|exclude]` is the lab handle. New metrics:
`mss_conference_whispers_live`, `mss_conference_route_changes_total`.

Verified in-process over real UDP sockets (5 new tests in `tap_plane.rs`, plus
`mix.rs` and registry unit tests): a whisperer injecting 8000 in a three-leg
conference is read at ~8000 by its target, under 300 by the other two **and by
the leg it was injected on**, and appears on a `mixed`-only hub subscriber;
`mix_monitor=exclude` keeps the target's ear and empties the mixed track; the
flip to `all` lands in all three ears; a fourth leg joining mid-whisper leaves
the route intact; detaching the whisperer restores private playback.

Residuals: `all` includes the injecting leg's own ear (an injector has no
minus-self link), so a human barging through their own leg hears themselves —
use a dedicated silent leg until P4-5 decides on a minus-self variant; a whisper
sourced from a member's **own RTP** (`mix_source=leg`) is not built here and
belongs with P4-5's mute/deaf/hold row/column verbs — **item 40 built it**, see
the member-controls section below; and nothing here had faced a real SIP peer
when this was written (item 41 put three legs on real sockets, but still without
SIP).

### conference.rs + recorder.rs — native conference recording (item 39, Phase 4, 2026-08-24)

Two shapes, both reusing surfaces that already existed, both under the frozen
`${accountID}/${recordingID}.${format}`. Neither needed a new RPC or transport.

- **The room, one mono object.** `Attach{FILE_S3, selector.only="mixed"}` on any
  member session. The conference publishes its full sum to every member's hub as
  `Track::Mixed` (item 37), `recording_selection_of` maps `only=mixed` to
  `TrackSelection::Only(Track::Mixed)` and `layout_of` to
  `Layout::Mono(Track::Mixed)`, so the recorder is an ordinary one-track hub
  consumer. Pause excises the paused span from that single object; a spilled
  segment (item 30) renders per target layout, so it stays mono and stitches
  back in order. Nothing in `recorder.rs` needed changing for this — the tests
  are what proves it, which is the point of them.
- **Every participant, one object each.** A recording **group** over the member
  sessions with `selector.only="customer"` (an inline leg's own audio is on
  `Track::Customer`), writing `<account>/<recording>/<label>.wav` per member,
  time-aligned on the group's `opened_at` through item 29's
  `Segmenter::lead_with_silence`. A member that joins the conference — and the
  group — late opens its file with silence back to t=0.

**The mix is published on each member's own clock.** This was the real defect
here. A member's own audio carries the **leg's** frame counter (0 at its join);
the mix used to carry the **conference's** (0 at the conference's open), so on
any member that joined late the two tracks on one hub were offset by the join
delay — 640 ms in the test that now guards it. `Seated` gained
`seated_at_frame`, stamped from the conference's `frames` at seat time, and the
per-member publish is `(frames - seated_at_frame) * ptime_ms`. Mono shapes never
noticed (the segmenter anchors on the first timestamp it sees); a stereo
`selector=all` object of a member ("me left, the room right") and any
cross-track timestamp correlation did. **Keep this invariant** when touching the
release block: anything published to a *member's* hub is on that member's clock,
not the conference's.

**A recording group of the mixed track is refused on a conference.** Every
member's `mixed` track is the same audio, so a group of them writes N identical
objects under one prefix. `open_recording_attachment` checks
`conference_of(session)` and refuses by name, naming both supported shapes in
the message. The refusal is scoped to a conferenced session on purpose: on a
plain tap `only=mixed` is the injected/playback track and differs per session,
so a group of those is legitimate.

**The event names the shape.** `RecordingStarted` gained `string shape = 3`
(additive, wire-compatible), emitted per object:

| shape | what it is |
| --- | --- |
| `stereo` | the two-party object, customer left / agent right |
| `track` | one named track as a mono object |
| `mixed` | the mixed track as one object (injected audio on a tap) |
| `participant` | one member of a recording group |
| `conference-mixed` | the whole room as one object |
| `conference-participant` | one member of a conference recording group |

`recorder::RecordingShape::of(layout, grouped).named(conferenced)` is the only
place that string is built — a new shape goes there and nowhere else. A consumer
therefore never has to parse the object key or know the session's group to tell
a room recording from a participant recording.

Residuals: the room object hangs off **one member's** session, so it ends when
that member leaves and its t=0 is its attach moment rather than the conference's
open (tasks.md D20); D16's pod-local recording group still applies to the
per-participant shape; and there is no AGC, so the room object clips exactly
when the mix clips.

### conference.rs — member controls, room prompts and mix_source=leg (item 40, Phase 4, 2026-08-24)

The conference feature tail, still with **no new RPC**: the vehicles are
`UpdateAttachmentRequest.metadata` (item 38's metadata-verb channel) and
`StartPlayback.target_tag`. The generic feature list and the adapter parity
table live in architecture.md Appendix B; this section is the module context.

**Vehicle choice.** `mix_target` (item 38) belongs to the *attachment* that owns
it and reverts on detach. Mute, deaf and hold are **member state**, so they ride
on any attachment of that member's own session (`member_mute` / `member_deaf` /
`member_hold`, each `on` or `off`, absent = untouched) and they **outlive that
attachment on purpose** — `close_attachment` reverts a whisper route and does
*not* revert member state, because muting somebody is not a property of the
consumer that asked for it. That also means no owner and no lease: tasks.md D22
— item 49 then made the state **readable** on `DescribeSession` without giving
it an owner (see the item-49 section below).
No capability is required (the API caller's own authentication is the
authorization, exactly like `paused`); `MemberControl::from_metadata` in
`session-core/src/mix.rs` is the only parser, and `EventKind::MemberControlled`
(proto payload tag 26, `MemberControlled{mute,deaf,hold}` from the *merged*
metadata) is the audit trail.

**Semantics, as matrix cells.**

- **mute** = `mute_contributor(party.contributor)`: the row, monitor cell
  included, so a muted member is off the **mixed track** and therefore off the
  recording as well as out of every ear.
- **deaf** = the column, minus what is *addressed* to that ear. It is **not**
  `deafen_listener`, which would also close the member's own injector and kill
  hold audio. `apply_matrix` collects `(contributor, listener)` pairs that were
  routed with `route_only` — the member's own private injector, and any whisper
  named **at** them — and zeroes every other contributor into that ear,
  including the room prompt and a barge. So: the room goes quiet, audio somebody
  aimed at them still lands.
- **hold** = both, which is why hold audio needs no special path: it is an
  ordinary `StartPlayback` on that session, arriving through the member's own
  injector, which is addressed to their own ear.

**`reroute_injectors` became `apply_matrix`, and it is now the only writer of
non-default cells.** Item 37's sharp edge still bites — `join_listener` resets a
whole column to defaults — so **every** membership change, route change and
member verb re-derives the whole matrix from `seated`: prompt row, then per
member the party row, the injector row and the monitor cell, then the deaf pass
last (it must run after every row-based operation, since `route_to_all` and
`route_only` rewrite whole rows). Add a feature by adding to this pass, never by
mutating one cell somewhere else.

**`mix_source=leg`** (the residual item 38 left) picks *which* contributor a
`mix_target` moves: `inject` (default, item 38's behavior) or `leg`, the
member's own RTP. `mix_source=leg` + `mix_target=<member>` is the coach shape —
the coach's own voice reaches one ear, the room hears them no longer, and
`mix_monitor` still decides whether the coaching is on the record (default:
include). The member's injector stays private in that case, so private playback
into the coach's own ear keeps working. `mix_target=own` + `mix_source=leg` is
*not* a leg route (`MixRoute::is_own_leg` is false): it is an ordinary member.
A leg route still requires an `INJECT` attachment, on the rule that moving audio
around a room is the inject right, even when nothing is injected.

**Room prompts.** A conference owns one prompt contributor fed by one
`ArrayQueue<Vec<i16>>` (`PROMPT_CAPACITY` 64 chunks of `EGRESS_CHUNK_MS`), routed
`route_to_all`, so a prompt lands in every ear **and** on the mixed track. The
verb is `StartPlayback` with `target_tag="all"` on any member session;
`target_tag` empty or `own` is the old private path (unchanged), and any other
value is now **refused by name** on an inline leg, because an inline leg has no
SIP from-tag to target. `StopPlayback{target_tag="all"}` flushes the queue and
the in-flight chunk (an `AtomicBool` the mix thread swaps, like the egress
flush). One source per room means two overlapping prompts **queue**, they do not
mix; a per-member prompt is the private playback path, and long-form audio
belongs on an `INJECT` attachment with `mix_target=all`.

**Enter/exit prompts are a verb, not a trigger** — MSS does not decide that a
join deserves a beep. Join-triggering is integrator policy: watch the room's
session events, call `StartPlayback{target_tag="all"}`. Prompt selection, tenant
policy and localization stay out of the media plane. Same answer for **DTMF**:
conference control is API-first, MSS delivers digits to consumers (WS `dtmf`,
gRPC `DtmfFrame`) and interprets none of them — and today those digits never
reach `mss.events`, which an API-first digit menu would want (D21).

`InjectFeed` became `ChunkFeed` (`fill(pop) -> filled_samples` + `frame()` +
`discard() -> unplayed_samples`, zero-padding a short frame) so the injector
feed and the prompt feed are one piece of code; the egress accounting
(`account_mixed_in` / `discard_pending`, which is what makes a `Mark` on a
conferenced leg mean "mixed") stayed at the injector call site, since a room
prompt has no leg to account to.

New metrics: `mss_conference_member_controls_total`,
`mss_conference_prompt_frames_total`, and the gauges
`mss_conference_{muted,deaf,held}_members` (muted and deaf count held members
too, since hold is both).

**Verified** over in-process UDP sockets and replay, no live run (the three
container peers are P4-6): six new `tap_plane.rs` tests — a muted member is
inaudible to both other members *and* on the mixed track and comes back on
`off`; a deaf member's ear is silent while the room and the record still carry
her; a held member hears his hold audio alone at full level while the room
loses him and the record never carries the hold audio; a room prompt is heard by
all three members and the mixed track and stops on `StopPlayback{all}`; a coach
routed `mix_source=leg` is heard by the agent and not by the customer while
still hearing everybody; and the refusals (a flag that is not `on`/`off`, a
member verb on a leg that is in no conference, a from-tag-shaped playback target
on an inline leg) — plus the mix-metadata parser tests and a registry audit
test.

### conference.rs — the control-world member mirror, read back by Describe (item 49, D22, 2026-08-26)

`Conference` — the **control-plane handle**, not the mixing thread — now keeps a
`MirroredMember` per seated session: external id, `mute`/`deaf`/`hold`, the live
`MixRoute` and the `AttachmentId` that asked for it. Before item 49 the handle
held only `Vec<SessionId>`; the authoritative copy of member state was
`Seated` **inside the mix loop**, and the control world could not read it without
a round trip through the media thread.

**Where to write member state now.** The mirror is written by exactly the four
calls that enqueue a media-thread command — `seat`, `route`, `control`,
`unseat` — so a new member verb means updating both the mirror field and the
`ConferenceCommand` arm, or the read-back silently lies. `route` grew an
`owner: Option<AttachmentId>` parameter for this (`route_injection` passes the
attachment, `revert_injection` passes `None` with `MixRoute::private()`); the
`ConferenceCommand::Route` payload is unchanged, because the mixer does not care
who asked.

**The read path.** `Conference::member_state` → `MemberStateView`
(`session-core/src/mix.rs`, plane-agnostic) → `MediaPlane::member_state`, a
**synchronous** trait method defaulting to `None` exactly like
`inline_egress_sink`, so a media plane without conferences is unaffected.
`TapPlane` answers from `conference_of(session)` plus the conference table.
`SessionController::session_message` calls it **before** it takes the registry
lock — the registry lock and the conference-table lock must never nest, and the
media plane never reaches back into the registry, which is what keeps that
ordering safe. On the wire: `Session.member` (field 13) and
`Session.conference` (14), both additive; the next free `Session` field is 15
and **no `MediaEvent` payload tag was taken — the next free payload tag is still
28**. `mss_ctl describe` needed no change: it renders the whole message.

**Reporting rules worth keeping.** `routes` is empty when the route equals
`MixRoute::private()` — the seat default, meaning the member's injected audio
reaches its own ear only and the mixed track does not carry it — and holds one
entry otherwise, including `own` + `mix_monitor=include`, which is a real
non-default route (private playback that the record carries). One entry, not
many, because two INJECT attachments on one leg share one injector and the last
writer wins (item 38's documented edge); the field is `repeated` so that edge can
stop being an edge without a wire break. Being whispered *at* is **not** a route
of one's own, so the addressee reports none: routes describe what a member sends,
never what it receives. `conference.members` is the mirror's own list, so it
still names a room whose whisper target has left — the stale route stays
auditable instead of vanishing.

**Still no lease, when this landed (D22's other half).** Nothing reclaimed a mute
when the controller that set it died; the read-back made a room reconcilable on
reconnect, and that was all it made. **Item 56 closed this** — see *member state
with a lease* below — and it did so without inventing an owner: a deadline
beside the flag in this same mirror bounds the state without deciding whose it
is.

### conference.rs + tap_plane.rs — the room is a session (item 55, D20, 2026-08-27)

Before this item a room recording hung off **one member's** session, so the
object ended when that member left even though the conference kept mixing, and
its t=0 was its attach moment. `CreateSession{kind=MIX, group=<conference>}` now
creates — or **adopts**, if a member opened the conference first — the room
itself as a session with no leg. Everything else is unchanged: the room session
is an ordinary `LiveSession` whose hub happens to be the conference's, so every
attachment kind works on it with the verbs that already existed.

**The room hub.** `Conference::start` creates a second `Hub` (`Hub::new()`, the
same pair as a leg's), moves the `Hub` into `Mixed` and keeps the `HubClient`
on the control-plane handle. Each tick the mix thread calls
`room_hub.poll_commands()` beside every member's, and after `matrix.mix()`
publishes the monitor listener's full sum into it with
`timestamp_ms = frames * ptime_ms` — **the conference clock**, frames counted
from the conference's own open, distinct from the per-member publishes which are
stamped `(frames - seated_at_frame) * ptime_ms` (item 39's fix). It is one more
bounded-queue `force_push` per frame on a queue nobody may block on: no
allocation, no lock, no I/O, and a room consumer that falls behind loses its
oldest frames and is counted exactly like any other. When the mix thread ends,
the `Hub` drops and closes its subscriptions, which is how a room recorder sees
end-of-stream without a special case.

**A conference with no member still mixes.** The room session opens the
conference when it arrives first, and `Mixed::run` with zero seated members
still ticks and still publishes the monitor frame — silence — so a room
recording opened before anybody joins **records the wait**, rather than needing
a pad. That is measured, not assumed
(`a_room_session_opened_before_anybody_joins_records_the_wait_and_then_the_room`).
A room-opened conference takes its format from the **pod's** tap format
(`TapPlaneConfig::format`), since there is no leg to negotiate one; a member
whose rate or ptime differs is refused by name by item 37's `accepts`.

**t=0 is the conference's open, for both recording shapes.** `Conference` stamps
`opened_at: Instant` (handed to the mix thread, which uses it as its release
epoch) and `opened_at_wall: SystemTime` at `start`. Two consumers of that
instant:

- an **ungrouped** recording on the room session gets
  `RecorderSpec.group_anchor = opened_at_wall` (`room_anchor_of`), so a recorder
  attached late pads back to the open through item 29's `lead_with_silence`
  seam. `resume_ms > 0` still clears it, exactly as for a group: an adopted
  recording's spilled frames already carry the lead;
- `group_anchor_for(session)` — item 54's seam, which returned
  `SystemTime::now()` — now returns the **conference's** open for any session
  seated in one, so the per-participant group of the same conference anchors on
  the same instant. The two shapes are therefore sample-aligned by construction
  rather than by being attached together.

`RecordingShape` is untouched: a room object is still `conference-mixed`,
because `conference_of` answers for the room session too. A **group** on the
room session is refused by name (a room has no participant seat), and item 39's
grouped-mixed refusal on a member is unchanged — recording the room off a member
is still possible, and the D20 row now says the room session is the way not to.

**Lifetime.** `Conference` gained `room: Option<RoomSeat>` (the owning session
and its external id) and `seated_ever`, and `unseat` returns `RoomFate`:
`Mixing` (members remain), `Stopped(JoinHandle)` (today's behaviour — no room
session, so the last leg out closes the conference) or `Emptied` (a room session
holds it open). On `Emptied`, `close_session` either ends the room session
**synchronously** — the default, `MSS_CONFERENCE_LINGER_SECS=0` — or arms a
linger. `EndSession` on the room session ends its attachments (so the recording
uploads), unseats the room and stops the mix only if no member remains; members
that remain keep mixing with `ConferenceView.room_session` empty.

**The linger is a spawned sleep, not a housekeeping tick.** There is no
control-world sweep in this daemon (only `RegistryKeeper::run` and
`health::watch` recur), so a linger is one `tokio::spawn(sleep(linger))` per
emptied conference, its `JoinHandle` parked in `Conference::linger` and
**aborted by `seat`** — a member that rejoins cancels it — and by `stop_now`.
On expiry the task re-checks under the conference lock that the room is still
empty before ending it. The task needs the plane, and a `&TapPlane` cannot be
moved into a task, so `main.rs` hands the plane a `Weak<TapPlane>` of itself
(`linger_through`, the `observe_through`/`discover_through` idiom). **With no
weak self set** — which is every test that does not ask for a linger — a
non-zero linger warns and the room stays open until the API ends it; that is the
safe direction (nothing is lost, a mix idles). Item 56 may replace this with a
general sweep; nothing here builds one.

**Ending a session from the media plane needed one new seam.**
`ObservationSink` gained `fn session_finished(&self, session, reason)`, a
defaulted no-op implemented by `SessionController` as
`registry.destroy_session(...)` through `commit`, so an auto-ended room
publishes `AttachmentDown`/`SessionEnded` on `mss.events` in its own sequence
and frees its external id. That is the **only** way the media plane ends a
control-plane session, and it exists because a room session has no hangup of its
own to be told about. The plane closes its own half first (attachments, then the
mix), so the uploads are already in flight — item 50's `finishing` state keeps
the record alive for the late `UploadCompleted`.

**Playback on a room session.** `playback_reach` takes the session kind: on a
room, an empty target **or** `all` is the room prompt, and `own` is refused by
name (a room has no ear). So `StartPlayback` on the room session is a room
prompt, `StopPlayback` flushes the room queue, and — because the generic
non-inline INJECT path turns an utterance plus a `Mark` into a `StartPlayback`
blob on its own session — an INJECT attachment on the room session is a room
prompt too, capped by `MAX_UTTERANCE_SAMPLES` and the room's prompt queue.
`StartPlayback{target_tag=all}` from a **member** still works; no verb was
removed. Route and member verbs (`mix_target`, `member_mute`…) on the room
session are refused by `Conference::{route,control}`'s `NotSeated`, since the
room is not a member of itself.

**Read-back.** `MemberStateView` grew `room_session`, `opened_at` and `seated`;
`Conference::room_state()` builds the room's view (members, room session,
conference open, no flags, `seated: false`) and `member_state` fills its own
fields over it. `TapPlane::member_state` answers with the room's view when the
session **is** the room, so `session_message` reports `Session.conference` for a
room and omits `Session.member` (a room is not a member of itself), and
`Session.opened_at_unix_ms` is the **conference's** open for a MIX session and
the registry's own stamp for everything else. Proto: `Session.opened_at_unix_ms
= 15` and `ConferenceView.room_session = 4`, both additive; **the next free
`Session` field is 16 and the next free `MediaEvent` payload tag is still 28**.

**Adoption.** `PersistedSession::is_room()` (kind 3) joins `is_inline()` under a
new `is_pod_bound()`, and `is_rebuildable()` now excludes it **explicitly** —
it was already false by the empty `call_id`, which is an accident this makes a
rule — so `registry_keeper::adopt_orphans` releases a room record with a message
that says why instead of half-restoring it. A conference is one pod's threads;
nothing about that changed.

**New metrics.** `mss_conference_rooms_live` (gauge: conferences owned by a room
session) and `mss_conference_rooms_auto_ended_total`.

### conference.rs + tap_plane.rs + main.rs — member state with a lease (item 56, closes D22, 2026-08-27)

Item 40 made `member_mute` / `member_deaf` / `member_hold` deliberately outlive
the attachment that set them, and item 49 made them readable. What was still
missing was an end: a controller that died between `on` and `off` left a member
muted for the life of the conference. This item bounds that with a **lease**, and
deliberately does **not** answer the ownership question item 40 rejected — the
deadline sits beside the flag it bounds, so nothing has to decide whose mute it
is.

**The wire and the parser (session-core/src/mix.rs).** A fourth metadata key,
`member_state_ttl_ms` (`MEMBER_STATE_TTL_METADATA_KEY`), parsed in
`MemberControl::from_metadata` into `MemberControl.ttl_ms: Option<u64>`. The
`Option` is load-bearing: `None` is "the request named none, take the pod's
default" and `Some(0)` is "explicitly no lease", which is why the field is not a
bare `u64`. A value that is not a whole number is refused by name through a new
`MixRouteError::MemberStateTtl { key, value }`, the same shape as
`MemberFlag`. Two helpers carry the policy: `lease_ms(default_ms)` resolves the
default, and `MemberControl::releasing(mute, deaf, hold)` builds the release —
`Some(false)` for each named flag, `None` for the rest — so an expiry is
literally the same value an `off` would produce. A TTL with **no** flag set `on`
in the merged metadata still parses to `None` overall (`is_empty` counts flags
only): it qualifies a flag, it does not set one, and an `UpdateAttachment`
carrying `member_mute=off` beside a leftover TTL must stay legal.
`MemberStateView` grew `mute_expires_in_ms` / `deaf_expires_in_ms` /
`hold_expires_in_ms` (plain `u64`, `0` = no lease), which is the read-back shape.

**The deadline lives in the control-world mirror.** `MirroredMember` gained
`mute_until` / `deaf_until` / `hold_until: Option<Instant>`, written by the same
call that writes the flag. `Conference::control` is now a thin wrapper over
`control_at(session, control, now)`; for each flag it sets, it sets the deadline
to `now + ttl` when the flag goes **on** with a non-zero lease and clears it
otherwise. `off` therefore clears the flag and its deadline in one path, as
before. **No timer went anywhere near `Mixed::run`**: the mix thread still only
receives `ConferenceCommand::Control`, and it cannot tell an expiry from an
explicit `off` — which is the point.

**Expiry is applied through the exact same path an `off` takes.**
`Conference::expire_member_state(now)` collects the members whose deadlines have
passed (`MirroredMember::expired_flags`, a pure function of the mirror and
`now`), then calls **`control_at`** for each with the release — the same mirror
write and the same enqueue. It counts one `member_state_expired` per **flag**
lifted, logs the member, and returns `(SessionId, MemberControl)` pairs. A
conference whose command queue is momentarily full logs and leaves the deadline
in place, so the next sweep retries: an expiry that cannot be enqueued is
postponed, never dropped. That needed one ordering fix in `control_at` — it now
verifies the seat, **enqueues the command, and writes the mirror last**, where it
used to write the mirror before a push that could fail. A failed `Control` used
to leave the mirror claiming a state the mix thread had never been told about
(and, for an expiry, with the deadline already cleared, so nothing would retry).
Every caller of `control` gets that fix, not only the sweep.

**The sweep is the control world's, on a tick main.rs did not have.**
`TapPlane::sweep_member_state(now)` takes the conference-table lock once, runs
`expire_member_state` over every conference, **drops the lock**, and only then
publishes one `Observation::MemberStateExpired` per released member through the
plane's existing `observe` seam — the same one DTMF and the recording callbacks
use — so no observation sink is ever called with the conference table held. `main.rs` spawns a `tokio::time::interval`
of `MEMBER_STATE_SWEEP` (**500 ms**, `MissedTickBehavior::Delay`) that calls it;
that is the daemon's first housekeeping tick, and it is why a flag lifts up to
half a second after its lease runs out. **A deliberate deviation from item 56's
letter:** the spec had the *caller* turn the returned pairs into events. The
plane emits them itself, because the plane is what already holds the
`ObservationSink` (`observe_through`) and every other media-initiated event goes
out that way; `main.rs` only logs a count. The pairs are still returned, and that
is what the unit tests assert against.

**The event, and the registry mirror behind it.** `Observation` gained
`MemberStateExpired { mute, deaf, hold }` — *which flags expired*, not the
resulting state. `SessionRegistry::observe` maps it through
`release_member_state`, which does two things at once: it rewrites `on` to `off`
for the expired keys in **every attachment metadata map of that session**, so a
later `UpdateAttachment` diffs `controlled_before` against reality rather than
against a stale `on` (without this, re-muting after an expiry would be a no-op
and publish nothing), and it folds the session's attachments back together to
report the state that is **left**. So an expiry event carries `mute: false` while
a hold that was never leased stays `true`. The event takes the session's next
`seq` like any other, with `attachment: None`, because nobody asked for it. The
TTL key itself is left in the metadata on purpose: a later `member_mute=on` with
no TTL of its own then re-leases at the same length the client last asked for.

**`EventKind::MemberControlled` gained `cause: MemberControlCause
{ Requested, Expired }`** (`session-core/src/event.rs`, `Default = Requested`),
and proto `MemberControlled.cause = 4` with
`MEMBER_CONTROL_CAUSE_REQUESTED = 0`, so a consumer written before this reads
every controller-driven event byte-identically. `MemberState` gained
`mute_expires_in_ms = 6` / `deaf_expires_in_ms = 7` / `hold_expires_in_ms = 8`.
**All additive: the next free `Session` field is still 16 and the next free
`MediaEvent` payload tag is still 28.** `convert.rs` grew
`member_control_cause_wire` and three fields in `member_state_wire`.

**Deployment and instruments.** `TapPlaneConfig.member_state_ttl: Duration` is
the pod default, applied in `TapPlane::control_member` — which rewrites
`ttl_ms` to `Some(lease_ms)` before it reaches the conference, so exactly one
place resolves the default. `MSS_MEMBER_STATE_TTL_SECS` sets it (default `0` =
today's behaviour; an empty value is treated as absent rather than as a parse
failure). New counter `mss_conference_member_state_expired_total`. `mss_ctl
member <attachment> mute on ttl 30000` sends the key, and
`lab/conference_drill.sh` grew `MUTE_TTL_MS`: the mute phase leases instead of
sending an `off`, waits out the lease plus one sweep and asserts the counter
moved. The drill has **not** run — the lab stack was down for this session.

**What the lease is not.** It is pod-local (it is an `Instant` in a pod's
memory), so it neither survives a pod loss nor moves with a member; conferences
are pod-bound anyway. It is not an owner: two controllers muting the same member
still race, and the last lease wins. And with no TTL — still the default —
nothing about member state changed at all.

### tap_plane.rs — the control plane's hands in the media world

`TapPlane` implements `control_api::MediaPlane` over the machinery the
Phase-0 spike proved, which is what turns `MediaControl` from a registry
into something that actually taps calls.

It serves three session kinds: `Tap` (`open_tap_session`, an rtpengine
subscription), `Inline` (`open_inline_session`, MSS's own socket, optionally
seated in a conference) and — since item 55 — `Mix` (`open_room_session`, the
conference room itself, with no leg and no ports; see the room-is-a-session
section above for its lifetime, its anchor and its refusals).

- **Transcoding at the tap is now a choice, not a constant (2026-08-22).**
  `TapPlaneConfig.transcode_at_tap` (daemon env `MSS_TAP_TRANSCODE`, default
  `on`, so nothing changed for an existing deployment) selects between two
  shapes:
  - **on** — `transcode: [PCMU]` as before. rtpengine normalises whatever the
    carrier picked, the tap format is `config.format`, and the answer adds
    payload type 0 to the offer, which is what requests the conversion.
  - **off** — an empty transcode list. rtpengine converts nothing, the tap
    format comes from the offer (`offered_tap_format`), and the answer echoes
    the offered codec first so it adds nothing. A call whose codec the
    pipeline cannot decode is **refused by name**, with the error naming
    transcoding as the fix, rather than tapped into silence.
  Why it exists: transcoding cannot happen in rtpengine's kernel module — the
  module forwards and can do SRTP, but carries no codec — so asking for a
  transcode is very likely what keeps a subscription in rtpengine's userspace.
  This flag is the MSS half of testing that (tasks.md item 18, probe 2); the
  rtpengine half needs a host that can load the module, which this lab cannot.
  It is also strictly *less* total codec work: today rtpengine decodes and
  re-encodes and then MSS decodes again (three operations); with the flag off
  MSS simply decodes the wire codec (one).
  **`offered_tap_format` requires every offered stream to agree** on one
  format, because a tap decodes one format for all its legs; disagreement is
  an error naming transcoding as the fix. Two legs of one call have never
  disagreed in the lab, and if they ever do, transcoding is the right answer
  rather than a per-leg pipeline.
- **Detach and hangup end a consumer politely (2026-08-23, item 27, defect
  D3).** `close_attachment` and `close_session` both go through
  `TapPlane::end_attachment`, which ends the hub subscription and *waits*
  (up to `POLITE_CLOSE`, 2 s) for the consumer to finish on its own instead
  of aborting its task: the WS consumer drains its queue and sends the
  Twilio `stop` frame, the gRPC pump drains and then a
  `StreamFrame::Stop { reason }` goes down the stream as `StreamStop`. The
  reason names why ("the attachment was detached" / "the call ended"), which
  is the difference a consumer can act on. A consumer that will not finish in
  time is still aborted, with a warning; a gRPC consumer too far behind for
  one more frame gets the channel close instead of the `Stop`. The cost of
  the fix is that `Detach` can now wait up to 2 s on an unresponsive
  consumer — the same shape as D11's upload wait, and worth watching in a
  pilot.
- **Pause reaches consumer transports (2026-08-23, item 27, defect D10).**
  `update_attachment` sets the subscription's pause flag for `Ws` and
  `Grpc` attachments as well as calling `RecorderHandle::set_paused` for
  recordings. A gRPC attachment holds its pause state (`LiveAttachment::Grpc
  { paused }`) even before a consumer subscribes, so `open_stream` applies it
  to the new subscription — a stream that opens while paused stays silent
  until it is resumed.
- **`stop_playback` is aimed (2026-08-23, item 27, defect D2).** The
  `MediaPlane::stop_playback` signature gained the playback's `target_tag`,
  which `SessionRegistry::stop_playback` now returns (`StoppedPlayback`)
  from the `PlaybackRecord` it wrote at start. `target_of` maps it to
  `PlayTarget::HeardBy(tag)`, or `HeardByEveryone` (`all: all`) when the
  playback was for everyone — which the lab probe showed is exactly what an
  `all: all` playback needs, since a targeted stop only removes one
  participant from it.
- **`LiveSession` now carries the format the tap actually settled on**, and
  `session_format()` is what the attachment paths consult. This matters
  because `config.format` stopped being the truth the moment transcoding
  became optional: a gRPC attachment's format is validated against the real
  tap (`ConsumerEncoder::supports(tap, requested)`) and the encode pump is
  handed the real tap format as its source. The WS guard is unchanged and
  still demands PCMU 8k — that is the frozen dialect the bridge speaks, and it
  is independent of the tap's codec because the hub carries PCM and the bridge
  encodes µ-law at its own edge.
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
- **Which participant is the *customer* is a separate question, and MSS refuses
  to guess it (D17, item 47).** SSRC correlation answers "which stream carries
  which participant's voice"; it says nothing about which participant called.
  That came from `from_tags[0]`, and when the caller's tag was not supplied
  `complete_from_tags` filled `from_tags` from `NgReply::tags()` — a `BTreeMap`,
  so **lexicographic by tag**. `order_participants()` (pure, in `tap_plane.rs`)
  now sorts the participants by their `created` stamp (stable, unstamped last)
  and yields `Attribution::Inferred` only when the first two differ strictly;
  otherwise `Unknown`. Under `Unknown`, `convert::track_name_under` /
  `tracks_under` rename the gRPC stream tracks, the gRPC media/DTMF frames, the
  event payload tracks and the recording group's object keys to
  **`leg_a`/`leg_b`**; `consumer_ws::track_name` is untouched, because the
  Twilio dialect is frozen (Article VII). `LiveSession` and `SessionHandles`
  carry the verdict so every attachment opened later names itself consistently,
  and `Observation::LegsAttributed` reports it back to the registry, which is
  what puts `attribution` on `DescribeSession` and on the bus. See the
  rtpengine-ng section for why `created` cannot separate a two-party call's legs,
  and `lab/leg_attribution_drill.sh` for the live proof in both directions.
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
- **Known gaps at the time — all three since closed**, kept because the middle
  one's reasoning still binds: the Redis registry landed 2026-08-17 (a tap now
  outlives its pod by adoption); `stop_playback` targets the playback's own
  participant since item 27 (D2) — but rtpengine's `stop media` still targets a
  participant and **not** a playback id, so two playbacks aimed at one
  participant cannot be stopped independently, which is D2's measured residual;
  and `close_attachment` closes politely since item 27 (D3).

#### Inline sessions (item 33, 2026-08-23)

`open_session` now dispatches on `SessionKind`: `Tap` keeps the whole
subscribe/answer/capture path unchanged as `open_tap_session`, `Inline` runs
`open_inline_session`, `Mix` is refused naming Phase 4. The trait signature
changed with it — `MediaPlane::open_session` returns `OpenedSession { sdp_answer
}` instead of `()`, which is how the answer reaches `CreateSession`'s response
without a second RPC or a side channel.

What an inline session shares with a tap, deliberately: one `TapLeg`, one
`StreamPipeline`, one `Hub`, one capture thread. The peer is
`Track::Customer`, so consumers, recorders, recording groups, DTMF observation
and the metrics that were built for taps all work on an inline leg with no new
code. What differs:

- `LiveSession.transport` is now `Option<Arc<NgTransport>>` — an inline leg has
  no rtpengine subscription at all. `session_handles` returns it as an option
  (as the named `SessionHandles` struct, since the tuple had grown past what
  clippy tolerates) and `require_subscription` turns `None` into an error that
  says why. `close_session` skips the `unsubscribe`.
- **`StartPlayback` is a local mix-in.** `play_into_inline_leg` decodes the wav
  (16-bit mono at the negotiated rate; anything else refused naming the
  mismatch, since resampling a prompt is the caller's decision) and queues it in
  100 ms chunks, refusing the *whole* playback up front if the queue has no room
  rather than playing a truncated prompt. The 60 KB NG-datagram cap moved into
  `ng_play_source`, where it belongs — it is a property of rtpengine's control
  protocol, not of audio.
- **`StopPlayback` on an inline leg flushes the egress queue** and returns.
  That is the barge seam end to end: `Clear` → queue emptied → `pacer.clear()`
  → the next tick is a silence frame. P3-4 measures it; the construction
  already bounds it at one ptime.
- The answer is not persisted. See `session_store.rs` on why an inline session
  is not adoptable.
- **The plane reaches the session store for one thing only: recording groups.**
  `share_groups_through(Arc<dyn SessionStore>)` is a `OnceLock` set from
  `main.rs` right where the keeper is built, the same shape as
  `discover_through`. It is consulted once per grouped `FILE_S3` attachment and
  once when one leaves — control world, never a frame path. See *Recording
  groups as a shared record* under `recorder.rs`.
- **A `group` on an inline session makes it a conference leg (item 37).**
  `open_inline_session` builds the same socket, pipeline, hub and egress and
  then, instead of spawning a per-session capture thread, hands the bundle to
  `conference.rs` through `seat_in_conference`; `LiveSession` remembers the
  name so `close_session` can `leave_conference` and join the mix thread when
  it was the last member. A refused join (rate/ptime mismatch, a full
  conference, a mix that is not draining its command queue) leaves **no**
  half-open session behind: the error comes back before the session table is
  touched.

### main.rs — how the daemon chooses what to be

Three modes, in priority order: the Phase-0 tap spike when its env vars are
set (unchanged scaffolding), the **control plane** when
`MSS_CONTROL_LISTEN` is an `ip:port`, and otherwise an idle process that
waits for a signal — **SIGTERM or SIGINT** since item 42, which also gave the
control-plane mode the bounded drain sequence in `drain.rs`. `MSS_RTPENGINE_NODE` becomes the default node for
sessions that do not name one, `MSS_TAP_LOCAL_IP` the media address, and
`MSS_POD_NAME` the `owner_pod` reported by `DescribeSession`.

Verified against the running binary, not just in tests: `CreateSession`
toward an unreachable rtpengine returns
`Unavailable: subscribe request: no reply from rtpengine ... after 3
attempts`, and the follow-up `DescribeSession` returns `NotFound` — the
rollback works in the daemon, not only against the test fake.
`crates/control-api/examples/mss_ctl.rs` is the small client used for that
and is the quickest way to poke a running control plane by hand; it covers
`create`, `describe`, `attach` (ws), `consume` (a `GRPC_STREAM` consumer with
`SINK`+`EVENTS`, so it may report speech — added 2026-08-23 for the barge
drill, since `attach` only makes WS consumers), `record` (the `FILE_S3`
identity), `pause`, `detach`, `play` and `destroy`.
`crates/control-api/examples/mss_stream_probe.rs` is its data-plane sibling:
it attaches a `GRPC_STREAM` consumer, subscribes, and writes what it hears
as a wav per track with rms and peak — the tool the item-10 lab proof used.

### drain.rs — the shutdown sequence (item 42, G1, 2026-08-26)

The module exists because a graceful shutdown is a *sequence with a deadline*,
and a sequence is only trustworthy if it can be tested without a lab. So the
steps are a trait (`DrainSteps`) and the ordering + budgeting is a pure function
over it (`run_drain`), with `ControlPlaneDrain` in `main.rs` as the only real
implementation. The unit tests use a fake that records call order and can hang
on any one step; time is `tokio::time::Instant` throughout (**not**
`std::time::Instant` — the tests run under `start_paused`, where the std clock
does not move and every budget assertion would be meaningless).

- `next_shutdown_signal()` selects over `ctrl_c()` and
  `SignalKind::terminate()`. Both mediaserverd modes use it — the control plane
  and the idle "no `MSS_CONTROL_LISTEN`" process. A platform without SIGTERM
  degrades to SIGINT with a warning rather than failing to start.
- `DrainState` is a single `AtomicBool` behind an `Arc`. It is deliberately not
  the controller's `watch` channel: a readiness probe wants a cheap synchronous
  read from an HTTP handler. `metrics.rs` renders it as `mss_draining`, and G4
  will answer `/readyz` from the same flag.
- `exit_on_second_signal()` spawns a task that `exit(0)`s on the next signal.
  Registering a second SIGTERM stream is fine; tokio's handler is process-wide
  and never unregisters, so an impatient operator gets an immediate exit rather
  than the default disposition.
- Budget arithmetic: one deadline for the whole drain, and each step gets
  `remaining - reserve`, capped. Lease hand-off is capped at `budget/4` and the
  event flush holds a reserve of `min(10 s, budget/3)`. The point is that a hung
  Redis (or a hung anything early) cannot consume the window that closes
  consumers and finishes recordings. A step that expires or is skipped is named
  in the log and in `DrainReport`; the process still exits 0, because the pod is
  being replaced either way.

Why the step order is what it is: the lease is handed off **before** the taps are
unsubscribed, so the adopter can re-subscribe while our copy is still flowing.
That allows a brief double subscription rather than a gap; rtpengine gives each
subscription its own to-tag, so our unsubscribe cannot touch the adopter's. The
alternative order guarantees a hole in the consumer's audio, which is the defect
item 11 measured at 14.41 s.

Spilling live recordings is not a step of its own. `close-sessions` calls
`destroy_session`, which is `TapPlane::close_session` — it ends every attachment
(the D3 polite path), and a recording attachment's `finish()` uploads or, on
failure, spills to `MSS_RECORDING_SPILL_DIR` for the next boot's
`recording_spill::salvage`. A separate spill step would duplicate that fallback
and race it.

### Related changes in the modules around it (item 42)

- `session_store.rs` gained `SessionStore::release_lease(external_id, owner)`:
  DEL the lease key when we still hold it, **keep** the session record and the
  index entry. `forget` (an ended call) and `release_lease` (a call that should
  move) are different operations and the drill's assertions depend on the
  difference.
- `registry_keeper.rs` gained `hand_off_leases()`, which drains
  `persisted_here` through `release_lease` and counts
  `mss_registry_handed_off_total`. `run` now takes `Arc<Self>` so `main.rs` can
  keep the keeper alive after aborting its renew task — the abort must come
  first, or a later `persist_and_renew` tick would `forget` records the adopter
  now holds.
- `control-api`: `create_session` and `attach` answer
  `UNAVAILABLE: this pod is draining` (`describe`/`destroy`/`detach` keep
  working — a draining pod must still be able to close its own work).
  `begin_drain` now uses `watch::Sender::send_replace`: `send` is a **no-op when
  no receiver is alive**, so a pod with no live `MediaStream` could be told to
  drain and stay `draining=false`. That was a real latent bug in the existing
  `serve_authenticated_until` shutdown path, found by a unit test.
- `main.rs`: the tonic server now runs as a task whose shutdown future waits on
  the controller's drain watch, rather than being awaited inline with a `ctrl_c`
  future inside it. Awaiting it inline meant a long-lived `MediaStream` could
  hold the whole shutdown open with nothing bounding it.

### media_ports.rs — the media port range and the advertised address (items G2 + G3, 2026-08-26)

`MediaPortAllocator` is the only thing in the daemon that binds a media socket.
`ephemeral()` (no range configured) binds port 0 and behaves exactly as the code
did before this item; `over_range(min, max)` keeps a `VecDeque` of the **even**
ports in the range behind a `Mutex` and hands them out front to back.

`bind(local_ip)` returns a `BoundMediaSocket { socket, port, lease }`. The lease
is the whole lifetime story: `PortLease::drop` decrements `in_use` and pushes the
port back, so a port is released by *dropping the thing that owns it* rather than
by remembering to call a free function on every path. `LiveSession` holds
`Vec<PortLease>` (two for a two-leg tap, one for an inline leg), and
`TapPlane::close_session` takes them out **after** joining the capture thread and
logs `released_ports` — the order matters: a lease dropped before the thread
joins could hand a port to a new session while the old socket is still bound to
it. Drain reaches the same path through `destroy_session`, so a drained pod
returns its whole range.

A candidate port that will not bind (another process holds it) is skipped and
counted, up to `BIND_ATTEMPTS_PER_REQUEST` (64) per request; the skipped ports go
back on the free list, because the squatter may be gone by the next call. Only an
empty free list is a refusal — `MediaPortError::RangeExhausted`, which names the
range in its message and bumps `mss_media_ports_exhausted_total`.

The advertised address is a plain `IpAddr` on `TapPlaneConfig`
(`advertised_media_address`), read once at startup by
`media_ports::advertise_address(local)`. Every SDP that names MSS to a peer now
goes through one of two pure functions in `tap_plane.rs`, `tap_answer_sdp` and
`inline_answer_sdp`, which take the advertised address as an argument — that is
what makes the "advertised in, bind address absent" assertions unit-testable
without a socket. Nothing else in the daemon renders an address into SDP.

Not in the range, deliberately: the NG control socket (`NgTransport::bind`). It
is an outbound flow to rtpengine's 22222 and the range exists to be opened
*inbound* on a firewall; the lab drill prints those sockets so the distinction
stays visible rather than looking like a leak.

`lab/media_port_drill.sh` is the live check. It reads `/proc/net/udp` inside the
container (the lab's rust image has no `ss`), so it sees every UDP socket the
process holds, and asserts: sockets inside the range while tapping, ingest
datagrams climbing, nothing inside the range once the session is destroyed, and
`in_use` back to `capacity`.

### discovery.rs — the optional call-id → rtpengine node map (item 51, G11, 2026-08-26)

Three pieces, deliberately small. `parse_mapped_node(&str)` is pure and holds
every format decision: a value that does not start with `{` must parse as a
`SocketAddr` (the bare `host:port` form); one that does is a `MappedNode`
(`node`, optional `from_tags`, optional `caller_tag`) and the caller's tag is
moved to the **front** of the tag list, deduped, with blanks dropped. It returns
`Result<DiscoveredNode, String>` — a `String` because the only consumer logs it.

`NodeMap` is the one-method trait (`read(key) -> Option<String>`) that keeps the
lookup testable without Redis. `RedisSessionStore` implements it through its new
`read_key`, which is a plain `GET` on the connection helper the registry already
uses: **this daemon has one Redis client type**, and a second one would have been
a second place to get connection handling wrong. `NodeDiscovery` owns the prefix
and the counters and does the logging; `resolve(call_id)` returns
`Option<DiscoveredNode>` and *never* an error, because there is no failure mode
here that should reach the caller — every one of them is "use the default node,
and say so".

In `tap_plane.rs` the entry point is `discover_through`, a `OnceLock` set after
the store connects (the plane is built before the registry in `main.rs`, so the
wiring has to be late — the same shape as `observe_through`). `resolve_node`
replaced the direct `node_for` call in `open_tap_session` and returns
`ResolvedNode { node, view, caller_named }`. Two things about it are load-bearing:

- `caller_named` starts as "the request named from_tags" and is **only** raised
  by a map value carrying `caller_tag`. It is passed into `complete_from_tags`,
  which used to derive the same fact from `from_tags.is_empty()` — that
  inference is exactly what a map-supplied tag list would have broken, silently
  promoting an unattributed call to `explicit`. Now a full tag list with no named
  caller returns `Attribution::Unknown` and warns, and a partial one lets the
  `query` complete the set but still cannot claim a direction (`seeded` in that
  function);
- the map fills `view.from_tags` **only if the request left them empty**, and
  truncates to `MAX_TAPPED_LEGS`. A caller who named tags is never second-guessed.

When both tags come from the map the tap issues **no `query`** — that is the
whole point of the map, and the reason `from_tags` is in the value format at all.

Not done here, and worth knowing: the resolved node is not persisted into the
session registry, so an adopting pod re-reads the map rather than inheriting the
answer. That is fine while the call is up and the key's TTL holds, and it is why
`deploy.md` says the TTL must outlive the longest call.

`metrics.rs` renders the three counters only when `MetricsSources.discovery` is
`Some`, which it is only when a prefix is configured — the same "absent beats a
lying zero" rule the pump and keeper counters follow.

### health.rs — the readiness snapshot behind /readyz (item 44, G4, 2026-08-26)

`Readiness` is the whole design in one sentence: a `Mutex<[DependencyHealth; 3]>`
that background tasks write and the HTTP handler only reads. Nothing on the
request path touches a socket, so a kubelet probe with a 1 s timeout cannot be
made to hang by a wedged Redis — the worst it can read is a stale verdict, which
is bounded by `MSS_HEALTH_PROBE_INTERVAL_SECS` (default 10).

The three dependencies are fixed (`Dependency::{Rtpengine, Redis, Kafka}`) and
indexed by `slot()` into the array — a fixed array rather than a map because the
set is not extensible at runtime and the exposition wants a stable order. Each
entry carries a `Condition` (`NotConfigured`, `NotProbedYet`, `Ready`,
`Failing(reason)`), the optional address to name in the report, the last `Instant`
it answered, and its consecutive-failure count. `NotProbedYet` is deliberately
**not ready**: a pod that has not learned its state must not take traffic. In
practice nobody sees it, because `main.rs` records `Ready` or `NotConfigured` for
Redis and Kafka at connect time and for rtpengine at its first-contact ping — all
before the metrics listener binds.

`snapshot()` derives everything the two consumers need (the `/readyz` body and the
`mss_ready` / `mss_dependency_ready` series) in one lock pass; `verdict()` renders
the body. Its first line is the machine-readable part — `ready`, or `not ready: `
followed by the reasons joined with `; ` — because `kubectl describe` shows only
the first line of a failed probe's body. The rest is one line per dependency plus
`draining: yes|no`. Draining is read live off `DrainState`, not recorded, which is
why `/readyz` flips 503 on the *first* drain step (`stop_accepting`) with no new
wiring: the live check caught the transition 7 ms before the listener went away.

`watch(readiness, probe, interval)` is the only loop. It sleeps
`next_probe_delay(interval, consecutive_failures)` — the interval on success, and
1 s → 2 → 4 → 8 → capped at the interval while failing — then runs the probe under
a 15 s timeout, a timeout being a failure like any other. The backoff runs
*upward from short*, the opposite of a retry backoff: a failing dependency is
re-probed sooner than a healthy one, because the pod wants back into service as
soon as the outage ends. The three `HealthProbe` impls are thin:
`SessionStoreProbe` → `SessionStore::ping` (Redis `PING` on a fresh multiplexed
connection), `EventBusProbe` → `EventTransport::reachable` (a partition-offset
fetch, never a produce, so probes leave no records on `mss.events`; the trait
method defaults to `Ok` so test transports keep meaning what they meant), and
`NgNodeProbe` → NG `ping`, which on success calls
`NodeCapabilityLog::observe` (`report_first_contact` until item 57 moved the
per-probe sampling in behind it) and on failure calls the new
`NodeCapabilityLog::forget`. That pair is what closes item 23's open re-probe: a
node's capabilities are re-learned the first time it answers after any failure,
instead of the pod keeping a dead node's verdict forever.

Routing lives in `metrics.rs::route`, a pure function over the request bytes so
every status code is a unit test rather than a socket test. It reads only the
request line, splits the query/fragment off the path, answers `/metrics`,
`/healthz` and `/readyz`, and returns **404** for anything else and **405** for a
non-GET. That last part is a behavior change: the listener used to serve the
exposition for *any* path. Every lab script, `soak.py` and the alert rules ask for
`/metrics` by name, so nothing in the repo depended on it — but an operator with a
scrape config pointing at `/` will now get a 404.

`MetricsSources` gained `readiness`, and `main.rs` passes the same `Arc` to the
listener and the watchers. `probe_configured_rtpengine_node` grew from a one-shot
into "probe once inline, then spawn the watcher", and now also treats an empty or
whitespace `MSS_RTPENGINE_NODE` as unset (item 43's rule) and a malformed one as a
permanent failure rather than as "no node configured".

### hub.rs — the fan-out core (M3), first increment
The per-session pub/sub the roadmap calls the fan-out hub. Since item 55 a
`Hub` is not always a session's leg: a conference owns one more of them, the
**room hub**, whose only publisher is the mix thread and whose owner is the room
session — same struct, same bounded rings, same drop-oldest accounting. Two-worlds
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
- **Two shapes of "everything" (2026-08-23, item 27, defect D13).**
  `TrackSelection::All` means every track *including* `Mixed`;
  `TrackSelection::Speakers` means customer and agent only. Consumers are
  attached as `Speakers` for `TrackSelector::All` so that what arrives
  matches the `tracks` their start frame advertised, while the recorder is
  attached as `All` because injected bot speech belongs in the recording.
  A consumer that wants the injected track asks for it by name
  (`TrackSelector::Only(Mixed)`), which is advertised as `["mixed"]`.
  The frozen Twilio start frame was not touched.
- **Pause lives on the subscription (2026-08-23, item 27, defect D10).**
  `Subscription::control()` hands the control world a
  `SubscriptionControl` — a clone of the shared state — with
  `set_paused` and `end_of_stream`. `publish` checks the flag per frame:
  a paused consumer's frame is skipped and counted in
  `suppressed_while_paused` (exported as
  `mss_consumer_suppressed_while_paused_total`), so pause costs nothing
  downstream and resume starts at the live edge instead of replaying a
  backlog. `publish` also skips a closed subscription rather than filling
  a ring nobody will read.
- `Subscription::next()` is async and cancel-safe (pop-then-wait against a
  `Notify`; a permit stored by a racing publish is consumed on the next
  poll). Hub drop, detach, or `SubscriptionControl::end_of_stream` closes
  the subscription: `next()` drains what is queued, then returns `None`,
  which is what tells the WS consumer to send `stop`. `end_of_stream` is
  how the control world asks a consumer to finish politely without the
  session ending (defect D3).
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
  Opus output remains open (tasks item 16d, which superseded item 9).
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
- **On an INLINE session inbound media flows straight to the leg** (2026-08-23,
  item 34). `ConsumerConfig.egress: Option<InlineEgressHandle>` is set by
  `tap_plane` only when the attachment declared INJECT *and* the session has an
  inline egress; `InlineInject` then holds it. With it present, each `media`
  event's µ-law is decoded and pushed immediately (no utterance, no 700 ms idle
  flush, no `BridgeCommand`), `clear` flushes the queue so the next paced frame
  is silence, and `mark` is remembered and acked with the dialect's existing
  `Outbound::Mark` bytes once the queue drains past its watermark — polled every
  20 ms, capped at 64 outstanding (oldest dropped, since the dialect has no
  error frame). The optional `sampleRate` must equal the leg's rate or the frame
  is counted as an unknown encoding; the *encoding* need not match the leg's,
  because the dialect is decoded to PCM and a PCMA leg re-encodes on the way
  out. Without the handle every branch behaves exactly as it did for taps.
- **The inbound dialect carries no speech report, and never will (D19, found
  2026-08-23, closed 2026-08-23).** `media`/`mark`/`clear`/`end_of_interaction`
  are all a WS consumer can send, and this dialect's bytes are frozen (Article
  VII), so there is nowhere to put one. The fix went to the **native** surface
  instead: `ConsumerToServer.SpeechReport` on the gRPC `MediaStream` stream
  reaches `SessionRegistry::report` (see stream.rs above), and
  `lab/barge_drill.sh` now triggers on a real report — cut-through p95 4.8 ms,
  see lab.md. The consequence for this adapter is permanent and worth stating
  plainly: **a WS consumer cannot report speech**. Its only barge stays `clear`,
  which takes a different road — a direct rtpengine `stop media` from
  `tap_session`, unevented and untargeted. Interactive voice-AI that needs
  barge-in should attach over gRPC.
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
- `version()` and `statistics()` landed 2026-08-23 (item 23). `statistics()`
  returns the parsed `RtpengineStatistics` rather than the raw reply, so a reply
  without a `statistics` dict is a `MissingField` error instead of a report of
  all zeroes; `version()` returns the raw reply because the interesting outcome
  is the *error* (`Unrecognized command`) and the caller must see it.
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
- **Both since done.** Real lab validation against rtpengine landed with the
  Phase-0 spike and every drill since. Re-subscribe on pod loss lives in
  `registry_keeper.rs` and was proved on a live `kill -9` (tasks item 11), with
  the orphaned subscription it exposed fixed in item 25. A node registry keyed
  by call→node discovery is still absent and no longer needed: MSS resolves a
  call's participants itself (see the section above).

### rtpengine_capability.rs — the first-contact capability log (2026-08-23, item 23)
Answers, in the daemon's own log, "what is this rtpengine and can my taps use
its kernel module?" — once per node, on the first NG contact with it.
- Two pure decisions, both unit-tested, no I/O: `VersionReport::from_outcome`
  maps a `version` round-trip onto `Reported` / `NoVersionCommandOnThisNode` /
  `Unavailable(reason)` — and only the exact remote reason
  `"Unrecognized command"` becomes "this protocol has no version command", so a
  timeout never gets misreported as a missing feature.
  `TapKernelVerdict::decide(kernel, transcode_at_tap)` is the eligibility rule:
  **transcoding wins over everything** (a transcoded tap is a userspace tap
  whatever the node is doing), otherwise the node's `KernelForwarding` decides.
- `report_first_contact` claims the node in a `HashSet` **before** awaiting, so
  concurrent `open_session` calls on a cold node cost one probe, not N.
- Two call sites: `main`'s startup NG probe (so a daemon with
  `MSS_RTPENGINE_NODE` reports before serving) and `TapPlane::open_session`
  (so a node first seen through a session's own `rtpengine_node` is reported
  too). One shared `Arc<NodeCapabilityLog>` carries the set across both, which
  is also why `transcode_at_tap()` is now read once in `main` and handed to
  `TapPlaneConfig` — it used to be read twice and logged twice.
- The transcoding verdict is logged at **WARN**, everything else at INFO,
  because "your taps cannot use the kernel module" is an operational finding
  rather than a status line.
- Verified live against the lab (2026-08-23) in both modes. With
  `MSS_TAP_TRANSCODE=on`: `version="unknown: this rtpengine's NG protocol has
  no version command"`, `relayed_packets_in_kernel=0`,
  `relayed_packets_in_userspace=130865`, plus the transcoder chain
  `["PCMU/8000 -> opus/48000/2"]` and the WARN verdict. With `off`: the same
  facts and the "no kernel path for a tap to ride" verdict.
- **The sample is no longer log-only (item 57, 2026-08-26).** `NodeCapabilityLog`
  now also keeps `last: Mutex<HashMap<SocketAddr, NodeSample>>`, and a
  `NodeSample` is the verdict plus the numbers handoff H3 asks for:
  `relayedpackets_kernel`/`_user` (totals since the node started),
  `media_kernel`/`_userspace`/`_mixed` and `transcodedmedia` (current), the
  node's live session count, and the `Instant` the sample was taken.
  `NodeSample::from_statistics` is pure, so the mapping is a unit test; `samples()`
  hands `metrics.rs` a node-ordered `Vec` under one lock; `forget` clears the
  sample as well as the reported-once set, so a node that stops answering stops
  being reported rather than freezing at its last numbers.
- **Two entry points now, because they cost different things.** `observe` always
  takes a `statistics` round-trip and refreshes the sample, and on the *first*
  contact with a node it also asks `version` and writes the whole first-contact
  log line. `report_first_contact` is `observe` behind a read of the
  reported-once set, so it is a lock and a return for a node already seen.
  `health.rs`'s `NgNodeProbe` calls `observe` (one extra NG command per node per
  `MSS_HEALTH_PROBE_INTERVAL_SECS` — per probe, never per packet, never on the
  media path); `main`'s startup ping and `TapPlane::open_session` keep calling
  `report_first_contact`, so opening a session on a known node still costs
  nothing. That is also why the *log* stays once-per-node while the *sample*
  refreshes: a line per probe interval per node would be noise.
- Still not done: `verdict` is re-decided from each sample, so an rtpengine
  restarted under a running daemon gets a fresh verdict on the next probe, but
  the first-contact **log line** (and the version report in it) is not re-emitted
  unless the node fails a probe first.

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
- **`release_frame_with` is the mixer's seam (item 37).** A released frame is
  handed to an optional `MixedFrameSink` (`&mut dyn FnMut(&[i16])`) alongside
  the hub publish, so `conference.rs` pushes that leg's contribution into the
  mix matrix without a copy or a second buffer, and `release_frame` stays the
  one-line no-sink case every tap uses. `drain` and `MAX_DATAGRAM` are `pub`
  for the same reason: the conference loop is a second media loop over the same
  `TapLeg`, not a fork of it.
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
starts (a diagnostic must not be able to stop the service). Since 2026-08-23 a
successful ping is followed by the first-contact capability report
(`rtpengine_capability.rs`), which is why the probe now takes the shared
`NodeCapabilityLog`; a node that fails the ping is not probed further.

### media_rt.rs — thread/tick skeleton real; the real capture went elsewhere
The worker loop still only ticks and counts, and that is not a gap in the media
path: the plan below was realised in `tap_spike.rs` (per-leg capture, one thread
per tap), `inline_leg.rs` (the egress pump) and `conference.rs` (one owner
thread per conference), each driven from `tap_plane.rs` rather than from this
generic worker. This module is kept as the shape a future *shared* media worker
would take if per-session threads ever stop scaling. Per-iteration plan:
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

Re-run 2026-08-22 for tasks item 19 (Article VIII asks for the benchmark after a
pipeline change, and items 14/17 were that), same machine, `cargo bench -p
media-core` four times:

| Run condition | `parse_jitter_decode_per_packet` | `ingest_only_per_packet` |
| --- | --- | --- |
| item 17's recorded numbers | 269.8 ns | 25.6 ns |
| **quiet box, lab stack stopped** | **262.1 ns**, 281.3 ns | **26.1 ns**, 26.2 ns |
| lab stack resident, straight after a 47 min soak | 308.5 ns, 302.2 ns | 28.1 ns, 27.8 ns |

**No regression.** The quiet-box pair brackets the item-17 row, and
`git log -- crates/media-core` shows no commit has touched the crate since item
17's, so the +14% criterion reported on the first attempt was the eleven
resident lab containers, not code. The useful by-product is a repeatability
number for this box: **±9% run to run on the full path**, and ~15% just for
leaving the lab up. Stop the lab before any future Article-VIII comparison here,
and treat sub-10% deltas as noise.

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
Since item 30 the audio does not all stay in memory: closed segments spill to
local disk as the call runs — see `recording_spill.rs` below for the journal,
the restart salvage and what adoption can and cannot recover.
Since item 39 the same recorder also serves a conference, both as one mono
object of the room and as one object per participant — see *conference.rs +
recorder.rs — native conference recording* above for the two shapes, the
`RecordingStarted.shape` vocabulary and the mix-clock invariant.

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
- **A spill write never holds the recorder's hub drain (review fix to item 53,
  2026-08-27).** Closing a segment used to `await` the journal's `append` on the
  loop, bounded only by `SPILL_TIMEOUT` (10 s); the hub subscription behind it is
  `CONSUMER_QUEUE_FRAMES` = 200 frames, drop-oldest, so under
  `MSS_RECORDING_SPILL_TO=s3` a bucket that was slow but inside the timeout cost
  up to 6 s of the recording itself every spill interval — the "blocking I/O on
  the pump" class architecture §7.1 forbids. The write is now a spawned task and
  the loop only renders, seals and commits; see *the write is off the recorder's
  loop* under `recording_spill.rs` below for the seam, the seal and the finish
  path. `a_slow_spill_store_never_costs_the_recording_a_frame` is the proof: a
  3 s-per-put store, a real 200-frame subscription, 500 frames at ptime, zero
  dropped — the same test against the old loop dropped exactly 100.
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
  loses the audio — that is the honest state, and the alert says so. Since item
  30 the same directory also carries the **as-it-runs** segment journal, so the
  spill is no longer only a last-resort dump on a failed upload.
- **Bounded, like everything else.** *(Superseded in part by item 30 — see
  `recording_spill.rs` below: closed segments now spill to disk as the call
  runs, so only the live tail is in memory. The cap below still applies.)* The
  recording was buffered entirely in
  memory: 8 kHz stereo is ~32 KB/s, so `MAX_RECORDING` (2 h) caps one
  recording at ~230 MB and further frames are counted
  (`frames_beyond_cap`, `mss_recordings_truncated_total`) rather than
  silently dropped. Streaming multipart upload is the fix when calls longer
  than that matter; it is not built.
- **`StopRecording` no longer waits for the upload (item 50, D11).** The
  recorder task now has two phases. Phase one is the capture loop; when it ends
  it publishes `RecordingStopped`, sends a `StopReport`
  (duration/frames/segmenter stats) down a oneshot, and only then goes on.
  `RecorderHandle::finish()` awaits that report — bounded by `STOP_TIMEOUT`
  (5 s), which is a segment close and no I/O — and hands the still-running task
  back as a `FinishedCapture`. Phase two acquires an upload permit, renders,
  encodes, uploads and publishes `UploadCompleted` or the new `UploadFailed`.
  So `Detach`/`DestroySession` answer as soon as the audio is safe, and the
  upload's own event arrives later. `FINISH_TIMEOUT` (90 s) is now a
  test-only helper (`FinishedCapture::settle`), not a production bound; the
  production bounds are `STOP_TIMEOUT`, `UPLOAD_TIMEOUT` (60 s per object) and
  `recording_uploads::UPLOAD_SETTLE_TIMEOUT` (10 min).
- **Bounded background upload concurrency.** `RecordingSupport` carries an
  `Arc<Semaphore>` sized by `MSS_RECORDING_UPLOAD_CONCURRENCY` (default 4,
  `upload_concurrency_from_env`, logged at startup). The permit is taken *after*
  the stop is reported, so a queue of uploads never delays a detach — it only
  delays the uploads. The startup salvage pass does **not** take a permit: it
  runs once, before any call, and serialising it against nothing would only slow
  a restart.
- **A failed upload now says so.** `Observation::UploadFailed { recording_id,
  key, error }` → `EventKind::UploadFailed` → proto payload tag **28** (the next
  free one). Before this, a recording that never reached storage produced
  `RecordingStopped` and then silence, so an integrator waiting for
  `UploadCompleted` waited forever. Both the wav-encoding failure and the
  upload failure emit it, with the store's own message as `error`, and the audio
  is still spilled for the D9 salvage pass.
- **Known gaps:** no multipart/streaming upload (hence the cap and the memory
  cost); `recordingChannels=mono` metadata is not honoured (a single-track
  *selector* gives a mono file, a mono *mix* of both parties does not exist);
  no re-upload of spilled files (an operator job today); and a recording is
  per pod, so a pod that dies mid-call loses the audio it had buffered even
  though the session itself is adopted elsewhere (the adopted session
  re-taps, but the recording restarts).

### recording_uploads.rs — the background upload watch (item 50, D11, 2026-08-26)

`UploadTracker` is the only thing that knows an upload outlived the RPC that
stopped it. `TapPlane::finish_recording` calls `adopt(finished, observer)`, which

- asks the observation sink to **retain the session for the upload** before the
  detach returns, so the registry cannot forget it in between;
- counts the hand-off (`mss_recording_uploads_backgrounded_total`) and keeps a
  live gauge (`mss_recording_uploads_in_flight`);
- spawns one small watcher per upload that awaits the recorder task with
  `UPLOAD_SETTLE_TIMEOUT` (10 min), logs what landed, releases the retention and
  then notifies `wait_idle`. On expiry it aborts, counts
  `mss_recording_upload_settle_timeouts_total`, and releases anyway — a stuck
  upload must not pin a session record forever, and the audio is on the spill
  disk for the D9 salvage pass.

`wait_idle()` is what the drain waits on: `drain.rs` gained an `await-uploads`
step between `close-sessions` and `control-plane-idle`, because an upload that
settles after `flush-events` would have its event stranded in the outbox when
the process exits. The step returns how many were in flight when it began
(`DrainReport::uploads_settled`) — informational, and deliberately not asserted
anywhere: it is a racing number by nature.

### The gapless-sequence problem, and why the session record is kept (item 50)

Publishing an event after the session ended is the whole difficulty of D11.
`push_event` assigns `seq` from the session record and **silently drops** an
event whose session is gone, so a backgrounded upload would lose
`UploadCompleted` on every hangup — which is why the blocking detach existed.

Two designs were on the table. **Reserving a seq** at detach and publishing the
late event with it keeps the sequence gapless but not *monotonic*: while the
session lives on (a `StopRecording` that is not a hangup), later events take
higher numbers and the reserved one arrives after them, so a consumer with a
low-water mark stalls on a hole that is already spoken for. **Keeping the
session record alive** costs one `BTreeMap` entry per settling upload and keeps
both properties, so that is what landed:

- `SessionRecord` gained `pending_uploads` and `finishing`.
  `retain_for_upload` / `release_after_upload` bracket a background upload;
  `destroy_session` marks the record `finishing` instead of removing it when
  `pending_uploads > 0`, and the last release removes it for real.
- A finishing session is **not** live: `session_ids()` and `session_count()`
  skip it, so `snapshot()`, the registry keeper and the drain never see it, and
  `live()` (the new guard used by `attach`, `start_playback` and
  `destroy_session`) refuses it with `UnknownSession` — a finishing session
  cannot be adopted, attached to, or ended twice.
- The `external_index` entry **is** dropped at destroy, so the external id is
  reusable immediately. The consequence, deliberate: `DescribeSession` by
  *session id* still answers a finishing session, by *external id* does not.
- `ObservationSink` gained `retain_for_upload` / `release_after_upload` with
  default no-op implementations, so every test fake still compiles; when nothing
  retains, the behaviour degrades to exactly what it was before (the late event
  is dropped and logged).
- For a consumer of `mss.events` this means one new rule:
  **`UploadCompleted`/`UploadFailed` may arrive after `SessionEnded` for the
  same session, and is then the last event of that session's sequence.**

### recording_spill.rs — the segment journal and the restart salvage (item 30, D9, 2026-08-23)

The recorder no longer holds a whole call in RAM until the end. Two seams do
the work, and both are deliberately dumb:

- **Peek then commit, in the segmenter.** `closable_frames()` returns the
  whole-millisecond prefix that can be closed (millisecond-aligned so the
  anchor rebase below is exact), `render_closable(frames, layout)` renders it
  per target *without* mutating, and `close_segment(frames)` drops it from
  memory. The recorder writes before it commits, so a failed disk write costs
  nothing: the audio stays in memory and the next tick retries. `close_segment`
  advances `anchor_ms` by exactly the milliseconds removed instead of clearing
  it, which is what keeps customer/agent alignment across a segment boundary —
  the opposite of `pause()`, which clears the anchor on purpose so the paused
  wall-clock gap is dropped. `spilled_frames` keeps `duration_ms()`,
  `total_frames()` and the `MAX_RECORDING` cap honest across spills.
- **The journal on disk.** `<MSS_RECORDING_SPILL_DIR>/journal/<first object
  key>/` holds `manifest.json` plus `<target index>-<seq>.pcm` — raw
  interleaved i16 LE, no header, one chunk stream per target of one recorder
  (a group member with two mono objects keeps both under the member's own
  directory). The manifest names the recording id, sample rate, owning pod,
  frames on disk and the chunk list per key, and is replaced by atomic rename
  after every segment. Chunks and manifest are written on `spawn_blocking`.
  Segments close every `MSS_RECORDING_SPILL_SECONDS` (default 30, `SPILL_EVERY`)
  and on every pause.

At finish the recorder reads the journal back and prepends it to the tail it
still holds, so **one** object lands at the frozen
`${accountID}/${recordingID}.${format}` key and the journal is deleted. A
recording that never reached storage leaves its journal behind on purpose.

`salvage()` runs in `main.rs` before the daemon serves: every journal on this
pod's disk is stitched and uploaded. It asks `RecordingSink::exists` first
(`object_store`'s `head`, via `ObjectStoreExt`) and **skips** a key that is
already in storage — that is the case where another pod adopted the session and
finished the object, and overwriting it with this pod's prefix would be data
loss. Skipped and failed journals stay on disk, counted
(`mss_recording_salvage_skipped_total`, `mss_recording_salvage_failures_total`)
and logged with their path: **the spill directory has no retention policy, an
operator owns it.**

**Adoption.** `PersistedAttachment.recording` (`{recording_id, owner,
recorded_ms, spilled_ms}`, `serde(default)`) is filled from the live recorder's
`RecordingProgress` through `TapSubscriptions::recording_journal(attachment)`
and the keeper stamps the owning pod. `rebuild` derives
`mss.recording.resumeMs` / `mss.recording.spillOwner` into the rebuilt
attachment's metadata (and `persisted_from` strips both, so they are re-derived
each time and never accumulate); `open_recording_attachment` reads them into
`RecorderSpec.resume_ms`. The recorder then opens the journal at the same key —
a same-pod restart finds its own segments and continues them — and turns
whatever the registry says was recorded but is not readable here into leading
silence (`Segmenter::lead_with_silence`, item 29's seam), counting every such
frame in `mss_recording_frames_lost_on_adopt_total`. Past `MAX_ADOPT_LEAD`
(5 min) the padding is refused rather than allocated, since the pad is real
memory; the loss is still counted.

**The limit item 53 removed:** with `MSS_RECORDING_SPILL_TO=disk` (the default)
the spill directory is per-pod local disk, so a cross-pod adopter reads nothing
and the dead pod's audio becomes counted silence. With `s3` the journal lives in
the recording bucket and any pod reads it back — see the next section.

### recording_spill.rs — the journal in the recording bucket (item 53, D9, 2026-08-26)

The journal grew a backend seam so the same journal can live on local disk or in
the recording bucket, and the recorder above it did not change shape at all.

- **`SpillStore`** is the seam: `write(journal, manifest, chunks)`,
  `read(journal, chunks)`, `read_manifest(journal)`, `list_manifests()`,
  `remove(journal)` and `describe(journal)`. A *journal* is named by the first
  target's object key — the same identity the disk layout already used — so
  `SpillManifest` is untouched and a `disk` journal written before this change
  is still read back byte for byte.
- **`DiskSpill`** is today's code, extracted: `<MSS_RECORDING_SPILL_DIR>/journal/
  <first object key>/manifest.json` plus `<target index>-<seq>.pcm`, atomic
  rename for the manifest, every syscall on `spawn_blocking`.
- **`ObjectSpill`** writes the same layout into the recording bucket under
  `MSS_RECORDING_SPILL_PREFIX` (default `_spill/`), through the same
  `RecordingSink` the finished object goes to: `_spill/<first object
  key>/manifest.json` and `_spill/<first object key>/<target index>-<seq>.pcm`.
  Chunks go up first and the manifest last, so a manifest never names a chunk
  that is not there. Every call is bounded by `SPILL_TIMEOUT` (10 s), and the
  whole write once more by the same bound in `SpillWrite::perform`; a timeout is
  counted like any other failed spill and the audio stays in memory for the next
  tick. The recorder task does **not** await it — see the next block. The prefix
  is **reserved**: nothing else may be
  written under it, and it is documented in deploy.md beside the frozen
  identity, with a bucket lifecycle rule (expire `_spill/` after 7 days) as the
  retention policy MSS still does not implement.
- `RecordingSink` therefore gained `get`, `list` and `delete` beside `put` and
  `exists` (`object_store` has all three; `list` walks the `BoxStream` and
  returns plain keys). `UploadError::Missing` is the "not there" answer `get`
  needs, so a first-ever `read_manifest` is not logged as a failure.
  `S3RecordingSink` maps `NotFound` to it for both `get` and `delete`.

**The write is off the recorder's loop (review fix, 2026-08-27).** The first
cut of this item awaited `SegmentJournal::append` inline on the `segment_close`
tick and on `Pause`. While that await was pending the recorder drained nothing
from its hub subscription — `CONSUMER_QUEUE_FRAMES` = 200 frames, 4 s at 20 ms,
drop-oldest — so a bucket that was slow but inside `SPILL_TIMEOUT` cost up to
6 s of audio from the recording itself, every spill interval; disk had the same
shape and merely completed in milliseconds. The invariant now is **a spill write
never holds the recorder's hub drain**, and it is built as follows:

- **The journal's append is split at its I/O.** `SegmentJournal::begin_append`
  is synchronous: it validates, names the chunks from the current `sequence`,
  builds the next manifest and returns an owned `SpillWrite` (store handle,
  journal name, owner, next manifest, encoded manifest body, chunks).
  `SpillWrite::perform` is the I/O — the ownership re-read, then `store.write`
  — under `SPILL_TIMEOUT`, and returns `SpillWritten::{Landed(manifest),
  Surrendered(owner), Failed(error)}`. `SegmentJournal::commit(manifest)` is the
  book-keeping (`manifest = landed; sequence += 1`). `append` still exists as
  `begin_append → perform → commit` and behaves exactly as before, but it and
  `surrendered()` are `#[cfg(test)]` now — production consumes
  `SpillWritten::Surrendered` directly — which is what keeps the ownership,
  salvage and adoption tests (and `tests/minio_upload.rs`) untouched.
- **The recorder spawns the write and keeps draining.** On a tick with no write
  pending, `begin_spill` renders the closable prefix (`render_closable`, in
  memory, no mutation), calls `begin_append`, **seals** that many frames in the
  segmenter and hands `perform()` to `tokio::spawn`, remembering the frame count
  as an `InFlightSpill`. `close_segment` is **not** called yet: the frames stay
  in memory, the segmenter keeps accepting, and `closable_frames()` only grows,
  so the sealed count stays valid. `run`'s `select!` gained an arm that receives
  the outcome: `Landed` → `commit`, `close_segment(frames)` for exactly the
  written count, `segments_spilled` and `spilled_ms` as before; `Failed` →
  counted, frames stay in memory, the next tick retries with a larger prefix;
  `Surrendered` → `spill_lost_ownership`, journal handle dropped, as before.
  **At most one write is in flight per recording**; a tick while one is pending
  is a no-op (`if journal.is_some() && in_flight.is_none()` on the arm).
- **`Pause` goes through the same mechanism**, not an inline await: the pause
  edge calls `begin_spill` and, if a write is already pending, does nothing —
  the paused frames are the whole closable buffer, and the next tick or the
  finish path carries them. A `Resume` or `Finish` queued behind a pause is
  therefore never deaf behind a bucket (session-playbook §7).
- **The seal.** `Segmenter::seal(frames)` marks the prefix a write is carrying;
  `accept` shifts a frame whose position would land inside it to the seal
  boundary and counts it in `frames_out_of_order`, which is exactly where the
  old inline close would have put a straggler for the same tick (its timestamp
  fell before the advanced anchor, so it saturated to the segment start).
  `unseal()` on a failed or surrendered write, and `close_segment` clears it.
  `a_frame_that_lands_inside_a_sealed_prefix_is_kept_at_the_boundary` pins both
  halves.
- **Finish.** The capture loop breaks, drains `try_next`, publishes
  `RecordingStopped` and sends the `StopReport` **first** — the report is still
  a segment close and no I/O, so `STOP_TIMEOUT` still holds — then awaits the
  pending write under `SPILL_TIMEOUT` (aborting it on expiry) and settles it
  before `read_back`, so the stitch never races a chunk. If it landed, the
  frames are closed and read back from the store; if it failed, they are still
  in memory and rendered as the tail: the object is complete either way. The
  `RecordingOutcome.stats` are re-read after that settle, so
  `segments_spilled` there counts a write that landed at finish; the
  `StopReport.stats` snapshot predates it by design.
- **Abort on drop.** `InFlightSpill` aborts its task when dropped, so a recorder
  task that is itself aborted (the pod-loss tests, a kill mid-write) cancels its
  write rather than leaving an orphan that could overwrite an adopter's freshly
  claimed manifest — the same cancellation the inline await gave for free.

Tests, all under paused tokio time on a real `Hub` subscription of
`CONSUMER_QUEUE_FRAMES` (moved from `tap_plane.rs` to `hub.rs`, beside the
subscription it sizes, so the tests can name it) with frames published at ptime: `a_slow_spill_store_never_costs_the_recording_a_frame` (3 s
per put, 500 frames, `dropped_oldest` stays 0, the object holds all 500; the same
test against the inline await dropped 100),
`a_spill_write_that_fails_leaves_its_frames_in_memory_for_the_next_tick` (a
refused write is counted, `spilled_ms` stays 0, the retry carries ≥ 600 ms, the
object is complete) and
`finishing_while_a_spill_write_is_failing_still_uploads_every_frame` (stop while
a doomed write is pending: the stop report does not wait on it, the failure is
counted before the outcome, nothing lands in the spill store, the object is
complete). Every pre-existing spill, adoption and salvage test passes untouched.

**Ownership lives in the manifest, and it is what makes this safe.**
`SpillManifest.owner` already existed. `SegmentJournal::open` on an adopting pod
finds a journal that *continues* its recording, rewrites `owner` to itself and
**writes the manifest immediately** — the claim happens before the first
`append`, not with it. Every `append` re-reads the manifest first: if `owner` is
no longer this pod, the append is refused, `surrendered()` goes true,
`spill_closed_segment` counts `mss_recording_spill_lost_ownership_total`, drops
the journal handle and keeps recording into memory. A partitioned-but-alive pod
therefore cannot corrupt an adopted journal, and it also cannot delete it: with
no journal handle there is nothing to `discard`.

**Read-back no longer depends on which pod spilled.** `PersistedRecording.owner`
is still carried to the adopter as `mss.recording.spillOwner` (it is what the
log needs to say whose audio is missing), but it gates nothing: `recorder::run`
reads back whatever the configured store holds for that journal and pads only
the frames that are in neither memory nor the store. With `s3` that makes
`mss_recording_frames_lost_on_adopt_total` at most one
`MSS_RECORDING_SPILL_SECONDS` on **any** pod, which is the invariant
`an_adopter_on_any_pod_loses_at_most_one_spill_interval_when_the_journal_is_in_the_bucket`
is named after.

**Salvage stays same-pod.** `recording_spill::salvage` (still `main.rs`, before
the daemon serves) now lists manifests through the store, and a manifest whose
`owner` is another pod is **left alone** and counted
(`mss_recording_spill_foreign_manifests`) — with a shared store, salvaging a
journal another pod is still writing would race it, and adoption is the cross-pod
path. Its own journals behave exactly as before, `exists`-first and never over an
object that already reached storage.

**What still costs audio:** the unspilled tail (bounded by the spill interval),
a pod that dies between its last spill and the adopter's `open`, and the
`_spill/` objects of a recording that never finished on any pod, which nothing
expires — retention is the operator's, and that is D9's remaining residual
together with the live drill.

### recorder.rs — the recording store is a flag: S3 or a filesystem (item 58, 2026-08-27)

`RecordingSink` had one production implementation, so "which store" had never
been a decision. It is now one variable, and nothing above the trait knows which
one it got.

- **`decide_store(store, root, object_store_variables_set)`** is the whole
  policy, and it is a **pure function** — the env vars are read by `from_env`
  and passed in, so the matrix is unit-tested without touching the process
  environment (there is no env lock in this crate; every existing env test
  set/removes and hopes). It returns `StoreDecision { store, ignored }`:
  `RecordingStore::ObjectStore` for unset/empty/`s3` (case- and
  whitespace-insensitive), `RecordingStore::Filesystem(root)` for `filesystem`
  with an absolute `MSS_RECORDING_ROOT`, and `UploadError::Configuration`
  otherwise — an unknown store, or `filesystem` with no root or a relative one.
  `from_env` propagates that error, and `main.rs` already refuses to start on it,
  the same path an unusable bucket takes. `ignored` is the cross-set variables:
  the bucket/S3 names under `filesystem`, `MSS_RECORDING_ROOT` under `s3`. They
  are named in **one** `warn!` and then ignored; that list is what the test
  asserts, not the log line.
- **`probe_recording_root`** is the startup probe: `create_dir_all`, write
  `.mss-recording-store-probe.part`, rename it over
  `.mss-recording-store-probe`, delete it. Write-but-cannot-rename is a real
  failure mode of some FUSE and SMB mounts, and it would otherwise surface as
  every recording failing mid-call. It is plain `std::fs` on purpose: it runs
  once, before the listener binds, and keeps `from_env` synchronous.
- **`FilesystemRecordingSink`** is `tokio::fs`, not `object_store`'s `fs`
  backend, whose path rules (percent-encoding, its own notion of a valid path)
  are not ours. `put` creates parents, writes a sibling
  `.<basename>.<owner>.<part>` file, `sync_all`s it and **renames** it over the
  final name, so a watcher never reads a half file; the owner in the name is the
  pod (`MSS_POD_NAME`, non-alphanumerics folded to `_`), so two pods writing the
  same key cannot share a temp file. A failed write removes its part file. The
  returned URI is `file://<root>/<key>`, which is what `UploadCompleted.uri`
  carries — the attachment transport stays `file-s3`/`TRANSPORT_FILE_S3`, frozen
  (Constitution VII), and the URI scheme is what says which store answered.
- **Keys are validated, not trusted**: empty, absolute, backslash-separated, and
  any empty/`.`/`..`/control-character segment are `UploadError::Key`, so no key
  can name a path outside the root. `get` on a missing file is
  `UploadError::Missing`, which is exactly what `ObjectSpill::read_manifest`
  distinguishes from a real failure.
- **`list(prefix)` matches the S3 sink's contract**, which `ObjectSpill` and
  `salvage` depend on: the trailing `/` is trimmed, the remainder names a
  directory that is walked recursively, and the keys come back **relative to the
  root** with `/` separators — the same shape `meta.location.as_ref()` gives.
  A missing prefix is an empty list, and `.<name>.part` names are skipped, so a
  half-written file is invisible to salvage as well as to a watcher. Keys are
  sorted, which the S3 listing effectively is too.
- **`delete`** treats NotFound as success (as the S3 sink does) and then prunes
  now-empty parent directories up to the root, so a removed `_spill/` journal
  does not leave a skeleton of directories behind.
- **No new no-overwrite rule.** `exists` is consulted in exactly one place —
  `recording_spill::salvage` — so that is where "never over an object that
  already exists" lives, unchanged; the sink itself is last-write-wins like
  `put_opts`. `a_second_put_replaces_the_file_because_the_no_overwrite_rule_lives_in_salvage`
  is the test that pins which layer owns the rule.
- **`MSS_RECORDING_SPILL_TO`** accepts `store` as a synonym for `s3` (`disk`
  unchanged), and with the filesystem store `ObjectSpill` is built over the
  filesystem sink exactly as it is over S3 — the journal lands at
  `<root>/_spill/…` on the shared volume, so cross-pod adoption works there too.
  `a_journal_spilled_onto_the_filesystem_store_is_salvaged_into_the_identity_path`
  runs a real `SegmentJournal` → `salvage` round trip on a temp directory and
  reads the WAV back from `<root>/acct-42/rec-99.wav`.

**Verified against a real temporary directory** (`std::env::temp_dir()` plus the
existing `scratch()` helper, cleaned up per test): the five sink methods, the
`.part` file invisible to `list` and gone after `put`, refused escaping keys, the
journal round trip and salvage, the probe (including a root under a plain file),
and the `decide_store` matrix. **The lab drill did not run** — no Docker daemon
on this machine, so nothing here has been observed on a live call; the WAV
assertion is the unit test's, not `track_dump.py`'s.

**Operationally:** `lab/preflight.sh --recording-root DIR` runs the same
write/fsync/rename/delete probe from outside (and SKIPs the S3 check when the
store is `filesystem`), `deploy/k8s/overlays/filesystem-recording` is the
variable plus an RWX PVC at `/var/lib/mediaserverd/recordings`, and
`validate_fields.py` now asserts that a `filesystem` store's root is a mounted
volume backed by a `ReadWriteMany` claim — with RWO the second pod does not
schedule and adoption silently recovers nothing.

**What is not solved by choosing a filesystem:** retention (nothing expires
finished recordings or `_spill/` journals, exactly as in a bucket), and the fact
that a full or hung volume fails every pod at once. S3 remains the default and
the recommendation.

### Recording groups — N sessions, one recording (item 21, landed 2026-08-23)

A FreeSWITCH conference is N SIP dialogs = N rtpengine calls = N MSS sessions
(a session is one call-id and at most `MAX_TAPPED_LEGS` = 2 legs), so recording
a conference cannot be one session's job. A **recording group** is the join:
`AttachRequest.group` (proto field 11, additive) puts a `FILE_S3` attachment
into a group keyed `(accountID, group)`, and every member writes **its own mono
object** under the recording's own prefix.

- **Keys.** `${accountID}/${recordingID}/${label}.${format}` per participant,
  and `${label}.customer` / `${label}.agent` when a member selects both tracks.
  The endpoint is still the frozen identity
  `${accountID}/${recordingID}.${format}` — `RecordingIdentity` is unchanged
  and gained one method, `participant_key`. An **empty group is byte-identical
  to before**: same single object at `object_key()`, pinned by
  `an_ungrouped_recording_still_writes_the_frozen_two_leg_identity`.
- **Why per-participant files and not one N-channel WAV.** Each member is a
  different rtpengine call with its own RTP clock and its own tap start time.
  Interleaving them into one file would make one slow member's buffer the whole
  conference's. Separate files keep the members independent; the group time
  anchor below is what makes them line up anyway, so a consumer can lay the
  objects side by side without consulting the event timeline.
- **The label is a path segment, so it is validated.** `participant_label`
  refuses empty, `/`, whitespace, control characters and `.`/`..` by name; an
  attachment with no label falls back to the session's `external_id`.
- **A recorder now has targets, not a layout.** `RecorderSpec.targets` is a
  `Vec<RecordingTarget { key, layout }>`, and `Segmenter` stopped filtering by
  layout at `accept` — it always buffers customer/agent/mixed and **renders**
  per target (`render(Layout)`), so one member's task can write two mono files
  from one buffered call. That is what keeps the callbacks honest: one
  `RecordingStopped` per member however many files it writes, and one
  `UploadCompleted` per object, whose `uri` is what disambiguates the
  participant. `RecordingOutcome.uri` became `uris`.
  `RecorderSpec::one_object` is the ungrouped constructor.
- **Refusals, all by name and all counted**
  (`mss_recording_group_joins_refused_total`): a second member reusing a
  participant label (it would overwrite the first participant's object), a
  member naming a different `recordingID` in a group that already has one, and
  a non-empty group on any transport other than `FILE_S3` — the frozen Twilio
  dialect is two-track by construction and multi-party gRPC is its own item.
  Pause is per member: `UpdateAttachment{paused}` reaches that member's
  recorder only.
- **The group was in one pod's memory in v1, and is a shared record since item
  54.** `TapPlane` still holds
  `groups: Mutex<HashMap<GroupKey, RecordingGroup>>` — it is the in-process
  cache and the source of the `mss_recording_groups_live` /
  `mss_recording_group_members_live` gauges, which stay **per pod** — but what
  a group *is* now lives in the session store. See *Recording groups as a
  shared record* below.

### The recording-group time anchor (P2-1, closes D18, 2026-08-23)

D18: every member's segmenter anchored on **its own first frame**, so a member
that joined ten seconds into a conference produced a file whose sample 0 was
ten seconds later than the first member's — two objects of different lengths
with nothing in the audio to say where the second one starts. Reassembly
needed the `RecordingStarted` event timeline, and the two-node drill's members
came back 90.32 s vs 90.26 s.

Now **the group owns t=0**. `RecordingGroup` records `opened_at: Instant` when
its first member joins; `join_group` returns that instant to every later
member, `RecorderSpec.group_anchor: Option<Instant>` carries it into the
recorder task, and when that member's **first media frame** arrives the task
turns `now - anchor` into leading silence via
`Segmenter::lead_with_silence(Duration)`. All members therefore share t=0 and,
if they stop together, the same length to within one frame.

- **Why the first frame and not the attach.** The pad has to cover everything
  between group open and the member's own audio, including its subscribe
  round-trip; measuring at the first frame folds that in, and the same latency
  on every member cancels out of their relative alignment.
- **`lead_with_silence` is the sans-IO seam** — a `Duration` in, no clock
  inside `Segmenter` — so the padding, the pause interaction and the
  equal-length property are all replay tests. Wall time is read once, in
  `absorb`, which is control-plane code.
- **Pause cannot double-count it.** The pad is written into `segment_start`
  exactly once (guarded by `stats.lead_silence_frames`, which is also the
  reported quantity: `SegmenterStats.lead_silence_frames`, logged as
  `lead_silence_frames` when the file closes). A later `pause` sets
  `segment_start = frames().max(segment_start)`, so a pause before the first
  frame cannot erase the pad and a pause after it cannot re-add it.
- **The pad counts against `MAX_RECORDING`** — a member joining an hour into a
  two-hour cap has an hour of its own audio, not two — and against nothing
  else: an ungrouped recording passes `group_anchor: None` and is
  byte-identical to before.
- **A spill must not advance the anchor past the pad** (D23, found by the
  conference drill, item 41). `close_segment(frames)` drops `frames` from the
  front of the buffers, so the timestamp that now maps to buffer index 0 moved
  by `frames - segment_start`, **not** by `frames`: the first `segment_start`
  frames of what was dropped were the pad, which no timestamp ever mapped to.
  Advancing `anchor_ms` by the whole `frames` subtracted the pad a second time
  and every frame after the first spill landed one pad-length early, so the
  member's file kept its pad and lost that much audio off its tail (55.88 s
  against 66.16 s live, before the fix). Only a *padded* recording could see it:
  `segment_start` is 0 for every ungrouped one. `a_padded_member_keeps_its_whole
  _tail_across_a_spill` fails by exactly the lead if this is reverted.
- **Unchanged:** the frozen identity, per-member pause and the refusal shapes.
  The anchor's *type* changed with item 54: it is a `SystemTime`, because two
  pods cannot compare each other's `Instant`s. Everything above holds
  unchanged — `lead_with_silence` still takes a `Duration`, and `absorb` still
  reads the clock exactly once.

### Recording groups as a shared record, not one pod's memory (item 54, closes D16, 2026-08-27)

D16: `TapPlane::groups` was the whole truth about a recording group, so every
member of one conference recording had to attach to the same pod. There is no
placement that guarantees that (D8), a member whose session was adopted
elsewhere was **refused** rather than restored (its participant object simply
ended at the pod that died), and a group name reused on a second pod silently
produced a second half-recording under the same object prefix.

**The group is now a record in the session store**, with `TapPlane::groups`
demoted to a per-pod cache. Two keys beside the session keys, same namespace:

- `mss:group:<account>/<group>` → JSON
  `GroupRecord { recording_id, format, opened_at_unix_ms, created_by }`,
  created with `SET NX`. The loser of a race **reads the winner back**, so two
  first-members on two pods agree on one `recording_id` and one anchor.
- `mss:group:<account>/<group>:members` → hash `object key → owner pod`.
  `HSETNX` is the duplicate-participant refusal, and because the value is the
  pod, a cross-pod refusal can name the pod already writing that object. `HDEL`
  on leave; both keys are `DEL`ed (best effort) when the hash empties, and both
  are `EXPIRE`d at `GROUP_RECORD_TTL` (3 h = `MAX_RECORDING` + 1 h) on every
  join as the backstop against a pod that dies without leaving.

`SessionStore` gained `open_or_join_group` / `leave_group`; `RedisSessionStore`
implements them with the commands above, and `MemorySessionStore` mirrors the
semantics exactly so every `TapPlane` test runs the store path without Redis.
`TapPlane` reaches the store through `share_groups_through(Arc<dyn
SessionStore>)`, a `OnceLock` set from `main.rs` beside the keeper — the same
idiom as `discover_through`. **No store configured means the old, pod-local
behaviour**, which is right for a single pod.

Decisions, and why:

- **A store error refuses the grouped attachment.** `join_group`'s failure path
  never falls back to a local-only group: a half-group is worse than no group,
  because the member would open a second recording under a prefix another pod
  is already writing. The refusal is counted like every other
  (`mss_recording_group_joins_refused_total`) and names the registry.
- **The anchor became wall-clock.** `RecordingGroup.opened_at`,
  `RecorderSpec.group_anchor` and `recorder::absorb`'s lead computation are all
  `SystemTime` now (unix ms in the record). Cross-pod alignment is therefore as
  good as the nodes' clock sync — a skew of *s* misaligns two participants by
  *s*, which deploy.md says out loud. The `resume_ms > 0` rule that clears
  `group_anchor` stays: an adopted recording's spilled frames already carry the
  lead silence, so padding again would double it.
- **Adoption takes the seat back rather than being refused by it.** A
  participant's seat in the members hash is held by the pod that died, so a
  plain `HSETNX` would refuse the adopter its own object. `open_or_join_group`
  takes a `take_over` flag, and `open_recording_attachment` sets it when the
  attach carries `mss.recording.spillOwner` — the metadata key
  `RegistryKeeper::rebuild` adds and nothing else does. The justification is
  that the lease claim already arbitrated ownership of that session: `HSET`
  here is not a race, it is recording the outcome of one already decided.
- **Order inside `join_group`:** a sync local precheck (cheap, and it keeps the
  single-pod error messages byte-identical), then the store round trip, then a
  sync commit into the cache. The lock is never held across the await. If the
  commit fails after the store accepted, the seat is released again. That store
  round trip is **once per grouped attachment, in the control world** — never
  on a frame path.
- **`registry_keeper::rebuild` stopped skipping grouped attachments** and
  passes `group` through like every other field; `join_group` finds the record
  and the adopter rejoins. `KeeperCounters.grouped_not_adopted` and
  `mss_registry_grouped_not_adopted_total` are **deleted** — the behaviour they
  counted no longer exists.
- **`group_anchor_for(session)` is the seam item 55 fills.** Today it returns
  `SystemTime::now()`. When a conference owns its own recording, the first
  member of a new group whose session is a conference member must anchor on the
  *conference's* open instant instead, and that is the one function that has to
  change.

What this does **not** do: it does not place a group's sessions onto one pod
(D8 is still open), and it does not make a conference survive a pod loss — a
conference member is an inline session and inline sessions are never adopted
(`PersistedSession::is_rebuildable`). What survives is a **tapped** session's
recording and its membership in a group.

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
  the recorder and, for WS/gRPC, sets the hub subscription's pause flag
  (`SubscriptionControl::set_paused`) so their media really stops — that was
  control-plane state only until item 27 closed D10, and skipped frames are now
  counted.
  `close_attachment` **awaits** the recorder's finish so the callbacks land
  before the caller's `Detach` returns, and `close_session` now sweeps *all*
  of a session's attachments (previously it left WS tasks and map entries
  behind on `DestroySession`) finishing recordings first.
  `TapPlane::observe_through` takes a `Weak<dyn ObservationSink>` — weak, so
  the controller↔plane cycle cannot leak the controller.
- **`hub.rs`**: `Subscription::try_next` is no longer test-only. The recorder
  drains what is already queued after it is told to finish, so a stop does not
  discard up to 200 buffered frames (4 s) of audio.
- **`metrics.rs`**: thirteen new series — the ten below plus item 21's
  `mss_recording_groups_live`, `mss_recording_group_members_live` and
  `mss_recording_group_joins_refused_total`. Note that
  `mss_recording_uploads_total` and `mss_recording_bytes_uploaded_total` count
  **objects**, so a group member writing both tracks moves them by two;
  `mss_recordings_stopped_total` still counts recordings.
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

## crates/sip-uas — the SIP front door (scaffolded, transactions, dialogs and session timers, 2026-08-30)

An answer-only media plane still has to be *reached*, and until now something
else did the reaching: `lab/sip_shim.py`, 336 lines of Python with in-memory
dialog state and one instance, terminating the leg FreeSWITCH bridges to and
translating it into `CreateSession{kind=INLINE, sdp_offer}`. That put a
single-instance stateful process in the setup and teardown path of every
media-owning call, and it left mid-call re-INVITE with no owner anywhere. This
crate replaces it in-process. See architecture §7 for why the "SIP stack — not
needed" row changed.

**Adopted, not written.** `rvoip-sip-core` (crates.io, MIT) supplies parsing and
message building. Article XI's rule for codecs applies to protocol stacks too:
adopt a proven implementation, do not reimplement. The published crate carries
its own interop evidence against Asterisk and FreeSWITCH — hold/resume, blind
transfer, RFC 4733 DTMF, UDP and TLS — plus a SIPp matrix to 2000 CPS. Its own
README declines to call that carrier certification, and neither do we.
Deliberately *not* taken: the `rvoip-sip` umbrella, which brings call-control
and media opinions this repository already has its own answers for. Only the
parsing and building layer is a dependency, and — since the 2026-08-30 probe
recorded in architecture §7 — it is the only one that will be:
`rvoip-sip-transport` and `rvoip-sip-dialog` were the named candidates for
transactions and are no longer taken. There is also no `rvoip-transaction-core`
to take; it is deprecated and folded into `rvoip-sip-dialog` and the umbrella.

**Sans-IO, like everything else here.** Three modules, no sockets, no tasks and
no clock: `message.rs` turns bytes into intent and intent into bytes,
`timing.rs` holds T1/T2/T4 and the doubling rule, and `transaction.rs` is the
RFC 3261 §17 server state machines. Time is a parameter everywhere, so all 33
tests run without a network.

**Generic on purpose, because this crate is meant to be publishable.** Nothing
in the public API knows what MSS is. The dialled user part is
`Invite::request_uri_user`, not `dialled_group` — reading it as a conference
group is the *application's* contract (the one `lab/sip_shim.py` established),
and it stays in the caller. `FrontDoorError` became `MessageError` for the same
reason. The layer hands the application a `Request` and takes back a `Response`;
what an INVITE *means* is never its business.

**The API is deadline-driven, not timer-callback-driven.**

```
on_datagram(bytes, from, now) -> Vec<Action>
respond(key, response, now)   -> Result<Vec<Action>, RespondError>
poll(now)                     -> Vec<Action>
next_deadline()               -> Option<Duration>
```

`Action` is only `Send { datagram, to }` and `Deliver { key, event }`. There is
deliberately no `StartTimer`/`StopTimer` action: the layer owns its own
deadlines and the driver asks `next_deadline()` when to wake up. That keeps the
driver down to a socket and a sleep, and it matches the deadline-anchored rule
the guidelines already impose on pacing.

**Timer names are behaviour, since a comment cannot say which letter is which.**

| RFC 3261 §17 | in this crate |
| --- | --- |
| Timer G — retransmit a non-2xx final | `retransmit_at` on a `Completed` invite |
| Timer H — give up waiting for the ACK | `give_up_at` on a `Completed` invite |
| Timer I — absorb ACK retransmissions | `absorb_until` on a `Confirmed` invite |
| Timer J — absorb request retransmissions | `absorb_until` on a `Completed` non-invite |
| Timer L (RFC 6026) — the `Accepted` state | `give_up_at` on an `Accepted` invite |

**Three decisions worth knowing.**

1. **A 2xx ACK is matched on Call-ID and CSeq, not on the branch.** The ACK for
   a 2xx is a new end-to-end transaction and carries its *own* branch, so
   branch-keying alone never finds the INVITE it acknowledges. The layer tries
   the branch key first (which is right for a non-2xx ACK, since that one does
   share the branch) and falls back to `(call_id, cseq)` across transactions in
   `Accepted`. `the_ack_for_a_two_hundred_arrives_on_its_own_branch_and_still_finds_its_invite`
   is that rule.
2. **Timer L is held at 64×T1 on reliable transports too, which RFC 6026 sets
   to 0.** Terminating the `Accepted` state instantly on TCP would mean the
   application never receives `Acknowledged` there, and a uniform event on both
   transports is worth more here than the letter of the timer. Named in
   `an_accepted_invite_waits_for_its_ack_on_a_reliable_transport_too`.
3. **CANCEL is answered by the layer; the 487 is the application's.** A CANCEL
   gets an automatic 200 when it matches a live INVITE in `Proceeding` and an
   automatic 481 when it does not — there is nothing else either could say —
   and the INVITE transaction is delivered `Cancelled` so the application sends
   the 487 itself. That is RFC 3261 §9.2's division of labour. The 100 Trying
   on an INVITE is automatic for the same reason.

**What it does today**

| | |
| --- | --- |
| parse an INVITE, read its call-id, dialled user part and SDP offer | done |
| build a 200 OK, a refusal with a reason phrase, a 100 Trying | done |
| INVITE server transaction — proceeding / completed / confirmed / accepted | done |
| non-INVITE server transaction — trying / proceeding / completed | done |
| branch matching on the topmost Via, with ACK keyed to its INVITE | done |
| retransmit a final response on the doubling schedule to T2 | done |
| absorb retransmitted requests without reaching the application twice | done |
| tell the application when an ACK never arrived, at 64×T1 | done |
| CANCEL, and the 481 for a CANCEL with nothing to cancel | done |
| reliable-transport behaviour (no retransmission, no absorb window) | done |
| dialogs (RFC 3261 §12) — id, tags, remote target, reversed route set | done |
| in-dialog CSeq ordering, with the 500 for a number that did not advance | done |
| re-INVITE recognised as in-dialog, with the 500 + `Retry-After` for a second one | done |
| session timers (RFC 4028) — negotiation, the 422 below Min-SE, expiry | done |
| 481 for an in-dialog request naming a dialog that does not exist | done |

**What fixes what.** `lab/sip_shim.py` has a replay cache keyed on Call-ID and,
in its own words, *"no timers"*. So it absorbs a retransmission correctly and
never retransmits its own 200, and nothing ever expires: an INVITE that is
answered and never acknowledged holds an MSS session — and a mixer slot —
forever. `an_invite_whose_ack_never_comes_tells_the_application_at_thirty_two_seconds`
is that defect written as a test.

**Three more decisions, from the dialog layer.**

4. **The refresher is always the caller.** RFC 4028 lets the UAS pick; an
   answer-only media plane that made itself the refresher would have to send
   re-INVITEs, which is a UAC. So a `Session-Expires: 1800;refresher=uas` is
   answered `refresher=uac`, and on expiry the door **ends the media session and
   does not send a BYE** — tearing down the dialog is call control's, and MSS
   has no UAC to do it with. `a_session_expires_above_the_floor_is_negotiated_with_the_caller_refreshing`.
5. **A re-INVITE whose offer is unchanged is answered with the session's
   existing SDP answer.** That covers the session-timer refresh and the
   re-INVITE-to-refresh case completely. An offer that actually differs is still
   refused **488 by name**, because `session-core` has no renegotiation path —
   the SIP half of P3-2 is closed and the media half is not.
6. **A dialog tag is generated per call, and `rvoip-sip-core` cannot be trusted
   to do it.** `SimpleResponseBuilder::dialog_response` writes the literal
   string `"local-tag-value"` as the To tag — its own source says *"In a real
   implementation, generate a unique tag"* — so every concurrent call would
   share a dialog identity. Neither the front door nor `Invite::answered_with`
   uses it; both take the tag as a parameter.
   `two_calls_are_given_different_to_tags` is the regression test.

**What it does not do yet, in the order it matters**

1. **No UAC, deliberately.** No client transactions, no forking, no registrar,
   no ability to send a BYE or a re-INVITE. This is the boundary the design
   picked, not an omission: a media plane answers.
2. **A changed offer still has no answer.** The transaction and the dialog run
   correctly; `session-core` cannot renegotiate the RTP session, so hold with a
   changed media description, attended transfer and codec change are refused
   488. That is the remaining half of P3-2 and it is `session-core` work, not
   SIP work.
3. **No TCP or TLS.** `Reliability::Reliable` is implemented and tested, but the
   driver binds UDP only.
4. **The mutation sweep is not libFuzzer.** `robustness.rs` drives 25 000
   deterministically mutated datagrams through the layer on every `cargo test`
   and the seed is fixed so a failure reproduces. A real fuzz target needs
   nightly, which the pinned toolchain is not.
5. **Never met a real SIP endpoint.** The socket is real and the controller is
   real, but every datagram in the tests is constructed here. FreeSWITCH,
   OpenSIPS and a softphone are still lab work.

## crates/mediaserverd/src/sip_front_door.rs — the door on the wire (2026-08-30)

The Tokio task that turns the sans-IO layer into a listening port. `serve` binds
`MSS_SIP_LISTEN`; `serve_on` takes an already-bound socket, which is what the
tests use so they can pick an ephemeral port. It is a **control-plane task and
never touches a media thread**.

**One entrance, not two.** The door holds `Arc<SessionController>` and calls
`create_session` / `destroy_session` on it directly — the same methods the gRPC
service calls, one function call earlier. There is no parallel session path, and
that is the rule the front door exists under. It also means the door bypasses
the wire's bearer check: `MSS_SIP_LISTEN` is its own trust boundary and must be
firewalled to the signalling elements that should reach it.

**Nothing blocks the socket.** `CreateSession` is spawned and its answer comes
back over an mpsc channel that the same `select!` loop drains, so retransmissions
arriving while the media plane is opening a session are still absorbed. The
automatic 100 Trying goes out before any of it. This is the shim's one
structural flaw — a blocking control call on the only thread — not carried
forward.

**The loop sleeps on the earliest deadline** across both the transaction layer
and the dialog store (`next_deadline()`), capped by a 250 ms idle tick.

| what arrives | what the door does |
| --- | --- |
| INVITE, no To tag | negotiate the session timer, then `CreateSession{kind=INLINE, group=<dialled user part>}`; answer 200 with the media plane's SDP, our Contact and a per-call To tag |
| INVITE below Min-SE | 422 with `Min-SE`, and no session is created |
| INVITE with no SDP | 488 by name |
| re-INVITE, offer unchanged or absent | 200 with the same SDP answer, session timer refreshed |
| re-INVITE, offer changed | 488 by name |
| second re-INVITE while one is unanswered | 500 with `Retry-After` |
| in-dialog CSeq that did not advance | 500 |
| BYE | `DestroySession`, 200, dialog dropped |
| CANCEL before the answer | 487 to the INVITE, and the session is destroyed |
| ACK that never comes | at 64×T1 the media session is destroyed rather than leaked |
| session timer expiry | the media session is ended; **no BYE is sent** |
| OPTIONS | 200 with `Allow` |
| in-dialog request naming an unknown dialog | 481 |
| anything unparsable | nothing at all |

**Verified over real UDP sockets against the real `SessionController`** (ten
tests, a fake `MediaPlane` supplying the SDP answer): the 100 then the 200
carrying the plane's own SDP, two calls getting different To tags, BYE closing
the leg, 481 for an unknown dialog, 422 below the floor, the negotiated
`Session-Expires` and `Require: timer` on the answer, a refreshing re-INVITE
answered and a changed one refused, OPTIONS, and four hostile datagrams followed
by a call that still gets answered.

**Not yet done here:** no metrics are exported for the door (answered and
refused are counted in the struct and logged at shutdown only), it is UDP only,
and nothing in `deploy/` opens the port.
