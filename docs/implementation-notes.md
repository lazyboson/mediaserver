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
  the legacy media gateway's codec mismatch silently passed garbage bytes through.
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
is `SessionKind::Inline` and **a conference is inline legs sharing a `group`**
(item 37), so both ride the same calls. `SessionKind::Mix` was the original guess
and is **unused** — it is refused by name in the controller; see the tap_plane
section on why a conference needed no new session kind.

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
`released`, `failed`, `grouped_not_adopted`, `orphans_unsubscribed`,
`orphans_still_subscribed`, `surrendered`) are exported as
`mss_registry_*_total`. A rising `lost`/`surrendered` means two pods believed
they owned one session and one gave up; a rising `orphans_still_subscribed`
means rtpengine is copying a call to a pod that is gone — the two worth
alerting on.

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
  verification tool and the reference for the legacy controller's translator.
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
`MediaEvent.mix_routed` (oneof tag 25; item 40 then took 26, so the next free
payload tag is 27) on the
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
consumer that asked for it. That also means no owner and no lease: tasks.md D22.
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

### tap_plane.rs — the control plane's hands in the media world

`TapPlane` implements `control_api::MediaPlane` over the machinery the
Phase-0 spike proved, which is what turns `MediaControl` from a registry
into something that actually taps calls.

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
- Not done: no metric is exported for the verdict (it is log-only), and the
  probe never repeats — an rtpengine restarted under a running daemon keeps its
  first-contact report. Both are cheap to add when something needs them.

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
- **`StopRecording` waits for the upload, deliberately.** `Detach` (and
  `DestroySession`) await the recorder's finish, so a the legacy controller `StopRecording`
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

**The limit to state plainly:** the spill directory is per-pod local disk, so a
cross-pod adopter reads nothing and the dead pod's audio becomes counted
silence. Whole-fix shape (shared spill volume, or one multipart upload per
segment straight to object storage) is the same shape D16 needs.

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
- **The group is in one pod's memory, so v1 is single-pod.** `TapPlane` holds
  `groups: Mutex<HashMap<GroupKey, RecordingGroup>>`; the group opens with its
  first member and dies with its last (`mss_recording_groups_live`,
  `mss_recording_group_members_live`). `PersistedAttachment` carries the group
  (serde `default`, so records written before this still decode) and
  `RegistryKeeper::rebuild` **refuses to restore a grouped recording on the
  adopting pod** — counted `grouped_not_adopted` — because rebuilding it there
  would split one recording across two pods' memory and two prefixes. That is
  soft spot D16; the fix is placement (schedule a group's sessions onto one
  pod, or make groups a shared-storage concept).

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
- **Unchanged:** the frozen identity, per-member pause, the refusal shapes, and
  D16 (a group is still one pod's memory, so the anchor is one pod's clock —
  which is also why a monotonic `Instant` is the right type here).

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
