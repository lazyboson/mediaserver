# Roadmap — from first tap to a FreeSWITCH with no media

This is the execution plan for the mission defined in
[architecture.md](architecture.md): remove every media workload from
FreeSWITCH, in phases, each phase retiring a specific FS mechanism and
each gated by explicit exit criteria. Update the status column as
milestones land (Constitution: phases advance only when the previous one
is boringly stable in production).

## Status at a glance

| Phase | Name | Retires from FreeSWITCH | Status |
| --- | --- | --- | --- |
| — | M1 scaffold | — | ✅ done (2026-08-13) |
| 0 | Groundwork spike (M2) | — (de-risking only) | ✅ code done; 3 org-side items open |
| 1 | Passive fan-out (M3–M4) | `uuid_audio_fork`, `uuid_google_transcribe2` media bugs | 🔶 M3 done, M4 code complete; the barge-in cut-through is **measured for every MSS-owned hop** (tasks item 5, p50 3.5 ms from a real consumer `SpeechReport`) — what remains is an integrator's own event consumer and the pilot |
| 2 | Recording | `record_session` bugs, shared-FS recording pipeline | 🔶 code complete (M5, tasks items 15/21/29/30); FS parity measured (item 31) — a two-party production-FS comparison and the tenant cutover remain |
| 3 | Interactive media | dummy leg + conference-per-AI-interaction; the legacy media gateway service | 🔶 code complete, lab-verified (items 32–35); production integration and org-gated criteria remain |
| 4 | Full media plane | conference mixing, monitor/whisper (`relate nospeak`), MOH | 🔶 code complete, lab-verified (items 36–41); production integration and org-gated criteria remain |

The ordered next-up list, with a definition of done per item, open defects
and what is blocked on other people, lives in [tasks.md](tasks.md).

End state: FreeSWITCH performs IVR prompting and call control only
(originate/answer/hangup/park/transfer/bridge/DTMF) — or is retired
entirely if IVR also moves. No media bugs, no mixers, no forks.

## Phase 0 — Groundwork spike (milestone M2)

Objective: prove the ingest mechanism and price it, before any product
code depends on it.

Work:
- Confirm deployed rtpengine version supports `subscribe request/answer`
  / `unsubscribe`; establish upgrade path if not. **Open (org-side)** —
  14.1.1.8 proven in the lab; the production version is unverified.
- ✅ Async NG transport in `mediaserverd` wrapping the sans-IO
  `rtpengine-ng` crate (UDP, cookie correlation, timeout/retry).
- ✅ Lab test: subscribe to a live call, receive both legs, run them
  through `media_core::jitter`, decode G.711, write stereo WAV — done
  against synthetic calls (2026-08-14) and against a real softphone
  through OpenSIPS + FreeSWITCH (2026-08-15), with the codec, silence
  and injection lessons recorded in [lab.md](lab.md).
- Ingest benchmark: `recvmmsg` batching, packets/sec/core, per-tap CPU on
  the rtpengine host at 100/500/1000 concurrent taps. **Partial (org-side)** — the
  WSL2 rough shape is recorded in [testing.md](testing.md) (pipeline
  244 ns/packet, ~41k taps/core pipeline-only); the socket path and the
  rtpengine-side delta still need the namespace rig.
- Decide call→rtpengine-node discovery (recommended: OpenSIPS writes
  call-id → node + tags to Redis at call setup). **MSS side built
  (2026-08-26, [tasks.md](tasks.md) item 51)** — `crates/mediaserverd/src/discovery.rs`
  reads the map, `mss_discovery_*` counts hits, misses and errors, and
  `lab/node_discovery_drill.sh` proves it live. **Open (org-side)** — the
  proxy half needs agreement with the OpenSIPS config owners.

Exit criteria: WAV artifact from a real tapped call; measured per-tap
cost on both MSS and rtpengine sides; discovery mechanism agreed with the
OpenSIPS config owners.

## Phase 1 — Passive fan-out (milestones M3–M4)

Objective: every listen-only consumer stops touching FreeSWITCH.

Work:
- ✅ Fan-out hub: per-session pub/sub, N consumers, attach/detach mid-call,
  per-consumer bounded queues with drop-oldest + counted drops
  (`crates/mediaserverd/src/hub.rs`), with the WS consumer ported onto it.
  ✅ Metrics are exported (2026-08-20): a Prometheus endpoint on
  `MSS_METRICS_LISTEN` serves the ingest, jitter, consumer, event and
  registry counters, the audio-flow watchdog is wired per leg, and
  `deploy/prometheus-alerts.yaml` alerts on every drop counter.
- ✅ Speaker attribution: each tap leg is named from the participant's own
  SSRC (rtpengine `query`) with elimination for transcode-restamped legs, so
  stereo recording and RTT speaker labels are trustworthy. ✅ A **mid-call
  SSRC change** now re-resolves too (2026-08-22, tasks.md item 14): the leg
  re-enters resolution and a control-world task re-queries rtpengine and
  pushes a fresh map into the capture loop over a bounded queue —
  replay-verified, not yet watched on a live re-INVITE. Remaining soft spot:
  a transfer that replaces a from-tag needs a re-subscribe, not a
  re-resolve.
- ✅ Codec pipeline (2026-08-22): G.711 → L16 → resample (8k/16k/48k) →
  per-consumer encode, replay-verified; the hub carries PCM and each
  consumer encodes its own format.
- ✅ **Opus ingest (2026-08-23)**: libopus via `opus-ffi` (the only crate with
  `unsafe`), decoding at 8/12/16/24/48 kHz with libopus's own concealment,
  proven against real rtpengine-generated Opus on a live call. The build now
  needs cmake/make/g++ — the hermetic-build objection was real but was the
  wrong thing to optimise for against codec correctness (tasks.md 16a/16b).
  Opus **output** to consumers is still deferred until one asks (16d).
- ✅ Consumer adapters: WebSocket Twilio dialect first (wire-compatible with
  the legacy media gateway — existing endpoints must not change), then gRPC
  `MediaStream` (proto/mediastream.proto, landed 2026-08-20 — binary frames,
  `ConsumerHello` token auth, capability-checked inject, mark/clear; **a live
  tapped call was heard over it in the lab on 2026-08-22**, L16/16k and
  ASR-verified intelligible — tasks.md item 10). Each attaches with a declared
  capability (`SINK` / `+EVENTS` / `+INJECT`) — architecture.md §5.2 — and
  exactly one per session is `authoritative` (§5.3).
- Control plane: tonic `MediaControl` over the Session/Attachment/Playback
  nouns (architecture.md §5.1). **The state machine underneath it has
  landed** — `crates/session-core`, sans-IO: capability enforcement, the
  one-authoritative-attachment rule, per-session event sequencing and
  idempotent retries, with `proto/mediacontrol.proto` as the contract.
  **The gRPC service has landed too** — `crates/control-api`, served over a
  real socket and tested through a generated client — and **`mediaserverd`
  now serves it** (`MSS_CONTROL_LISTEN`), with `tap_plane::TapPlane` turning
  `CreateSession`/`Attach` into a real rtpengine subscription and a real
  consumer websocket. **Events now publish to Kafka** (`mss.events`, typed
  `MediaEvent`, keyed by `external_id`, gapless per-session seq,
  `legacy_eligible` marking the authoritative attachment — verified off the
  wire against Redpanda in the lab, `crates/mediaserverd/src/event_pump.rs`)
  rather than streamed back over gRPC. **The Redis session registry landed
  2026-08-17** (ownership leases, adoption through the controller's own API,
  re-subscribe via `TapPlane`), and **auth landed 2026-08-20**
  (`MSS_AUTH_TOKEN` bearer interceptor on `MediaControl`, `ConsumerHello`
  token on the data plane; TelCompat deliberately open for client
  compatibility). **Re-subscribe recovery was observed on a live call
  2026-08-22** (tasks item 11): three pods on one Redis, `kill -9` on the
  owner mid-call, one survivor adopted 14.6 s later and the consumer's audio
  resumed after a **14.41 s** gap — bounded by lease TTL 15 s + adopt sweep
  10 s. It also showed that the dead pod's rtpengine subscription is left
  behind (D14). Still to come: the translator in the legacy controller that renders
  `mss.events` onto the existing `eventTopic` in the positional format
  the application server already consumes (§5.4 — written on the legacy translator branch, awaiting review and
  merge).
- `TelCompat` façade: the legacy verb API's message shapes verbatim, so a
  per-tenant flag routes `StartStream`/`StartRecording`/
  `StartCallTranscription` to the legacy controller's gRPC server or to MSS with no client
  change and rollback by config (§5.6).
- **ASR arrives with streaming, not separately.** Since the legacy controller moved ASR
  from Google to Deepgram, `PlayAndDetectSpeechWithGSR` *is* the audio
  fork, so RTT, gather and the voice-AI feed are one mechanism. The
  vocabulary to emit is `mod_audio_fork::{start_of_transcript,
  partial_speech_result, end_of_utterance, first_transcript}`.
- Legacy verb API parity: `StartStream/StopStream/StreamPause/StreamResume/
  StreamSendText/StartCallTranscription` route to MSS behind a per-tenant
  feature flag; mod_audio_fork stays installed for rollback.
- The legacy stream state machine keeps driving FS playback; it pauses/resumes the MSS consumer
  instead of the FS media bug — validate barge-in timing in pilot.

Exit criteria: measured `partial_speech_result` → `StopPlayback`
cut-through latency inside the barge-in budget (MSS is a single writer per
session and therefore on that critical path — architecture.md §5.5); zero
`uuid_audio_fork` / `uuid_google_transcribe2` invocations at steady state
for flagged tenants; measured FS CPU-per-call
reduction; one full quarter (or agreed period) of pilot stability;
audio-flow watchdog + re-subscribe recovery observed working in
production incidents, not just tests (the lab half of the re-subscribe
criterion is met — 2026-08-22, a real `kill -9` mid-call with a measured
14.41 s gap, and the D14 orphan subscription it exposed is fixed — tasks item 25; what remains is seeing the recovery in production).

## Phase 2 — Recording

Objective: recording leaves FreeSWITCH; the file/S3 contract survives.

Work:
- Per-leg taps → stereo segmenter (`hound`/`ogg`) → direct S3 upload.
- Preserve recording identity `${accountID}/${recordingID}.${format}` and
  the `recordStart/recordStop/recordPause/uploadCompleted` callback
  semantics, including pause = segment + defer + accumulated duration.
- Hold/pause state consumed from the legacy controller's events (Redis), not FS media-bug
  state.
- Compliance option: per-tenant dual-recording (FS + MSS) during
  transition; keep `record_session` as fallback until sign-off.

Status (updated 2026-08-24; tasks items 15, 21, 29, 30, 31): **code complete.** The recorder is a hub
consumer in the control world — stereo segmenter (customer left, agent right),
the frozen `${accountID}/${recordingID}.${format}` identity parsed from the
`FILE_S3` attachment endpoint, `recordStart/recordPause/recordStop/
uploadCompleted` on `mss.events` with pause = segment + defer + accumulated
duration, and direct upload through `object_store` to S3 or MinIO. Verified
against a real MinIO from a synthetic hub **and on a live tapped call**
(item 10). Since then: **recording groups** record N sessions as one recording
with one mono object per participant (item 21), time-aligned on the group's open
instant (item 29, closing D18), and closed segments **spill to disk as the call
runs** so a pod death costs the spill interval rather than the call (item 30),
and with `MSS_RECORDING_SPILL_TO=s3` that journal lives in the recording bucket
itself, so an adopter on **any** pod finishes what a dead pod started (item 53,
closing D9 but for retention and a live drill nobody has run).
Hold/pause arrives as `UpdateAttachment{paused}` (which now reaches the media
world at all) rather than from Redis; the legacy controller drives it through
`TelCompat`. Dual recording remains a legacy controller per-tenant flag and nothing here
prevents it.

Exit criteria: byte-comparable recordings vs FS output across codec/hold/
transfer scenarios; downstream consumers (playback UI, QA tooling) work
unmodified; dual-recording disabled for all tenants.

One live tapped call recorded end to end **landed 2026-08-22** with item 10's
lab drill: a real SIP call to MinIO, duration matching the reported
`duration_ms` to the sample, and the recording callbacks read off the real
`mss.events` topic.

The byte comparison ran against a **real FreeSWITCH recording** on 2026-08-23
(item 31, `lab/fs_parity_drill.sh` records one live call both ways): container,
channel layout and rms agree exactly, and a re-aligned 2 s window agrees on
1.0000 of samples at mean difference 0.6/32768 — there is no transform
difference. That run also **retired "byte-comparable" as a literal bar**: MSS and
FS conceal loss independently, so the inter-file offset wanders and a single
global offset fails by construction. The criterion is restated as container +
layout + duration + rms + windowed agreement.

Exit criteria still open: a **two-party** comparison on a production
FreeSWITCH — the lab's write side plays silence, so only one channel was truly
compared — the pause contract and a human listen against the tenant's codec; the
transfer scenario, which still lands on speaker naming by elimination until a
tag-replacing transfer triggers a re-subscribe; and the tenant decision to
disable dual recording.

## Phase 3 — Interactive media

Objective: things that talk back go through MSS inline legs; the
dummy-leg-conference construct and the legacy media gateway die.

Status (2026-08-23): **code complete and lab-verified; production integration
and the org-gated criteria remain.** The leg has met a real RTP peer — a
SIP-less container endpoint, not a SIP proxy's B2B leg — and barges in at
p50 12.2 ms / p95 20.4 ms (tasks.md item 35).
`CreateSession{kind=INLINE, sdp_offer}` binds a UDP socket
on the pod's media address, answers with MSS-owned SDP (PCMU/PCMA +
telephone-event; anything else refused by name), returns the answer in
`Session.sdp_answer`, feeds the peer's audio into the same jitter → decode →
hub pipeline the taps use as the `customer` track, and paces queued PCM back
out through the sans-IO `PlayoutPacer` (tasks.md items 32 and 33).
`StopPlayback` flushes the egress queue, which is the barge seam. Item 34 made
it **full duplex**: an INJECT attachment on either transport (gRPC
`MediaStream`, WS-Twilio) streams into that queue continuously, `Clear` flushes
it, and `Mark` is acked once the marked audio has drained out of it. Item 35
put a **real RTP peer** on the other end (`lab/inline_call_drill.sh`, no SIP and
no human) and measured what was owed: the peer hears the injected tone, the hub
still taps the peer at the same time, egress paces at 50.19 pkt/s with unbroken
sequence numbers, `Mark` acks 404 ms after a 400 ms lead, and **barge-in
cut-through is p50 12.2 ms / p95 20.4 ms / max 21.0 ms over 20 iterations** —
one ptime, as the pacer's flush promised. Owed now: a SIP/B2B leg, codecs other
than PCMU/8 kHz, and the integrator's own consumer half of barge-in.

Work:
- Inline RTP endpoint mode: answer INVITEs with MSS-owned SDP; per-pod
  addressable RTP (hostNetwork/port range).
  **Both halves now exist. The SDP answer and the media path landed in Phase 3;
  since 2026-08-30 ([item 59](tasks.md)) MSS also answers the SIP itself on
  `MSS_SIP_LISTEN` — a UAS with transactions, dialogs and session timers — so a
  B2BUA can bridge straight to it and no SIP element in front is required.
  What is still the integrator's: registration and authentication, and a
  re-INVITE that *changes* the offer, which is refused 488 because
  `session-core` has no renegotiation path (P3-2's media half). An inline leg
  remains deliberately not adoptable across pods — unlike a tap, its socket
  dies with its pod, so recovery is call control's.**
- Full-duplex sessions: caller audio to bot, streaming TTS from bot to
  caller through the playout pacer; barge-in cut-through in MSS.
- Pre-agent AI calls routed by OpenSIPS straight to MSS — FreeSWITCH
  never touches them.
- The voice-AI orchestrator switches its gateway call → MSS create-conversation
  API (same handshake, `X-API-REQUEST-TYPE: internal` semantics).
- Prompt/MOH injection into tapped calls via rtpengine `play media`
  where file-shaped audio suffices.
- Decommission the legacy media gateway.

Exit criteria: AI interactions run without any FS conference or dummy
leg; the legacy media gateway receives zero traffic; billing/disconnect event parity
on the legacy gateway's billing topic / `KAFKA_VOICE_AI_AGENT_TOPIC` verified.

## Phase 4 — Full media plane

Objective: mixing moves to MSS; FreeSWITCH has no media left.

Status (2026-08-24): **code complete and lab-verified; production integration
and the org-gated criteria remain.** Three container RTP peers have been in one
conference on real sockets, and twenty tone-per-phase assertions decided
minus-self, the monitor, whisper isolation, the barge flip, mute/unmute and both
recording shapes at a >=30:1 margin (tasks.md item 41, lab.md). What has *not*
happened is a SIP proxy's B2B leg into a conference, which is deployment-gated.

`media-core`'s `MixMatrix` is the sans-IO engine — N contributors x M listeners, minus-self by default, and all
three of monitor / whisper / barge are cells of that matrix rather than code
paths (tasks.md item 36). `mediaserverd`'s `conference.rs` wires it to sockets
(item 37): `CreateSession{kind=INLINE, group=<name>}` seats a leg in a
conference, the legs that share a group share one mix driven by one
capture-world clock, each hears everybody but itself, a prompt played into one
leg stays private to it, and the conference's full mix is published to every
member's hub as the `mixed` track so a monitor or recorder attaches with the
verbs that already exist. Verified over real UDP sockets in-process (three peers
hearing the other two and never themselves, a leg leaving mid-mix, the last leg
closing the conference); multi-rate conferences are refused by name rather than
resampled, and conferences are pod-local.

Monitor, whisper and barge landed as **matrix cells named by attachment
metadata** rather than new RPCs (item 38): `selector.only="mixed"` on any member
session is the monitor, `mix_target=<member>|all` on an INJECT attachment is the
whisper and the barge flip, and `mix_monitor=include|exclude` decides whether the
mixed track — which is also the recording feed — carries it. **Native conference
recording** landed on top of that (item 39) with no new noun either: a
`FILE_S3` attachment with `only=mixed` on any member records the room as one
mono object, a recording **group** over the member sessions with
`only=customer` records one object per participant time-aligned on the group
anchor, both shapes may run on one conference at once, and `RecordingStarted`
now names which shape an object is.

The **conference feature tail** closed the same way (item 40): `member_mute`,
`member_deaf` and `member_hold` are member-state metadata verbs — mute zeroes a
contributor row (off every ear and off the record), deaf silences the room into
one ear while audio addressed at that member still lands, hold is both with the
ear left open for hold audio — a prompt into the whole room is
`StartPlayback{target_tag="all"}`, and `mix_source=leg` routes a member's own RTP
to one ear, which is the coach shape. Enter/exit sounds are a verb rather than a
trigger and DTMF control is API-first, both on purpose; the generic conference
feature list and the ADAPTER parity table against one integrator's 14 conference
RPCs are architecture.md Appendix B. Items 37-40 are verified over in-process
UDP sockets; **item 41 then ran the three-peer drill for real** — three container
RTP peers in one conference, twenty green assertions, both recording shapes in
MinIO at once. What no conference here has met is a **SIP** peer.

Work:
- N-way mixer: conferences as MSS sessions of inline legs with a
  per-listener mix matrix (sum/saturate DSP, active-speaker, AGC).
- Monitor = a hub subscriber (no SIP leg at all); whisper = injection
  routed only into the agent's mix; barge = matrix flip — replacing
  conference `relate … nospeak`.
- Conference feature tail: member mute/deaf/hold, prompts into the room,
  conference recording (items 39–40 — done; enter/exit sounds and DTMF
  menus are deliberately integrator policy, see architecture.md
  Appendix B for the parity table against one integrator's 14
  conference RPCs).
- Fallback path if hand-rolled mixing disappoints: embed GStreamer
  (`gstreamer-rs`, `audiomixer`) behind the same session API
  (architecture.md Appendix A).
- After parity: FS conferences drained tenant-by-tenant; FS reduced to
  IVR + call control, or retired if IVR moves too.

Exit criteria: supervisor monitor/whisper/barge indistinguishable from FS
behavior in agent/supervisor UX testing; conference audio quality signed
off under load; zero FS media bugs or mixers in production; FS capacity
planning no longer mentions media.

## Standing rules for every phase

- Feature-flag per tenant; FS mechanism stays installed until the phase's
  exit criteria are met, then is removed deliberately.
- Every phase re-runs the Phase-0 benchmarks after pipeline changes
  (Constitution, Article VIII).
- Every new wire surface gets byte-exact serialization tests before first
  deployment (Article VII).
- An incident caused by a phase rolls the flag back first and debugs
  second; pcap capture + replay test before the fix merges (Article III).
