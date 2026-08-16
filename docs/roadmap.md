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
| 0 | Groundwork spike | — (de-risking only) | 🔶 in progress |
| 1 | Passive fan-out | `uuid_audio_fork`, `uuid_google_transcribe2` media bugs | 🔶 hub core landed |
| 2 | Recording | `record_session` bugs, shared-FS recording pipeline | ⬜ |
| 3 | Interactive media | dummy leg + conference-per-AI-interaction; mediagateway service | ⬜ |
| 4 | Full media plane | conference mixing, monitor/whisper (`relate nospeak`), MOH | ⬜ |

End state: FreeSWITCH performs IVR prompting and call control only
(originate/answer/hangup/park/transfer/bridge/DTMF) — or is retired
entirely if IVR also moves. No media bugs, no mixers, no forks.

## Phase 0 — Groundwork spike (milestone M2)

Objective: prove the ingest mechanism and price it, before any product
code depends on it.

Work:
- Confirm deployed rtpengine version supports `subscribe request/answer`
  / `unsubscribe`; establish upgrade path if not. **Open** — 14.1.1.8
  proven in the lab; the production version is unverified.
- ✅ Async NG transport in `mediaserverd` wrapping the sans-IO
  `rtpengine-ng` crate (UDP, cookie correlation, timeout/retry).
- ✅ Lab test: subscribe to a live call, receive both legs, run them
  through `media_core::jitter`, decode G.711, write stereo WAV — done
  against synthetic calls (2026-08-14) and against a real softphone
  through OpenSIPS + FreeSWITCH (2026-08-15), with the codec, silence
  and injection lessons recorded in [lab.md](lab.md).
- Ingest benchmark: `recvmmsg` batching, packets/sec/core, per-tap CPU on
  the rtpengine host at 100/500/1000 concurrent taps. **Partial** — the
  WSL2 rough shape is recorded in [testing.md](testing.md) (pipeline
  244 ns/packet, ~41k taps/core pipeline-only); the socket path and the
  rtpengine-side delta still need the namespace rig.
- Decide call→rtpengine-node discovery (recommended: OpenSIPS writes
  call-id → node + tags to Redis at call setup). **Open** — a polling
  stand-in (`lab/call_watcher.py`) works in the lab; the Redis design
  needs agreement with the OpenSIPS config owners.

Exit criteria: WAV artifact from a real tapped call; measured per-tap
cost on both MSS and rtpengine sides; discovery mechanism agreed with the
OpenSIPS config owners.

## Phase 1 — Passive fan-out (milestones M3–M4)

Objective: every listen-only consumer stops touching FreeSWITCH.

Work:
- Fan-out hub: per-session pub/sub, N consumers, attach/detach mid-call,
  per-consumer bounded queues with drop-oldest + metrics. **Core landed**
  (`crates/mediaserverd/src/hub.rs`) with the WS consumer ported onto it;
  metrics are counters surfaced in logs, not yet exported.
- Codec pipeline: G.711 → L16 → resample (8k/16k) → per-consumer encode;
  Opus via `audiopus` when a consumer needs it.
- Consumer adapters: WebSocket Twilio dialect first (wire-compatible with
  mediagateway — existing endpoints must not change), then gRPC
  `MediaStream` (proto/mediastream.proto). Each attaches with a declared
  capability (`SINK` / `+EVENTS` / `+INJECT`) — architecture.md §5.2 — and
  exactly one per session is `authoritative` (§5.3).
- Control plane: tonic `MediaControl` over the Session/Attachment/Playback
  nouns (architecture.md §5.1). **The state machine underneath it has
  landed** — `crates/session-core`, sans-IO: capability enforcement, the
  one-authoritative-attachment rule, per-session event sequencing and
  idempotent retries, with `proto/mediacontrol.proto` as the contract.
  Still to come: the tonic service itself, Redis session registry with
  ownership leases + re-subscribe on pod loss, and **events published to
  Kafka**
  (`mss.events`, typed `MediaEvent`) rather than streamed back over gRPC —
  a translator in cigol renders them onto the existing `eventTopic` in the
  positional format `appServer` already consumes (§5.4).
- `TelCompat` façade: `telsvc.proto` message shapes verbatim, so a
  per-tenant flag routes `StartStream`/`StartRecording`/
  `StartCallTranscription` to `telServer` or `mssServer` with no client
  change and rollback by config (§5.6).
- **ASR arrives with streaming, not separately.** Since cigol moved ASR
  from Google to Deepgram, `PlayAndDetectSpeechWithGSR` *is* the audio
  fork, so RTT, gather and the voice-AI feed are one mechanism. The
  vocabulary to emit is `mod_audio_fork::{start_of_transcript,
  partial_speech_result, end_of_utterance, first_transcript}`.
- telservice parity: `StartStream/StopStream/StreamPause/StreamResume/
  StreamSendText/StartCallTranscription` route to MSS behind a per-tenant
  feature flag; mod_audio_fork stays installed for rollback.
- streamfsm keeps driving FS playback; it pauses/resumes the MSS consumer
  instead of the FS media bug — validate barge-in timing in pilot.

Exit criteria: measured `partial_speech_result` → `StopPlayback`
cut-through latency inside the barge-in budget (MSS is a single writer per
session and therefore on that critical path — architecture.md §5.5); zero
`uuid_audio_fork` / `uuid_google_transcribe2` invocations at steady state
for flagged tenants; measured FS CPU-per-call
reduction; one full quarter (or agreed period) of pilot stability;
audio-flow watchdog + re-subscribe recovery observed working in
production incidents, not just tests.

## Phase 2 — Recording

Objective: recording leaves FreeSWITCH; the file/S3 contract survives.

Work:
- Per-leg taps → stereo segmenter (`hound`/`ogg`) → direct S3 upload.
- Preserve recording identity `${accountID}/${recordingID}.${format}` and
  the `recordStart/recordStop/recordPause/uploadCompleted` callback
  semantics, including pause = segment + defer + accumulated duration.
- Hold/pause state consumed from cigol events (Redis), not FS media-bug
  state.
- Compliance option: per-tenant dual-recording (FS + MSS) during
  transition; keep `record_session` as fallback until sign-off.

Exit criteria: byte-comparable recordings vs FS output across codec/hold/
transfer scenarios; downstream consumers (playback UI, QA tooling) work
unmodified; dual-recording disabled for all tenants.

## Phase 3 — Interactive media

Objective: things that talk back go through MSS inline legs; the
dummy-leg-conference construct and mediagateway die.

Work:
- Inline RTP endpoint mode: answer OpenSIPS B2B INVITEs
  (`X-Conversation-ID` / `X-ccId` correlation, `ua_session_reply` via MI)
  with MSS-owned SDP; per-pod addressable RTP (hostNetwork/port range).
- Full-duplex sessions: caller audio to bot, streaming TTS from bot to
  caller through the playout pacer; barge-in cut-through in MSS.
- Pre-agent AI calls routed by OpenSIPS straight to MSS — FreeSWITCH
  never touches them.
- vaiorchestrator switches `callMediaGateway` → MSS create-conversation
  API (same handshake, `X-API-REQUEST-TYPE: internal` semantics).
- Prompt/MOH injection into tapped calls via rtpengine `play media`
  where file-shaped audio suffices.
- Decommission mediagateway.

Exit criteria: AI interactions run without any FS conference or dummy
leg; mediagateway receives zero traffic; billing/disconnect event parity
on `MEDIAGATEWAY_BILLING_TOPIC` / `KAFKA_VOICE_AI_AGENT_TOPIC` verified.

## Phase 4 — Full media plane

Objective: mixing moves to MSS; FreeSWITCH has no media left.

Work:
- N-way mixer: conferences as MSS sessions of inline legs with a
  per-listener mix matrix (sum/saturate DSP, active-speaker, AGC).
- Monitor = a hub subscriber (no SIP leg at all); whisper = injection
  routed only into the agent's mix; barge = matrix flip — replacing
  conference `relate … nospeak`.
- Conference feature tail: enter/exit sounds, member mute/deaf/hold,
  conference recording, DTMF controls (parity list from cigol's 14
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
