# Tasks — what is done, what is next

Living work list. [roadmap.md](roadmap.md) holds the *why* and the phase exit
criteria; this file holds the *what next*, ordered, with a definition of done
for each item. Update it in the same PR that changes the state of an item.

Status as of **2026-08-17**.

## Milestones

| Milestone | Scope | State |
| --- | --- | --- |
| **M1 — scaffold** | workspace, sans-IO cores (RTP, G.711, DTMF, jitter), NG bencode, consumer dialects, two-world daemon skeleton, watchdog | ✅ done (2026-08-13) |
| **M2 — Phase-0 spike** | real NG subscribe against lab rtpengine, both legs jitter-buffered to WAV, per-tap cost | ✅ **code done**; 3 org-side items open (below) |
| **M3 — fan-out hub** | per-session pub/sub, N consumers, WS-Twilio adapter, pause/resume/send_text parity | ✅ done |
| **M4 — control plane** | `MediaControl` gRPC, session state machine, Kafka events, Redis registry, tenant-flag pilot | 🔶 **~85%** — metrics, MediaStream gRPC and auth remain |
| M5+ | Phases 2–4 (recording, interactive media, full media plane) | ⬜ not started |

### What landed, concretely

- **Sans-IO cores** — `media-core` (RTP, G.711 with companding, RFC 4733
  DTMF, jitter buffer with accounted telephone-events, `last_audio_ssrc`),
  `rtpengine-ng` (bencode, subscribe/answer/unsubscribe, `play media`,
  `stop media`, `query` + `ssrc_by_tag`, subscription SDP).
- **Tap ingest proven live** — real calls through OpenSIPS + FreeSWITCH +
  rtpengine tapped to stereo WAV, with the codec, silence, injection and
  DTMF lessons recorded in [lab.md](lab.md).
- **Fan-out hub** — bounded per-consumer queues, drop-oldest with counted
  drops, mid-call attach/detach, `TrackSelection`, injected audio as a
  continuous mixed track.
- **WS-Twilio consumer adapter** — frozen dialect, wire-verified against the
  real bridge; inbound `send_text` path.
- **Control plane** — `session-core` (capability authorization, one
  authoritative attachment, event identity and sequencing, idempotent
  retries), `control-api` (`MediaControl` served over gRPC, protoc-free
  build, drain-safe watchers), wired into `mediaserverd` so `CreateSession`
  really taps and `Attach` really connects a consumer, with rollback when
  the media world refuses.
- **Kafka events** — typed `MediaEvent` on `mss.events`, keyed by
  `external_id`, gapless per-session sequence, `legacy_eligible` marking the
  authoritative attachment; verified by consuming the topic during a live
  tapped call.
- **Leg identity** — SSRC correlation from rtpengine `query` plus
  elimination for transcode-restamped legs; verified per-track on two
  consecutive live calls, independent of stream order.
- **Lab** — OpenSIPS/FreeSWITCH/rtpengine/Redpanda compose stack, synthetic
  caller (`host_test_caller.py`), per-track analyser (`track_dump.py`),
  RTT and recorder mock consumers, NG probes, `mss_ctl` control-plane CLI,
  `mss_events_tail` bus consumer.

## Next up — ordered

### 1. cigol event translator — ✅ WRITTEN, awaiting review and merge
**State (2026-08-17):** implemented on the cigol branch
`feature/mss-event-translator` as `pkg/telservice/msstranslator`, **local and
uncommitted by instruction**. Pure `Render` plus a Kafka consumer on its own
group (`mssEventTranslator`), wired into `cmd/telServer` behind
`MSS_EVENTS_TOPIC` so an unconfigured deployment is unchanged. 15 unit cases
plus a broker-backed test (`-tags=integration`, `MSS_TEST_BROKERS`) that
publishes a MediaEvent and reads the legacy event back off the target topic:
verified end to end against the lab Redpanda, rendering
`mod_audio_fork::first_transcript` with the exact `gsrResult` body
`handleTranscribe` unmarshals, and dropping a non-authoritative attachment's
transcript in the same run. Confirmed against cigol's code that
`request_uuid` is the FreeSWITCH channel UUID (it is passed straight to
`uuid_audio_fork <uuid> start`), so `external_id` keys and shards correctly.
Remaining: review, commit, merge, and a pilot tenant.

### 1b. (original description, for reference) cigol event translator
**Where:** the `cigol` repo, not here (architecture §5.4 — the positional
format *is* `constants.MapKeyIndex`, a Go constant table; encoding it in Rust
would couple MSS to a file that changes without our knowing).
**What:** ~200-line Go consumer: read typed `MediaEvent`, drop anything with
`legacy_eligible == false`, render `mod_audio_fork::*` names into
`Events{repeated string}` on `eventTopic`, preserve the UUID key and OTel
context.
**Why now:** MSS publishes events nothing consumes yet. Until this exists no
tenant can be flipped, so it gates everything else in Phase 1.
**Done when:** `appServer` drives `streamfsm` from an MSS-tapped call with
`mod_audio_fork` uninvolved, and the event-name mapping table in
`proto/mediacontrol.proto` matches the shim one-for-one.

### 2. `TelCompat` façade — ✅ DONE (2026-08-17)
Landed in `crates/control-api/src/telcompat.rs` with
`proto/telcompat.proto` declaring `protos.TelService` so the method paths match
cigol's byte for byte. Serves stream/recording/playback verbs onto the nouns,
one test per mapping row, both surfaces on one port, proven over a real socket
with a generated cigol client. `StartCallTranscription` returns `UNIMPLEMENTED`
by design (the ASR endpoint is not in its request message).
**Blocked before a tenant can be flipped:** a TelCompat session has only the
channel uuid, so it needs the OpenSIPS→Redis discovery map (M2 item 3) to
resolve call-id and tags before it can tap.

### 2b. (original description, for reference) `TelCompat` façade
**Where:** `crates/control-api`.
**What:** a second gRPC service reusing `telsvc.proto` message shapes
verbatim — `StartStream`, `StopStream`, `StreamPause`, `StreamResume`,
`StreamSendText`, `StreamPlayFile`, `StartCallTranscription`,
`StartRecording`, `StopRecording` — translated onto Session/Attachment/
Playback per the §5.6 table, setting `authoritative` from the verb that
created the session.
**Why now:** it is the migration switch: a tenant flag routes cigol to
`telServer` or `mssServer` with no client change and rollback by config.
**Done when:** the mapping table is executable (one test per row), and a
`StartStream` call produces the same session/attachment shape as the
equivalent native calls.

### 3. Redis session registry + re-subscribe on pod loss — ✅ DONE (2026-08-17)
`session_store.rs` (namespaced keys, TTL'd leases, atomic `SET NX` claim) plus
`registry_keeper.rs` (persist, renew, adopt, release). Adoption rebuilds through
the controller's own API so every invariant applies to a rebuilt session, and
`TapPlane` re-establishes the subscription. Verified against real Redis,
including six pods racing for one orphan producing exactly one owner, and on a
live lab call. Sessions with no call identity are released rather than
half-restored; ended sessions are forgotten so no pod adopts a dead call.
**Still to prove:** the exit criterion wants a real pod kill mid-call observed
end to end (two daemons against one Redis), not just the unit and store-level
proofs.

### 3b. (original description, for reference)
**Where:** `crates/mediaserverd` (new module) + `session-core` stays sans-IO.
**What:** persist session/attachment state with CAS-safe writes and TTL'd
ownership leases renewed by heartbeat; on lease expiry another pod
re-establishes the tap (subscriptions are re-creatable — the HA advantage
over inline legs) and tears down orphaned rtpengine subscriptions.
**Why now:** today a tap lives and dies with its pod; this is a Phase-1 exit
criterion ("re-subscribe recovery observed working").
**Done when:** killing the owning pod mid-call moves the tap to another pod
with a bounded audio gap, and no rtpengine subscription is left behind.

### 4. Exported metrics
**Where:** `crates/mediaserverd`.
**What:** Prometheus endpoint over the counters that already exist —
per-leg ingest (datagrams, loss, concealed, suppressed, companded,
`unknown_ssrc`), per-consumer queue depth and `dropped_oldest`, event pump
(accepted/published/failed/dropped), live session and attachment counts,
audio-flow watchdog state.
**Why now:** every silent-drop counter is currently a log line; the
Constitution wants them first-class with alerts, and Phase-1 pilot needs
them for the FS-CPU-reduction claim.
**Done when:** `/metrics` serves them and the drop counters have alert rules.

### 5. Barge-in cut-through measurement
**Where:** lab.
**What:** measure `partial_speech_result` → `StopPlayback` end to end
(consumer → MSS → Kafka → translator → cigol → FS `break`), since MSS is a
single writer per session and therefore on that critical path (§5.5).
**Why now:** it is a **Phase-1 exit criterion**, and if the Kafka hop is too
slow the fallback (a gRPC stream for speech events only) is a design change,
not a tuning knob — better known early.
**Done when:** a number exists, compared against the barge-in budget.

### 6. gRPC `MediaStream` data plane
**Where:** `crates/control-api`.
**What:** implement the generated `MediaStream::Subscribe` — binary frames,
`ConsumerHello` auth, `inject` honored only with `INJECT` capability, marks
and `clear`.
**Why now:** the native, allocation-cheap consumer transport; WS-Twilio stays
the compatibility surface. Lower priority than 1–5 because every consumer
today speaks WS.
**Done when:** a consumer receives a live tap over gRPC and an unprivileged
attachment's `inject` is refused.

### 7. Auth on attachments
**Where:** `crates/control-api`.
**What:** verify `ConsumerHello.token`; an interceptor on `MediaControl`.
Nothing authenticates today (fine for the lab, not for a pilot).
**Done when:** an unauthenticated consumer cannot attach, and MediaControl
rejects unauthenticated callers.

### 8. Codec pipeline breadth
**Where:** `media-core`.
**What:** resample 8k↔16k↔48k (`rubato`), L16 output for ASR that wants it,
Opus via `audiopus` (Article XI: adopt the C library, do not reimplement).
**Why now:** consumers are all G.711/8k today; needed before an ASR vendor
asks for 16k L16 or a bandwidth-sensitive consumer asks for Opus.
**Done when:** a consumer can request L16/16k and get it, verified by replay.

## Open defects and soft spots

| # | Item | Where | Severity |
| --- | --- | --- | --- |
| D1 | A **mid-call SSRC change** (re-INVITE, transfer, codec renegotiation) does not re-resolve leg identity; `ssrcs_seen` makes it visible but nothing acts on it | `tap_spike.rs` | medium — affects transferred calls |
| D2 | `stop_playback` stops **all** playback on the call: rtpengine's `stop media` targets a participant, not a playback id | `tap_plane.rs` | low until multiple concurrent playbacks exist |
| D3 | `close_attachment` **aborts** the consumer task instead of closing the websocket politely (no `stop` frame) | `tap_plane.rs` | low, but consumers see a truncated stream |
| D4 | Only `WS_TWILIO` attachments are served; other transports are refused by name | `tap_plane.rs` | expected — item 6 above |
| D5 | Event delivery is **at-most-once**; a broker outage drops events (counted, and the gapless seq makes gaps detectable) | `event_pump.rs` | medium before pilot |
| D6 | `play media` `from-tag` semantics are **unmeasured** — architecture §6's claim was retracted after the instrument turned out to be broken (see lab.md correction) | docs + lab | low, but §6 must not be trusted until re-probed |
| D7 | Jitter buffer: fixed target depth, no adaptive sizing, no timestamp-aware gap handling, silence instead of real PLC | `jitter.rs`, `pipeline.rs` | medium for quality under real impairment |
| D8 | `owner_pod` is a config string; real placement and load-aware scheduling do not exist | `main.rs` | low until multi-pod |

## Waiting on other people (M2 close-out)

These are not code and have blocked since Phase 0:

1. **Production rtpengine version check** — lab is 14.1.1.8 with `subscribe`
   working; the deployed version is unverified. If it lacks `subscribe`, the
   whole ingest model needs an upgrade path first.
2. **rtpengine-side per-tap cost** — MSS-side cost is measured; the userspace
   copy cost on the rtpengine host at 100/500/1000 taps is not, and it sets
   the rtpengine capacity plan.
3. **OpenSIPS → Redis call→node discovery** — MSS needs call-id → rtpengine
   node + tags. A polling stand-in (`lab/call_watcher.py`) works in the lab;
   the production design needs agreement with the OpenSIPS config owners.
   Everything in Phase 1 depends on it at pilot time.

## Later phases

**Phase 2 — Recording** is the closest and mostly assembled already: per-leg
taps with correct speaker attribution (done), stereo segmenter → direct S3,
the `${accountID}/${recordingID}.${format}` identity contract, and
`recordStart/recordStop/recordPause/uploadCompleted` callback semantics
including pause = segment + defer + accumulate. Needs D1 fixed if transferred
calls must record correctly.

**Phase 3 — Interactive media** needs the inline RTP leg (`SessionKind::INLINE`
is already accepted by the API), streaming TTS playback, and barge-in
cut-through in MSS.

**Phase 4 — Full media plane** is the N-way mixer, monitor/whisper as
attachments and playbacks rather than conference tricks. Do not start before
Phases 1–3 are boringly stable (Constitution, Article VIII).
