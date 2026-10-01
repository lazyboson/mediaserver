# Tasks — what is left, and the record of what is done

Living work list. [roadmap.md](roadmap.md) holds the *why* and the phase exit
criteria; this file holds the *what next*, ordered, with a definition of done
for each item. Update it in the same PR that changes the state of an item.

Status as of **2026-08-30**.

---

## What is left — read this first

Everything after this section is the **record** of work already finished, kept
because the evidence in it is the reason each decision stands. This section is
the only part that describes work not yet done. Items are `L*` for engineering
in this repository, `P*` for proof that needs a lab box, and `H*` for what is
blocked on a deployment.

### L — engineering, in this repository

| # | Item | Why it matters | Done when |
| --- | --- | --- | --- |
| **L1** | **Opus egress** (item **16d**, *"still genuinely later"*) | `crates/media-core/src/encode.rs` refuses it by name — *"opus output is not built yet"*. The `opus-ffi` encoder exists and nothing wires it to the consumer encode path or the playout pacer. Opus is **ingest-only**, so an Opus-only consumer cannot be served and an inline leg cannot answer an Opus-only offer — which is every WebRTC browser that does not also offer G.711 | a consumer attaching with `format=opus` receives decodable Opus, an inline leg answers an Opus-only offer, and a replay test asserts the round trip |
| **L2** | **Renegotiation for a changed offer** (P3-2's media half) | The SIP half is closed — the transaction and the dialog run correctly — but `session-core` cannot produce a second answer, so a re-INVITE whose offer differs is refused **488**. Hold with a changed media description, attended transfer and codec change all land here. **A warm transfer *is* an attended transfer**, so this is a conference case, not a peripheral one | `session-core` re-answers an existing inline session, the front door returns it, and tests cover hold (`a=sendonly`), resume, and a codec change |
| **L3** | **Pod placement and room affinity** (defect **D8**) | `owner_pod` is a config string and there is no scheduler. A room is one pod's mix thread, so whoever opens a leg must already know which pod owns the room — over gRPC *or* over the SIP front door. This is the one that bites a multi-pod pilot | something decides placement — an MSS-side lookup, or a contract the caller follows that is written down — and a two-pod drill seats two legs of one `group` on one pod without the caller choosing |
| **L4** | **DTMF generation** | `media-core/src/dtmf.rs` decodes only. MSS detects RFC 4733 and cannot send a digit, so it cannot drive an external IVR | an RFC 4733 event train, three end retransmissions included, is generated on an inline leg and a replay test asserts it |
| **L5** | **Front door: metrics, and TCP/TLS** | Answered, refused and hangups are counted in a struct and logged at shutdown only — nothing reaches `/metrics`, so a pilot cannot see the door, and since [item 60](#60-call-control-over-the-event-stream-park-answer-hang-up--done-2026-09-03) it cannot see parked calls, park timeouts or BYEs that Timer F gave up on either. `Reliability::Reliable` is implemented and tested but only UDP is bound | `mss_sip_*` counters exported with alert rules in `deploy/`, and a TCP listener bound and drilled |
| **L6** | **Codec breadth — G.722, G.729** | `Encoding` is `Pcmu \| Pcma \| L16 \| Opus`. G.722 is common on modern desk phones and G.729 on trunks | each decoded at ingest and encoded at egress, **adopted not written** (Article XI), with `cargo deny` clean |
| **L7** | **SRTP/DTLS in-process — decide, then build or close** | rtpengine terminates crypto at the edge today, which is right for the reference deployment. It is only needed if an inline leg must face a WebRTC endpoint with no rtpengine in front | either recorded in architecture §7 as a permanent non-goal, or built behind an FFI wrapper crate |
| **L8** | **Conference feature tail** | Not built (Appendix B.2): member enumeration, room lock, moderator roles, floor control, per-member volume and energy thresholds. The matrix already carries a per-pair Q12 gain, so per-member volume is one metadata verb away | each is either a mix-matrix cell named by metadata, or recorded in Appendix B.2 as the controller's job |
| **L9** | **AGC, DC filter, echo cancellation** | `mss_conference_clipped_samples_total` is the signal that a room wants AGC; nothing acts on it | decided, and if built then adopted per Article XI rather than hand-rolled |
| **L10** | **D6 — `play media` from-tag semantics are unmeasured** | architecture §6's claim was retracted after the instrument turned out to be broken, and §6 must not be trusted until it is re-probed | re-probed on a live call and §6 either restored or corrected |
| **L11** | **eBPF tap ingest — decide, do not build** (item **18**) | The open question is whether RTP can be mirrored to MSS by an eBPF program on the rtpengine host instead of NG `subscribe`. It is gated on a measurement nobody has taken: the userspace copy cost a `subscribe` imposes on the rtpengine host (handoff **H3**) | the measurement exists and item 18 records a decision — build, or close it as a non-goal |
| **L12** | **TLS options** (item **52**, ⏸ parked) | Built and verified on branch `feat/tls-options` — tonic TLS and mTLS on the gRPC port, rskafka TLS/SASL, a TLS-capable Redis client, redacted URLs — and deliberately kept off `main` because the first deployment runs every hop inside one cluster. The security posture until then is network policy | a hop leaves the cluster, and the branch is rebased and merged. **The SIP front door is a new hop with the same question**, and it has no TLS at all (see L5) |
| **L14** | **The IVR between "invited" and "answered"** | [Item 60](#60-call-control-over-the-event-stream-park-answer-hang-up--done-2026-09-03) built the seam and nothing runs in it: a parked call has a media session, an RTP port and an SDP answer, but no audio flows until the 200, so a prompt-and-collect before answering has no path. Early media (183 + SDP) is the SIP half of that and is not built | a parked call can play a prompt and collect digits before it is answered, or the design records that the IVR runs after the answer and early media is a non-goal |
| **L15** | **The orchestrator pairing is unproven end to end** | The call-event contract and both RPCs were built against a specification, not against a running consumer, and `parked` mode with nothing listening rings every call until the park timeout. The `end_of_interaction` → `HangupSession` pairing — the defect item 60 exists to fix — has never been driven by a real orchestrator | a real orchestrator answers and hangs up a call through the stream, and the silence-after-farewell defect is observed gone rather than argued gone |
| **L13** | **Spill journal retention** (item 30 / **D9**'s residual) | Skipped and failed spill journals stay on disk until an operator removes them; nothing expires them, so a pod that fails uploads repeatedly fills its spill volume | a retention rule exists and is documented in deploy.md, or the growth is bounded and alerted |
| **L16** | **Idempotency keys on every mutating RPC** | Only `CreateSession`, `Attach`, `UpdateAttachment` and `StartPlayback` carry an `idempotency_key` (`proto/mediacontrol.proto`). `DestroySession`, `AnswerSession`, `HangupSession`, `Detach`, `SendToAttachment` and `StopPlayback` cannot be deduplicated, so a client retrying after a lost reply can repeat a BYE or a stop, or get an error back for an operation that actually succeeded | every mutating RPC is deduplicable by key, or each one left out is recorded here with the reason its replay is harmless |
| **L17** | **Trace context on `mss.events`** | The event pump publishes with `headers: Default::default()` (`crates/mediaserverd/src/event_pump.rs`), so no OpenTelemetry context crosses Kafka and a consumer cannot join an event to the RPC or call that caused it | published events carry a W3C `traceparent` header, or a decision not to is recorded here |

### P — proof, needs a lab box

| # | Item | Why it matters | Done when |
| --- | --- | --- | --- |
| **P1** | **Retire `lab/sip_shim.py`** | The front door does strictly more than the shim, but **no datagram from a real FreeSWITCH has ever reached it** — every test constructs its own. The shim is still the only proven path and both drills still reference it | `lab/fs_control_drill.sh` and `lab/fsless_call_drill.sh` point at `MSS_SIP_LISTEN`, the drill is green with the same assertions as the 2026-08-29 run, then `sip_shim.py` and `docker-compose.shim.yml` are deleted and `lab.md` records the run |
| **P2** | **A live run of the conference member verbs** | `deaf`, `hold` and the coach shape (`mix_source=leg`) have item 40's in-process socket tests only — item 41's real-socket drill covers minus-self, monitor, whisper, barge and mute, and says so | the three-peer drill grows phases for deaf, hold and coach, green at the same margin |
| **P3** | **A human listens** | **No measurement in this repository has been judged by ear.** Every assertion is a tone at a ratio; tones cannot hear distortion, clipping artefacts or a mix that is technically correct and unpleasant | somebody listens to a recorded conference and an inline leg and says whether it sounds right |
| **P4** | **The filesystem recording store, on a live call** | Item 58's residual: `MSS_RECORDING_STORE=filesystem` is unit-verified on a temp directory, and the lab drill never ran because there was no Docker daemon on the machine | a live call recorded to a mounted tree, with a `file://` `UploadCompleted` seen on the bus |

### Deliberate non-goals — these are not debt

Recorded here so they stop being re-proposed. Each is a boundary the design
picked on purpose, with the reason.

- **No UAC beyond the BYE.** MSS answers and never dials: no originate, no
  re-INVITE, no transfer initiation. The one client transaction is the
  in-dialog BYE `HangupSession` sends when call control asks for it
  ([item 60](#60-call-control-over-the-event-stream-park-answer-hang-up--done-2026-09-03)).
  An expired session timer ends the media and sends no BYE, because tearing
  down a dialog is call control's.
- **No registrar, no proxy, no routing, no forking.** A media server needs a
  fraction of a proxy; the rest belongs to OpenSIPS or the integrator's stack.
- **No video.** `m=video` is refused by name as `NonAudioMedia`.
- **No in-band DTMF menus and no automatic enter/exit sounds.** Conference
  control is API-first; MSS hands over digits and interprets none, and
  play-into-room is a verb rather than a trigger (Appendix B.2).
- **No cross-pod rooms, no multi-rate rooms.** A room is one pod's mix thread
  at one rate and one ptime, capped at 32 members.
- **No IVR, dialplan or scripting.** The state machine is the controller's; MSS
  provides the primitives it calls.

### H — blocked on a deployment

Eight items, unchanged in shape and listed in full under **Integration handoffs
(deployment-gated)** below. None of them can be closed from a lab. Two more
buckets of externally-blocked work sit further down and are open too:
**Waiting on other people (M2 close-out)** — the deployed rtpengine version, the
rtpengine-side per-tap cost, and one `cachedb_redis` config block on somebody
else's proxy — and **item 1**, the legacy controller's event translator, which is written and
awaiting review and merge on their side.

---

## Milestones

| Milestone | Scope | State |
| --- | --- | --- |
| **M1 — scaffold** | workspace, sans-IO cores (RTP, G.711, DTMF, jitter), NG bencode, consumer dialects, two-world daemon skeleton, watchdog | ✅ done (2026-08-13) |
| **M2 — Phase-0 spike** | real NG subscribe against lab rtpengine, both legs jitter-buffered to WAV, per-tap cost | ✅ **code done**; 3 org-side items open (below) |
| **M3 — fan-out hub** | per-session pub/sub, N consumers, WS-Twilio adapter, pause/resume/send_text parity | ✅ done |
| **M4 — control plane** | `MediaControl` gRPC, session state machine, Kafka events, Redis registry, tenant-flag pilot | 🔶 **code complete** — pilot gates: translator merge (external), and the consumer half of the barge-in number — **every MSS-owned hop is measured (item 5, 2026-08-23: cut-through p95 4.8 ms from a real consumer `SpeechReport`)**, the D19 ingress gap it found being fixed in item 28. The gRPC lab proof (item 10) and the **live pod-kill drill (item 11, gap 14.41 s)** are both **done 2026-08-22**; the D14 orphan subscription the drill found is **fixed (item 25, 2026-08-23)**, fake- and Redis-verified rather than re-measured live |
| **M5 — recording (Phase 2)** | per-leg taps → stereo segmenter → S3, identity + callback contract, dual-recording | 🔶 **code complete (2026-08-22)** — a live tapped call recorded end to end to a real MinIO, callbacks read off `mss.events` (2026-08-22, item 10's drill); **recording groups** — N sessions recorded as one recording, one mono object per participant, time-aligned on the group's open instant since item 29 — landed 2026-08-23 (item 21); FS byte-parity **measured against a real FS recording** 2026-08-23 (item 31): container/layout/rms exact, a re-aligned 2 s window agrees 1.0000 at mean diff 0.6/32768; owed: a two-party production-FS comparison and a human listen. **Store choice (item 58, 2026-08-27):** the same recorder now writes to S3 (default) or a shared filesystem tree at `MSS_RECORDING_ROOT` — one variable, the frozen identity verbatim, `file://` in the event; unit-verified on a real temp directory, never yet on a live call |
| M6+ | Phases 3–4 (interactive media, full media plane) | 🔶 **Phase 3 code complete and lab-verified (items 32–35, 2026-08-23)** — an inline leg answers an SDP offer, is spoken to over a continuous inject stream, and barges in **p50 12.2 ms / p95 20.4 ms** measured against a real RTP peer. **Phase 4 is code complete and lab-verified too (2026-08-24):** the N-way mix matrix (item 36) and **conferences of inline legs** (item 37, 2026-08-24 — one clock per conference, each leg hears everybody but itself, mixed track on the hub) and **monitor / whisper / barge as metadata-named matrix cells** (item 38, 2026-08-24 — `only=mixed` is the monitor, `mix_target=<member>|all` on an INJECT attachment is the whisper and the barge flip) are code complete and verified over in-process sockets, as is **native conference recording** (item 39, 2026-08-24 — one mono object for the room via `only=mixed`, one object per participant via a recording group, both at once, the shape named in `RecordingStarted`) and **the conference feature tail** (item 40, 2026-08-24 — `member_mute`/`member_deaf`/`member_hold` as metadata verbs, `StartPlayback{target_tag=all}` as a prompt into the room, `mix_source=leg` for a coach's own voice, plus the generic feature list and the adapter parity table in architecture.md Appendix B). **All of it is lab-verified on real sockets (item 41, 2026-08-24):** three container peers in one conference, twenty tone-per-phase assertions green at a ≥30:1 margin — minus-self, monitor, whisper isolation, the barge flip, mute/unmute off every ear and off the mixed track, and both recording shapes landing in MinIO at once. Production integration (a SIP proxy's B2B leg into a conference) and the org-gated criteria remain |

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
  consecutive live calls, independent of stream order. A **mid-call SSRC
  change** re-resolves too (item 14): the leg re-enters resolution and a
  control-world task re-queries and pushes a fresh map over a bounded queue
  — replay-verified, not yet watched on a live re-INVITE.
- **Lab** — OpenSIPS/FreeSWITCH/rtpengine/Redpanda/MinIO compose stack,
  synthetic caller (`host_test_caller.py`), per-track analyser
  (`track_dump.py`), RTT and recorder mock consumers, NG probes, `mss_ctl`
  control-plane CLI, `mss_events_tail` bus consumer, `mss_stream_probe` gRPC
  consumer and `grpc_stream_drill.sh` (item 10) which taps a live call over
  the gRPC data plane, records it to S3 and scrapes the metrics in one
  command. Since item 11 the stack also runs **three MSS pods** on one Redis,
  and `pod_kill_drill.sh` kills the owning one mid-call and measures the
  consumer's audio gap (`gap_consumer.py`, `ng_call_tags.py`). Since item 19
  there is also `soak.py` — N concurrent calls for hours through a walk of
  impairment profiles, asserting on `/metrics` every minute and failing loudly —
  with `netem.sh` for the tap-link impairment on a kernel that has netem.
  Since item 21 there is `group_recording_drill.sh` — two fabricated calls, two
  sessions, one recording group, one object per participant in MinIO — and
  `call_driver.py` takes a `COOKIE_PREFIX` so two drivers can fabricate two
  calls at once without colliding in rtpengine's reply cache. Since item 23
  there is `kernel_probe.sh` (does this rtpengine forward in the kernel,
  answered over NG alone, runnable on a metal box) and `opus_call_driver.py`
  (a **native** Opus call, libopus in the endpoints, so rtpengine transcodes
  nothing).

## The record, items 1–9 — the original next-up list

All of these are finished; the open list is **What is left** above.

### 1. Legacy controller event translator — ✅ WRITTEN, awaiting review and merge
**State (2026-08-17):** implemented on the legacy translator branch as an `msstranslator` package, **local and
uncommitted by instruction**. Pure `Render` plus a Kafka consumer on its own
group (`mssEventTranslator`), wired into the legacy controller's gRPC server behind
`MSS_EVENTS_TOPIC` so an unconfigured deployment is unchanged. 15 unit cases
plus a broker-backed test (`-tags=integration`, `MSS_TEST_BROKERS`) that
publishes a MediaEvent and reads the legacy event back off the target topic:
verified end to end against the lab Redpanda, rendering
`mod_audio_fork::first_transcript` with the exact `gsrResult` body
`handleTranscribe` unmarshals, and dropping a non-authoritative attachment's
transcript in the same run. Confirmed against the legacy controller's code that
`request_uuid` is the FreeSWITCH channel UUID (it is passed straight to
`uuid_audio_fork <uuid> start`), so `external_id` keys and shards correctly.
Remaining: review, commit, merge, and a pilot tenant.
**Review note added 2026-08-22 (item 13):** MSS event delivery is now
**at-least-once**, so `handle` should dedupe on `(external_id, seq)` — it
forwards every record today, and its `MarkMessage`/auto-commit loop can
already replay one on a rebalance.

### 1b. (original description, for reference) Legacy controller event translator
**Where:** the legacy controller's repo, not here (architecture §5.4 — the positional
format *is* `constants.MapKeyIndex`, a Go constant table; encoding it in Rust
would couple MSS to a file that changes without our knowing).
**What:** ~200-line Go consumer: read typed `MediaEvent`, drop anything with
`legacy_eligible == false`, render `mod_audio_fork::*` names into
`Events{repeated string}` on `eventTopic`, preserve the UUID key and OTel
context.
**Why now:** MSS publishes events nothing consumes yet. Until this exists no
tenant can be flipped, so it gates everything else in Phase 1.
**Done when:** the application server drives the legacy stream state machine from an MSS-tapped call with
`mod_audio_fork` uninvolved, and the event-name mapping table in
`proto/mediacontrol.proto` matches the shim one-for-one.

### 2. `TelCompat` façade — ✅ DONE (2026-08-17)
Landed in `crates/control-api/src/telcompat.rs` with
`proto/telcompat.proto` declaring `protos.TelService` so the method paths match
the legacy controller's byte for byte. Serves stream/recording/playback verbs onto the nouns,
one test per mapping row, both surfaces on one port, proven over a real socket
with a generated legacy controller client. `StartCallTranscription` returns `UNIMPLEMENTED`
by design (the ASR endpoint is not in its request message).
**No longer blocked (2026-08-17):** a TelCompat session has only the channel
uuid, but MSS resolves the rest itself — the caller passes the SIP call-id and
the caller's from-tag (both already on the channel) and `TapPlane` asks
rtpengine's `query` for the participants. The OpenSIPS→Redis discovery map is
now an optimisation, not a prerequisite (see the M2 close-out list below).

### 2b. (original description, for reference) `TelCompat` façade
**Where:** `crates/control-api`.
**What:** a second gRPC service reusing the legacy verb API's message shapes
verbatim — `StartStream`, `StopStream`, `StreamPause`, `StreamResume`,
`StreamSendText`, `StreamPlayFile`, `StartCallTranscription`,
`StartRecording`, `StopRecording` — translated onto Session/Attachment/
Playback per the §5.6 table, setting `authoritative` from the verb that
created the session.
**Why now:** it is the migration switch: a tenant flag routes the legacy controller to
its own gRPC server or to MSS with no client change and rollback by config.
**Done when:** the mapping table is executable (one test per row), and a
`StartStream` call produces the same session/attachment shape as the
equivalent native calls.

### 3. Redis session registry + re-subscribe on pod loss — ✅ DONE (2026-08-17), live pod-kill observed 2026-08-22
`session_store.rs` (namespaced keys, TTL'd leases, atomic `SET NX` claim) plus
`registry_keeper.rs` (persist, renew, adopt, release). Adoption rebuilds through
the controller's own API so every invariant applies to a rebuilt session, and
`TapPlane` re-establishes the subscription. Verified against real Redis,
including six pods racing for one orphan producing exactly one owner, and on a
live lab call. Sessions with no call identity are released rather than
half-restored; ended sessions are forgotten so no pod adopts a dead call.
**Proved on 2026-08-22 (item 11):** three pods on one Redis, `kill -9` on the
owner mid-call, one survivor adopted 14.6 s later and the consumer's audio
resumed after a **14.41 s** gap. What the same run showed missing is D14: the
dead pod's rtpengine subscription is never cancelled.

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

### 4. Exported metrics — ✅ DONE (2026-08-20)
`crates/mediaserverd/src/metrics.rs`: a dependency-free Prometheus text
endpoint on `MSS_METRICS_LISTEN`, serving ingest/jitter/consumer/event-pump/
registry counters plus live gauges, with the capture thread publishing
per-leg stats into shared atomics each release tick and `TapPlane` folding
finished legs into retired totals so every counter stays monotonic across
session churn. The audio-flow watchdog (`supervisor.rs`) is finally wired:
per-leg stall state and stall transitions are exported (`mss_legs_stalled`,
`mss_ingest_stalls_total`). Alert rules for every drop counter live in
`deploy/prometheus-alerts.yaml`. Verified by unit tests including a real
HTTP GET; not yet scraped by a real Prometheus in the lab.

### 4b. (original description, for reference) Exported metrics
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

### 5. Barge-in cut-through measurement — 🔶 **every MSS hop measured (2026-08-23)**
**Where:** lab — `lab/barge_drill.sh`, `lab/barge_translator.py`.
**What was measured:** the whole chain MSS owns, against the live lab stack —
a real gRPC consumer sends `SpeechReport(STARTED)` → MSS publishes
`SpeechStarted` on Kafka `mss.events` → a mock translator consumes it → it
calls `StopPlayback` → MSS acks. Two runs of 10 iterations, 10/10 completed,
no event missed: **cut-through p50 3.98 / 3.54 ms, p95 4.78 / 4.38 ms, max
4.78 / 4.38 ms**; the consumer wire plus the bus (hops 1–2) 1.7–2.4 ms; the
translator's own decide-and-call 1.9–2.6 ms; container/host clock skew
−0.14 ms. The first version of this drill had to trigger on `PlaybackStarted`
because hop 1 had no wire (D19, found by that run and **now fixed**); it read
p50 3.31 / 3.15 ms, so **putting the real consumer report in front of the bus
cost under a millisecond**. The whole chain fits inside one 20 ms frame with an
order of magnitude to spare, so **architecture §9 risk 9's fallback (a gRPC
stream for speech events only) is not needed** on these numbers. Method, the
per-interval table and the caveats are in
[lab.md](lab.md#barge_drillsh--barge-in-cut-through-all-four-hops-2026-08-23).
**What is still owed, and by whom:** the **integrator's consumer half is
theirs to add** — hop 3 here is a Python mock with the topic to itself, and
the reference deployment's translator (awaiting review in its own repo) is one
such consumer with its own traffic and a call-control hop after `StopPlayback`.
Two limits remain, both by nature: the consumer's own **detection** latency
(how long an ASR takes to decide speech began) is the consumer's, not MSS's,
and `StopPlayback` acked is not the last audible sample — the media-path cut
needs an ear on the leg. **That ear now exists (item 35, 2026-08-23):** on an
inline leg the media-path cut from `Clear` to silence at the peer measures
**p50 12.2 ms, p95 20.4 ms** over 20 iterations, one ptime as the pacer
promised. On a *tapped* call the equivalent tail is rtpengine's `stop media`,
still unmeasured for want of an ear on a tapped leg.

### 6. gRPC `MediaStream` data plane — ✅ DONE (2026-08-20)
`crates/control-api/src/stream.rs` implements `MediaStream::Subscribe` over a
new `MediaPlane::open_stream` seam that `TapPlane` serves from the hub. A
consumer attaches via `MediaControl` (transport `GRPC_STREAM`), then dials in
with `ConsumerHello{attachment_id, token}`; it gets `StreamStart`, raw binary
frames (no base64), DTMF, `send_text` passthrough as `TextFrame`, and a
`StreamStop` on detach, session end or drain. `inject` is capability-checked
at the first frame — an unprivileged attachment gets `PERMISSION_DENIED` and
the stream ends (a protocol violation, not a no-op); an authorized utterance
flushes on `Mark` through the registry-tracked `StartPlayback` path (so it is
evented, capability-attributed and idempotent) and `Clear` discards + stops
the last playback (the barge shape). Wire-verified through a generated client
against a real socket, including the drain regression. Limits, deliberate:
one connected consumer per attachment (reconnect allowed after it drops),
the attachment format must be one the encoder serves (since item 8: g711 at
the tap rate, or L16 at 8k/16k/48k — Opus is item 9),
and an utterance is capped at one playback datagram — chunked/paced playback
stays with the WS bridge. This was proved on a **live tapped call** in item 10
(2026-08-22): L16/16k over the gRPC data plane, ASR-verified intelligible.

### 7. Auth on attachments — ✅ DONE (2026-08-20)
`crates/control-api/src/auth.rs`: `AuthPolicy`, a shared bearer secret from
`MSS_AUTH_TOKEN`. It is a tonic interceptor on `MediaControl` (missing or
wrong `authorization: Bearer …` → `UNAUTHENTICATED`) and verifies
`ConsumerHello.token` on `MediaStream` (constant-time comparison). Unset env
= open lab mode, logged loudly at startup. **`TelCompat` is deliberately not
intercepted**: its contract is byte-identical legacy controller clients with no client
change, and the legacy controller does not send metadata — the pilot fronts it with network
policy instead. `mss_ctl` sends the bearer when `MSS_AUTH_TOKEN` is set.

### 6b/7b. (original descriptions, for reference)
**6:** implement the generated `MediaStream::Subscribe` — binary frames,
`ConsumerHello` auth, `inject` honored only with `INJECT` capability, marks
and `clear`. **Done when:** a consumer receives a live tap over gRPC and an
unprivileged attachment's `inject` is refused.
**7:** verify `ConsumerHello.token`; an interceptor on `MediaControl`.
**Done when:** an unauthenticated consumer cannot attach, and MediaControl
rejects unauthenticated callers.

### 8. Codec pipeline breadth — ✅ DONE for L16 + resampling (2026-08-22)
`media-core/src/encode.rs`: `ConsumerEncoder`, one per consumer track —
G.711 (µ/A) passthrough at the tap rate, L16 little-endian at 8k, and
resampled L16 at 16k/48k via `rubato` 5 (`Fft`, fixed-input chunks matching
the tap's 20ms frames; short frames zero-padded so streams stay continuous).
The hub now carries PCM (`TapEvent` holds `i16` samples), which makes the
"L16 interchange, one decode per ingest, N encodes per consumer" rule real:
the WS bridge encodes its own µ-law (dialect unchanged, byte-exact tests
untouched) and the gRPC pump encodes per attachment format. A grpc-stream
attachment may declare L16/16k at Attach; the WS transport refuses anything
but PCMU 8k by name (frozen dialect). gRPC inject also accepts L16.
Done-when met: an L16/16k consumer is verified by replay
(`l16_16k_doubles_the_sample_count_and_preserves_the_tone`, pump test).

### ~~9. Opus output~~ — superseded by 16a and 16d
**The build trade this item existed to pose has been decided** (16a,
2026-08-23): the toolchain is accepted, libopus is vendored and bound through
the `opus-ffi` crate, and building needs cmake/make/g++. **Opus ingest is
done**; Opus *output* to consumers is tracked in **16d** and is still genuinely
later, because no consumer has asked for it. The original framing is kept below
for the reasoning it records, but read 16a/16d for the current state.
**Where:** `media-core` (+ a dedicated FFI wrapper crate — now `opus-ffi`).
**What:** Opus encode (Article XI: adopt libopus, never reimplement).
**Why later:** no consumer asks for Opus yet; L16/16k covers the ASR
vendors we know about.
**Done when:** a consumer can request Opus and get it, verified by replay,
with the build documented in CI and the Dockerfile.

## The record, items 10–59 — everything since

All of these are finished; the open list is **What is left** above.

Items 1–9 above are M4 history; this section is the executable plan for
whoever picks the project up next (human or AI session — it assumes no
memory of the sessions that built M1–M4). **Read
[CLAUDE.md](../CLAUDE.md), then [session-playbook.md](session-playbook.md),
then this list, in that order.** The standing rules that are not optional:

- Work on a branch, open a PR — never commit to `main` directly.
- The gate, run as separate commands, every one read before the next:
  `cargo test --workspace` · `cargo fmt --all --check` ·
  `cargo clippy --all-targets -- -D warnings` ·
  `grep -rn '//' crates --include='*.rs'` (must print nothing) ·
  `cargo deny check all`.
- No comments in `.rs` files; module context goes to
  [implementation-notes.md](implementation-notes.md) **in the same PR**;
  item state changes update this file in the same PR; milestones update
  [roadmap.md](roadmap.md).
- Probe vendors before building on their behavior (`lab/ng_*_probe.py`
  pattern); machine-verify (`lab/host_test_caller.py`,
  `lab/ear_intelligibility_probe.py`) before asking a human to dial;
  one change per verification cycle.
- A checkbox claims only what was measured. If a thing ran only against a
  fake, say so where you record it done.

### 10. Lab proof of the gRPC data plane — ✅ DONE (2026-08-22)
**What shipped:** `crates/control-api/examples/mss_stream_probe.rs` is the
gRPC consumer — it attaches a `GRPC_STREAM` attachment at a format it chooses
(`MSS_PROBE_ENCODING`/`MSS_PROBE_RATE`/`MSS_PROBE_TRACKS`, default L16/16k
customer), dials `MediaStream::Subscribe` with a `ConsumerHello`, decodes the
little-endian frames and writes one mono wav per track with frames, bytes,
duration, rms and peak per track, then detaches. `lab/grpc_stream_drill.sh`
drives the whole thing against a live call with no human dialing, and
`mss_ctl` grew `record` / `pause` / `detach` so the same call can carry a
`FILE_S3` recording. `lab/docker-compose.microsip.yml` now gives
`mss-control` `MSS_METRICS_LISTEN` on a published port.
**Verification rung reached: (a) a full SIP lab call.** MicroSIP-shaped host
caller → OpenSIPS → rtpengine 14.1.1.8 → FreeSWITCH 9000, tapped by the real
`mediaserverd` control plane in the compose stack. Measured over a 30 s
probe on a 45 s call, L16/16k, `tracks=all`: customer 1500 frames /
480,000 samples / 30.00 s at **rms 614.5**, agent rms 8 (FreeSWITCH
`silence_stream`), mixed rms 0; both tap legs `jitter_lost: 0`,
`frames_concealed: 0`, `recv_errors: 0`. **Intelligible, ASR-judged**:
`ear_intelligibility_probe.py` replayed the customer wav into
stream-llm-bridge and Deepgram returned the sentence verbatim on three of
three complete repetitions. Metrics scrape:
`mss_consumer_delivered_total` 3657 → 11088 across the probe window,
`mss_consumer_dropped_oldest_total 0`, `mss_consumer_queue_depth_frames_max
0`, `mss_legs_unknown_ssrc 0`, `mss_legs_stalled 0`. Procedure, tables and
the full scrape are in [lab.md](lab.md).
**It found a production defect (D12, fixed here).** The third back-to-back
run was pure silence with `datagrams: 0` on both legs: `CookieSequence`
restarted its serial at 0 and `TapPlane` binds a new `NgTransport` per
session, so every session's first NG command was cookie `<prefix>-0` — and
two sessions inside rtpengine's duplicate-cookie reply-cache window got the
**same cached subscribe answer**, so the second tap pointed at a
subscription that no longer existed. The serial is now process-wide. Proved
both ways in the lab: before, two runs 53 s apart got byte-identical offers
(source ports 30008/30016) and the second was silent; after, they get
distinct subscriptions and both carry audio at rms 655.5.
**Also proved, for item 15:** the same call recorded to MinIO —
`acct-grpc/rec-<id>.wav`, 2 ch / 8 kHz / 39.88 s exactly equal to the
reported `duration_ms`, customer left (rms 610) and agent right (rms 7) —
with `RecordingStarted`, two `RecordingPaused` edges carrying the *same*
`duration_ms`, `RecordingStopped` and `UploadCompleted` read off the real
`mss.events` topic with `mss_events_tail`.
**Left open, recorded as D13:** with `TrackSelector::All` the `StreamStart`
frame advertises `["customer","agent"]` but a silent `mixed` track arrives
too, at the full frame rate.

### 10b. (original description, for reference) Lab proof of the gRPC data plane
**Where:** `crates/control-api/examples/` + `lab/`.
**What:** the `MediaStream` service (item 6) has only ever run against the
fake plane. Write `mss_stream_probe.rs` — an example binary using the
generated `MediaStreamClient` (copy `mss_ctl.rs`'s shape): attach a
`GRPC_STREAM` consumer with `L16/16k` to a live lab call, subscribe with
`ConsumerHello`, decode the little-endian frames, write a WAV.
**Direction:** drive a call with `lab/host_test_caller.py`; create/attach
via `mss_ctl`; validate the WAV with `lab/ear_intelligibility_probe.py`.
Scrape `MSS_METRICS_LISTEN` during the run and check
`mss_consumer_delivered_total` moves and `mss_consumer_dropped_oldest_total`
stays 0. Remember `MSS_AUTH_TOKEN` unset = open lab mode.
**Done when:** a live tapped call is intelligible from a gRPC/L16-16k
consumer's WAV, and the run's metrics scrape is pasted into the PR.

### 11. Pod-kill re-subscribe drill (Phase-1 exit criterion) — ✅ DONE (2026-08-22)
**What shipped:** `lab/pod_kill_drill.sh`, plus two instruments it needed —
`lab/gap_consumer.py` (a WS_TWILIO consumer built to measure an outage: it
stamps every frame's arrival, survives the reconnect, and writes one wav per
track on an arrival timeline so the outage is a run of samples no frame ever
covered) and `lab/ng_call_tags.py` (asks rtpengine `query` how many taps a
call carries). `docker-compose.microsip.yml` gained `mss-control-b` and
`mss-control-c`: three pods on one Redis, differing only in pod name,
published ports and tap address. **Two** survivors on purpose — with one
candidate, "exactly one adopter" is a tautology.
**The measured gap: 14.41 s** (longest arrival gap 14407 ms, longest
uncovered run 14380 ms, identical on all three tracks; the WS connection was
dead 14.35 s). Adoption landed 14.6 s after `kill -9`, by pod C. Two earlier
runs of the same drill: 19.55 s / 19.7 s and 17.97 s / 18.6 s. All three sit
inside the arithmetic the design implies — lease 15 s renewed every 5 s, adopt
sweep every 10 s, so **worst case 25 s**.
**The three assertions, as measured:** exactly one adopter
(`mss_registry_adopted_total` 1 → 2 on pod C, unchanged on pod B); zero
`mss_registry_lost_total` on both survivors (and zero `unrebuildable`, zero
`failed`); **the orphan assertion failed** — see D14 below. The rebuilt tap
was healthy, not merely present: Customer 5551 / Agent 5640 datagrams,
`jitter_lost: 0`, `frames_concealed: 0`, `recv_errors: 0`,
`dropped_oldest: 0`, both legs named. Procedure, tables and the rtpengine
teardown evidence are in [lab.md](lab.md).
**It found a defect (D14, filed not fixed).** The adopter creates a new
subscription and nothing cancels the dead pod's: the tap's `to-tag` is not
persisted, so no survivor can. rtpengine's teardown block priced it —
**14,743 packets / 2.5 MB copied to a pod that had been dead for 110 s**,
four ports held until the call ended. The fix needs a new seam (persist the
to-tag; `unsubscribe` it on adopt) plus a decision about a
partitioned-but-alive owner, which is more than this lab item should land.
**Two instrument findings worth reusing** (both in lab.md): rtpengine
`query` does **not** show a *lone* subscription — both taps appeared only
once a second `subscribe` touched the call, so a 0 from `ng_call_tags.py`
means "0 or 1" — and a query reply's `stats_out`/`last packet` are stale,
while the teardown "Final packet stats" block has the real totals. Under
Docker Desktop on WSL2 the pods reach a host-run consumer at
`host.docker.internal`, not at the lab bridge gateway (refused) and not at
`ADVERTISED_IP` (times out).
**Also filed:** D15 — an adopted attachment loses its negotiated format
(`PersistedAttachment` has no format field). Found by reading the adoption
path, not observed, since this drill's consumer was WS/PCMU.
**Not covered:** the D9 half of a pod death — this drill attached no recorder.
Item 30 has since spilled closed segments to disk and made the adopter pad and
count what it cannot read, but that is replay-proven only: a pod-kill drill
*with a recorder attached* has still never been run. And the gap is a lab number on an idle box; the
soak suite (item 19) should re-measure it under load.

### 12. Barge-in cut-through measurement — 🔶 **MSS+bus half measured (2026-08-23)**
Item 5 above carries the numbers, and they are the ones to quote: a real
consumer `SpeechReport` to an acked `StopPlayback` at **p50 3.98 / 3.54 ms and
p95 4.78 / 4.38 ms over two runs of 10 live iterations** (the earlier ~3.2 ms
figure was the pre-D19 `PlaybackStarted` trigger). The Kafka hop makes the budget
and the gRPC-for-speech-events fallback stays unbuilt. What remains for the
Phase-1 exit criterion is not ours to measure: the **integrator's consumer
half** (the legacy controller's translator merge, still external) plus the two gaps named in
item 5 — of which the missing consumer→MSS speech-report wire (D19) is now
**fixed and measured** (item 28), leaving the audible cut inside rtpengine.

### 13. Event delivery durability (defect D5) — ✅ DONE (2026-08-22)
**Where:** `crates/mediaserverd/src/event_pump.rs`.
**What shipped:** a FIFO retry backlog inside the pump worker. The head is
retried (100 ms doubling to a 5 s cap) before any newer event is attempted,
so nothing is reordered; the worker keeps draining the handoff queue while
it waits, so `accept` stays non-blocking; the backlog caps at 8192 and
evicts the **oldest** unsent event beyond that, counted. Each attempt has a
5 s timeout, and shutdown is bounded (10 s flush in `main`, then 3 attempts
before the remainder is counted `abandoned`). New series:
`mss_events_retried_total`, `mss_events_dropped_oldest_total`,
`mss_events_abandoned_total`, gauge `mss_events_retry_depth`, all alerted in
`deploy/prometheus-alerts.yaml`. `mss_events_failed_total` now counts failed
**attempts**, not lost events.
**Semantics shipped: at-least-once.** A retry after an ambiguous failure can
duplicate a record, so **the legacy controller's translator must treat `(external_id,
seq)` as idempotent**. Checked in the translator's source on the legacy translator branch
(`msstranslator/consumer.go`,
read not run): it dedupes nothing — `handle` renders and forwards every
record — but it is *already* an at-least-once consumer, because it
`MarkMessage`s after handling and sarama auto-commits, so a rebalance or
crash already replays records. Our change therefore adds no new class of
duplicate; it does make the case for a `(external_id, seq)` seen-set in
`handle` concrete. **Flag it in the item-1 review.** No duplicate was
observed in the drill (0 of 60), but the guarantee is at-least-once.
**Verified against a real broker** (`lab/event_outage_drill.sh`, Redpanda
`docker stop` for 30 s mid-run, 60 events at 1/s through the production
`RskafkaTransport`): `accepted=60 published=60 failed=6 retried=6 dropped=0
dropped_oldest=0 unsent=0`, seq 0-59 gapless on one partition at contiguous
offsets, confirmed independently with `mss_events_tail`. The 6 failed
attempts are what the old at-most-once path would have lost. The drill is
the env-gated integration test `crates/mediaserverd/tests/kafka_outage.rs`
(`MSS_TEST_KAFKA_BROKERS`).
**Verified against fakes only** (unit tests, `PumpTuning` shortens the
backoff): drop-oldest past the cap keeping the newest survivors in order, a
transient refusal retried without a duplicate landing, a send that never
answers, and the shutdown give-up path. **Not exercised:** a live tapped
call during the outage — the drill drives the pump directly rather than
through rtpengine, so it proves the pump and the transport, not the whole
call path. Also still open by design: a pod that dies holding a backlog
loses it (memory only, no disk spool).

### 14. Mid-call SSRC re-resolution (defect D1) — ✅ DONE (2026-08-22)
**Where:** `crates/mediaserverd/src/tap_spike.rs` + `tap_plane.rs`.
**What shipped:** a leg now *re-enters* resolution when the sender's SSRC
changes, and a control-world task answers with a fresh map.
- `TapLeg` keeps `observed_ssrc` and re-resolves whenever the pipeline
  reports a different one. A new SSRC that the map does not name puts the
  leg back into the unresolved state (`unknown_ssrc = Some(new)`) while it
  **keeps the name it had** — a wrong-but-stable label beats a flapping one,
  and elimination still covers the two-leg case.
- A change needs **3 consecutive packets** of the new SSRC to be adopted
  (`SSRC_CHANGE_CONFIRMATIONS`), so two SSRCs interleaved on one leg (the
  dual-SSRC fault from the echo-loop sessions) cannot rename it per packet.
  The first SSRC on a leg still resolves on its first packet.
- The seam is the hub's: a per-leg bounded `ArrayQueue<SsrcTracks>`
  (capacity 4, drop-oldest so the newest map wins) polled once per capture
  tick. `SsrcTracks` is a fixed `[Option<(u32, Track)>; 8]` — `Copy`, so
  applying a map allocates nothing on the media thread. **No NG round trip
  moved into the media world.**
- The control-world half is `reresolve_speakers` in `tap_plane.rs`: every
  500 ms it reads each leg's unresolved SSRC out of `SharedLegStats`, and
  the first time it sees one it has not asked about it re-runs
  `query` + `speaker_ssrcs` and publishes the result to every leg.
  `SsrcRequeries` bounds that to **one query per distinct unknown SSRC**
  (memory of 16, oldest forgotten), so a permanently unknown SSRC cannot
  hammer rtpengine. The task is spawned only when the initial map is
  non-empty and is aborted with the session.
- `settle_by_elimination` needed no change but now matters more: it never
  latches (an eliminated leg stays `resolved_track() == None`), so when
  a re-resolution flips one leg the other is re-derived on the next tick.
  Test: `a_leg_that_starts_carrying_the_other_speaker_flips_both_names`.
- New series: `mss_legs_ssrc_changes_total`, `mss_ssrc_requeries_total`,
  `mss_legs_ssrc_reresolved_total`, and `mss_legs_unknown_ssrc` now
  **recovers to 0** when a re-resolution lands. Alert
  `MssLegSpeakerUnresolved` fires only when it does *not* recover (2m).
**Verified by replay/unit tests only** (`tap_spike::reresolution_tests`,
real loopback sockets through the real `capture` loop): a leg whose sender
switches SSRC mid-call goes unresolved, keeps its name, and is renamed
correctly when the refreshed map arrives — customer/agent naming intact on
both legs, `ssrc_changes=1`, `reresolutions=1`, and the sequence jump shows
up as exactly one jitter `Reset` (a re-INVITE's discontinuity, as expected)
with audio flowing again after it. Also covered: interleaved SSRCs rename
nothing, the newest map wins over queued stale ones, and the shared-stats
seam the control task reads.
**Not verified on a live call.** Two things a lab run still owes:
1. that rtpengine's `query` reports the **new** SSRC for a tag after a
   mid-call change (measured before only for a transcode-restamped leg at
   subscribe time — implementation-notes tap_plane point 4);
2. the end-to-end recovery timing on a real re-INVITE.
**Still open by design:** a transfer that replaces a *tag* (new agent, new
from-tag) is not resolved — `from_tags` is fixed at subscribe time, so the
new tag's SSRC is not in the map and the leg is named by elimination. Fixing
that means re-subscribing, not re-resolving; it is the natural follow-up if
transfer-heavy tenants need it.

### 15. Phase 2 — recording to S3 (milestone M5) — ✅ CODE DONE (2026-08-22)
**What shipped**, in the four slices the plan named:
1. **`FILE_S3` attachments** (`tap_plane.rs`). The endpoint *is* the frozen
   identity `${accountID}/${recordingID}.${format}`; `RecordingIdentity::parse`
   accepts that and refuses everything else by name (no separator, a nested
   prefix, an empty account or id, `.`/`..`, no extension, whitespace, any
   format but `wav`), and `object_key()` round-trips it byte for byte.
   `RecordingStarted` is raised through `SessionRegistry::observe` — which had
   no caller at all before this item; the seam is the new
   `control_api::ObservationSink`, held by `TapPlane` as a `Weak`.
2. **The segmenter** (`recorder.rs`, sans-IO). Three mono buffers placed by
   `timestamp_ms`, interleaved at the end as Customer **left** / Agent
   **right** (`write_wav`'s convention), with injected `Track::Mixed` audio
   saturating-summed into the right channel so a voice-AI call's bot side is
   not lost. **pause = segment + defer + accumulate**: pause cuts the segment
   and drops (counts) frames while paused, resume re-anchors so the next
   segment is written directly after the last, nothing is uploaded until stop,
   and `duration_ms` is recorded audio rather than wall clock.
   `UpdateAttachment{paused}` now reaches the media world at all — a new
   `MediaPlane::update_attachment` (default `Ok(())`), with a refused update
   rolled back in the registry.
3. **Upload** via `object_store`'s `AmazonS3` in the control world, never on
   the capture thread (WAV build and spill both on `spawn_blocking`), with
   `Content-Type: audio/wav`, bounded retries and timeouts, then
   `RecordingStopped{duration_ms}` before the upload and `UploadCompleted{uri}`
   only when the object landed. A failed upload spills to
   `MSS_RECORDING_SPILL_DIR` rather than vanishing. Ten new metrics series and
   three alert rules.
4. **Dual recording** stays the legacy controller's per-tenant flag: nothing here forbids
   `record_session` running alongside, since MSS records from its own tap and
   writes its own object.
**A fifth thing the contract needed:** `RecordingPaused{recording_id, paused,
duration_ms}` — a new `Observation`/`EventKind`/proto field 24. The frozen
callback set includes `recordPause` and MSS had no way to express it. The
resume edge reuses the variant with `paused: false`; that half is our
extension, and it is documented in `proto/mediacontrol.proto`.
**Also reordered, deliberately:** `DestroySession` now closes the media plane
*before* the registry forgets the session. `observe` refuses an unknown
session, so without that flip a recording ended by a hangup could never
publish its stop/upload callbacks. Test asserts the order AttachmentUp,
RecordingStopped, UploadCompleted, AttachmentDown, SessionEnded.
**Verified live (real infrastructure, no SIP):** `MSS_TEST_S3_ENDPOINT`-gated
`crates/mediaserverd/tests/minio_upload.rs` against a real MinIO container —
1500 ms published with a 500 ms paused interval produced a 1000 ms, 32044-byte
stereo WAV at `acct-drill/rec-<id>.wav`, `Content-Type: audio/wav`,
`s3://bucket/<key>` on `UploadCompleted`, the pause marker absent from the
audio, and the four callbacks in order — read back out of the bucket by the
test and confirmed independently with `mc`. Identical input gave an identical
ETag across runs. Procedure and output in [lab.md](lab.md).
**Verified synthetic only (unit/replay):** everything else — the identity
table, the interleave, zero-fill alignment, the pause duration math, the
saturating bot mix, the mono selector, the length cap, the WAV container, the
spill path, the `DestroySession` ordering and the pause rollback.
**Verified on a live tapped SIP call (2026-08-22, item 10's drill):** the same
call the gRPC probe listened to was also recorded — `RECORD=1
./lab/grpc_stream_drill.sh` — and produced `acct-grpc/rec-<id>.wav` in MinIO,
2 channels at 8 kHz, 319,040 frames = **39.88 s exactly equal to the reported
`duration_ms` of 39880**, customer left (rms 610) / agent right (rms 7),
`Content-Type: audio/wav`, 1,276,204 bytes matching
`mss_recording_bytes_uploaded_total`. The callbacks were read off the real
`mss.events` topic with `mss_events_tail`: `RecordingStarted`,
`RecordingPaused{paused:true, duration_ms:8140}`,
`RecordingPaused{paused:false, duration_ms:8140}` — the same duration on both
edges, so nothing accumulated while paused — `RecordingStopped{39880}` and
`UploadCompleted{s3://lab-recordings/acct-grpc/rec-<id>.wav}`, ten events
accepted and ten published with zero failures.
**FS byte-parity: measured 2026-08-23 (item 31).** `lab/fs_parity_drill.sh`
recorded one live call both ways and `lab/recording_parity.py` finally saw a
real FreeSWITCH recording: container, channel layout and rms agree exactly, and
a re-aligned 2 s window agrees on **1.0000** of samples at mean difference
**0.6/32768** — so there is no transform difference. Byte-for-byte parity at
one fixed offset is *not* achievable across two independent jitter buffers and
should not be the bar. What is still owed is a two-party call, the pause
contract compared, and the tenant's own codec — see item 31.
**One dependency decision to know about:** an S3 client needs TLS, and TLS in
Rust needs a crypto provider. `object_store`'s `aws` feature pulls aws-lc-rs
(cmake — absent from `rust:1.95-slim-bookworm`), so the features are
`aws-base, reqwest, ring` plus `reqwest/rustls-no-provider` and `rustls/ring`,
with the ring provider installed once at first use (without it, building a
client panics at runtime). `cargo deny check all` passes with **one added
allowance: CDLA-Permissive-2.0**, the licence of `webpki-root-certs` — a data
licence on the Mozilla CA bundle, not code. Reasoning is in `deny.toml`.
**Item 14's caveat still applies:** a mid-call SSRC change re-resolves, so a
re-INVITE no longer stales a recording's speaker labels, but a **transfer that
replaces a from-tag** needs a re-subscribe (out of scope here); a recording of
one is only as right as elimination makes it.

### 15b. (original description, for reference) Phase 2 — recording to S3
**Where:** new `crates/mediaserverd/src/recorder.rs`; `tap_plane.rs` for
the `FILE_S3` transport; `session-core` already has the event variants.
**What:** the roadmap's Phase 2, most parts already exist. The recorder is
just another hub consumer: subscription (all tracks) → stereo interleave
by `timestamp_ms` (Customer left, Agent right — `write_wav` in
`tap_spike.rs` shows the exact convention) → segment WAVs → upload.
**Direction, in landable slices:**
  1. `FILE_S3` attachments in `TapPlane::open_attachment`: endpoint is the
     frozen identity `${accountID}/${recordingID}.${format}` — parse it,
     refuse anything else loudly. Emit `RecordingStarted` via
     `SessionRegistry::observe` (the variant exists, unraised).
  2. Segmenter: buffer PCM per track, cut a segment on pause/stop;
     **pause = segment + defer + accumulate duration** (the frozen
     callback semantics — see roadmap Phase 2). `UpdateAttachment{paused}`
     is the pause signal; wire it through `MediaPlane` (pause reaches no
     media code today — that gap is part of this item).
  3. Upload: prefer the `object_store` crate (pure Rust, MIT/Apache —
     keeps the hermetic build; the lab needs MinIO or localstack in
     `lab/docker-compose`). Emit `RecordingStopped{duration_ms}` and
     `UploadCompleted{uri}`. Uploads run in the control world;
     never on the capture thread.
  4. Dual-recording is per tenant and lives in the legacy controller's flag, not here.
**Done when:** a lab call produces a stereo file in MinIO under the frozen
identity, callbacks appear on `mss.events` in order, a paused interval is
absent from audio but the duration math matches, and — the roadmap's exit
bar — a byte-comparison harness against FS `RECORD_STEREO` output exists
even if FS parity sign-off is a later human step. Item 14 (mid-call SSRC
re-resolution) has landed, so a re-INVITE no longer stales the speaker
labels of a recording; a tag-replacing transfer still can.

### 16. Opus — promoted to required (WebRTC legs are real)
**Why it moved:** WebRTC media on the customer side is Opus, and the only way
to tap it today is to have rtpengine transcode it. That works — the lab
rtpengine reports `opus: fully supported` — but it costs rtpengine an Opus
decode plus a G.711 encode per tap, which is far heavier than G.711
companding, and a transcoding subscription is exactly what keeps the tap out of
the kernel path (item 20). So Opus in MSS is a cost and kernel-path
requirement, not a capability gap: **WebRTC calls are tapped successfully
today.**

#### 16a. Opus decoder — ✅ DONE (2026-08-22), on libopus
New crate **`crates/opus-ffi`**: a safe wrapper over **libopus**, and the only
crate in the workspace that contains `unsafe` (every logic crate keeps
`#![forbid(unsafe_code)]`). `media-core/src/opus.rs` is a thin `AudioFormat`
adapter over it.
**The codec is libopus because Article XI says so** — vetted bindings to proven
C libraries, the same code FreeSWITCH wraps. libopus has shipped in every
browser, WhatsApp, Zoom, Signal, FreeSWITCH and Asterisk since 2012.
**A pure-Rust port (`opus-rs`) was tried first and rejected on evidence:** six
months old, **30 releases** in that window, **zero external dependents**, docs
coverage 1.49%, and a changelog entry for a table bug that made >160 kbps
stereo decode to garbage against libopus, fixed days earlier. It built with no
cmake, which was the whole attraction — but trading codec correctness on real
customer audio for a build convenience inverts the Constitution's own ordering.
**The binding is `opusic-sys` 0.7.5, not the more popular `opus` crate**, for a
measured reason: `opus` 0.3.1 pulls `audiopus_sys` 0.2.2 (unmaintained since
2021) which vendors libopus 1.3, whose CMakeLists declares
`cmake_minimum_required` below 3.5 — **CMake 4.x removed that compatibility, so
it does not build at all.** `opusic-sys` is current, builds in ~14 s under the
pinned toolchain, and its BSD-3-Clause was already allowlisted so `cargo deny`
needed **no new allowance**.
**Build cost, paid and verified:** `cmake`, `make`, `g++` at build time only —
libopus links statically, so nothing is needed at run time. One cached layer in
the Dockerfile builder (the shipped distroless image builds, the binary starts,
36.6 MB); the lab pods now build from `lab/Dockerfile.rust`; **CI unchanged**
because `ubuntu-latest` carries all three. The `make` requirement was found by
building the image, not assumed — cmake alone fails with
`CMAKE_MAKE_PROGRAM is not set`.
**What the wrapper adds over raw FFI:** mono and the five legal rates enforced
by name; `frame_samples` reads the packet TOC so an undersized buffer is a named
error *before* the decode; libopus error codes rendered through
`opus_strerror`; and **`conceal()` passes a NULL packet so Opus's own PLC
handles Opus loss** rather than the G.711 Appendix I concealer. An empty packet
is a distinct error from concealment, so the two cannot be confused at a call
site.
**Verified:** tone round-trip at all five rates with exact frame counts,
silence quiet, libopus concealing a lost frame, malformed packets erroring
rather than crashing, a decoder moving between threads, and — the bar for a
speech codec — real lab speech round-tripped through libopus at 8 kHz
transcribed **verbatim by Deepgram** at 9.8 kbps
(`examples/opus_speech_probe.rs` + `ear_intelligibility_probe.py`).

#### 16b-1. Negotiate Opus on the tap leg — ✅ DONE (2026-08-22)
The named blocker is gone: **MSS can now ask rtpengine for Opus and answer an
Opus offer.** `NegotiatedCodec {payload_type, encoding, clock_rate_hz}` in
`rtpengine-ng/sdp.rs` is what an answer is built from; `OfferedStream::negotiate`
resolves static payload types through `media-core`'s table and **dynamic** ones
by their `a=rtpmap` encoding name, which is the only way Opus can be recognised.
`SubscriptionAnswer.answer_with` is **required** rather than optional — an
earlier draft defaulted it to "negotiate from the offer" and silently broke the
transcode path, so the caller now states which mode it is in
(`from_static_format(configured)` for transcode-on, `negotiate()` for off).
**Three RFC 7587 rules obeyed, read from the RFC rather than assumed** (§4's
history is a list of answers rtpengine rejected): rtpmap clock **must** be 48000
and channels **must be 2 even for mono** (mono is in-band); the RTP timestamp
clock is 48000 Hz for every Opus mode and sample rate, so
`samples_per_packet` derives from the **clock rate**, not the rate we decode to;
and the payload type is dynamic, so the answer **echoes the offer's**. A wrong
opus clock rate is refused (`OpusClockRate`), and an unknown dynamic codec
(EVS, G.722) is skipped rather than guessed.
Also fixed here: `negotiated_tap_codec` requires every offered stream to agree
on one codec, mirroring the format rule from item 20.
**Verified by unit tests only** (46 in `rtpengine-ng`): negotiation at the
dynamic PT, the exact `a=rtpmap:111 opus/48000/2` line, g711 keeping its
two-field rtpmap, the clock-rate refusal, static-wins-when-offered-first, and
an explicit answer codec overriding the offer. Proven on a live call in 16b-2.

#### 16b-2. Make Opus actually flow — ✅ DONE (2026-08-23)
**Opus now flows end to end against real rtpengine-generated Opus.** All three
named blockers are gone, and one more was found by running it.

1. **`StreamPipeline` decodes Opus.** The decoder is now an enum
   (`Decoder::{G711, Opus}`) chosen by `PipelineConfig`, which is the new way to
   build a pipeline when the wire codec is not the decode format.
   `StreamPipeline::new` remains for the static-mapped G.711 case and simply
   delegates. The two conflated numbers are now separate: `timestamp_increment`
   comes from `PipelineConfig.clock_rate_hz` (48000 for Opus, per RFC 7587)
   while `samples_per_packet` comes from the decode format — so a 16 kHz Opus
   tap advances the jitter buffer by **960** per packet and emits **320**
   samples. Loss is concealed by **libopus itself** for Opus and by the G.711
   concealer for G.711; they are not interchangeable.
2. **`jitter::MAX_PAYLOAD` is 1276** (RFC 6716's maximum Opus packet), and the
   conflation flagged in 16c is split: `MAX_PAYLOAD` sizes the wire slot,
   the new `MAX_FRAME_SAMPLES` (960) sizes the PCM scratch.
3. **`ConsumerEncoder` resamples before the G.711 encode**, so a 16 kHz Opus
   tap feeds the frozen PCMU-8k WebSocket bridge unchanged. The refusal is now
   "g711 is defined at 8 kHz only" — the codec's own constraint — rather than
   "only at the tap rate". Pinned by
   `a_wideband_tap_still_feeds_the_frozen_pcmu_bridge`.
4. **Found by running it, not by reading it: a long Opus frame was being
   silently truncated.** A first cut normalised every decoded frame to
   `samples_per_packet`, which is correct for 20 ms senders and throws away
   two thirds of the audio from a **60 ms** sender — a legal, and for WebRTC on
   a bad network common, choice. The pipeline now carries the surplus in a
   fixed `Carry` (no allocation) and releases it over the following frames.
   Pinned by `a_sixty_millisecond_opus_sender_keeps_every_sample_instead_of_being_truncated`,
   which asserts the tone survives in **every** released frame, and by the new
   `frame_size_mismatch` / `carry_overflow_samples` counters, which are logged
   per leg so truncation can never again be silent.

**Configuration.** `MSS_TAP_FORMAT` (`pcmu`|`pcma`|`opus`) picks what the tap
decodes; `MSS_OPUS_DECODE_RATE_HZ` picks the Opus decode rate, default
**16000** — libopus resamples internally for free, so decoding straight to the
rate consumers want is cheaper than decoding at 48 kHz and resampling after.
An illegal rate is refused by name rather than rounded.

**Also fixed here: the transcode path could not name a dynamic codec.** It
answered with `from_static_format(configured)`, which has no answer for Opus.
`OfferedStream::negotiate_encoding(encoding)` now finds the payload type for the
codec we actually asked rtpengine for, falling back to the static mapping when
the offer omits it. This also fixes a latent G.711 bug: asking rtpengine to
transcode to PCMU while the offer led with PCMA used to answer with the wrong
codec (pinned by
`a_transcoded_tap_picks_the_codec_it_asked_for_not_the_first_one_offered`).

**Verified in the lab, not only in unit tests.** rtpengine 14.1.1.8 (which
links libopus and libavcodec directly) was asked to `transcode: [opus]` on a
live call; it offered **payload type 96**, MSS answered with it at clock 48000,
and decoded at 16 kHz:
`unknown_payload_type=0 unparsable=0 undecodable_frames=0
frame_size_mismatch=0 carry_overflow_samples=0 jitter_lost=0
jitter_silence_gaps=0` on both legs, with DTMF still detected. The decisive
check is the audio, not the counters: the captured WAV's dominant frequency is
**440 Hz** — the tone pumped in — at RMS 8504 against the 8485 a 12000-amplitude
sine should produce. The raw datagram log confirms the wire: RTP timestamp
delta **960** on every packet, no sequence gaps, TOC `0x08` (SILK NB, 20 ms,
one frame per packet). Recipe in [lab.md](lab.md).

**That open question is now closed — see item 23 (2026-08-23).** A native Opus
call (`lab/opus_call_driver.py`, libopus in the endpoints, rtpengine transcoding
nothing) taps at **50.0 packets/s**, against **3.8/s** for the transcoded tap on
the same rtpengine the same afternoon and **51.1/s** for the same call as PCMU.
The under-production is rtpengine's G.711→Opus transcoder; the relay path and
MSS are innocent, and the production shape — a call already carrying Opus,
tapped with `MSS_TAP_TRANSCODE=off` — runs at full rate. The original write-up
of the question follows.

**One open question, and it is rtpengine's, not ours.** When rtpengine
transcodes G.711→Opus it emits only ~10 packets/second where the same call
tapped as PCMU gives ~51 — a ~15% duty cycle, so the tap is mostly silence.
Ruled out by measurement: not CPU (rtpengine sat at 1–2%), not DTX or VAD (a
continuous 440 Hz tone behaves the same as the byte-ramp fixture), not loss
(zero jitter anomalies, contiguous sequence numbers), and not MSS (every packet
that arrived decoded, and the identical code path on the identical call gives
1018/1018 datagrams for PCMU). Distinguishing an rtpengine transcoder pacing
bug from a lab artefact needs a **native** Opus source rather than a transcoded
one. **Not blocking:** production taps a WebRTC call that is *already* Opus with
transcoding off, which asks rtpengine to transcode nothing at all — the case
this item's vehicle deliberately avoided in order to test without a WebRTC
endpoint.

#### ~~16c. Buffer sizing for Opus payloads~~ — done in 16b-2
`jitter::MAX_PAYLOAD` is 1276 and the wire-slot/PCM-scratch conflation is
split. `hub::MAX_FRAME_SAMPLES` needed no change: the default decode rate is
16 kHz (320 samples). Decoding at 48 kHz works in the pipeline and is exercised
in the lab, but 960 samples exceeds the hub frame, so a 48 kHz tap cannot fan
out to consumers yet — that is the remaining piece if anyone ever wants it.

#### 16d. Opus output to consumers — ⬜ still genuinely later
The original item 9 framing. libopus — already vendored and bound through
`opus-ffi` for ingest (16a) — ships an encoder too, so this is now mostly
plumbing, but no consumer has asked and ingest is what WebRTC needs.

### ~~17. Jitter hardening (defect D7)~~ — done 2026-08-22
**Where:** `crates/media-core` (`jitter.rs`, `pipeline.rs`, new `plc.rs`,
`replay.rs`), plus the daemon plumbing that feeds arrival times in and
reports the new counter out.
**What landed:** adaptive target depth from the RFC 3550 interarrival
estimate (rises at once, shrinks after 250 calm packets, ceiling 4× the
configured floor); timestamp-aware gap classification, so a gap the
timestamps say was sender silence is filled as silence and counted in
`silence_gaps` instead of `lost`; comfort noise (PT 13) accounted like a
telephone event rather than dropped as an unknown payload type; G.711
Appendix I-shaped PLC (AMDF pitch search, period repeat, 10 ms flat then
fade to silence by 60 ms, cross-fade back in) replacing the silence fill on
`PopOutcome::Lost`; `restart()` on a mid-call SSRC change, which fixes the
`TooLate`-run the item-14 handoff described when the new sender starts at a
nearby sequence number.
**Measured:** the [testing.md](testing.md) impairment matrix is now a set of
deterministic replay tests (`impairment_matrix` in `pipeline.rs`) — uniform
loss 1%/5%, an 8-packet burst, reorder inside and beyond the depth,
duplication, modelled arrival jitter, silence suppression, DTMF under loss.
`cargo bench -p media-core` on the dev box: full path 240.9 → 269.8 ns/packet
(+10%), ingest-only 16.3 → 25.6 ns (+52%); recorded with the machine in
[implementation-notes.md](implementation-notes.md).
**Left for later:** no perceptual check of the concealment yet (the
ASR-as-judge probe under `tc netem` burst loss is the cheap one), the
adaptive ceiling is a multiple of the floor rather than a millisecond
budget, and the chosen depth is logged per leg but not exported as a gauge.

### 20. Optional transcoding at the tap — ✅ DONE (2026-08-22)
**Why this exists:** rtpengine's kernel module carries no codec — it forwards
and can do SRTP, nothing more — so **asking for a transcode is very likely what
keeps a subscription in rtpengine's userspace**, which is the cost item 18 is
about. Confirmed from the vendor's own source
(`kernel-module/nft_rtpengine.h`): a kernel forwarding target has
`num_destinations` with `RTPE_MAX_FORWARD_DESTINATIONS 32` and a `do_intercept`
flag, so **in-kernel fan-out to an extra destination is a first-class feature of
the module**. That reorders item 18's options: getting the tap onto the kernel
path may be one flag on our side, and eBPF drops to a last resort.
**What shipped:** `MSS_TAP_TRANSCODE` (default `on`, so an existing deployment
is bit-for-bit unchanged).
- `media-core`: `Encoding::from_static_payload_type` / `static_clock_rate_hz`,
  round-trip tested. Only PT 0/8 map, because those are exactly what the
  pipeline decodes.
- `rtpengine-ng`: `OfferedStream::offered_format` picks the first offered
  payload type we can decode, in **offer order**, with the rtpmap's clock rate
  and the stream's ptime. `to_sdp` needed no change — handing it the offered
  format is the fix, because it already lists `format` first and echoes the
  rest. New error `NoStaticCodecOffered(offered)`.
- `mediaserverd`: `TapPlaneConfig.transcode_at_tap`; `offered_tap_format`
  requires every stream to agree; `LiveSession` records the format the tap
  settled on and `session_format()` feeds the gRPC attachment check and the
  encode pump, because `config.format` stopped being the truth.
**Verified live (2026-08-22), full SIP call, `MSS_TAP_TRANSCODE=off`:**
rtpengine offered **`[8, 101]` on both streams** — PCMA only, nothing
transcoded, versus `[0, 101]` with the flag on — MSS settled on
`Pcma/8000/20ms` from the offer, and both legs ran clean: Customer 2166 and
Agent 2203 datagrams, **every datagram played**, `companded=0`,
`unknown_payload_type=0`, `jitter_lost=0`, `frames_concealed=0`,
`recv_errors=0`. Customer track **rms 613.5** against 614.5 on the transcoding
baseline, and Deepgram transcribed it **verbatim** ("If you can hear this,
injection works.", confidence 0.99) through
`ear_intelligibility_probe.py`. `companded=0` is the number that matters: MSS
decoded A-law **natively** rather than converting it, so the system went from
three codec operations per stream (rtpengine decode + re-encode, then our
decode) to one.
`lab/docker-compose.microsip.yml` carries the knob as
`MSS_TAP_TRANSCODE: ${MSS_TAP_TRANSCODE:-on}` on every pod, so the run is
`MSS_TAP_TRANSCODE=off docker compose up -d --force-recreate mss-control`
then `./lab/grpc_stream_drill.sh`. **The lab was restored to `on` afterwards.**
**The exposure this creates, and it is real:** with the flag off, MSS sees the
carrier's codec. G.711 either way is free. **Opus, G.722 and EVS would land in
`unknown_payload_type` and be dropped** — which is why an undecodable offer is
refused loudly instead. Before turning this on for a tenant, ask the platform
team what codecs actually appear on customer and agent legs. The adaptive
version — `query` the call's codec first and ask for transcoding only when we
cannot decode it — is the natural follow-up and needs no extra round trip,
since `complete_from_tags` already queries before subscribing; it was **not**
built because it rests on `query` reporting per-media codecs, which is
unprobed.
**Bonus worth knowing:** without transcoding, rtpengine does not re-stamp the
leg with a generated SSRC, so leg identity gets *cleaner* — `unknown_ssrc`
stayed 0 in the drill without needing elimination to cover a restamped leg.

### 18. RESEARCH — eBPF tap ingest (decide, don't build)
**The question:** can we mirror RTP to MSS with an eBPF program on the
rtpengine host instead of NG `subscribe`?

First, terms: eBPF is not a kernel module — it is verified programs
attached to kernel hooks (TC `clsact` here); rtpengine separately has its
own kernel module (`xt_RTPENGINE`) that forwards media in-kernel. The
worry that makes eBPF attractive: a `subscribe` may pull the subscribed
legs out of kernel forwarding into rtpengine's userspace, making the
per-tap cost on the rtpengine host non-trivial — **and that cost is
exactly the still-open measurement** (Waiting-on-people item 2).

The shape, if it ever wins: a TC egress program on the rtpengine host
with a BPF map of tracked flows (5-tuple → MSS address), maintained by a
small privileged agent; `bpf_clone_redirect` duplicates matching packets,
the program rewrites IP/UDP headers (and checksums) toward the MSS pod;
MSS ingest is **unchanged** (same UDP socket, jitter buffer, SSRC
naming). NG stays for control (`query` still supplies tags/SSRCs) — only
the media-copy mechanism changes. What it buys: near-zero per-tap cost,
no dependency on the production rtpengine's `subscribe` support (removes
Phase-0 blocker #1), taps that cannot perturb the call. What it costs: a
privileged agent on every rtpengine host, a kernel/CO-RE support matrix,
flow tracking through re-INVITEs, no tap-leg transcode (fine for G.711 —
MSS compands both variants — but Opus/EVS calls would need decoding in
MSS), and a second copy path invisible to rtpengine's own accounting.

**Reordered 2026-08-22 by reading the vendor source.** The kernel module
already fans one forwarding target out to many destinations
(`num_destinations`, `RTPE_MAX_FORWARD_DESTINATIONS 32`) and carries a
`do_intercept` flag, so **in-kernel mirroring is a feature of the module, not
something eBPF must supply.** eBPF cannot "use" that module either — it would
be a parallel datapath that bypasses rtpengine and re-derives its session
state. So the ladder is now: (A2) subscribe **without** transcoding — item 20
shipped the MSS half, verified live; (B) an explicit kernel intercept if NG
exposes one; (C) eBPF, last resort.

**Decision gate, in order — do not write eBPF before all three:**
  1. Get the rtpengine-side per-tap cost measured (the open org item).
     If `subscribe` at the target tap count costs little, stop here;
     eBPF is unjustified complexity. D14 is fixed (item 25), so orphaned
     taps no longer pollute the measurement.
  2. Probe whether a subscription is kernel-forwarded, and **whether
     dropping the transcode request is what decides it** — that is the
     hypothesis item 20 exists to make testable, since the kernel module
     has no codec and therefore cannot transcode. **The instrument for this
     now exists (item 23, 2026-08-23):** `lab/kernel_probe.sh <ng-host>
     <ng-port>` gives the verdict from rtpengine's own `statistics` over NG
     alone, runs unchanged on a metal box, and mediaserverd logs the same
     judgement per node at startup. The full read-only checklist is in
     architecture §8.1. On a host that can load the module also
     `cat /proc/rtpengine/<table>/list` at baseline, after a subscribe
     **with** transcode, and after one **without**, comparing
     `num_destinations` on the target entries. Run rtpengine with
     `--no-fallback` so it refuses to start rather than silently
     degrading to userspace. **This cannot be probed in this lab** —
     `--table=-1`, no `/proc/rtpengine`, and no kernel headers to build
     the module against, and `kernel_probe.sh` was machine-verified here on
     exactly that "no module" answer. On a production-shaped host that
     already runs the module it needs **no config change and is read-only**,
     so bundle it with open items 1 and 2 above in one visit.
     rtpengine's kernel module also has a packet-mirroring path used by
     `rtpengine-recording` — probe whether that reaches an arbitrary UDP
     destination; if yes, that is the same win with vendor support and no
     eBPF.
  3. Only then: a one-day TC `bpf_clone_redirect` PoC against the lab
     rtpengine container with a hardcoded flow map, measuring per-packet
     overhead and packet integrity at the MSS socket.
**Done when:** a decision record lands in
[architecture.md](architecture.md) (build / vendor-mirror / stay-on-NG),
with the three probes' numbers.

### 19. Soak + impairment suite — the "full testing" bar — ✅ DONE (2026-08-22)
**What shipped:** `lab/soak.py`, plus the three things the lab needed before a
soak was possible and the netem tool for the box that can run it.
- **`lab/soak.py`** runs N concurrent synthetic calls back to back for a
  configurable duration through a walk of impairment phases
  (`SOAK_PHASES="profile:seconds,…"`), one session per slot round-robin over the
  three pods, one `gap_consumer.py` keyed per call, scraping every pod's
  `/metrics` each minute. It asserts, as **deltas from a pre-run baseline**:
  `mss_legs_stalled` 0; `dropped_oldest` ≤ `MAX_DROPPED_OLDEST` (default 0);
  the event pump loses nothing (`failed`/`abandoned`/`dropped`/`dropped_oldest`
  flat); `recv_errors` and `unparsable` flat; `registry_lost`/`_failed` flat;
  the consumer still fed whenever a session is live; and at the end nothing
  leaked (`sessions_live`/`legs_live`/`consumers_live` 0, Redis `mss:sessions`
  empty) with daemon RSS flat — read from `/proc` **inside** the pod matched on
  `comm == mediaserverd`, since `docker stats` folds in page cache and the
  `cargo run` wrapper. Every violation prints by name and the exit code is
  non-zero, so a cron or CI can own it.
- **`host_test_caller.py` is now N callers**, everything unique per caller behind
  an env var with the old single-caller defaults intact, so items 10/11's drills
  are untouched. Naming its own `CALL_ID` removes discovery entirely: with
  OpenSIPS in the path rtpengine's call-id *is* the SIP Call-ID, and
  `mss_ctl create` resolves the rest of the tags from `query`. It also grew
  `IMPAIR_LOSS`/`_REORDER`/`_DUPLICATE`/`_JITTER_MS`.
- **`gap_consumer.py` grew a soak mode** (`GAP_BY_CALL`, `GAP_KEEP_AUDIO`,
  `GAP_JOURNAL`), all defaulting to item-11 behaviour, so one consumer can serve
  N calls for an hour in bounded memory and still report per-call continuity.
- **`lab/netem.sh`** (`probe|apply|clear|show`) implements the impairment matrix
  on the tap link: a `prio` qdisc in rtpengine's namespace whose priomap sends
  everything to band 1:1, with u32 filters steering only MSS-bound packets into
  the netem band, so the call legs stay clean.
- **rtpengine's port range went 30000-30020 → 30000-30099.** A tapped two-party
  call costs four port pairs, so 10 pairs held exactly **two** tapped calls; the
  soak needs more.
**The green run (recorded in [testing.md](testing.md) and [lab.md](lab.md)):**
`soak-1787401045`, **46 min 57 s, 3 concurrent calls of 120 s, 69 sessions
created/tapped/destroyed (23 per pod), 0 violations.** 768,344 tap datagrams,
1,164,153 frames delivered. `mss_legs_stalled` 0 at all 43 scrapes and
`ingest_stalls` never even transitioned; `dropped_oldest` 0; events 69/69 with
0 failed, 0 retried; `late_drops`/`duplicates`/`silence_gaps`/`resets`/
`unparsable`/`recv_errors` all 0; registry empty at the end. **RSS 24,272 →
25,472 kB / 26,424 → 27,160 / 26,316 → 27,116** — under +1.2 MB per pod across
69 full session lifecycles. Consumer continuity over 207 tracks: worst arrival
gap **67 ms**, median 31 ms, p90 45 ms, none over 100 ms.
**The impairment half, and its honest limit.** `tc netem` is **impossible on this
box**: `CONFIG_NET_SCH_NETEM` is unset in the WSL2 kernel that Docker Desktop
also runs containers on, with no `sch_*` module to load and no passwordless
`sudo`; the fix is the custom-kernel detour testing.md prices at half a day and
it needs a Windows-side change. So the soak injects at the matrix's **other**
point, the endpoint, and `SOAK_NETEM=auto|on|off` means the same script produces
the stronger measurement unchanged on a netem-capable box. What that bought:
1% injected read **1.06%**, 5% read **5.01%**, and `frames_concealed` equalled
`jitter_lost` **to the packet** in every phase — so item 17's G.711 Appendix I
PLC has now run on a **real link**, not only in replay, with `silence_gaps` 0
(the buffer called it loss, not sender silence). Reorder and arrival jitter
(±35 ms) cost **zero** loss and **zero** late drops. And a measured negative:
**duplication never reaches the tap** — rtpengine absorbs it upstream of the
subscription, so `mss_jitter_duplicates_total` stayed 0 through a phase
duplicating 1% of the caller's packets. That row, plus burst loss and
reorder-beyond-depth, still needs netem on the tap link.
**The Article-VIII re-run** (items 14/17 changed the pipeline):
`parse_jitter_decode_per_packet` **262.1 / 281.3 ns** and
`ingest_only_per_packet` **26.1 / 26.2 ns** on a quiet box, bracketing item 17's
269.8 / 25.6 — **no regression**, and `git log -- crates/media-core` confirms no
commit has touched the crate since. The same benchmark read 308.5 / 28.1 ns with
the lab stack still resident, which is a **±9% repeatability band** for this
machine: stop the lab before any future comparison, and treat sub-10% deltas as
noise.
**Left open:** the pod-kill gap was **not** re-measured under load (item 11's
14.41 s is still an idle-box number) — killing pod A mid-soak makes its
`/metrics` unreachable and trips the soak's own assertions, so the two drills
need a combined harness rather than one run of each. Phases are also attributed
a little loosely: calls are 120 s and scrapes 60 s, so a phase's first settled
scrape still carries the tail of a call dialed under the previous profile (the
reorder row's 97 lost packets are loss5 bleed) — read the frozen interior, not
the boundary. And the concealment has still never been judged perceptually.

### 21. Recording groups — multi-party conference recording — ✅ DONE (2026-08-23)
**The problem it solves:** a FreeSWITCH conference is N SIP dialogs = N
rtpengine calls = **N MSS sessions** (a session is one call-id and at most
`MAX_TAPPED_LEGS` = 2 legs), and the recorder was stereo customer-left /
agent-right. There was no way to record N participants as one logical
recording.
**What shipped:** `AttachRequest.group` (proto field 11, **additive**) plumbed
through `control-api` → `session-core` → `tap_plane`. A `FILE_S3` attach with a
non-empty group joins a per-pod recording group keyed `(accountID, group)`, and
each member writes **its own mono object**:
`${accountID}/${recordingID}/${label}.${format}`, with `.customer`/`.agent`
suffixes when a member selects both tracks. The participant label is the attach
`label`, falling back to the session's `external_id`, and it is validated as
one path segment (no `/`, whitespace, control characters, `.`/`..`) because it
lands in an object key. **An empty group is byte-identical to before** — same
single object under the frozen `${accountID}/${recordingID}.${format}` — pinned
by `an_ungrouped_recording_still_writes_the_frozen_two_leg_identity`.
**Per-participant files are the design, not a shortcut:** interleaving N
sessions into one N-channel WAV would make one slow member's buffer the whole
conference's. Separate files keep the members independent, and since item 29
they are time-aligned anyway — every member anchors on the instant the group
opened, so a consumer can lay the objects side by side.
**Recommend `TRACK_CUSTOMER` per member** — a participant's own voice is leg
index 0 — so each file is one speaker.
**Three refusals, by name and counted** (`mss_recording_group_joins_refused_total`):
a second member reusing a participant label (it would overwrite the first
member's object), a member naming a **different** `recordingID` inside a group
that already has one, and a non-empty group on any transport but `FILE_S3`.
Pause stays per member. The group opens with its first member and dies with its
last (`mss_recording_groups_live`, `mss_recording_group_members_live`).
**The recorder grew targets:** `RecorderSpec.targets` is a list of
`RecordingTarget { key, layout }` and `Segmenter` renders per target instead of
filtering at `accept`, so a two-track member writes two mono files from **one**
buffered call — one `RecordingStopped`, one `UploadCompleted` per object, with
`path`/`uri` disambiguating the participant while `recording_id` stays the
group's. `mss_recording_uploads_total` and `_bytes_uploaded_total` therefore
count **objects** now.
**Verified against a real MinIO** (`crates/mediaserverd/tests/minio_upload.rs`,
`MSS_TEST_S3_ENDPOINT`-gated): three members in one group produced four objects
— `alice.wav`, `bob.wav`, `carol.customer.wav`, `carol.agent.wav` — each mono
8 kHz carrying **only its own participant's tone**, with the pause applied to
`bob` alone (25 frames in his file against alice's 50, and alice saw no pause
callback), and the frozen two-leg key **absent** from the bucket. Confirmed
independently with `mc`: 16 KiB / 7.9 KiB / 7.9 KiB / 7.9 KiB,
`Content-Type: audio/wav`.
**Verified on two live rtpengine calls** (the stretch, done — `lab/group_recording_drill.sh`,
recipe in [lab.md](lab.md)): two fabricated calls through the lab rtpengine
14.1.1.8, two sessions on one pod of its own, one group, `mss_sessions_live 2`
/ `mss_recordings_live 2` / `mss_recording_groups_live 1` /
`mss_recording_group_members_live 2`, both live refusals fired with their exact
messages, and detaching both members uploaded
`acct-conf/rec-<id>/alice.wav` (47.34 s, rms 9960) and `bob.wav` (47.40 s, rms
9971) — mono 8 kHz, each **exactly equal to the `duration_ms` reported**, group
and member gauges back to 0 afterwards.
**A lab instrument bug found and fixed on the way:** `lab/call_driver.py` could
not fabricate **two** calls at once. Both drivers started their NG cookies at
`lab-1`, so rtpengine's duplicate-cookie reply cache handed the second driver
the *first* call's answer — both "calls" claimed ports 30028/30042. It is the
D12 shape on the driver side. `COOKIE_PREFIX` (default `lab`, so every existing
drill is unchanged) fixes it; with distinct prefixes the two calls get distinct
ports.
**Left open at the time, filed as D16 — closed by item 54 (2026-08-27):** a
group lived in one pod's memory. The group was persisted on the attachment, but
`RegistryKeeper::rebuild` **refused** to restore a grouped recording on an
adopting pod (counted `grouped_not_adopted`) rather than split one recording
across two pods. Item 54 moved the group into the session store instead, so a
member joins from any pod and an adopter rejoins; `grouped_not_adopted` no
longer exists. Placement (D8) is still absent, but no longer a correctness
requirement.

### 22. (number unused)

There is no item 22. The number was skipped when items were being written in
parallel and is left unused rather than renumbered, so that every reference to
an item number elsewhere in this file, in commit messages and in
implementation-notes keeps pointing at the same work.

### 23. rtpengine kernel-module readiness — ✅ DONE (2026-08-23)
**Why this exists:** item 18's decision gate cannot be opened without knowing
whether a tap rides rtpengine's kernel path, and the production deployment runs
the kernel module. **No MSS media-path change was needed or made** — MSS speaks
NG over UDP and receives plain RTP, so a kernel-forwarded subscription and a
userspace one are identical at our socket. What decides it is **transcoding**,
which is why item 20's `MSS_TAP_TRANSCODE=off` is the kernel-eligible mode.

**What shipped.**
- **`rtpengine-ng/stats.rs`** — `NgClient::statistics` / `NgClient::version`,
  `RtpengineStatistics` and `KernelForwarding`, sans-IO and unit-tested (74
  tests in the crate now). The shape was **probed against the live node before
  it was typed**, and the probe corrected two assumptions: `uptime` is a bencode
  *string*, and the kernel/userspace split lives in two places
  (`totalstatistics.relayedpackets_kernel/_user` for lifetime,
  `currentstatistics.packetrate_kernel` + `media_kernel`/`media_mixed` for now).
  `module_in_play()` returns `Option<bool>` so "cannot tell" can never read as
  "no".
- **`mediaserverd/rtpengine_capability.rs`** — a first-contact-per-node
  capability log, called from both the startup NG probe and
  `TapPlane::open_session` (since **item 44** that startup probe *repeats* on
  `MSS_HEALTH_PROBE_INTERVAL_SECS` and a node is forgotten on any failure, so a
  restarted rtpengine is re-learned instead of keeping the dead node's verdict). It logs the version (or why it cannot be had), the
  relay split, live sessions, the active transcoder chains, and a plain-English
  kernel verdict; when transcoding is on it says at **WARN** that transcoded
  taps are processed in rtpengine userspace and the kernel module cannot help
  them. Both modes verified live.
- **`lab/kernel_probe.sh <host> <port>`** — the same judgement from a shell,
  over NG alone, so it runs unchanged on a metal box later. Exit 0 kernel in
  play, 1 userspace only, 2 cannot tell (with the reason), 3 unreachable. On
  the rtpengine host it adds the `/proc/rtpengine` and `lsmod` evidence NG
  cannot expose; anywhere else it says that half is skipped rather than
  guessing. **Machine-verified against the lab's "no module" path**: 150,462
  packets relayed, every one in userspace, 207/s live, exit 1.
- **`lab/opus_call_driver.py`** — a native Opus caller (libopus via ctypes,
  CBR 24 kbit/s, VBR and DTX off, a phase-continuous 440 Hz tone at 20 ms) that
  places an Opus↔Opus call through rtpengine so nothing is transcoded. It
  refuses to pump if rtpengine renumbers the payload type, so a run that
  reaches the tap really is native Opus.
- **Docs:** architecture §8.1 "Running MSS against a kernel-module rtpengine" —
  the eligibility checklist, the read-only on-metal probe list (including
  `--no-fallback` so a degraded start cannot masquerade as a negative result),
  and the WSL2 limitation. Lab recipes and numbers in [lab.md](lab.md).

**There is no NG `version` command — anywhere.** 14.1.1.8-jambonz11 answers
`Unrecognized command`, and neither does upstream's documented command list
contain one; brute-forcing 37 candidate names against the live node found only
`ping`, `list`, `statistics`, `transform` and the call-scoped verbs. **Decision:
ship the builder anyway as a probe** — one datagram at startup, a future build
that grows the command is picked up for free, and the universal outcome is a
first-class state (`VersionReport::NoVersionCommandOnThisNode`) rather than an
error. Every kernel judgement rests on `statistics`, which does exist and is
rich. The version itself has to come from the process, the package or
`--listen-cli`; that is now written into the Phase-0 blocker below.

**16b-2's open question is settled: it was rtpengine's transcoder.** Same
rtpengine, same afternoon, audio packets per second at the tap:

| tap | rtpengine's codec work | packets/s |
| --- | --- | --- |
| PCMU, from a G.711 call | pass-through | **51.1** |
| Opus, **rtpengine transcoded it** from the same G.711 call | G.711 → Opus | **3.8** |
| Opus, **native** from the endpoints, `MSS_TAP_TRANSCODE=off` | none | **50.0** |

Native Opus arrives at the sender's full rate (750 datagrams in 15 s against
750 sent, both legs), with `unknown_payload_type=0 unparsable=0
undecodable_frames=0 frame_size_mismatch=0 carry_overflow_samples=0
jitter_lost=0 recv_errors=0`, a **440.0 Hz** dominant tone at rms 8465.9 on both
channels, and a wire the datagram log confirms independently: pt 111, the
endpoints' **own** SSRCs (rtpengine did not re-stamp them), sequence deltas all
1, RTP timestamp deltas all 960, a constant 60-byte payload, and both the
one-frame (`0x48`) and two-frame (`0x4B`) SILK TOC shapes decoded without a
single `frame_size_mismatch`. The transcoded case measured **worse** than the
~10/s recorded in 16b-2 — 3.8/s, and on one run its Customer leg produced
**zero** Opus packets for 15 s. **This never affected the production shape**,
which is a WebRTC call already carrying Opus, tapped with transcoding off — the
50.0/s column. It does mean `transcode: [opus]` is only a smoke test, not a way
to manufacture Opus.

**What this does *not* answer, and why it cannot here.** Whether a subscription
on a kernel-module rtpengine stays in the kernel. This box runs `--table=-1`,
has no `/proc/rtpengine`, an empty `lsmod` and no
`/lib/modules/$(uname -r)/build`, so the module cannot even be built without a
custom-kernel detour. That half is now a read-only checklist for the platform
team (architecture §8.1) rather than an unknown, and `kernel_probe.sh` is the
instrument it hands them.

**Left open:** ~~the kernel verdict is log-only, not a metric~~ — **struck
2026-08-26 (item 57)**: every health probe's `statistics` sample is now exported
per node as `mss_rtpengine_tap_kernel_verdict{node,verdict}` and the relay-split
gauges beside it. The capability *log line* still does not repeat (a node that
answers every probe keeps its first-contact report, though its verdict is
re-decided on every sample and a failed probe forgets it), and
`controlstatistics.proxies` and the per-interface blocks are read by the shell
probe but not modelled in Rust.

### 24. WebRTC agent leg on a second rtpengine node — ✅ DONE (2026-08-23)
**Why this exists:** every drill before it anchored both legs of a call in one
rtpengine and fed MSS synthetic Opus. A real deployment anchors the two legs of
a call in **different** rtpengine nodes — one near the carrier interconnect, one
near the agents — and the agent side is increasingly a browser. Both halves of
that were untested.

**What shipped** (`lab/`, no crate changes — the daemon needed none):
- **`docker-compose.webrtc.yml`**, an overlay on the MicroSIP compose: a second
  rtpengine (RE2, `172.31.99.11`, ports 30100-30199), `opensips-agent` (ws 5062
  for the browser, UDP 5060 for FreeSWITCH), a page server for `lab/webrtc/`, a
  headless Chrome that dials by itself with a wav file for a microphone, and a
  watcher that reports RE2's view of the call. `AGENT_ADVERTISE_IP` switches
  between a browser on this box and one on the LAN.
- **`lab/webrtc/`** — `index.html` + `agent.js` (JsSIP 3.10.10, vendored;
  SDP munging per profile, guarded so it can never produce a codec the offer
  did not contain), `make_agent_audio.py` (the microphone fixture: a 440 Hz
  tone plus optional speech) and `wav_summary.py` (channels, duration, rms,
  peak, silence runs, Goertzel purity at 440/1000 Hz).
- **`lab/opensips/opensips-agent.cfg`** — a WebSocket-facing proxy that
  registers the browser and routes FreeSWITCH's INVITE back over the socket it
  registered on, with the RFC 7118 `.invalid` Contact problem solved by
  remembering `$si:$sp` at REGISTER and setting `$du`.
- **`lab/freeswitch/dialplan-default.xml`** — the image's own default context
  with the lab extensions **merged in** (4001 G.711 bridge, 4002 PCMU-only),
  because FreeSWITCH does not merge sibling `<context name="default">` blocks.
  `dialplan-lab.xml` is kept as the standalone original.
- **`lab/webrtc_agent_drill.sh`** — one profile per run
  (`control`/`opus`/`dtx`/`red`/`ptime60`/`cbr`/`stereo`/`dsp`),
  `AGENT=headless` by default so no human is needed, `AGENT=browser` when you
  want to hear it. **`lab/webrtc_record_live.sh`** is the manual companion: you
  place the call, it discovers both call-ids, taps both legs and records them.

**Verified run (stamp 1787492636, `PROFILE=control`).** Two MSS sessions on two
rtpengine nodes, both settled Pcma/8 kHz/`transcoding: false`; browser tone
Goertzel purity 0.707 against a human speaking on MicroSIP; **zero
`jitter_lost`, zero `recv_errors`, zero `unknown_ssrc`**; a stereo recording of
the customer leg plus a **cross-node recording group** (one object per
participant track, peaks 11520/16128 matching across the two nodes), seven
uploads and zero failures to MinIO. Numbers, artifacts and the five vendor
findings that each cost a run are in [lab.md](lab.md).

**Left open:** the Opus profiles have **not** been run — the lab FreeSWITCH
image has no `mod_opus`, so FS cannot bridge an Opus call, and the profiles need
an rtpengine codec-mask arrangement that keeps FS out of the codec decision
(future item; the surface they cover is described in
[testing.md](testing.md#the-webrtc-codec-surface)). So MSS has decoded browser
**G.711**, not browser **Opus**. Two defects came out of the run (D17, D18
below), and the browser leg **outlived the hung-up call by ~12 s** — billable
media tail after hangup, worth an eye in a pilot.

### 25. Orphaned tap after a pod death (defect D14) — ✅ DONE (2026-08-23)

**What the drill in item 11 priced:** a `kill -9` on the owning pod left its
rtpengine subscription in place for the rest of the call — 14,743 packets /
2.5 MB copied to an address nobody was listening on, four ports held. A pod
restart during a call permanently doubled that call's cost on the rtpengine
host.

**What shipped.** Three changes, one seam:

1. `PersistedSession.subscription_tag` — the `to-tag` rtpengine answered the
   `subscribe request` with, persisted every keeper tick. `#[serde(default)]`,
   so a record written before the field existed still decodes and is still
   adoptable (asserted both in unit tests and against a real Redis).
2. `TapSubscriptions`, a two-method trait in `registry_keeper.rs` that
   `TapPlane` implements (`subscription_tag`, `unsubscribe_orphan`). `rebuild`
   cancels the previous owner's tap **before** creating its own; the ordering
   is asserted through a shared journal and the NG `unsubscribe` bytes against
   a fake rtpengine socket. A refused unsubscribe does not block the adoption —
   it counts `orphans_still_subscribed`, as does a legacy record with no tag.
3. The **partitioned-but-alive owner**: `upsert` now writes the lease with
   `SET NX` instead of overwriting it, so a pod that was adopted away cannot
   take the lease back 5 s later (it could, before — which made the whole
   partition case undetectable), while an incumbent still re-acquires a lease
   that merely expired unclaimed. `renew` returning `false` therefore means
   exactly "another pod owns this", and the losing pod now **destroys the
   session locally** — its own `close_session` unsubscribes its own tap — and
   deliberately leaves the registry record for its successor. `surrendered`
   counts it.

**Decisions recorded:** the adopter unsubscribes only after winning
`claim_unleased`'s atomic `SET NX`, never speculatively; lease loss is fatal
for the session, accepting that a pod stalled past 15 s drops a call it could
still have served (the adopter has already re-tapped it, and the alternative
is D14 for the life of the call).

**Verified against what.** Unit tests (store roundtrip including a legacy
record, adoption ordering, refused unsubscribe, surrender leaves the record
alone), a fake rtpengine UDP socket for the wire bytes, and the whole
`session_store` suite against the lab's real Redis (18 tests,
`MSS_TEST_REDIS_URL=redis://127.0.0.1:16379`). **Not** re-measured on a live
pod kill: `lab/pod_kill_drill.sh` needs a human-dialed SIP call and a rebuilt
pod image, so no new rtpengine teardown block was collected. Re-running it is
the honest close-out for the rtpengine-side per-tap cost handoff, which was
blocked on this fix.

### 26. An adopted attachment kept the session default, not its format (defect D15) — ✅ DONE (2026-08-23)

`PersistedAttachment` carried no format, so `rebuild` passed
`format: None` on every re-`Attach` — and `None` means
`AudioFormat::pcmu_8k_20ms()` in `control_api::convert::format`. A consumer
that negotiated L16/16 kHz came back after an adoption as g711/8 kHz on the
same stream: not an error, a silently wrong sample rate feeding an ASR.

**What shipped.** `PersistedAttachment.format: Option<PersistedFormat>`
(`#[serde(default)]`), persisted from `AttachmentView.format` on every keeper
tick and replayed into the `AttachRequest` on adoption. `PersistedFormat` is
the **wire** shape — encoding as the `proto.Encoding` number, plus
`sample_rate_hz` / `channels` / `ptime_ms` — matching how `kind`, `transport`
and `capabilities` are already persisted, so no new mapping table exists to
drift from the proto. `None` keeps meaning "the default", which is exactly the
right reading of a record written before this field existed.

**Verified against what.** Unit tests: the JSON roundtrip, a legacy record
without the field decoding to `None`, and an adoption test where two
attachments on one session — a WS consumer on the default and a gRPC consumer
on L16/16 kHz/20 ms — are both re-opened on the adopting pod with the format
each one asked for (asserted on the `AttachmentView` the media plane receives,
against a fake plane). Plus the whole `redis_registry` suite against the lab's
real Redis (19 tests, `MSS_TEST_REDIS_URL=redis://127.0.0.1:16379`), whose
roundtrip asserts full attachment equality — that legacy-record test now
carries an attachment, since it had asserted its field on an empty list. **Not**
observed on a live call: it would take a live pod kill with a gRPC L16
consumer attached, which is the D14 close-out drill's shape.

### 27. Consumer-facing defect batch: D2, D3, D10, D13 — ✅ DONE (2026-08-23)

Four defects that all live on the seam between the hub and a consumer.

**D13 — an unannounced silent track.** The hub's `TrackSelection::All` meant
literally every track, so a consumer that asked for `All` was fed the `mixed`
track (the injected-playback track, silence whenever nothing is playing) at the
full frame rate, while its `StreamStart` / Twilio `start` frame advertised only
`["customer","agent"]`: an unannounced track and 50% extra bandwidth for
silence. The selection now has two shapes — `All` (every track, **including**
`mixed`) and `Speakers` (customer and agent only). Consumers map
`TrackSelector::All` to `Speakers`, so they receive exactly the tracks they were
told about; the **recorder keeps `All`**, because bot speech played into the
call belongs in the recording. Nothing about the frozen Twilio start frame or
`StreamStart.tracks` changed — the delivery was brought in line with the
advertisement, not the other way round.

**D10 — pause was a note in the registry.** A paused `WS_TWILIO` /
`GRPC_STREAM` attachment kept receiving media; only the recorder honoured
pause. A subscription now carries a pause flag the control world flips
(`SubscriptionControl::set_paused`) and the hub checks per frame: a paused
consumer is skipped and the skips are counted
(`mss_consumer_suppressed_while_paused_total`). Resume delivers from where the
tap now is — no backlog is replayed. A gRPC attachment paused before its
consumer subscribes stays paused when the stream opens. Recorder pause is
untouched (it still segments).

**D3 — detach aborted the consumer task.** `close_attachment` (and
`close_session`) called `JoinHandle::abort()`, so a websocket consumer's stream
just stopped mid-frame with no Twilio `stop` message, and a gRPC consumer got a
dropped channel. Both paths now go through `TapPlane::end_attachment`, which
ends the *subscription* (`SubscriptionControl::end_of_stream`) and lets the
consumer finish on its own: the WS task drains its queue and sends the `stop`
frame it always had code for, the gRPC pump drains and a new
`StreamFrame::Stop { reason }` becomes a `StreamStop` carrying why the stream
ended ("the attachment was detached" / "the call ended"). A consumer that will
not finish within `POLITE_CLOSE` (2 s) is still aborted, counted as a warning
in the log. This also means a normal hangup now ends every consumer stream
politely, not just an explicit `Detach`.

**D2 — `stop_playback` stopped every playback on the call.** It sent NG
`stop media` with `all: all` regardless of what the playback was aimed at. The
registry now remembers each playback's `target_tag` (`PlaybackRecord`), returns
it from `stop_playback` (`StoppedPlayback`), and the media plane passes it to
rtpengine as `from-tag` — or `all: all` when the playback was for everyone.

**Verified against what.** Unit/replay tests: the hub (a `Speakers` consumer
never sees `mixed` while an `All` consumer does; a paused consumer is fed
nothing and resumes at the current tap position with the suppressions counted;
an ended stream drains then finishes), `tap_plane` (detaching a WS consumer
lets its task run to completion instead of being aborted, detaching a gRPC
consumer ends its stream with a `Stop` frame carrying the reason, pausing
through `update_attachment` stops hub delivery), `stream.rs` over the wire (a
`Stop` frame reaches a real gRPC consumer as `StreamStop` with its reason), the
registry (a stopped playback reports the participant it was played to), and the
controller end-to-end against a recording fake (two playbacks, one aimed at
`from-b` and one at everyone, are stopped with `Some("from-b")` and `None`
respectively). D2's rtpengine half was measured on the live lab node with the
new `lab/ng_stop_media_probe.py` — see lab.md; the numbers are in the D2 row
below. No live MSS call was driven for D3/D10/D13: the lab's `mss-control`
image predates this commit.

### 28. The speech-report ingress had no wire (defect D19) — ✅ DONE (2026-08-23)

`SessionRegistry::report` — `ConsumerEvent` → `SpeechStarted` / `Partial` /
`Final` / `EndOfUtterance` / `EndOfInteraction`, the documented head of the
barge-in chain — had **no caller outside its own tests**. A consumer could hear
the caller start talking and had no way to say so.

**What shipped.** `ConsumerToServer` gains a `SpeechReport` message (field 5,
additive; `protox` regenerates), carrying `kind`
(`STARTED`/`PARTIAL`/`FINAL`/`END_OF_UTTERANCE`/`END_OF_INTERACTION`), `track`,
`text`, `confidence`, `observed_at` and a `reason` for
`END_OF_INTERACTION`. The gRPC pump converts it (`convert::speech_report`) and
calls `SessionController::record_report`, which commits through the registry
like every other event — so it publishes on `mss.events` through the same
`event_pump`, with `first_final` still decided by MSS rather than the consumer.

**Capability enforcement.** `Registry::report` already required
`Capabilities::EVENTS`; the wire now surfaces that as the same
protocol-violation shape as an unprivileged `inject` — `PERMISSION_DENIED` and
the stream ends, not a silent no-op.

**Two decisions worth recording.** *The consumer's clock is not trusted:*
`observed_at` is carried and logged as a lag (`convert::observed_lag_ms`), but
the published `MediaEvent.at` is stamped by MSS, because reconciling two clocks
across a bus is not something an event consumer can do after the fact. *An
unknown or unspecified `kind` is `INVALID_ARGUMENT`*, never a default — a
consumer built against a newer proto learns it was misunderstood.

**The WS residual, accepted.** The `WS_TWILIO` dialect gets **no** speech
report: its bytes are frozen (Article VII) and it has no message that could
carry one, so a WS consumer cannot report speech and its only barge stays the
`clear` message's direct rtpengine `stop media`. Interactive voice-AI should
attach over gRPC. This is now the documented adapter limitation rather than an
open defect.

**Verified against what.** Unit tests on the conversion (every kind, an unknown
kind, an unknown track, `END_OF_INTERACTION` needing no track, the lag
calculation) and two over-the-wire gRPC tests (an `EVENTS` consumer's reports
arrive on `WatchEvents` as `SpeechStarted` then `FinalTranscript{first_final:
true}`; a `SINK`-only consumer gets `PERMISSION_DENIED` naming `EVENTS`).
**Live:** `lab/barge_drill.sh` was extended to attach a real gRPC consumer
(`mss_ctl consume`, new subcommand) and trigger on a real `SpeechReport`; two
runs of 10 iterations against the live stack, 10/10 each, no event missed, the
consumer taking 378 tapped audio frames on the same stream while it measured.
Numbers in item 5 and lab.md.

### 29. Recording-group members were not time-aligned (defect D18) — ✅ DONE (2026-08-23)

Every member's segmenter anchored on **its own first frame**, so a participant
that joined a conference recording ten seconds late produced a file whose
sample 0 was ten seconds later than the first member's. Two objects under one
recording prefix, different lengths, and nothing in the audio to say where the
second one begins: reassembly needed the `RecordingStarted` event timeline.

**What shipped.** The group owns t=0. `RecordingGroup` stamps `opened_at:
Instant` when its first member joins, `join_group` hands that instant to every
later member, `RecorderSpec.group_anchor: Option<Instant>` carries it into the
recorder task, and when that member's **first media frame** arrives the task
converts `now - anchor` into leading silence through the new sans-IO seam
`Segmenter::lead_with_silence(Duration)`. All members share t=0, and members
that stop together are the same length to within one frame.

**Three decisions.** *Measured at the first frame, not at the attach*, so the
member's own subscribe round-trip is inside the pad (and the same latency on
every member cancels out of their relative alignment). *Padded once per
recording, never per segment*: the pad is written into `segment_start` under a
`stats.lead_silence_frames` guard and `pause` now takes
`frames().max(segment_start)`, so pause/resume (pause = segment + defer) can
neither erase nor re-add it. *The pad counts against `MAX_RECORDING`* — a
member joining an hour into the 2 h cap gets an hour of its own audio, not two.
An ungrouped recording passes `group_anchor: None` and is byte-identical to
before.

**Verified against what.** Four new tests: the late member's file opens with
silence to the anchor and its own audio lands after it; two members that end
together render the same length (one padded 1 s, one not); a paused late member
pads its lead once and a second `lead_with_silence` is refused; and one
end-to-end recorder test (spawn → hub frames → real WAV bytes out of a fake
sink) proving a 400 ms anchor becomes 400 ms of leading zeros in the uploaded
object. `join_group` also asserts every member gets the same instant.
**Live:** `lab/group_recording_drill.sh` grew `JOIN_STAGGER_SECONDS` (default
5) and was run against the live lab — bob joined 5 s late, the pod logged
`lead_silence_ms=5016`, and the object read back off MinIO opens with **40128
zero samples = 5016 ms**. Lengths: alice 25.030 s vs bob 25.116 s, an **86 ms**
difference where the stagger was 5 s. The residual 86 ms is the *tail*: at the
time `Detach` waited for the upload (D11, closed by item 50 on 2026-08-26) and
the drill detaches the members one after the other. Numbers in lab.md.

**Residual.** Head alignment is exact; equal length still assumes the members
stop together, and D16 (a group is one pod's memory, so the anchor is one pod's
monotonic clock) is unchanged. D11's blocking detach — the 86 ms tail measured
here — is closed by item 50.

### 30. Recording durability across pod death (defect D9) — 🔶 **partly closed (2026-08-23)**

A recording used to live entirely in the recording pod's memory until the call
ended: `kill -9` on that pod lost every second of it, the adopting pod started
a fresh recording under the same object key, and nothing said how much audio
had gone. The spill directory existed but was only ever written *after* a
failed upload, which never happens if the process dies first.

**What shipped.**

1. **Closed segments leave memory for disk while the call is still up.** The
   segmenter grew a peek-then-commit seam — `closable_frames()`,
   `render_closable(frames, layout)`, `close_segment(frames)` — so the recorder
   renders a whole-millisecond prefix per target, writes it, and only then
   drops it from memory: a failed disk write loses nothing, it just leaves the
   audio in RAM and retries on the next tick. Closing rebases the segmenter's
   timestamp anchor by exactly the frames removed, so track alignment across a
   segment boundary is preserved rather than re-zeroed (that is what makes this
   different from a pause, which deliberately drops the wall-clock gap). The
   2 h `MAX_RECORDING` cap now counts spilled frames too, so spilling does not
   hand a recording a fresh length budget.
2. **A spill journal per recording.** `crates/mediaserverd/src/recording_spill.rs`
   writes `<MSS_RECORDING_SPILL_DIR>/journal/<object key>/{manifest.json,
   <target>-<seq>.pcm}`: raw interleaved i16 chunks plus a manifest naming the
   recording id, sample rate, owning pod, frames on disk and one entry per
   object key. The manifest is rewritten by atomic rename after each segment.
   Segments close every `MSS_RECORDING_SPILL_SECONDS` (default 30) and on every
   pause. The final upload **stitches** the journal's chunks with the tail still
   in memory into one object at the frozen
   `${accountID}/${recordingID}.${format}` key, then deletes the journal; if
   the upload fails the journal stays for the next start.
3. **Restart salvage.** `recording_spill::salvage` runs in `mediaserverd`'s
   boot path, before it serves: every journal left on this pod's disk is
   stitched and uploaded. It is guarded by a new `RecordingSink::exists` —
   if the object is already in storage (because another pod adopted the session
   and finished it), the salvage is **skipped, counted and left on disk** for an
   operator, never written over the fuller object.
4. **A recording's progress is in the registry.** `PersistedAttachment.recording:
   Option<PersistedRecording>` (serde(default), legacy records decode)
   carries `{recording_id, owner, recorded_ms, spilled_ms}`, fed by a new
   `TapSubscriptions::recording_journal(attachment)` seam off the live
   recorder's `RecordingProgress`. On adoption the keeper injects
   `mss.recording.resumeMs` / `mss.recording.spillOwner` into the rebuilt
   attachment's metadata (derived every time, stripped when persisting, so it
   never accumulates).
5. **The adopter recovers what it can read and counts what it cannot.** The new
   recorder opens the journal at the same key: if the journal is on *this* pod's
   disk (same-pod restart) its segments are picked up and continue the same
   object. Whatever the registry says was recorded but is not on a disk this pod
   can read becomes leading silence, so the object keeps its wall-clock
   timeline, and every one of those frames is counted in
   **`mss_recording_frames_lost_on_adopt_total`**. Beyond `MAX_ADOPT_LEAD`
   (5 min) the padding is refused rather than allocated, and the loss is
   counted and logged.

**What is NOT possible today, honestly.** The spill directory is per-pod local
disk. An adopter on another pod **cannot read the dead pod's segments**, so
cross-pod stitching does not exist: the adopter's object contains silence for
the dead pod's audio, and that audio is only recoverable if the dead pod comes
back with the same volume *and* the object has not already been written (the
`exists` guard then keeps the salvage from clobbering it, which is the correct
choice but means the audio stays on disk as an operator's problem). Making this
whole needs a spill target every pod can read — a shared volume or a multipart
upload straight to object storage per segment — which is the same shape D16
(recording groups are one pod's memory) needs. **The spill directory has no
retention policy: skipped and failed journals stay until an operator removes
them.**

**Verified: replay/unit only** (fake `SessionStore`, fake object store, real
local disk under a scratch directory) — a closed segment leaving memory without
moving the recording's clock, the cap surviving a spill, a spilled segment plus
the in-memory tail uploading as **one** object in the right order, a journal
left behind being salvaged on the next start, salvage refusing to overwrite an
object that already exists, an adopted recording padding and counting what its
dead pod never spilled, and an adopted recording that finds its own spill
keeping that audio (spilled | silence | new). The keeper test proves the
journal is persisted with the owning pod and handed to the adopter as metadata.
**Not observed live**: no pod-kill drill was re-run with a recorder attached,
so the numbers a real `kill -9` costs a recording are still unmeasured.

### 31. FS byte-parity against a real FreeSWITCH recording — 🔶 **measured, one blocker documented (2026-08-23)**

`lab/recording_parity.py` had existed since item 15 but had **never seen a
FreeSWITCH recording**; the Phase-2 exit criterion asks for "byte-comparable
recordings vs FS output". `lab/fs_parity_drill.sh` now records **one live call
both ways** — FS's own `uuid_record` with `RECORD_STEREO=true`, and an MSS
`FILE_S3` attachment on the same call — pulls both wavs and runs the harness.

**The lab FS image can record.** Probed: `mod_dptools` gives `record`,
`record_session`, `record_session_pause/resume/mask/unmask`,
`stop_record_session` and `mod_commands` gives `uuid_record`; `mod_sndfile`
provides the `wav` file format; `/var/lib/freeswitch/recordings` is writable
(the container runs as root). So the "the image cannot record" escape hatch in
the item's original framing does **not** apply.

**The run (2026-08-23, 25 s of a live PCMA call through OpenSIPS → rtpengine →
FS ext 9000, MSS tapping the same call):**

| Check | MSS | FreeSWITCH | verdict |
| --- | --- | --- | --- |
| container | 2ch 8000 Hz 16-bit | 2ch 8000 Hz 16-bit | **exact agreement** |
| channel layout | customer left, agent right | read left, write right | **agrees** |
| customer rms | 624 | 623 | **agrees to 1 part in 624** |
| duration | 25500 ms | 25120 ms | 380 ms apart |
| samples @ one global offset | identical 0.3746, mean_diff 485.6 | | fails the 200 bar |
| samples, best 2 s window | agreeing **1.0000**, mean_diff **0.6** | | **parity** |

**What this establishes.** Container, channel layout and amplitude are in exact
agreement — an MSS stereo recording is drop-in for an FS `RECORD_STEREO` one as
far as any downstream consumer can tell. And there is **no transform
difference**: re-aligned per 2 s window, one window reaches a 1.0000 agreeing
ratio at a mean absolute difference of **0.6 out of 32768**, i.e. MSS's
PCMA→L16 decode and FS's produce the same samples.

**What fails, and why it is not a recorder defect.** A single global offset
cannot hold for the whole call: MSS and FS have **independent jitter buffers
and conceal loss independently** (MSS does G.711 Appendix I PLC since item 17;
FS does not), so the offset between the two files wanders and per-sample
identity collapses everywhere the grid slips. The 380 ms duration gap is drill
skew — the MSS attach precedes the `fs_cli uuid_record` by three `docker exec`
round trips — not accumulated drift. The harness gained `--drift-window` /
`--drift-stride` so this is reproducible rather than a one-off observation.

**The honest conclusion: byte-for-byte parity at a fixed offset is not an
achievable bar across two independent jitter buffers, and should not be the
exit criterion.** The bar that means what the criterion intended is: identical
container and layout, duration within tolerance, matching per-channel rms, and
near-perfect agreement in a re-aligned window. All four are met.

**What a production-FS visit still owes** (this is the residual, and it is
deployment-gated):
1. **A two-party call.** The lab's ext 9000 answers and plays
   `silence_stream://-1`, so FS's write side is silence: the agent/right
   channel compares silence against silence (mss rms 8 vs fs rms 0) and only
   the customer channel is a real comparison. Two live voices need a bridging
   extension on a richer rig.
2. **The pause contract compared**, `record_session_pause` against MSS `Pause`
   — this drill did not pause.
3. **The tenant's own codec, rate and any recording post-processing**; this run
   was PCMA/8000 only.
4. **A human listening to both files**, which the harness has never claimed to
   replace.
5. The windowed offsets in this run **clip at `--align-search` (1600 frames)**,
   so the reported 260 ms wander is a floor, not a measurement. A wider search
   is O(search x window) and was not worth a second lab cycle.

### 32. Inline-leg egress groundwork — the playout pacer (Phase 3) — ✅ DONE (2026-08-23)

Phase 3 needs a *mouth*: taps can only listen, so an interactive session has to
own an RTP sender. `crates/media-core/src/pacer.rs` is that sender's brain,
sans-IO and thread-free: the control world pushes PCM, the media thread calls
`tick(now)` once per wakeup, and the pacer hands back at most **one
ready-to-send RTP datagram per ptime** — header included (SSRC, sequence,
timestamp, payload type, marker), built with the `rtp.rs` serializer that
already existed, and payload encoded by the existing `ConsumerEncoder` (so
G.711 µ-law/A-law, L16, and 8→16/48 kHz resampling come for free; Opus
**output** is still unbuilt — decode-only, as `encode.rs` says by name).

Contract, decided here and recorded so P3-2/P3-3 do not re-litigate it:

- **Deadlines are wall-clock-anchored** (architecture §7): the first `tick`
  anchors, each emission advances the deadline by exactly one ptime. A caller
  that overslept catches up one packet per call and every skipped deadline is
  counted in `late_ticks` — no burst inside one tick, no drift.
- **Underrun never starves the far end.** Default `UnderrunPolicy::Silence`
  emits an encoded silence frame (counted `silence_frames`); a partially
  filled frame is zero-padded (`partial_frames`). `UnderrunPolicy::Suppress`
  is the DTX-shaped alternative: it emits nothing, still advances the
  timestamp (`suppressed_frames`) and does **not** consume a sequence number,
  so a far-end jitter buffer reads the gap as silence, not as loss.
- **Marker on talkspurt start** — the first packet of the stream and the first
  voice packet after any silence or suppression carry it.
- **The queue is bounded and drops the oldest**, in samples, counted
  (`dropped_samples`); a push larger than the whole queue keeps only its tail.
  `clear()` flushes it and counts `flushed_samples` — that is the barge-in
  cut-through seam P3-3's `Clear` and P3-4's measurement will use: the tick
  after a `clear()` is silence.
- **No allocation after construction.** The sample ring, the frame scratch
  buffer and the datagram buffer are sized once from the negotiated formats
  (payload capacity carries 8 samples of resampler slack). An encode or
  serialize failure is counted (`encode_errors`) and drops that one frame
  instead of panicking.

Replay-tested (14 tests, `cargo test -p media-core pacer`): tick cadence at 5 ms
polling emits exactly at 0/20/40… ms; oversleeping is counted and caught up;
underrun → silence → marker on resume; header fields on the wire re-parsed and
compared against the reported ones with timestamps stepping 160 with no skip;
suppression advancing timestamp but not sequence; drop-oldest keeping the newest
two frames; sequence/timestamp wrap; partial-frame padding; the wideband→G.711
resampling path; and **10 000 ticks with all three internal buffer capacities
unchanged** at the end (with drops and silence both exercised in the loop).

This is replay-only by construction — media-core has no sockets. The pacer is
not wired to anything yet; P3-2 (inline session + SDP answer + UDP socket pair)
is what puts it on the wire, and only then can the cadence be judged against a
real far end.

### 33. Inline sessions in mediaserverd — the leg on the wire (Phase 3) — ✅ DONE (2026-08-23)

`SessionKind::INLINE` stops being refused. `CreateSession{kind=INLINE,
sdp_offer=...}` now binds a UDP socket on `MSS_TAP_LOCAL_IP`, answers the offer
as an RTP endpoint and returns the answer in the new `Session.sdp_answer` field.
The peer's audio enters the **existing** capture pipeline (jitter → decode →
hub) as the session's `customer` track, so every consumer, recorder and
recording group already built for taps works on an inline leg unchanged; audio
leaves through item 32's `PlayoutPacer`, driven from the capture thread's own
clock.

Decisions, recorded so P3-3/P3-4 do not re-litigate them:

- **The SDP lives in `rtpengine-ng/src/sdp.rs`**, next to the subscription
  offer/answer, as `InlineOffer`/`InlineAnswer`. That module was already a
  plain sans-IO SDP implementation depending only on `media-core`; a second
  parser in `media-core` would have duplicated the line splitting, the rtpmap
  table and the codec-negotiation types, and `media-core` must not depend on
  `rtpengine-ng` (dependency direction). The crate name is now narrower than
  its sdp module — if a third dialect appears, split the module into its own
  crate rather than duplicating it.
- **PCMU or PCMA, in the offer's own order, plus telephone-event.** Anything
  else is refused **by name** (`SdpError::NoInlineCodecOffered` lists what was
  offered, from the rtpmaps, so an opus/G722 offer says so). The reason is
  honest: `ConsumerEncoder` has no Opus *encoder*, so an Opus inline leg could
  hear but not speak. The answer echoes the offered telephone-event payload
  type with `fmtp 0-15`, one `m=audio`, `a=sendrecv`, and the offer's ptime
  (falling back to the configured one, refusing anything over 120 ms).
- **The egress queue is the buffer; the pacer ring is the prebuffer.** The
  control world pushes `Vec<i16>` chunks into a bounded `ArrayQueue` (64 chunks
  ≈ 6.4 s at the 100 ms chunking `StartPlayback` uses) exactly as the hub's
  inject path already does; the media thread pops at most 4 chunks per tick and
  only while the pacer holds less than two frames. A full queue **refuses** the
  chunk (counted) rather than blocking the control world, and a playback longer
  than the free queue is refused whole rather than truncated.
- **`StartPlayback` on an inline leg is a local mix-in, not an NG command.**
  rtpengine `play media` needs a subscription and an inline leg has none, so a
  wav blob/file is decoded in the control world (16-bit mono at the negotiated
  rate; anything else is refused naming the mismatch) and queued. **`StopPlayback`
  on an inline leg flushes the egress queue** — that is the barge seam, and the
  tick after it is silence by construction (item 32), so P3-4's job is to
  measure it, not to build it. Streaming playback still refuses by name: it is
  an INJECT attachment (P3-3).
- **An inline session is not adoptable, and the registry says so.** A tap is
  re-creatable from another pod because MSS asks rtpengine for the copy; an
  inline leg *is* the RTP destination the peer is sending to, and that socket
  died with the pod. `PersistedSession::is_rebuildable()` is now false for
  kind INLINE, the keeper releases such an orphan with a counter
  (`mss_registry_inline_not_adopted_total`) and a log line saying recovery is
  call control's job. The SDP is deliberately **not** persisted — persisting it
  would only invite a dishonest rebuild.
- **Every event now carries the session kind** (`MediaEvent.session_kind`,
  wire field 7), rather than a new `SessionCreated` event. A bus consumer can
  tell a tap's events from an inline leg's without asking the API, and no
  existing event sequence shifted.
- `CreateSession` validation is symmetric: INLINE without an offer is
  `INVALID_ARGUMENT`, and a TAP *with* an offer is too (a tap has no SDP).

Tested: 10 SDP offer/answer replay tests (answer bytes asserted exactly, our own
answer re-parsed, offer-order PCMA, opus/G722/bare-PT refusals by name, two
m-lines, port 0, no `c=`, absurd ptime, stream-level `c=` winning); 6 egress
tests (paced RTP the fake peer parses with pt/seq/ts checked, clear-then-silence,
queue-full refusal, unreachable peer counted not stalled, ssrc derivation, an
Opus wire format refused); and an **over-the-wire test with a fake peer socket**:
the peer sends 8 G.711 packets and the hub delivers them as `customer` frames
(tone amplitude asserted), then a queued wav comes back as ≥8 paced datagrams
from the port MSS answered on, all PT 0, sequence never skipping. Plus
`stop_playback` flushing, the long-playback and wrong-rate refusals, the keeper's
inline-orphan release, the API's answer round-trip through `DescribeSession`, and
the registry's offer/answer bookkeeping. `mss_ctl inline <id> <call-id>
<offer-file>` prints the answer for the P3-5 drill.

**Not verified live.** No SIP peer has ever answered this leg: everything above
is replay or a fake socket in-process. P3-5's `inline_call_drill.sh` is what
puts a real RTP peer (and a real tone) on it, and only then can the cadence,
the DTMF path and the 20 ms budget be judged.

### 34. Full duplex to consumers — a continuous inject stream (Phase 3) — ✅ DONE (2026-08-23)

An INJECT-capable attachment on an **INLINE** session now feeds the leg's egress
queue continuously, on both transports. Nothing about a **TAP** session changed:
its `Mark` still builds one wav and calls `StartPlayback`, its `Clear` still
stops that playback, and the one-playback-datagram cap (`MAX_UTTERANCE_SAMPLES`)
still applies there — an rtpengine `play media` really does carry one datagram,
so the cap is the protocol, not our choice.

The seam is one trait: `control_api::InlineEgressSink` (`egress_format`,
`push_pcm`, `flush`, `pushed_watermark`, `drained_watermark`), returned by the
new `MediaPlane::inline_egress_sink(session)` — default `None`, so a media plane
that has no inline legs (and every fake) needs no change. `TapPlane` implements
it over item 33's `InlineEgressHandle`, and the trait is resolved **once** per
stream, so a per-frame inject costs a lock-free queue push and no async hop into
the plane.

Decisions, recorded so P3-4/P3-5 and Phase 4 do not re-litigate them:

- **The consumer's declared format is decoded to PCM, and the rate must match
  the leg.** PCMU/PCMA/L16 decode as they always did (`decode_inject`); Opus is
  still refused by name. What is *new* is a refusal: an inject stream whose
  attachment declared a sample rate other than the one the leg negotiated is
  `FAILED_PRECONDITION` naming both rates, at the first inject frame. The
  honest reason: one pacer serves the whole leg, its `source` rate is the
  negotiated rate, and there is no streaming resampler on the inject path —
  `ConsumerEncoder` resamples a *packet* at a time and truncates past
  `chunk_in`, so reusing it here would silently eat audio. A 16 kHz TTS
  consumer therefore attaches at the leg's rate (or as L16/8k) until an inject
  resampler is built. Mismatch is checked once, at the first inject, not per
  frame, and never at subscribe time — a SINK-only consumer on an inline leg is
  perfectly legal and must not be refused for a rate it will never use.
- **`Clear` flushes the egress; it does not stop a playback.** On an inline
  session `Clear` calls `flush()` (item 32's queue+pacer clear, so the next tick
  is silence by construction) and drops every pending mark. It also **requires
  INJECT**, like `Inject` itself: flushing a leg's mouth is a media-affecting
  act, and so is `Mark`, so the whole inject sub-protocol is capability-gated on
  an inline session. On a tap, `Clear`/`Mark` keep their old unauthorized-but-
  harmless behavior — that path only touches the consumer's own playback.
- **`Mark` acks when the marked audio has actually drained**, not on enqueue.
  This is cheaply knowable because every sample the control world queues ends up
  in exactly one of three places, so `InlineEgressShared` publishes
  `drained_samples = samples flushed straight out of the chunk queue +
  pacer.pushed_samples - pacer.queued_samples()` — an exact identity (a partial
  frame drains the remainder, a ring overflow counts as dropped). A `Mark`
  records the queue's `pushed_samples` watermark, and the ack goes out when
  `drained_samples` passes it. The pump does not notify, so each stream **polls
  every 20 ms while a mark is outstanding** (one ptime; no timer at all when
  none is) — an ack is therefore accurate to within one frame, late rather than
  early, which is the safe direction for "the prompt finished". At most 64 marks
  may be outstanding (gRPC refuses beyond that; the WS dialect drops the oldest,
  having no error frame).
- **The gRPC ack needed a wire, so `ServerToConsumer` gained `Mark mark = 6`**
  (additive; next free tag is 7). The WS dialect already had `Outbound::Mark`
  and its bytes are untouched — the Twilio serialization tests still pin them.
  A tap's `Mark` is never acked on either transport: it starts a playback, and
  rtpengine gives no completion signal.
- **A full egress queue drops the chunk and counts it** rather than ending the
  stream. 64 chunks is ≈6.4 s of backlog; a consumer that far ahead of the
  wire is misbehaving, but killing a live voice-AI stream over a transient
  overrun is worse than dropping a frame. Visible as
  `mss_inline_egress_chunks_refused_total` plus a warning, and on WS as
  `ConsumerStats::inject_dropped`.
- **WS inbound media on an inline session flows straight through** — no
  utterance accumulation, no 700 ms idle flush, no `BridgeCommand`. The
  accumulate-then-play path stays exactly as it was for taps. The dialect's
  optional `sampleRate` is honored: a value other than the leg's rate is
  counted as an unknown encoding and dropped. The dialect decodes µ-law to PCM,
  so a PCMA leg is fed correctly — only the *rate* has to agree.

New metrics: `mss_inline_egress_pushed_samples_total`,
`mss_inline_egress_drained_samples_total`.

Tested: 4 new over-the-wire gRPC tests (three inject frames land as 480 samples
of decoded PCM in the sink with **no** playback issued; `Clear` flushes it and
stops nothing; a mark stays unacked for 300 ms while its audio is queued and is
acked by name the moment the sink drains; a 16 kHz-vs-8 kHz mismatch refused by
name; an unprivileged inject still `PERMISSION_DENIED` with an empty sink) and 3
new WS tests against a **real** `InlineEgress` and a real peer socket (inbound
media reaches the peer as paced µ-law rather than an utterance, `Clear` makes the
next paced frame silence, a mark is acked only after six pumps drained it). The
pre-existing tap tests are the regression proof that taps did not move: the fake
plane now offers an inline sink for *every* session, and the tap path still
builds its wav and stops its playback.

**Not verified live.** Both transports were driven against a fake plane or an
in-process socket; no SIP peer and no real voice-AI consumer has spoken through
this. P3-5's drill is what proves a real ear hears it, and P3-4 owes the
measured cut-through.

### 35. Inline barge-in measured, and the inline lab drill (Phase 3) — ✅ DONE (2026-08-23)

Items 32–34's mouth met an ear. `lab/inline_call_drill.sh` runs the whole inline
path with no SIP stack and no human: `lab/inline_peer.py` offers PCMU and
becomes the far end of MSS's own socket, `mss_ctl inline` answers it,
`lab/inline_consumer.py` attaches with `SINK | INJECT` and speaks 1000 Hz into
the leg while the peer speaks 440 Hz back, and `lab/inline_barge_report.py`
turns the peer's per-packet arrival timeline into the number Phase 3 owed. Both
python actors run as containers on the lab network so their timestamps come from
one kernel clock — a host-to-VM skew would be a large error against a 20 ms
target.

Measured live (stamp 1787508102, 20 iterations; full table in
[lab.md](lab.md#inline_call_drillsh--an-inline-leg-with-a-real-ear-and-the-barge-in-number-2026-08-23)):

- **barge-in cut-through, `Clear` sent → first silent packet at the peer's ear:
  p50 12.2 ms, p95 20.4 ms, max 21.0 ms, min 2.4 ms (n=20).** The target was one
  ptime and the max is one ptime plus a millisecond of transport. The
  distribution is uniform over one frame because `Clear` flushes the chunk queue
  *and* the pacer ring, leaving only the wait for the pacer's next 20 ms
  deadline — the replay claim ("the tick after `clear()` is a silence frame") is
  now a live number, and it is an *arrival* measurement, so it includes the gRPC
  hop, the 5 ms pump tick and the lab bridge.
- **The peer hears the injected audio**: 1710/3312 egress packets carried the
  1000 Hz tone, peak Goertzel 11910 of a theoretical 12000.
- **The hub still taps the peer while the leg is being spoken to**: 2770 frames
  of the peer's own 440 Hz to the same consumer, peak rms 17132.
- **Egress is properly paced**: 50.19 pkt/s over 66 s, 0 sequence breaks, rtp
  timestamp step 160 for every one of 3311 gaps, `late_ticks_total` 0,
  `dropped_samples_total` 0, `send_errors_total` 0, and
  `drained_samples_total == pushed_samples_total`.
- **`Mark` acks when the audio drained**: 410 / 404 / 404 ms against 400 ms of
  queued audio.

Two corrections the drill forced, both worth remembering:

- **The first cut-through numbers were phase-locked and too flattering.** A
  1.5 s tone plus a 1.0 s gap is exactly 125 packets, so every `Clear` landed at
  nearly the same phase of the 20 ms grid and the run sampled a quarter of the
  distribution (p95 15.3 ms, monotonically decreasing per iteration). The
  consumer now jitters the gap by up to one frame (`JITTER_MS`), and the honest
  p95 is 20.4 ms. Any future paced-media measurement in this repo must jitter
  its period or it measures its own arithmetic.
- **A real defect, found and fixed here: the mark drain-poll starved.** The
  first run acked a `Mark` after **8221 ms** although the audio drained on time
  at 400 ms. Both stream loops built item 34's 20 ms poll as
  `tokio::time::sleep(...)` *inside* `tokio::select!`, so the timer was
  recreated — and reset — on every loop iteration; with tapped frames arriving
  every 20 ms and `select!` picking randomly among ready branches, the sleep was
  cancelled before it ever elapsed. One `tokio::time::interval` pinned outside
  the loop, with the branch gated on `if marks_pending`, fixes it in
  `control-api/src/stream.rs` and `mediaserverd/src/consumer_ws.rs`. A `sleep`
  inside `select!` is a timeout, not a timer.

**What this does not prove:** one PCMU/8 kHz leg, one INJECT consumer, an idle
box; no PCMA/L16/Opus inline leg, no impairment, no second leg, no SIP (the
offer and answer travel through two files), no human listening to
`peer_ear_*.wav`, and the consumer's own detection latency is still outside the
number — item 5 measures the speech-report→`StopPlayback` hop, this one measures
`Clear`→silence, and adding them is as close to end-to-end barge-in as MSS can
honestly get on its own.

### 36. The N-way mixer core (Phase 4) — ✅ DONE (2026-08-23)

Phase 4 is where FreeSWITCH loses its last media job, and this is its engine:
`crates/media-core/src/mixer.rs`, a sans-IO `MixMatrix` of **N contributors x M
listeners**. Push each contributor's frame for tick T, call `mix()`, get one
frame per listener back — i32 accumulate, saturate to i16, minus-self by
default. Nothing about it knows what a session, a socket or an rtpengine is;
P4-2 is what puts legs on either side of it (per-leg ingest in, one `pacer.rs`
per listener out).

The design decision worth recording is that **all three Phase-4 features are
the same matrix**, not three code paths: contributors and listeners are
separate memberships, so a *party* is one of each linked by a muted self-pair, a
**monitor** is a listener with no contributor (hears all, contributes nothing),
a **whisper** is a contributor whose row is unity into exactly one listener and
muted elsewhere (`route_only`), and **barge** is `route_to_all` — a matrix
flip. Mute is a zeroed row, deaf is a zeroed column, and their inverses restore
the minus-self defaults, which is the whole of P4-5's routing work.

Membership is generational: `ContributorId`/`ListenerId` carry the slot
generation, so a handle from a party that already left is refused rather than
addressing whoever reused the index; leaving clears the pending frame, the
speech state, the self-link, the gain row and column, and the listener's output
region, so a reused slot cannot leak the previous occupant's routing or a tail
of their audio. Active-speaker flags are per contributor with attack **and**
hangover (default 2 frames up, 12 frames ≈ 240 ms down) so they cannot flap;
they are reported (`speaking`, `level`, `active_speakers`) and never affect
routing. Per-pair gain is Q12 fixed point, capped at 8x so no sum can overflow
i32; clipping is counted (`clipped_samples`) rather than hidden, which is the
signal that a conference wants AGC — AGC and any DC filter are not built.

Replay-tested, 23 tests (`cargo test -p media-core mixer`): 2/3/8 parties each
hearing exactly everyone-but-self; a frame consumed by one tick only; an absent
contributor as silence; positive and negative saturation with the clip count
asserted; per-pair gain scaling one direction only; whisper heard by its target
and by nobody else, then promoted to all; a monitor hearing the sum and
contributing nothing; mute/deaf as row/column zeroing and their restore;
active-speaker attack, hangover and the alternating-frame case that must produce
**zero** onsets; a stale identity refused after leaving; a reused slot starting
clean and silent; growth past the initial capacity; and **10 000 ticks of an
8-party conference plus a monitor with every buffer length and capacity
unchanged at the end** (the no-allocation-per-frame rule, asserted rather than
asserted-in-prose).

Deliberately deferred, and recorded in implementation-notes so P4-2 does not
rediscover them: the sum-once-subtract-self fast path (invalid the moment
whisper or per-pair gain is in play, so it needs a "matrix is default" flag if
conference fan-in ever demands it); multi-rate conferences (every contributor
must arrive at the conference rate and frame size — resampling belongs to the
leg); AGC; and Opus egress, still absent crate-wide, so a conference of Opus
legs transcodes on the way out. This is replay-only by construction: media-core
has no sockets, so mixing quality against real legs cannot be judged until the
P4-6 drill.

### 37. Conference sessions in mediaserverd (Phase 4) — ✅ DONE (2026-08-24)

`CreateSession{kind=INLINE, group="<conference>"}` now joins a leg into a
conference: the legs that name one group share **one** `MixMatrix` driven by
**one** capture-world clock, each hears everybody but itself, and the
conference's full mix is published to every member's hub as the `mixed` track so
monitors and recorders attach with the verbs that already exist. New proto
fields (additive, wire-compatible): `CreateSessionRequest.group = 9` and
`Session.group = 11`, mirroring the recording group on `Attach` rather than
inventing a conference noun; a `group` on a TAP is refused by name, since a
recording group is named on the attachment. `mss_ctl inline <external-id>
<call-id> <offer-file> [conference-group]` is the lab handle for it.

The design decision this item owed was **threading**, and it is recorded in
implementation-notes: one *conference-owner thread the legs migrate onto*. Each
inline leg is still built in the control world exactly as a two-party leg is
(socket, jitter/decode pipeline, hub, `InlineEgress`), but a grouped one is then
handed to the conference thread over a bounded queue instead of getting a thread
of its own. Mixing is frame-synchronous, so a matrix shared between per-leg
threads would need a lock in the packet path — the two-world rule forbids
exactly that. Membership is decided in the control world under the conference
table's lock (so a join cannot race the last leave), the thread never removes
itself from the table, and the leave that empties a conference hands its
`JoinHandle` back to `close_session`, which joins it the same way it joins a
per-session capture thread. Per-session teardown stays one path.

Per tick: each leg's released frame goes to its own hub as `customer` **and**
into the matrix as its contributor (`TapLeg::release_frame_with`, no copy); each
member's queued playback/inject audio is cut into exact frames and pushed as
that member's private injector (`route_only` into its own ear, so a prompt
played into one leg still reaches only that leg); one `mix()`; then every
member's minus-self ear goes to its own `PlayoutPacer` and the monitor
listener's full sum goes to every member's hub as `mixed`. A leg that releases
nothing is silence and counted; a leg leaving is a command handled at the top of
a tick, so **a member leaving cannot stall the mix**. One mixer sharp edge found
here and now documented: `join_listener` resets its column to the defaults, so
every non-default route (the injectors') must be re-applied after each
membership change.

**Multi-rate conferences are refused by name**, as the item asked: the joining
leg's rate, ptime and channels must equal the conference's (its first leg sets
them), and the error says why. Encoding may differ, because each leg owns its
own egress encoder. Per-leg resampling is the residual.

Verified in-process over real UDP sockets (5 new tests in `tap_plane.rs`, plus a
control-api test for the API rule): three fake peers in one group with 1000 /
2000 / 4000 tones read **6000 / 5000 / 3000** off the wire — the sum of the
other two, never their own; a hub monitor on one member sees the mixed track at
**7000** (everyone, self included); closing one leg mid-mix leaves the survivors
reading exactly each other while the conference stays live, and the last leg out
closes it; a prompt played into one leg is absent from the other's ear; a 40 ms
leg is refused from a 20 ms conference. New metrics: `mss_conferences_live`,
`mss_conference_members_live`, plus joins/leaves/mixed-frames/clipped/absent/
reanchor/refused counters.

**Not proven and not claimed:** no real SIP peer has ever been in a conference —
that is P4-6's drill. Conferences are pod-local (a group is one pod's table, and
an inline leg is not adoptable anyway), have no tenant scope on the group name
(two tenants picking the same name would share a mix; prefix it until sessions
carry a tenant), and there is no AGC. Conference recording as one mixed file is
P4-4; monitor/whisper/barge attachments are P4-3; member mute/deaf/hold verbs
are P4-5.

### 38. Monitor, whisper and barge (Phase 4) — ✅ DONE (2026-08-24)

Three conference features, **no new RPC**: they are matrix cells named by
attachment metadata, exactly as item 36 predicted.

**Monitor** is the verb that already existed and now has a name:
`Attach{transport=GRPC_STREAM|WS_TWILIO, capabilities=SINK,
selector.only="mixed"}` on **any** member session. The conference's full sum is
published to every member's hub as `mixed`, so a consumer that selects that one
track hears the whole conference (itself included — a monitor is a record, not
an ear) and injects nothing. Verified: a `mixed`-only subscriber on a
three-tone conference reads **7000** (1000+2000+4000), and the plane accepts a
`only=mixed` SINK attachment on a conference leg. No code was needed for it.

**Whisper** is `mix_target=<member-external-id>` in the metadata of an
**INJECT** attachment on a member session: everything that attachment injects
is routed into that member's ear **only**. The supervisor case is a supervisor
leg in the conference plus an INJECT attachment on it naming the agent.
`mix_target` without INJECT is refused by name (`invalid_argument`), and so is
`mix_target` on a leg that is in no conference.

**Barge** is the same key flipped to `mix_target=all`, carried by
`UpdateAttachment` — which gained `map<string,string> metadata = 6` (additive;
merge semantics: named keys are overwritten, the rest untouched). That was the
cheapest additive path: pause/resume already lives on `UpdateAttachment`, so
every metadata-carried verb can ride it without a new RPC. `mss_ctl mix
<attachment-id> <own|all|member-id> [include|exclude]` drives it from the lab.

**Decision — the mixed track hears whispers, and a flag can silence it.**
Default: a whisper and a barge are audible on `mixed`, private playback
(`mix_target=own`, the item-37 behavior) is not. The mixed track is the monitor
**and** the recording feed, and a recording that omits what the agent was told
mid-call is a recording that lies about the call; audit beats privacy here
because a whisper is a human speaking into a live conversation. Deployments that
disagree set `mix_monitor=exclude` (and `include` forces the other direction on
private playback). Both are documented reserved keys in
`session-core/src/mix.rs`, parsed in exactly one place.

Events: `MediaEvent.mix_routed` (oneof tag 25, `MixRouted{mix_target,
monitor_audible}`) is published on the attach that declares a route and on every
change, so `mss.events` can answer *who whispered to whom, and was it on the
record*. A pause/resume that leaves the route alone says nothing.

Membership churn was the sharp edge (item 37's warning): `join_listener` resets
its column, so `conference.rs::reroute_injectors` re-resolves **every** route by
name after every join and leave. Two consequences fell out of that and are now
tested: a whisper to a member who has not joined yet is **muted, not
broadcast**, and it starts being heard the moment that member joins.

New metrics: `mss_conference_whispers_live` (gauge) and
`mss_conference_route_changes_total`.

Verified in-process over real UDP sockets (5 new tests in `tap_plane.rs`, 3 in
`session-core`, 6 in `mix.rs`): whisperer injecting 8000 into a three-leg
conference — target reads **~8000**, the other two and the injecting leg itself
read **< 300**, and the `mixed` track carries it; `mix_monitor=exclude` keeps
the target's ear and empties the mixed track; the flip to `all` puts it in all
three ears; a fourth leg joining mid-whisper does not disturb the route; and
detaching the whisperer puts the leg's injected audio back to private playback.

**Residuals.** (a) `all` is audible to the injecting leg too (the injector has
no minus-self link) — a human barging through their own leg hears themselves;
inject on a dedicated silent leg until P4-5 decides whether to add a
minus-self variant. (b) The route belongs to the **leg**, not the attachment
(one injector per leg): two INJECT attachments on one leg share it, last
writer wins, and detaching the owner reverts it to private. (c) A whisper
sourced from a member's **own RTP** rather than an injected stream (the
`mix_source=leg` shape) is not built — that is the same row/column verb family
as P4-5's mute/deaf/hold and belongs there. (d) No real SIP peer has whispered
yet: P4-6. (e) The reference deployment's monitor/whisper/barge RPC mapping is
**not** here on purpose — it goes in P4-5's adapter parity table.

### 39. Native conference recording — both shapes at once (Phase 4) — ✅ DONE (2026-08-24)

A conference records two ways, and both may run on the same conference at the
same time. Neither needed a new RPC, a new transport or a change to the frozen
identity `${accountID}/${recordingID}.${format}`.

**The room, as one object.** `Attach{transport=FILE_S3, selector.only="mixed",
endpoint="<account>/<recording>.wav"}` on **any** member session records the
whole conference as one **mono** object. The conference already publishes its
full sum to every member's hub as `mixed` (item 37), so the recorder is an
ordinary hub consumer of one track and `Layout::Mono(Track::Mixed)` renders it;
pause excises the paused span from that single object, and a spilled segment
(item 30) stays mono and stitches back in order.

**Every participant, one object each.** A recording **group** over the member
sessions with `selector.only="customer"` writes one mono object per member under
`<account>/<recording>/<label>.wav`, time-aligned on the group's `opened_at`
(item 29's anchor), so a member that joins after the recording started opens its
file with silence back to t=0 rather than at its own join moment.

**The clock was the one real defect.** A member's own audio is published on its
hub with the **leg's** frame counter (0 at its join) while the mix was published
with the **conference's** (0 at the conference's open). On any member that joined
late the two tracks were therefore offset by the join delay — measured at
**5120 samples (640 ms)** in the new test before the fix — which made a stereo
`selector=all` object of a conference member ("me left, the room right")
misaligned and any timestamp correlation across the two tracks wrong.
`conference.rs` now stamps each member's `seated_at_frame` and publishes the mix
to that member's hub on **that member's** clock. Mono shapes were unaffected
(the segmenter anchors on the first timestamp it sees), so this is a fix for the
stereo and correlation cases.

**Decision — a recording group of the mixed track is refused on a conference.**
Every member's `mixed` track carries the same audio, so a group of them would
write N byte-identical objects under one prefix. `open_recording_attachment`
refuses it by name and names the two supported shapes in the error. On a
**non**-conference session `only=mixed` is still the injected/playback track and
a group of those is legitimate, so the refusal is scoped to a session that is
seated in a conference (`conference_of`).

**Decision — the event names the shape.** `RecordingStarted` gained
`string shape = 3` (additive, wire-compatible; next free field on that message
is 4), emitted per object:
`stereo` | `track` | `mixed` | `participant`, prefixed `conference-` when the
session is a conference member. So `mss.events` distinguishes a room object
(`conference-mixed`) from a participant object (`conference-participant`) from
an ordinary two-party recording (`stereo`) without the consumer having to parse
the object key or know the session's group. `RecordingShape::of(layout, grouped)
.named(conferenced)` is the only place that string is built.

**Verified** over in-process UDP sockets and replay, no live run: four new
`recorder.rs` tests (a mixed-only object is mono, carries only the mix and keeps
the frozen key; pause excises the room and leaves no gap; a spilled mixed
segment stays mono and stitches; the shape vocabulary) and three new
`tap_plane.rs` tests (one conference recording the room **and** three
participants at once — the room object reads 6500–7600 for 1000+2000+4000 while
each participant object reads only its own tone, and the late member's object
opens with a pad and matches the others' length; the late member's own track and
the room within 4 frames of each other in a stereo object; the grouped-mixed
refusal). **Item 41 then ran it live** — three container RTP peers in one
conference, both recording shapes landing in MinIO at once. What has still never
been through this path is a real **SIP** peer.

**Residuals.** (a) The room object is attached to **one member's** session, so
it ends when that member leaves even though the conference lives on — D20.
(b) D16 still applies: a recording group is one pod's memory, so every member of
a per-participant conference recording must be on the same pod. (c) The room
object's own t=0 is its attach moment; the shapes align with each other only if
they are attached together (there is no conference-wide recording anchor).
(d) No AGC: the room object clips exactly when the mix clips
(`mss_conference_clipped_samples_total`).


### 40. The conference feature tail + the parity table (Phase 4) — ✅ DONE (2026-08-24)

Member **mute / deaf / hold**, **prompts into the room**, and a whisper sourced
from a member's **own RTP** — the residuals items 37–39 left. Still **no new
RPC**: the vehicles are item 38's `UpdateAttachmentRequest.metadata` and
`StartPlayback.target_tag`. The **generic conference feature list** (what exists,
what is deliberately absent and why) and the clearly-marked **ADAPTER parity
table** against one integrator's 14 conference RPCs are in
**architecture.md Appendix B**.

**Member verbs are member state, and that is the design decision here.**
`member_mute` / `member_deaf` / `member_hold`, each `on` or `off`, ride on **any
attachment of that member's own session** (an absent key means untouched, per
the merge semantics of the metadata channel). Unlike `mix_target`, which belongs
to the whisperer and reverts when it detaches, member state **outlives the
attachment that set it** — muting somebody is not a property of the consumer
that asked for it. No capability is required: the API caller's authentication is
the authorization, exactly like `paused`. `MemberControlled{mute,deaf,hold}`
(payload tag 26, additive; **next free payload tag is 27**) publishes each change
to `mss.events`.

- **mute** zeroes the member's contributor row, monitor cell included, so a
  muted member is out of every ear **and** off the mixed track, which is the
  recording feed.
- **deaf** silences the room into that member's ear — but not audio *addressed*
  to them. `apply_matrix` remembers which contributors were routed at a listener
  with `route_only` (their own private injector, a whisper named at them) and
  zeroes everything else into that ear, prompts and barge included. Blanket
  `deafen_listener` was **not** used: it would close the member's own injector
  and there would be no way to play hold audio.
- **hold** is both, which is why hold audio needed no new path: it is an
  ordinary `StartPlayback` on that member's session.

**Prompt into the room.** A conference owns one prompt contributor fed by one
bounded queue, routed to everybody, so a prompt is in every ear and on the
mixed track. `StartPlayback` with `target_tag="all"` on any member session plays
it; `StopPlayback` with `"all"` flushes it; empty or `own` is the unchanged
private path, and **any other value is now refused by name on an inline leg**
(an inline leg has no SIP from-tag to target). Two overlapping prompts **queue**
rather than mix, and long-form room audio belongs on an INJECT attachment with
`mix_target=all`.

**Decision — enter/exit prompts are a verb, not a trigger.** MSS does not decide
that a join deserves a beep. Play-into-room is the verb; join-triggering is
integrator policy (watch the room's session events, call
`StartPlayback{target_tag="all"}`), which keeps prompt selection, tenant policy
and localization out of the media plane.

**Decision — conference control is API-first, so there is no in-band DTMF menu.**
MSS delivers digits to consumers (WS `dtmf` frames, gRPC `DtmfFrame`) and
interprets none of them; an integrator that wants `*6` to mute maps the digit to
an `UpdateAttachment` call. Gap found while documenting it: those digits never
reached `mss.events` — D21, closed by item 48; the mapping is still the
integrator's, but it can now be driven from the bus instead of from a media
stream.

**`mix_source=leg`.** A `mix_target` now picks its source: `inject` (default,
item 38) or `leg`, the member's own RTP. `leg` + a member target is the **coach**
shape — the coach's own voice in one ear, the room no longer hearing them,
`mix_monitor` still deciding whether the coaching is recorded (default: include).
The coach's injector stays private, so private playback into their own ear still
works. A leg route still requires an INJECT attachment, on the rule that moving
audio around a room is the inject right even when nothing is injected.

`reroute_injectors` became `apply_matrix`: the **single writer** of every
non-default matrix cell, re-derived from the seated members on every membership
change, route change and member verb (item 37's `join_listener` resets a whole
column, so nothing may be applied once and forgotten). The deaf pass runs last,
after every row-based operation. New metrics:
`mss_conference_member_controls_total`, `mss_conference_prompt_frames_total`,
`mss_conference_{muted,deaf,held}_members`. `mss_ctl` gained
`member <attachment-id> <mute|deaf|hold> <on|off> ...`, `mix` gained the
`inject|leg` qualifier, and `play <session> <wav> all` is the room prompt.

**Verified** over in-process UDP sockets and replay, **no live run** (the three
container peers are P4-6): a muted member inaudible to both other members and on
the mixed track, back on `off`; a deaf member's ear silent while the room and the
record still carry her; a held member hearing his hold audio alone at full level
while the room loses him and the record never carries the hold audio; a room
prompt heard by all three members and the mixed track and silenced by
`StopPlayback{all}`; a coach heard by the agent, never by the customer, still
hearing everybody; and the refusals (a flag that is not `on`/`off`, a member verb
off a conference, a from-tag-shaped playback target on an inline leg). Plus the
mix-metadata parser tests and a registry audit test for `MemberControlled`.

**Residuals.** (a) Member state has no owner — D22; since item 49
`DescribeSession` reads it back on the member's own session, and since item 56
`member_state_ttl_ms` bounds how long it holds without a refresh, but nothing
decides whose it is. (b) One prompt source per room: overlapping prompts queue.
(c) Enter/exit sounds and DTMF menus are integrator work by design; since item 48
the digits themselves are on the bus (D21), but nothing in MSS interprets them. (d) Per-member volume/energy, member
enumeration, room lock and moderator roles are not built (architecture.md
Appendix B lists them with recommendations). (e) Everything here is pod-local,
like the conference itself.


### 41. The conference lab drill — Phase 4 on real sockets (Phase 4) — ✅ DONE (2026-08-24)

`lab/conference_drill.sh`: three `lab/inline_peer.py` containers at
440 / 880 / 1320 Hz seated in **one conference** (`mss_ctl inline <id> <call>
<offer> <group>`), with **no FreeSWITCH and no rtpengine in the path** — an
inline leg is MSS's own socket. New lab pieces: `lab/conference_actor.py` (one
file, two roles: `ROLE=monitor` attaches `SINK` + `only=mixed`; `ROLE=injector`
attaches `SINK`+`INJECT` with `mix_target=<member>`, then flips itself to
`mix_target=all` over `UpdateAttachment` — the barge) and
`lab/conference_report.py` (`ears` / `wavs` / `lengths`). `inline_peer.py`
gained `EAR_TONES` (a Goertzel **per tone per arriving packet**, stamped with
its arrival wall clock) and `TONE_AMPLITUDE`.

**Everything below is machine-decided**: the drill stamps phase boundaries on
the same kernel clock the peers stamp arrivals with (both are containers on the
lab network), trims 400 ms off each edge, and judges each tone present or absent
against the loudest tone in that same ear during that same phase. **Twenty
expectations, all green, present ~3000 against absent 36–98 — a ≥30:1 margin.**
Full tables in [lab.md](lab.md).

- **minus-self**: no ear ever carried its own tone (`three/A`: 880 = 2996,
  1320 = 3003, own 440 = 98).
- **monitor** (`only=mixed`, needing no code since item 37): all three tones.
- **whisper** (`mix_target=<member B>` at a 4th tone, 1760 Hz): B = 2989,
  **A = 63, C = 56** — isolation, not attenuation.
- **barge** (`mix_target=all`): 1760 in every ear, the injecting leg's included
  (item 38's known residual, now observed).
- **mute** (`member_mute=on` on one of A's attachments): 440 leaves both other
  ears *and* the mixed track (53 / 69 / 98); `off` restores it.
- **both recording shapes at once**: `acct-conf/room-<stamp>.wav` 71.88 s
  carrying all three tones, and a group's `party-<stamp>/{a,b,c}.wav` at
  71.96 / 72.02 / 72.10 s carrying **only** their own tone (cross-talk 0–8
  against 3000), with C's file opening on **10.66 s** of the P2-1 anchor pad.
  The three group files agree to 140 ms (sequential detaches, each of which
  waited for its upload before item 50 closed D11).
- `mss_conference_clipped_samples_total` = **0**, deliberately: all four sources
  run at `TONE_AMPLITUDE=6000`. At the peers' default 24000 the mix clips, and
  clipping intermodulates onto exactly the harmonics being measured — the
  drill's absent-tone assertions would have been reading their own distortion.

**Found and fixed here (D23):** `Segmenter::close_segment` advanced `anchor_ms`
by the whole closed segment while *also* subtracting it from `segment_start`,
double-counting the group anchor pad — the first run's `party-c.wav` was
**55.88 s against a/b's 66.16/66.24**, pad at the front and 10.28 s missing off
the tail. Ungrouped recordings have `segment_start == 0` and never saw it;
item 29's drill is too short to spill. See the defect row.

**What it does not prove:** no SIP, no rtpengine, no human; one pod (D16, D22);
the monitor and the room object both hang off one member's session (D20);
`deaf` and `hold` have item 40's socket tests only; PCMU 8 kHz/20 ms only.


### 42. SIGTERM: drain on the signal Kubernetes actually sends (G1) — ✅ DONE (2026-08-26)

**The bug this closes.** `main.rs` waited on `tokio::signal::ctrl_c()` only, so
the only signal that started a shutdown was **SIGINT**. Kubernetes sends
**SIGTERM**, and mediaserverd runs as PID 1 in its container, where an unhandled
signal is *ignored* — so every rollout, scale-down and eviction ended in SIGKILL
after `terminationGracePeriodSeconds`, exit 137: no lease release, no consumer
stop frame, no recording upload, no unsubscribe. That is the pod-loss path of
item 11 (a **14.41 s** consumer gap, plus the D14 orphan subscription) running on
*every planned* restart.

**What landed.** A new `crates/mediaserverd/src/drain.rs`:

- `next_shutdown_signal()` selects over `ctrl_c()` and
  `SignalKind::terminate()`, and names which arrived; both the control-plane and
  the idle mode use it.
- `DrainState` is the readiness `AtomicBool` (**G4 wires `/readyz` to it**),
  exported now as the gauge `mss_draining`.
- `run_drain(steps, budget)` runs the sequence against a `DrainSteps` trait, so
  it is unit-testable with fakes, and bounds every step against one deadline:
  **`MSS_DRAIN_TIMEOUT_SECS` (default 30)**. `exit_on_second_signal()` makes a
  second signal an immediate `exit(0)`.

The sequence, and who does the work:

| Step | What it does |
| --- | --- |
| `stop-accepting` | `DrainState::begin()` (readiness off) + `SessionController::begin_drain()`: `CreateSession` and `Attach` answer `UNAVAILABLE: this pod is draining`, `MediaStream` refuses new streams and sends `Stop` to live ones, `WatchEvents` ends, and the tonic listener stops accepting |
| `hand-off-leases` | aborts the keeper's renew task, then `RegistryKeeper::hand_off_leases()` → the new `SessionStore::release_lease` (DEL the lease key, **keep** the session record) so an adopter takes the call on its next sweep instead of after the 15 s TTL. Counter `mss_registry_handed_off_total` |
| `close-sessions` | `destroy_session` per live session, which is the existing polite path: consumers get their WS `stop` frame / gRPC `Stop` (D3), recordings are finished — uploaded, or spilled for the next boot's salvage (D9) — and the tap is unsubscribed |
| `control-plane-idle` | awaits the tonic server task, so in-flight RPCs and streams end before the process does |
| `flush-events` | the existing 10 s `await_empty_backlog` window on the Kafka pump |

**Measured live (2026-08-26), `lab/drain_drill.sh`** — a real MicroSIP call
through OpenSIPS/rtpengine/FreeSWITCH, tapped by pod A with a WS consumer, pod B
idle on the same Redis, then `docker stop -t 60` (SIGTERM; `-t 60` because
docker's default 10 s is shorter than the drain window). Pod A's PID 1 is
`mediaserverd` itself — `cargo run` exec-replaces itself — which is what makes
the measurement about the daemon and not about cargo; the drill asserts it.

```
08:27:33.448 INFO shutdown signal received; draining the control plane signal=SIGTERM live_taps=1 drain_budget_secs=30
08:27:33.448 INFO readiness is off and no new session or attachment will be taken here live_taps=1
08:27:33.449 INFO drain step finished step=stop-accepting elapsed_ms=0
08:27:33.450 INFO lease released for adoption ... external_id=draindrill-1787732824 owner=lab-control
08:27:33.450 INFO drain step finished step=hand-off-leases elapsed_ms=1
08:27:33.450 INFO the consumer websocket was closed after its stop frame media_sent=2022
08:27:33.452 INFO tap leg finished track=Customer datagrams=1007 jitter_lost=0 recv_errors=0
08:27:33.452 INFO tap leg finished track=Agent datagrams=1017 jitter_lost=0 recv_errors=0
08:27:33.452 INFO session closed for shutdown: consumers stopped, recording finished, tap unsubscribed
08:27:33.452 INFO drain step finished step=close-sessions elapsed_ms=2
08:27:33.452 INFO the control plane listener closed
08:27:33.452 INFO drain step finished step=control-plane-idle elapsed_ms=0
08:27:33.478 INFO drain step finished step=flush-events elapsed_ms=25
08:27:33.478 INFO drain complete elapsed_ms=30 leases_handed_off=1 sessions_closed=1 unsent_events=0
08:27:33.478 INFO session registry totals at shutdown persisted=4 handed_off=1 lost=0 failed=0
08:27:33.479 INFO event bus totals at shutdown published=3 failed=0 dropped=0 unsent=0
08:27:33.483 INFO mediaserverd stopped
```

- **exit code 0**, `docker stop` returned in **0.44 s** — the whole drain took
  **30 ms** of its 30 s budget, so the budget is a ceiling, not a cost.
- **pod B adopted 2.7 s after the signal** (2.2 s after pod A exited) and
  re-dialed the same WS endpoint; the lease moved `lab-control` →
  `lab-control-b`.
- the consumer's **audio gap was 1.96 s**, against **14.41 s** for the same
  drill's SIGKILL sibling (item 11) — the same instrument, `lab/gap_consumer.py`,
  and one artifact spanning the handover (2022 frames, then 12752).

**Found and fixed on the way.** `SessionController::begin_drain` used
`watch::Sender::send`, which is a **no-op when no receiver is alive** — a pod
with no active `MediaStream` could be told to drain and stay `draining=false`.
Now `send_replace`, which always updates. Caught by the new control-api test,
not by the lab.

**Decisions** (defaults preserve today's behavior exactly):

- **Leases are handed off before taps are unsubscribed.** That allows a brief
  double subscription (the adopter subscribes with its own to-tag while ours is
  still up) and rules out a *gap*, which is the worse of the two; our
  unsubscribe names our own to-tag, so it cannot disturb the adopter's.
- **Budget shares:** lease hand-off is capped at `budget/4` and the event flush
  keeps a reserve of `min(10 s, budget/3)`, so a hung Redis cannot eat the window
  that closes consumers and finishes recordings. Unit-tested with fakes.
- **Exit is always 0**, including when the window expires and when a second
  signal cuts the drain short: a rollout must not read a shutdown as a crash
  loop, and an unfinished recording upload is already covered by the spill +
  next-boot salvage (D9).
- **Spilling is not its own step.** `close-sessions` finishes each recording,
  which uploads or spills on failure; a separate "spill everything" step would
  duplicate `recorder.rs`'s own fallback.
- `MSS_DRAIN_TIMEOUT_SECS=0` is legal and means "stop accepting and exit".

**What it does not prove:** one pod stopped, not a rolling replace of many; no
Kubernetes (the `terminationGracePeriodSeconds >= MSS_DRAIN_TIMEOUT_SECS`
manifest is G6's); the timeout-expiry branches are fake-verified only, since the
live drain finished in 30 ms.

### 43. Media port range + advertised address (G2 + G3) — ✅ DONE (2026-08-26)

**The bug this closes.** Every media socket bound `local_media_address:0` — an
ephemeral port from the kernel's whole range. No firewall can be written for
that: the operator must open UDP from the rtpengine hosts to the port MSS
receives the tap copy on, and MSS could not say which ports those would be.
The same code put the *bind* address into every SDP it hands a peer, so a pod
that binds a private address and is reached on a different one (NAT, a
hostNetwork node with a routed VIP, a cloud load balancer) told rtpengine to
send audio to an address that does not route back.

**What landed.** A new `crates/mediaserverd/src/media_ports.rs`:

- `MediaPortAllocator` — a free list of **even ports only** over
  `[MSS_MEDIA_PORT_MIN, MSS_MEDIA_PORT_MAX]`. `bind(local_ip)` returns the
  socket, its port and a `PortLease`; the lease's `Drop` returns the port. A
  port another process already holds is skipped and counted, not fatal.
  Exhaustion is a refused session with the range named in the error.
- `MSS_MEDIA_ADVERTISE_IP` — `media_ports::advertise_address()`. The tap's
  subscribe answer and the inline SDP answer are now built by
  `tap_answer_sdp` / `inline_answer_sdp` from the **advertised** address, while
  the socket still binds the **local** one. Symmetric RTP is unchanged: inline
  egress still sends from the receive socket.
- Metrics: `mss_media_ports_in_use`, `mss_media_ports_free`,
  `mss_media_ports_capacity` (gauges), `mss_media_ports_exhausted_total`,
  `mss_media_ports_bind_conflicts_total` (counters).

All three variables default to today's behavior exactly: no range means
ephemeral binds, no advertise IP means the bind address is advertised.

**Measured live (2026-08-26), `lab/media_port_drill.sh`** — a real MicroSIP call
through OpenSIPS/rtpengine/FreeSWITCH, tapped by a pod started with
`MSS_MEDIA_PORT_MIN=40100 MSS_MEDIA_PORT_MAX=40139` (20 RTP sockets) and
`MSS_MEDIA_ADVERTISE_IP=172.31.99.31`:

```
drill: the pod reports capacity=20 free=20 in_use=0
drill: udp sockets inside the range before the call: [none]
drill: udp sockets inside the range while tapping: [40100 40102]
drill: udp sockets outside the range (ng control sockets, by design): [45745 46345]
drill: in_use=2 free=18 exhausted=0 conflicts=0
drill: ingest datagrams 0 -> 1503 over 15s
drill: udp sockets inside the range after the session closed: [none]
drill: in_use=0 free=20
drill: PASS
```

Two tapped legs took the first two **even** ports; audio flowed at the expected
~50 pkt/s per leg; both ports came back to the range when the session was
destroyed. `ss` is not in the lab's rust image, so the drill reads
`/proc/net/udp` inside the container — which is also why the NG control sockets
are visible and worth stating: they are **outside** the range on purpose.

**Decisions** (defaults preserve today's behavior exactly):

- **The allocator lives in `mediaserverd`, not `media-core`.** It is I/O-adjacent
  bookkeeping (it binds sockets), and `media-core` stays sans-IO.
- **Even ports only.** The odd successor of every allocated port is never handed
  out, so a peer that wants RTCP on `port+1` can have it without a second
  allocator. A 40-port range therefore serves **20** sockets, and the
  `capacity` gauge says so rather than leaving the operator to divide.
- **The NG control socket keeps an ephemeral port.** The range exists so a
  firewall can admit *inbound* media from the rtpengine hosts; the NG socket is
  an outbound control flow to rtpengine's 22222, and spending range ports on it
  would halve the tap capacity for no gain.
- **A port held by another process is skipped, up to 64 tries per bind**, and
  counted in `mss_media_ports_bind_conflicts_total`. Refusing the session
  because one port in the range is squatted would be worse than moving on.
- **Ports are returned on `Drop`, after the capture thread joins.**
  `close_session` joins the media thread, then releases the leases and logs
  `released_ports`, so a port is never re-handed-out while a socket still holds
  it. Drain closes sessions the same way, so a drained pod frees its range.
- **An empty string means unset** for all three variables (and now for
  `MSS_DRAIN_TIMEOUT_SECS` too): a compose/Kubernetes passthrough of an unset
  variable arrives as `""`, and that must mean the default, not a warning.

**What it does not prove:** the advertised address is the same as the bind
address in the lab, because rtpengine sends the tap copy to whatever MSS
advertised — a genuinely different address is proved only by the SDP tests
(`a_tap_answer_carries_the_advertised_address_not_the_bind_address`,
`an_inline_answer_carries_the_advertised_address_while_the_socket_binds_locally`)
and needs a NAT/hostNetwork deployment to see live. Exhaustion is unit- and
in-process-verified (an inline session refused on a one-port range, then served
after a close), never hit live. No hostPort/hostNetwork manifest yet — that is
G6.

### 44. Liveness and readiness probes (G4) — ✅ DONE (2026-08-26)

**The gap this closes.** The metrics listener answered `/metrics` (and, in fact,
*any* path) and nothing else. A Kubernetes Deployment therefore had no probe to
point at: no liveness endpoint, and no readiness endpoint — so a pod took traffic
while its rtpengine node was unreachable, while Redis or Kafka were down, and
**for the whole drain**, because nothing outside the process could see the
`mss_draining` gauge flip. Item 23 also left the rtpengine capability probe as a
one-shot at startup: a node that restarted kept its stale verdict for the life of
the pod.

**What landed.** A new `crates/mediaserverd/src/health.rs` plus two routes on the
existing dependency-free listener (`metrics.rs`):

- `GET /healthz` → **200 `alive`** while the process runs. It never consults a
  dependency: liveness must not restart a pod for someone else's outage.
- `GET /readyz` → **200** only when the pod is not draining *and* every
  configured dependency answered its last probe; **503** otherwise, with a
  plain-text body whose first line names the reasons and whose remaining lines
  report each dependency. An **unconfigured** dependency (no `MSS_RTPENGINE_NODE`,
  no `MSS_REDIS_URL`, no `MSS_KAFKA_BROKERS`) counts as ready and says
  `not configured`.
- `GET /metrics` unchanged, plus two new series: `mss_ready` and
  `mss_dependency_ready{dependency="rtpengine"|"redis"|"kafka"}` (configured
  dependencies only — an unconfigured one leaves its series out rather than
  lying).
- Any other path is now **404** and a non-GET is **405**; every lab script and
  the alert rules already ask for `/metrics` by name.
- `Readiness` is a cached snapshot behind one mutex: the request path reads state
  and **never** makes a network call. Background watchers (`health::watch`, one
  per dependency) do the probing — `PING` on the Redis connection
  (`SessionStore::ping`), a partition-offset fetch on the Kafka topic
  (`EventTransport::reachable`), and NG `ping` on the rtpengine node. Each waits
  `MSS_HEALTH_PROBE_INTERVAL_SECS` (default **10**) after a success and backs off
  1 s → 2 → 4 → 8 → interval while failing, so a restarted dependency is noticed
  fast; a probe that hangs is a failure after 15 s.
- The rtpengine watcher also calls `NodeCapabilityLog::forget` on failure, so the
  next success re-runs `report_first_contact` — **item 23's open re-probe**.
- Drain needs no new wiring: `DrainSteps::stop_accepting` already calls
  `DrainState::begin`, and the readiness verdict reads that flag, so `/readyz`
  turns 503 on the *first* step of the drain.

**Measured live in the lab (2026-08-26)**, pod recreated with
`MSS_HEALTH_PROBE_INTERVAL_SECS=5`:

```
$ curl -i 127.0.0.1:9464/healthz        -> HTTP/1.1 200 OK   "alive"
$ curl -i 127.0.0.1:9464/readyz         -> HTTP/1.1 200 OK
ready
rtpengine 172.31.99.10:22222: ready (last ok 0s ago)
redis: ready (last ok 0s ago)
kafka: ready (last ok 0s ago)
draining: no
$ curl -o /dev/null -w %{http_code} 127.0.0.1:9464/  -> 404      (POST /metrics -> 405)
mss_ready 1 / mss_dependency_ready{dependency="redis"} 1  in /metrics

$ docker stop mss-microsip-redis-1
/readyz -> 503 after 8 s:
not ready: redis unreachable: session store: timed out
rtpengine 172.31.99.10:22222: ready (last ok 0s ago)
redis: unreachable: session store: timed out (2 consecutive failures, last ok 10s ago)
kafka: ready (last ok 3s ago)
draining: no
   /healthz stayed 200 throughout; mss_ready 0, mss_dependency_ready{redis} 0
$ docker start mss-microsip-redis-1     -> /readyz 200 again after 3 s
   log: "this dependency answered again; readiness is back on"

$ docker restart mss-microsip-rtpengine-1
   "rtpengine node capabilities on first contact" logged 1 -> 2 times: the
   restarted node was re-probed and re-learned (item 23's gap)

$ docker stop -t 60 mss-microsip-mss-control-1   (SIGTERM), /readyz polled ~5 ms
t+0.028 s  200 ready
t+0.035 s  503 "not ready: draining ... draining: yes"
t+0.042 s  connection refused (listener gone), exit code 0 at t+0.404 s
```

**Decisions** (defaults preserve today's behavior):

- **A dependency nobody has probed yet is *not* ready.** A pod that has not yet
  learned its state must not take traffic. In practice the window is nil: Redis
  and Kafka are recorded ready at connect (they already refuse to start
  otherwise) and rtpengine at its first-contact ping, all before the listener
  binds.
- **Liveness is not "all my dependencies are up".** Restarting a pod because
  Redis is down would turn one outage into a crash-loop; `/healthz` is
  deliberately dumb.
- **Kafka's probe is a partition-offset read, not a produce.** A produce would
  put probe records on `mss.events`. A transport that cannot be probed is
  assumed reachable (`EventTransport::reachable` defaults to `Ok`), so the fakes
  in the tests keep meaning what they meant.
- **Readiness is one cached snapshot, never a network call in the request path.**
  A kubelet probe with a 1 s timeout must not be able to block on a hung Redis.
- **`MSS_RTPENGINE_NODE` empty or whitespace now means unset**, matching item
  43's rule, and a *malformed* value is a permanent 503 rather than a silent
  "no node configured".
- **The reason body is plain text, not JSON.** `kubectl describe` shows the
  first line of a failed probe's body, so that line carries the reasons.

**What it does not prove:** no Kubernetes — the endpoints were driven by `curl`
and `docker stop`, not by a kubelet (the manifests with `readinessProbe`/
`livenessProbe` wired to these paths are G6); the Kafka probe was never watched
failing live (Redpanda stayed up; the failure path is unit-tested only); and a
readiness flap under load has not been soaked.

### 45. Preflight — check a target environment before deploying (G5) — ✅ DONE (2026-08-26)

**The gap this closes.** Everything MSS needs from a deployment — an rtpengine
that supports `subscribe request`, a media range rtpengine can actually reach, a
Redis that expires keys, a Kafka topic, a writable bucket, a synchronised clock —
was discoverable only by deploying mediaserverd and reading its logs when a call
failed. The first session on real gear would have spent itself on someone else's
firewall.

**What landed.** `lab/preflight.sh`: POSIX sh front end (arg parsing, `MSS_*`
defaults) plus one python3 **standard-library** engine, because a jump host has no
`pip`. It calls `lab/kernel_probe.sh` for the kernel verdict and reuses the
bencode/NG patterns from `ng_probe.py` / `ng_subscribe_probe.py` / `call_driver.py`
rather than growing a second copy of them.

Thirteen lines, each `PASS`/`FAIL`/`SKIP` with one sentence of why; non-zero exit
on any `FAIL`; `--json` for a machine-readable report. The full table, both run
transcripts and every flag are in [deploy.md](deploy.md#preflight--check-the-environment-before-deploying-into-it).

- `ng_ping`, `ng_subscribe`, `ng_tap_media`, `ng_cleanup` — the tap handshake is
  performed, not inferred: the tool **fabricates its own throwaway call**
  (`offer`/`answer`, call-id `mss-preflight-<pid>-<epoch>`), subscribes to it,
  pumps ~1 s of PCMU and counts what comes back on the subscription socket, then
  `unsubscribe`/`delete` and a `query` that must answer *Unknown call-id*. Unique
  cookie prefix per run and per command (defect D12).
- `rtpengine_version` — SKIP by default, saying in one line that it **cannot be
  asked over NG** and naming the three places to read it on the host (H2 stays
  open, but is now self-explaining).
- `redis` — `SET NX EX 30` / `TTL` / `DEL` over raw RESP; that *is* the registry
  lease. `kafka` — TCP to each broker. `s3` — put/head/delete of a probe object
  with SigV4 signed in `hmac`/`hashlib`, then a HEAD proving it is gone.
- `media_ports` — the range's real capacity (even ports only) and a bind test.
  `media_udp` — with `--ssh <rtpengine-host>`, a datagram sent *from* rtpengine's
  host into the range. `clock` — chrony/timedatectl, 100 ms tolerance.

**Measured live in the lab (2026-08-26)**, run inside `mss-microsip_lab` from a
`python:3-slim` container because the NG port is not published to the WSL host:

```
green (exit 0):  10 PASS, 0 FAIL, 3 SKIP
   ng_tap_media  49 tapped RTP datagrams arrived at 172.31.99.2:36965 of 98 pumped
   s3            put/head/delete on MinIO via stdlib SigV4, HEAD -> 404 afterwards
   redis         SET NX / TTL 30s / DEL round-tripped
red (exit 1):    3 PASS, 3 FAIL, 6 SKIP  (--ng ...:22223, --bucket wrong-bucket-name)
   FAIL ng_ping            no reply in 3 tries over 6 s
   FAIL kernel_forwarding  kernel_probe.sh could not reach rtpengine
   FAIL s3                 PUT -> HTTP 404 NoSuchBucket
third run:       kafka_topic PASS (probe record produced to a topic and read back,
                 with kafka-python-ng installed) and media_udp PASS (datagram from
                 the "rtpengine host" arrived on 40100, via an ssh shim)
after all three: rtpengine answered "Unknown call-id" for every fabricated call-id
                 and the bucket held no probe object
```

**Decisions** (recorded here so nobody relitigates them from the code):

- **Userspace-only forwarding is a PASS**, not a FAIL. MSS taps a userspace relay
  just as well; the difference is rtpengine's CPU per call (architecture §8.1).
  `kernel_probe.sh` exit 2 ("cannot tell") is a SKIP, exit 3 (unreachable) a FAIL.
- **A SKIP always names the command that would answer the question.** "Probably
  fine" is not a preflight result; the summary line says every SKIP is an
  unchecked assumption.
- **The Kafka wire protocol is not hand-rolled.** `kafka_topic` produces and reads
  back only if a `kafka` client is importable; otherwise it SKIPs pointing at
  `rpk topic describe` on a broker host. A second implementation of what rskafka
  already does would be a liability, not a check.
- **S3 is SigV4 in the standard library**, ~60 lines, because it is the only way
  to check a bucket from a host with no boto3 and no CLI; `aws` is the fallback
  when no keys are passed (it can read a role or a profile) and the line always
  says which path was used.
- **The version check is advisory and `ng_subscribe` is the authority.** Doing the
  handshake beats reading a number, and rtpengine will not give the number anyway.
- **The tap check pumps real RTP.** A subscription that is accepted but delivers
  nothing is the exact failure a firewall or a wrong `MSS_MEDIA_ADVERTISE_IP`
  produces, and it is invisible to a handshake-only probe.

**What it does not prove:** no real deployment — every run was against the lab
(one rtpengine, MinIO, Redpanda, Redis on one Docker network); the `--ssh`
transport itself was exercised through a **shim** that ran the remote command
locally, so argument passing and the sender snippet are proven but a real
`ssh` hop is not; `clock` has never run anywhere with chrony present; the `aws`
and `mc` fallbacks are code-reviewed, not run; and a probe record on the
configured topic is the one side effect the tool cannot take back.

### 46. Deploy manifests + the operator guide (G6) — ✅ DONE (2026-08-26)

**The gap this closes.** Everything MSS needs to be deployed existed only as
knowledge in this repository's history: there were no manifests, and
`docs/deploy.md` was a stub holding the rows items 42–45 had added. The first
session on real gear would have written a Deployment from scratch and guessed at
the grace period, the port range, the advertised address and the probe paths —
the four things the previous four items had just made measurable.

**What landed.** `deploy/k8s/`, a Kustomize tree with no Helm and no templating
language, so what you read is what gets applied:

- `base/` — Deployment, ConfigMap (**every** non-secret `MSS_*` variable with its
  meaning), Secret **template** (every value the literal `REPLACE_ME`, behind a
  banner saying so, documenting the key names), two headless Services (gRPC needs
  client-side balancing or every session pins to one pod), PDB
  (`maxUnavailable: 1`), ServiceAccount (no RBAC — mediaserverd calls no
  Kubernetes API), ServiceMonitor and PrometheusRule.
- `overlays/hostport/` — pod network, an enumerated `hostPort` range,
  `MSS_MEDIA_ADVERTISE_IP` from `status.hostIP`, and a NetworkPolicy that is the
  firewall matrix in machine-readable form. `regenerate.sh <min> <max>` rewrites
  the port entries and the ConfigMap range **together**.
- `overlays/hostnetwork/` — `hostNetwork: true`,
  `dnsPolicy: ClusterFirstWithHostNet`, both `MSS_MEDIA_ADVERTISE_IP` and
  `MSS_TAP_LOCAL_IP` from `status.hostIP`, and a 1000-port range.
- `sync-alerts.sh` — wraps `deploy/prometheus-alerts.yaml` into the
  PrometheusRule, so the operator manifest and the plain-Prometheus rule file
  cannot drift. Verified: the generated `spec` is byte-identical to the alert
  document (16 rules in 4 groups).
- `validate.sh` + `validate_fields.py` — the check that runs without a cluster.

`docs/deploy.md` is now the whole guide: what MSS needs and never needs, the
manifests and the hostPort/hostNetwork trade-off table, **every** `MSS_*`
variable in six grouped tables (name / default / meaning / when to change / which
item added it) plus the lab-only spike variables and the warning that
`MSS_TAP_CALL_ID` silently turns the daemon into a one-shot tap, the port and
firewall matrix (including the two flows people forget: NG **egress** to 22222,
and the **outbound** dial to a `WS_TWILIO` consumer), media-range sizing, pod
sizing from architecture §8 with an explicit "these are estimates", the HA table
of what adopts and what does not, the drain sequence, alerts, the §8.1 kernel
pointer, the preflight, and a six-step "first day on real gear" runbook.

**Validation — no cluster was available, so three layers, honestly labelled:**

```
PASS  render      base (9 objects, kubectl kustomize v1.25.9 / kustomize v4.5.7)
PASS  render      overlays/hostport (10 objects)
PASS  render      overlays/hostnetwork (9 objects)
SKIP  schema      no kubeconform on PATH
PASS  fields      72 assertions over 3 rendered overlays
```

`kubectl apply --dry-run=client` is **not** one of the layers and never will be:
it fetches its schemas from a live API server, so with no cluster it fails with a
connection error and proves nothing. The 72 assertions were **negative-tested** —
shortening `terminationGracePeriodSeconds` to 20 and breaking the readiness path
made 6 of them fail with exit 1 — so the layer is known to bite.

**Decisions** (recorded here so nobody relitigates them from the YAML):

- **Both network shapes ship, neither is blessed.** The choice belongs to whoever
  owns the cluster, and the manifests state the cost of each: a `hostPort` cannot
  express a range, so a wide range costs one manifest entry per port and one pod
  per node; `hostNetwork` buys any range at the price of pod isolation and
  invisible port collisions. Both set the advertised address from `status.hostIP`
  through the downward API, because in both cases the address a peer must reach
  is the node's and only the node knows it.
- **No `preStop` hook.** The drain is signal-driven (item 42) and its *first* step
  flips `/readyz` to 503 — exactly what a `preStop: sleep` exists to emulate.
  Adding one would delay the SIGTERM and eat the grace period. Said in the
  manifest, not just here.
- **No CPU limit, and the reason is in the file.** The packet path runs on
  dedicated real-time threads; CFS throttling there is pacing jitter and consumer
  underruns, not a slow API. Requests carry a "MEASURE THIS" note pointing at
  architecture §8's estimate.
- **`emptyDir` for the spill dir, not a PVC.** It only has to survive a container
  restart: a cross-pod adopter cannot read another pod's disk either way (D9's
  residual), so a PVC would buy nothing and add a scheduling constraint.
- **Headless Services.** One long-lived HTTP/2 connection per gRPC client behind
  a ClusterIP would pin every session on that client to one pod. The file names
  the fallback for clients that cannot balance themselves.
- **The Secret is a template, and loudly.** Every value is `REPLACE_ME`, because
  the failure it prevents is real: the control plane would trust the token
  `REPLACE_ME` from any caller.
- **The alert rules are generated, not copied.** `deploy/prometheus-alerts.yaml`
  stays the source of truth and usable by a plain Prometheus; the diff
  `sync-alerts.sh` produces is the drift check.

**What it does not prove:** nothing here has been applied to a Kubernetes
cluster — not once. Every object is rendered and field-checked, and the values in
it are the ones measured live in the lab by items 42–45, but the manifests
themselves are unexercised: the probes have never been called by a kubelet, the
`hostPort` mapping has never carried a packet, `status.hostIP` has never been
substituted by a real API server, and no rollout has ever drained a pod under a
Deployment controller. Steps 1, 5 and 6 of the runbook are where that gets
found out.

### 47. Leg labels that do not depend on rtpengine's query order (G7, D17) — ✅ DONE (2026-08-26)

**The gap this closes.** With `from_tags` unspecified (`-`), `TapPlane` labelled
the two legs in the order `NgReply::tags()` returned them — and that order is a
`BTreeMap`'s, i.e. **lexicographic by tag string**. The two-node drill (item 24)
hit exactly that: FreeSWITCH's tag sorted first, so `customer` and `agent` were
swapped in the recording and in the `tracks` a consumer sees, silently, with no
signal to the integrator that the names were a coin flip.

**What we measured before writing any code** (`lab/ng_tag_created_probe.py`, new,
against the lab's rtpengine **14.1.1.8**; the fix's whole shape turns on it):

- a participant entry in a `query` reply carries exactly **two** scalar fields of
  its own: `tag` and **`created`** (integer, whole seconds). There is no
  microsecond field per tag — `created_ts` and `created_us` exist only at the
  **call** level, next to the top-level `created`;
- **`created` is stamped per dialogue, not per participant.** An offer and an
  answer **4 s apart** produced two tags with the *same* `created`
  (`1787737315`). A third and fourth leg offered/answered on the same call-id
  12 s later shared a *different* one (legA/legB `…383`, legC/legD `…395`). So
  creation time separates B2B dialogues on one call-id and **can never separate
  the two legs of one dialogue** — which is the only case leg labelling cares
  about;
- so "order the participants by creation time" — the shape D17 proposed, and what
  `call_watcher.py` was believed to do — is **not implementable** against this
  vendor. (`call_watcher.py` orders *calls* by `created`; for tags it uses the
  lab-only trick of recognising FreeSWITCH's media IP, and its own docstring says
  `created` "ties on a fast answer".)
- one lab gotcha found on the way, worth the line: rtpengine replays a **cached
  reply for a repeated cookie**, so a probe with a fixed cookie prefix reads the
  *previous* run's call. The probe now randomises its prefix (the D12 shape).

**What shipped.** `Attribution` (`session-core/src/attribution.rs`) —
`explicit | inferred | unknown` — threaded from resolution to every consumer-
facing name:

- `NgReply::tags_created()` exposes the per-tag `created`; the pure
  `order_participants()` in `tap_plane.rs` sorts by it (stable, so ties keep the
  reply's own order, and unstamped tags sort last) and returns `Inferred` **only**
  when the first two participants carry *strictly different* seconds — otherwise
  `Unknown`. It never guesses;
- `explicit` when the caller's from-tag was supplied (any non-empty `from_tags`),
  and for every INLINE session by construction — MSS answered that leg, so its
  own capture is unambiguous;
- under `unknown` the gRPC `StreamStart.tracks`, every gRPC media/DTMF frame's
  `track`, every event payload's `track` and the recording group's object keys
  are named **`leg_a` / `leg_b`** instead of `customer` / `agent`. `convert::track()`
  accepts the new names back, so a selector still round-trips;
- **the frozen WS Twilio vocabulary does not move** (Article VII):
  `inbound`/`outbound` regardless of attribution. A WS consumer's only warning
  that its names are a guess is the event below, which is why interactive
  attribution-sensitive work belongs on gRPC;
- the verdict is auditable three ways: `Session.attribution` on `DescribeSession`,
  a `string attribution` on **every** `MediaEvent` envelope, and a
  `LegsAttributed { attribution, tracks }` event emitted once per tap naming the
  track names a consumer will actually see. All three are additive proto fields;
- and the daemon logs it: INFO when the caller was named or creation times
  separated the legs, **WARN** naming the tags, their stamps and
  `callerFromTag` when they tie.

**Verified.** 14 new replay tests (three attribution states through
`order_participants`, the naming, the recording keys, the frozen WS names, the
registry write-back and event stamping) plus two over-the-wire tests on the real
gRPC surface. Then **live** (`lab/leg_attribution_drill.sh`, new), on a fabricated
two-leg call built to reproduce the inversion — caller `zz-caller`, callee
`aa-callee`, so the callee sorts **first**:

- rtpengine reported both legs with `created=1787738648` — identical, as the probe
  predicted;
- **from-tags `-`**: `DescribeSession` → `attribution: "unknown"`; the gRPC start
  frame → `tracks=["leg_a", "leg_b"]`; MinIO received
  `rec-…/alice.leg_a.wav` and `alice.leg_b.wav`; the WARN line above appeared with
  both stamps in it; `mss.events` carried the `LegsAttributed` record and every
  later event for that session was stamped `unknown`. The old code would have
  called `aa-callee` the customer — the drill shows the callee's DTMF digit `2`
  arriving on `leg_a`, honestly unnamed instead of falsely `customer`;
- **from-tags `zz-caller`**: `attribution: "explicit"`, `tracks=["customer",
  "agent"]`, objects `alice.customer.wav` / `alice.agent.wav`, and digit `1` on
  `customer` — correct.

**Decisions.** (1) Event payload track names follow attribution too, so nothing
in MSS's own vocabulary claims a direction the session cannot back up; the frozen
WS dialect and the legacy stream state machine's event names are unaffected (they carry no
track). (2) `Attribution` is **not** persisted in the session store: a session
created without from-tags is not rebuildable (`is_rebuildable()` requires them),
and one created with them re-derives `explicit` on the adopting pod. (3) The
`inferred` state is real code with a real test but is **unreachable for a
two-party call on this rtpengine version** — it can only fire where a call-id
carries participants from more than one dialogue. It is kept rather than
collapsed into `explicit`/`unknown` because the honest thing to record is that
the vendor, not MSS, is what makes creation order useless.

**What it does not prove:** nothing here recovers attribution from SIP. The only
way to get `customer`/`agent` on a tap is to pass the caller's from-tag —
`docs/deploy.md` now says so under "Leg attribution", and the integrator's
control plane is where that has to come from.

### 48. DTMF digits on the event bus (G8, D21) — ✅ DONE (2026-08-26)

**The gap this closes.** MSS decoded every RFC 4733 press (`DtmfDetector`,
counted in `mss_ingest_dtmf_digits_total`) and handed it to *consumers* — the
Twilio WS `dtmf` frame and the gRPC `DtmfFrame` — and to nobody else. `mss.events`
carried no digit, so the integrator who wants `*6` to mute (item 40's decision:
conference control is API-first, MSS interprets no digit) had to hold a media
stream open just to hear one, and a recording-only or bus-only integration could
not build a digit menu at all.

**Gating decision: session level, no capability, no consumer needed.** A digit is
a property of the **call**, not of a consumer: MSS decodes it from the call's own
RTP, so there is no attachment to authenticate and nothing for an EVENTS
capability to authorise. Digits therefore go through `Registry::observe` — the
session-level path that `RecordingStarted` and `LegsAttributed` already use — and
are published whenever the session exists, with `attachment: none`. This is
deliberately **unlike** `SpeechReport` (D19/item 28), which originates *from* a
consumer reporting inference it made and is gated on `CAPABILITY_EVENTS`: there
the attachment is the claimant and must be privileged to speak for the call. The
attachment-level EVENTS capability is irrelevant to a digit; an attachment with
it gets no extra digits, and a session with no attachment at all still gets them
all. (Proven both ways: `a_digit_is_published_with_no_consumer_attached_at_all`
in `registry.rs`, and the live drill below runs with no consumer attached.)

**What shipped.**

- `media_core::dtmf::DigitPress { digit, duration_ms, rtp_timestamp }` replaces
  the bare `char` in `IngestOutcome::Dtmf`. The **dedupe was already right** and
  is now guarded by its own test: the detector reports on the end bit and keys on
  `(digit, rtp_timestamp)`, so RFC 4733's three end retransmissions are one
  press. `duration_ms` converts the reported duration through the **negotiated
  RTP clock rate** (`PipelineConfig::clock_rate_hz`, per RFC 4733 §2.4.1 the
  event stream shares the audio clock), so an 800-tick press reads 100 ms at
  8 kHz and 16 ms at 48 kHz rather than being hardcoded to narrowband;
- `crates/mediaserverd/src/digits.rs` (new): a bounded `ArrayQueue<Digit>` + a
  `Notify`, the only bridge between the capture thread and the control plane. The
  media thread's `publish` never blocks, never allocates and counts a refusal
  (`mss_dtmf_events_dropped_total`) instead of waiting; a per-session Tokio task
  drains it and calls `observe`. Capacity 64, which at "digits are rare" is a
  drop that means something is wrong rather than a routine loss;
- every tap leg **and** every inline leg carries the sink, so a digit pressed into
  an inline (bot) leg is published too; `close_session` closes the queue, the
  publisher drains what is queued and ends;
- `Observation::Dtmf` / `EventKind::Dtmf` / `proto.Dtmf` gained `duration_ms` and
  `rtp_timestamp` (fields 3 and 4 — **additive**, the payload oneof tag stays 14).
  Track names honour item 47's attribution, so an unattributed session's digits
  arrive on `leg_a`/`leg_b`, never a guessed `customer`.

**Verified.** Replay/in-process: the detector's duration and clock-rate maths, the
queue (cross-thread order, a full queue counting instead of blocking, close
draining), the wire payload under `explicit` and `unknown` attribution, the
registry's no-attachment publish, and an **end-to-end** test that opens a real
inline leg on a UDP socket, sends one digit-start plus three end packets, and
asserts exactly one `Observation::Dtmf { customer, '1', 100 ms, ts 160 }` reached
the sink. Then **live** (`lab/dtmf_event_drill.sh`, new) on a fabricated tapped
call with **no consumer attached at all**, `lab/call_driver.py` pressing `1` on
the caller and `2` on the callee every 6 s and repeating each end packet three
times — `mss.events` carried, one per press:

```
payload=Some(Dtmf(Dtmf { track: "customer", digit: "1", duration_ms: 100, rtp_timestamp: 79200 }))
payload=Some(Dtmf(Dtmf { track: "agent",    digit: "2", duration_ms: 100, rtp_timestamp: 79200 }))
payload=Some(Dtmf(Dtmf { track: "customer", digit: "1", duration_ms: 100, rtp_timestamp: 127040 }))
payload=Some(Dtmf(Dtmf { track: "agent",    digit: "2", duration_ms: 100, rtp_timestamp: 127040 }))
```

`mss_dtmf_events_dropped_total 0`; `mss_ingest_dtmf_digits_total` leads the bus
count only because it keeps counting after the tail window closes. `seq` is
gapless and every record is `attachment: none`.

**What it does not do.** MSS still interprets no digit — no menu, no collection,
no inter-digit timer, no `#` terminator, and the frozen `firstDtmf`/`dtmfResult`
The legacy stream state machine's events remain a consumer-side concern. An integrator building a menu
consumes `mss.events` and calls the API, which is item 40's decision unchanged.
Nothing rate-limits digits: a stuck endpoint blasting end packets at distinct
timestamps would publish one event each, bounded only by the queue's drop
counter.

### 49. Member state read-back (G9, D22) — ✅ DONE (2026-08-26)

**The gap this closes.** Item 40 made `member_mute` / `member_deaf` /
`member_hold` deliberately outlive the attachment that set them, and then no API
reported them. A controller that muted a member and died left that member muted
with no way to find out: the only evidence was the aggregate gauges
(`mss_conference_{muted,deaf,held}_members`), which say *how many*, never *who*.
Nor could an integrator enumerate a room — the member list lived only inside the
mixing thread.

**Decision — no new RPC, and no new noun either.** `DescribeSession` on a member
session now answers with that member's own state and, in the same message, the
room it is seated in. There is no `DescribeConference`: a conference is not an
addressable object in this API, it is a name that member sessions share (item 37),
so the read-back follows the same rule as the write path — member verbs ride on
the member's own session, and so does the read.

**Decision — read the control-world mirror, never the mixing thread.**
`Conference` (the control-plane handle) now holds a `MirroredMember` per seated
session — external id, `mute`/`deaf`/`hold`, its `MixRoute` and the attachment
that asked for it — written by exactly the calls that enqueue the media-thread
command (`seat`, `route`, `control`, `unseat`). `Describe` reads that mirror under
the conference table's lock. The alternative, asking the mixing thread, would put
a request/response round trip into the packet path for a read that the control
world already knows the answer to; the mirror is also why a read-back is
immediate rather than eventually consistent with the command queue.

**What shipped.**

- `session_core::mix::MemberStateView` / `MemberRouteView`: the plane-agnostic
  shape (conference name, member external ids, the three flags, the mix source
  and the live routes);
- `MediaPlane::member_state(session)` — a **synchronous** trait method defaulting
  to `None`, like `inline_egress_sink`, so a media plane with no conferences is
  unchanged. `TapPlane` answers it from `conference_of` + the conference table;
  `SessionController::session_message` calls it **before** taking the registry
  lock, so the two locks are never nested;
- proto `Session.member = 13` and `Session.conference = 14` (**additive**; the
  next free `Session` field is 15, and no payload tag was taken — the next free
  `MediaEvent` payload tag is still 28), carrying the new `MemberState`,
  `MemberRoute` and `ConferenceView` messages. `mss_ctl describe` prints them
  already, since it renders the whole message.

**What a reader sees.** `member` is absent unless the session is seated in a
conference on this pod. `routes` is empty for a plain member — its injected audio
reaches its own ear only and the mixed track does not carry it — and holds one
entry the moment anything is routed, including `own` with
`mix_monitor=include`; each entry names the `target`, the `source`
(`inject`|`leg`), whether the recording feed carries it (`monitor_audible`) and
the `attachment_id` that owns it. Being whispered *at* is not a route of one's
own, so the addressee reports none. `conference` lists the room even after the
member it names has left, which is what makes a stale whisper auditable rather
than invisible.

**Verified over the wire** (`describing_a_member_reads_back_its_own_state_and_
enumerates_the_room` in `tap_plane.rs`): a real `TapPlane` behind a real
`SessionController` on a real TCP socket, driven by a generated
`MediaControlClient` — three inline legs seated in `sales-standup`, B muted
through `UpdateAttachment` metadata, A whispering to C through the same channel.
Describe on A reports the whisper (`target carol`, `source inject`,
`monitor_audible`, the owning attachment id) and enumerates
`[alice, bob, carol]`; Describe on B reports `mute` with no routes; Describe on C
reports neither. C is then destroyed: the room reads back as `[alice, bob]` with
`member_count 2`, A's route still names `carol`, and B is **still muted** — the
lease residual, demonstrated rather than described.

**Residual when this landed — no lease (the other half of D22).** Member state
had no owner and no expiry: nothing reclaimed a mute when the controller that set
it died. The read-back made that recoverable (an integrator can reconcile a room
on reconnect) but not automatic. **Item 56 closed it** without answering the
ownership question this item deferred: `member_state_ttl_ms` bounds how long a
flag holds without a refresh, and the control world lifts what runs out through
the same path an `off` takes. There is still no owner — that part of D22's
wording stays true and is now the whole of its residual.

### 50. Non-blocking StopRecording/Detach (G10, D11) — ✅ DONE (2026-08-26)

**The gap this closes.** `Detach`/`StopRecording` used to block for as long as
the upload took (bounded 60 s per object, 90 s overall), because the recorder
task did the upload before its `JoinHandle` resolved and the RPC awaited that
handle. The reason it was written that way is the real difficulty: `push_event`
takes an event's `seq` from the session record and **silently drops** an event
whose session is gone, so a backgrounded upload would lose `UploadCompleted` on
every hangup — which is every recording that ends with the call.

**Decision — keep the session record, do not reserve a seq.** Two designs were
weighed and the choice is recorded in `docs/implementation-notes.md`
("The gapless-sequence problem"). Reserving a terminal `seq` at detach keeps the
sequence gapless but **not monotonic**: when the session lives on past the
recording, later events take higher numbers and the reserved one lands after
them, so a consumer with a low-water mark stalls on a hole that is already
spoken for. Keeping the record alive in a `finishing` state costs one
`BTreeMap` entry per settling upload and keeps both properties. So:

- `SessionRecord` gained `pending_uploads` + `finishing`;
  `retain_for_upload`/`release_after_upload` bracket the background upload, and
  `destroy_session` marks the record `finishing` rather than removing it while
  an upload is still owed. The last release removes it.
- A finishing session is **not adoptable and not listable**: `session_ids()`,
  `session_count()` and therefore `snapshot()`, the registry keeper and the
  drain all skip it, and the new `live()` guard makes `attach`,
  `start_playback` and a second `destroy_session` refuse it by name.
- Its `external_index` entry **is** dropped at destroy, so the external id is
  free again immediately. The trade-off, deliberate and documented:
  `DescribeSession` by session id still answers a finishing session, by
  external id does not.
- **New rule for consumers of `mss.events`:** `UploadCompleted`/`UploadFailed`
  may arrive after `SessionEnded` for the same session, and is then the last
  event of that session's sequence.

**The recorder is now two phases.** Phase one is the capture loop; it publishes
`RecordingStopped`, sends a `StopReport` (duration, frames, segmenter stats)
down a oneshot and goes on. `RecorderHandle::finish()` awaits only that report,
bounded by the new `STOP_TIMEOUT` (5 s — a segment close, no I/O), and hands the
still-running task back as a `FinishedCapture`. Phase two takes an upload permit
from `MSS_RECORDING_UPLOAD_CONCURRENCY` (default **4**), renders, encodes,
uploads and publishes the result. `FINISH_TIMEOUT` (90 s) is now a test-only
helper; the production bounds are `STOP_TIMEOUT`, `UPLOAD_TIMEOUT` (60 s per
object) and `UPLOAD_SETTLE_TIMEOUT` (10 min, after which a stuck upload is
aborted, counted and its retention released rather than pinning a session
record forever — the audio is on the spill disk for D9's salvage).

**A failed upload now says so.** `UploadFailed { recording_id, key, error }`
(proto payload tag **28**, the next free one) is published for a refused upload,
a timed-out upload and a wav-encoding failure. Before this a recording that
never reached storage produced `RecordingStopped` and then silence, so an
integrator waiting for `UploadCompleted` waited forever.

**New: `recording_uploads.rs`.** `UploadTracker` retains the session, counts the
hand-off (`mss_recording_uploads_backgrounded_total`), holds the gauge
(`mss_recording_uploads_in_flight`), watches each upload and releases the
retention when it settles. `drain.rs` gained an **`await-uploads`** step between
`close-sessions` and `control-plane-idle` — an upload that settles after
`flush-events` would strand its own event in the outbox at exit.

**Verified — replay/in-process.** `a_stop_is_reported_before_the_upload_starts_and_the_upload_runs_on_alone`
(a 1.5 s fake sink: `finish()` returns in under 100 ms with the stop already
published and the upload not; `UploadCompleted` lands afterwards),
`background_uploads_run_no_wider_than_their_configured_concurrency` (one permit
serialises two uploads while both detaches return at once),
`a_refused_upload_still_reports_the_stop_and_keeps_the_audio_on_disk` (now
asserts `UploadFailed` with the store's message), two registry tests for the
sequence (`a_recording_upload_holds_a_session_record_open_so_its_own_event_keeps_the_sequence`,
`a_failed_upload_is_the_last_event_of_the_session_it_belonged_to`), the drain
step order, and **over the wire** in
`a_detach_is_answered_before_the_upload_and_the_upload_event_ends_the_sequence`
— a real `TapPlane` + `SessionController` on a socket, two recordings on one
session, `Detach` and `DestroySession` both answered in under 100 ms against a
sink that sleeps 1.5 s, and the eleven events read off the watcher come back
`seq 0..10` with both `UploadCompleted`s after `SessionEnded`.

**Verified — live lab** (`lab/detach_latency_drill.sh`, new, 2026-08-26):

| | phase A (MinIO healthy) | phase B (MinIO `docker pause`d) |
| --- | --- | --- |
| `Detach` RPC | **49 ms** | **11 ms** |
| `DestroySession` RPC | — | **10 ms** |
| upload took | 29 ms | **12.07 s** (11:51:25 → 11:51:37) |
| while it ran | — | `mss_recording_uploads_in_flight 1` with `mss_sessions_live 0` |
| terminal event | `UploadCompleted` seq **13** | `UploadCompleted` seq **10**, i.e. *after* `SessionEnded` seq 9 |
| sequence | gapless `0..14` | gapless `0..10` |
| object | 469 KiB | 252 KiB |

The pod's own words in phase B: `recording stopped; its upload runs in the
background` → `this session will be remembered until its recording upload
settles` → (12 s later) `recording uploaded` → `a backgrounded recording upload
settled` → `the last upload of this ended session settled; forgotten`. Both RPC
times include `mss_ctl`'s process start and gRPC connect, so the server-side
figure is smaller still; before this item the same detach would have returned
only after the upload, which phase B held for 12 s deliberately.

**Residual.** `UploadFailed` is proved in-process and replay-only — the live
drill made storage *slow*, not permanently broken, so no live `UploadFailed` was
observed. The retention is per-pod state: a pod killed with `-9` between the
detach and the upload still loses the event (D9's salvage recovers the audio on
that pod's next start, without an event). And `MSS_RECORDING_UPLOAD_CONCURRENCY`
bounds concurrency, not memory: N uploads in flight hold N rendered WAVs.

### 51. rtpengine node discovery — the optional Redis map (G11) — ✅ DONE (2026-08-26)

**The gap this closes.** `CreateSession` takes an `rtpengine_node`, and a caller
that knows it is fine. A caller that knows only the SIP Call-ID is not: it lands
on `MSS_RTPENGINE_NODE`, and with more than one rtpengine that is a guess.
`TelCompat` is exactly such a caller — `StartStream` carries `sipCallId` and no
node — so the façade that exists to need no changes in the legacy controller was the one surface
that could not pick a node. This is the discovery map decided in Phase 0
(architecture §4) and demoted to an optimisation on 2026-08-17, now built as
one.

**Decision — pull, not push, and nothing cached.** One Redis `GET` per
`CreateSession` that names no node, on the create path only. No cache, no
watcher, no invalidation: a call is created once, so a per-create read is the
same order of cost as the `query` MSS already does, and a stale cache is a class
of bug this cannot have. Resolution order is **request node → map → default
node**, and the map is only consulted when the request named nothing.

**Decision — plain `host:port` first, JSON when the proxy knows more.** The
minimum an integrator writes is `10.0.0.5:22222`. The richer form is
`{"node":"…","caller_tag":"…","from_tags":["…","…"]}`, and it earns two things:
both tags let MSS subscribe the tap **without a `query`**, and `caller_tag`
makes the attribution `explicit`. A tag list *without* `caller_tag` stays
**`unknown`** (tracks `leg_a`/`leg_b`, item 47's rule) — the map may not decide
who called by accident of ordering. That is the one D17 interaction, and it is
documented in `docs/deploy.md`.

**Decision — a lookup failure is never a session failure.** Redis unreachable, no
key, or a value that is not an address: WARN, count it, use the default node. The
create proceeds and fails or succeeds on its own merits. `MSS_DISCOVERY_REDIS_URL`
that cannot be connected at startup switches discovery **off** with a WARN rather
than refusing to start, unlike `MSS_REDIS_URL` (the registry is a promise of HA;
the map is an optimisation).

**What shipped.**

- `crates/mediaserverd/src/discovery.rs` — `parse_mapped_node` (pure, 6 tests),
  the `NodeMap` trait, `NodeDiscovery::resolve`, `DiscoveryCounters`. Redis
  access reuses `RedisSessionStore` through its new `read_key`, so there is one
  Redis client type in this daemon and one place that handles its connections;
- `TapPlane::discover_through` + `resolve_node`, which replaces the direct
  `node_for` call in `open_tap_session`. `node_for` still answers the two ends of
  the order (a named node, the default); the map sits between them.
  `complete_from_tags` now takes `caller_named` instead of inferring it from
  "from_tags is non-empty", which is what keeps a map-supplied tag list honest;
- env `MSS_DISCOVERY_REDIS_KEY_PREFIX` (unset = off) and
  `MSS_DISCOVERY_REDIS_URL` (unset = the registry's Redis), read and logged in
  `main.rs`, passed through all three lab `mss-control` pods;
- metrics `mss_discovery_hits_total` / `_misses_total` / `_errors_total`, exposed
  **only when discovery is configured** (an absent series beats a lying zero);
- the proxy side: `lab/opensips/opensips.cfg` gained a templated block, **off by
  default** (`__DISCOVERY__` → `off`, with `__DISCOVERY_PREFIX__`,
  `__DISCOVERY_TTL__`, `__DISCOVERY_NODE__` beside it), and `docs/deploy.md`
  carries the `cachedb_redis` `cache_store`/`cache_remove` snippet an integrator
  copies.

**A packaging finding worth keeping.** `opensips/opensips:3.4` ships
`cachedb_local.so` and `cachedb_sql.so` but **no `cachedb_redis.so`**, and
`apt.opensips.org` no longer publishes a 3.4 component for bullseye (only
3.6/4.0 and devel), so the module cannot be installed into that image. The lab
proxy therefore writes the map through `exec.so` + `lab/opensips/
discovery_publish.py` (a 90-line RESP client). The key and the value are
byte-identical to what the documented `cache_store` writes; only the writer
differs. Check `ls /usr/lib/x86_64-linux-gnu/opensips/modules | grep cachedb` on
your own image before copying the snippet.

**Verified — live lab** (`lab/node_discovery_drill.sh`, new, 2026-08-26). The
pod's default node was pointed at a **black hole** (`172.31.99.199:22222`) so
nothing but the map could work:

| | |
| --- | --- |
| the proxy published | `{"node":"172.31.99.10:22222","caller_tag":"hosttest","from_tags":["hosttest","y3HmFyeQae04N"]}` |
| `mss_ctl create <id> <call-id> -` (no node, no tags) | tapped **1192 datagrams in 12 s**, `attribution=explicit` |
| counters | `mss_discovery_hits_total 1`, misses 0, errors 0 |
| an unmapped call-id | one **miss**, then refused: `no reply from rtpengine at 172.31.99.199:22222 after 3 attempts` — the fallback, not a guess |
| the BYE | `cache_remove` equivalent ran; `GET` → `(nil)` |

Both legs came from the map, so that tap issued **no `query` at all** — the
optimisation the Phase-0 decision was after.

**Residual.** The resolved node is not written back into the session registry, so
a pod that adopts an orphaned session re-reads the map (fine while the call is up
and the key's TTL holds) and falls back to the default node if the map is gone by
then. A deployment whose calls outlive the key's TTL should either lengthen the
TTL or name the node on `CreateSession`. Nothing here is proved on a **real**
`cachedb_redis` proxy — the lab image cannot load the module — so the snippet in
`deploy.md` is documentation, not a tested artifact; the key/value it writes is
what was tested.

### 52. TLS options (G12) — ⏸ PARKED on branch `feat/tls-options` (2026-08-26)
Built and verified (tonic TLS + mutual TLS on the gRPC port, rskafka TLS and
SASL, a TLS-capable `redis` client, redacted Redis URLs in logs, the lab tools
following through `connect_endpoint`), but **not on `main`**: the first
deployment runs every MSS hop inside one Kubernetes cluster, so the decision
was to keep the plaintext surface and the smaller dependency tree. The branch
holds one commit (`6621c27`) with its own `tasks.md`/`implementation-notes.md`/
`deploy.md` write-up; rebase and merge it the day a hop leaves the cluster.
Until then the security posture is network policy, documented in
[deploy.md](deploy.md).

## Multi-pod recording and conference ownership — the last lab-closable defects

Items 53–57 close D9 (cross-pod half), D16, D20 and D22 and the one residual
from item 23. They are ordered so that each builds on the one before; run them
one per PR on `feat/multipod-recording`. **Read [CLAUDE.md](../CLAUDE.md),
[session-playbook.md](session-playbook.md) and the "road from here" rules above
first.** Every item ends with the five-command gate, an
[implementation-notes.md](implementation-notes.md) section, this file's item
and defect rows updated, and a commit whose message says what was measured
against a fake and what against real MinIO/Redis. Facts the code map established
on 2026-08-26 and that these items rely on:

- Inline sessions are **never adopted** (`PersistedSession::is_rebuildable`,
  `session_store.rs:74`), and conference members are inline sessions. So
  cross-pod recovery of a *conference* is not a goal of any item here; what
  must survive a pod loss is a **tapped** session's recording and its
  membership in a recording group.
- The spill journal (`recording_spill.rs`) already has a clean seam —
  `SegmentJournal::{open, append, read_back, discard}` plus
  `recording_spill::salvage` — and `RecordingSink` (`recorder.rs:528`) is the
  trait every fake implements (`MemorySink`, `BucketSink`, `NowhereSink`).
- The recording group lived only in `TapPlane::groups` (`tap_plane.rs:447`)
  with `opened_at: Instant`, and nothing about groups or conferences was in
  Redis. **Item 54 changed the group half of that**: the group is a record
  under `mss:group:…`, `opened_at` is a `SystemTime`, `registry_keeper::rebuild`
  no longer skips grouped attachments and `grouped_not_adopted` is gone.
  **Conferences are still nowhere in Redis, and item 55 left them there on
  purpose**: a room became a *session* (`kind=MIX`), not a shared record, because
  a mix thread cannot move between pods — a room session is pod-bound exactly
  like the inline legs it mixes.
- A `Conference` had **no open instant** before item 55; it now stamps
  `opened_at: Instant` (the mix thread's release epoch) and
  `opened_at_wall: SystemTime`, publishes the room hub on the **conference**
  clock and every member's `mixed` track on that member's own
  (`seated_at_frame`).
- `SESSION_KIND_MIX = 3` existed in `proto/mediacontrol.proto` and was refused
  by `TapPlane::open_session`; **item 55 made it the room session**
  (`open_room_session`), so that refusal is gone.
- Member verbs reach the mix through `MemberControl::from_metadata`
  (`session-core/src/mix.rs:162`) → `TapPlane::control_member`
  (`tap_plane.rs:1702`) → `Conference::control` (`conference.rs:221`), mirror
  first, then `ConferenceCommand::Control` to the owner thread. No timestamps
  anywhere in that state; the control world has no housekeeping tick (only
  `RegistryKeeper::run` and `health::watch` recur).

### 53. Spill to the recording bucket, so any pod can resume a recording (D9) — ✅ DONE (2026-08-26)

**What shipped.** The segment journal grew a backend seam and a second backend,
and nothing above it changed shape.

- `SpillStore` in `recording_spill.rs` — `write` / `read` / `read_manifest` /
  `list_manifests` / `remove` / `describe` — with `DiskSpill` (today's code
  extracted, byte-for-byte the same layout and the same `spawn_blocking` calls)
  and `ObjectSpill` over `RecordingSink`. A journal is named by the first
  target's object key, which is the identity the disk layout already used, so
  `SpillManifest` is unchanged and a journal written before this commit is still
  read back.
- `MSS_RECORDING_SPILL_TO=disk|s3` (default `disk`, so the default deployment
  behaves exactly as before; `disk` with no `MSS_RECORDING_SPILL_DIR` still means
  "no spill") and `MSS_RECORDING_SPILL_PREFIX` (default `_spill/`). Under `s3`
  the journal is `_spill/<first object key>/manifest.json` plus
  `<target index>-<seq>.pcm` beside it, in the recording bucket, written through
  the same sink the finished object goes to.
- `RecordingSink` gained `get`, `list` and `delete` beside `put`/`exists` (no new
  dependency — `object_store` 0.14 has all three), plus
  `UploadError::Missing` so a first-ever manifest read is an answer rather than a
  logged failure. Every fake implements them: `MemorySink` (recorder.rs),
  `BucketSink` / `NowhereSink` (tap_plane.rs).
- Metrics `mss_recording_spill_lost_ownership_total` and
  `mss_recording_spill_foreign_manifests`, in `metrics.rs` and in deploy.md's new
  "Recording spill series" table.
- `lab/pod_kill_drill.sh` gained `RECORD=1`: a `FILE_S3` attachment on the tapped
  call, and after the kill and the adopter's destroy it reads the object's size
  back out of MinIO, turns it into seconds and compares against the tapped
  length with an allowance of one spill interval plus the measured adoption gap
  — printed as a fourth assertion. The three lab pods gained
  `MSS_RECORDING_SPILL_TO` / `_PREFIX` / `_SECONDS` passthrough, and the drill
  **refuses `RECORD=1`** unless pod A's own log says the journal is in the
  bucket, because the lab mounts one `./out` into all three pods and a disk
  spill would otherwise look cross-pod when it is not.

**Decisions, as specified and as built.**

- **Ownership is in the manifest.** An adopter's `SegmentJournal::open` rewrites
  `owner` to itself and **writes the manifest at once** — the claim is a write in
  `open`, not a side effect of the first `append`, because the original pod's
  next `append` has to see it. Every `append` re-reads the manifest first; if
  `owner` is no longer this pod the append is refused, `surrendered()` goes true,
  `spill_closed_segment` counts `spill_lost_ownership`, drops the journal handle
  and keeps recording into memory. Dropping the handle also means the partitioned
  pod never `discard`s the adopted journal.
- **`PersistedRecording.owner` gates nothing any more.** It is still persisted
  and still handed to the adopter as `mss.recording.spillOwner`, but only as a
  log field: `recorder::run`'s resume path reads back whatever the configured
  store holds and pads only the frames that are in neither memory nor the store.
  `frames_lost_on_adopt` therefore falls to at most one
  `MSS_RECORDING_SPILL_SECONDS` on **any** pod.
- **Journal I/O stayed off the hot path.** Disk stays on `spawn_blocking`; every
  object-store call is awaited under a 10 s `SPILL_TIMEOUT` and a timeout is
  counted like any other failed spill, never fatal — the audio stays in memory
  and the next tick retries.
- **Startup salvage stays same-pod.** `recording_spill::salvage` lists manifests
  through the store and leaves a manifest whose `owner` is another pod alone,
  counting `spill_foreign_manifests`: with a shared store, salvaging a journal
  another pod is still writing would race it. Adoption, not salvage, is the
  cross-pod path.

**Verified — unit tests only, against the in-crate fakes. Nothing here ran
against real storage or a real pod.** The lab Docker stack was down for this
session (Docker Desktop not running), so `tests/minio_upload.rs` and the drill
were written but **not executed**.

| Test | What it proves |
| --- | --- |
| `an_adopter_on_any_pod_loses_at_most_one_spill_interval_when_the_journal_is_in_the_bucket` | one `MemorySink` shared by two `RecordingSupport`s ("pod-a", "pod-b"): pod A records, spills twice and is killed mid-call (its recorder task is aborted, so nothing is uploaded and nothing is discarded); pod B opens the same journal out of the bucket, finishes and uploads. The object opens with **every frame pod A spilled**, the unspilled remainder is silence, pod B's own audio follows, `frames_lost_on_adopt` is within one spill interval, and the reserved `_spill/` namespace is empty afterwards |
| `a_pod_that_lost_its_journal_to_an_adopter_stops_spilling_into_it` | the ownership steal: pod A appends, pod B adopts (which claims the manifest) and appends, pod A's next append is **refused** and reported as `surrendered()`, and the journal holds A's then B's frames with none of A's post-steal audio |
| `salvage_leaves_another_pods_journal_in_the_bucket_alone` | a startup salvage on "pod-a" over a journal owned by "pod-z" uploads nothing, counts one `spill_foreign_manifests`, and leaves the journal where its owner can still finish it |
| `the_reserved_spill_namespace_always_ends_in_one_separator` | the prefix normalisation and the `_spill/` default deploy.md documents |
| the pre-existing disk spill tests (stitch, salvage, no-clobber, the padded-member tail) | the `disk` backend is unchanged by the extraction — they pass untouched |

Gate: `cargo test --workspace` 296 mediaserverd unit tests + every other target
green, `cargo fmt --all --check` clean, `cargo clippy --all-targets -D warnings`
clean, the comment scan empty, `cargo deny check all` clean.

**Review fix (2026-08-27) — the spill write was on the recorder's loop.**
`spill_closed_segment` awaited `SegmentJournal::append` inline on the
`segment_close` tick and on `Pause`, bounded only by `SPILL_TIMEOUT` (10 s).
While that await was pending the recorder drained nothing from its hub
subscription, which is `CONSUMER_QUEUE_FRAMES` = 200 frames (4 s at 20 ms,
drop-oldest), so with `MSS_RECORDING_SPILL_TO=s3` a bucket that was slow but
inside the timeout cost up to **6 s of audio from the recording itself, every
spill interval** — the "blocking I/O on the pump" class architecture §7.1 exists
to forbid; disk spill had the same shape and merely finished in milliseconds.
**Invariant now: a spill write never holds the recorder's hub drain.** The
peek-then-commit seam is kept and the write taken off the loop: `begin_append`
(sync) returns an owned `SpillWrite`, `perform` (the I/O, under `SPILL_TIMEOUT`)
runs on a spawned task, and the loop's new `select!` arm applies the outcome —
`commit` + `close_segment` for exactly the written count, or the frames stay in
memory and the next tick retries with a larger prefix. At most one write is in
flight per recording; a tick while one is pending is a no-op; `Pause` uses the
same mechanism instead of an inline await; the finish path sends the stop report
first and then awaits the pending write (same timeout) before `read_back`. The
segmenter **seals** the prefix a write is carrying so a straggler frame for that
range is kept at the boundary, where the old inline close put it. Verified on a
real 200-frame `Hub` subscription with frames at ptime under paused tokio time:
`a_slow_spill_store_never_costs_the_recording_a_frame` (3 s per put, 500 frames,
zero dropped and all 500 in the object — the same test dropped exactly 100
against the old loop), `a_spill_write_that_fails_leaves_its_frames_in_memory_for_the_next_tick`,
`finishing_while_a_spill_write_is_failing_still_uploads_every_frame` and
`a_frame_that_lands_inside_a_sealed_prefix_is_kept_at_the_boundary`; every
existing spill, adoption and salvage test unchanged and green. Details under
*the write is off the recorder's loop* in
[implementation-notes.md](implementation-notes.md).

**Owed, and it is the honest half of this item.**

1. `tests/minio_upload.rs::a_journal_spilled_to_a_real_bucket_is_read_back_by_another_pod`
   is written and env-gated on `MSS_TEST_S3_ENDPOINT`; it has **never run**. It
   spills two segments as "pod-a" into real MinIO, adopts as "pod-b", finishes,
   and asserts the object's three tones in order and an empty `_spill/`. Run it
   the next time the lab is up: `MSS_TEST_S3_ENDPOINT=http://127.0.0.1:9000
   cargo test -p mediaserverd --test minio_upload`.
2. `RECORD=1 ./lab/pod_kill_drill.sh` — the live pod-kill-with-a-recorder drill
   D9 has owed since item 30. Its number goes in [lab.md](lab.md), and D9's row
   stays honest until it does.

**Residual.** No retention: MSS deletes a journal when its recording lands and
skips a foreign one, so what accumulates under `_spill/` is the journals of
recordings that finished on **no** pod, and nothing expires them. deploy.md now
asks the operator for a bucket lifecycle rule (expire `_spill/` after 7 days) and
says plainly that MSS implements none. Also unchanged: the unspilled tail is
still lost (that is what the spill interval buys), `MAX_RECORDING` is still 2 h,
and a `kill -9` between detach and upload still loses the `UploadCompleted`
event (D11's residual) even though the audio is now salvageable from any pod.

### 54. Recording groups as a shared record, not one pod's memory (D16) — ✅ DONE (2026-08-27)

**What shipped.** A recording group stopped being one pod's memory and became a
record every pod can read; `TapPlane::groups` is now the in-process cache in
front of it, and nothing above `join_group` changed shape.

- Two keys beside the session keys, same namespace prefix:
  `mss:group:<account>/<group>` → JSON
  `GroupRecord { recording_id, format, opened_at_unix_ms, created_by }`, created
  with `SET NX` so the loser of a race reads the winner back and two
  first-members on two pods agree on one recording and one anchor; and
  `mss:group:<account>/<group>:members` → hash `object key → owner pod`, where
  `HSETNX` **is** the duplicate-participant refusal. `HDEL` on leave, both keys
  `DEL`ed (best effort) when the hash empties, and both `EXPIRE`d at
  `GROUP_RECORD_TTL` (3 h = `MAX_RECORDING` + 1 h) on every join as the backstop
  against a pod that dies without leaving.
- `SessionStore` gained `open_or_join_group` / `leave_group`, implemented by
  `RedisSessionStore` with those commands and mirrored exactly by
  `MemorySessionStore`, so every `TapPlane` test now runs the store path with no
  Redis anywhere. `TapPlane::share_groups_through(Arc<dyn SessionStore>)` is a
  `OnceLock` set from `main.rs` beside the keeper — the `discover_through`
  idiom. **No store configured keeps the old pod-local behaviour**, which is
  correct for a single pod.
- `registry_keeper::rebuild` stopped skipping grouped attachments; the
  `grouped_not_adopted` counter and `mss_registry_grouped_not_adopted_total` are
  **deleted** from code and docs, because the behaviour they counted no longer
  exists.
- `lab/group_recording_drill.sh` gained `PODS=2`: pod-starting became a
  `start_pod` function, a second pod comes up at 172.31.99.123 (control 19092,
  metrics 19093), both pods get `MSS_REDIS_URL` pointing at the lab redis, and
  bob's create/record/detach and both live refusals go to pod B. `PODS=1` is
  byte-for-byte the drill that ran before.

**Decisions, as specified and as built.**

- **The anchor is wall-clock.** `RecordingGroup.opened_at`,
  `RecorderSpec.group_anchor` and `recorder::absorb`'s lead computation are all
  `SystemTime`; the record carries unix ms. Two pods cannot compare each other's
  `Instant`s, so cross-pod alignment is exactly as good as the nodes' clock
  sync — a skew of *s* misaligns two participants by *s*, and deploy.md says so
  next to the key table. The `resume_ms > 0` rule that clears `group_anchor` is
  untouched: an adopted recording's spilled frames already carry the lead.
- **A store error is a refusal, never a silent local group.** `join_group`'s
  failure path counts `group_joins_refused` and names the registry in the
  message. A member that cannot see the group would open a second
  half-recording under a prefix another pod is already writing, so a
  half-group is worse than no group.
- **Adoption takes its seat back instead of being refused by it.** The dead
  pod's seat is still in the members hash, so a plain `HSETNX` would refuse the
  adopter its own object. `open_or_join_group` takes `take_over`, and
  `open_recording_attachment` sets it when the attach carries
  `mss.recording.spillOwner` — the metadata key `rebuild` adds and nothing else
  does. The lease claim already arbitrated that session's ownership; the `HSET`
  records an outcome, it does not race for one.
- **Order inside `join_group`:** sync local precheck (cheap, and it keeps the
  single-pod refusal messages byte-identical), then the store round trip, then a
  sync commit into the cache — the group lock is never held across the await,
  and a commit that fails after the store accepted releases the seat again. One
  Redis round trip per grouped attachment, in the control world, never on a
  frame path.
- **`group_anchor_for(session)` is the seam item 55 fills.** It returns
  `SystemTime::now()` today. When a conference owns its recording, the first
  member of a new group whose session is a conference member must anchor on the
  conference's open instant, and this is the one function that changes.

**Verified — unit tests against `MemorySessionStore` and the tap_plane fakes.
Nothing here ran against real Redis, real MinIO or a real pod:** the lab Docker
stack was down for this session (Docker Desktop not running).

| Test | What it proves |
| --- | --- |
| `tap_plane::two_pods_sharing_one_store_pad_their_members_back_to_one_anchor` | the whole point, end to end on real sockets: two `TapPlane`s ("pod-a", "pod-b") sharing one `MemorySessionStore` and one `BucketSink`, alice recording on pod A and bob joining the same group on pod B ~0.6 s later. One prefix, two objects, and **bob's WAV opens with ≥ 250 ms of zeros** back to the anchor pod A stamped, with the two lengths equal to within half a second. Point pod B at its own store and it fails at 168 samples of lead — measured, not assumed |
| `tap_plane::a_group_name_reused_on_a_second_pod_joins_it_instead_of_opening_another` | pod A opens the group and its cache is then dropped; pod B's join returns **the same anchor**, `created_by` stays pod A, and the members hash names which pod writes which object. A reused participant label on pod B is refused with a message naming **pod-a** |
| `tap_plane::an_adopted_member_takes_its_seat_back_from_the_pod_that_died` | the same participant on a second pod is refused without `take_over` and seated with it, keeping the original anchor, and the seat then names the adopting pod |
| `tap_plane::a_group_the_store_cannot_answer_for_is_refused_rather_than_kept_locally` | an unreachable store refuses the grouped attach naming the registry, counts one refusal, and leaves `groups_live` / `group_members_live` at 0 with an empty local table |
| `registry_keeper::a_grouped_recording_is_rebuilt_on_the_adopting_pod_with_its_group` | the rewritten adoption test: the grouped `FILE_S3` attachment **is** rebuilt on pod B and re-attached **with `group=conf-9`**, where it used to be skipped and counted |
| the pre-existing group tests (refusal shapes, one-recording-per-group, group dies with its last member, the recorder's lead-silence and padded-tail tests) | unchanged and green, now running through `MemorySessionStore` because `plane()` installs one by default — the single-pod messages and counters did not move |

Gate, run as separate commands: `cargo test --workspace` green (304
mediaserverd unit tests, every other target unchanged), `cargo fmt --all
--check` clean, `cargo clippy --all-targets -- -D warnings` clean, the comment
scan empty, `cargo deny check all` — advisories, bans, licenses, sources ok.

**Owed, and it is the honest half of this item.**

1. `tests/redis_registry.rs::two_pods_opening_one_recording_group_agree_on_one_anchor`
   is written and env-gated on `MSS_TEST_REDIS_URL`; it has **never run**. Six
   concurrent `open_or_join_group` calls with six distinct participant labels
   must produce one `created_by` and one `opened_at_unix_ms` (the `SET NX`
   race), a TTL inside `GROUP_RECORD_TTL`, a reused label refused as
   `ParticipantHeld` naming its pod, a second recording id refused as
   `RecordsAnother`, and both keys gone (`TTL == -2`) once the last member
   leaves. Run it the next time the lab is up:
   `MSS_TEST_REDIS_URL=redis://127.0.0.1:6379 cargo test -p mediaserverd --test redis_registry`.
2. `PODS=2 ./lab/group_recording_drill.sh` — the two-pod drill. Its numbers
   (one prefix, two objects, equal lengths, the cross-pod refusal messages) go
   in [lab.md](lab.md), and D16's row stays honest until they do.

**Hardening (2026-08-27) — the reserved metadata keys were not refused on the
wire.** `mss.recording.resumeMs` and `mss.recording.spillOwner` are the
keeper's private channel into `attach`, but `RegistryKeeper` reaches the
controller through the **same** `SessionController::attach(Request<AttachRequest>)`
the gRPC server serves, and nothing refused those keys from a client. Any
authenticated caller could silence-pad a recording (pre-existing since item 30)
and, after this item, set `take_over` and claim another pod's recording-group
seat. Low severity — the bearer token is cluster-internal — and now closed:
`WireFacing(Arc<SessionController>)` in `control-api/src/server.rs` implements
`MediaControl` by delegation and refuses `INVALID_ARGUMENT`, naming the key, for
any metadata key under `RESERVED_METADATA_PREFIX` (`mss.`) in `attach` and
`update_attachment`; `serve_authenticated_until` serves that wrapper while the
keeper and every in-process caller keep the bare `Arc`. The prefix and the two
key constants moved to `session-core/src/metadata.rs` (control-api cannot see
`mediaserverd`) with `recorder.rs` re-exporting the names it already used, so
nothing else moved. **`mss.` rather than `mss.recording.` was verified safe**:
a session-core test pins every client-facing key this API documents — the eight
telcompat/Twilio keys and the six `mix_*`/`member_*` verbs — as still allowed.
**The TelCompat façade needed the guard too and is not covered by `WireFacing`**:
it is served over the same socket but calls the controller *in-process*, and
`stream_metadata` copies caller metadata straight through, so
`TelCompat::attach_sink` — the single funnel for every façade attach — checks it
itself. Verified over a **real socket**
(`control-api/tests/over_the_wire.rs`): an `Attach` carrying
`mss.recording.resumeMs` and one carrying `mss.recording.spillOwner` are both
refused `INVALID_ARGUMENT` with the key in the message, an `UpdateAttachment`
carrying `spillOwner` likewise, an `Attach` carrying `accountId` / `streamSid` /
a free-form `tenant.note` still succeeds, and a legacy `StartStream` carrying
`resumeMs` is refused through the façade — while the keeper's own rebuild tests,
which carry both keys in-process, stay green. That contrast is the proof the
guard sits at the right layer. Five session-core unit tests cover the predicate
itself, including that the named key is the lexicographically first reserved one
so the message is reproducible out of an unordered map, and that
`x-mss.recording` is *not* reserved.

**Residual.** Placement is still not a thing (D8) — a group's sessions land
wherever they land; the difference is that they no longer *have* to share a
pod. Cross-pod head alignment is only as good as NTP, and MSS neither measures
nor reports node clock skew. A conference still does not survive a pod loss,
because a conference member is an inline session and inline sessions are never
adoptable — that is D20/item 55 territory, not this one. Nothing expires a
group record early: a pod that dies without leaving holds its seats for up to
3 h, which blocks only that participant *label* in that group, and only until
the record expires.

### 54b. (original description, for reference) Recording groups as a shared record
**Where:** `crates/mediaserverd/src/{session_store.rs,tap_plane.rs,registry_keeper.rs,recorder.rs}`,
`lab/group_recording_drill.sh`.
**What:** move what a `RecordingGroup` *is* — its `recording_id`, format, open
instant and participant set — into the session store, so a member can join
from any pod, an adopter can rejoin, and a reused name on a second pod finds
the existing group instead of starting a second half-recording.
**Decisions, made:**
- Keys beside the session keys, same namespace prefix:
  `mss:group:<account>/<group>` → JSON `{recording_id, format,
  opened_at_unix_ms, created_by}` created with `SET NX` (loser reads the
  winner back, so two first-members on two pods agree on one anchor);
  `mss:group:<account>/<group>:members` → hash `participant label → owner
  pod`, `HSETNX` is the duplicate-participant refusal, `HDEL` on leave, both
  keys `DEL`ed when the hash empties (best effort) and `EXPIRE`d at
  `MAX_RECORDING + 1 h` on every join as the backstop. Add
  `open_or_join_group` / `leave_group` to `SessionStore`; `MemorySessionStore`
  implements them so every TapPlane test keeps running without Redis.
- **The anchor becomes wall-clock.** `RecordingGroup.opened_at` is
  `SystemTime` (unix ms in the record); `RecorderSpec.group_anchor` and
  `recorder::absorb`'s lead computation follow. Cross-pod alignment is then
  as good as the nodes' clock sync — say so in deploy.md (Kubernetes nodes
  run NTP; a skew of *s* misaligns two participants by *s*). The
  `resume_ms > 0` rule that clears `group_anchor` stays: the spilled frames
  already carry the lead silence.
- `TapPlane::groups` stays as the in-process cache and the source of the
  `groups_live`/`group_members_live` gauges; the store is consulted first on
  every `join_group`, and a store error is a **refusal** of the grouped
  attachment (counted), never a silent local group — a half-group is worse
  than no group.
- `registry_keeper::rebuild` stops skipping grouped attachments: it passes
  `group` through and `join_group` finds the record. Delete
  `grouped_not_adopted` and its metric; update deploy.md's metric table and
  `deploy/prometheus-alerts.yaml` if the counter is referenced.
- The conference-anchor rule: when the first member of a new group is a
  conference member, the group's `opened_at` is the conference's
  `opened_at_wall` (added in item 55; if 54 lands first, leave a named
  `TODO`-free seam — a function `group_anchor_for(session)` that returns
  `SystemTime::now()` — and item 55 fills it).
**Verify:** two `MemorySessionStore`-backed planes sharing one store: member A
on plane 1, member B on plane 2, both objects padded to the same anchor; a
reused group name on plane 2 after plane 1 dropped its cache joins, does not
recreate; adoption test in `registry_keeper.rs` where a grouped attachment is
rebuilt and its file length matches the survivor's; `tests/redis_registry.rs`
gains the `SET NX` race (two joins concurrently, one record). Lab:
`group_recording_drill.sh` gains `PODS=2` placing the two sessions on two
pods (the stack already runs three) and asserts one prefix, two objects, equal
lengths.
**Done when:** D16 reads closed, `grouped_not_adopted` is gone from code and
docs, and the two-pod drill number is in lab.md.

### 55. The room is a session: conference-owned recording (D20) — ✅ DONE (2026-08-27)

**What shipped.** `CreateSession{kind=MIX, group=<conference>}` creates — or
adopts, if a member opened the conference first — the **room itself** as a
session with no leg, and a `FILE_S3 only=mixed` attachment on it records the room
from the conference's open to its close however the members come and go. No new
RPC, no new enum value, two additive proto fields.

- `Conference` stamps `opened_at: Instant` (which is now the mix thread's release
  epoch) and `opened_at_wall: SystemTime` at `start`, and owns a **room hub** —
  one more `Hub`, its `Hub` half moved into `Mixed`, its `HubClient` kept on the
  control-plane handle. Each tick the mix thread polls it beside every member's
  and publishes the monitor listener's full sum into it on the **conference
  clock** (`frames * ptime_ms`), one bounded `force_push`, no allocation and
  nothing to block on. The room session's `LiveSession` points at that hub, so
  every attachment kind works on it unchanged.
- **A conference with no member still mixes**, so a room recording opened before
  anybody joins records the wait as real silence rather than needing a pad. A
  room-opened conference takes the pod's tap format, since there is no leg to
  negotiate one.
- **t=0 is the conference's open, for both shapes.** An ungrouped recording on
  the room session gets `RecorderSpec.group_anchor = opened_at_wall`, and item
  54's `group_anchor_for(session)` — the seam it left — now returns the
  conference's open for any session seated in one, so the per-participant group
  of the same conference anchors on the same instant. The two shapes are
  sample-aligned by construction instead of by being attached together.
  `RecordingShape` still says `conference-mixed`.
- **Lifetime.** `Conference::unseat` returns `RoomFate::{Mixing, Emptied,
  Stopped}`: with no room session the last leg out still closes the conference,
  and with one the conference is held. The room session then ends on
  `EndSession`, or by itself once the conference has held a member and emptied,
  after `MSS_CONFERENCE_LINGER_SECS` (default **0**, which ends it synchronously
  on the last leave). Ending it ends its attachments (the recording uploads) and
  stops the mix only if no member remains; members that remain keep mixing.
- **Read-back.** `Session.opened_at_unix_ms = 15` (the *conference's* open for a
  MIX session, the registry's own stamp otherwise — `SessionRecord` now stamps
  every session) and `ConferenceView.room_session = 4`. `DescribeSession` on the
  room reports `conference` and **not** `member` (a room is not a member of
  itself); every member reports the room session back, so a controller can find
  where the room recording belongs from any leg. Next free `Session` field is
  **16**; next free `MediaEvent` payload tag is still **28**.
- `mss_ctl create <id> --kind mix --group <conference>` (the `create` subcommand
  learned flags; its positional TAP form is untouched), plus
  `mss_conference_rooms_live` and `mss_conference_rooms_auto_ended_total`.

**Decisions, as specified and as built.**

- **The room session is inline-like for adoption.** `PersistedSession::is_room()`
  (kind 3) joins `is_inline()` under `is_pod_bound()`, and `is_rebuildable()`
  now excludes it **explicitly** — it was already false through the empty
  `call_id`, an accident this turns into a rule — so `adopt_orphans` releases a
  room record with a message that says why. deploy.md's HA table has the row.
- **The linger is a spawned sleep, not a housekeeping tick,** because this daemon
  has none: one `tokio::spawn(sleep)` per emptied conference, its handle parked
  in `Conference::linger`, aborted by `seat` (a member that rejoins cancels it)
  and re-checked under the lock on expiry. Linger 0 ends the room inline on the
  last leave and spawns nothing. The task needs the plane, so `main.rs` hands it
  a `Weak<TapPlane>` of itself (`linger_through`, the `observe_through` idiom);
  **with no weak self set, a non-zero linger warns and the room stays open until
  the API ends it** — the safe direction. Item 56 may generalise this into a
  sweep; nothing here builds one.
- **Ending a control-plane session from the media plane needed one new seam:**
  `ObservationSink::session_finished(session, reason)`, defaulted to a no-op and
  implemented by `SessionController` as `destroy_session`, so an auto-ended room
  publishes `AttachmentDown`/`SessionEnded` in its own gapless sequence and frees
  its external id. It is the only such call, and it exists because a room session
  has no hangup of its own to be told about.
- **A room session's shape is validated in session-core,** not in the controller:
  `room_session_shape` requires a non-empty `group` and refuses a `call_id`,
  `from_tags` or `sdp_offer` by name (`ControlError::RoomSessionShape` →
  `INVALID_ARGUMENT`). The controller's own rules only had to stop refusing a
  `group` on a MIX and start refusing an offer on one.
- **A group on the room session is refused by name** (a room has no participant
  seat), and item 39's grouped-mixed refusal on a *member* is unchanged.
  Recording the room off a member still works — D20 stays *possible* there, and
  the row now says the room session is the way not to have it.
- **Playback on the room is the room's.** `playback_reach` takes the session
  kind: on a room an empty target or `all` is the room prompt and `own` is
  refused by name (a room has no ear). `StartPlayback{target_tag=all}` from a
  member is untouched — no verb was removed — and because the generic
  non-inline INJECT path turns an utterance plus a `Mark` into a `StartPlayback`
  blob on its own session, an **INJECT attachment on the room session is a room
  prompt** too. It is utterance-and-`Mark` shaped, not the continuous
  full-duplex pacer an inline leg gets; that is the honest scope of "an INJECT on
  it is a room prompt" and it is a residual below, not a claim.
- Route and member verbs on the room session are refused by
  `Conference::{route,control}`'s existing `NotSeated`, since the room is not a
  member of itself.

**Verified — in-process over real UDP sockets, against the in-crate recording
fakes. The lab Docker stack was down for this session (Docker Desktop not
running), so the drill did not run.**

| Test | What it proves |
| --- | --- |
| `tap_plane::a_room_session_opened_before_anybody_joins_records_the_wait_and_then_the_room` | a room session opens the conference with no member, its object opens with **≥ 250 ms of zeros** and then carries 1000+2000 summed (2700–3300); ending the room session leaves the members mixing and still hearing each other |
| `tap_plane::a_room_recording_attached_late_starts_at_the_rooms_open_and_matches_its_participants` | the other order: two members mix for ~0.5 s, *then* the room session adopts the conference. The room object **and** a participant group opened at the same late moment both open with ≥ 250 ms of lead and agree in length to within half a second — one anchor, two shapes |
| `tap_plane::the_room_object_outlives_a_member_and_closes_when_the_last_one_leaves` | D20 itself: one member leaves, the room object is still live and **nothing has been uploaded**; the object carries both members in its first third and bob alone after she left; the last member out ends the room session, uploads the object, counts `rooms_auto_ended = 1` and publishes `session_finished` naming the last member |
| `tap_plane::an_emptied_room_lingers_and_a_member_that_rejoins_cancels_the_linger` | with `MSS_CONFERENCE_LINGER_SECS` = 300 ms: the conference is held after its only member leaves, a member that rejoins inside the window cancels the wait (still mixing two linger-widths later), and the next emptying ends the room session when the linger expires |
| `tap_plane::a_room_session_is_refused_a_recording_group_and_a_second_owner` | the two refusals, by name: a group on the room session, and a second `kind=MIX` session for a conference that already has one |
| `tap_plane::a_room_session_that_names_no_conference_is_refused_rather_than_half_opened` | the old "phase-4 is not built" refusal, replaced: a MIX with no group opens no conference and leaves no session behind |
| `tap_plane::describing_a_room_session_reads_the_conferences_open_and_its_members` (`WiredRoom`, over a real gRPC socket) | `DescribeSession` on the room: no `member`, a `conference` naming both members and `room_session=the-room`, and `opened_at_unix_ms` **at least 100 ms earlier than the moment the room session was created** — i.e. the conference's open, not its own. Every member reports `room_session` back, and it goes empty when the room session ends while they keep mixing |
| `session-core::registry::a_room_session_is_a_conference_with_a_group_and_no_call_identity_of_its_own` | the shape rule and its four refusals, plus `opened_at` being stamped |
| `control-api::media_control::a_room_session_is_a_group_with_no_leg_and_reports_the_conferences_open` | the same rules as `INVALID_ARGUMENT` over the API, and `opened_at_unix_ms` on the wire |
| `session_store::a_room_session_is_bound_to_the_pod_that_mixes_it_and_is_never_adopted` | `is_room` / `is_pod_bound` / `is_rebuildable`, including a room record that *does* carry a call identity |

Gate, run as separate commands: `cargo test --workspace` green (311
mediaserverd unit tests, 67 session-core, 29 control-api `media_control`),
`cargo fmt --all --check` clean, `cargo clippy --all-targets -- -D warnings`
clean, the comment scan empty, `cargo deny check all` — advisories, bans,
licenses, sources ok.

**Owed.**

1. **`lab/conference_drill.sh` has not run.** It is updated per the spec and
   `sh -n` clean: the room object now hangs off a room session
   (`mss_ctl create conf-<stamp>-room --kind mix --group <conference>`, opened
   *before* the peers), a new **leave** phase hangs A up while B and C keep
   talking, the ear expectations for that phase are in the manifest, and the
   length comparison is now `room.wav` against `party-b.wav` and `party-c.wav`
   (one length, `LENGTH_TOLERANCE_MS`) plus a new check that `party-a.wav` is
   shorter than the room object by most of the time A was gone — which is the
   D20 assertion in one number. Run it the next time the lab is up and put the
   numbers in [lab.md](lab.md).
2. Nothing here has been judged by a human ear, and no **SIP** peer has ever
   been in a conference (still item 41's residual).

**Residual.**

- An INJECT attachment on the room session is a *prompt* path (utterance +
  `Mark` → `StartPlayback` blob, capped by `MAX_UTTERANCE_SAMPLES` and the room's
  prompt queue), not the continuous full-duplex inject an inline leg gets. Long
  form room audio still belongs on an INJECT attachment on a member with
  `mix_target=all`.
- A room-opened conference fixes its rate and ptime from the **pod's** tap
  format, so a member that negotiated something else is refused by name; open
  the conference from its first leg if the room's format must follow the call.
- A room session is not adoptable and a conference is still pod-local (D16's
  residual, D8): the room's recording survives a pod loss only as far as the
  spill does, and the room itself does not move.
- With a non-zero linger and no `linger_through` (any embedder that builds a
  `TapPlane` without handing it a `Weak` of itself), an emptied room stays open
  until the API ends it. `main.rs` always sets it.
- Member state had no lease when this landed; **item 56 gave it one** (D22
  closed). The room session is still not an owner for it: it holds no member
  state of its own.

### 56. Member state with a lease (D22) — ✅ DONE (2026-08-27)

**What shipped.** A member flag can now name how long it holds. A fourth
metadata key on the same `Attach`/`UpdateAttachment` that carries the verbs —
`member_state_ttl_ms` — leases every flag set `on` in that request; when the
lease runs out unrefreshed, the pod lifts the flag itself, through the exact same
path an explicit `off` takes, and says so on the bus. Absent or `0` is
**today's behaviour, unchanged**: the flag holds until a controller says
otherwise.

- Parsed in `MemberControl::from_metadata` into `ttl_ms: Option<u64>` — `None`
  is "the request named none, take the pod's default", `Some(0)` is "explicitly
  no lease" — with a bad value refused by name through a new
  `MixRouteError::MemberStateTtl { key, value }`. A TTL with no flag `on` beside
  it leases nothing rather than being an error, because `member_mute=off` next to
  a leftover TTL has to stay legal.
- `MSS_MEMBER_STATE_TTL_SECS` (default `0`) is the deployment default, resolved
  in exactly one place — `TapPlane::control_member` rewrites `ttl_ms` before the
  value reaches the conference — and an explicit `0` in the request outranks it.
- The deadline lives in the **control-world mirror**: `MirroredMember` gained
  `mute_until` / `deaf_until` / `hold_until: Option<Instant>`, written by the
  same call that writes the flag. `Conference::control` is a wrapper over
  `control_at(session, control, now)`.
- Expiry: `Conference::expire_member_state(now)` finds the passed deadlines and
  applies them by calling **`control_at` with `MemberControl::releasing(..)`** —
  the same mirror write and the same `ConferenceCommand::Control` an `off`
  enqueues. `TapPlane::sweep_member_state(now)` runs it over every conference
  under one lock, drops the lock, and publishes the observations.
  `main.rs` spawns the daemon's **first housekeeping tick** — a
  `tokio::time::interval` of 500 ms, `MissedTickBehavior::Delay` — that calls it.
  **`Mixed::run` is untouched:** no timer, no new work, and it cannot tell an
  expiry from an `off`.
- Event: `EventKind::MemberControlled` gained `cause: MemberControlCause
  { Requested, Expired }`, proto `MemberControlled.cause = 4` with
  `MEMBER_CONTROL_CAUSE_REQUESTED = 0`. Read-back: `MemberState`
  `.{mute,deaf,hold}_expires_in_ms = 6,7,8` (0 = no lease), filled from the
  mirror. **All additive — next free `Session` field is still 16, next free
  `MediaEvent` payload tag is still 28.**
- Metric `mss_conference_member_state_expired_total` (one per **flag** lifted),
  `mss_ctl member <attachment> mute on ttl <ms>`, and `MUTE_TTL_MS` in
  `lab/conference_drill.sh`.

**Decisions, as specified and as built.**

- **The lease answers "how long", never "whose".** Item 40 rejected the
  attachment as owner and this API still does not model the caller, so nothing
  here invents an owner: the deadline is a field beside the flag it bounds. Two
  controllers muting the same member still race, and the last lease wins.
- **Expiry is a control-world event applied through the write path.** The
  alternative — a deadline the mix thread checks — would have put a timer and a
  clock comparison per member per tick into the packet path for something the
  control world already knows. The mirror is where item 49 put member state; the
  deadline belongs beside it.
- **The registry's own mirror is corrected by the expiry.**
  `SessionRegistry::observe` maps `Observation::MemberStateExpired` through
  `release_member_state`, which rewrites `on` to `off` in the metadata of every
  attachment of that session **and then** folds them back together to report the
  state that is left. Both halves matter: without the rewrite a later
  `UpdateAttachment` would diff `member_mute=on` against a stale `on` and
  publish nothing when a controller re-mutes; without the fold, an expiry event
  could not say that a hold which was never leased is still held. The event takes
  the session's next `seq` with `attachment: None`, because nobody asked for it.
  The TTL key is deliberately left in the metadata, so a later bare
  `member_mute=on` re-leases at the length that client last asked for.
- **A refresh is the same request again, and publishes no event.** Resending
  `member_mute=on` + a TTL moves the deadline out; the declared state did not
  change, so `update_attachment`'s diff emits nothing. That is on purpose: a
  10-second refresh loop must not put an event on Kafka every ten seconds.
- **One ordering fix fell out of it.** `control_at` now verifies the seat,
  **enqueues the command, and writes the mirror last**. It used to write the
  mirror first, so a full command queue left the mirror claiming a state the mix
  thread had never been told about — and, for an expiry, with the deadline
  already cleared, so nothing would ever retry. Every caller of `control` gets
  that fix.
- **A deviation from this item's letter, and why.** The item had
  `sweep_member_state` return the expired pairs "so the caller can emit events".
  It does return them, but the **plane** publishes the observations, because the
  plane is what holds the `ObservationSink` (`observe_through`) and every other
  media-initiated event — DTMF, the recording callbacks, item 55's
  `session_finished` — goes out that way; `main.rs` only logs a count. The
  returned pairs are what the unit tests assert against.

**Verified — unit tests, in-process, on real UDP and real gRPC sockets. The lab
Docker stack was down for this session (Docker Desktop not running), so the
conference drill did not run.**

| Test | What it proves |
| --- | --- |
| `tap_plane::a_muted_members_lease_lifts_the_mute_with_no_off_and_a_refresh_holds_it` | the whole loop on three real conference peers: a mute with a 500 ms lease is heard by nobody (alice hears carol alone at ≈4000, the mixed track ≈5000) **and stays muted past its own deadline for as long as nobody sweeps** — the mix thread keeps no timer, which is the invariant in one assertion; a refresh with a 60 s TTL then makes the sweep a no-op; a sweep at the new deadline releases exactly `mute: Some(false)` (and `deaf: None`), is observed once as `Observation::MemberStateExpired` on bob's own session, counts one `member_state_expired`, and alice hears bob again (≈6000, mixed ≈7000) **with no `off` ever sent** |
| `tap_plane::a_deployment_default_leases_a_member_flag_that_names_no_ttl_of_its_own` | `MSS_MEMBER_STATE_TTL_SECS`' half: a `member_deaf=on` that names no TTL counts down from the pod's 400 ms default and is swept away; an explicit `member_state_ttl_ms=0` outranks that default and is **never** swept, even an hour later — the pre-lease behaviour, on demand |
| `tap_plane::a_leased_member_flag_counts_down_over_the_wire_and_expires_as_its_own_cause` (`WiredRoom`, a real `MediaControlClient` over TCP) | the wire: `DescribeSession` reports `mute_expires_in_ms` inside `1..=60000` with the two unleased flags at `0`; a `WatchEvents` stream on that member then receives `MemberControlled{mute: false, cause: MEMBER_CONTROL_CAUSE_EXPIRED}`, and a second Describe reads the flag off with no lease left |
| `session-core::registry::a_member_state_lease_that_runs_out_is_audited_as_expired_and_leaves_the_metadata_off` | the registry half: the expiry event carries the state that is **left** (`mute: false`, the never-leased `hold: true`), takes the session's next `seq` with no attachment, flips the attachment's `member_mute` metadata to `off` and leaves `member_hold` alone — and re-muting afterwards is a real change that publishes `cause: Requested` rather than being swallowed by a stale mirror |
| `session-core::mix::a_member_flag_may_carry_a_lease_and_absent_still_means_it_holds_forever` | the parser: absent → `None` → the deployment default; one TTL applying to every flag `on` in the request; a trimmed value; an explicit `0` outranking the default; a TTL alone leasing nothing; and two refusals by the key's own name |
| `session-core::mix::a_release_names_only_the_flags_whose_lease_ran_out` | `MemberControl::releasing` is `Some(false)` for the expired flags and `None` for the rest — an expiry is byte-for-byte the value an `off` produces |
| `metrics::the_exposition_counts_the_member_state_leases_this_pod_lifted_itself` | the counter is declared and rendered on a pod with no conference at all |
| the pre-existing member-verb, whisper, room and read-back tests | unchanged and green: with no TTL anywhere, nothing about member state moved |

Gate, run as separate commands: `cargo test --workspace` green (315
mediaserverd unit tests, 70 session-core), `cargo fmt --all --check` clean,
`cargo clippy --all-targets -- -D warnings` clean, the comment scan empty,
`cargo deny check all` clean.

**Owed.**

1. **`lab/conference_drill.sh` has not run.** Its mute phase is updated per this
   item and `sh -n` clean: with `MUTE_TTL_MS` set (it must exceed the mute
   window, or the drill refuses it by name) the mute is sent as
   `member <A> mute on ttl $MUTE_TTL_MS`, the unmute phase sends **no `off`** —
   it waits out the rest of the lease plus one sweep and fails unless
   `mss_conference_member_state_expired_total` moved — and the existing ear
   expectations judge the two windows unchanged. Run it the next time the lab is
   up (`MUTE_TTL_MS=12000 ./lab/conference_drill.sh`) and put the numbers in
   [lab.md](lab.md).
2. Nothing here has been judged by a human ear, and no lease has ever bounded a
   **SIP** peer's mute.

**Residual.**

- **The lease is pod-local.** It is an `Instant` in one pod's memory, so it
  neither survives a pod loss nor moves with a member. Conferences are pod-bound
  anyway (D16's residual), so this adds no new exposure — but a controller cannot
  treat a TTL as a durable promise.
- **Still no owner.** The lease bounds member state without deciding whose it is,
  which is the honest scope: concurrent controllers still race and the last lease
  wins, and a member muted by a policy engine with no TTL is exactly as
  unreclaimable as it was before.
- **Half a second of slack.** The sweep runs every 500 ms, so a flag lifts up to
  that late. Anything tighter would either poll harder in the control world or
  put a clock in the mix thread.
- **A refresh is silent on the bus** (no state change, no event), so an auditor
  cannot see refreshes — only the set, and the lift.

### 56b. (original description, for reference) Member state with a lease
**Where:** `crates/session-core/src/{mix.rs,event.rs,registry.rs}`,
`crates/mediaserverd/src/{conference.rs,tap_plane.rs,main.rs,metrics.rs}`,
`crates/control-api/src/convert.rs`, `proto/mediacontrol.proto` (fields only),
`docs/deploy.md`.
**What:** let a controller say how long a `member_mute`/`member_deaf`/
`member_hold` should hold without being refreshed, so a controller that dies
between `on` and `off` leaves a member muted for that long, not for the life of
the room.
**Decisions, made:**
- A fourth metadata key, `member_state_ttl_ms`, applying to every flag set
  `on` in the same `Attach`/`UpdateAttachment`; absent or `0` means no lease —
  **today's behaviour unchanged**. `MSS_MEMBER_STATE_TTL_SECS` is the
  deployment default applied when the request carries none (default `0`).
  Refresh is any request that sets the flag `on` again with a TTL; `off` clears
  the flag and its deadline. Parsed in `MemberControl::from_metadata` (a bad
  value is a `MixRouteError::MemberFlag`-style refusal by name).
- The deadline lives in the **control-world mirror** (`MirroredMember` gains
  `mute_until/deaf_until/hold_until: Option<Instant>`) and expiry is applied
  through the same path as `off`: a new control-world housekeeping task in
  `main.rs` (`tokio::time::interval`, 500 ms) calls `TapPlane::sweep_member_state(now)`,
  which collects expired flags under the conferences lock, calls
  `Conference::control` with `Some(false)` for each, and returns
  `(session, MemberControl)` pairs that the caller turns into events. The
  mix thread is untouched except for receiving the resulting `Control`
  commands — no timers in the media world.
- Event: `EventKind::MemberControlled` gains `cause: MemberControlCause
  { Requested, Expired }`; the proto payload gains `cause` with `REQUESTED`
  as the zero value so existing consumers read unchanged. The registry emits
  the expired event with the session's next `seq` like any other.
- Read-back: `MemberState` gains `mute_expires_in_ms`, `deaf_expires_in_ms`,
  `hold_expires_in_ms` (0 = no lease), filled from the mirror.
- Metric: `mss_conference_member_state_expired_total`.
**Verify:** `mix.rs` parser tests for the new key; a tap_plane socket test
where a mute with a 300 ms TTL is heard to lift (the member's tone returns to
every ear and to the mixed track) without any `off`, the `MemberControlled
{cause: Expired}` event is observed, and a refresh before expiry keeps it
muted; a `WiredRoom` test reading `mute_expires_in_ms` back; the no-TTL test
proving nothing changed. Lab: `conference_drill.sh` mute phase gains a TTL
variant (`MUTE_TTL_MS`) and lets it expire instead of sending `off`.
**Done when:** D22 reads closed, deploy.md documents the key and the env
default with the refresh loop a UI should run, and Appendix B's no-lease
paragraph is rewritten.

### 57. The kernel verdict, and rtpengine's relay split, as metrics — ✅ DONE (2026-08-26)
**What shipped:** the rtpengine `statistics` reply that every `/readyz` probe
already fetched no longer dies in a log line. `NodeCapabilityLog` keeps
`last: Mutex<HashMap<SocketAddr, NodeSample>>` — the verdict,
`relayedpackets_kernel`/`_user`, `media_kernel`/`_userspace`/`_mixed`,
`transcodedmedia`, the node's live session count and the sample's `Instant` —
and `metrics.rs` renders it per node in the `mss_dependency_ready{…}` labelled
style: `mss_rtpengine_tap_kernel_verdict{node,verdict} 1` (one series per node,
the four `TapKernelVerdict` names as label values),
`mss_rtpengine_relayed_packets_kernel{node}`,
`mss_rtpengine_relayed_packets_user{node}`, `mss_rtpengine_media_kernel{node}`,
`mss_rtpengine_media_userspace{node}`, `mss_rtpengine_media_mixed{node}`,
`mss_rtpengine_transcoded_media{node}`, `mss_rtpengine_sessions_live{node}` and
`mss_rtpengine_sample_age_seconds{node}`. `MssTapsFellOutOfKernel`
(`deploy/prometheus-alerts.yaml`, new `mss-rtpengine` group, synced into
`deploy/k8s/base/prometheusrule.yaml`) fires on
`verdict="TranscodedTapsAreProcessedInUserspace"` or on userspace media rising
for 10 min while kernel media stays flat, with architecture §8.1 as the runbook.
The split that keeps it cheap: `observe` refreshes the sample on every health
probe (one extra NG command per node per `MSS_HEALTH_PROBE_INTERVAL_SECS`, never
on the media path), while `report_first_contact` — still what `main`'s startup
ping and `TapPlane::open_session` call — is now `observe` behind a read of the
reported-once set, so opening a session on a known node stays free and the
first-contact log line stays once per node. `forget` clears the sample too, so a
node that stops answering stops being reported instead of freezing at its last
numbers. `mss_rtpengine_sessions_live` is one series more than this item
specified; it is the same sample field the item already required be kept, and
H3 wants it beside the packet counters.
**Verified:** unit tests only. `NodeSample::from_statistics` is pure, so the
whole mapping is asserted from a constructed `RtpengineStatistics` (userspace
node → `ThisNodeIsNotUsingTheKernelModule`; the same node with
`transcode_at_tap` → `TranscodedTapsAreProcessedInUserspace`); a second test
proves the last sample per node wins, that `samples()` comes back in node order,
and that `forget` drops one node and leaves the other. In `metrics.rs`, a
constructed `NodeCapabilityLog` holding **two** nodes — one userspace, one
kernel — renders both verdict series with their labels, all seven per-node
gauges with the right values, one `# TYPE` line per metric, and a sample age;
and a pod that has probed nothing emits none of the series. The yaml of both
alert files parses and the generated `PrometheusRule` carries the new group.
**Owed:** the **lab run is not done** — the Docker stack was down in this
session, so nothing here has been read off a real rtpengine. The lab check still
owed is a run against the compose rtpengine showing
`verdict="ThisNodeIsNotUsingTheKernelModule"` with non-zero
`mss_rtpengine_relayed_packets_user` and a `sample_age_seconds` that stays under
the probe interval, plus one `MSS_TAP_TRANSCODE=on` run showing the transcoding
verdict. `MssTapsFellOutOfKernel`'s PromQL has never been evaluated by a
Prometheus.
**Residual:** the numbers are the *node's*, not this pod's, so two pods tapping
one rtpengine report the same counters — aggregate with `max by (node)`, never
`sum`. A verdict that changes leaves the previous label set in Prometheus until
it goes stale (the price of the one-series-per-node shape the item chose). And a
node that keeps answering `ping` but stops answering `statistics` keeps its last
sample, visible only as a growing `mss_rtpengine_sample_age_seconds`.

## Recording store selection — the one storage decision that is the deployment's

### 58. A flag picks the recording store: S3 or a FreeSWITCH-style filesystem — ✅ DONE (2026-08-27)
**Where:** `crates/mediaserverd/src/recorder.rs` (a second `RecordingSink`),
`recording_spill.rs` (no change intended — `ObjectSpill` already takes any
sink), `lab/preflight.sh`, `deploy/k8s`, `docs/deploy.md`.
**Why:** the architecture chose direct-to-S3 over the legacy shared-filesystem
recording (architecture.md §3, "no shared filesystem, no SQS hop") and that stays
the default. But a deployment with no object store — or one whose downstream
recording pickup already watches a mounted tree the way it watched FreeSWITCH's
`record_session` output — should be able to keep that tree and still retire
FreeSWITCH. `RecordingSink` is a trait with one production implementation; the
choice between two is configuration, not architecture.
**What:**
- `MSS_RECORDING_STORE` ∈ {`s3` (default, unchanged behaviour), `filesystem`}.
  Any other value is a storage misconfiguration and **refuses to start**, the
  same way an unusable bucket does.
- `filesystem` requires `MSS_RECORDING_ROOT`, an absolute directory on a volume
  every recording pod mounts (RWX PVC, EFS, NFS — MSS does not care which). At
  start the process writes, renames and deletes a probe file under it and
  refuses to start if it cannot. With `filesystem` selected the `MSS_RECORDING_S3_*`
  and bucket variables are ignored with one warning naming them; with `s3`
  selected a set `MSS_RECORDING_ROOT` is ignored the same way.
- The tree under the root **is the frozen identity, verbatim**:
  `<root>/${accountID}/${recordingID}.${format}` and
  `<root>/${accountID}/${recordingID}/${participant}.${format}` — the layout
  `record_session` produced, so an existing pickup job keeps working. Nothing
  else is written outside `_spill/`.
- A file lands **atomically**: written to a sibling `.${basename}.${owner}.part`
  in the same directory, fsynced, then renamed over the final name, so a watcher
  never reads a half file; `list` never reports `.part` files. `exists` keeps the
  rule that MSS never writes over an object that already exists.
- `UploadCompleted.uri` is `file://<root>/<key>`. The attachment transport stays
  `TRANSPORT_FILE_S3` / `file-s3`: the wire is frozen (Constitution VII); the
  store behind it is deployment configuration and the event's URI scheme says
  which one it was.
- `MSS_RECORDING_SPILL_TO=s3` keeps working and means "the recording store,
  whichever it is" — the journal lands at `<root>/_spill/…` through the same
  `ObjectSpill`, so cross-pod adoption works on a shared filesystem too. Accept
  `store` as the clearer synonym; `disk` is unchanged.
**Verification, in order:** unit tests on a real temporary directory — the five
sink methods, the `.part` file invisible to `list` and gone after `put`, a refused
overwrite, `ObjectSpill` over the filesystem sink round-tripping a journal and
`salvage` finishing it, and the `from_env` matrix (default is `s3`; `filesystem`
without a root refuses; an unknown store refuses; the cross-set variable warns).
Then, **if the lab stack is reachable**, run `lab/grpc_stream_drill.sh RECORD=1`
with the pod switched to `MSS_RECORDING_STORE=filesystem` and a bind-mounted root,
and check the WAV at the identity path with `track_dump.py`; if the lab is not
up, the commit message must say so and name the unit tests as the evidence.
**Also:** `lab/preflight.sh --recording-root DIR` (the same probe write); a
`deploy/k8s/overlays/filesystem-recording` overlay (an RWX `PersistentVolumeClaim`
with a placeholder storage class, mounted at `/var/lib/mediaserverd/recordings`,
`MSS_RECORDING_STORE=filesystem` and the root in the ConfigMap) that
`deploy/k8s/validate.sh` passes, with `validate_fields.py` asserting the root is a
mounted volume whenever the store is `filesystem`; the `docs/deploy.md` Recording
table and a short "which store" paragraph; one clause on architecture.md's
recording-sink line saying S3 is the default and a shared filesystem the
alternative; an implementation-notes.md section; this item and the tables above
updated.
**Done when:** the five-command gate passes, both stores are selectable by one
variable with everything else unchanged, and a recording made with
`MSS_RECORDING_STORE=filesystem` is a WAV at
`<root>/${accountID}/${recordingID}.wav` that `track_dump.py` reads, announced by
an `UploadCompleted` whose URI starts `file://`.
**What shipped:** `MSS_RECORDING_STORE` ∈ {`s3` (default, unchanged), `filesystem`}
decided by a **pure** `decide_store(store, root, cross_set)` — so the matrix is
tested without touching the process environment — which refuses to start on an
unknown store, on `filesystem` with no root, and on a relative root, and names the
cross-set variables in one `warn!` before ignoring them. `filesystem` runs the
write/rename/delete probe on `MSS_RECORDING_ROOT` at startup
(`probe_recording_root`) and refuses to start if it fails. `FilesystemRecordingSink`
(`tokio::fs`, **not** `object_store`'s `fs` backend, whose path rules differ) lands
every file atomically through a sibling `.<basename>.<owner>.part` +`sync_all` +
`rename`, validates keys so none can leave the root, returns
`file://<root>/<key>` for `UploadCompleted.uri`, and implements `list` to the S3
sink's contract exactly — recursive under a `/`-trimmed prefix, keys relative to
the root, `.part` files invisible — which is what `ObjectSpill`/`salvage` depend
on. `MSS_RECORDING_SPILL_TO` gained `store` as the synonym for `s3`, so the
journal lands at `<root>/_spill/…` and cross-pod adoption works on a shared
filesystem too. The transport name `file-s3`/`TRANSPORT_FILE_S3` is untouched
(Constitution VII), and the no-overwrite rule stayed where it already lived —
`recording_spill::salvage`'s `exists` check — rather than being duplicated in the
sink. Also shipped: `lab/preflight.sh --recording-root DIR` (the same probe, and
an S3 check that SKIPs under the filesystem store),
`deploy/k8s/overlays/filesystem-recording` (RWX PVC with a placeholder storage
class at `/var/lib/mediaserverd/recordings`), `validate_fields.py` asserting that
a `filesystem` store's root is a mounted volume backed by a `ReadWriteMany` claim
(`validate.sh`: 4 overlays render, 104 field assertions pass), the deploy.md rows
and "which store" paragraph, the architecture.md clause, and an
implementation-notes section. **Evidence: unit tests on a real temporary
directory** — the five sink methods, the `.part` file invisible to `list` and gone
after `put`, refused escaping keys, an `ObjectSpill` journal round trip whose
`salvage` writes the WAV at `<root>/acct-42/rec-99.wav`, the probe, and the
`decide_store` matrix. **The lab drill did NOT run: no Docker daemon on this
machine**, so no live call has been recorded to a filesystem store and no
`file://` `UploadCompleted` has been seen on the bus — that is what a pilot owes
this item.

### 59. The SIP front door: transactions, dialogs and session timers — ✅ DONE (2026-08-30)

The media plane answers SIP itself. `crates/sip-uas` grew from a parser into a
**UAS**, and `crates/mediaserverd/src/sip_front_door.rs` put it on a socket.

**Written, not adopted** — architecture §7 records the probe that reversed the
recorded plan: there is no transaction crate to take (`rvoip-transaction-core`
is deprecated), `rvoip-sip-dialog` + `rvoip-sip-transport` cost **59 new crates
and 24 version skews**, two of them likely `cargo deny` failures, and
`rvoip-infra-common` installs `mimalloc` as the process-wide
`#[global_allocator]` by default with a comment that misdescribes its own `cfg`.
Above all it is tokio-timer-driven, and this is the one layer that is entirely
timers. `rvoip-sip-core` stays the syntax layer and is still the only dependency.

**What landed**

- **RFC 3261 §17 server transactions**, INVITE (with RFC 6026's `Accepted`
  state) and non-INVITE: branch matching on the topmost Via, ACK keyed to its
  INVITE, retransmission of a final on the doubling schedule to T2, timers
  G/H/I/J/L, and the 481 for a CANCEL with nothing to cancel.
- **RFC 3261 §12 dialogs**: id, tags, remote target, reversed route set, CSeq
  ordering, and re-INVITE recognised as in-dialog.
- **RFC 4028 session timers**: negotiation, 422 below Min-SE with our floor
  stated, and expiry.
- **The driver**: `MSS_SIP_LISTEN` / `MSS_SIP_ADVERTISE`, a non-blocking
  `CreateSession` so the socket is never stalled, and the same
  `SessionController` the gRPC service uses — one entrance, not two.

**Two defects found and fixed while doing it.** `SimpleResponseBuilder::dialog_response`
writes the literal To tag `"local-tag-value"`, so every concurrent call would
have shared a dialog identity; the door and `Invite::answered_with` take the tag
as a parameter instead. And `lab/sip_shim.py`'s replay cache has, in its own
words, *"no timers"* — an INVITE it answers and never sees acknowledged holds an
MSS session and a mixer slot forever. The door ends that session at 64×T1.

**Verified.** 56 tests in `sip-uas` (including a deterministic 25 000-datagram
mutation sweep) and 10 in `sip_front_door` over **real UDP sockets against the
real `SessionController`** with a fake `MediaPlane`. Whole workspace green.

**Done when** — met, except the last row: transport ✅, transactions ✅, dialogs
✅, session timers ✅, re-INVITE's SIP half ✅, robustness sweep ✅, wired to
`session-core` ✅, **met a real SIP endpoint ❌** (lab work, see below).

**What this does not close.** A re-INVITE whose offer actually *changes* is
still refused 488, because `session-core` has no renegotiation path — the SIP
half of P3-2 is closed and the media half is not. There is no UAC, so an expired
session ends the media and sends no BYE. UDP only. No metrics for the door. And
no datagram in any of these tests came from FreeSWITCH, OpenSIPS or a
softphone — pointing `lab/fs_control_drill.sh` at `MSS_SIP_LISTEN` instead of
`lab/sip_shim.py` is the next lab run, and it is what retires the shim.

### 60. Call control over the event stream: park, answer, hang up — ✅ DONE (2026-09-03)

The front door answered every INVITE immediately and never sent a BYE, so an
`END_OF_INTERACTION` from a voice-AI attachment tore nothing down: the caller sat
in silence after the agent's farewell until they hung up themselves. This item
moves the decision to answer and the decision to hang up **out of the media
plane** and onto whatever reads `mss:call-events`, and gives the door the one
client transaction that needs.

**What landed**

- **Two RPCs on `MediaControl`**, both idempotent and both addressing a session
  the way every other RPC does: `AnswerSession(SessionRef) returns (Session)`
  sends the 200 with the SDP answer the media plane already built;
  `HangupSession(HangupRequest) returns (Ack)` sends an in-dialog **BYE** on an
  answered dialog and **480 Temporarily Unavailable** on a parked INVITE, ending
  the media session and publishing `ended` either way. `HangupRequest.reason`
  becomes the `SessionEnded` reason, so an orchestrated hangup is
  distinguishable from a caller's own BYE.
- **`MSS_SIP_ANSWER_MODE`** = `immediate` (default, today's behaviour byte for
  byte) or `parked`: 100, then **180 Ringing**, then `invited` on the stream, then
  wait. **`MSS_SIP_PARK_TIMEOUT_MS`** (default `60000`, `0` disables) refuses an
  unanswered park 480 and ends it.
- **Two new call-event kinds**, back-compatibly: `invited` and
  `end_of_interaction` — the latter published once per session when the
  session's *authoritative* attachment reports it, carrying its reason, and
  tearing nothing down. `answered` and `ended` are unchanged on the wire because
  `reason` is skipped when empty.
- **The one client transaction.** `crates/sip-uas/src/client.rs` is RFC 3261
  §17.1.2 — Timer E doubling to T2, Timer F at 64×T1, Timer K absorbing a
  retransmitted final — and `ClientTransactions::begin` **refuses
  `Method::Invite` by name**, so "MSS never dials" is a property of the type and
  not a convention. `Dialogs::in_dialog_request` builds the BYE from dialog
  state with loose *and* strict routing per §12.2.1.1.

**A defect found and fixed while doing it.** `Event::AckNeverArrived` looked the
orphaned session up by `external_id.ends_with(key.branch())` — an `sip-<Call-ID>`
against a Via branch, which never matches — so an *answered* call whose ACK never
came leaked its media session, despite item 59 documenting the opposite. The door
now keeps `invite_of: TransactionKey → external_id`, and the test that was
missing exists.

**Verified.** 70 tests in `sip-uas` (the mutation sweep now drives responses at
the client layer too, with a live BYE transaction for them to match), 30 in
`sip_front_door` over **real UDP sockets against the real `SessionController`** —
the eleven that were there before unchanged, which is how `immediate` mode is
pinned — 10 in `call-events`, and `convert.rs` for the new message. Whole
workspace green: 356 in `mediaserverd`, clippy clean at `-D warnings`, zero
comments in `crates`.

**What this does not close.** No orchestrator exists in this repository — the
contract is what was built against, and the pairing has not been driven by a
real one. Still no INVITE client transaction, no registrar, no forking. Still no
datagram from a real FreeSWITCH, OpenSIPS or softphone. `end_of_interaction`
rides the controller's 256-deep `broadcast`, so a door that falls behind logs the
lag and may miss one; the Kafka consumption that retires `crates/call-events`
retires that too. No metrics for the door, still. And an IVR between "invited"
and "answered" is the next thing this seam exists for, unbuilt.

## Open defects and soft spots

| # | Item | Where | Severity |
| --- | --- | --- | --- |
| ~~D1~~ | ~~A **mid-call SSRC change** (re-INVITE, transfer, codec renegotiation) does not re-resolve leg identity~~ — **fixed 2026-08-22 (item 14)**: the leg re-enters resolution on a confirmed SSRC change and a control-world task re-queries rtpengine and pushes a fresh map through a bounded queue. Replay-verified only, not yet on a live call. Residual: a transfer that replaces a *from-tag* still needs a re-subscribe, not a re-resolve | `tap_spike.rs`, `tap_plane.rs` | closed |
| ~~D2~~ | ~~`stop_playback` stops **all** playback on the call~~ — **fixed 2026-08-23 (item 27)**: the registry remembers each playback's `target_tag` and `stop_playback` sends NG `stop media` with that `from-tag` (`all: all` only when the playback itself was for everyone). Measured on the lab node with `lab/ng_stop_media_probe.py`, three consistent runs: with a player on each participant, `stop media {from-tag: tagA}` left tagA at **1 packet** (a tail) and tagB still at **75 packets per 1.5 s**; an `all: all` player stopped with one from-tag keeps playing to the *other* participant (1 vs 75), which is why "no target" still maps to `all: all`. **Residual, now measured rather than assumed:** a second `play media` at the *same* from-tag is accepted, and one `stop media` for that from-tag clears the participant entirely (1 packet in a 3 s window) — rtpengine has no playback identifier, so two playbacks aimed at one participant cannot be stopped independently. MSS is now as precise as the protocol allows | `tap_plane.rs`, `registry.rs` | closed (residual documented) |
| ~~D3~~ | ~~`close_attachment` **aborts** the consumer task instead of closing the websocket politely (no `stop` frame)~~ — **fixed 2026-08-23 (item 27)**: `TapPlane::end_attachment` ends the hub subscription and lets the consumer finish, so a WS consumer sends its Twilio `stop` frame and a gRPC consumer gets a `StreamStop` naming the reason ("the attachment was detached" / "the call ended"); a consumer that will not finish inside `POLITE_CLOSE` (2 s) is still aborted, with a warning. `close_session` takes the same path, so an ordinary hangup is polite too. Replay-verified (the task runs to completion instead of being aborted; the `Stop` frame reaches a real gRPC consumer over the wire); not observed against a live consumer | `tap_plane.rs` | closed |
| ~~D4~~ | ~~`WS_TWILIO` and `GRPC_STREAM` attachments are served; `FILE_S3` (phase 2) and `RTP_INLINE` (phase 3) are refused by name~~ — **`FILE_S3` now served (2026-08-22, item 15)**: the recorder is a hub consumer with the frozen identity, pause-segmenting and `object_store` upload. `RTP_INLINE` is still refused by name as an **attachment transport**, and item 33 (2026-08-23) did not change that: an inline leg is a session *kind*, and a consumer reaches one over `GRPC_STREAM`/`WS_TWILIO` like any other — the INJECT direction is P3-3. `RTP_INLINE` may end up never being needed | `tap_plane.rs` | partly closed — the transport stays unused |
| ~~D9~~ | ~~A recording lives in the recording pod's memory until the call ends~~ — **closed but for retention, 2026-08-26 (item 53)**: closed segments spill every `MSS_RECORDING_SPILL_SECONDS` (default 30) and on pause, and with `MSS_RECORDING_SPILL_TO=s3` the journal lives in the **recording bucket** under the reserved `_spill/` prefix, so an adopter on **any** pod reads the dead pod's closed segments back and pads only the unspilled tail — `mss_recording_frames_lost_on_adopt_total` is bounded by one spill interval wherever the session lands, not by the pod. Ownership lives in the manifest: an adopter claims it on open, and a partitioned-but-alive pod's next append is refused and counted (`mss_recording_spill_lost_ownership_total`) instead of corrupting the journal; startup salvage leaves a foreign manifest alone (`mss_recording_spill_foreign_manifests`). The default is still `disk`, where the same-pod guarantee from item 30 holds and a cross-pod adopter still recovers nothing. **What remains: (a) retention** — nothing expires the journals of recordings that finished on no pod, so deploy.md asks the operator for a lifecycle rule on `_spill/` (expire after 7 days); **(b) the live drill** — every claim here is proved against the in-crate fakes only, `tests/minio_upload.rs`'s spill case has never run, and `RECORD=1 lab/pod_kill_drill.sh` **ran 2026-08-27** (lab.md): a SIGKILL after one spilled segment cost **14.77 s of 185.57 s against a 19.25 s allowance**, `frames_lost_on_adopt=0`, four spill segments, one upload, no lost ownership, and an empty `_spill/` namespace afterwards -- the adoption gap dominates the loss, not the spill interval. That run needed two drill fixes first: the gate grepped for prose the daemon stopped printing when item 58 made the store a flag. `MAX_RECORDING` (2 h) is unchanged | `recorder.rs`, `recording_spill.rs`, `registry_keeper.rs` | closed (retention + the drill owed) |
| ~~D11~~ | ~~`StopRecording`/`Detach` **blocks until the upload finishes** (bounded 60 s/90 s), because `observe` needs a live session and a backgrounded upload would lose `UploadCompleted` on every hangup~~ — **fixed 2026-08-26 (item 50)**: the recorder splits into a capture phase that publishes `RecordingStopped` and releases the caller (bounded by `STOP_TIMEOUT`, 5 s, no I/O) and a background upload phase bounded by `MSS_RECORDING_UPLOAD_CONCURRENCY` (default 4). The event is not lost because the registry keeps the session record in a `finishing` state until its uploads settle — not adoptable, not listable, external id freed at once — so the late `UploadCompleted`/the new `UploadFailed` gets the next `seq` in that session's own sequence, gaplessly and in order. Live: `Detach` **11 ms** and `DestroySession` **10 ms** against a `docker pause`d MinIO that held the upload **12.07 s**, `uploads_in_flight 1` with `sessions_live 0`, then `UploadCompleted` at seq 10 after `SessionEnded` at seq 9. **Residual:** `UploadFailed` is replay-proved only, and a `kill -9` between detach and upload still loses the event (the audio is salvaged, per D9) | `recorder.rs`, `recording_uploads.rs`, `registry.rs` | closed (residual documented) |
| ~~D10~~ | ~~Pause is honoured by the recorder only; a paused `WS_TWILIO`/`GRPC_STREAM` attachment keeps receiving media~~ — **fixed 2026-08-23 (item 27)**: the hub checks a per-subscription pause flag before every frame, so `StreamPause` really stops feeding an ASR; skipped frames are counted (`mss_consumer_suppressed_while_paused_total`) and resume starts at the current tap position rather than replaying a backlog. A gRPC attachment paused before its consumer subscribes stays paused when the stream opens. Recorder pause behaviour is unchanged. Replay-verified through `update_attachment`; not observed live | `tap_plane.rs`, `hub.rs` | closed |
| ~~D5~~ | ~~Event delivery is **at-most-once**; a broker outage drops events~~ — **fixed 2026-08-22 (item 13)**: bounded retry backlog, order preserved, drop-oldest counted. Now **at-least-once**, so the translator must dedupe by `(external_id, seq)`; a backlog past its 8192 cap or a pod death still loses events | `event_pump.rs` | closed |
| D6 | `play media` `from-tag` semantics are **unmeasured** — architecture §6's claim was retracted after the instrument turned out to be broken (see lab.md correction) | docs + lab | low, but §6 must not be trusted until re-probed |
| ~~D7~~ | ~~Jitter buffer: fixed target depth, no adaptive sizing, no timestamp-aware gap handling, silence instead of real PLC~~ — **fixed 2026-08-22 (item 17)**: adaptive depth from the RFC 3550 estimate, timestamp-aware silence gaps, comfort noise accounted, G.711 Appendix I-shaped PLC, restart on SSRC change. Replay-verified across the impairment matrix, and since item 19 also **on a real link** in the soak — 1%/5% injected loss reported as 1.06%/5.01% with `frames_concealed` equal to `jitter_lost` to the packet, reorder and ±35 ms jitter costing zero loss and zero late drops. **Impaired with `tc netem` on the tap link 2026-08-28** on a Debian host whose kernel has `sch_netem` as a module (lab.md): the whole matrix ran at the strong injection point, `SOAK_EXIT=0` with zero violations, `frames_concealed` equal to `jitter_lost` in every row, and the three rows the endpoint could never produce -- **131 duplicates reaching the tap** (rtpengine absorbs them upstream of the subscription, so this needed netem), **late drops** at 3 and then 7 as the delay widened to 120 ms +- 60 ms, and loss accounted **exactly**: the qdisc dropped 12 packets and MSS reported 12. That exactness exposed the matrix's `burst` profile as inert -- `loss 10% 50%` is netem's correlation form and dropped 0.11% against a nominal 10% -- so it is `loss gemodel 3.3% 30%` now, measured at 10.2% dropped with 1,073 lost and 1,073 concealed. Still open: the concealment has never been judged perceptually, and nothing has impaired the **call legs**, only the tap link | `jitter.rs`, `pipeline.rs`, `plc.rs` | closed |
| D8 | `owner_pod` is a config string; real placement and load-aware scheduling do not exist | `main.rs` | low until multi-pod |
| ~~D12~~ | ~~**NG cookies repeat across sessions on one pod**: `CookieSequence` restarted its serial at 0 and `TapPlane` binds a new `NgTransport` per session, so every session's first command was `<prefix>-0`. Two sessions inside rtpengine's duplicate-cookie reply-cache window get the *same cached subscribe answer*, and the second tap receives **no media at all** while looking healthy~~ — **fixed 2026-08-22 (item 10)**: the serial is process-wide, unit-pinned and lab-proved before/after | `ng_transport.rs` | closed — was **high**, it silently broke every second tap within a minute |
| ~~D15~~ | ~~An adopted attachment loses its **negotiated format**: `rebuild` passes `format: None`, so a consumer that attached as L16/16k comes back at the session default.~~ — **fixed 2026-08-23 (item 26)**: `PersistedAttachment.format` (`Option<PersistedFormat>`, `serde(default)`, the wire shape used for the other persisted enums) is written every keeper tick and replayed on adoption; a record without it still decodes and still means the default. Unit-tested (roundtrip, legacy record, an L16/16k gRPC consumer and a default WS consumer re-opened side by side on the adopting pod) and run against the lab's real Redis; **never observed live** — that needs a pod kill with a gRPC L16 consumer attached | `session_store.rs`, `registry_keeper.rs` | closed |
| ~~D14~~ | ~~**A dead pod's rtpengine subscription is never torn down.**~~ — **fixed 2026-08-23 (item 25)**: `PersistedSession` now carries the tap's `to-tag` (`subscription_tag`, `serde(default)` so older records still decode), the adopter sends NG `unsubscribe` for it **before** re-subscribing (after winning the atomic claim), and a pod that loses its lease destroys the session locally so a partitioned-but-alive owner unsubscribes its own tap instead of double-tapping. `upsert` also stopped rewriting the lease key unconditionally (now `SET NX`) — it had made a lease unloseable, so the partitioned case could never be detected. New counters `mss_registry_orphans_unsubscribed_total`, `mss_registry_orphans_still_subscribed_total`, `mss_registry_surrendered_total`. **Verified in unit tests, against a fake rtpengine socket (the `unsubscribe` bytes) and against the lab's real Redis — not re-measured on a live pod kill**; the residual is that a refused `unsubscribe` still leaks one tap, counted rather than retried | `session_store.rs`, `registry_keeper.rs`, `tap_plane.rs` | closed |
| ~~D23~~ | ~~**A padded recording-group member lost its pad's worth of audio off the tail.** `Segmenter::close_segment` subtracted the closed frames from `segment_start` (the lead-silence offset) *and* advanced `anchor_ms` by the same frames, so every spill moved a late joiner's timeline forward by the pad twice~~ — **found and fixed 2026-08-24 (item 41)**: the anchor now advances only by `frames - segment_start`. Invisible to every earlier test because an ungrouped recording has `segment_start == 0` and item 29's group drill (5 s stagger, 20 s run) never reached the 30 s spill. Live in the conference drill: `party-c.wav` **55.88 s against 66.16/66.24** before, **72.10 against 71.96/72.02** after, with the 10.66 s pad still at the front. Guarded by `a_padded_member_keeps_its_whole_tail_across_a_spill`, which fails by exactly the lead if the fix is reverted | `recorder.rs` | closed |
| ~~D20~~ | ~~**A room recording belongs to a member, not to the conference.**~~ — **fixed 2026-08-27 (item 55)**: `CreateSession{kind=MIX, group=<conference>}` opens the room itself as a session with no leg. The conference stamps its own open (`opened_at_wall`) and publishes its full sum into a **room hub** on the conference clock; a `FILE_S3 only=mixed` attachment on the room session records from that open to the room's close, so a member leaving no longer ends the object — and `group_anchor_for` now gives the per-participant group the **same** anchor, which makes the two shapes sample-aligned whenever they are attached. The room session ends on `EndSession` or by itself once the conference has held a member and emptied (`MSS_CONFERENCE_LINGER_SECS`, default 0), uploading its object; `DescribeSession` on it reports the conference's `opened_at_unix_ms` and its members, and every member names the room back (`ConferenceView.room_session`). **Verified over in-process UDP sockets and the recording fakes only** — including the room object outliving the member who left, and a room recording opened before anybody joined; `lab/conference_drill.sh` is rewritten to record through the room session, with a **leave** phase and a length comparison that is exactly this defect. **That run happened 2026-08-27** on a second box (lab.md, "The whole suite on a second box"): every assertion passes, the room object outlives the member who left by exactly the 6 s she was gone, and the members who stayed agree to **60 ms against a 600 ms bar**. It also found what the rewrite missed -- the room **monitor** was still attached to party A, the member the leave phase removes, so the server ended its stream and the drill could not satisfy its own `leave/monitor` expectation; the monitor now hangs off the room session. **What remains:** that run, and the residuals in item 55 — an INJECT on the room is a prompt path rather than a full-duplex one, a room-opened conference fixes its format from the pod's tap format, and a room is still pod-local and never adopted | `tap_plane.rs`, `conference.rs` | closed |
| ~~D21~~ | ~~**DTMF digits never reach the event bus.** A tapped or inline leg's digits are delivered to consumers (WS `dtmf` frames, gRPC `DtmfFrame`) and counted in `mss_ingest_dtmf_digits_total`, but nothing publishes `Observation::Dtmf`, so `mss.events` carries no digit~~ — **fixed 2026-08-26 (item 48)**: every press on a tap leg or an inline leg is published as a session-level `MediaEvent` carrying `digit`, `track` (attribution-aware, so `leg_a`/`leg_b` when unproven), `duration_ms` (through the negotiated RTP clock) and the event's `rtp_timestamp`. **No capability and no consumer**: unlike `SpeechReport`, which a consumer *claims* and which is gated on `CAPABILITY_EVENTS`, a digit is a property of the call MSS decoded itself, so it goes out whenever the session exists. RFC 4733's three end retransmissions stay one event. The capture thread hands presses to the control plane over a bounded lock-free queue that counts refusals (`mss_dtmf_events_dropped_total`) rather than blocking the media path. Live-proved with no consumer attached (`lab/dtmf_event_drill.sh`). **Residual, by design:** MSS interprets no digit — no menu, no collection, no inter-digit timer (item 40: conference control is API-first) — and nothing rate-limits presses beyond the queue's drop counter | `digits.rs`, `tap_spike.rs`, `tap_plane.rs`, `registry.rs` | closed |
| ~~D22~~ | ~~**Member state has no owner and no lease**~~ — **read-back 2026-08-26 (item 49), lease 2026-08-27 (item 56)**: `DescribeSession` on a member session reports its `mute`/`deaf`/`hold` (now with `*_expires_in_ms` beside each), its mix routes and the room's members, and `member_state_ttl_ms` on the same request that sets a flag `on` says how long it holds without a refresh — `MSS_MEMBER_STATE_TTL_SECS` is the deployment default, a 500 ms control-world sweep lifts what runs out **through the same `Conference::control` path an explicit `off` takes**, and the lift is published as `MemberControlled{cause=EXPIRED}` and counted by `mss_conference_member_state_expired_total`. So a controller that dies between `on` and `off` now costs one lease rather than the life of the conference. **Residual:** the lease is pod-local (an `Instant` in one pod's memory: it neither survives a pod loss nor moves with a member), there is still **no owner** — item 40 rejected the attachment and this API does not model the caller, so two controllers muting one member race and the last lease wins — a flag lifts up to one sweep (500 ms) late, and with no TTL, still the default, member state holds until an `off` exactly as before. Verified by unit tests on real sockets, and **live 2026-08-27** with `MUTE_TTL_MS=12000` through `conference_drill.sh`: the mute phase sent no `off` at all and `mss_conference_member_state_expired_total` moved **0 -> 1**, so the sweep lifted the lease on its own and the member was audible again in the next window | `conference.rs`, `tap_plane.rs`, `registry.rs` | closed (residual documented) |
| ~~D16~~ | ~~**A recording group is one pod's memory.**~~ — **fixed 2026-08-27 (item 54)**: the group moved into the session store. `mss:group:<account>/<group>` (`SET NX`, so two first-members on two pods agree on one recording and one anchor) plus a `mss:group:…:members` hash keyed by object key and valued by owner pod (`HSETNX` is the duplicate-participant refusal, and it can now name the pod holding the seat), both expiring at `MAX_RECORDING + 1 h`. A member may attach on **any** pod, `registry_keeper::rebuild` **rebuilds** a grouped attachment with its group instead of skipping it (`grouped_not_adopted` is deleted), and a reused group name joins the existing group instead of opening a second half-recording under the same prefix. The group's open instant became **wall-clock** (`SystemTime`) so two pods can share it, which makes cross-pod alignment as good as the nodes' NTP — deploy.md says so beside the new key table. A store MSS cannot read is a **refusal** of the grouped attachment, counted, never a silent local group. **Verified against `MemorySessionStore` and the in-crate recording fakes only** — including two planes sharing one store whose second pod's object opens with the lead silence back to the first pod's anchor; the env-gated Redis `SET NX` race test has never run, and `PODS=2 lab/group_recording_drill.sh` **ran 2026-08-27** (lab.md) against the lab's real Redis: the second member joined on the **other pod**, was padded back to the anchor the first pod opened (`lead_silence_ms` 5176 against 183) so both objects came back the same length, and the duplicate-label refusal crossed pods naming the pod that holds the seat, `mss_recording_group_joins_refused_total` 0 -> 2. **What remains:** those two runs, and placement (D8) — which is now an optimisation rather than a correctness requirement | `session_store.rs`, `tap_plane.rs`, `registry_keeper.rs`, `recorder.rs` | closed (two runs owed) |
| ~~D13~~ | ~~`StreamStart` (and the Twilio `start` frame's `tracks`) advertises `["customer","agent"]` for `TrackSelector::All`, but a silent `mixed` track is delivered too~~ — **fixed 2026-08-23 (item 27)**: the hub selection split into `All` (every track, including `mixed`) and `Speakers` (customer + agent). Consumers get `Speakers`, so delivery matches the advertisement exactly; the **recorder keeps `All`** because injected bot speech belongs in the recording. The frozen Twilio start frame and `StreamStart.tracks` were not touched — the delivery was brought in line with them. A consumer that wants the injected track can still ask for it by name (`TrackSelector::Only(Mixed)`). Replay-verified | `hub.rs`, `tap_plane.rs` | closed |
| ~~D17~~ | ~~**Leg labels invert when the caller's from-tag is not given.** With `from_tags` unspecified (`-`), `TapPlane` labels the two legs in the order rtpengine's `query` returns them, and in the two-node drill that put **FreeSWITCH's** tag first — so `customer` and `agent` were swapped in the recording and in the `tracks` a consumer sees~~ — **fixed 2026-08-26 (item 47)**: the order was in fact `BTreeMap` order, i.e. lexicographic by tag. MSS now refuses to name a direction it cannot back up: `attribution=explicit` when a from-tag was supplied (and for every inline leg), `inferred` when rtpengine's per-participant `created` seconds strictly order the legs, `unknown` otherwise — and under `unknown` the gRPC tracks, the event payload tracks and the recording object keys are `leg_a`/`leg_b`, with a WARN log, a `LegsAttributed` event and `attribution` on `DescribeSession` and on every event envelope. The frozen WS Twilio names never move. Live-proved on a call built to invert (callee tag sorting first): `unknown` + `leg_a`/`leg_b` with no from-tag, `explicit` + `customer`/`agent` with one. **Residual, and it is the vendor's:** `created` is stamped per *dialogue*, so the two legs of one call always tie — `inferred` cannot fire for a two-party call on rtpengine 14.1.1.8, and an integrator who needs speaker attribution **must** pass the caller's from-tag (`docs/deploy.md`, "Leg attribution") | `tap_plane.rs`, `attribution.rs` | closed (residual is the vendor's) |
| ~~D18~~ | ~~**Recording-group members are not time-aligned.** Each member's file anchored on **its own first frame**, so a late joiner's file started at its join moment and two members of one group differed in length (90.32 s vs 90.26 s in the two-node drill), leaving reassembly to the event timeline~~ — **fixed 2026-08-23 (item 29)**: a recording group stamps `opened_at` when its first member joins and every later member's segmenter pads its first segment with silence from that anchor to its own first frame (`Segmenter::lead_with_silence`, reported as `lead_silence_frames`), padded once per recording so pause/resume cannot double-count it. Replay-verified (late joiner padded, two members equal length, the pause interaction, and a WAV read back out of a fake sink) **and live**: the drill's staggered re-run had bob join 5 s late and his object came back opening with 5016 ms of zeros, 25.116 s against alice's 25.030 s. **Residual:** equal length still assumes the members stop together — the 86 ms here was D11's blocking detach, closed by item 50, and D16 keeps the anchor inside one pod's clock | `recorder.rs`, `tap_plane.rs` | closed (residual documented) |
| ~~D19~~ | ~~A consumer cannot tell MSS that the caller started speaking: `Registry::report` had no caller outside tests~~ — **fixed 2026-08-23 (item 28)**: `ConsumerToServer.SpeechReport` on the gRPC `MediaStream` stream (kind `STARTED`/`PARTIAL`/`FINAL`/`END_OF_UTTERANCE`/`END_OF_INTERACTION`, track, text, confidence, the consumer's own `observed_at`) reaches `Registry::report`, gated on `CAPABILITY_EVENTS` — an attachment without it gets `PERMISSION_DENIED` and the stream ends, the same protocol-violation shape as an unprivileged `inject`. Proven on a live tapped call: `lab/barge_drill.sh` now triggers on a real `SpeechReport` and measures cut-through p50 3.54–3.98 ms (item 5). **Residual, accepted:** the `WS_TWILIO` dialect cannot report speech — its bytes are frozen (Article VII) and it carries no such message, so a WS consumer's only barge stays the `clear` message's direct rtpengine `stop media` (unevented; the D2 shape). Interactive voice-AI on WS should attach over gRPC instead | `stream.rs`, `convert.rs`, `session-core/registry.rs` | closed |

## Integration handoffs (deployment-gated)

Everything below is **out of scope for this repository's code** and cannot be
closed from a lab: each item needs a deployment — its SIP proxy, its
FreeSWITCH, its metal, its tenants, its sign-off. They are listed here so no
future session mistakes them for unfinished engineering. MSS is a generic
media plane: any deployment whose media anchors in rtpengine can integrate it,
and the compatibility surfaces (the Twilio Media Streams dialect, the
`mod_audio_fork` event names, the TelCompat façade) are **optional adapters**. The
reference deployment named in [CLAUDE.md](../CLAUDE.md) appears below only as
the worked example of each handoff.

| # | Handoff | What MSS already provides | What the integrator owes |
| --- | --- | --- | --- |
| H1 | **An event consumer for `mss.events`** | typed `MediaEvent` on one Kafka topic, keyed by `external_id`, gapless per-session `seq`, at-least-once since D5 (so dedupe by `(external_id, seq)`), `legacy_eligible` marking the authoritative attachment | a consumer that renders those events onto whatever the existing control plane already understands. *Worked example:* the reference deployment's translator, which maps them onto its legacy positional `eventTopic` format — written, awaiting review and merge in its own repository (item 1) |
| H2 | **The deployed rtpengine version check** | `subscribe` verified against lab rtpengine 14.1.1.8; `lab/kernel_probe.sh` prints the finding on any host, and `lab/preflight.sh` prints it as one `rtpengine_version` line, and [deploy.md](deploy.md#first-day-on-real-gear--an-ordered-runbook) step 0 makes reading it the stopping condition when `ng_subscribe` fails | read the version from the process, the package or rtpengine's CLI interface (`--listen-cli`) on the target host. **It cannot be asked over NG** — rtpengine has no NG `version` command, in this build or upstream (item 23). If the deployed build lacks `subscribe`, the ingest model needs an upgrade path first |
| H3 | **rtpengine-side per-tap cost on the target metal** | the MSS-side cost is measured, and since item 57 the rtpengine side is on **Prometheus**: every health probe samples NG `statistics` and exports `mss_rtpengine_tap_kernel_verdict{node,verdict}`, `mss_rtpengine_relayed_packets_kernel`/`_user`, `mss_rtpengine_media_kernel`/`_userspace`/`_mixed`, `mss_rtpengine_transcoded_media`, `mss_rtpengine_sessions_live` and `mss_rtpengine_sample_age_seconds`, with `MssTapsFellOutOfKernel` watching them. `lab/kernel_probe.sh` plus the read-only checklist in architecture §8.1 is the second instrument, sequenced as [deploy.md](deploy.md#first-day-on-real-gear--an-ordered-runbook) step 3 | read the three moments — baseline / taps-with-transcode / taps-without-transcode — off the metrics first, then confirm on the real box with the shell probe (it sees `controlstatistics.proxies` and the per-interface blocks, which MSS does not model). This sets the rtpengine capacity plan. D14 is fixed (item 25), so a pod restart mid-probe no longer pollutes the numbers |
| H4 | **End-to-end barge-in through the integrator's stack** | every MSS-owned hop is measured: consumer `SpeechReport` → bus → `StopPlayback` at **p50 3.5 ms** (item 5), and inline `Clear` → silence at the peer's ear at **p50 12.2 ms**, one ptime (item 35); [deploy.md](deploy.md#first-day-on-real-gear--an-ordered-runbook) step 6 says to measure the whole path while the inline leg is first bridged | the tail is theirs: their event consumer (H1) and their prompt player. Measure the whole path against their perceptual budget |
| H5 | **The SIP proxy's B2B integration for inline legs** | `CreateSession{kind=INLINE, sdp_offer}` returns a real SDP answer and the leg speaks and listens on real sockets; a `group` seats it in a conference; [deploy.md](deploy.md#high-availability-what-is-adoptable-and-what-is-not) records that an inline leg does **not** survive a pod loss, so recovery is call-control's. **A worked example of the plumbing now exists in-repo:** `lab/sip_shim.py` is a minimal UAS that answers a SIP INVITE with an MSS inline leg, and `lab/fs_control_drill.sh` drives a real FreeSWITCH B2BUA into it with `bypass_media` — four FreeSWITCH channels, `_undef_` RTP counters on every one, two callers hearing each other through MSS (lab.md, 2026-08-29) | the same plumbing from **their** proxy or B2BUA. Since [item 59](#59-the-sip-front-door-transactions-dialogs-and-session-timers--done-2026-08-30) MSS answers SIP itself on `MSS_SIP_LISTEN`, so the shim is no longer the only route in and the parts it never did — transactions, dialogs, session timers, in-dialog BYE — are now the media plane's. Still theirs: registration and authentication, and a re-INVITE that **changes** the offer (refused 488, P3-2's media half), transfer and hold. The lifecycle rule that a lone party is hung up is call control's too — [fs-orchestrator](https://github.com/lazyboson/fs-orchestrator) is the worked example, one inbound event socket for the whole switch |
| H6 | **FS byte-parity against real production recordings** | item 31 measured a live call recorded both ways: container, channel layout and rms agree exactly, and a re-aligned 2 s window agrees on 1.0000 of samples at mean diff 0.6/32768; [deploy.md](deploy.md#first-day-on-real-gear--an-ordered-runbook) step 4 walks the recording checks, frozen identity first. It also established that **byte-parity at a fixed offset is not an achievable bar** — the two recorders conceal independently, so the inter-file offset wanders | a **two-party** comparison on their FreeSWITCH, with their codec, their pause contract, and a human listen. The lab's write side plays silence, so only one channel was truly compared |
| H7 | **Retiring the legacy media path** | the workloads are served: fan-out, recording, inline legs, conferences, monitor/whisper/barge | the tenant decision to turn the old media bugs off (`record_session`, the audio fork, the conference-per-AI-interaction dummy leg), and to decommission whatever gateway service they run today. Rollback stays config-only while both paths are installed |
| H8 | **A pilot, a stability period and UX sign-off** | metrics on `MSS_METRICS_LISTEN` with alert rules in `deploy/`, a soak harness (`lab/soak.py`) and an impairment matrix, plus deployable manifests: `deploy/k8s/` with both network shapes, probes, a drain-safe grace period and a `ServiceMonitor`/`PrometheusRule` generated from those alert rules (item 46) | run flagged tenants for the agreed period; watch the conference defects D16/D20/D21/D22 in the field, and confirm that the integrator's control plane really passes the caller's from-tag — without it every tap is `attribution=unknown` and its tracks are `leg_a`/`leg_b` (item 47); get a human to judge audio quality, which no automated assertion in this repository claims to have done |

## Waiting on other people (M2 close-out)

These are not code and have blocked since Phase 0. The first two are the
Phase-0 face of handoffs **H2** and **H3** above — recorded twice on purpose,
once as a milestone blocker and once as an integration handoff:

1. **Production rtpengine version check** — lab is 14.1.1.8 with `subscribe`
   working; the deployed version is unverified. If it lacks `subscribe`, the
   whole ingest model needs an upgrade path first. **It cannot be asked over
   NG** (item 23, 2026-08-23): rtpengine has no NG `version` command, in this
   build or upstream. Read it from the process, the package, or rtpengine's CLI
   interface (`--listen-cli`); `lab/kernel_probe.sh` prints the same finding so
   whoever visits the host is not left guessing.
2. **rtpengine-side per-tap cost** — MSS-side cost is measured; the userspace
   copy cost on the rtpengine host at 100/500/1000 taps is not, and it sets
   the rtpengine capacity plan. Take `lab/kernel_probe.sh` (item 23) on that
   visit: `relayedpackets_kernel` vs `_user` and `media_kernel` vs
   `media_userspace` across baseline / taps-with-transcode /
   taps-without-transcode is the measurement, and architecture §8.1 has the
   full read-only checklist. D14 is fixed (item 25), so a pod restart during the probe no longer
   pollutes it — though the fix has not been re-measured live.
3. **OpenSIPS → Redis call→node discovery** — **MSS's half is built
   (item 51, 2026-08-26); what is left is one config block on someone else's
   proxy.** Set `MSS_DISCOVERY_REDIS_KEY_PREFIX` and a `CreateSession` that
   names no node reads `<prefix><Call-ID>` from Redis before falling back to
   `MSS_RTPENGINE_NODE`; hits/misses/errors are counted and a lookup failure is
   always a fallback, never a refused session. The proxy side is a
   `cache_store`/`cache_remove` pair (`cachedb_redis`) copy-pasteable from
   [deploy.md](deploy.md) — and the lab proved the whole path live with the
   pod's default node pointed at a black hole. It stays listed here because
   **only the integrator can add it to a production proxy**, and because the
   pinned `opensips/opensips:3.4` image ships no `cachedb_redis.so`, so the
   module must be present on whatever build the deployment runs. Still an
   optimisation, not a prerequisite: MSS resolves participants itself through
   rtpengine's `query` when nobody publishes the map.

## Later phases

**Phase 2 — Recording** is **code complete (item 15, 2026-08-22)**: the
stereo segmenter, the `${accountID}/${recordingID}.${format}` identity, the
`recordStart/recordPause/recordStop/uploadCompleted` callbacks with pause =
segment + defer + accumulate, and direct upload to S3/MinIO all landed and are
verified against real object storage from a synthetic hub. What the phase
still owes: the tenant decision to turn `record_session` off. The live tapped
call landed with item 10, and FS byte-parity was measured against a real FS
recording in item 31 (container/layout/rms exact, a re-aligned window agreeing
1.0000 at mean difference 0.6; a two-party comparison on production FS is the
residual). D1 (item 14) is fixed for a mid-call SSRC change on
the same from-tag; a transfer that replaces a tag still lands on elimination,
so a recording of one is only as right as that.

**Phase 3 — Interactive media** is **code complete and lab-verified**: item 32
built the sans-IO `PlayoutPacer`, item 33 made `CreateSession{INLINE, sdp_offer}`
bind a socket, answer the offer (PCMU/PCMA + telephone-event) and pump both
directions — the peer into the tap hub as the `customer` track, queued PCM back
out one paced packet per ptime, with `StopPlayback` as the barge flush. Item 34
made it full duplex: an INJECT attachment on either transport streams into that
queue continuously, `Clear` flushes it and `Mark` is acked when the marked audio
has drained. Item 35 then measured the cut-through against a **real RTP peer**
over 20 live iterations — `Clear` to the first silent packet at the peer's ear
**p50 12.2 ms / p95 20.4 ms**, one ptime, as the pacer's design predicts — with
egress at 50.19 pkt/s, no sequence breaks and the tap still feeding its consumer.
What Phase 3 still owes is not code: no leg here has met a **SIP** endpoint, so
a proxy's B2B integration and an end-to-end barge through an integrator's own
stack remain deployment-gated.

**Phase 4 — Full media plane** is **code complete and lab-verified**: the N-way
mixer, with monitor/whisper as attachments and playbacks rather than conference
tricks. Its core landed with item 36: `media-core`'s `MixMatrix` mixes N
contributors into M listeners with minus-self defaults, per-pair gain, and
monitor/whisper/barge/mute/deaf all expressed as rows and columns of the one
matrix. Item 37 gave a conference one clock — inline legs sharing a `group` share
a mix, on one owner thread, with the room's full sum published to every member's
hub. Item 38 named monitor, whisper and barge as metadata verbs on existing
nouns (`only=mixed`, `mix_target=<member>|all`), item 39 recorded a conference
both ways at once (one mono object for the room, one per participant through a
recording group), and item 40 added the member-control tail
(`member_mute`/`member_deaf`/`member_hold`, room prompts, `mix_source=leg`) plus
the generic feature list and the adapter parity table in architecture.md
Appendix B. Item 41 judged all of it on **real sockets**: three container peers
at 440/880/1320 Hz in one conference, **twenty tone-per-phase assertions green**
at a ≥30:1 margin, and both recording shapes in MinIO at once. What the phase
owes is deployment-gated (a SIP proxy's B2B leg into a conference, a pilot) plus
the defects it left open: D16 (closed by item 54) and D20 (closed by item 55),
with D21 closed by item 48 and D22 closed by items 49 and 56 (read-back, then a
lease; no owner, by design).
