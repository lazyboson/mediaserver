# Tasks — what is done, what is next

Living work list. [roadmap.md](roadmap.md) holds the *why* and the phase exit
criteria; this file holds the *what next*, ordered, with a definition of done
for each item. Update it in the same PR that changes the state of an item.

Status as of **2026-08-22**.

## Milestones

| Milestone | Scope | State |
| --- | --- | --- |
| **M1 — scaffold** | workspace, sans-IO cores (RTP, G.711, DTMF, jitter), NG bencode, consumer dialects, two-world daemon skeleton, watchdog | ✅ done (2026-08-13) |
| **M2 — Phase-0 spike** | real NG subscribe against lab rtpengine, both legs jitter-buffered to WAV, per-tap cost | ✅ **code done**; 3 org-side items open (below) |
| **M3 — fan-out hub** | per-session pub/sub, N consumers, WS-Twilio adapter, pause/resume/send_text parity | ✅ done |
| **M4 — control plane** | `MediaControl` gRPC, session state machine, Kafka events, Redis registry, tenant-flag pilot | 🔶 **code complete** — pilot gates: translator merge (external), barge-in number (item 5). The gRPC lab proof (item 10) and the **live pod-kill drill (item 11, gap 14.41 s)** are both **done 2026-08-22**; the drill left D14 open (orphan subscription after a pod death) |
| **M5 — recording (Phase 2)** | per-leg taps → stereo segmenter → S3, identity + callback contract, dual-recording | 🔶 **code complete (2026-08-22)** — a live tapped call recorded end to end to a real MinIO, callbacks read off `mss.events` (2026-08-22, item 10's drill); owed: FS byte-parity sign-off (harness exists) |
| M6+ | Phases 3–4 (interactive media, full media plane) | ⬜ not started |

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

## Next up — ordered

### 1. the legacy controller event translator — ✅ WRITTEN, awaiting review and merge
**State (2026-08-17):** implemented on the the legacy controller branch
`feature/legacy-translator` as `pkg/the legacy verb API/msstranslator`, **local and
uncommitted by instruction**. Pure `Render` plus a Kafka consumer on its own
group (`mssEventTranslator`), wired into `cmd/the legacy gRPC server` behind
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

### 1b. (original description, for reference) the legacy controller event translator
**Where:** the `the legacy controller` repo, not here (architecture §5.4 — the positional
format *is* `constants.MapKeyIndex`, a Go constant table; encoding it in Rust
would couple MSS to a file that changes without our knowing).
**What:** ~200-line Go consumer: read typed `MediaEvent`, drop anything with
`legacy_eligible == false`, render `mod_audio_fork::*` names into
`Events{repeated string}` on `eventTopic`, preserve the UUID key and OTel
context.
**Why now:** MSS publishes events nothing consumes yet. Until this exists no
tenant can be flipped, so it gates everything else in Phase 1.
**Done when:** `the application server` drives `the legacy stream fsm` from an MSS-tapped call with
`mod_audio_fork` uninvolved, and the event-name mapping table in
`proto/mediacontrol.proto` matches the shim one-for-one.

### 2. `TelCompat` façade — ✅ DONE (2026-08-17)
Landed in `crates/control-api/src/telcompat.rs` with
`proto/telcompat.proto` declaring `protos.TelService` so the method paths match
the legacy controller's byte for byte. Serves stream/recording/playback verbs onto the nouns,
one test per mapping row, both surfaces on one port, proven over a real socket
with a generated the legacy controller client. `StartCallTranscription` returns `UNIMPLEMENTED`
by design (the ASR endpoint is not in its request message).
**Blocked before a tenant can be flipped:** a TelCompat session has only the
channel uuid, so it needs the OpenSIPS→Redis discovery map (M2 item 3) to
resolve call-id and tags before it can tap.

### 2b. (original description, for reference) `TelCompat` façade
**Where:** `crates/control-api`.
**What:** a second gRPC service reusing `the legacy verb API.proto` message shapes
verbatim — `StartStream`, `StopStream`, `StreamPause`, `StreamResume`,
`StreamSendText`, `StreamPlayFile`, `StartCallTranscription`,
`StartRecording`, `StopRecording` — translated onto Session/Attachment/
Playback per the §5.6 table, setting `authoritative` from the verb that
created the session.
**Why now:** it is the migration switch: a tenant flag routes the legacy controller to
`the legacy gRPC server` or `MSS` with no client change and rollback by config.
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

### 5. Barge-in cut-through measurement
**Where:** lab.
**What:** measure `partial_speech_result` → `StopPlayback` end to end
(consumer → MSS → Kafka → translator → the legacy controller → FS `break`), since MSS is a
single writer per session and therefore on that critical path (§5.5).
**Why now:** it is a **Phase-1 exit criterion**, and if the Kafka hop is too
slow the fallback (a gRPC stream for speech events only) is a design change,
not a tuning knob — better known early.
**Done when:** a number exists, compared against the barge-in budget.

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
stays with the WS bridge. **Still to prove: a live tap over gRPC in the lab**
— every consumer today speaks WS, so this ran only against the fake plane.

### 7. Auth on attachments — ✅ DONE (2026-08-20)
`crates/control-api/src/auth.rs`: `AuthPolicy`, a shared bearer secret from
`MSS_AUTH_TOKEN`. It is a tonic interceptor on `MediaControl` (missing or
wrong `authorization: Bearer …` → `UNAUTHENTICATED`) and verifies
`ConsumerHello.token` on `MediaStream` (constant-time comparison). Unset env
= open lab mode, logged loudly at startup. **`TelCompat` is deliberately not
intercepted**: its contract is byte-identical the legacy controller clients with no client
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

### 9. Opus output
**Where:** `media-core` (+ a dedicated FFI wrapper crate).
**What:** Opus encode via `audiopus` (Article XI: adopt libopus, never
reimplement). **The build trade must be decided first:** `audiopus_sys`
compiles libopus with cmake/gcc, which breaks the hermetic
pure-Rust build the repo chose twice already (protox over protoc, rskafka
over rdkafka). Options: accept the toolchain in CI + Dockerfile, or a
prebuilt static lib, or a pure-Rust decoder-only stopgap.
**Why later:** no consumer asks for Opus yet; L16/16k covers the ASR
vendors we know about.
**Done when:** a consumer can request Opus and get it, verified by replay,
with the build documented in CI and the Dockerfile.

## The road from here — ordered handoff

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
**Not covered:** the D9 half of a pod death — a recording buffered in the
dead pod's memory is still lost, and the adopter starts a new segment; this
drill attached no recorder. And the gap is a lab number on an idle box; the
soak suite (item 19) should re-measure it under load.

### 12. Barge-in cut-through measurement
Item 5 above, unchanged — still gated on the the legacy controller translator merge
(external). It is a **Phase-1 exit criterion**; if the Kafka hop misses the
budget, the fallback (a gRPC stream for speech events only) is a design
change better known early.

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
duplicate a record, so **the the legacy controller translator must treat `(external_id,
seq)` as idempotent**. Checked in the translator's source on the legacy controller branch
`feature/legacy-translator` (`pkg/the legacy verb API/msstranslator/consumer.go`,
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
**Not verified at all:** FS byte-parity —
`lab/recording_parity.py` exists and was exercised on the drill's own output
and on perturbed copies, but **has never seen a FreeSWITCH recording**. That
comparison is the remaining human step for the Phase-2 exit criterion.
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
The original item 9 framing. `opus-rs` ships an encoder too, so this is now
mostly plumbing, but no consumer has asked and ingest is what WebRTC needs.

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
     eBPF is unjustified complexity. **Fix D14 first** or orphaned taps
     will pollute the measurement.
  2. Probe whether a subscription is kernel-forwarded, and **whether
     dropping the transcode request is what decides it** — that is the
     hypothesis item 20 exists to make testable, since the kernel module
     has no codec and therefore cannot transcode. On a host that can load
     the module: `cat /proc/rtpengine/<table>/list` at baseline, after a
     subscribe **with** transcode, and after one **without**, comparing
     `num_destinations` on the target entries. Run rtpengine with
     `--no-fallback` so it refuses to start rather than silently
     degrading to userspace. **This cannot be probed in this lab** —
     `--table=-1`, no `/proc/rtpengine`, and no kernel headers to build
     the module against. On a production-shaped host that already runs
     the module it needs **no config change and is read-only**, so bundle
     it with open items 1 and 2 above in one visit.
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

## Open defects and soft spots

| # | Item | Where | Severity |
| --- | --- | --- | --- |
| ~~D1~~ | ~~A **mid-call SSRC change** (re-INVITE, transfer, codec renegotiation) does not re-resolve leg identity~~ — **fixed 2026-08-22 (item 14)**: the leg re-enters resolution on a confirmed SSRC change and a control-world task re-queries rtpengine and pushes a fresh map through a bounded queue. Replay-verified only, not yet on a live call. Residual: a transfer that replaces a *from-tag* still needs a re-subscribe, not a re-resolve | `tap_spike.rs`, `tap_plane.rs` | closed |
| D2 | `stop_playback` stops **all** playback on the call: rtpengine's `stop media` targets a participant, not a playback id | `tap_plane.rs` | low until multiple concurrent playbacks exist |
| D3 | `close_attachment` **aborts** the consumer task instead of closing the websocket politely (no `stop` frame) | `tap_plane.rs` | low, but consumers see a truncated stream |
| ~~D4~~ | ~~`WS_TWILIO` and `GRPC_STREAM` attachments are served; `FILE_S3` (phase 2) and `RTP_INLINE` (phase 3) are refused by name~~ — **`FILE_S3` now served (2026-08-22, item 15)**: the recorder is a hub consumer with the frozen identity, pause-segmenting and `object_store` upload. `RTP_INLINE` is still refused by name and stays Phase 3 | `tap_plane.rs` | partly closed — inline is phase 3 |
| D9 | A recording lives in the recording pod's memory until the call ends: a pod death loses the buffered audio even though the *session* is adopted elsewhere, and no upload is resumed. Also caps a recording at `MAX_RECORDING` (2 h) | `recorder.rs` | medium once a tenant records for real |
| D11 | `StopRecording`/`Detach` **blocks until the upload finishes** (bounded 60 s/90 s), because `observe` needs a live session and a backgrounded upload would lose `UploadCompleted` on every hangup. A pilot may find the latency unacceptable; the fix is a session-independent event path | `tap_plane.rs`, `recorder.rs` | medium — watch it in the pilot |
| D10 | Pause is honoured by the recorder only. A paused `WS_TWILIO`/`GRPC_STREAM` attachment keeps receiving media (registry state only) — now logged explicitly instead of being invisible, but `StreamPause` still does not stop feeding an ASR | `tap_plane.rs` | medium for cost, low for correctness |
| ~~D5~~ | ~~Event delivery is **at-most-once**; a broker outage drops events~~ — **fixed 2026-08-22 (item 13)**: bounded retry backlog, order preserved, drop-oldest counted. Now **at-least-once**, so the translator must dedupe by `(external_id, seq)`; a backlog past its 8192 cap or a pod death still loses events | `event_pump.rs` | closed |
| D6 | `play media` `from-tag` semantics are **unmeasured** — architecture §6's claim was retracted after the instrument turned out to be broken (see lab.md correction) | docs + lab | low, but §6 must not be trusted until re-probed |
| ~~D7~~ | ~~Jitter buffer: fixed target depth, no adaptive sizing, no timestamp-aware gap handling, silence instead of real PLC~~ — **fixed 2026-08-22 (item 17)**: adaptive depth from the RFC 3550 estimate, timestamp-aware silence gaps, comfort noise accounted, G.711 Appendix I-shaped PLC, restart on SSRC change. Replay-verified across the impairment matrix, and since item 19 also **on a real link** in the soak — 1%/5% injected loss reported as 1.06%/5.01% with `frames_concealed` equal to `jitter_lost` to the packet, reorder and ±35 ms jitter costing zero loss and zero late drops. Still open: never impaired with `tc netem` on the **tap link** (this box's kernel has none), so burst loss, reorder-beyond-depth and dedupe stay replay-only, and the concealment has never been judged perceptually | `jitter.rs`, `pipeline.rs`, `plc.rs` | closed |
| D8 | `owner_pod` is a config string; real placement and load-aware scheduling do not exist | `main.rs` | low until multi-pod |
| ~~D12~~ | ~~**NG cookies repeat across sessions on one pod**: `CookieSequence` restarted its serial at 0 and `TapPlane` binds a new `NgTransport` per session, so every session's first command was `<prefix>-0`. Two sessions inside rtpengine's duplicate-cookie reply-cache window get the *same cached subscribe answer*, and the second tap receives **no media at all** while looking healthy~~ — **fixed 2026-08-22 (item 10)**: the serial is process-wide, unit-pinned and lab-proved before/after | `ng_transport.rs` | closed — was **high**, it silently broke every second tap within a minute |
| D15 | An adopted attachment loses its **negotiated format**: `PersistedAttachment` has no format field and `rebuild` passes `format: None`, so a consumer that attached as L16/16k comes back at the session default (g711 at the tap rate). Found by reading the adoption path during item 11, **not** observed — that drill's consumer was WS/PCMU, where the default is the only legal answer. A gRPC consumer would notice | `session_store.rs`, `registry_keeper.rs` | low today, medium once ASR consumers ask for L16/16k |
| D14 | **A dead pod's rtpengine subscription is never torn down.** The adopter re-subscribes but nothing cancels the old tap: `PersistedSession` does not carry the subscription's `to-tag`, and only the pod that created it holds one. Measured in the item-11 drill from rtpengine's teardown block: **14,743 packets / 2.5 MB copied to a pod that had been dead for 110 s**, four ports held for the rest of the call — i.e. a pod death permanently doubles that call's cost on the rtpengine host, which is exactly the capacity number still open with the platform team. Fix shape: persist the to-tag, have the adopter `unsubscribe` it before subscribing, and decide what an adopter should do when the previous owner is partitioned rather than dead (`mss_registry_lost_total` is the signal) | `session_store.rs`, `registry_keeper.rs`, `tap_plane.rs` | medium — every pod restart during a call leaks one tap |
| D13 | `StreamStart` (and the Twilio `start` frame's `tracks`) advertises `["customer","agent"]` for `TrackSelector::All`, but a silent `mixed` track is delivered too, at the full frame rate: a consumer must tolerate an unannounced track and pays 50% extra bandwidth for silence. Fixing it means either naming `mixed` in the start frame or not carrying it under `All` — the latter touches the frozen Twilio surface | `stream.rs`, `tap_plane.rs`, `hub.rs` | low for correctness, medium for cost |

## Waiting on other people (M2 close-out)

These are not code and have blocked since Phase 0:

1. **Production rtpengine version check** — lab is 14.1.1.8 with `subscribe`
   working; the deployed version is unverified. If it lacks `subscribe`, the
   whole ingest model needs an upgrade path first.
2. **rtpengine-side per-tap cost** — MSS-side cost is measured; the userspace
   copy cost on the rtpengine host at 100/500/1000 taps is not, and it sets
   the rtpengine capacity plan.
3. **OpenSIPS → Redis call→node discovery** — **no longer a blocker
   (2026-08-17).** MSS now resolves a call's participants itself: the legacy controller passes
   the SIP call-id and the caller's from-tag (both already on the channel as
   `Variable_sip_call_id` and `Variable_sip_full_from`) and `TapPlane` asks
   rtpengine's `query` for the rest. The map remains the better long-term
   answer — it avoids a `query` per tap and works when MSS never sees the
   channel — but it is now an optimisation, not a prerequisite for a pilot.

## Later phases

**Phase 2 — Recording** is **code complete (item 15, 2026-08-22)**: the
stereo segmenter, the `${accountID}/${recordingID}.${format}` identity, the
`recordStart/recordPause/recordStop/uploadCompleted` callbacks with pause =
segment + defer + accumulate, and direct upload to S3/MinIO all landed and are
verified against real object storage from a synthetic hub. What the phase
still owes: a live tapped call recorded end to end, the FS byte-parity
comparison (harness in `lab/recording_parity.py`), and the tenant decision to
turn `record_session` off. D1 (item 14) is fixed for a mid-call SSRC change on
the same from-tag; a transfer that replaces a tag still lands on elimination,
so a recording of one is only as right as that.

**Phase 3 — Interactive media** needs the inline RTP leg (`SessionKind::INLINE`
is already accepted by the API), streaming TTS playback, and barge-in
cut-through in MSS.

**Phase 4 — Full media plane** is the N-way mixer, monitor/whisper as
attachments and playbacks rather than conference tricks. Do not start before
Phases 1–3 are boringly stable (Constitution, Article VIII).
