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
| **M4 — control plane** | `MediaControl` gRPC, session state machine, Kafka events, Redis registry, tenant-flag pilot | 🔶 **code complete** — pilot gates: translator merge (external), barge-in number (item 5), live pod-kill drill (item 11), gRPC lab proof (item 10) |
| **M5 — recording (Phase 2)** | per-leg taps → stereo segmenter → S3, identity + callback contract, dual-recording | ⬜ next — item 15 below is the plan |
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
  consecutive live calls, independent of stream order.
- **Lab** — OpenSIPS/FreeSWITCH/rtpengine/Redpanda compose stack, synthetic
  caller (`host_test_caller.py`), per-track analyser (`track_dump.py`),
  RTT and recorder mock consumers, NG probes, `mss_ctl` control-plane CLI,
  `mss_events_tail` bus consumer.

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

### 10. Lab proof of the gRPC data plane
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

### 11. Pod-kill re-subscribe drill (Phase-1 exit criterion)
**Where:** `lab/`, no product code expected.
**What:** the Redis registry (item 3) was proven at store level; the exit
criterion wants a real kill observed end to end.
**Direction:** two `mediaserverd` processes against one Redis (distinct
`MSS_POD_NAME`, distinct control ports, same `MSS_REDIS_URL`); live call
tapped by pod A with a WS consumer attached; `kill -9` pod A mid-call;
pod B's keeper adopts within the lease TTL (15 s) and re-subscribes.
Measure the audio gap in the consumer's artifact (silence run length),
assert exactly one adopter (`mss_registry_adopted_total`), zero
`mss_registry_lost_total`, and no orphan subscription left in rtpengine
(NG `query` before/after).
**Done when:** the drill script lives in `lab/`, the measured gap is
recorded here, and [lab.md](lab.md) documents the procedure.

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

### 14. Mid-call SSRC re-resolution (defect D1)
**Where:** `crates/mediaserverd/src/tap_spike.rs` + `tap_plane.rs`.
**What:** a re-INVITE/transfer changes a leg's SSRC; today `ssrcs_seen`
records it and nothing acts, so speaker labels can go stale. Blocks
correct Phase-2 recording of transferred calls.
**Direction:** the media thread must not do NG round trips. Reuse the two
seams that exist: the capture thread already publishes `ssrcs_seen` into
`SharedLegStats`-adjacent state, and the hub already shows how to hand
commands into the capture loop (bounded `ArrayQueue`, polled per tick).
A control-world task notices an unknown SSRC, re-runs `query` +
`speaker_ssrcs` (both exist in `tap_plane.rs`), and pushes an updated
ssrc→track map into the legs through a new command queue.
**Done when:** a replay/lab test that switches the sender's SSRC mid-call
(the `G711StreamGenerator` takes an SSRC — send with a second generator)
keeps customer/agent naming correct, and D1 is struck from the table.

### 15. Phase 2 — recording to S3 (milestone M5)
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
even if FS parity sign-off is a later human step. Requires item 14 for
transferred calls; note it if shipped without.

### 16. Opus output
Item 9 above, unchanged: decide the `audiopus_sys` cmake trade first.

### 17. Jitter hardening (defect D7)
**Where:** `crates/media-core` (`jitter.rs`, `pipeline.rs`).
**What:** the M2/M3 hardening list in
[implementation-notes.md](implementation-notes.md): adaptive target depth
from observed inter-arrival jitter, timestamp-aware gap handling so
silence-suppression gaps stop counting as loss, PLC on `PopOutcome::Lost`
(G.711 Appendix I repeat/attenuate — adopt, don't invent).
**Direction:** everything here is sans-IO — build it with `replay.rs`
(`disturb` scripts model loss/reorder/dup) and never against the lab
first. Re-run `cargo bench -p media-core` after; the pipeline budget is
the Article-VIII regression bar (~165 ns/packet on the recorded M2 run).
**Done when:** the impairment matrix in [testing.md](testing.md) passes at
the profiles it names, and the benchmark delta is recorded.

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

**Decision gate, in order — do not write eBPF before all three:**
  1. Get the rtpengine-side per-tap cost measured (the open org item).
     If `subscribe` at the target tap count costs little, stop here;
     eBPF is unjustified complexity.
  2. Probe whether `subscribe` actually kicks legs off kernel forwarding:
     `cat /proc/rtpengine/*/list` on the lab host before/after a
     subscribe (`lab/ng_*_probe.py` pattern). rtpengine's kernel module
     also has a packet-mirroring path used by `rtpengine-recording` —
     probe whether that reaches an arbitrary UDP destination; if yes,
     that is the same win with vendor support and no eBPF.
  3. Only then: a one-day TC `bpf_clone_redirect` PoC against the lab
     rtpengine container with a hardcoded flow map, measuring per-packet
     overhead and packet integrity at the MSS socket.
**Done when:** a decision record lands in
[architecture.md](architecture.md) (build / vendor-mirror / stay-on-NG),
with the three probes' numbers.

### 19. Soak + impairment suite — the "full testing" bar
**Where:** `lab/` + [testing.md](testing.md).
**What:** the three test altitudes exist (replay, lab, benchmark); what is
missing is the standing proof that hours-long operation is boring.
**Direction:** a `lab/soak.py` that runs N concurrent synthetic calls
(`host_test_caller.py` is the building block) for ≥1 hour under `tc netem`
impairment profiles from testing.md (loss 1%/5%, reorder, jitter), scraping
`/metrics` each minute and asserting: zero `mss_legs_stalled` at steady
state, `dropped_oldest` bounded, `mss_events_failed_total` zero, process
RSS flat (no leak). Add the M2 benchmark re-run as the closing step —
Article VIII requires it after any pipeline change (items 14/17 are that).
**Done when:** one green soak run is recorded in testing.md with its
numbers, and the script fails loudly on any assertion so CI or a cron can
own it later.

## Open defects and soft spots

| # | Item | Where | Severity |
| --- | --- | --- | --- |
| D1 | A **mid-call SSRC change** (re-INVITE, transfer, codec renegotiation) does not re-resolve leg identity; `ssrcs_seen` makes it visible but nothing acts on it | `tap_spike.rs` | medium — affects transferred calls; **item 14** |
| D2 | `stop_playback` stops **all** playback on the call: rtpengine's `stop media` targets a participant, not a playback id | `tap_plane.rs` | low until multiple concurrent playbacks exist |
| D3 | `close_attachment` **aborts** the consumer task instead of closing the websocket politely (no `stop` frame) | `tap_plane.rs` | low, but consumers see a truncated stream |
| D4 | `WS_TWILIO` and `GRPC_STREAM` attachments are served; `FILE_S3` (phase 2) and `RTP_INLINE` (phase 3) are refused by name | `tap_plane.rs` | expected — phase work |
| ~~D5~~ | ~~Event delivery is **at-most-once**; a broker outage drops events~~ — **fixed 2026-08-22 (item 13)**: bounded retry backlog, order preserved, drop-oldest counted. Now **at-least-once**, so the translator must dedupe by `(external_id, seq)`; a backlog past its 8192 cap or a pod death still loses events | `event_pump.rs` | closed |
| D6 | `play media` `from-tag` semantics are **unmeasured** — architecture §6's claim was retracted after the instrument turned out to be broken (see lab.md correction) | docs + lab | low, but §6 must not be trusted until re-probed |
| D7 | Jitter buffer: fixed target depth, no adaptive sizing, no timestamp-aware gap handling, silence instead of real PLC | `jitter.rs`, `pipeline.rs` | medium for quality under real impairment; **item 17** |
| D8 | `owner_pod` is a config string; real placement and load-aware scheduling do not exist | `main.rs` | low until multi-pod |

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

**Phase 2 — Recording** is the closest and mostly assembled already —
**item 15 above is its executable plan.** Per-leg taps with correct speaker
attribution are done; what remains is the stereo segmenter → direct S3, the
`${accountID}/${recordingID}.${format}` identity contract, and the
`recordStart/recordStop/recordPause/uploadCompleted` callback semantics
including pause = segment + defer + accumulate. Needs D1 (item 14) fixed if
transferred calls must record correctly.

**Phase 3 — Interactive media** needs the inline RTP leg (`SessionKind::INLINE`
is already accepted by the API), streaming TTS playback, and barge-in
cut-through in MSS.

**Phase 4 — Full media plane** is the N-way mixer, monitor/whisper as
attachments and playbacks rather than conference tricks. Do not start before
Phases 1–3 are boringly stable (Constitution, Article VIII).
