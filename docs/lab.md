# The local rtpengine lab

A Docker Compose lab that runs a **real rtpengine** against `mediaserverd`, so
the tap path can be exercised without the production stack — and, on a Mac,
without native rtpengine at all (there is none: its kernel fast path is a
Linux netfilter module and no Homebrew formula exists).

This documents what exists today. [testing.md](testing.md) is the plan for
what testing must become — the Linux functional box, the impairment matrix,
and the separate rig the M2 capacity number requires.

Nothing here needs OpenSIPS or FreeSWITCH. rtpengine accepts `offer`/`answer`
over NG directly, so `lab/call_driver.py` plays both endpoints and the
signalling proxy between them, then pumps G.711 for both legs.

## Running it

```sh
cd lab
mkdir -p out && chmod 777 out
docker compose up --build --abort-on-container-exit
python3 - <<'PY'
import wave; w = wave.open("out/tap.wav"); print(w.getnchannels(), w.getframerate(), w.getnframes())
PY
```

Addresses are static (`172.31.99.0/24`) because the shipped image is
distroless and has no shell to discover its own IP: rtpengine `.10`, the call
driver `.20`, `mediaserverd` `.30`. `MSS_TAP_LOCAL_IP` must therefore be a
literal, and all RTP stays inside the Linux VM — Docker Desktop on macOS has
no real `--network host`, so crossing the host boundary is the fragile path.

Startup order is enforced by the driver's healthcheck: it writes a marker file
once the call is live and pumping, and `mediaserverd` has
`depends_on: condition: service_healthy`, so the tap is never requested before
the call exists.

`lab/ng_probe.py` is the smaller tool: it creates a call, asks for a tap, and
prints rtpengine's raw replies. Use it first when something is wrong, since it
removes our Rust from the picture entirely.

Note `tmpfs: [/rec]` on the rtpengine service. The image's bundled config sets
`recording-method=pcap` with `recording-dir=/rec`, and rtpengine **exits** if
that directory is not writable.

## What this lab proves, and what it cannot

Confirmed against **rtpengine 14.1.1.8** (2026-08-14):

- `ping`, `offer`, `answer`, `subscribe request`, `unsubscribe` and `delete`
  all return `result=ok` for the exact datagram shapes `rtpengine-ng` builds.
  A two-tag `subscribe request` yields one `m=` section per tag, each
  `a=sendonly` — the shape `sdp.rs` was designed for.
- The real offer is pinned as a parser fixture
  (`parses_a_real_rtpengine_14_subscribe_offer`), which caught something the
  hand-written fixture had backwards: rtpengine emits **no session-level `c=`
  line at all**, only a media-level `c=` after each `m=`. Media-level
  precedence is not a nicety here, it is the only source of the address.
- `a=rtcp:` attributes appear and are ignored harmlessly.

It cannot give:

- **Capacity numbers.** A Docker VM on a laptop is not production-shaped
  hardware. Per-tap CPU at 100/500/1000 taps stays a Linux-host job, and it is
  the actual M2 exit criterion.
- **Kernel-module behavior** (`--table=-1` here). Minor for taps, since
  subscription legs are userspace work anyway.
- **Production signalling behavior**: hold/unhold as FreeSWITCH emits it,
  transfer moving a leg to a different rtpengine instance, re-anchoring.
- **Your deployed version's support.** This proves 14.1.1.8 works. The
  version running in production is a separate question and still open.

## What the lab found and fixed

**rtpengine rejects a subscription answer that drops the offered
`telephone-event` payload type.** The first real run failed with
`Failed to process subscription answer`, having offered `RTP/AVP 0 101` and
received `RTP/AVP 0` back from us. `lab/ng_answer_probe.py` isolated it by
trying four answer shapes against fresh subscriptions:

| Answer | rtpengine |
| --- | --- |
| PCMU only (what `sdp.rs` emitted) | **REJECTED** |
| PCMU + telephone-event | ACCEPTED |
| media-level `c=` instead of session-level | ACCEPTED |
| echo of rtpengine's offer, ports and direction swapped | ACCEPTED |

So the payload-type list is the cause, not the connection-line placement.
`SubscriptionAnswer::to_sdp` now parses `a=rtpmap:` per stream and echoes the
offered telephone-event payload type and its clock rate alongside the audio
codec, per stream, omitting it when the offer has none.

This was worth more than an unblocked lab. Answering with PCMU alone would
have meant **no RFC 4733 packets on any tap**, so `firstDtmf` and
`dtmfResult` — frozen streamfsm contracts (Constitution VII) — would have
gone silently missing in Phase 1, with `StreamPipeline`'s detector wired to a
payload type nothing was sending. It also means the telephone-event payload
type is now taken from the offer instead of assuming 101, which is
conventional but never guaranteed.

Successful run, 2026-08-14, 15s tap of both legs: 691 datagrams per leg, 691
frames played, **zero** concealed / lost / duplicated / late / reset /
unparsable / recv-error, both legs sample-aligned at 119,840 samples, 749
releases, 0 reanchors, and a 2-channel 8 kHz WAV of 14.98s carrying distinct
audio per channel. The 58 underruns per leg are the driver pumping slightly
under 50 packets/s against the pacer, not a pipeline fault.

## DTMF and leg identity, both now proven

`call_driver.py` sends a distinct digit per leg on a repeating interval —
caller `1`, callee `2` — and `TapLeg` records the digits each leg reports.
One run settles two questions at once:

| Leg | digits seen | telephone-event packets | digits reported |
| --- | --- | --- | --- |
| Customer (stream 0) | `111` | 21 | 3 |
| Agent (stream 1) | `222` | 21 | 3 |

- **RFC 4733 survives a real rtpengine subscription** now that the answer
  negotiates telephone-event, and 21 packets produce exactly 3 reported
  digits — the once-per-press dedupe the frozen `firstDtmf`/`dtmfResult`
  contract requires.
- **Leg-to-track mapping follows `from-tags` order.** The caller's digit
  arrived on stream 0 and the callee's on stream 1, so `track_for_stream`
  assigning stream 0 to Customer is correct rather than merely assumed. This
  is a channel-independent proof: it does not depend on interpreting audio.
- One timing lesson: digits must repeat. A single press three seconds in was
  missed entirely, because the healthcheck plus process start means the
  subscription does not exist yet — `telephone_event_packets` was 0 and it
  looked like rtpengine was stripping DTMF. `lab/ng_dtmf_probe.py` was what
  disproved that: with `codec accept PCMU` it showed pt101 arriving on both
  tap streams, which pointed at our timing rather than rtpengine.

## Audio can go back into a tapped call — but only as an utterance

A subscription is one-way, so the question of how bot speech reaches the
caller decides whether interactive voice-AI needs a Phase-3 inline leg.
`lab/ng_inject_probe.py` answers it by driving both legs itself: each leg
transmits mu-law silence, so any non-silent payload a leg receives can only
have come from the injection.

| Mechanism | rtpengine 14.1.1.8 |
| --- | --- |
| `play media`, `blob`, `from-tag: tagA` | **only tagA hears it** — 100 injected packets on the caller, 0 on the callee |
| `play media`, `all: "all"` | both legs hear it |
| `play media`, `blob64` | **rejected** — `No media file specified` |
| `publish` + continuous RTP | accepted, rtpengine receives our RTP, **no leg ever hears it** |

Two things follow. First, `from-tag` targeting is whisper-shaped for free:
the injected audio goes to exactly one participant, which is the primitive
Phase 4 needs for whisper/coach. Second, `publish` is **not** the
offer/answer-free injector §6 of the architecture hoped for — it is a
broadcast source for subscribers. 98 packets pushed into the port it
offered us, zero heard by either leg.

So injection into a live call is `play media` (hand rtpengine a complete
ffmpeg-decodable blob) or an inline leg (continuous stream), with nothing
in between. An AI agent can speak into a tapped call today, one utterance
at a time; streaming TTS with barge-in still needs Phase 3.

## The whole loop, closed in the lab

`lab/mock_bridge.py` stands in for stream-llm-bridge: a stdlib-only
WebSocket server that speaks the frozen Twilio dialect, logs what MSS sends
it, and every five seconds answers with an utterance (fifty `media` frames
then a `mark`). `docker compose up` now runs it alongside the call, so one
command exercises the full path.

A 15s run:

| Leg of the loop | Measured |
| --- | --- |
| tap → bridge | 1490 media frames (746 per leg), 8 dtmf events, **0 dropped** |
| bridge → MSS | 150 inbound frames, 0 undecodable, assembled into 3 utterances |
| MSS → call | 3 `play media` calls accepted, targeted at `tagA` |
| caller's ear | three 660 Hz bursts, 5s apart |
| callee's ear | nothing |

So an AI agent can hear a tapped call and speak back into it today, with
no inline leg and no FreeSWITCH conference.

**Human-confirmed 2026-08-16**: a MicroSIP caller held a live multi-turn
conversation with the echo bot — "What is your name?", "I guess I am
Ashutosh", "How are you?" — each sentence transcribed verbatim and the
reply heard on the phone. The one failed session along the way was
diagnosed from `tap-customer.dglog` alone: the raw A-law bytes were
near-zero codes, proving a muted-microphone problem at the phone before
anyone blamed the pipeline. That is the datagram log paying for itself,
and the bridge in the softphone compose now logs at debug so Deepgram's
`Speech started` events are visible during live tests.

**A tap carries what a party sends, not what it hears.** The injected tone
appears nowhere in `tap.wav`, which is not a fault: `play media` toward
`tagA` reaches the caller's *ear*, while the tap of `tagA` carries the
caller's *source*. This is why `call_driver.py` now records each endpoint's
received audio to `out/caller_ear.wav` and `out/callee_ear.wav` — injection
is only observable there. It also means **bot speech never echoes back into
the ASR feed**, which is a property worth keeping.

## Against the real stream-llm-bridge

`lab/docker-compose.yml` runs the actual service (`stream-llm-bridge:local`,
built from that repo) with `USE_LLM=false`, so ElevenLabs speaks
`WELCOME_MESSAGE` the moment `start` arrives. Keys live in `lab/.env`, which
is gitignored. The mock moved behind `--profile mock` for offline work.

A silent-endpoint run (`PUMP_SILENCE=1`, so only injected audio is audible)
ends with `caller_ear.wav` carrying **real speech** — syllable bursts with
pauses, peak 14972 — and `callee_ear.wav` peaking at **exactly 0**.
Deepgram also transcribed the tapped legs live, so both directions work
against the real service.

### Three things the real bridge corrected

`lab/bridge_probe.py` asks it which wire shape it accepts, the same way
`ng_answer_probe.py` asked rtpengine:

| Shape | Bridge |
| --- | --- |
| what `twilio.rs` emitted (`audio/x-mulaw`, numeric `timestamp`) | `invalid_json`, **connection closed** |
| what mediagateway emits (`PCMU`, string `timestamp`) | accepted |

1. **`timestamp` must be a string.** mediagateway sends
   `fmt.Sprintf("%d", ...)` (`pkg/mediaservice/client.go`) into a
   `Timestamp string` field, and the bridge unmarshals into a string too. A
   JSON number closes the connection. Our frozen test asserted the number.
2. **`mediaFormat.encoding` must be `PCMU`, not `audio/x-mulaw`.**
   mediagateway sends `codec.GetName()`. The bridge branches on
   `encoding == "PCMU" || "PCMA"` to pick its TTS output format, so
   `audio/x-mulaw` silently yields 16 kHz PCM instead of mu-law — no error,
   just wrong audio.
3. **The bridge never sends `mark`.** TTS arrives as a run of `media`
   events with no terminator, so utterance assembly cannot wait for one.
   The consumer now flushes after `UTTERANCE_IDLE` (700 ms) of inbound
   silence, keeping `mark` and `endOfInteraction` as explicit flushes.

Both wire defects predate this work and would have broken any real
consumer. The serialization tests now assert the *measured* bytes.

### The NG datagram ceiling

The first real run failed with `Message too long (os error 90)`. **The NG
protocol is UDP**, so a `play media` blob cannot exceed one datagram
(~64 KB), which is about 4 seconds of 16-bit 8 kHz WAV. An 11-second
utterance is ~175 KB and simply cannot be sent.

`inject_bridge_speech` now splits an utterance into
`BLOB_SAMPLES_PER_DATAGRAM` (3s) pieces and plays them back to back,
sleeping for each piece's duration so a piece does not cut off its
predecessor. The earlier probe missed this because its test tone was 32 KB.
The alternative — `play media {file}` on storage rtpengine can read — has
no size limit but needs a filesystem shared with the rtpengine host, which
across pods is not a given.

## A real softphone found the codec assumption

`docker-compose.microsip.yml` puts MicroSIP in front of the stack through
OpenSIPS and FreeSWITCH. The first real call failed the tap outright:

```
rtpengine offered a tap stream  payload_types: [8, 101]
tap spike failed: "Failed to process subscription answer"
```

Payload type 8 is **PCMA**. Every synthetic lab call had been PCMU, so the
hard-coded PCMU answer had never been wrong before. rtpengine rejects an
answer offering a codec it did not put in the offer, exactly as it did for
the missing telephone-event.

The subscribe request already asked for a codec, but with the wrong flag.
Measured against 14.1.1.8 on an A-law call:

| Subscription request | rtpengine offers |
| --- | --- |
| `codec: {accept: [PCMU]}` | `RTP/AVP 8 101` — PCMA only, so a PCMU answer is rejected |
| `codec: {transcode: [PCMU]}` | `RTP/AVP 8 0 101` — PCMA **and** PCMU |

`accept` means "use this if the leg already has it"; only `transcode` makes
rtpengine convert. `SubscribeRequest` now sends `transcode`, which is what
architecture.md section 4 always described ("requesting a codec on the
subscription leg so rtpengine transcodes at the tap").

That alone was not enough. With the offer widened to `8 0 101`, the answer
was still rejected, and `lab/ng_alaw_answer_probe.py` shows why:

| Subscription answer | rtpengine 14.1.1.8 |
| --- | --- |
| `RTP/AVP 0 101`, dropping the offered PCMA | **REJECTED** |
| `RTP/AVP 0 8 101`, ours first, PCMA kept | ACCEPTED |

So the rule found with telephone-event is more general than it looked:
**an answer may not drop any payload type the offer carried.** It was never
about telephone-event specifically. `SubscriptionAnswer::to_sdp` now lists
our codec first and then echoes every other offered payload type with its
rtpmap.

Preference order is honoured. Pumping PCMA into a call whose tap answered
`0 8 101` delivered 298 packets of payload type **0** to the tap: rtpengine
transcodes to the codec we listed first, so the pipeline stays PCMU whatever
the carrier negotiated.

**FreeSWITCH extension 9196 is not a test of this project.** It is FS's own
`echo()` application and answers instantly by design. If you hear yourself
there it proves SIP and RTP reachability and nothing else; 9000 answers with
`silence_stream://-1` so the only audio is what MSS injects.

## The echo loop, proven without a human

`lab/host_test_caller.py` plays the part of MicroSIP from the WSL host: SIP
to the published 127.0.0.1:5060, PCMA RTP to the published 30000-30020
range, a wav of real speech spoken five times, everything received recorded
per SSRC. It exists because "dial and tell me what you hear" was burning a
human retry on every fix. One run measures the whole loop:

- Deepgram transcribed the spoken sentence **verbatim, five out of five**
  through host → published port → rtpengine PCMA→PCMU transcode → tap →
  bridge. The caller-to-ASR path is intelligible, not merely connected.
- The TTS replies reached the caller's socket, and
  `lab/ear_intelligibility_probe.py` (which feeds the recorded ear back
  through the bridge) transcribed them **verbatim too** — the injected
  audio is objectively intelligible at the caller.

### Why a human still heard garbage: two simultaneous RTP streams

The test caller received the replies fine because it demultiplexes by SSRC.
A real softphone does not: it feeds one jitter buffer, and during injection
the caller was receiving **two concurrent streams** — FreeSWITCH's
`silence_stream` forwarded by rtpengine, and the media player, each with its
own SSRC and sequence space. Interleaving two sequence spaces through one
jitter buffer is why MicroSIP played a mangled first utterance and then
nothing.

`play media` with `flags: [block-egress]` fixes it, measured: without the
flag the peer stream keeps flowing during playback (97 packets vs 99
injected in the probe window); with it the peer stream pauses (2 vs 100)
and resumes afterwards. The caller then sees exactly one stream at a time,
and every injection arrives on one stable player SSRC — a plain source
switch, which softphones handle. Injection always sets it.

Two more findings from the same debugging arc, both invisible in the
synthetic lab:

- **A silence-suppressing caller starved the ASR.** MicroSIP stops sending
  RTP when the caller is quiet; the consumer used to forward nothing on
  jitter underrun, so Deepgram saw a 40s gap and closed with a timeout
  (`net0001`). The consumer now sends a silence frame per underrun — the
  stream a Twilio-dialect consumer receives is continuous, as mediagateway's
  was.
- **Both legs down one WebSocket is garbage to a consumer that does not
  demultiplex.** stream-llm-bridge logs `media.track` but feeds every
  packet to Deepgram, so two interleaved tracks at 100 pkt/s transcribed as
  nothing. MSS now streams only the Customer track by default;
  `MSS_CONSUMER_TRACKS=both` restores dual-track for consumers that split
  by track. mediagateway only ever sent one track, so this is also the
  wire-compatible behaviour.

## Findings resolved

- **Telephone-event packets are no longer counted as lost audio.** RFC 4733
  packets consume RTP sequence numbers while carrying no audio, so routing
  them past the jitter buffer made every DTMF press inflate `jitter_lost`
  and `frames_concealed` — an IVR-heavy tenant would have looked like a
  lossy network. The fix landed where the original finding said it
  belonged: the jitter buffer now *accounts for* a sequence number without
  carrying audio (`JitterBuffer::account`), playout emits a suppressed
  silence frame counted as `frames_suppressed`, and `jitter_lost` means
  loss again. The acceptance test from the impairment matrix — DTMF on a
  clean link reports **zero** loss, digits still deduped once per press —
  is `a_dtmf_press_on_a_clean_link_reports_zero_loss` and runs in CI.
  A clean lab run now shows `jitter_lost: 0`, `frames_concealed: 0`, and
  `frames_suppressed` equal to the telephone-event packet count.

## Findings still open

1. **The `mix` flag is untested.** `SubscribeRequest` supports it and the
   architecture proposes it for cheap supervisor listen, but no lab run has
   asked for a mixed mono feed.
2. **`stop media` cut-through latency is unmeasured.** It is the barge-in
   primitive for `play media` injection — how fast an utterance stops once
   the caller starts talking decides whether utterance-shaped bot speech
   feels interactive or not. The probe issues `stop media` but does not
   time it.
3. **Injected audio arrives alongside the peer's, not instead of it.** In
   the targeted run the caller received 245 packets, 100 of them the
   injected tone and the rest the callee's silence, so `play media` did not
   block egress. Whether an AI utterance and live caller audio should mix
   or the peer should be suppressed is a product question, and the
   `block egress` flag is the knob for it — untested.


## ng_subscribe_probe.py — what a subscription actually carries (2026-08-17)

Written because three questions could not be answered one call at a time
from the daemon: whether a subscription carries what a participant sends or
hears, whether `play media`'s `from-tag` names the speaker or the listener,
and why tap delivery was intermittent.

The method avoids inference. Each participant gets a pure tone at its own
frequency, injected with `play media` aimed at that participant, and every
subscription socket is scored with a Goertzel filter for both frequencies —
a tone is a fact, so whichever frequency turns up names the participant
without needing anyone to speak. Trials repeat so intermittency reads as a
rate. `SKIP_PLAY`, `ONLY_TAG_INDEX`, `SAME_SESSION_ID` and
`SKIP_UNSUBSCRIBE` isolate one variable at a time.

```sh
docker run --rm --network mss-microsip_lab -v "$PWD:/lab" -w /lab \
    -e CALL_ID=<from /shared/call.env> -e TRIALS=3 \
    python:3-slim python ng_subscribe_probe.py
```

Findings against rtpengine 14.1.1.8:

| Question | Answer |
| --- | --- |
| Are subscription streams labelled? | **No** — `a=label` is absent, and stream order does not follow the requested tag order |
| Two subscriptions on one call? | **Destroys the call** within seconds; every later command returns *Unknown call-ID* |
| One subscription per tag? | Sometimes survives its trial, sometimes not |
| Packet rate on a per-tag subscription | **890-2176/sec against a 50 pps call** (20-40x), carrying real audio |
| Packet rate on one multi-tag subscription | ~21-44/sec per leg, stable for minutes — the safe model |
| `play media {from-tag: X}` | The tone appears on the subscription made with the **other** participant's tag |

The last row cannot be reconciled with architecture.md §6, which says
`from-tag` selects who *hears* injected audio. One of the two readings is
wrong; neither should be trusted until re-probed.


### Correction (2026-08-17, evening)

Two findings in the table above are void, and the lesson is bigger than
either: **the stereo recorder's `RIGHT_TRACK` env was still `mixed`** from
the voice-AI demo, so every channel-based verdict mapped "right" to the
injection track, not the agent leg. Through that lens a healthy tap looked
inverted. `lab/track_dump.py` (one wav per track *name*) replaced
channel-based analysis, and with it:

- A subscription leg carries what the participant **sends**, stamped with
  the sender's SSRC — the "carries what X hears" conclusion is retracted.
- The `play media` row (tone on the "other" participant's subscription) was
  measured through the same broken lens plus a probe that answered with an
  unoffered payload type; treat it as unmeasured until re-probed.
- The call-death and 20-40x flood rows came from that same malformed-answer
  probe. The daemon's own subscriptions (correct answers) have never killed
  a call. rtpengine may still dislike concurrent subscriptions — unproven
  either way.

Leg identity is now solved in the daemon by SSRC correlation with
elimination; see implementation-notes. When a mock's env var can invert an
experiment's conclusion, the mock is part of the experiment: verify the
instrument before trusting a surprising result.

## event_outage_drill.sh — a broker outage costs no event (2026-08-22)

The drill behind item 13 / defect D5. Redpanda alone is enough, so it runs
without the SIP half of the lab:

```sh
DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml up -d redpanda
./lab/event_outage_drill.sh
```

The script builds `crates/mediaserverd/tests/kafka_outage.rs` (skipped
unless `MSS_TEST_KAFKA_BROKERS` is set), sends 60 `MediaEvent`s at 1/s
through the production `RskafkaTransport`, `docker stop`s Redpanda 15 s in
for 30 s, starts it again, and then asserts on the topic itself — every seq
present, ascending, no duplicate — rather than trusting the pump's own
counters. `QUIET_BEFORE`, `OUTAGE_SECONDS`, `COUNT` and `INTERVAL_MS`
override the schedule; the log lands in `lab/out/`.

First green run: `accepted=60 published=60 failed=6 retried=6 dropped=0
dropped_oldest=0 unsent=0`, 60 distinct seqs 0-59 on partition 0 at
contiguous offsets, cross-checked with

```sh
cargo run -q -p mediaserverd --example mss_events_tail -- 127.0.0.1:19092 mss.events.drill 8
```

Two things the drill taught, both about where the outage is absorbed:

- **rskafka hides most of a short outage.** Only 6 attempts failed across a
  30 s stop; its producer retries internally and the connection recovers
  after the broker returns without rebuilding `RskafkaTransport`. Those 6
  are precisely what the old at-most-once pump would have lost.
- **A dead broker can park a send indefinitely**, which is why each attempt
  now carries a 5 s timeout: without it the worker stops draining its
  handoff queue and the loss moves upstream, where drop-oldest cannot
  protect it.

What it does not prove: nothing here involves a real tapped call — the drill
drives the pump directly, so it exercises the pump and the transport, not
the call path. A live-call version belongs with item 10/11's lab work.

## MinIO — recording storage for phase 2 (2026-08-22)

Recording uploads (tasks item 15) need an S3 endpoint, so the compose stack
now carries one. `minio` serves the S3 API on `172.31.99.62:9000` (published
on the host as `127.0.0.1:9000`, console on `:9001`), and `minio-init` runs
`mc mb` once to create the bucket — MSS deliberately never creates buckets,
because a recorder that can create buckets can also create typos. Both use
the default `minioadmin:minioadmin` unless `MINIO_ROOT_USER` /
`MINIO_ROOT_PASSWORD` are set in `lab/.env`.

`mss-control` gets the matching env (`MSS_RECORDING_BUCKET`,
`MSS_RECORDING_S3_ENDPOINT`, region, key id, secret, and
`MSS_RECORDING_SPILL_DIR=/out/recordings` so a failed upload lands in
`lab/out/` instead of vanishing). With no bucket configured the daemon still
starts and refuses `FILE_S3` attachments by name.

Storage alone is enough for the recorder drill, so it runs without the SIP
half of the lab:

```sh
DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml up -d minio minio-init
MSS_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
  cargo test -p mediaserverd --test minio_upload -- --nocapture
```

`crates/mediaserverd/tests/minio_upload.rs` (skipped unless
`MSS_TEST_S3_ENDPOINT` is set) drives the real recorder task from a synthetic
hub — 500 ms of tone on both legs, a pause, 500 ms of tone the recorder must
drop, a resume, 500 ms more — then uploads through the production
`S3RecordingSink` and reads the object back out of the bucket to check it.

First green run against MinIO `RELEASE.2025-08-13`:

```
drill: uploading to bucket lab-recordings at http://127.0.0.1:9000
drill: s3://lab-recordings/acct-drill/rec-1787395753639.wav duration_ms=1000 bytes=32044 segments=2
drill: verified 8000 stereo frames at acct-drill/rec-1787395753639.wav in bucket lab-recordings
```

Confirmed independently with `mc`, which is inside the MinIO image:

```sh
DOCKER_API_VERSION=1.43 docker exec mss-microsip-minio-1 \
  sh -c 'mc alias set lab http://127.0.0.1:9000 minioadmin minioadmin && mc stat lab/lab-recordings/acct-drill/rec-1787395753639.wav'
```

```
Size      : 31 KiB
ETag      : efb92b24f14c20c635edd591a51dc5e6
Content-Type: audio/wav
```

Three things worth keeping from that run:

- **The paused second really is absent.** 1500 ms of audio was published,
  1000 ms is in the file, the pause marker sample appears nowhere in it, and
  `duration_ms` reported on `RecordingStopped` is 1000 — pause = segment +
  defer + accumulate, end to end.
- **The identity is the key, byte for byte.** The object is at
  `acct-drill/rec-<id>.wav`, and `UploadCompleted.uri` is
  `s3://bucket/<that key>`.
- **Identical input gives an identical ETag** across runs, which is what
  makes the parity harness below meaningful.

What this does not prove: no SIP, no rtpengine, no real speech. It exercises
the recorder, the segmenter, the WAV writer and the upload against a real
object store, not the call path. The live-call half (a real tapped call
recorded to MinIO, with the callbacks read off `mss.events`) is still owed
and belongs with item 10's lab session.

## recording_parity.py — the FS byte-comparison harness

The Phase-2 exit criterion asks for byte-comparable recordings against
FreeSWITCH `RECORD_STEREO` output. `lab/recording_parity.py` is that harness:

```sh
python3 lab/recording_parity.py --mss out/mss.wav --fs out/fs.wav
```

It compares container (channels, rate, width — a mismatch fails immediately
and stops), duration, alignment offset found by correlation, and per channel
the identical-sample ratio, mean absolute difference, worst difference and
first divergence. Defaults: 200 ms duration tolerance, mean difference ≤ 200,
identical-sample ratio only reported (`--identical-ratio 1.0` demands
bit-for-bit). Exit code is 0 only when every bar is met.

Exercised on the drill's own upload (`--mss` and `--fs` the same file):
`identical=1.0000 mean_diff=0.0` at offset 0, and on deliberately perturbed
copies, where it fails with numbers. **It has never seen a real FreeSWITCH
recording** — getting the same call recorded both ways is the human step the
exit criterion still needs, and the docstring says how to capture it.

One caveat the harness cannot see: bit-for-bit equality is not expected under
loss, because the two paths conceal differently (MSS grew G.711 Appendix I
PLC in item 17, FS does not). Compare on a clean link, or compare RMS and
mean difference rather than identity.

## grpc_stream_drill.sh — a live tapped call over the gRPC data plane (2026-08-22)

Item 10. Until this run the `MediaStream` service had only ever served the
fake plane, so nothing proved that a gRPC consumer hears a real call.
`crates/control-api/examples/mss_stream_probe.rs` is the consumer: it attaches
a `GRPC_STREAM` consumer at a format it picks (`L16/16k` by default), dials
`MediaStream::Subscribe` with a `ConsumerHello`, decodes the little-endian
frames and writes one wav per track with rms and peak per track.
`lab/grpc_stream_drill.sh` wires it to a live call with no human dialing:

```sh
DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
  up -d rtpengine opensips freeswitch call-watcher redpanda redis \
        minio minio-init llm-bridge mss-control
RECORD=1 ./lab/grpc_stream_drill.sh
```

`host_test_caller.py` plays the softphone from the WSL host, `call_watcher`
finds the call and the drill reads its call-id and tags straight out of the
watcher's `/shared/call.env`, `mss_ctl create` makes the session on the
running `mss-control` daemon, the probe attaches and subscribes, and the
metrics endpoint is scraped mid-call and again at the end. `RECORD=1` also
attaches a `FILE_S3` recording to the same session and pauses/resumes it
mid-call. Note the compose file now gives `mss-control`
`MSS_METRICS_LISTEN=0.0.0.0:9464`, published on the host, which is what makes
the scrape possible at all.

**The `mediaserverd` compose service must stay down** for this: it is the
Phase-0 spike, and it would tap the same call from its own process.

### The run (2026-08-22, rtpengine 14.1.1.8, 45 s call, 30 s probe)

L16/16k, `MSS_PROBE_TRACKS=all`, one wav per track:

| Track | frames | bytes | samples | seconds | rms | peak |
| --- | --- | --- | --- | --- | --- | --- |
| customer | 1500 | 960000 | 480000 | 30.00 | **614.5** | 6180 |
| agent | 1500 | 960000 | 480000 | 30.00 | 8.0 | 9 |
| mixed | 1500 | 960000 | 480000 | 30.00 | 0.0 | 0 |

640 bytes per 20 ms frame is 320 samples at 16 kHz, so the resampler is
doing what item 8 said it does; the agent leg is FreeSWITCH's
`silence_stream` and `mixed` is the injection track with nothing injected.
Both tap legs were clean — Customer 2154 datagrams, Agent 2200,
`jitter_lost: 0`, `frames_concealed: 0`, `frames_suppressed: 0`,
`recv_errors: 0`, `unknown_payload_type: 0`.

The metrics scrape the Done-when asks for, mid-call and after the probe:

```
mss_consumer_delivered_total 3657      (mid-call)
mss_consumer_delivered_total 11088     (after 30s of probe)
mss_consumer_dropped_oldest_total 0
mss_consumer_queue_depth_frames 0
mss_consumer_queue_depth_frames_max 0
mss_consumers_live 2
mss_legs_live 2
mss_legs_unknown_ssrc 0
mss_legs_stalled 0
```

**Intelligible, judged by the ASR rather than by ear.**
`ear_intelligibility_probe.py` (which now accepts any whole multiple of
8 kHz and averages it down, since the probe's wav is 16 kHz) replayed
`grpc-probe-*-customer.wav` into stream-llm-bridge, and Deepgram returned the
spoken sentence verbatim on **three of three complete repetitions**:

```
'Hello.'
'This is the media server speaking through your bridge.'
'If you can hear this, injection works.'
```

So the chain host softphone → published port → rtpengine PCMA→PCMU
transcode → tap → hub → `ConsumerEncoder` L16 resample → gRPC → wav is
intelligible end to end, not merely connected.

### The bug this drill found: NG cookies collided across sessions

The third back-to-back run delivered 750 frames of pure silence — rms 0.0 on
every track — while the call itself was healthy. The diagnostic order in the
playbook found it without touching code:

- `tap leg finished` said **`datagrams: 0`, `underruns: 1151`** on both legs,
  so the consumer path was fine and nothing arrived from rtpengine (the
  silence is the pipeline keeping the stream continuous on underrun, exactly
  as intended — which is also why "silence" and "no media" look alike from
  the consumer's end, and why the leg stats are the first thing to read).
- The daemon logged `rtpengine offered a tap stream source_port: 30008 …
  30016` — **byte-identical to the previous session's offer**, and its
  `unsubscribe` carried the *previous* session's to-tag.
- rtpengine's own log had **no `subscribe request` at all** for that call-id,
  and the call's teardown listed only its two call legs, no subscription
  ports.

So rtpengine never saw the request: it answered from its **duplicate-cookie
reply cache**. `CookieSequence` mixed a per-process prefix with a serial that
restarted at 0, and `TapPlane` binds a **new** `NgTransport` per session — so
every session's first command was cookie `<prefix>-0`. Two sessions inside
rtpengine's cache window (the failing pair were 53 s apart) got the same
cookie, and the second one was handed the first one's cached SDP, describing
a subscription that no longer existed. The serial is now process-wide
(`ng_transport.rs`), pinned by
`two_transports_on_one_pod_never_share_a_cookie`.

Measured before and after, same drill, two runs 25 s apart:

| | first subscription | second subscription | second run's audio |
| --- | --- | --- | --- |
| before | source ports 30008/30016 | **30008/30016 again** | datagrams 0, rms 0.0 |
| after | 30004/30010 | 30008/30016 | datagrams 1122/1146, rms 655.5 |

This is a production defect, not a lab artifact: a pod that starts two taps
within a minute is the normal case, and the second tap would have been
silent. It also explains why every earlier lab session — which tapped one
call at a time, minutes apart — never saw it.

### Recording the same call to MinIO (item 15's live-call half)

`RECORD=1` attached `acct-grpc/rec-1787397153.wav` to the same session and
paused it 8 s in for 4 s. From `mss.events` on the real broker, in order:

```
RecordingStarted  { recording_id: "rec-1787397153", path: "acct-grpc/rec-1787397153.wav" }
RecordingPaused   { paused: true,  duration_ms: 8140 }
RecordingPaused   { paused: false, duration_ms: 8140 }
RecordingStopped  { duration_ms: 39880 }
UploadCompleted   { uri: "s3://lab-recordings/acct-grpc/rec-1787397153.wav" }
```

The two `RecordingPaused` edges reporting the **same** `duration_ms` is the
pause semantics proved on a live call: no audio accumulated while paused.
The object read back out of the bucket is 2 channels at 8 kHz, 319,040
frames = **39.88 s, exactly the reported `duration_ms`**, customer left
(rms 610), agent right (rms 7), `Content-Type: audio/wav`, 1,276,204 bytes —
the same number `mss_recording_bytes_uploaded_total` reports. Ten events
were accepted and ten published with zero failures.

### Two things left open by this run

- **`StreamStart` under-advertises its tracks.** With `TrackSelector::All`
  the start frame lists `["customer","agent"]`, but a third `mixed` track
  arrives too, at the full frame rate, silent when nothing is injected
  (`hub.rs` publishes an injection-track frame every tick so the stream
  stays gap-free). A consumer must therefore tolerate a track it was never
  told about, and pays 50% extra bandwidth for silence. Either the start
  frame should name `mixed` or an `All` selection should not carry it —
  recorded as D13, not fixed here, because the same `tracks_of` shape feeds
  the frozen Twilio `start` frame.
- **`unsubscribe` returns `Unknown call-ID` at the end of every drill.** The
  synthetic caller hangs up before the drill destroys the session, so
  rtpengine has already deleted the call. Benign here, and the warning says
  the right thing, but a production hangup takes the same path.

## pod_kill_drill.sh — a pod dies mid-call and another pod re-subscribes (2026-08-22)

Item 11, the Phase-1 exit criterion "re-subscribe recovery observed
working". The Redis registry (item 3) had been proven at store level — six
pods racing for one orphan produce exactly one owner — but nothing had ever
killed a pod that was actually carrying audio.

Three `mediaserverd` pods now share the lab's Redis. They differ in exactly
the three things that must differ:

| | pod name | control | metrics | tap address |
| --- | --- | --- | --- | --- |
| `mss-control` (A, the victim) | `lab-control` | 50551 | 9464 | 172.31.99.31 |
| `mss-control-b` (B) | `lab-control-b` | 50552 | 9465 | 172.31.99.32 |
| `mss-control-c` (C) | `lab-control-c` | 50553 | 9466 | 172.31.99.33 |

Two survivors, not one, is deliberate: with a single candidate "exactly one
adopter" is a tautology. They share the compose build-cache volume, so B and
C start in seconds without recompiling.

```sh
DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
  up -d rtpengine opensips freeswitch call-watcher redpanda redis \
        minio minio-init llm-bridge mss-control mss-control-b mss-control-c
./lab/pod_kill_drill.sh
# pod A is dead afterwards, on purpose; before the next run:
DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml up -d mss-control
```

The drill dials with `host_test_caller.py`, reads the call-id and tags from
`call_watcher`, creates the session on **pod A** with a WS consumer attached,
lets it run 20 s, then `docker kill --signal=KILL`s pod A — the container's
PID 1 dies, so the whole namespace goes with it: no shutdown path, no lease
release, which is what a node loss looks like. Then it polls both survivors'
`mss_registry_adopted_total` until one moves.

**The consumer is a WS consumer, not `mss_stream_probe`.** With `WS_TWILIO`
the pod dials the consumer, so the adopting pod re-dials the *same* endpoint
and one artifact spans the outage; a gRPC consumer dials the pod, and would
have to discover the new pod and attachment id itself. `lab/gap_consumer.py`
is that consumer, built for measuring rather than listening: it stamps every
media frame's arrival, logs each connection open/close, and writes one wav
per track on an **arrival** timeline (each frame placed at
`round((arrival - t0) * 8000)`, everything no frame covered left as digital
silence). The outage is therefore a run of samples no frame ever covered,
and the drill reports it two ways — longest gap between consecutive
arrivals, and longest uncovered run.

It runs on the WSL host so it outlives pod A, which forced one lab discovery:
under Docker Desktop on WSL2 the pods reach the host at
**`host.docker.internal`** (192.168.65.254, in every container's
`/etc/hosts`). The lab bridge gateway 172.31.99.1 belongs to the Docker VM,
not to the WSL distro the script runs in — a connect there is *refused* — and
the WSL eth0 address (`ADVERTISED_IP`, which the SIP half uses) times out
from inside the lab network.

### The run (2026-08-22, rtpengine 14.1.1.8, 150 s call, kill 20 s in)

Recorded run: `killdrill-1787399380`, call `host-test-1787399382`.

| | value |
| --- | --- |
| **audio gap at the consumer** | **14.41 s** (longest arrival gap 14407 ms; longest uncovered run 14380 ms, identical on all three tracks) |
| WS connection dead time | closed at 21.13 s, re-dialed at 35.48 s = 14.35 s |
| adoption latency | 14.6 s after the kill (`subscribe request` on the wire 14.4 s after it) |
| adopter | pod C — `mss_registry_adopted_total` 1 → 2, pod B unchanged at 1 |
| lease | `mss:lease:killdrill-…` = `lab-control` with ttl 12 at the kill, `lab-control-c` after |
| `mss_registry_lost_total` | 0 on both survivors |
| the rebuilt tap | Customer 5551 datagrams, Agent 5640, `jitter_lost: 0`, `frames_concealed: 0`, `recv_errors: 0`, `unknown_payload_type: 0`, both legs named (not resolved by elimination alone) |
| the rebuilt consumer | `mss_consumer_dropped_oldest_total 0`, `queue_depth_frames_max 0`, `mss_legs_stalled 0` |

Two earlier runs of the same drill, for the shape of the distribution:
19.55 s gap / 19.7 s adoption (pod B adopting, before pod C existed) and
17.97 s gap / 18.6 s adoption (pod C). All three sit inside the arithmetic
the design implies: the lease is 15 s, renewed every 5 s, and the adopt
sweep runs every 10 s, so the worst case is **25 s** and the best is the
sweep landing just after expiry.

So the exit criterion holds: **a pod-kill mid-call costs the consumer one
bounded gap of ~15-20 s and then the audio comes back, with the tap
re-established, the leg names re-resolved and the consumer re-dialed, by a
pod that never saw the original request.**

### What it found: the dead pod's subscription is never torn down (D14)

The adopter creates a *new* subscription; nothing removes the dead pod's.
rtpengine's own teardown block is the proof, and it prices the leak:

```
--- Tag '7fc6e5bd…' (label 'mss-tap'), created 2:28 ago
------ Port 172.31.99.10:30004 <> 172.31.99.31:35533 … out 7317 p, 1258524 b
------ Port 172.31.99.10:30010 <> 172.31.99.31:38680 … out 7426 p, 1277272 b
--- Tag '56f4dd52…' (label 'mss-tap'), created 1:52 ago
------ Port 172.31.99.10:30006 <> 172.31.99.33:48248 … out 5551 p,  954772 b
------ Port 172.31.99.10:30002 <> 172.31.99.33:48575 … out 5640 p,  970080 b
```

172.31.99.31 is pod A, which had been dead for 110 s. rtpengine sent it
**14,743 packets / 2.5 MB** anyway — essentially every packet of the call,
since the leg totals were 7424 and 7533 — and held four ports for it until
the call ended. Nothing in rtpengine's log for this call is an `unsubscribe`
except pod C's own at teardown. Recorded as **D14**: the tap's `to-tag` is
not persisted, so no survivor can cancel it. Not fixed here — the fix needs
a new seam (persist the to-tag, and have the adopter `unsubscribe` it before
re-subscribing) plus a decision about a partitioned-but-alive owner, which is
more than a lab session should land.

### Instrument notes, all measured rather than assumed

`lab/ng_call_tags.py` asks rtpengine `query` who is attached to a call and
counts taps (a tap is an entry in a leg's `subscribers` list with
`"type": "pub/sub"`). Three things about that instrument, learned the hard
way in this drill:

- **A lone subscription is invisible.** With exactly one tap on the call and
  20 s of media delivered, `query` reported two tags and *no* pub/sub
  subscriber. Both taps appeared the moment a second `subscribe` touched the
  call. So the drill's `tapped-by-A=0` is not a bug in the script, and a 0
  from it means "0 or 1"; only counts above 1 are trustworthy.
- **The per-stream numbers in a query reply are stale.** On a call carrying
  50 packets/s, both `stats_out` and `last packet` came back identical from
  queries 15-20 s apart. Do not argue from them that media is or is not
  flowing; the teardown "Final packet stats" block has the real totals.
- **`created` is the field that separates an orphan from its replacement**
  (1787399384 for pod A's tap, 1787399420 for pod C's), and our
  subscriptions carry the label `mss-tap`, which makes them greppable in
  rtpengine's log.

The D12 trap from item 10 did not fire: the two pods' subscribes are 36 s
apart in rtpengine's log from two different source addresses, each answered
individually, so no cached duplicate-cookie reply was served to the
re-subscribe even though it reuses the same call-id. Cookie prefixes are
per-process wall-clock nanos, and the serial is process-wide since D12.

Also worth knowing for item 19's soak: D13 reproduces here on every run —
the start frame advertises `["inbound","outbound"]` and a silent `mixed`
track arrives anyway, which is why the gap consumer reports three tracks.
