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

## How precisely can a playback be stopped? (2026-08-23)

`lab/ng_stop_media_probe.py` answers the question defect D2 raised: MSS used to
stop *every* playback on a call, because it sent `stop media` with `all: all`.
The probe builds its own two-leg call over NG, both legs transmit mu-law
silence and count non-silent payloads, and it plays a 2 s 440 Hz blob with
`repeat-times: 30` so a player is still running when the stop arrives.

```sh
docker run --rm --network mss-microsip_lab --ip 172.31.99.20 \
    -v "$PWD/lab:/lab" -w /lab -e SELF_IP=172.31.99.20 \
    -e CALL_ID=stop-media-probe-5 -e CALLER_RTP_PORT=40050 \
    -e CALLEE_RTP_PORT=40052 python:3-slim python ng_stop_media_probe.py
```

Findings against rtpengine 14.1.1.8, reproduced in three runs (packet counts
are non-silent payloads received in a 1.5 s window, one packet being the tail
already in flight when the stop landed):

| Experiment | Result |
| --- | --- |
| a player on each participant, then `stop media {from-tag: tagA}` | **only tagA stops** — tagA 75 → 1, tagB 75 → 75 |
| two `play media` at the **same** from-tag | both accepted, but one `stop media {from-tag}` clears the participant (1 packet in a 3 s window) |
| a player started `all: all`, stopped with one from-tag | that participant stops, the **other keeps hearing it** — 1 vs 75 |
| `play media {from-tag: tagA}` | corroborates the injection probe: only tagA hears it (76 vs 0) |

So a targeted stop is exactly as precise as rtpengine gets: per participant.
MSS therefore aims `stop media` at the from-tag the playback was started with
and keeps `all: all` only for playbacks that were for everyone. What no NG
command can express is *which* playback to stop: two playbacks aimed at one
participant are one player as far as rtpengine is concerned.

**A trap this probe fell into first, worth remembering for any NG script:**
cookies must be unique per *run*, not just per command. The first version
restarted its serial at 1, so the second run's `offer`/`answer`/`play media`
were answered from rtpengine's duplicate-cookie reply cache — every command
came back "accepted" against a call that did not exist, and the run measured
pure silence while looking healthy. That is defect D12 in a script instead of
in mediaserverd: the cookie prefix now carries the pid and a timestamp. Any
lab run whose "while playing" control reads zero should be treated as invalid
rather than as a finding.

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
copies, where it fails with numbers. Since 2026-08-23 it has also seen a **real
FreeSWITCH recording** — `fs_parity_drill.sh` below captures one.

`--drift-window SECONDS` (with `--drift-stride N` to trade accuracy for time)
re-aligns **every window** instead of once for the whole file, and prints the
offset and agreement per window plus the best window and how far the offset
wandered. This is the mode that matters, for the reason the next section
measures: a single global offset assumes the two recorders hold one sample grid
for the whole call, and they do not.

One caveat the harness cannot see: bit-for-bit equality is not expected under
loss, because the two paths conceal differently (MSS grew G.711 Appendix I
PLC in item 17, FS does not). Compare on a clean link, or compare RMS and
mean difference rather than identity.

## fs_parity_drill.sh — the same live call recorded by FS and by MSS (2026-08-23)

Item 31. `lab/fs_parity_drill.sh` is the drill the parity harness was waiting
for: it dials a call with `host_test_caller.py`, reads the call-id and tags out
of `call_watcher`'s `/shared/call.env`, finds the **FreeSWITCH channel** by
matching `uuid_getvar <uuid> sip_call_id` against that call-id, then records the
one call twice — `uuid_record <uuid> start` with `RECORD_STEREO=true` on the FS
side, an MSS `FILE_S3` attachment on the other — pulls both wavs (`docker cp`
from FS, `mc cp` + `docker cp` from MinIO) and runs `recording_parity.py`.

```sh
DOCKER_API_VERSION=1.43 ./lab/fs_parity_drill.sh
```

**The lab FS image can record**, which had been an open question. Probed with
`show application` / `show api`: `mod_dptools` supplies `record`,
`record_session`, `record_session_pause`, `record_session_resume`,
`record_session_mask`, `record_session_unmask`, `stop_record_session`;
`mod_commands` supplies `uuid_record`; `mod_sndfile` supplies the `wav` format
(and `mod_native_file` PCMA/PCMU/L16); `/var/lib/freeswitch/recordings` exists
and is writable — the container runs as root. The drill prints this probe at the
top of every run.

### The run (25 s of a live PCMA call, MicroSIP → OpenSIPS → rtpengine → FS 9000)

```
mss:  2ch 8000Hz 16bit 204000 frames (25500 ms)
fs:   2ch 8000Hz 16bit 200960 frames (25120 ms)
duration difference: 380 ms (tolerance 200)
alignment: fs is offset by -1578 frames (-198 ms)
customer-left: identical=0.3746 mean_diff=485.6 rms mss=624 fs=623
agent-right:   identical=0.0000 mean_diff=8.0   rms mss=8   fs=0
windowed re-alignment (2s windows, stride 4):
  t=   6.0s offset= -1600 agreeing=0.9762 mean_diff=    9.2
  t=  16.0s offset=  +480 agreeing=1.0000 mean_diff=    0.6
  best window t=16.0s agreeing=1.0000 mean_diff=0.6
```

**Container, layout and amplitude agree exactly**: 2 channels at 8 kHz 16-bit
both, customer left / agent right matching FS's read-left write-right, and the
customer channel's rms is 624 against 623. An MSS recording is drop-in for an FS
`RECORD_STEREO` one as far as any downstream consumer can tell.

**There is no transform difference.** Re-aligned per window, one 2 s window
agrees on **1.0000** of its samples at a mean absolute difference of **0.6 out
of 32768** — MSS's PCMA→L16 decode and FS's produce the same samples.

**Why the global-offset comparison fails anyway.** MSS and FS have independent
jitter buffers and conceal loss independently, so the offset between the two
files wanders across the call and per-sample identity collapses wherever the
grid slips. That is why the windowed mode exists. The lesson for the Phase-2
exit criterion: **byte-for-byte parity at a fixed offset is not an achievable
bar across two independent jitter buffers**; the bar that means what the
criterion intended is identical container and layout, duration within
tolerance, matching per-channel rms, and near-perfect agreement in a re-aligned
window.

The 380 ms duration gap is **drill skew**, not drift: the MSS attach precedes
the `fs_cli uuid_record` by three `docker exec` round trips.

### What this lab cannot show, and a production FS still owes

- **A two-party call.** Ext 9000 answers and plays `silence_stream://-1`, so
  FS's write side is silence: the agent/right channel compares silence against
  silence (rms 8 vs 0) and **only the customer channel is a real comparison**.
  Set `DIAL` to a bridging extension on a rig that has two live legs.
- The **pause contract** compared (`record_session_pause` vs MSS `Pause`) — this
  drill does not pause.
- The tenant's **own codec and rate** (this was PCMA/8000) and any recording
  post-processing on their side.
- **A human listening to both files.**
- The per-window offsets **clip at `--align-search`** (1600 frames), so the
  reported 260 ms wander is a floor, not a measurement; a wider search costs
  O(search x window) per window.

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

## drain_drill.sh — the same pod stopped politely (SIGTERM, 2026-08-26)

`pod_kill_drill.sh`'s sibling, and the reason to read them together: identical
setup — a live 150 s MicroSIP call, tapped by pod A with a WS `gap_consumer.py`
attached, pod B idle on the same Redis — but pod A is **stopped** rather than
killed:

```sh
DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
  -f lab/docker-compose.webrtc.yml up -d rtpengine opensips freeswitch \
  call-watcher redpanda redis minio minio-init llm-bridge mss-control \
  mss-control-b
./lab/drain_drill.sh
# pod A is only stopped; bring it back with
DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml up -d mss-control
```

`docker stop -t 60`, because docker's default grace period is 10 s and the drain
window is 30 s — a shorter `-t` than `MSS_DRAIN_TIMEOUT_SECS` measures the
runtime's SIGKILL, not MSS's drain. The drill first asserts pod A's PID 1 is
`mediaserverd` (`cargo run` exec-replaces itself on Unix); if that ever changes,
the signal lands on cargo and the whole run means nothing.

### The run (stamp 1787732824, 2026-08-26)

| | SIGKILL (item 11) | SIGTERM drain (item 42) |
| --- | --- | --- |
| exit code | 137 | **0** |
| consumer audio gap | 14.41 s | **1.96 s** |
| adopted by the survivor | 14.4 s after the kill | **2.7 s after the signal** |
| lease | expired on its 15 s TTL | **released explicitly** |
| consumer close | socket dropped | Twilio `stop` frame after 2022 frames |
| rtpengine subscription | orphaned (D14) | unsubscribed |

`docker stop` returned in **0.44 s**; the drain itself logged
`elapsed_ms=30` against its 30 000 ms budget, so the timeout is a ceiling and
not a cost. Both tap legs finished with `jitter_lost=0`, `recv_errors=0`. The
consumer's two connections — 2022 frames, then 12752 after pod B re-dialed the
same endpoint — span one artifact, so the 1.96 s is measured on the wav, the
same way item 11 measured its 14.41 s. The full step-by-step log is in
[tasks.md item 42](tasks.md).

## soak.py — N calls for the better part of an hour (2026-08-22)

Item 19. Every drill above answers "does this work once". `lab/soak.py` answers
"is an hour of it boring", which is a different question and the one a pilot
actually asks. It runs N concurrent synthetic calls back to back for a
configurable duration under a walk of impairment profiles, scrapes every pod's
`/metrics` each minute, and **exits non-zero on any assertion** — so a cron or a
CI job can own it without anyone reading the log.

```sh
DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
  up -d rtpengine opensips freeswitch call-watcher redpanda redis \
        minio minio-init llm-bridge mss-control mss-control-b mss-control-c
SOAK_CALLS=3 CALL_SECONDS=120 SCRAPE_SECONDS=60 \
  SOAK_PHASES="clean:600,loss1:480,loss5:480,reorder:420,jitter:420,duplicate:300" \
  python3 lab/soak.py
```

As with every drill here, the compose `mediaserverd` service — the Phase-0
spike — **must stay down**: it taps whatever call it finds from its own process.

What it asserts, per pod, as deltas from a pre-run baseline (absolutes are
useless: these pods have been up through several drills):

| Assertion | Series |
| --- | --- |
| no leg is stalled | `mss_legs_stalled` is 0 at every scrape |
| the consumer queue never overflows | `mss_consumer_dropped_oldest_total` ≤ `MAX_DROPPED_OLDEST` (default 0) |
| no event is lost | `mss_events_failed_total`, `_abandoned_`, `_dropped_`, `_dropped_oldest_` all flat |
| the socket never overflows | `mss_ingest_recv_errors_total` flat, `mss_ingest_unparsable_total` flat |
| no lease is lost | `mss_registry_lost_total`, `mss_registry_failed_total` flat |
| the consumer is still being fed | `mss_consumer_delivered_total` moves whenever `mss_sessions_live > 0` |
| nothing leaked | at the end: `sessions_live` / `legs_live` / `consumers_live` all 0 and `mss:sessions` empty in Redis |
| no memory leak | daemon RSS at the end within `RSS_GROWTH`×start + `RSS_SLACK_KB` |

RSS is read from `/proc/<pid>/status` **inside** each pod, matched on
`comm == mediaserverd`, not from `docker stats`: the cgroup number folds in page
cache and the `cargo run` wrapper, and a leak hunt cannot afford that noise.

### Three things the lab needed before a soak was possible

- **`host_test_caller.py` is now N callers.** Everything that must be unique per
  caller became an env var — `SIP_PORT`, `RTP_PORT`, `CALL_ID`, `FROM_TAG`,
  `RTP_SSRC`, `EAR_PREFIX`, `WRITE_EARS` — with defaults that are exactly the
  single caller it has always been, so `grpc_stream_drill.sh` and
  `pod_kill_drill.sh` are untouched. `CALL_ID` is the one that removes work:
  with OpenSIPS in the path **rtpengine's call-id is the SIP Call-ID**, so a
  caller that names its own call-id tells the driver what to tap and no
  `call_watcher` polling is needed. Pairing that with `mss_ctl create`'s
  documented behaviour — pass the caller's from-tag and MSS asks rtpengine
  `query` for the rest — means a soak needs no discovery step at all.
- **`gap_consumer.py` grew a soak mode.** Its item-11 defaults key arrivals by
  track name and keep every payload in memory, which merges N calls into one
  timeline and costs hundreds of megabytes an hour. `GAP_BY_CALL=1` keys by
  `<callSid>/<track>` so each call gets its own continuity number,
  `GAP_KEEP_AUDIO=0` keeps only arrival stamps (the longest *arrival gap*
  survives; the longest *silent run* cannot, since it needs to know which sample
  slots a frame covered), and `GAP_JOURNAL=0` drops the 50-lines/s/track jsonl.
  All three default to the item-11 behaviour.
- **rtpengine's port range was too small for three calls.** One tapped
  two-party call costs **four port pairs**: one per call leg and one per tap
  stream. The lab's `--port-min=30000 --port-max=30020` is 10 pairs, so it held
  exactly **two** tapped calls and the third call's subscribe would have had
  nowhere to land. It is now 30000-30099 — 50 pairs, ~12 concurrent tapped
  calls with room for the ports rtpengine holds briefly after a teardown.
  Publishing 100 UDP ports through Docker Desktop recreates the container in
  about six seconds, so this costs nothing.

### `tc netem` is impossible on this box, and the probe is checked in

`lab/netem.sh` is the impairment tool testing.md calls "the single
highest-value item on this list", written the way it should be used:
`probe | apply <profile> | clear | show`, with the matrix's rows as profile
names (`loss1`, `loss5`, `burst`, `reorder`, `reorder-far`, `duplicate`,
`jitter`). It attaches a `prio` qdisc whose priomap sends **all** traffic to
band 1:1 and then steers only packets addressed to the MSS pods into band 1:3
where the netem lives — so the impairment lands on the tap link and nowhere
else, and loss MSS reports is loss MSS was given.

It cannot run here:

```
$ ./lab/netem.sh probe
netem: NOT available -- this kernel has no sch_netem
Error: Specified qdisc kind is unknown.
$ zcat /proc/config.gz | grep NET_SCH_NETEM
# CONFIG_NET_SCH_NETEM is not set
```

Docker Desktop on WSL2 runs containers on the WSL2 kernel itself
(`uname -r` inside a container is `5.15.153.1-microsoft-standard-WSL2`), and
that kernel has netem compiled out — not as a module either:
`/lib/modules/5.15.153.1-microsoft-standard-WSL2/` contains no `sch_*` at all.
`sudo` needs a password here, so even loading one is out. The documented fix is
the custom-kernel detour testing.md already prices at half a day
(`microsoft/WSL2-Linux-Kernel` + a Windows-side `.wslconfig` change), and it is
a Windows-side change, so it cannot be made from inside the distro.

Two instrument findings from getting that far, both worth keeping:

- **`docker exec` can never do this.** It cannot add a capability to a running
  container, so `tc` inside `mss-microsip-rtpengine-1` gets
  `RTNETLINK answers: Operation not permitted`. The way in is a sidecar sharing
  the namespace: `docker run --net container:<name> --cap-add NET_ADMIN`.
- **That sidecar needs `--user 0` or `--cap-add` is a no-op.** The rtpengine
  image's default user is unprivileged, and a non-root process gets
  `CapEff: 0000000000000000` however many `--cap-add` flags you pass — the same
  "Operation not permitted", from a different cause, which is exactly the sort
  of thing that gets misread as "netem does not work in Docker". With
  `--user 0` the capability is really there (`CapEff: 00000000a80435fb`) and the
  error changes to the honest one: unknown qdisc kind.

So the soak injects impairment at the matrix's **other** point instead — the
endpoint, upstream of rtpengine — using `host_test_caller.py`'s own
`IMPAIR_LOSS` / `IMPAIR_REORDER` / `IMPAIR_DUPLICATE` / `IMPAIR_JITTER_MS`
knobs, which damage that caller's outbound RTP in userspace and need no kernel
support. `soak.py` probes netem first and uses it when it is there
(`SOAK_NETEM=auto|on|off`), so the same script produces the stronger
measurement on a netem-capable box without an edit.

### The recorded run (2026-08-22)

`soak-1787401045`, 17:47:27 → 18:34:24 — **46 min 57 s**, rtpengine 14.1.1.8,
3 concurrent calls of 120 s, one slot per pod, scraping every 60 s, six
impairment phases (`clean:600,loss1:480,loss5:480,reorder:420,jitter:420,duplicate:300`).
**69 sessions created, tapped and destroyed — 23 per pod — and zero
assertions violated.**

| | pod A | pod B | pod C |
| --- | --- | --- | --- |
| sessions | 23 | 23 | 23 |
| tap datagrams | 256,066 | 256,170 | 256,108 |
| frames delivered to the consumer | 388,056 | 388,050 | 388,047 |
| `jitter_lost` = `frames_concealed` | 1,403 | 1,300 | 1,351 |
| `late_drops` / `duplicates` / `silence_gaps` / `resets` | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| `unparsable` / `recv_errors` / `ingest_stalls` | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 |
| `consumer_dropped_oldest` | 0 | 0 | 0 |
| events accepted / published / failed / retried | 69 / 69 / 0 / 0 | 69 / 69 / 0 / 0 | 69 / 69 / 0 / 0 |
| registry persists / adopted / leases lost | 517 / 0 / 0 | 518 / 0 / 0 | 517 / 0 / 0 |
| **daemon RSS, start → end** | **24,272 → 25,472 kB** | **26,424 → 27,160 kB** | **26,316 → 27,116 kB** |

768,344 tap datagrams and 1,164,153 delivered frames in total, and the leak
check is the last row: **+1.2 MB, +0.7 MB, +0.8 MB** across 47 minutes and 69
full session lifecycles each. `mss_ingest_stalls_total` never moved once, which
says the watchdog never even *transitioned* — the soak destroys each session
`TEARDOWN_LEAD` (6 s) before its caller hangs up, so no tap ever outlives its
audio and the 10 s stall window is never approached.

The consumer's own view, from `gap_consumer.py` (69 WS connections, all
carrying media, 207 tracks = 69 calls × the three D13 tracks):

| longest arrival gap per track | min | median | p90 | max |
| --- | --- | --- | --- | --- |
| over 1,164,153 frames | 21 ms | **31 ms** | 45 ms | **67 ms** |

Not one track's worst gap exceeded 100 ms against a 20 ms nominal spacing, and
the consumer's frame count matches the pods' `mss_consumer_delivered_total`
exactly, which is the two instruments agreeing.

### What each impairment profile cost

Summed over the three pods, counted only over the scrapes strictly inside each
phase. "Loss on the impaired leg" is `jitter_lost` against half the datagrams,
because the injection damages only the caller's leg while `datagrams` counts
both tap legs:

| Phase | injected | datagrams | lost | concealed | late | dup | loss on the impaired leg |
| --- | --- | --- | --- | --- | --- | --- | --- |
| clean | — | 132,015 | 0 | 0 | 0 | 0 | 0.00% |
| loss1 | `IMPAIR_LOSS=0.01` | 114,490 | 606 | 606 | 0 | 0 | **1.06%** |
| loss5 | `IMPAIR_LOSS=0.05` | 111,158 | 2,782 | 2,782 | 0 | 0 | **5.01%** |
| reorder | `IMPAIR_REORDER=0.10` | 98,691 | 97 | 97 | 0 | 0 | 0.20% (bleed, below) |
| jitter | `IMPAIR_JITTER_MS=35` | 98,797 | 0 | 0 | 0 | 0 | 0.00% |
| duplicate | `IMPAIR_DUPLICATE=0.01` | 65,950 | 0 | 0 | 0 | 0 | 0.00% |

Four things that table settles, none of which a replay test could:

- **Reported loss tracks injected loss, and concealment equals it exactly.**
  1% injected reads 1.06%; 5% reads 5.01%; and `frames_concealed` equals
  `jitter_lost` to the packet in every phase. That is the impairment matrix's
  first row, measured against a real rtpengine for the first time — and it means
  item 17's G.711 Appendix I PLC has now **run on a real link**, not only in
  replay. `silence_gaps` stayed 0 throughout, so the buffer correctly called
  this loss rather than sender silence.
- **Reorder and jitter cost nothing.** In the reorder phase the loss counter
  froze at 1403/1300/1351 and stayed there for five consecutive scrapes; in the
  jitter phase it did not move at all, across ±35 ms of arrival jitter, with
  `late_drops` 0 — the adaptive depth absorbed it. Matrix rows 3 and 6, on a
  real link.
- **Duplication never reaches the tap.** `mss_jitter_duplicates_total` stayed
  **0** through a phase that duplicated 1% of the caller's packets, so
  **rtpengine absorbs a duplicate upstream of the subscription**. The dedupe
  path therefore cannot be exercised from the endpoint at all; only netem on the
  tap link can reach it, which is one concrete thing this box's missing netem
  costs us.
- **Read per-phase numbers from the frozen interior, not the boundary.** The
  reorder row's 97 lost packets are loss5 bleed: calls are 120 s and scrapes
  60 s, so a phase's first settled scrape still contains the tail of a call that
  was dialed under the previous profile. Longer phases or shorter calls shrink
  it; the honest reading is the run of unchanged scrapes in the middle.

One number that is *not* ours: `underruns` grew at a steady ~0.25/s per leg in
**every** phase including clean. That is `host_test_caller.py` pacing slightly
under 50 pps against our wall-clock pacer — the same generator artifact lab.md
recorded for `call_driver.py` — and the pipeline answers it by feeding the
consumer a silence frame, which is why the stream stays gap-free.



## MSS_TAP_TRANSCODE=off — tapping the call's own codec (2026-08-22)

The question behind this run is an rtpengine-side one: **does asking for a
transcode keep a subscription out of rtpengine's kernel fast path?** The kernel
module has no codec — it forwards and can do SRTP — so a transcoding
subscription must be handled in userspace. Its own header shows the fan-out is
there (`num_destinations`, `RTPE_MAX_FORWARD_DESTINATIONS 32`, `do_intercept`),
so getting a tap onto the kernel path may be one flag on our side rather than an
eBPF project (tasks.md items 18 and 20).

This box **cannot** answer the rtpengine half: the lab runs `--table=-1`, so
`/proc/rtpengine` does not exist, and `/lib/modules/$(uname -r)/build` is absent
with `lsmod` empty, so the out-of-tree module cannot be built without the
custom-kernel detour. What it *can* answer is the MSS half, and that is what
this run is.

Procedure — the knob is on every pod as `${MSS_TAP_TRANSCODE:-on}`:

```sh
export DOCKER_API_VERSION=1.43
MSS_TAP_TRANSCODE=off docker compose -f lab/docker-compose.microsip.yml \
    up -d --force-recreate mss-control
./lab/grpc_stream_drill.sh
docker run --rm --network mss-microsip_lab -v "$PWD/lab":/lab:ro -w /lab \
    python:3-slim python3 ear_intelligibility_probe.py out/grpc-probe-<id>-customer.wav
```

The daemon says which path it took at startup, so a run is never ambiguous:
`accepting the call's own codec on the tap; rtpengine transcodes nothing`.

What changed on the wire — the lab caller offers PCMA, like MicroSIP:

| | transcode on (baseline) | transcode off |
| --- | --- | --- |
| offer's payload types, both streams | `[0, 101]` | **`[8, 101]`** |
| tap format MSS settled on | `Pcmu/8000/20ms` (from config) | **`Pcma/8000/20ms` (from the offer)** |
| rtpengine codec work per stream | decode + re-encode | **none** |
| MSS codec work per stream | decode | decode |
| `companded` | 0 | **0** |

Leg health, transcode off, 30 s probe on a 45 s call:

| track | datagrams | played | companded | unknown_pt | lost | concealed | recv_err |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Customer | 2166 | 2166 | 0 | 0 | 0 | 0 | 0 |
| Agent | 2203 | 2203 | 0 | 0 | 0 | 0 | 0 |

Customer track **rms 613.5** (baseline 614.5), agent rms 8.0 (FreeSWITCH
`silence_stream`), `mss_consumer_dropped_oldest_total 0`,
`mss_legs_unknown_ssrc 0`, `mss_legs_stalled 0`. Deepgram transcribed the
probe's own wav **verbatim** — "If you can hear this, injection works."
(confidence 0.99), "Hello.", "This is the media server speaking through your
bridge." — so the A-law tap is intelligible, not merely present.

Three things worth keeping from this run:

- **`companded=0` is the headline.** MSS decoded A-law natively rather than
  converting it to µ-law first. The system went from three codec operations per
  stream to one, so "MSS does the extra work" is backwards — rtpengine stops
  doing two and MSS keeps doing the one it always did.
- **Leg identity gets cleaner without transcoding.** rtpengine no longer
  re-stamps the leg with a generated SSRC, so `unknown_ssrc` stayed 0 without
  elimination having to cover a restamped leg.
- **The refusal is the safety net.** With the flag off, a G.722 or EVS call
  would land in `unknown_payload_type`; instead the subscribe is refused by name
  with the offered payload types in the message, naming transcoding as the fix.
  Opus is no longer in that list — it decodes natively since 2026-08-23, see
  the Opus tap below.
  Ask the platform team what codecs appear on real customer and agent legs
  before enabling this for a tenant.

**The lab was restored to `MSS_TAP_TRANSCODE=on` after the run**, which is the
compose default. A non-default lab left running is exactly how the
`RIGHT_TRACK=mixed` day happened.

## Tapping a call as Opus without a WebRTC endpoint

Opus ingest needed proof against real Opus on the wire, and building a WebRTC
caller to get it would have been the long way round. rtpengine can be asked to
transcode *to* Opus on the subscription leg, which produces genuine
libopus-encoded RTP from an ordinary G.711 call — so the whole path (dynamic
payload type, RFC 7587 answer, libopus decode, a 48 kHz RTP clock against a
16 kHz frame, the widened jitter slot) runs against something we did not
generate ourselves.

This runs **beside** a live lab rather than replacing it: it adds one more call
to the same rtpengine, so there is nothing to tear down and restore.

```sh
export DOCKER_API_VERSION=1.43
NET=mss-microsip_lab           # or mss-lab_lab

docker run -d --name opus-driver --network $NET --ip 172.31.99.120 \
  -v "$PWD/lab/call_driver.py:/call_driver.py:ro" \
  -e NG_NODE=172.31.99.10 -e NG_PORT=22222 -e SELF_IP=172.31.99.120 \
  -e CALL_ID=opus-proof -e FROM_TAG=finA -e TO_TAG=finB -e PUMP_SECONDS=300 \
  python:3-slim python3 /call_driver.py

docker run --rm --network $NET --ip 172.31.99.121 \
  -v "$PWD:/build:ro" -v ${NET}-target:/target \
  -v ${NET}-registry:/usr/local/cargo/registry \
  -v "$PWD/lab/out:/out" -w /build \
  -e CARGO_TARGET_DIR=/target -e RUST_LOG=info \
  -e MSS_RTPENGINE_NODE=172.31.99.10:22222 -e MSS_TAP_LOCAL_IP=172.31.99.121 \
  -e MSS_TAP_CALL_ID=opus-proof -e MSS_TAP_FROM_TAGS=finA,finB \
  -e MSS_TAP_FORMAT=opus -e MSS_OPUS_DECODE_RATE_HZ=16000 \
  -e MSS_TAP_TRANSCODE=on -e MSS_TAP_SECONDS=15 \
  -e MSS_TAP_OUTPUT=/out/opus_tap.wav -e MSS_TAP_DATAGRAM_LOG_DIR=/out \
  mss-lab-rust:1.95 cargo run --quiet -p mediaserverd
```

**The image matters.** `mss-lab-rust:1.95` (built from `lab/Dockerfile.rust`)
carries cmake, make and g++, which libopus's vendored build needs; the plain
`rust:1.95-slim` image the lab used before Opus landed fails at
`CMAKE_MAKE_PROGRAM is not set`. Build it with
`docker build -t mss-lab-rust:1.95 -f lab/Dockerfile.rust lab/`.

**Two calls expire, so run the tap right after the driver.** A subscribe to a
call rtpengine has forgotten answers `Unknown call-ID` — that is the expected
error, not a wiring fault. The first ever run also compiles libopus, which
takes long enough for a short-lived call to end underneath it; build once, then
run.

What a healthy Opus tap reported (2026-08-23, rtpengine 14.1.1.8, 15 s):

| track | datagrams | played | undecodable | frame_size_mismatch | carry_overflow | unknown_pt | lost |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Customer | 142 | 120 | 0 | 0 | 0 | 0 | 0 |
| Agent | 142 | 120 | 0 | 0 | 0 | 0 | 0 |

`answering the subscription with this codec payload_type=96
clock_rate_hz=48000 encoding=Opus decode_rate_hz=16000`, DTMF still detected on
both legs.

**Read the audio, not just the counters.** Zero-error counters only say nothing
crashed. The verdict is that the captured wav's dominant frequency is **440 Hz**
— the tone pumped in — at rms 8504, against the 8485 a 12000-amplitude sine
should give. The datagram log confirms the wire independently: RTP timestamp
delta **960** on every packet (20 ms at 48 kHz), contiguous sequence numbers,
TOC byte `0x08` (SILK narrowband, 20 ms, one frame per packet).

**Known: rtpengine under-produces when transcoding to Opus.** ~10 packets/s
where the same call tapped as PCMU gives ~51, so the tap is ~15% duty cycle and
mostly silence. Measured out: not CPU (rtpengine at 1–2%), not DTX or VAD (a
continuous tone behaves the same as the byte-ramp fixture — patch
`call_driver.py`'s payload line to a µ-law sine to check), not loss (zero jitter
anomalies), and not MSS (the identical code path on the identical call gives
1018/1018 for PCMU). Telling an rtpengine pacing bug from a lab artefact needs a
**native** Opus source. It does not affect the production shape, where the call
is already Opus and rtpengine transcodes nothing.

**Settled 2026-08-23 by `opus_call_driver.py`: it is rtpengine's transcoder.**
See the `kernel_probe.sh` and `opus_call_driver.py` sections below.

## group_recording_drill.sh — two calls, one recording (2026-08-23)

Item 21's recording groups had to be proved where they matter: **two separate
rtpengine calls recorded as one logical recording**, which is what a conference
is. No SIP is involved — `lab/call_driver.py` fabricates both calls — and the
drill runs **beside** a live lab on free IPs (`172.31.99.120-122`) with a pod
and a cargo target volume of its own, so nothing existing is restarted or
overwritten.

```sh
DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
  up -d rtpengine minio minio-init
./lab/group_recording_drill.sh
```

It builds `mediaserverd` in `mss-lab-rust:1.95` into its **own** volume
(`mss-group-target`, removed on exit — the lab pods' shared
`mss-microsip_lab-target` is deliberately untouched), starts two call drivers
and one control-plane pod at `172.31.99.122` with the control port published on
`127.0.0.1:19090` and metrics on `:19091`, then drives everything with
`mss_ctl` from the host:

```sh
mss_ctl $CONTROL create conf-alice group-call-a gaA,gaB
mss_ctl $CONTROL record conf-alice acct-conf/rec-<id>.wav alice conf-drill customer
```

`record` grew three optional arguments — `[label] [group] [track]` — where the
group is what joins two sessions into one recording and the label names the
participant's own file.

First green run (2026-08-22 20:07 UTC, rtpengine 14.1.1.8, MinIO
`RELEASE.2025-08-13`):

| gauge | while recording | after both detach |
| --- | --- | --- |
| `mss_sessions_live` | 2 | 0 |
| `mss_recordings_live` | 2 | 0 |
| `mss_recording_groups_live` | 1 | 0 |
| `mss_recording_group_members_live` | 2 | 0 |
| `mss_recording_group_joins_refused_total` | 2 | 2 |

```
acct-conf/rec-1787429250/alice.wav   740 KiB  47.34 s  mono 8 kHz  rms 9960
acct-conf/rec-1787429250/bob.wav     741 KiB  47.40 s  mono 8 kHz  rms 9971
Content-Type: audio/wav
```

Both files' frame counts equal the `duration_ms` the daemon reported to the
packet (378,720 / 8000 = 47.340 s), and `acct-conf/rec-1787429250.wav` — the
frozen two-leg key — does **not** exist, which is the check that a grouped
recording never writes the ungrouped object as well. (That first pass was
driven by hand for ~47 s; the scripted run records `RECORD_SECONDS`, 20 s by
default, and produced the same shape — 314 KiB and 346 KiB. The members differ
by ~2 s because `Detach` **waits for the upload** (D11) and the drill detaches
them one after the other, so the second member keeps recording while the first
one uploads.)

The two refusals in that run are the ones a caller will actually hit, and they
say what to do:

```
recording group acct-conf/conf-drill already has a participant writing
  acct-conf/rec-1787429250/alice.wav; each member needs a label of its own
recording group acct-conf/conf-drill is already recording rec-1787429250 and
  one group writes one recording; attach with acct-conf/rec-1787429250.wav or
  a different group
```

### The staggered re-run that proved the group time anchor (P2-1, 2026-08-23)

The drill grew `JOIN_STAGGER_SECONDS` (default 5): alice attaches, the drill
waits, **then** bob attaches, so the second member joins a group that is
already open — which is what D18 was about. With P2-1, bob's file is padded
back to the group's open instant instead of starting at its join moment.

Run 2026-08-23 16:03 UTC, `JOIN_STAGGER_SECONDS=5 RECORD_SECONDS=20`:

```
pod log: this recording group member joined late; lead_silence_ms=10    (alice)
pod log: this recording group member joined late; lead_silence_ms=5016  (bob)

acct-conf/rec-1787500885/alice.wav  400524 B  200240 frames  25.030 s
acct-conf/rec-1787500885/bob.wav    401900 B  200928 frames  25.116 s
```

Read back off MinIO and measured sample by sample: **bob.wav opens with 40128
zero samples = 5016 ms of silence**, exactly its pad, and alice.wav opens with
50 ms. Both files therefore start at the same wall instant, and they differ in
length by **86 ms** rather than by the 5 s stagger — the residual is the tail,
not the head: `Detach` waits for the upload (D11) and the drill detaches
alice first, so bob records through alice's ~47 ms upload.

`mss_recording_group_joins_refused_total` is still 2 (the reused label and the
second recording id), so the anchor changed no refusal.

**The drill found a bug in the instrument first (worth keeping).**
`call_driver.py` could not fabricate **two** calls at once: both containers
number their NG cookies from `lab-1`, so rtpengine's duplicate-cookie reply
cache answered the second driver with the *first* call's SDP — both "calls"
claimed source ports 30028/30042 and the second tap would have pointed at the
first call's subscription. This is exactly D12's shape, on the driver side.
`COOKIE_PREFIX` (default `lab`, so every existing drill behaves as before) is
the fix; with `grpa`/`grpb` the two calls get 30028/30042 and 30090/30096. If
you ever write a multi-call drill, give every driver its own prefix.

**What this does not prove:** no SIP, no FreeSWITCH conference, and the audio
is the driver's byte-ramp fixture rather than speech — it exercises the group,
the participant keys, the per-member pause and the uploads against real
rtpengine and real object storage, not a real conference. It also runs one pod
by construction: a group lives in that pod's memory (D16).

## kernel_probe.sh — is rtpengine's kernel module in play? (2026-08-23)

`lab/kernel_probe.sh <host> <port>` answers "does this rtpengine forward media
in the kernel?" over NG alone, so the same script runs against a container, a
staging node or a production box. It exists because item 18's decision gate
needs that answer on a host this box cannot be, and because the answer changes
what a tap costs on the rtpengine side.

The evidence is rtpengine's own accounting. `statistics` splits both the
lifetime relay totals and the live rates between the kernel module and
userspace, so no `/proc` access and no root are needed:

| key | where | what it settles |
| --- | --- | --- |
| `relayedpackets_kernel` / `_user` | `totalstatistics` | has the module *ever* forwarded on this node |
| `packetrate_kernel` / `_user` | `currentstatistics` | is it forwarding *now* |
| `media_kernel` / `media_userspace` / `media_mixed` | `currentstatistics` | how many media streams sit on each path |
| `transcoders[].chain` / `.packets` | top level | which codec conversions are running, i.e. which streams *cannot* be in the kernel |

Verdicts and exit codes: `0` module in play (now, or earlier on this node),
`1` userspace only, `2` cannot tell (with the reason — either the node has
relayed nothing yet, or its statistics carry no kernel/userspace split),
`3` unreachable. Run on the rtpengine host it also reports `/proc/rtpengine`
and `lsmod`, which NG cannot expose; anywhere else it says that half is
skipped rather than guessing.

The NG port is not published to the WSL host, so reach it from inside the lab:

```sh
export DOCKER_API_VERSION=1.43
docker run --rm --network mss-microsip_lab -v "$PWD/lab":/lab:ro \
    python:3-slim sh /lab/kernel_probe.sh 172.31.99.10 22222
```

What the lab answered (2026-08-23, rtpengine 14.1.1.8-jambonz11, `--table=-1`):

```
kernel_probe: no /proc/rtpengine (module not loaded here, or not this host)
kernel_probe: this rtpengine has no NG version command ('Unrecognized command')
kernel_probe: uptime 24752s, sessions now 4, transcoding media now 0
kernel_probe: relayed packets total=150462 kernel=0 userspace=150462
kernel_probe: packets/s now kernel=0 userspace=207; media now kernel=0
              userspace=4 mixed=0
kernel_probe: transcoding 'PCMU/8000 -> opus/48000/2', 7378 packets
              (a transcoded stream cannot be kernel-forwarded)
kernel_probe: VERDICT the kernel module is NOT in play -- every packet this
              node relayed went through userspace
```

Exit code 1. That is the expected answer for this lab and it machine-verifies
the "no module" path: 150k packets relayed, every one of them in userspace,
including 207/s live while calls were up.

**Two things the probing itself taught us.**

- **There is no NG `version` command — anywhere.** Not in 14.1.1.8-jambonz11
  (`Unrecognized command`) and not in the upstream protocol documentation,
  whose command list runs ping, offer, answer, delete, query, recording and
  media verbs, `statistics`, publish/subscribe/unsubscribe, connect, create,
  mesh — and no version. Brute-forcing 37 candidate names against the live node
  found exactly six that exist: `ping`, `list`, `statistics`, `transform`, plus
  the call-scoped verbs. The version has to come from the process, the package
  or the CLI interface (`--listen-cli`), none of which is NG. MSS therefore
  probes for it, reports "unknown: this rtpengine's NG protocol has no version
  command", and bases every kernel judgement on `statistics` instead.
- **rtpengine caches NG replies by cookie, and a probe script must not reuse
  one.** The first cut of `kernel_probe.sh` used a fixed cookie for all three
  commands and got the *ping* reply back for `statistics` and `version` —
  `{'result': 'pong'}` — which read exactly like a node that answers nothing
  useful. The same cache is what makes MSS's retransmit-the-same-cookie retry
  idempotent (and what caused defect D12), so this is a trap for every future
  probe: one cookie per command.

## opus_call_driver.py — a native Opus call, and the verdict on rtpengine's Opus under-production (2026-08-23)

`lab/opus_call_driver.py` is `call_driver.py` with libopus in place of the
G.711 byte ramp: both legs offer and answer `opus/48000/2` at a dynamic payload
type, encode a real 440 Hz tone with libopus through ctypes (CBR 24 kbit/s, VBR
and DTX explicitly off), and pump 960-sample frames on a wall-clock-anchored
20 ms pacer. rtpengine only relays. The tone is built as a whole number of
periods across a whole number of frames (5 frames of 960 samples = 4 periods of
1200) so replaying the frame list is phase-continuous forever.

It exists to settle 16b-2's one open question: rtpengine emitted ~10 packets/s
when *it* transcoded G.711 to Opus, where the same call tapped as PCMU gave
~51. CPU, DTX, VAD, loss and MSS were already ruled out by measurement; the
missing experiment was Opus that rtpengine did not generate.

```sh
export DOCKER_API_VERSION=1.43
NET=mss-microsip_lab

docker run -d --name opus-native-driver --network $NET --ip 172.31.99.130 \
  -v "$PWD/lab/opus_call_driver.py:/opus_call_driver.py:ro" \
  -e NG_NODE=172.31.99.10 -e SELF_IP=172.31.99.130 \
  -e CALL_ID=opus-native-proof -e FROM_TAG=natA -e TO_TAG=natB \
  -e PUMP_SECONDS=420 \
  python:3-slim sh -c 'apt-get update -qq && apt-get install -y -qq libopus0 \
                       && python3 /opus_call_driver.py'

docker run --rm --network $NET --ip 172.31.99.131 \
  -v "$PWD:/build:ro" -v ${NET}-target:/target \
  -v ${NET}-registry:/usr/local/cargo/registry -v "$PWD/lab/out:/out" \
  -w /build -e CARGO_TARGET_DIR=/target -e RUST_LOG=info \
  -e MSS_RTPENGINE_NODE=172.31.99.10:22222 -e MSS_TAP_LOCAL_IP=172.31.99.131 \
  -e MSS_TAP_CALL_ID=opus-native-proof -e MSS_TAP_FROM_TAGS=natA,natB \
  -e MSS_TAP_FORMAT=opus -e MSS_OPUS_DECODE_RATE_HZ=16000 \
  -e MSS_TAP_TRANSCODE=off -e MSS_TAP_SECONDS=15 \
  -e MSS_TAP_OUTPUT=/out/opus_native_tap.wav \
  -e MSS_TAP_DATAGRAM_LOG_DIR=/out \
  mss-lab-rust:1.95 cargo run --quiet -p mediaserverd
```

The driver refuses to pump if rtpengine renumbered the Opus payload type, so a
run that reaches the tap really is a native-Opus call. It did not: rtpengine
answered pt **111**, the offered type, and the tap saw `payload_types [111,
101]` — nothing transcoded.

**The number, and the verdict.** Same rtpengine, same afternoon:

| tap | rtpengine's codec work | audio packets in 15 s | packets/s |
| --- | --- | --- | --- |
| PCMU, same G.711 call | pass-through | 766 | **51.1** |
| Opus, **rtpengine transcoded it** from that same G.711 call | G.711 → Opus | 85 − 28 telephone-events = 57 | **3.8** |
| Opus, **native** from the endpoints, `MSS_TAP_TRANSCODE=off` | none | 750 | **50.0** |

**rtpengine's G.711→Opus transcoder under-produces; the relay path and MSS are
innocent.** Native Opus arrives at the sender's full rate — 750 datagrams in
15 s against 750 sent, both legs — and rtpengine relays it 1:1 (the driver's
own ears also read 50.00/s each way over a 100 s control run). The transcoded
case was *worse* than the ~10/s recorded in 16b-2, at 3.8/s, and on one run its
Customer leg produced **zero** Opus packets for 15 s while the same call gave
51.1/s as PCMU. **This does not affect the production shape**, which is a
WebRTC call that is already Opus tapped with transcoding off — exactly the
column that measured 50.0/s. It does mean `transcode: [opus]` is not a usable
way to *manufacture* Opus for anything but a smoke test.

Leg health on the native run, `MSS_TAP_TRANSCODE=off`, 15 s:

| track | datagrams | played | undecodable | frame_size_mismatch | carry_overflow | unknown_pt | lost | concealed | recv_err |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Customer | 750 | 746 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Agent | 750 | 746 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

`answering the subscription with this codec payload_type=111
clock_rate_hz=48000 encoding=Opus decode_rate_hz=16000`. The four unplayed
frames per leg are the jitter buffer's target depth (5) still holding audio at
teardown, not loss.

**Read the audio, not just the counters.** The captured wav's dominant frequency
is **440.0 Hz on both channels** at rms 8465.9, against the 8485 a
12000-amplitude sine should give, with 238698 of 239680 samples non-zero. The
datagram log confirms the wire independently: 750 packets per leg, payload type
**111**, **the endpoints' own SSRCs** (`0x33333333`/`0x44444444` — rtpengine did
not re-stamp them, the same "leg identity gets cleaner without transcoding"
effect item 20 found), sequence deltas all **1**, RTP timestamp deltas all
**960**, and a constant **60-byte** payload (CBR 24 kbit/s at 20 ms). The TOC
bytes are `0x4B` (551 packets) and `0x48` (199) — SILK wideband, config 9,
mono, one packet carrying two 10 ms frames and the other one 20 ms frame. MSS
decoded both shapes with `frame_size_mismatch=0`, which is the multi-frame
path 16b-2 built the `Carry` for.

**One unexplained observation, recorded rather than claimed.** On the 420 s run
the caller's own ear fell from 50/s to ~16/s average partway through while the
callee's stayed at 50/s; the tap itself was unaffected. Two discriminating runs
say it was not the tap: a 100 s call with **no** tap held 50.00/s in both ears,
and a 110 s call **tapped and unsubscribed mid-call** also held 50.00/s in both
ears. So an MSS subscribe/unsubscribe does not damage the tapped call. The long
run overlapped other agents' lab traffic and it has not reproduced; if it ever
does, the discriminating experiment is a long call with nothing else on the box.

## webrtc_agent_drill.sh — a real browser as the agent leg, on its own rtpengine node (2026-08-23)

Every drill above anchors both legs of a call in **one** rtpengine, and every
Opus packet MSS had decoded up to this point came from `lab/opus_call_driver.py`.
This is rung 2: **two legs, two rtpengine nodes, and a real browser** on the
agent side, so that "MSS taps each leg where that leg is anchored" and "MSS
decodes what a browser actually sends" stop being assumptions.

```
MicroSIP (PCMA)  -> opensips      -> rtpengine RE1 (172.31.99.10) -> FreeSWITCH
Chrome (WebRTC)  <- opensips-agent <- rtpengine RE2 (172.31.99.11) <- FreeSWITCH
                              both legs meet in one FS bridge (ext 4001)
MSS taps each leg at its own node: two sessions, one recording group
```

It is a **bridge, not a conference** — the lab FreeSWITCH image has no
`mod_conference` (and no `mod_opus`, and no `mod_loopback`), so `4001` sets
`absolute_codec_string` and bridges to `sofia/internal/agent@172.31.99.71`,
which is the agent proxy. Naming the codec on the inbound leg fixes it on the
outbound leg too, which is what makes a nothing-transcodes control run possible.

```sh
export DOCKER_API_VERSION=1.43 ELEVENLABS_API_KEY=x DEEPGRAM_API_KEY=x
docker compose -f lab/docker-compose.microsip.yml \
               -f lab/docker-compose.webrtc.yml up -d
./lab/webrtc_agent_drill.sh                      # PROFILE=opus AGENT=headless
PROFILE=control ./lab/webrtc_agent_drill.sh      # PCMU end to end
PROFILE=opus AGENT=browser ./lab/webrtc_agent_drill.sh
./lab/webrtc_record_live.sh                      # you place the call; it taps
```

The overlay adds five containers: `rtpengine-agent` (RE2, ports 30100-30199),
`opensips-agent` (ws on 5062 for the browser, UDP 5060 so FS can reach it),
`webrtc-page` serving `lab/webrtc/` with a vendored JsSIP 3.10.10, a headless
`chrome-agent` that dials on its own with a wav file for a microphone, and a
`call-watcher-agent` that reports RE2's view of the call. `AGENT=browser` prints
a URL instead, for the run you want to hear yourself.
`AGENT_ADVERTISE_IP` (default `127.0.0.1`) is what RE2 advertises to the
browser: leave it alone for a browser on this box, set it to the box's LAN
address for a browser on another machine — and expect a corporate endpoint
firewall to be the reason the LAN variant goes silent (ESET, here).

### The verified run (stamp 1787492636, PROFILE=control)

Two MSS sessions on two rtpengine nodes, both settled **Pcma / 8 kHz /
`transcoding: false`** — the control case, nothing transcoding anywhere. The
browser sent a 440 Hz tone (Goertzel purity **0.707**, i.e. a pure sine) while a
human spoke into MicroSIP (rms 200-3000, irregular). Across both sessions:
**zero `jitter_lost`, zero `recv_errors`, zero `unknown_ssrc`**. Seven uploads,
zero failures. The artifacts, read back with `lab/webrtc/wav_summary.py`:

| object | shape | rms | peak |
| --- | --- | --- | --- |
| `acct-webrtc/rec-1787492636.wav` | 2 ch 8 kHz 90.06 s | ch0 3448 / ch1 690 | 11520 / 16128 |
| `acct-webrtc/grp-1787492636/customer.wav` | 1 ch 8 kHz 90.26 s | 3445 | 11520 |
| `acct-webrtc/grp-1787492636/agent.agent.wav` | 1 ch 8 kHz 90.32 s | 4609 | 11520 |
| `acct-webrtc/grp-1787492636/agent.customer.wav` | 1 ch 8 kHz 90.32 s | 689 | 16128 |

The peaks are the check that matters: **11520 and 16128 appear on both nodes**.
The stereo file is the customer leg tapped at RE1 with both directions in one
object; the group members are the same two directions arriving through a
*separate* subscription to RE2 — so the cross-node recording group really did
assemble one recording out of two rtpengine calls, one object per participant
track. (The agent member was attached with track `all`, which is why it wrote
two objects rather than one.)

Two soft spots this run exposed are recorded as defects in
[tasks.md](tasks.md): the leg labels **inverted** when `from_tags` was left
unspecified (`-`), because rtpengine's `query` answered with FreeSWITCH's tag
first (**D17** — speaker attribution is only trustworthy when the caller tag is
passed explicitly), and the group's member files were **not time-aligned**: each
anchored on its own first frame, so the three lengths above differ by up to
0.26 s and a late joiner's file simply started at its join moment (**D18**,
fixed 2026-08-23 by the group time anchor — see the staggered re-run above;
these numbers are from before that fix).
The browser leg also **outlived the hung-up call by ~12 s** — the tone ran to
second 29 against the customer leg's 17 — which is billable media tail after
hangup and deserves an eye in a pilot.

### Five vendor findings, each of which cost a run

- **FreeSWITCH does not merge two sibling `<context name="default">` blocks.**
  A lab dialplan dropped in beside the image's own was visible in
  `xml_locate` and still answered `NO_ROUTE_DESTINATION`. The fix is to merge
  the extensions **into** the image's `default.xml` — which is why
  `lab/freeswitch/dialplan-default.xml` is a merged artifact and
  `dialplan-lab.xml` is kept only as the standalone original.
- **XML comments cannot contain `--`.** An en-dash-style comment silently
  broke the whole dialplan parse.
- **JsSIP registers an RFC 7118 Contact** —
  `sip:x@y.invalid;transport=ws` — and OpenSIPS 3.4.18's `lookup()` plus
  `fix_nated_register()` still tries to **DNS-resolve** it (`tm` reports
  "failure to add branches"). The fix in `opensips-agent.cfg` is to remember
  `$si:$sp` at REGISTER in a `cfgutils` shared variable (declared with
  `shvset`) and set `$du` from it at call time.
- **Chrome refused rtpengine's offer** until `max-bundle` was dropped
  page-side and `generate-mid` was added to `rtpengine_offer`.
- **An answer munged to `[opus, telephone-event]` against a no-Opus offer
  leaves DTMF-only SDP**, and FreeSWITCH answers `INCOMPATIBLE_DESTINATION`.
  The page now refuses to munge unless the chosen codec is actually present in
  the offer.

### What this does not prove

The Opus profiles (`opus`, `dtx`, `red`, `ptime60`, `cbr`, `stereo`, `dsp`)
**have not been run**: this FreeSWITCH image has no `mod_opus`, so FS cannot sit
in the middle of an Opus call. Running them needs an rtpengine codec-mask
arrangement that keeps FS out of the codec decision, which is a future item.
Until then MSS has decoded browser **G.711**, not browser **Opus** — and the
gap between browser Opus and `opus_call_driver.py` is spelled out in
[testing.md](testing.md).

## barge_drill.sh — barge-in cut-through, all four hops (2026-08-23)

Barge-in is four hops, and all four are MSS's once D19 is fixed:

1. a consumer (ASR, voice-AI) decides the caller started talking and **tells
   MSS**;
2. MSS publishes that as a `MediaEvent` on Kafka `mss.events`;
3. somebody's translator consumes the event and decides to cut the prompt;
4. that translator calls `StopPlayback` on `MediaControl`, and MSS stops the
   media.

`lab/barge_drill.sh` + `lab/barge_translator.py` measure all four hops against
the live lab.

**The first version of this drill could only measure hops 2–4**, because hop 1
had no wire: `session-core`'s `Registry::report` — the `ConsumerEvent` →
`SpeechStarted`/`Partial`/`Final` path that feeds the pump — was reachable only
from unit tests (defect D19, found by that run). Its trigger event was
therefore `PlaybackStarted`. **D19 is fixed** (2026-08-23): a gRPC consumer
holding `CAPABILITY_EVENTS` sends `ConsumerToServer.SpeechReport` on its
`MediaStream.Subscribe` stream and MSS publishes the event the report names, so
the drill now triggers on a **real `SpeechReport(STARTED)` → `SpeechStarted`**.

Two things are still out of frame, both by nature rather than by omission. The
consumer's own **detection** latency — how long an ASR takes to decide speech
began — belongs to whatever consumer an integrator runs and nothing in MSS can
measure it. And a **WS-Twilio consumer cannot report speech at all**: that
dialect's bytes are frozen and carry no such message, so its only barge is the
Twilio `clear`, which takes a different road entirely (a direct rtpengine
`stop media` from `tap_session.rs`, never an event).

The drill runs **beside** the live compose lab and adds one container: a
fabricated call (`lab/call_driver.py`, no SIP) at 172.31.99.123, tapped by the
lab's own `mss-control` pod, so events travel the real pump into the real
Redpanda (`mss.events`, published to the host at 127.0.0.1:19092). The mock
translator runs on the WSL host: a `kafka-python-ng` consumer parked at the
end of the topic plus a warm gRPC channel to 127.0.0.1:50551. It plays **both**
integrator roles on purpose — it starts the prompt, **reports the speech** as
the gRPC consumer, *and* barges it as the translator — so every interval in the
headline is measured on one clock, with no container/host skew in it. The
consumer role is a real attachment (`mss_ctl <endpoint> consume`, transport
`GRPC_STREAM`, capabilities `SINK`+`EVENTS`) on a live stream: it took **378
tapped audio frames** while measuring, drained on a thread because a consumer
that lets its outbound queue back up gets dropped. Skew was measured anyway (`PlaybackStopped` is stamped inside the
`StopPlayback` call, so its `at` must fall inside this process's send/ack
window): **median −0.12 ms**, i.e. negligible.

### Numbers with the real speech report (two runs of 10 iterations, 2026-08-23)

Every hop MSS owns, from the consumer's word to the stop being acked. 10/10
iterations completed in both runs, no event missed.

| interval | run A p50 / p95 / max | run B p50 / p95 / max |
| --- | --- | --- |
| **cut-through**: `SpeechReport` sent → `StopPlayback` acked | **3.98 / 4.78 / 4.78 ms** | **3.54 / 4.38 / 4.38 ms** |
| hops 1–2: `SpeechReport` sent → `SpeechStarted` consumed | 1.79 / 2.43 / 2.43 ms | 1.73 / 2.23 / 2.23 ms |
| hops 3–4: event in hand → `StopPlayback` acked | 2.04 / 2.63 / 2.63 ms | 1.86 / 2.15 / 2.15 ms |
| MSS's event `at` → consumed (skewed) | 0.93 / 1.46 / 1.46 ms | 0.94 / 1.23 / 1.23 ms |
| `StartPlayback` acked → `PlaybackStarted` consumed | 1.57 / 2.43 / 2.43 ms | 1.62 / 2.01 / 2.01 ms |

**Adding the missing first hop cost under a millisecond.** The earlier
`PlaybackStarted`-triggered runs (25 iterations each, same stack, same day)
read cut-through p50 3.31 / 3.15 ms and p95 4.19 / 3.64 ms; putting the real
consumer wire in front of the bus moved p50 to 3.54–3.98 ms. The consumer
report is, to the measurement's resolution, free.

Per-iteration rows are kept in `lab/out/barge-drill-<stamp>.jsonl`.

**The Kafka hop is not the problem.** The whole chain MSS owns fits inside a
single 20 ms frame with an order of magnitude to spare — p95 4.8 ms — so
architecture §9 risk 9's fallback (a gRPC stream *for speech events only*) is not
needed on these numbers. `StartPlayback` itself is the slow call in the drill
(p50 11–43 ms: it ships a WAV blob to rtpengine), which is a prompt-start cost,
not a barge cost.

What no lab number here covers: `StopPlayback` **acked** is not the last
audible sample. Measuring the audible cut needs an ear on the leg — the
Phase-3 inline drill.

**Why the last row is not the bus latency.** `at` is stamped when the registry
commits the event, and in the `StartPlayback` path that happens *before* MSS's
rtpengine `play media` round trip inside the same RPC (that RPC's own p50 was
10.84 ms). So `at` is a **commit** timestamp, not a publish timestamp, and the
11 ms is mostly rtpengine. The honest publish→consume number is the
`PlaybackStopped` row: **~1 ms**. Run A's 38.88 ms outlier tracks a 38.9 ms
`play media` round trip in the same iteration, not a broker stall.

### Setup notes

- The host `python3` (WSL, 3.10) has no `ensurepip`, so there is no venv:
  `pip install --user grpcio grpcio-tools kafka-python-ng`. The drill
  generates the `MediaControl` stubs into `lab/out/pb` itself.
- A playback blob is capped at 60 000 bytes (`MAX_PLAYBACK_BLOB_BYTES`, one NG
  datagram) — about 3.7 s of 8 kHz s16. A 6 s tone is refused with *"exceeds
  what one NG datagram carries; chunked playback is not implemented"*, so the
  drill's prompt is 3 s of 440 Hz; `TONE_SECONDS` must stay under the cap.
- One `COOKIE_PREFIX` per call driver, as always (the D12 shape).

### What this does not prove

- **Hop 1 does not exist** (D19), so no consumer-detection latency is in these
  numbers, and the *shape* of the eventual speech-report hop could add its own
  cost.
- **The integrator's half is theirs.** Hop 3 here is a 40-line Python mock with
  the topic to itself; a real translator (the reference deployment's, awaiting
  review in its own repo) has other traffic, other consumers in its group, and
  a downstream call-control hop after `StopPlayback`.
- **`StopPlayback` acked is not the last audible sample.** The ack means
  rtpengine accepted `stop media`; how quickly the caller stops hearing the
  prompt is a media-path number that needs an ear on the leg, and that
  measurement belongs to the Phase-3 inline drill.
- An idle box, one session, one playback at a time, a single-broker Redpanda
  and no competing load. Under the soak suite these numbers should be
  re-taken.

## inline_call_drill.sh — an inline leg with a real ear, and the barge-in number (2026-08-23)

Items 32–34 built MSS's *mouth* — a socket that answers an SDP offer, a
sans-IO pacer that speaks on a 20 ms grid, and an inject stream that keeps it
fed — and none of it had met an endpoint. This drill is the endpoint, and it is
also the instrument for the one number Phase 3 owed: how long after a consumer
says *stop talking* does the caller stop hearing the bot.

Three new pieces, no SIP and no human anywhere in it:

- **`lab/inline_peer.py`** — the far end of one RTP flow, in the
  `call_driver.py` style but with rtpengine removed, because an inline leg *is*
  MSS's own socket. It writes its offer to `offer.sdp`, waits for the drill to
  drop `answer.sdp` beside it, then sends 440 Hz µ-law at 50 pkt/s and records
  every datagram MSS sends with its **arrival wall clock**, ssrc, sequence
  number, rtp timestamp and the Goertzel energy of the injected tone *in that
  one 20 ms payload*. It writes one ear wav per ssrc on the rtp-timestamp
  timeline as well, for a human or `wav_summary.py`.
- **`lab/inline_consumer.py`** — the voice-AI half: it attaches itself with
  `CAPABILITY_SINK | CAPABILITY_INJECT` (this is the lab's only INJECT actor;
  `mss_ctl consume` asks for SINK+EVENTS), streams 1000 Hz as continuous
  `inject` frames keeping `LEAD_MS` of audio queued, exercises `Mark`, then
  runs N rounds of *talk 1.5 s, send `Clear`*, stamping each `Clear`.
- **`lab/inline_barge_report.py`** — reads both timelines and asserts pacing,
  the ear, the tap, the mark, and prints the cut-through distribution.

Both python actors run as **containers on the lab network**, which is not a
detail: the barge number is a difference between a stamp taken in the consumer
and a stamp taken in the peer, and under Docker Desktop on WSL2 a host process
and a container process do not share a clock. Two containers do — same kernel —
so no skew estimate is needed and none is claimed.

### The run (stamp 1787508102, 20 iterations)

| What | Measured |
| --- | --- |
| SDP answer | `c=IN IP4 172.31.99.31`, `m=audio 39974 RTP/AVP 0 101` (PCMU + telephone-event) |
| egress pacing at the ear | **50.19 pkt/s** over 66.0 s, **0** sequence breaks, rtp timestamp step 160 for all 3311 gaps |
| the injected tone arrived | 1710/3312 packets above the floor, peak Goertzel **11910** of a theoretical 12000 (a clean 1000 Hz sine) |
| the tap still worked | 2770 frames of the peer's own audio to the same consumer, peak rms 17132, peak 440 Hz energy 12213 |
| `Mark` drain barrier | acked at **410 / 404 / 404 ms** against 400 ms of queued audio |
| **barge-in cut-through** | n=20, **p50 12.2 ms, p95 20.4 ms, max 21.0 ms**, min 2.4 ms |
| pod counters | `clears_total` 20, `drained_samples_total` == `pushed_samples_total`, `late_ticks_total` 0, `dropped_samples_total` 0, `send_errors_total` 0 |

The cut-through distribution is the interesting part, and it is exactly the
shape the design predicts: `Clear` flushes the chunk queue *and* the pacer ring,
so the only thing left between the flush and silence at the ear is the wait for
the pacer's next 20 ms deadline. That wait is uniform over one ptime, which is
why the samples spread almost evenly from 2.4 ms to 21.0 ms with a p50 near half
a frame. **The target was one ptime and the max is one ptime plus a millisecond
of transport.**

### Two ways the first attempt lied, and the fixes

- **A phase-locked measurement.** The first run reported p50 12.6 / p95 15.3 /
  max 16.0 ms with the per-iteration numbers decreasing monotonically — 16.0,
  15.3, 14.5 … 9.8. Nothing was drifting: the iteration period was
  1.5 s + 1.0 s = exactly **125 packets**, so every `Clear` landed at nearly the
  same phase of the pacer's 20 ms grid and the run sampled a quarter of the
  distribution. `JITTER_MS` (default 20) now adds up to one frame of random
  delay per iteration, and the p95 moved from 15.3 to 20.4 ms — the tighter
  number was the wrong one.
- **A real defect in the mark ack (fixed here).** The first run's `Mark` was
  acked **8221 ms** after it was sent, while the ear went quiet 400 ms after it,
  exactly on time — so the audio drained correctly and only the *ack* was late.
  Cause: both stream loops built the 20 ms drain poll as
  `tokio::time::sleep(...)` *inside* `tokio::select!`, so the timer was
  recreated — and therefore reset — on every loop iteration. With tapped frames
  arriving every 20 ms and `select!` choosing randomly among ready branches, the
  sleep was cancelled before it ever completed; the ack waited for a lucky gap.
  Fixed by pinning one `tokio::time::interval` outside the loop and gating the
  branch on `if marks_pending` (`stream.rs`, `consumer_ws.rs`); the ack is now
  403–410 ms against a 400 ms lead, i.e. accurate to one frame as documented.
  The lesson is general: **a `sleep` inside `select!` is a timeout, not a
  timer**, and it starves under any branch that fires more often.

### What this does not prove

- **One PCMU/8 kHz leg, one INJECT consumer, on an idle box.** No PCMA leg, no
  L16 consumer, no Opus (an inline leg cannot do Opus at all — there is no
  encoder), no impairment, no competing load, no second inline leg.
- **No SIP.** The offer/answer travels through two files and `mss_ctl inline`;
  a real B2B leg brings re-INVITEs, hold, and DTMF, none of which is exercised.
- **The consumer's own detection latency is still not in the number** — the
  same honest boundary `barge_drill.sh` draws. This drill measures
  `Clear`→silence; item 5 measures speech-report→`StopPlayback`. Adding them is
  the closest MSS gets to an end-to-end barge-in claim, and the ASR's decision
  time belongs to whoever ships the ASR.
- **The cut-through is an arrival measurement**, so it *includes* the gRPC hop,
  the 5 ms pump tick, and the lab bridge. It is an upper bound on MSS's own
  cost, never a lower one.
- The peer's ear was checked by Goertzel and rms, **not by a human listening**
  to `peer_ear_*.wav`.

## conference_drill.sh — three legs in one conference, and the whole feature set (2026-08-23)

`lab/conference_drill.sh` is Phase 4's proving run: three `lab/inline_peer.py`
containers, at 440 / 880 / 1320 Hz, seated in **one conference** by
`mss_ctl inline <id> <call> <offer> <group>`. There is no FreeSWITCH in the
path and **no rtpengine either** — an inline leg is MSS's own UDP socket
answering an SDP offer, so this drill exercises the mix, the routes and the
recorders with nothing else able to take the blame.

Everything is decided by numbers. Each peer computes a Goertzel **per tone per
arriving packet** (`EAR_TONES`) and writes it with the packet's arrival wall
clock; the drill and the injector stamp phase boundaries on the same clock
(both actors are containers on the lab network, so it is one kernel's clock);
`lab/conference_report.py` joins the two and judges each tone as present or
absent **relative to the loudest tone in that same ear during that same
phase**. Nobody listened to anything.

### The run (stamp 1787515279)

Phases, each 7–8 s with 400 ms trimmed off both edges: `pair` (A+B only),
`three` (C joins ~10 s late), `whisper`, `barge`, `mute`, `unmute`. Twenty
expectations, all green. A "present" tone reads **~3000** and an "absent" one
**36–98** — a **≥30:1** margin, so nothing here is near a threshold:

| phase / ear | present | absent |
| --- | --- | --- |
| pair / A | 880 = 2993 | 440 = 69, 1320 = 64, 1760 = 54 |
| three / A | 880 = 2996, 1320 = 3003 | **440 = 98** (its own tone) |
| three / B | 440 = 3000, 1320 = 2998 | 880 = 91 |
| three / C | 440 = 3011, 880 = 2999 | 1320 = 74 |
| three / monitor (`only=mixed`) | 440 = 2999, 880 = 2992, 1320 = 2999 | 1760 = 69 |
| whisper / B | 440 = 3011, 1320 = 3014, **1760 = 2989** | — |
| whisper / A | 880 = 2996, 1320 = 3003 | **1760 = 63** |
| whisper / C | 440 = 3011, 880 = 2999 | **1760 = 56** |
| whisper / monitor | all four, 1760 = 2988 | — |
| barge / A, B, C | 1760 = 2992 / 2989 / 2982, plus the other two members | — |
| mute / B | 1320 = 3000 | **440 = 53** |
| mute / C | 880 = 2993 | **440 = 69** |
| mute / monitor | 880 = 2996, 1320 = 3002 | **440 = 98** |
| unmute / B, C, monitor | 440 is back (3000 / 3011 / 2999) | — |

So, machine-verified on real sockets: **minus-self** (nobody hears their own
tone), a **monitor** attached with `only=mixed` hears all three, a **whisper**
named at one member lands in that member's ear and in **neither** of the other
two, the **barge** flip puts it in every ear including the injecting leg's,
**mute** takes a member off every ear *and* off the mixed track, and unmute
restores it. Identical readings recur across phases (`three/A` == `mute/monitor`
to the digit) because the sources are deterministic sines — that is the harness
agreeing with itself, not a copy-paste.

### Both recording shapes, at once, from the same conference

One `FILE_S3` attachment with `only=mixed` (the room) and a recording **group**
over the three member sessions with `only=customer` (per participant) ran
together:

| object | length | contents |
| --- | --- | --- |
| `acct-conf/room-<stamp>.wav` | 71.88 s, 1 ch | 440 = 2664, 880 = 2993, 1320 = 2583 — all three, the two lower because A was muted for 8 s and C joined late |
| `party-<stamp>/a.wav` | 71.96 s | 440 = 3000; 880 = 0, 1320 = 8, 1760 = 0 |
| `party-<stamp>/b.wav` | 72.02 s | 880 = 2992; everything else 0 |
| `party-<stamp>/c.wav` | 72.10 s | 1320 = 2556; everything else ≤ 3 — and **10.66 s of leading silence**, the P2-1 group anchor pad for its late join |

Cross-talk in a participant file is **0–8** against 3000: an inline leg's own
track really is its own. The three group files agree to **140 ms** (the
sequential detaches, D11).

### The bug this drill found in the recorder — and the tone amplitude that nearly hid it

**The first run's `party-c.wav` was 55.88 s against a/b's 66.16/66.24 s** — the
10.43 s pad was at the front, correctly, and 10.28 s of C's audio was **missing
off the tail**. `Segmenter::close_segment` subtracted the closed frames from
*both* `segment_start` (the lead-silence offset) *and* `anchor_ms`, so the first
spill (30 s by default) advanced the timeline by the pad twice and everything
after it landed one pad-length early. Ungrouped recordings never showed it
because their `segment_start` is 0; the group drill never showed it because its
5 s stagger and 20 s run finished before the first spill. Fixed here
(`past_lead = frames - segment_start` is what the anchor advances by) with a
test that fails by exactly the lead, and re-measured live: 71.96 / 72.02 /
72.10 s.

Also worth keeping: **`TONE_AMPLITUDE` must leave headroom.** At the peers'
default 24000 a three-way mix plus a whisper clips, and clipping
intermodulates onto exactly the harmonics being measured (440/880/1320/1760).
The drill runs all four sources at 6000 and `mss_conference_clipped_samples_total`
read **0**. A drill whose tones are harmonics of each other and whose mixer has
no AGC has to be arithmetically incapable of clipping, or its absent-tone
assertions are measuring their own distortion.

### What this does not prove

- **No SIP, no rtpengine, no human.** The peers are raw UDP sockets; a real
  B2B leg into a conference is deployment-gated.
- **One pod.** Conferences, recording groups and member state are all pod-local
  (D16, D22); nothing here survives a pod kill.
- The monitor is one consumer on the **room** session, which is what item 55
  opened it for. Until 2026-08-27 this drill attached it to party A instead --
  a leftover from before the room was a session -- so the server ended the
  monitor's stream when A left and the drill could not satisfy its own
  `leave/monitor` expectation. Nothing about the room recording inherits D20
  any more; the room object outlives every member.
- `deaf` and `hold` are **not** in this drill — they have socket-level tests
  from item 40 only.
- Only PCMU at 8 kHz/20 ms. No per-leg resampler exists, and a conference
  refuses a rate/ptime mismatch by name.

## preflight.sh — the environment check, tried on the lab (2026-08-26)

`lab/preflight.sh` (item 45, G5) is not a drill: it is the tool an operator runs
on a jump host *before* deploying mediaserverd, and the lab is simply the first
environment it was pointed at. The check table, every flag and both run
transcripts live in [deploy.md](deploy.md#preflight--check-the-environment-before-deploying-into-it);
what belongs here is how to run it against this lab and what the lab could not
exercise.

The NG port is not published to the WSL host, so run it inside the lab network:

```sh
DOCKER_API_VERSION=1.43 docker run --rm --network mss-microsip_lab \
  -v "$PWD/lab":/lab:ro -w /lab python:3-slim sh /lab/preflight.sh \
  --ng 172.31.99.10:22222 --redis redis://172.31.99.61:6379 \
  --kafka 172.31.99.60:9092 --s3-endpoint http://172.31.99.62:9000 \
  --bucket lab-recordings --access-key minioadmin --secret-key minioadmin \
  --media-ports 40100-40139 --rtpengine-version 14.1.1.8
```

**10 PASS, 0 FAIL, 3 SKIP, exit 0.** The interesting line is `ng_tap_media`: the
tool fabricates its own two-legged call through rtpengine, subscribes to the
caller's from-tag, pumps ~1 s of PCMU into both legs and counted **49 datagrams**
arriving on its subscription socket — the same mechanism `tap_live_call.sh` uses,
compressed into a check that needs no lab and no mediaserverd. Then it
unsubscribes, deletes, and `query` answers *Unknown call-id*.

Pointing it at the wrong NG port and a bucket that does not exist turned it red
(`3 PASS, 3 FAIL, 6 SKIP`, exit 1) with `NoSuchBucket` quoted from MinIO's own
error body.

### What the lab container cannot exercise, and what was done about it

- **No kafka client library** in `python:3-slim`, so `kafka_topic` SKIPs by
  design. A third run with `pip install kafka-python-ng` produced a probe record
  to `mss.preflight.probe` and read it back at partition 0 offset 0 — then the
  topic was deleted again, because a preflight should not litter a broker.
- **No ssh, and no second host to ssh to.** `media_udp` was proved through a
  shim on `PATH` named `ssh` that runs the command locally: the listener, the
  remote sender snippet and the argument passing are real, the ssh hop is not.
- **No chrony, no timedatectl** in a container — `clock` SKIPs and says so. That
  check is only meaningful on the host mediaserverd will run on.

## detach_latency_drill.sh — a detach that does not wait for its upload (2026-08-26)

Item 50 (defect D11) made `Detach`/`StopRecording` answer as soon as the
recording's segment is closed, with the upload running on behind it. Proving that
needs a slow object store, so the drill *makes* one: it runs beside the live lab
against the ordinary `mss-control` pod and, for its second phase, `docker pause`s
MinIO — a paused container's network stack is frozen, so the upload hangs inside
its 60 s window instead of failing fast.

```sh
DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
  -f lab/docker-compose.webrtc.yml up -d rtpengine redis redpanda minio \
  minio-init mss-control
./lab/detach_latency_drill.sh
```

Two fabricated calls (`lab/call_driver.py`, no SIP), one tap each, one `FILE_S3`
recording each. The run of stamp 1787745041:

| | phase A (MinIO healthy) | phase B (MinIO paused) |
| --- | --- | --- |
| `Detach` RPC | **49 ms** | **11 ms** |
| `DestroySession` RPC | — | **10 ms** |
| upload took | 29 ms | **12.07 s** (11:51:25.339 → 11:51:37.405) |
| during the upload | — | `mss_recording_uploads_in_flight 1` with `mss_sessions_live 0` |
| terminal event | `UploadCompleted` seq **13** | `UploadCompleted` seq **10**, after `SessionEnded` seq 9 |
| the whole sequence | gapless `0..14` | gapless `0..10` |
| object in the bucket | 469 KiB | 252 KiB |

Both RPC times are measured around the `mss_ctl` **process**, so they include its
start and its gRPC connect; the server-side figure is smaller. Before this item
the phase-B detach would have returned only when the upload did — 12 s later, by
construction.

Phase B is the interesting half, because the session is **destroyed while the
upload is stalled**. The pod's own words, in order: `recording stopped; its
upload runs in the background` → `this session will be remembered until its
recording upload settles, so the upload's own event keeps its place in the
session's sequence` → (12 s later, after `docker unpause`) `recording uploaded`
→ `a backgrounded recording upload settled` → `the last upload of this ended
session settled; forgotten`. That is the finishing-session design working on real
sockets: the session is gone from `mss_sessions_live` and from every listing the
instant `DestroySession` returns, yet its `seq` continues for exactly one more
event.

### What this does not prove

- **No live `UploadFailed`.** The drill makes storage slow, then lets it
  recover. The failure event is proved in-process only (a refusing sink, and the
  registry sequence test); a live run against permanently broken storage would
  have to wait out the 60 s `UPLOAD_TIMEOUT`.
- **Nothing about a pod dying mid-upload.** The retention is per-pod state, so a
  `kill -9` between the detach and the upload still loses the event; the audio is
  recovered by D9's salvage pass on that pod's next start, silently.
- **The concurrency bound is not exercised here** — one recording at a time. That
  is `background_uploads_run_no_wider_than_their_configured_concurrency`, in
  process.

## node_discovery_drill.sh — the proxy tells MSS which rtpengine to ask (2026-08-26)

Item 51 (G11) lets a `CreateSession` that names no rtpengine node read the node
out of Redis, where the proxy published it. The lab proxy publishes it for real
— through `exec.so` + `lab/opensips/discovery_publish.py`, because
`opensips/opensips:3.4` ships **no `cachedb_redis.so`** and apt.opensips.org no
longer carries a 3.4 component for bullseye. The bytes written are identical to
the `cache_store` snippet in [deploy.md](deploy.md).

The trick that makes the drill a proof rather than a coincidence: the pod's
**default** node is pointed at a black hole, so a tap can only work if the node
came from the map.

```sh
export DOCKER_API_VERSION=1.43 DISCOVERY=on
export MSS_DISCOVERY_REDIS_KEY_PREFIX=mss:call-node:
export MSS_RTPENGINE_NODE=172.31.99.199:22222
docker compose -f lab/docker-compose.microsip.yml \
  -f lab/docker-compose.webrtc.yml up -d --force-recreate \
  rtpengine opensips freeswitch redis redpanda minio minio-init \
  call-watcher mss-control
./lab/node_discovery_drill.sh
```

A real SIP call through OpenSIPS (`lab/host_test_caller.py`), then:

| | |
| --- | --- |
| `redis-cli GET mss:call-node:<call-id>` | `{"node":"172.31.99.10:22222","caller_tag":"hosttest","from_tags":["hosttest","y3HmFyeQae04N"]}` |
| `mss_ctl create <id> <call-id> -` (no node, no from-tags) | **1192 ingest datagrams in 12 s**, `attribution=explicit` |
| counters | `mss_discovery_hits_total 1`, misses 0, errors 0 |
| a call-id nobody published | one **miss**, then `Unavailable: no reply from rtpengine at 172.31.99.199:22222 after 3 attempts` |
| after the BYE | `GET` → `(nil)` |

Both tags came from the map, so that tap issued **no `query`** — visible as the
absence of a query line in the pod log, and the reason `from_tags` is in the
value format at all.

The toggle is **off by default**: `DISCOVERY` unset renders `$var(discovery) =
"off"` into `/tmp/opensips.cfg` and `MSS_DISCOVERY_REDIS_KEY_PREFIX` unset makes
the pod expose no `mss_discovery_` series at all. Both were checked after the
run, so every other drill sees the lab it has always seen.

### What this does not prove

- **Nothing ran against a real `cachedb_redis`.** The lab image cannot load the
  module, so the OpenSIPS snippet in deploy.md is documentation; what was tested
  is the key, the value and MSS's half.
- **No multi-node placement.** The map named the one rtpengine the lab anchors
  calls on. A deployment with several instances is the case the map exists for,
  and it is still deployment-gated.

## The whole suite on a second box, and the twelve things that stopped it (2026-08-27)

Every measurement above was taken on the box that built this project. This is
the first run of the suite somewhere else: a 2-vCPU / 3.6 GB Debian 11 host with
docker, no Rust, no rtpengine and no FreeSWITCH of its own. Nothing in the media
path needed changing. **Twelve environment and drill defects did**, and each one
is worth knowing because each one presented as a product failure.

### What the box had to be given

`cmake`, `make`, `g++` and docker were already there. Added: rustup pinned to
`rust-toolchain.toml`'s 1.95.0 (the drills build `mss_ctl` and
`mss_stream_probe` **on the host**, not in a container), `python3-pip` plus
`grpcio grpcio-tools protobuf kafka-python-ng numpy`, and **gawk** — see below.
Redpanda needs `--memory=512M --reserve-memory=0M` on a host this small;
unbounded it reserves most of the machine and leaves nothing for FreeSWITCH,
rtpengine, MinIO and a Rust daemon.

### The twelve, and what each cost

| # | What it looked like | What it was |
| --- | --- | --- |
| 1 | `docker compose up` refused to parse the lab at all | `${ELEVENLABS_API_KEY:?}` / `${DEEPGRAM_API_KEY:?}`. Compose interpolates **every** service before it filters by profile, so two paid keys were required to bring up rtpengine. Both labs now default to the checked-in `mock_bridge.py`; the real bridge is `--profile real` |
| 2 | `rig-freeswitch:latest` does not exist | It never existed in this repo. `lab/freeswitch/rig/Dockerfile` builds one from any FreeSWITCH image (`FS_BASE_IMAGE`), reconciling the four ways a source build differs from the Debian package |
| 3 | mediaserverd would not build in the base lab | `lab/docker-compose.yml` pinned `rust:1.95-slim-bookworm`, which has no cmake, and `media-core -> opus-ffi -> opusic-sys` builds a vendored libopus. The microsip lab had the right image all along |
| 4 | every SIP drill died before dialling | `host_test_caller.py` requires `out/bridge_tts.wav`, which `*.wav` in `.gitignore` keeps out of the repo. It now falls back to `webrtc/make_agent_audio.py` at 8 kHz and says so |
| 5 | "no media socket landed in the range — FAILED" | `media_port_drill.sh` parsed `/proc/net/udp` with `strtonum()`, a **gawk** extension. Under Debian's mawk every port read as empty and the drill blamed the product. The hex is converted by hand now |
| 6 | "FS is not in the path" on a call FreeSWITCH had just answered | `fs_parity_drill.sh` calls bare `fs_cli`, which a source-built FreeSWITCH does not put on PATH. It resolves `$FS_CLI` first |
| 7 | a parity comparison that FAILED still exited 0 | `recording_parity.py`'s status died in a pipe into `tee`, finished off by `|| true`. The drill propagates it now |
| 8 | `leave/monitor: no frames arrived in the window` | item 55 made the room a session, but this drill's **monitor** was left attached to party A — then A is the member who leaves. Hosted on `$ROOM_SESSION` the whole drill passes |
| 9 | `preflight.sh` printed a Python traceback | An unbindable `--local-ip` reached `socket.bind()` inside `check_ng`. A tool whose contract is "every line is PASS, FAIL or SKIP" now reports FAIL and names the cause |
| 10 | `media_port_drill.sh` failed its idle-pod precondition | `barge_drill.sh` destroyed its session by **session id**, but `mss_ctl` wraps `argv[2]` in `SessionRef::ExternalId` -- the destroy found nothing, the error went to `/dev/null`, and the leaked session still held two ports when the next drill started |
| 11 | `PODS=2 group_recording_drill.sh`: "listen tcp4 127.0.0.1:19092: bind: address already in use" | Its second pod defaulted to host port **19092**, which is where this lab publishes redpanda's EXTERNAL kafka listener. The default could never bind while the lab it needs was up; it is 19094/19095 now |
| 12 | `RECORD=1 pod_kill_drill.sh` refused to start, saying the pods needed `MSS_RECORDING_SPILL_TO=s3` when they had it | The gate grepped the pod's log for "spill into the recording **bucket** itself"; the daemon says **store**, and has since item 58 made the store a flag. It matches the structured `"spill_in_store":true` field now, so prose can move without blocking a drill |

Defect 8 is the interesting one: the D20 row in [tasks.md](tasks.md) recorded
that item 55's rewritten drill **had never run**. This was its first execution,
and it found the one thing the rewrite missed.

### What the suite measured here

Every headline number in this file reproduced on unfamiliar hardware.

| Drill | Here | Previously recorded |
| --- | --- | --- |
| `inline_call_drill.sh` | 20/20, cut-through **p50 11.5 ms**, p95 18.0 | p50 12.2 ms |
| `barge_drill.sh` | **p50 3.32–3.42 ms** cut-through, bus 0.84 ms | p50 3.5 ms |
| `conference_drill.sh` | **all** assertions, ≥30:1 tone margins, 60 ms length spread against a 600 ms bar | 61 ms, one assertion unrunnable |
| `drain_drill.sh` | drained **0.82 s**, exit 0, pod B adopted in **5.5 s** | 14.41 s on SIGKILL |
| `pod_kill_drill.sh` | one adopter, no lease lost, 2 taps after adoption, 0 after destroy | as recorded |
| `grpc_stream_drill.sh` | 9678 frames, 0 dropped; 30.02 s of L16/16 kHz | as recorded |
| `media_port_drill.sh` | sockets on 40100/40102, even only, returned on close | as recorded |
| `event_outage_drill.sh` | 60/60 events across a 30 s outage | as recorded |
| `dtmf_event_drill.sh` | 14 events, 0 dropped, correct digit per track | as recorded |
| `preflight.sh` | **9 PASS, 0 FAIL, 5 SKIP** against the live lab | as recorded |
| `node_discovery_drill.sh` | PASS on 2026-08-28: a session naming **no** node and no from-tags landed on the anchoring rtpengine (hits 0 -> 1) and carried 1196 datagrams with `attribution=explicit` from the proxy's `caller_tag`; an unmapped call-id was refused against the black-hole default rather than guessed (misses 0 -> 1) | as recorded |
| `fsless_call_drill.sh` | ear_a +1000=1183 / −440=0, ear_b +440=1348 / −1000=3, FreeSWITCH `exited` throughout | as recorded on its branch |

`event_outage_drill.sh` must be run as `./lab/event_outage_drill.sh`, not
`sh lab/...`: its shebang is bash and it uses `set -o pipefail`, which dash
rejects.

### The runs the defect list had owed

Defects 11 and 12 were each blocking one run that [tasks.md](tasks.md) records
as never having happened. With them fixed, both ran.

**D9, the live pod kill with a recorder attached** (`RECORD=1
lab/pod_kill_drill.sh`, pods started with `MSS_RECORDING_SPILL_TO=s3`). Pod A
spilled one closed segment, took a SIGKILL with no lease release, and pod C
adopted the session. The object came back at **170.80 s of the 185.57 s
tapped -- missing 14.77 s against an allowance of 19.25 s** (one 5 s spill
interval + the 12.25 s adoption gap + 2 s of slack), with
`frames_lost_on_adopt=0`, four spill segments, one upload, no failures, no lost
ownership and no foreign manifests. The reserved `_spill/` namespace was empty
afterwards, because the adopter finished the object and discarded the journal.
So the claim item 53 made against fakes -- that a pod death costs the spill
interval rather than the call -- now has a live number, and it is the adoption
gap that dominates it, not the spill.

**D16, a recording group across two pods** (`PODS=2
lab/group_recording_drill.sh`). Two pods on one Redis, the second member joining
5 s late on the *other* pod: pod B joined the group **pod A had opened** and
padded bob's file back to pod A's anchor (`lead_silence_ms` 5176 against the
first member's 183), so both participant objects came back the same length -- a
staggered join is padded, not shifted, across a pod boundary as well as within
one. The duplicate-label refusal also crossed pods and named the pod holding the
seat ("already has a participant writing .../alice.wav, on pod pod-group"),
which is what item 54's `HSETNX` was for;
`mss_recording_group_joins_refused_total` moved 0 -> 2. The frozen two-leg
identity is correctly absent for a grouped recording.

Note that this drill builds in a target volume of its own and drops it on the
way out, so every `PODS=2` run pays a full cold build -- about ten minutes on a
2-vCPU box.

### FS parity: what a tone source can and cannot show

`fs_parity_drill.sh` ran three times here and its byte comparison failed every
time, while the things the Phase-2 claim actually rests on held:

| | MSS | FreeSWITCH |
| --- | --- | --- |
| container / layout | 2 ch 8000 Hz 16-bit | 2 ch 8000 Hz 16-bit |
| customer rms | 2326–2374 | 2319–2330 |
| duration | 25.46–25.64 s | 25.10 s |

With `MSS_TAP_TRANSCODE=on` (A-law leg -> PCMU tap -> L16) the two agree at
**correlation 0.73**; with `MSS_TAP_TRANSCODE=off`, so MSS companded exactly
once as FreeSWITCH does, **0.83** and rms within **0.3%**. Neither reaches the
`identical=1.0000, mean_diff=0.6` this file records for the original run, and
the reason is the **source**, not the recorder: that run pumped real bridge TTS,
and this box has no speech to pump (defect 4), so it pumped alternating 440 +
1000 Hz tones. A pure tone at a fractional offset cannot align sample-exactly,
and the drill's own alignment search puts the offset at 182–278 ms.

So the tolerances in that drill (duration ≤ 200 ms, mean sample difference
≤ 200) are only meetable with a broadband source. **Nothing here re-opens the
Phase-2 finding** — it says the drill needs real speech to judge byte
agreement, and now that its exit code is honest (defect 7) that shows up as a
red run rather than a green one.

## The impairment matrix, on the tap link at last (2026-08-28)

[testing.md](testing.md) calls this "the single highest-value item" on the lab
list, and the section above records why it had never run: the box that built
this project has `CONFIG_NET_SCH_NETEM` compiled out, so `lab/soak.py` always
fell back to damaging the *caller's* RTP upstream of rtpengine. The Debian host
of the second-box run has netem as a module. `modprobe sch_netem`, and:

```
$ ./lab/netem.sh probe
netem: available on eth0 in mss-microsip-rtpengine-1
```

`soak.py` then reports `impairment injection: tc netem on the tap link` instead
of the endpoint fallback, and the whole matrix runs where it was meant to.

### Two things had to be fixed before a number here meant anything

**The filter was impairing the control channel.** `netem.sh` steered packets
into the netem band by **destination address alone**, and rtpengine's NG replies
are addressed to the pod exactly as the tap's RTP is. Under `loss5` the pods
logged `ng node did not reply, attempts 3`, a subscribe died, and a pod sat with
a live session and **zero** datagrams — the phase measured a broken subscription
rather than a jitter buffer, which is the opposite of the tool's purpose. NG
replies (source port 22222) are now pinned to the clean band at a higher filter
priority. `tc filter show` proves the shape: `pref 1 ... flowid 1:1 match
56ce0000/ffff0000 at 20` is the source port, `pref 2 ... flowid 1:3 match
ac1f631f at 16` is the destination address.

**Two orphaned sessions were skewing every pod.** The first run failed 116
assertions, including in the `clean` phase, and the soak named the cause itself:
*"the Redis registry still holds 2 session(s): conf-alice conf-bob"*.
`group_recording_drill.sh` removed its ad-hoc pods **without destroying their
sessions** — which is precisely the pod-loss case the registry exists for, so
the lab's own pods adopted two orphans that had no call behind them and held
stalled legs for twenty hours, one of them reporting `underruns: 1439999` over
an eight-hour leg. The drill destroys its sessions in cleanup now.

### The matrix

Two concurrent calls, 90 s per phase, summed over three pods, deltas taken
strictly inside each phase. `SOAK_EXIT=0`, 32/32 calls tapped, **zero**
assertion violations.

| phase | injected on the tap link | datagrams | lost | concealed | late | dup | loss |
| --- | --- | --- | --- | --- | --- | --- | --- |
| clean | — | 13,702 | 0 | 0 | 0 | 0 | 0.00% |
| loss1 | `loss 1%` | 14,034 | 119 | 119 | 0 | 0 | **0.84%** |
| loss5 | `loss 5%` | 13,429 | 599 | 599 | 0 | 0 | **4.27%** |
| burst | `loss 10% 50%` (see below) | 13,992 | 127 | 127 | 0 | 0 | 0.90% |
| reorder | `delay 30ms 20ms reorder 25% 50%` | 14,002 | 1 | 1 | **3** | 0 | 0.01% |
| reorder-far | `delay 120ms 60ms reorder 25% 50%` | 14,117 | 11 | 11 | **7** | 0 | 0.08% |
| duplicate | `duplicate 1%` | 14,151 | 21 | 21 | 0 | **131** | 0.15% |
| jitter | `delay 20ms 15ms distribution normal` | 14,083 | 0 | 0 | 1 | 15 | 0.00% |

**`frames_concealed` equals `jitter_lost` in every row**, as it did against the
endpoint injection — so item 17's G.711 Appendix I concealment has now run on
the tap link, at the packet.

### Three rows that only this injection point could produce

- **Duplicates reach the tap: 131 of them.** The endpoint run recorded
  `mss_jitter_duplicates_total` stuck at **0** through a phase that duplicated
  1% of the caller's packets, and concluded that rtpengine absorbs a duplicate
  upstream of the subscription — so "only netem on the tap link can reach it".
  It can, and the dedupe path counts them, with `jitter_resets` at 0.
- **Late drops exist: 3, then 7 as the delay widens to 120 ms ± 60 ms.**
  Reorder-beyond-depth was replay-only until here.
- **Loss accounting is exact, not approximate.** Under the old `burst` spec the
  qdisc's own counter read `Sent 10548 pkt (dropped 12)` and MSS reported
  `lost=12` — the same twelve packets, not a rate that merely tracks.

### The `burst` profile was measuring nothing, and that is now fixed

That exactness is what exposed it. `loss 10% 50%` is netem's legacy correlation
form, and the correlation collapses the effective rate: **0.11% actually
dropped against a nominal 10%**. The profile has been the matrix's burst row
since it was written, and it was never bursty and never 10%.

It is `loss gemodel 3.3% 30%` now — Gilbert-Elliott, where `p` is good→bad and
`r` is bad→good, so steady-state loss is `p/(p+r)` = 10% and a burst runs `1/r`
≈ 3.3 packets. Measured by the qdisc that applies it:

```
qdisc netem 30: parent 1:3 limit 1000 loss gemodel p 3.3% r 30% 1-h 100% 1-k 0%
 Sent 2834670 bytes 10954 pkt (dropped 1119, overlimits 0 requeues 0)
```

10.2% dropped, and MSS reported **1,073 lost, 1,073 concealed** in-phase with
the soak still green. So the concealment has now been measured against ~10%
**bursty** loss on a real link, and the matrix's burst row means what it says.

### What this still does not give

The concealment has never been **judged perceptually** — every claim here is a
counter agreeing with another counter. `SOAK_CALLS=2` and 90 s phases are a
smoke-sized matrix on a 2-vCPU box, not the hour-long soak item 19 describes.
And the impairment lands on the tap link only: nothing here damages the call
legs themselves, which is the customer-visible path.

## fs_control_drill.sh — FreeSWITCH answers the call and carries none of it (2026-08-29)

`fsless_call_drill.sh` proves MSS can carry a call with FreeSWITCH **stopped**.
That is the end state, and it is not the state anybody migrates into. This drill
proves the shape a deployment moves through first, and the one an operations
team will actually accept: FreeSWITCH is running and still owns the call — it
answers, it decides which conference the caller belongs to, it decides when the
call ends — while every byte of audio belongs to MSS.

The mechanism is one line of dialplan ahead of the bridge:

```xml
<extension name="control_only_orchestrated">
  <condition field="destination_number" expression="^(7200)$">
    <action application="set" data="bypass_media=true"/>
    <action application="set" data="mss_conference=$1"/>
    <action application="set" data="hangup_after_bridge=true"/>
    <action application="bridge" data="sofia/internal/$1@172.31.99.14:5080"/>
  </condition>
</extension>
```

`bypass_media` is set while the channel is still **unanswered**, so FreeSWITCH
negotiates the caller's SDP against the far leg's and then leaves the RTP path
entirely. The far leg is `lab/sip_shim.py`, which hands the offer to
`CreateSession{kind=INLINE, group=7200}` and answers with MSS's SDP — so the
answer FreeSWITCH relays as its own 200 OK is MSS's answer, and two callers who
dial 7200 are two inline legs in one MSS mix.

### The run (stamp 1788007501, FreeSWITCH 1.10.12, rtpengine 14.1.1.8)

Two scripted softphones (`lab/host_test_caller.py`, PCMA, real sockets through
OpenSIPS and rtpengine), 30 s, A speaking 440 Hz and B 1000 Hz.

FreeSWITCH held four channels — an inbound leg and a leg to the shim, per
caller — and **not one of them counted a single RTP byte**:

```
e3538cdc-202c-499e-b7b0-65d8c29a4e55  bypass_media=true    application=bridge  rtp_in=_undef_  rtp_out=_undef_
3757611b-c255-448a-8382-eb99112ea55e  bypass_media=_undef_ application=_undef_ rtp_in=_undef_  rtp_out=_undef_
79aa416e-f8bf-4831-9f84-bb33ebd85552  bypass_media=true    application=bridge  rtp_in=_undef_  rtp_out=_undef_
0ef67641-6630-4c3b-9b36-3ccf737678a3  bypass_media=_undef_ application=_undef_ rtp_in=_undef_  rtp_out=_undef_
```

`_undef_` is FreeSWITCH saying the variable does not exist: not zero bytes
counted, but no counter ever created, because no RTP session was ever set up.
Meanwhile the pod reported `mss_conferences_live 1`,
`mss_conference_members_live 2`, `mss_inline_legs_live 2` and 665 mixed frames,
and each caller's ear carried the other's tone and not its own:

| ear | the other leg's tone | its own tone |
| --- | --- | --- |
| A (heard 1000 Hz) | 1176 | 0 |
| B (heard 440 Hz) | 1341 | 3 |

That is the whole claim in one table. FreeSWITCH ran throughout — the container
was `running` before and after — and the audio existed without it.

### Conference lifecycle, and why the media plane must not own it

MSS closes a mix when its last member leaves and deliberately does **not** hang
up a survivor: what should happen to the other party is a call-control
decision. Without a rule somewhere, a caller sits in a silent room — two of
them did here, for 292 s and 494 s, before there was one.

So the rule lives in call control, in a companion service
([fs-orchestrator](https://github.com/lazyboson/fs-orchestrator)) that holds
**one inbound event socket for the whole switch** and keeps a map of conference
to parties. Given `ORCHESTRATOR_LOG`, this drill also drives the three outcomes
that rule can have and asserts the controller's own account of each:

| outcome | what the drill did | what was observed |
| --- | --- | --- |
| the last party alone is hung up | A left at 12:46:08 | `alone_for=20s` at 12:46:28, FreeSWITCH back to 0 channels |
| a rejoin cancels it | a third party arrived 2 s into the window | `somebody joined during the grace period, so the last party stays  parties=2`, four channels still live |
| a lonely party leaving is not chased | the survivor hung up 5 s into its own grace window | `the last party hung up during the grace period, so there is nobody to hang up` — no kill attempted |

The third row is the one worth keeping. It was originally the same log line as
the second, because the timer asked "is this party still alone" and got a
boolean: a room with **nobody** in it fails "exactly one member" the same way a
room with two does. An ordinary two-party call ending was therefore reported as
"somebody joined … the last party stays", and the kill fired at a uuid
FreeSWITCH had already torn down. Three outcomes need a three-valued answer.

### Two things cost a run each

**`localnet.auto` locks out `fs_cli`.** `mod_event_socket` with no
`apply-inbound-acl` answers a connection from the docker bridge host with
`Content-Type: text/rude-rejection / Access Denied, go away.`, so a controller
outside the container cannot attach at all — while `fs_cli` inside the container
keeps working and hides it. The obvious fix, `apply-inbound-acl localnet.auto`,
is a trap: that list is built from FreeSWITCH's own interfaces, so it covers the
docker subnet but **not `127.0.0.0/8`**, which is how `fs_cli` connects. It
admits the controller and locks out every drill. It also fails
unrecognisably — because the module accepts the TCP connection and only then
refuses, the handshake dies with no `auth/request` and `fs_cli` prints

```
[ERROR] fs_cli.c:1699 main() Error Connecting []
```

which reads like nothing is listening, on a port that is open. The rig image now
declares its own list (`rig_esl`: loopback plus the RFC1918 ranges, deny by
default) instead of borrowing an automatic one.

**A grace timer from the media phase lands in the middle of round 1.** The
media phase ends with one caller outliving the other, which arms a timer; it
fires 20 s later, correctly, in the middle of the lifecycle rounds, where its
line reads as round 1's. The drill now waits out one grace period between the
phases before it marks the log.

### What this does not prove

The inline leg has now met a **real SIP endpoint** — FreeSWITCH, as a B2BUA,
offering to `lab/sip_shim.py` — which is the H5 shape, and the shim is a worked
example of the plumbing an integrator owes. It is not the whole of it:
registration and authentication are OpenSIPS's here and untested against the
shim, a re-INVITE is refused by name (`488`, P3-2), and no transfer, hold or
mid-call renegotiation has been attempted on one of these legs. Nothing here
was judged by a human ear, and the audio was tones rather than speech.

A production dialplan does IVR before this bridge, and a media-bypassed
FreeSWITCH **cannot play a prompt** — every prompt in this shape is MSS's to
play, which is a real migration cost and is not exercised here. And the rule
that a lone party is hung up after 20 s is this lab's policy, not a finding:
a deployment where an agent routinely parks a caller for longer needs a
different number, or a different rule.
