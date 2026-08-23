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
passed explicitly), and the group's member files are **not time-aligned**: each
anchors on its own first frame, so the three lengths above differ by up to
0.26 s and a late joiner's file would simply start at its join moment (**D18**).
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

## barge_drill.sh — the half of barge-in cut-through that MSS owns (2026-08-23)

Barge-in is four hops, and only three of them are MSS's:

1. a consumer (ASR, voice-AI) decides the caller started talking and **tells
   MSS**;
2. MSS publishes that as a `MediaEvent` on Kafka `mss.events`;
3. somebody's translator consumes the event and decides to cut the prompt;
4. that translator calls `StopPlayback` on `MediaControl`, and MSS stops the
   media.

`lab/barge_drill.sh` + `lab/barge_translator.py` measure hops 2–4 against the
live lab. **Hop 1 has no wire today** (see the D19 row in
[tasks.md](tasks.md)): `session-core`'s `Registry::report` — the
`ConsumerEvent` → `SpeechStarted`/`Partial`/`Final` path that feeds the pump —
is reachable only from unit tests. Neither the `WS_TWILIO` inbound dialect nor
the gRPC `ConsumerToServer` stream carries a speech report, and the Twilio
`clear` message takes a different road entirely: it becomes a direct
rtpengine `stop media` from `tap_session.rs`, never an event. So the drill's
**trigger event is `PlaybackStarted`, not `SpeechStarted`**. For the bus half
that costs nothing in fidelity — every `MediaEvent` goes through the same
`event_pump`, the same topic and the same per-`external_id` partition key
whatever payload it carries — but the consumer's own detection latency and
whatever hop 1 will eventually cost are out of frame.

The drill runs **beside** the live compose lab and adds one container: a
fabricated call (`lab/call_driver.py`, no SIP) at 172.31.99.123, tapped by the
lab's own `mss-control` pod, so events travel the real pump into the real
Redpanda (`mss.events`, published to the host at 127.0.0.1:19092). The mock
translator runs on the WSL host: a `kafka-python-ng` consumer parked at the
end of the topic plus a warm gRPC channel to 127.0.0.1:50551. It plays **both**
integrator roles on purpose — it starts the prompt *and* barges it — so every
interval in the headline is measured on one clock, with no container/host skew
in it. Skew was measured anyway (`PlaybackStopped` is stamped inside the
`StopPlayback` call, so its `at` must fall inside this process's send/ack
window): **median −0.12 ms**, i.e. negligible.

### Numbers (two runs of 25 iterations, 2026-08-23)

| interval | run A p50 / p95 / max | run B p50 / p95 / max |
| --- | --- | --- |
| **cut-through**: `StartPlayback` acked → `StopPlayback` acked | **3.31 / 4.19 / 4.27 ms** | **3.15 / 3.64 / 3.90 ms** |
| event on the bus after the `StartPlayback` ack | 1.50 / 1.84 / 1.84 ms | 1.41 / 1.61 / 1.68 ms |
| translator decides: event in hand → `StopPlayback` acked | 1.86 / 2.35 / 2.57 ms | 1.78 / 2.03 / 2.36 ms |
| publish → consume, measured on `PlaybackStopped` | 1.09 / 1.33 / 1.38 ms | 1.02 / 1.16 / 1.56 ms |
| MSS's own event `at` → consumed | 11.01 / 11.95 / 38.88 ms | 10.67 / 11.25 / 12.50 ms |

Per-iteration rows are kept in `lab/out/barge-drill-<stamp>.jsonl`.

**The Kafka hop is not the problem.** The whole MSS+bus half fits inside a
single 20 ms frame with an order of magnitude to spare — p95 4.2 ms — so
architecture §9 risk 9's fallback (a gRPC stream *for speech events only*) is not
needed on these numbers.

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
