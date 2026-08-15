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

## Findings still open

1. **Telephone-event packets are counted as lost audio.** Both legs above
   report `jitter_lost: 21` and `frames_concealed: 21` — exactly the DTMF
   packet count. RFC 4733 packets consume RTP sequence numbers, and
   `StreamPipeline` deliberately routes them to the DTMF detector instead of
   the jitter buffer, so the buffer sees each one as a missing audio packet.
   Playing silence for the event is roughly right (endpoints suppress audio
   during a press), but **counting it as loss is not**: every DTMF press
   inflates the loss metric, so an IVR-heavy tenant would look like a lossy
   network and real loss would be hidden in the noise. Article VIII wants
   these counters truthful. The fix belongs in the jitter buffer — a
   sequence number can be *accounted for* without carrying audio — and is
   tracked with the jitter hardening work, not patched around here.
2. **The `mix` flag is untested.** `SubscribeRequest` supports it and the
   architecture proposes it for cheap supervisor listen, but no lab run has
   asked for a mixed mono feed.
3. **`stop media` cut-through latency is unmeasured.** It is the barge-in
   primitive for `play media` injection — how fast an utterance stops once
   the caller starts talking decides whether utterance-shaped bot speech
   feels interactive or not. The probe issues `stop media` but does not
   time it.
4. **Injected audio arrives alongside the peer's, not instead of it.** In
   the targeted run the caller received 245 packets, 100 of them the
   injected tone and the rest the callee's silence, so `play media` did not
   block egress. Whether an AI utterance and live caller audio should mix
   or the peer should be suppressed is a product question, and the
   `block egress` flag is the knob for it — untested.
