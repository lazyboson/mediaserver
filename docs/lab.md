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
`dtmfResult` — frozen the legacy stream fsm contracts (Constitution VII) — would have
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
