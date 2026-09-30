# Session playbook — how to work on this repo without repeating our mistakes

Every rule here was paid for by a real failure in the sessions that built
Phases 0–1. Rules cite their incident so you can judge whether they apply.

## 1. Verify against reality, never against documentation or memory

The vendor's implementation is the spec. This repo's biggest wins came from
probes, not reading: `codec accept` vs `transcode` (docs implied either
works; only transcode converts), `publish` (documented as an injector;
measured as a broadcast source that reaches no participant), `blob64`
(documented, rejected at runtime), subscription answers (RFC 3264 allows
dropping codecs; rtpengine rejects it).

**Before building on any rtpengine/Deepgram/Azure/FreeSWITCH behavior,
write a throwaway probe.** The pattern is established:
`lab/ng_*_probe.py` — fresh state per variant, print ACCEPTED/REJECTED,
measure packets not return codes. An afternoon of probes beats a week of
debugging code built on a documented lie.

Corollary for frozen wire contracts (Constitution VII): the serialization
tests assert **measured** bytes, not documented ones. `media.timestamp` is
a string because the legacy media gateway sends a string, whatever Twilio's docs say.

## 2. Machine-verify before spending a human test

A human dialing a softphone is the most expensive verification step we
have, and "no luck" carries almost no diagnostic signal.
`lab/host_test_caller.py` is a synthetic MicroSIP (SIP through the
published ports, real speech, per-SSRC ear recording) and
`lab/ear_intelligibility_probe.py` uses the ASR itself as the audio-quality
judge. **Run the machine pass after every media-path change; only then ask
the human.**

Know what your test cannot see: the machine caller demuxes by SSRC, so it
"heard" perfectly through the dual-SSRC bug that gave a real phone mush.
When the machine passes and the human fails, the delta *is* the diagnosis
— enumerate what differs (jitter buffer behavior, NAT hops, audio devices)
instead of re-testing the parts that already passed.

## 3. When something breaks, read the artifacts before touching code

More than half of this project's "it's broken" reports were not code:
muted microphone (proved by near-zero A-law codes in the datagram log),
Windows→WSL2 localhost forwarding eating return UDP (proved by rtpengine's
out-packet counters vs a passing machine call), a BYE that never arrived
(Record-Route carried an unroutable address), a zombie call shadowing
fresh dials, a user playing the wrong recording, a recording that didn't
exist because the call had never ended.

The diagnostic order that worked every time: **tap stats → bridge logs →
rtpengine per-port packet counters → raw datagram log**. The `tap leg
finished` line localizes the failing hop by itself: `datagrams` says
whether media arrived, `unknown_payload_type` says codec mismatch,
`frames_suppressed` vs `jitter_lost` separates DTMF from loss,
`media_dropped` indicts the consumer queue. Every run also leaves
`out/tap-*.dglog` — decode it before speculating about what was on the
wire. Fix code only after the failing hop is identified with data.

## 4. Scripted edits lie; verify every edit landed

Two incidents: `python str.replace` no-oped silently because `cargo fmt`
had reformatted the target text, and the resulting still-red test was then
pushed because a `&&` chain swallowed the failure.

- Prefer the Edit tool (errors on no-match) over sed/python for source
  edits; when a script must edit, **grep for the new text afterwards**.
- Never chain `&&` through a test run. Run the gate as separate commands
  and read each result:
  `cargo test --workspace` · `cargo fmt --all --check` ·
  `cargo clippy --all-targets -- -D warnings` ·
  `grep -rn '//' crates --include='*.rs'` (must be empty) ·
  `cargo deny check all`.
- `//` inside a **string literal** fails the comment scan too — build test
  URLs with an escaped separator, as existing tests do.

## 5. Survey completely before making architecture claims

An early survey of the legacy controller used `ls | head` and missed its verb
API, the voice-AI orchestrator and five binaries — producing an integration design for
a system that no longer existed. When a conclusion will shape design:
enumerate fully (no `head` on the listing), check the branch is current
(`git fetch` + rev-list), and state *what you read* alongside what you
concluded. Look for existing seams before inventing one — `the legacy proto package`,
the legacy verb API proto and the Kafka event producer were all sitting there,
already shaped like the answer.

## 6. Make our side robust instead of steering the vendor

When rtpengine sent PCMA despite a PCMU-first answer, the fix was not to
reverse-engineer its per-stream transcode heuristics — it was companding
at ingest so **whatever arrives is handled**. Prefer robustness on our
side of a protocol boundary over divining a peer's undocumented choices;
it also survives the peer's next upgrade.

## 7. Timing discipline (the sleeps that are and aren't allowed)

Allowed: wall-clock-anchored deadline pacers (`t0 + n·ptime`, reanchor
counted), select-with-timeout, sleeping out the known duration of audio
just handed to a player **while still receiving commands**. Not allowed:
sleep-as-synchronization, being deaf while sleeping (the barge-in command
once waited 3 s behind a piece-pacing sleep), fixed-nap test assertions
(poll to a deadline instead), and blocking I/O on the tokio runtime
(`block_in_place` for artifact writes).

## 8. Streams must be continuous for downstream ASR

Deepgram closes after a quiet window (net0001). This bit us twice — caller
silence suppression, then the bot track's gaps between utterances. Any
track a speech engine might consume must carry silence frames when idle.
Underruns feed the consumer silence; `frames_suppressed` and `underruns`
keep the metrics honest while the stream stays gap-free.

## 9. Respect the phase boundaries and the docs contract

The spike's env-var bootstrap is scaffolding; the hub is product. Don't
grow M4 features inside spike scaffolding silently — and don't "improve"
frozen surfaces. Every module change updates
`docs/implementation-notes.md` in the same commit (sources carry no
comments, so that file is where context lives); milestones update
`docs/roadmap.md`; lab discoveries go to `docs/lab.md` with the probe that
proved them. Checkboxes claim only what was measured.

## 10. Cost discipline for live debugging

One change per verification cycle. Design human tests to discriminate,
not just exercise ("dial and listen, don't speak" separates output path
from mic; 9196 is FS's own echo and tests nothing of ours). Keep the lab
stack warm between attempts — and remember the environment quirks live in
the memory files: `DOCKER_API_VERSION=1.43` on every docker command, LF
line endings enforced by `.gitattributes`, the rustup volume, the WSL IP
that changes on reboot (`ADVERTISED_IP` in `lab/.env` must follow it).
