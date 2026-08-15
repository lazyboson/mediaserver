# Rust Guidelines

These rules are binding for every `.rs` file in this repository. CI
enforces the mechanical ones; review enforces the rest. The constitution
([CONSTITUTION.md](../CONSTITUTION.md)) outranks this document.

## 1. No comments — strictly

No comment of any form is permitted in Rust source files: no `//`, no
`/* */`, no `///`, no `//!`. CI fails the build if `//` appears anywhere
in `crates/**/*.rs`.

Where the information goes instead:

| You want to write… | Put it in… |
| --- | --- |
| What a module/type/function does | A better name, a narrower type, a smaller function |
| Why a decision was made | `docs/` (architecture doc, decision records), the PR description, the commit message |
| A warning about tricky behavior | A test that fails when the behavior changes, named after the behavior |
| A TODO | An issue in the tracker, referenced from the PR |
| Public API documentation | `docs/` and the proto files' message definitions |
| An example | An integration test or an `examples/` binary |

Corollaries: string literals must not be used to smuggle commentary;
`#[allow(...)]` attributes must be justified in the PR description since
they cannot be justified inline; if a URL is ever needed in a string
literal, the CI check is amended deliberately in the same PR, not worked
around.

## 2. Naming carries the intent

Names state units, domains, and invariants: `ptime_ms`, `sample_rate_hz`,
`stall_after`, `seq_delta`, `PopOutcome::Waiting`. A function name is a
sentence about what it does (`reports_once_despite_retransmitted_end` is a
test name from this repo — that is the standard). Abbreviations are
allowed only for domain vocabulary (RTP, SDP, SSRC, DTMF, NG).

## 3. Errors

- `thiserror` enums per crate; error text states what failed and the
  offending value (`"packet too short: {0} bytes"`).
- `unwrap`/`expect` are permitted only in `#[cfg(test)]` code and in
  `main()`-adjacent initialization that must abort on misconfiguration.
- Anything parsing network or file input returns `Result`/`Option`
  (Constitution, Article IV).
- No `anyhow` in library crates; concrete error types only. `anyhow` is
  acceptable in the binary crate's initialization path.

## 4. The hot path

- Zero heap allocation per packet after session setup (Article V).
  Preallocated slots, caller-provided buffers, `&[u8]` views.
- No locks, no `await`, no syscalls other than the sockets the worker
  owns, no channel with unbounded capacity.
- Time is a parameter (`Instant`, tick counts). `Instant::now()` is called
  by the world that owns the clock (worker loop), never inside core logic.
- Pacing is deadline-anchored (`t0 + n·ptime`), never sleep-accumulated.

## 5. Crate discipline

- `media-core` and `rtpengine-ng` are sans-IO: adding a socket, clock, or
  `async fn` to them is an architecture violation, not a style issue.
- `#![forbid(unsafe_code)]` in every logic crate; FFI, when it arrives
  (libopus), lives in a dedicated `-sys`-wrapping crate that is the only
  exception. Constitution Article XI fixes the preference order for
  anything codec- or DSP-shaped: out-of-process engine (rtpengine
  transcodes at the tap) over vetted C-library bindings over in-tree code,
  and in-tree only for trivial algorithms or the session machinery no
  library provides. Reimplementing a hardened library is a rejected PR,
  not a style choice.
- Dependencies require justification in the PR that adds them and must
  pass `cargo deny` (licenses: permissive only; no unmaintained advisories).
- Public API surface is `pub` only where another crate consumes it.

## 6. Tests

- Every parser has malformed-input tests asserting errors, not panics.
- Every state machine has tests for its edge transitions (wraparound,
  reset, duplicate, loss — see `jitter.rs`).
- Wire-compatibility types have byte-exact serialization tests; those
  tests are the frozen contract (Article VII).
- Test names describe the behavior, since comments cannot.
- New media-path code lands with a pcap-replay test once the replay
  harness exists (M2).

## 7. Formatting and lints

- `cargo fmt` — default configuration, no overrides, enforced in CI.
- `cargo clippy --all-targets -- -D warnings` — zero warnings, no blanket
  `#[allow(clippy::...)]`; narrow, PR-justified allows only.
- Rust edition and toolchain are pinned (`rust-toolchain.toml`); bumps are
  their own PR.

## 8. Commits and PRs

- Commit messages explain *why*; they are the only place near the code
  where prose is welcome, so use them well.
- A PR does one thing. Scaffolding, behavior change, and dependency bumps
  do not share a PR.
- Anything touching `crates/media-core/src/jitter.rs`, the pacing loop, or
  the fan-out queues requires a benchmark run in the PR description.
