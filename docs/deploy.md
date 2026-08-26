# Deploying mediaserverd — operator guide

**Stub.** This file is being filled in by the integration-readiness work; the
complete guide (every env var, the port/firewall matrix, sizing, HA behavior,
rollout behavior and the preflight tool) lands with item G6. Until then it holds
the rows that earlier items added, and [lab.md](lab.md) remains the worked
example of a running deployment.

## Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `MSS_DRAIN_TIMEOUT_SECS` | `30` | Ceiling on the whole shutdown drain (see [tasks.md item 42](tasks.md)): stop accepting, hand registry leases to an adopter, close every session politely (consumer stop frames, recordings finished, taps unsubscribed), flush the event backlog. The process exits **0** whether or not the window is used up; a second SIGTERM/SIGINT exits at once. `0` means "stop accepting and exit". Set the pod's `terminationGracePeriodSeconds` **at or above** this value, or the container runtime will SIGKILL the drain half-done — with `docker stop`, whose default grace period is 10 s, pass `-t` above this value. |

## Rollout behavior

mediaserverd drains on **SIGTERM** as well as SIGINT, so an ordinary Kubernetes
rollout or eviction is graceful: readiness goes false first (the `mss_draining`
gauge flips to 1), the registry lease is released so another pod adopts the call
on its next sweep rather than after the 15 s TTL, and consumers are closed with
their protocol's own stop frame instead of a dropped socket.

Measured in the lab (2026-08-26, `lab/drain_drill.sh`): a tapped live call, pod
stopped with `docker stop -t 60`, **exit 0 in 0.44 s**, drain 30 ms, the second
pod adopting **2.7 s** after the signal and the consumer's audio gap **1.96 s** —
against **14.41 s** for the same call killed with SIGKILL.
