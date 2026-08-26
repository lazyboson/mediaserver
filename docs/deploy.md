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
| `MSS_MEDIA_PORT_MIN` / `MSS_MEDIA_PORT_MAX` | unset (ephemeral ports) | The inclusive UDP port range every media socket binds inside — the tap sockets rtpengine sends the subscribed copy to, and inline legs' RTP. Set both, or neither. **Even ports only** are handed out, so a range of *N* ports serves *N/2* RTP sockets (one per tapped leg: a two-party tap takes two) and the `mss_media_ports_capacity` gauge reports the real number. Unset means today's behavior: an ephemeral port per socket, which no firewall can describe. Open this range inbound from every rtpengine host. The NG control socket is **not** in it — that is an outbound flow to rtpengine's `22222`. |
| `MSS_MEDIA_ADVERTISE_IP` | unset (= `MSS_TAP_LOCAL_IP`) | The address MSS puts in every SDP it hands a peer: the tap's subscribe answer and the inline leg's answer. Sockets still **bind** `MSS_TAP_LOCAL_IP`. Set this when the address a peer must reach is not the address the pod binds — a NAT, a routed VIP, a `hostNetwork` node behind a load balancer. If it is wrong, rtpengine's tap copy goes nowhere and the tap looks up with no audio. |
| `MSS_TAP_LOCAL_IP` | `0.0.0.0` | The local address media and NG sockets bind to. On `hostNetwork: true` set it to the node address that rtpengine can reach, not `0.0.0.0`, so the source address of MSS's own packets is predictable. |

## Media ports and firewalling

A deployment with a firewall between rtpengine and MSS needs three things to
agree: `MSS_MEDIA_PORT_MIN`/`MAX` (what MSS binds), the range the firewall admits
inbound from the rtpengine hosts, and — if they differ — `MSS_MEDIA_ADVERTISE_IP`
(what MSS tells rtpengine to send to). Size the range at **two ports per
concurrent tapped call** (one even port per leg, and the odd successor is left
free for RTCP), plus headroom.

Watch `mss_media_ports_in_use` against `mss_media_ports_capacity`;
`mss_media_ports_exhausted_total` rising means sessions are being refused for
want of a port, and `mss_media_ports_bind_conflicts_total` rising means something
else on the host is inside MSS's range.

Verified in the lab (2026-08-26, `lab/media_port_drill.sh`): a live tapped call
on a 40-port range bound `40100` and `40102`, carried 1503 datagrams in 15 s, and
returned both ports to the range when the session was destroyed.

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
