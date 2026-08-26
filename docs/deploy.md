# Deploying mediaserverd — operator guide

Everything needed to run mediaserverd against a real deployment: what it talks
to, every variable it reads, the firewall it needs, how it behaves during a
rollout and a pod loss, how to check an environment **before** deploying into
it, and an ordered runbook for the first day on real gear.

MSS is a generic product. Anything whose media anchors in **rtpengine** can run
it; nothing below assumes a particular proxy, softswitch or controller. Where a
concrete stack appears — OpenSIPS, FreeSWITCH, a legacy telephony controller —
it is a **worked example** of one integrator's shape, never a requirement. The
lab that shape runs in is [lab.md](lab.md).

## What mediaserverd needs, and what it never needs

| It needs | Why | Optional? |
| --- | --- | --- |
| An **rtpengine** with `subscribe request`/`subscribe answer` | this is the tap. There is no fallback and no workaround: without it MSS cannot get audio | no |
| A **UDP port range** rtpengine can reach | where rtpengine sends the tap copy, and where an inline leg lives | no |
| **Redis** | ownership leases, so another pod adopts a tap when one dies | yes — without it sessions live and die with their pod |
| **Kafka** | the `mss.events` stream everything downstream reacts to | yes — without it events stay in-process and nobody sees them |
| **S3-compatible storage** | recordings, at `${accountID}/${recordingID}.${format}` | yes — without it `file-s3` attachments are refused |
| A **writable directory** | recording spill, so a container restart does not lose buffered audio | yes, but recommended |
| A **synchronised clock** | recording timestamps, event order, recording-group alignment across pods | no |

It never needs: a SIP stack (no signalling reaches MSS — the proxy or softswitch
keeps that), a FreeSWITCH, a Kubernetes API token (it calls no API), root, or a
writable root filesystem.

## The manifests

[`deploy/k8s`](../deploy/k8s) is a Kustomize tree — no Helm, no templating
language, so what you read is what gets applied.

```
deploy/k8s/base/                Deployment, ConfigMap, Secret template, two
                                Services, PDB, ServiceAccount, ServiceMonitor,
                                PrometheusRule
deploy/k8s/overlays/hostport/     pod network + an enumerated hostPort range
deploy/k8s/overlays/hostnetwork/  the node's network stack + a wide range
deploy/k8s/validate.sh            checks the tree without a cluster
deploy/k8s/sync-alerts.sh         regenerates the PrometheusRule from
                                  deploy/prometheus-alerts.yaml
```

```sh
kubectl create namespace mediaserver
kubectl -n mediaserver create secret generic mediaserverd-secrets \
  --from-literal=MSS_AUTH_TOKEN="$(openssl rand -hex 32)" \
  --from-literal=MSS_RECORDING_S3_ACCESS_KEY_ID=... \
  --from-literal=MSS_RECORDING_S3_SECRET_ACCESS_KEY=...
kubectl kustomize deploy/k8s/overlays/hostnetwork   # read it first
kubectl apply -k deploy/k8s/overlays/hostnetwork
```

The base is **not** deployable on its own for media: it leaves the media range
empty, so every tap socket takes an ephemeral port and no firewall can describe
it. Pick an overlay.

`base/secret.yaml` is a **template** whose every value is the literal string
`REPLACE_ME`, carrying a banner that says so. It exists to document the key
names; create the real Secret out of band (above) or from your
sealed-secrets/external-secrets operator and drop the file from
`base/kustomization.yaml`. `MSS_AUTH_TOKEN` is the value that matters most:
unset or empty, **the control plane accepts unauthenticated callers**, and the
daemon logs a warning saying exactly that.

### hostPort or hostNetwork

Both ship, because the choice belongs to whoever owns the cluster.

| | `overlays/hostport` | `overlays/hostnetwork` |
| --- | --- | --- |
| Media range | one `containerPort`/`hostPort` entry **per port** — Kubernetes cannot express a range — so it is capped at what stays hand-maintainable (the shipped example is 20 ports = 10 tapped legs = 5 two-party calls per pod) | as wide as the node has to spare; the shipped example is 1000 ports = 250 two-party calls per pod |
| Widening it | `overlays/hostport/regenerate.sh <min> <max>` rewrites the entries and the ConfigMap values together | edit two numbers |
| Pod isolation | keeps the pod network, NetworkPolicy, a CNI you need not reason about | gone: the container's ports **are** the node's ports |
| Advertised address | the **node** IP (`status.hostIP`) — that is where the mapping lives | the node IP, because that is the pod's own address |
| Media path | DNAT'd by the node | no translation at all |
| Pods per node | one (they collide on every port) — anti-affinity is `required` | one, same reason |
| Port collisions | scheduler-visible: the pod will not schedule | invisible until a session binds; `mss_media_ports_bind_conflicts_total` counts them |

Pick `hostnetwork` when media volume is the point, `hostport` when the platform
team will not allow it. Neither affects egress: MSS opens the NG socket to
rtpengine itself, and Redis/Kafka/S3 are ordinary outbound TCP.

### Validating the tree

```sh
sh deploy/k8s/validate.sh
```

Three layers, each announced in its own line: `kubectl kustomize` (or
`kustomize build`) renders every overlay; `kubeconform -strict` validates the
rendered objects against the Kubernetes schemas **if it is installed**; and a
python pass asserts what a schema cannot — both probes on the metrics port with
the documented periods, `terminationGracePeriodSeconds` clearing
`MSS_DRAIN_TIMEOUT_SECS`, the spill directory being a mounted volume, the
`hostPort` entries and `MSS_MEDIA_PORT_MIN..MAX` describing the *same* range, the
advertised address coming from the downward API rather than a literal, and no
Secret shipping a real-looking value.

`kubectl apply --dry-run=client` is deliberately **not** one of the layers: it
fetches its schemas from a live API server, so without a cluster it fails with a
connection error and proves nothing.

**Verified 2026-08-26** on this repo, `kubectl` v1.25.9 / kustomize v4.5.7, no
cluster reachable: all three overlays render (9, 10 and 9 objects) and **72
field assertions pass**; `kubeconform` is not installed here, so that layer
reported `SKIP`. The assertions were negative-tested — shortening
`terminationGracePeriodSeconds` to 20 and breaking the readiness path made 6 of
the 72 fail with exit 1.

## Environment

The authority for this table is `crates/mediaserverd/src` — the `*_ENV`
consts — and every value is logged at startup, as the value or as "unset". An
**empty string means unset** for every knob with an unset state, so a variable
can be neutralised in a ConfigMap without deleting it.

### Listeners

| Variable | Default | Meaning | When to change | Added by |
| --- | --- | --- | --- | --- |
| `MSS_CONTROL_LISTEN` | unset | `ip:port` for `MediaControl`, the `TelCompat` façade and the gRPC `MediaStream` data plane — **all three on one port**. Unset means the daemon starts, probes rtpengine and serves nothing ("idling"). A malformed value is refused at startup | always set it; `0.0.0.0:50051` in the manifests | M4 |
| `MSS_METRICS_LISTEN` | unset | `ip:port` for `/metrics`, `/healthz` and `/readyz`. Unset means counters stay in the logs and the pod's probes have nothing to talk to. A bind failure here **refuses to start**, on purpose | always set it; `0.0.0.0:9464` in the manifests | M4 / [item 44](tasks.md) |
| `MSS_AUTH_TOKEN` | unset | Bearer token every control- and data-plane caller must present. **Unset or empty = unauthenticated callers are accepted**, with a warning in the log | always set it, from a Secret | M4 |
| `MSS_POD_NAME` | `mediaserverd` | The registry lease owner. Left at the default, every pod calls itself the same thing and adoption cannot tell them apart | never by hand — the manifests take it from `metadata.name` | M4 |
| `RUST_LOG` | `info` | `tracing-subscriber` EnvFilter. Logs are JSON either way | `debug` while chasing something; per-module targets are cheaper than global debug | — |

### rtpengine and the tap

| Variable | Default | Meaning | When to change | Added by |
| --- | --- | --- | --- | --- |
| `MSS_RTPENGINE_NODE` | unset | `ip:port` of the default NG control socket, used when `CreateSession` names no node. Pinged at startup and re-probed on the health interval; its capabilities are logged on first contact | set it to the rtpengine that anchors most calls; a per-call node in the API overrides it | M2 |
| `MSS_TAP_TRANSCODE` | `on` | `on` asks rtpengine to transcode the tap into `MSS_TAP_FORMAT`. `off` (also `false`/`0`/`no`) takes the call's own codec — **the only knob on our side that can keep a tapped call on rtpengine's kernel fast path** | `off` once you know which codecs actually appear on the legs; with `off`, a codec MSS cannot decode is refused **by name** at subscribe time rather than dropped silently. See [architecture §8.1](architecture.md#81-running-mss-against-a-kernel-module-rtpengine) | [item 20](tasks.md) |
| `MSS_TAP_FORMAT` | `pcmu` | What a tap is decoded as: `pcmu`/`ulaw`, `pcma`/`alaw`, `opus`. An unrecognised value warns and falls back to `pcmu` | `opus` for WebRTC legs. Measured: rtpengine's **transcoder** under-produces Opus (3.8 pkt/s against 50.0 native), so pair `opus` with `MSS_TAP_TRANSCODE=off` | [item 16](tasks.md) |
| `MSS_OPUS_DECODE_RATE_HZ` | `16000` | The rate Opus taps are decoded at: 8000, 12000, 16000, 24000 or 48000 (libopus's own list). Anything else warns and falls back | match what your consumers want; 16 kHz is the usual ASR rate | [item 16](tasks.md) |

### Media addressing

| Variable | Default | Meaning | When to change | Added by |
| --- | --- | --- | --- | --- |
| `MSS_TAP_LOCAL_IP` | `0.0.0.0` | The local address media **and NG** sockets bind | on `hostNetwork: true`, set it to the node address rtpengine can reach, so the source address of MSS's own packets is predictable | Phase 0 |
| `MSS_MEDIA_ADVERTISE_IP` | unset (= the bind address) | The address MSS puts in **every SDP it hands a peer**: the tap's subscribe answer and an inline leg's answer. Sockets still bind `MSS_TAP_LOCAL_IP` | whenever the address a peer must reach is not the address the pod binds — a NAT, a routed VIP, a `hostPort` mapping, a `hostNetwork` node behind a load balancer. **If it is wrong, rtpengine's tap copy goes nowhere and the tap looks up with no audio** | [item 43](tasks.md) |
| `MSS_MEDIA_PORT_MIN` / `MSS_MEDIA_PORT_MAX` | unset (ephemeral ports) | The inclusive UDP range every media socket binds inside — tap sockets and inline RTP. Set both, or neither. **Even ports only** are handed out, so *N* ports serve *N/2* RTP sockets (one per tapped leg; a two-party tap takes two) and `mss_media_ports_capacity` reports the real number | always set it in a firewalled deployment. Unset means an ephemeral port per socket, which no firewall can describe | [item 43](tasks.md) |

### Session registry and events

| Variable | Default | Meaning | When to change | Added by |
| --- | --- | --- | --- | --- |
| `MSS_REDIS_URL` | unset | `redis://…` for ownership leases (TTL 15 s, renewed by heartbeat). Unset = **no HA**: sessions live and die with their pod. Configured but unreachable = **refuse to start** | always set it for more than one pod | [item 3](tasks.md) |
| `MSS_DISCOVERY_REDIS_KEY_PREFIX` | unset (= off) | Turns on the **rtpengine node discovery map** below. When set, a `CreateSession` that names no `rtpengine_node` reads `<prefix><SIP Call-ID>` from Redis before falling back to `MSS_RTPENGINE_NODE`. One `GET` per create, nothing cached | set it once your proxy publishes the map; leave it unset and every session uses the default node | [item 50](tasks.md) |
| `MSS_DISCOVERY_REDIS_URL` | unset (= `MSS_REDIS_URL`) | A **separate** Redis for the discovery map, for when the proxy publishes into its own instance. Unreachable at startup = discovery is switched off with a WARN, never a refusal to start | when the proxy's Redis is not the registry's. If neither this nor `MSS_REDIS_URL` is set, the prefix is ignored with a WARN | [item 50](tasks.md) |
| `MSS_KAFKA_BROKERS` | unset | Comma-separated bootstrap brokers for `MediaEvent`. Unset = events stay in-process, so nothing downstream sees them. Set but naming no broker, or unreachable = **refuse to start** | always set it once anything consumes events | M4 |
| `MSS_EVENTS_TOPIC` | `mss.events` | The topic events are published to | a per-environment topic name | M4 |
| `MSS_EVENTS_PARTITIONS` | `4` | Partition count used when the topic has to be created. Events are keyed by `external_id`, so per-session order survives any partition count | more partitions for more consumer parallelism | M4 |

Events are **at-least-once** since defect D5: a consumer must dedupe by
`(external_id, seq)`. `seq` is gapless per session, so a hole is visible.

### Recording

| Variable | Default | Meaning | When to change | Added by |
| --- | --- | --- | --- | --- |
| `MSS_RECORDING_BUCKET` | unset | The bucket holding `${accountID}/${recordingID}.${format}` — a **frozen** identity scheme (Constitution, Article VII). Unset = `file-s3` attachments are refused by name; unusable = **refuse to start** | always set it for recording | M5 |
| `MSS_RECORDING_S3_ENDPOINT` | unset (AWS) | Endpoint URL for a non-AWS S3 API | MinIO, Ceph, any S3-compatible store | M5 |
| `MSS_RECORDING_S3_REGION` | `us-east-1` | Region for request signing | match the bucket | M5 |
| `MSS_RECORDING_S3_ACCESS_KEY_ID` / `…_SECRET_ACCESS_KEY` | unset | Static credentials. Leave **both** out to let the object store client pick up an instance/IRSA/workload-identity role instead; a half-set pair is the failure that looks like a bug | prefer a role; use keys where there is none | M5 |
| `MSS_RECORDING_SPILL_DIR` | unset (memory only) | Closed segments spill here so a container restart does not lose them (defect D9), and this pod's leftovers are salvaged on its next start — never over an object that already exists | always set it, to a writable volume. The root filesystem is read-only and the process runs as uid 65532, so it must be a mount | [item 30](tasks.md) |
| `MSS_RECORDING_UPLOAD_CONCURRENCY` | `4` | How many finished recordings upload at once. `Detach`/`StopRecording` never waits for an upload ([item 50](tasks.md), D11): it returns as soon as the segment is closed and `RecordingStopped` is published, and the upload runs on in the background. This bounds how many run at a time — and therefore the memory, since each holds one rendered WAV. Empty = unset = the default | raise it only if `mss_recording_uploads_in_flight` sits at the cap while calls end faster than uploads finish; lower it to protect a slow object store | [item 50](tasks.md) |
| `MSS_RECORDING_SPILL_SECONDS` | `30` | How often a live recording spills. This **is** the worst-case audio loss when a container dies mid-call on the same pod | lower for shorter worst-case loss, at more IO | [item 30](tasks.md) |

### Lifecycle

| Variable | Default | Meaning | When to change | Added by |
| --- | --- | --- | --- | --- |
| `MSS_DRAIN_TIMEOUT_SECS` | `30` | Ceiling on the whole shutdown drain: stop accepting, hand registry leases to an adopter, close every session politely (consumer stop frames, recordings finished, taps unsubscribed), flush the event backlog. The process exits **0** whether or not the window is used up; a second SIGTERM/SIGINT exits at once. `0` means "stop accepting and exit". Set `terminationGracePeriodSeconds` **at or above** it — with `docker stop`, whose default grace is 10 s, pass `-t` above it | raise it if long calls need longer to close politely; the grace period must follow | [item 42](tasks.md) |
| `MSS_HEALTH_PROBE_INTERVAL_SECS` | `10` | How often the background watchers re-probe each **configured** dependency for `/readyz`: Redis `PING`, a Kafka partition-offset read, NG `ping`. A failing dependency is re-probed sooner — 1 s, 2, 4, 8, then this interval — so a restarted dependency is picked up quickly; a probe that hangs is a failure after 15 s. The request path never probes, so this bounds only how stale a `/readyz` answer can be | lower for a faster readiness reaction, raise to cut chatter | [item 44](tasks.md) |

### Lab instruments — never set these in a deployment

Setting **`MSS_TAP_CALL_ID`** switches the binary out of the daemon into the
one-shot phase-0 tap spike: it taps that one call, writes a file, exits, and
serves **no API at all**. Its companions do nothing without it and are
documented in [lab.md](lab.md): `MSS_TAP_FROM_TAGS`, `MSS_TAP_OUTPUT`,
`MSS_TAP_SECONDS`, `MSS_TAP_DATAGRAM_LOG_DIR`, `MSS_LISTENERS`,
`MSS_CONSUMER_URL`, `MSS_CONSUMER_ACCOUNT_ID`, `MSS_CONSUMER_STREAM_SID`,
`MSS_CONSUMER_TRACKS`, `MSS_INJECT_TARGET`.

## Ports and firewall matrix

Hand this to whoever owns the firewall. "MSS host" is the pod IP on
`hostport`/pod-network deployments and the node IP on `hostNetwork`.

| Dir | Proto | Port | Peer | What it carries | Required |
| --- | --- | --- | --- | --- | --- |
| in | TCP | `MSS_CONTROL_LISTEN` (50051) | whatever drives MSS: your controller, the `TelCompat` callers, gRPC `MediaStream` consumers | `MediaControl` + `TelCompat` + the data plane, one port, bearer-authenticated | yes |
| in | TCP | `MSS_METRICS_LISTEN` (9464) | Prometheus, and the kubelet for the probes | `/metrics`, `/healthz`, `/readyz` | yes |
| in | **UDP** | `MSS_MEDIA_PORT_MIN..MAX` | **every** rtpengine host that may own a call | the tap copy rtpengine sends, and inline-leg RTP | yes |
| out | **UDP** | 22222 (rtpengine's NG port) | every rtpengine node | NG control: `offer`, `answer`, `subscribe request`/`answer`, `unsubscribe`, `query`, `statistics`, `ping`. **Not inside the media range** — MSS initiates it, which is how a tap gets asked for at all | yes |
| out | TCP | 6379 | Redis | ownership leases | if HA |
| out | TCP | 9092 (or your broker port) | every Kafka broker | `mss.events` | if events |
| out | TCP | 443 / your endpoint port | object storage | recording uploads | if recording |
| out | TCP | 443 / 80 / whatever the consumer listens on | `WS_TWILIO` consumer URLs | MSS **dials out** to a websocket consumer, so this is egress, not ingress | if WS consumers |
| out | UDP+TCP | 53 | DNS | every hostname in the ConfigMap. A default-deny egress policy that forgets this looks exactly like a broker outage | yes |
| out | UDP | 123 | NTP | see the clock requirement — usually the node's job, not the pod's | yes |

**No inbound SIP, ever.** Signalling never reaches MSS; the proxy or softswitch
keeps it, and MSS learns about a call through its own API and through rtpengine.

Media is deliberately absent from the Kubernetes Services: rtpengine sends the
tap copy straight to the address MSS advertised, and a Service in that path
would rewrite the destination and break symmetric RTP.

### Sizing the media range

Three things must agree: `MSS_MEDIA_PORT_MIN`/`MAX` (what MSS binds), the range
the firewall admits inbound from the rtpengine hosts, and — if they differ —
`MSS_MEDIA_ADVERTISE_IP` (what MSS tells rtpengine to send to).

Size it at **two ports per concurrent tapped call**: one even port per leg, with
the odd successor left free for RTCP. So a 1000-port range = 500 even ports =
500 tapped legs = **250 two-party calls per pod**. Add headroom: a port is
returned when the session is destroyed, and a busy pod churns.

Keep the range clear of the node's ephemeral range
(`sysctl net.ipv4.ip_local_port_range`, commonly 32768–60999) or the kernel and
MSS will fight over ports — reserve it with `net.ipv4.ip_local_reserved_ports`
if it must overlap.

Watch `mss_media_ports_in_use` against `mss_media_ports_capacity`;
`mss_media_ports_exhausted_total` rising means sessions are being refused for
want of a port, and `mss_media_ports_bind_conflicts_total` rising means
something else on the host is inside MSS's range.

Verified in the lab (2026-08-26, `lab/media_port_drill.sh`): a live tapped call
on a 40-port range bound `40100` and `40102`, carried 1503 datagrams in 15 s,
and returned both ports to the range when the session was destroyed.

## Sizing a pod

The estimate, from [architecture §8](architecture.md#8-scaling--deployment-model):
a passive tap of both legs at G.711/20 ms is ~100 pkt/s in, one decode+resample
and per-consumer encodes — **comfortably a few thousand concurrent sessions per
modern 8-core pod** with an allocation-free hot path. An interactive **inline**
session costs roughly **2×** (bidirectional plus playout pacing). A conference
costs the mix, once per room, plus a per-member encode.

These are estimates to validate, **not promises**, which is why the manifests
say so where the numbers live. Start from the shipped requests — `cpu: 2`,
`memory: 1Gi`, `limits.memory: 2Gi` — for a few hundred concurrent tapped calls,
then measure your own codec mix and consumer count and move them.

Two deliberate choices in that block:

- **No CPU limit.** The packet path runs on dedicated real-time OS threads; CFS
  throttling on those threads surfaces as pacing jitter and consumer underruns,
  not as a slow API. Bound the pod with requests and node-level allocation.
- **The spill volume is an `emptyDir`, not a PVC.** It only has to survive a
  container restart: a cross-pod adopter cannot read another pod's disk either
  way. Size it at (spill interval) × (concurrent recordings) × 16 kB/s per
  8 kHz 16-bit channel — about 1 MB per channel-minute — with headroom for a
  bucket outage.

Scale on active session count (and CPU) rather than requests per second. Calls
are minutes long, so **scale-in is slow by nature**: a pod cannot leave until its
calls end or are adopted. Plan for it.

## Leg attribution — pass the caller's from-tag

**If you want a tap's two tracks named `customer` and `agent`, your control
plane must tell MSS which from-tag is the caller's.** `CreateSession.from_tags`
takes it (the `telcompat` façade reads it from `callerFromTag`). This is the one
piece of a call's identity MSS cannot recover on its own, and the reason is
rtpengine's, measured on 14.1.1.8 (item 47):

- a `query` reply gives each participant a `created` stamp of **whole seconds**,
  and it is stamped **per dialogue** — the offering and the answering leg of one
  call carry the *same* value even when the answer came seconds later. Only
  legs from *different* dialogues on one call-id differ;
- so ordering the participants by creation time cannot tell the caller from the
  callee, and neither can the order the reply lists them in (it is
  lexicographic by tag).

MSS therefore refuses to guess. Every session reports an **`attribution`** on
`DescribeSession` and on every `MediaEvent`:

| `attribution` | When | What a consumer sees |
| --- | --- | --- |
| `explicit` | `from_tags` was supplied — and always for an inline leg | `customer` / `agent` |
| `inferred` | rtpengine's `created` seconds strictly ordered the participants (only possible when one call-id carries more than one dialogue) | `customer` / `agent` |
| `unknown` | nothing separated them | **`leg_a` / `leg_b`** — no direction is claimed |

Under `unknown` the daemon logs a WARN naming both tags and their stamps, and
publishes one `LegsAttributed` event carrying the track names the consumer will
actually see. Audit it in one line:

```sh
mss_ctl "$CONTROL" describe "$EXTERNAL_ID" | grep -o 'attribution: "[a-z]*"'
```

Two consequences worth knowing before a pilot:

- the **WS Twilio** dialect is frozen: its tracks stay `inbound`/`outbound`
  whatever the attribution says, so a WS consumer's only warning is the event.
  Attach over `GRPC_STREAM` where speaker identity matters;
- recording object keys follow the same names, so an unattributed recording
  group writes `…/<participant>.leg_a.wav` — a downstream job keyed on
  `.customer.wav` will not find it. That is the intended failure: better a
  missing file than a confidently mislabelled speaker.

## Which rtpengine anchors the call — the optional discovery map

`CreateSession` can name the node (`rtpengine_node`), and a control plane that
already knows it should keep doing that: it is the cheapest and most explicit
path. But a caller that only knows the SIP Call-ID — the `telcompat` façade is
one, since `StartStream` carries `sipCallId` and no node — otherwise lands on
`MSS_RTPENGINE_NODE`, and in a deployment with more than one rtpengine that is a
guess. Your **proxy already knows the answer**: it picked the instance. Let it
publish that fact.

Set `MSS_DISCOVERY_REDIS_KEY_PREFIX` and MSS reads one key per create:

| | |
| --- | --- |
| Key | `<prefix><SIP Call-ID>` — e.g. `mss:call-node:a84b4c76e66710@host` |
| Value, minimum | `172.31.99.10:22222` — a bare `ip:port` |
| Value, richer | `{"node":"172.31.99.10:22222","caller_tag":"<caller from-tag>","from_tags":["<caller>","<callee>"]}` |
| TTL | yours to choose. Longer than your longest call; the proxy deletes the key on BYE |

Order of resolution, per `CreateSession`: **the node in the request** wins;
otherwise the map; otherwise `MSS_RTPENGINE_NODE`. Nothing is cached between
creates — a map that changes mid-call is read fresh by the next session, and
there is no cache to invalidate.

What the two value forms buy:

- `node` alone saves the guess. MSS still asks rtpengine `query` for the call's
  participants, as it does today;
- `from_tags` **also saves that `query`** when both legs are named: the tap is
  subscribed straight from the map;
- `caller_tag` is what makes the attribution `explicit` (`customer`/`agent`).
  **A tag list without `caller_tag` stays `unknown`** — tracks are `leg_a` /
  `leg_b`, exactly as in *Leg attribution* above. The map may not silently
  decide who called: if your proxy knows the caller, mark it.

Failure is always a fallback, never a refusal: an unreachable Redis, a missing
key or an unreadable value all leave the session on the default node with a WARN
in the log. Three counters make it auditable — and they appear on `/metrics`
**only when discovery is configured**:

| Metric | Meaning |
| --- | --- |
| `mss_discovery_hits_total` | the map named the node |
| `mss_discovery_misses_total` | no key for this call-id; the default node was used |
| `mss_discovery_errors_total` | Redis unreachable, or the value was not an `ip:port` / node object |

### The OpenSIPS side — copy this

`cachedb_redis` writes the key where MSS reads it. `$ci` is the Call-ID, `$ft`
the caller's from-tag, `$tt` the callee's. The prefix and the TTL are the two
knobs; keep the prefix identical to `MSS_DISCOVERY_REDIS_KEY_PREFIX`.

```
loadmodule "cachedb_redis.so"
modparam("cachedb_redis", "cachedb_url", "redis://10.0.0.20:6379/")

# Two knobs live in this snippet, spelled out at every call site because
# OpenSIPS cannot concatenate a #!define into a string:
#   the key prefix  "mss:call-node:"  == MSS_DISCOVERY_REDIS_KEY_PREFIX
#   the TTL         14400 seconds     >  your longest call

route {
    ...
    if (is_method("INVITE") && !has_totag()) {
        rtpengine_offer();
        # the node is known here: publish it with the caller's tag
        cache_store("redis", "mss:call-node:$ci",
                    "{\"node\":\"10.0.0.10:22222\",\"caller_tag\":\"$ft\"}",
                    14400);
    }
    if (has_totag() && is_method("BYE")) {
        rtpengine_delete();
        cache_remove("redis", "mss:call-node:$ci");
    }
}

onreply_route[...] {
    if (has_body("application/sdp")) {
        rtpengine_answer();
        # now BOTH tags are known -- rewriting the key here saves MSS a `query`
        cache_store("redis", "mss:call-node:$ci",
                    "{\"node\":\"10.0.0.10:22222\",\"caller_tag\":\"$ft\",\"from_tags\":[\"$ft\",\"$tt\"]}",
                    14400);
    }
}

failure_route[...] {
    cache_remove("redis", "mss:call-node:$ci");
}
```

Two practical notes. **Pick the node from the same variable your rtpengine
module selected**, not a literal, if your proxy load-balances across a set —
otherwise the map is confidently wrong, which is worse than absent. And check
your OpenSIPS image actually ships the module: `ls
/usr/lib/x86_64-linux-gnu/opensips/modules | grep cachedb`. The pinned
`opensips/opensips:3.4` lab image does **not** (it has `cachedb_local.so` and
`cachedb_sql.so` only, and apt.opensips.org no longer carries a 3.4 component
for bullseye), so `lab/opensips/opensips.cfg` publishes the same key and value
through `exec.so` + `lab/opensips/discovery_publish.py` instead. The bytes in
Redis are identical; only the writer differs.

Verified live (2026-08-26, `lab/node_discovery_drill.sh`) with the pod's default
node pointed at a black hole (`172.31.99.199:22222`) so nothing but the map
could work: the proxy published
`{"node":"172.31.99.10:22222","caller_tag":"hosttest","from_tags":["hosttest","y3HmFyeQae04N"]}`
on the answer; `mss_ctl create <id> <call-id> -` with **no node and no
from-tags** tapped 1192 datagrams in 12 s with `attribution=explicit` and
`mss_discovery_hits_total 1`; an unmapped call-id counted one miss and was
**refused** on the black-hole default (`no reply from rtpengine at
172.31.99.199:22222 after 3 attempts`) rather than guessing; the BYE removed the
key.

## Digit menus — drive them from the event bus

Every DTMF press on a tapped or inline leg is published to `mss.events` as a
`Dtmf` payload carrying `digit`, `track`, `duration_ms` and the event's
`rtp_timestamp`, **with no consumer attached and no capability required** — one
event per press, whatever the endpoint's end-packet retransmissions. So an
integrator can map `*6` to an `UpdateAttachment` call, or drive any in-call menu,
straight off the bus; a media stream is no longer needed just to hear a digit.
MSS interprets no digit itself: no menu state, no collection, no inter-digit
timer. Track names follow the attribution above, so an unattributed session's
digits arrive on `leg_a`/`leg_b`.

## Recordings — stopping one does not wait for its upload

`Detach` (and the legacy controller's `StopRecording`, and `DestroySession`) answers as soon as
the recording's last segment is closed and `RecordingStopped` is on the bus. The
upload then runs in the background, up to
`MSS_RECORDING_UPLOAD_CONCURRENCY` at a time, and reports itself afterwards with
`UploadCompleted` — or, new since [item 50](tasks.md), with **`UploadFailed`**
carrying the object key and the store's own error, so an integrator waiting for a
recording never waits forever. Measured in the lab: `Detach` **11 ms** and
`DestroySession` **10 ms** while the object store was frozen and held the upload
**12 s**.

Two rules follow for whoever consumes `mss.events`:

- **`UploadCompleted`/`UploadFailed` may arrive after `SessionEnded`** for the
  same session, and is then the last event of that session's sequence. `seq` is
  still gapless and still in order — MSS keeps the session record in a
  *finishing* state until its uploads settle, precisely so the event keeps its
  place. Do not treat `SessionEnded` as "no further events for this session".
- A finishing session is **gone for every other purpose**: it is not listed, not
  adoptable, cannot be attached to, and its `external_id` is free again
  immediately, so re-creating a session with the same external id right after a
  hangup works. `DescribeSession` by session id still answers it; by external id
  it does not.

Watch `mss_recording_uploads_in_flight` (should return to 0),
`mss_recording_uploads_backgrounded_total`,
`mss_recording_upload_failures_total` and
`mss_recording_upload_settle_timeouts_total` (a stuck upload abandoned after
10 minutes — its audio is on the spill disk for the next start's salvage).
A pod killed with SIGKILL between the detach and the upload loses that event; the
audio is recovered by the salvage pass, without an event. A **graceful** drain
does not: it waits for the uploads (step 4 of the rollout sequence below).

## Conference member state — read it back before you trust it

`DescribeSession` on a conference member's session reports that member's live
`mute`/`deaf`/`hold`, its mix routes (target, source, whether the recording feed
carries it, and the attachment that owns each) and the room it sits in — group
name, member count and every member's external id — so a UI can reconcile a whole
room from any one member, with no extra RPC. Member state has **no lease**: if
your controller dies between `mute on` and `mute off` the member stays muted for
the life of the conference, so reconcile on reconnect rather than assuming.

## High availability: what is adoptable and what is not

Ownership is a TTL'd lease in Redis, renewed by heartbeat. On pod loss another
pod takes the lease and re-subscribes. That works because taps are
**pull-initiated** — MSS asks rtpengine to send it a copy — so the new pod can
simply ask again. Nothing else in the system has that property.

| Workload | Survives a pod loss? | What actually happens |
| --- | --- | --- |
| **Passive taps** | **yes** | another pod adopts the session and re-subscribes. Measured: **1.96 s** of consumer audio gap after a graceful SIGTERM drain (adoption 2.7 s after the signal), against **14.41 s** for the same call killed with `SIGKILL` |
| **Attachments MSS dials out to** (`WS_TWILIO`, `file-s3`) | yes | re-established by the adopter along with the tap |
| **Attachments that dial into MSS** (`GRPC_STREAM`) | no, by construction | the consumer's HTTP/2 connection died with the pod; it must reattach. This is why the Service is headless — a client resolving all pod IPs notices |
| **Inline legs** (Phase 3) | **no** | the peer's SDP points at a socket that no longer exists. This fails like any media endpoint failure and needs recovery at call-control level |
| **Conferences** (Phase 4) | **no** | the mixer is pod-local; a room does not move. Defect D16 — pod-local groups — is open pending a placement decision |
| **Recordings** | **partly** | closed segments spill to per-pod local disk, so a **same-pod** restart loses at most `MSS_RECORDING_SPILL_SECONDS`. A **cross-pod** adopter cannot read that disk: it recovers what it can, pads the rest, and counts it in `mss_recording_frames_lost_on_adopt_total`. Defect D9's residual |
| **Events** | yes | at-least-once with a bounded retry backlog; a pod death or an overfull backlog still loses events, and the gapless per-session `seq` makes the hole visible |

Run at least two pods, spread across nodes (the base prefers it; both overlays
require it, because host ports collide). The PodDisruptionBudget holds voluntary
disruption to `maxUnavailable: 1` — two pods draining at once means two adoption
storms at once.

## Health probes

`MSS_METRICS_LISTEN` serves three paths and nothing else (anything else is a
404, a non-GET a 405):

| Path | Meaning | Probe |
| --- | --- | --- |
| `/healthz` | 200 `alive` while the process runs. Consults no dependency on purpose — restarting a pod because Redis is down turns one outage into a crash-loop. | `livenessProbe` |
| `/readyz` | 200 only when the pod is **not draining** and every **configured** dependency answered its last probe; otherwise **503**, first line `not ready: <reasons>`, then a line per dependency. An unconfigured dependency reads `not configured` and counts as ready. | `readinessProbe` |
| `/metrics` | Prometheus exposition, including `mss_ready` and `mss_dependency_ready{dependency="rtpengine"\|"redis"\|"kafka"}`. | `ServiceMonitor` |

```
readinessProbe: { httpGet: { path: /readyz, port: 9464 }, periodSeconds: 5 }
livenessProbe:  { httpGet: { path: /healthz, port: 9464 }, periodSeconds: 10 }
```

Readiness turns 503 on the **first** step of the drain, so a rollout stops
sending new sessions to a pod before it starts closing the ones it holds —
measured in the lab (2026-08-26): `docker stop -t 60` on a live pod, `/readyz`
polled every ~5 ms, 200 at t+0.028 s, **503 `not ready: draining` at
t+0.035 s**, listener gone at t+0.042 s, exit 0 at t+0.404 s. A dependency
outage is measured too: stopping the lab Redis turned `/readyz` 503 within
**8 s** with `not ready: redis unreachable: session store: timed out`,
`/healthz` stayed 200, and starting Redis again returned 200 within **3 s**.

Do not point a probe at `/metrics`: it renders the whole exposition and says
nothing about readiness.

The manifests also carry a `startupProbe` on `/healthz` (2 s × 30): first
contact with rtpengine, the spill salvage pass and the Kafka connect all happen
before the listener binds, and a slow one of those should not trip liveness.

## Rollout behavior

mediaserverd drains on **SIGTERM** as well as SIGINT, so an ordinary Kubernetes
rollout or eviction is graceful. The sequence, in order, all inside
`MSS_DRAIN_TIMEOUT_SECS`:

1. **stop accepting** — readiness goes false (`/readyz` 503 `not ready:
   draining`, `mss_draining` flips to 1); no new session or attachment is taken
   here.
2. **hand off leases** — the registry lease is released so another pod adopts on
   its next sweep rather than after the 15 s TTL.
3. **close sessions** — each one politely: consumers get their protocol's own
   stop frame, recordings are stopped, taps are unsubscribed.
4. **await uploads** — every recording upload backgrounded by step 3 is waited
   for. It comes *before* the event flush on purpose: an upload that settles
   afterwards would strand its own `UploadCompleted` in the outbox at exit.
5. **control plane idle** — the listener closes once in-flight calls finish.
6. **flush events** — the backlog is given a 10 s window to reach Kafka.

Then exit **0**, whether or not the window was used up. A **second** signal
exits at once.

Measured in the lab (2026-08-26, `lab/drain_drill.sh`): a tapped live call, pod
stopped with `docker stop -t 60`, **exit 0 in 0.44 s**, drain 30 ms, the second
pod adopting **2.7 s** after the signal and the consumer's audio gap **1.96 s** —
against **14.41 s** for the same call killed with SIGKILL.

Two things follow for the manifests:

- **`terminationGracePeriodSeconds` must clear `MSS_DRAIN_TIMEOUT_SECS`.** The
  base ships 45 against a 30 s drain — margin for the kubelet's own round
  trips. Below the drain timeout the runtime SIGKILLs a half-done drain and
  every call on that pod pays the 14 s gap instead of the 2 s one.
  `validate.sh` asserts the relationship.
- **No `preStop` hook, on purpose.** The drain is signal-driven: mediaserverd
  handles SIGTERM itself and its *first* step flips `/readyz` to 503, so the
  endpoint is pulled by exactly the mechanism a `preStop: sleep` exists to
  emulate. Adding one would only delay the SIGTERM and eat the grace period.

`maxSurge: 1, maxUnavailable: 0` and the PDB keep a rollout to one draining pod
at a time. A rollout of a fleet carrying long calls takes as long as the calls
do — that is the design, not a stall.

## Alerts and metrics

[`deploy/prometheus-alerts.yaml`](../deploy/prometheus-alerts.yaml) is the single
source of truth: it works as a plain Prometheus `rule_files:` entry, and
`deploy/k8s/sync-alerts.sh` wraps it into
`deploy/k8s/base/prometheusrule.yaml` for the Prometheus Operator, so the two
cannot drift (the diff the script produces **is** the drift check). Every
silent-drop counter has an alert — Constitution, Article VIII: a drop is a
first-class metric, never a silence. The thresholds are Phase-1 starting points;
tune them against your own baseline.

The `ServiceMonitor` scrapes every 15 s, because the rules use `rate(…[5m])`
windows and anything slower makes a short drop burst invisible. It selects by
`release: prometheus` — match your own Prometheus's selector or the monitor is
silently ignored. No operator? Drop both files and scrape
`mediaserverd-scrape:9464/metrics` however you already scrape things; nothing in
mediaserverd depends on the operator.

## Running against a kernel-module rtpengine

Whether a tap drags the tapped legs out of rtpengine's kernel fast path is
decided by **transcoding, not tapping** — the module carries no codec, so
anything rtpengine must convert is handled in its userspace, while fan-out to an
extra destination is something the module is built for. No MSS media-path code is
involved either way.

The eligibility checklist for a tenant, the on-metal read-only probes, and why
the version cannot be asked over NG are all in
[architecture §8.1](architecture.md#81-running-mss-against-a-kernel-module-rtpengine).
The short form: set `MSS_TAP_TRANSCODE=off`, confirm codec coverage first, run
`lab/kernel_probe.sh <host> <port>` at baseline **and** with taps running, and
read the daemon's own `rtpengine node capabilities on first contact` line.

## Preflight — check the environment before deploying into it

`lab/preflight.sh` asks the target environment every question mediaserverd will
ask it, from the outside, before mediaserverd is there to ask. It is POSIX sh
plus python3 from the standard library and nothing else, so it runs on a jump
host with no `pip`, and it copies alongside `lab/kernel_probe.sh` (which it
calls for the kernel verdict).

```sh
./lab/preflight.sh \
  --ng 10.0.0.5:22222 --redis redis://10.0.0.6:6379 --kafka 10.0.0.7:9092 \
  --topic mss.events --s3-endpoint https://s3.eu-west-1.amazonaws.com \
  --bucket call-recordings --region eu-west-1 \
  --media-ports 40000-40999 --advertise-ip 10.0.0.8 \
  --ssh rtpengine-1 --rtpengine-version 14.1.1.8 [--json]
```

Every option defaults to the `MSS_*` variable mediaserverd itself reads
(`MSS_RTPENGINE_NODE`, `MSS_REDIS_URL`, `MSS_KAFKA_BROKERS`, `MSS_EVENTS_TOPIC`,
`MSS_RECORDING_S3_ENDPOINT`, `MSS_RECORDING_BUCKET`, `MSS_RECORDING_S3_REGION`,
`MSS_RECORDING_S3_ACCESS_KEY_ID`, `MSS_RECORDING_S3_SECRET_ACCESS_KEY`,
`MSS_MEDIA_PORT_MIN`/`MAX`, `MSS_MEDIA_ADVERTISE_IP`, `MSS_TAP_LOCAL_IP`), so on
a host with the deployment's environment file sourced, bare `./preflight.sh`
checks exactly what that deployment would use. `--json` puts a machine-readable
report on stdout and the lines on stderr. **Exit 0 when nothing FAILed, 1 when
something did, 2 on bad usage.**

Every line is `PASS`, `FAIL` or `SKIP` with one sentence saying why. A `SKIP` is
an *unchecked assumption* — it always names what to run to check it by hand — not
a pass.

| Check | What it does | FAIL means |
| --- | --- | --- |
| `ng_ping` | NG `ping` to the rtpengine control socket. | Wrong port, a firewall, or not an NG socket. |
| `ng_subscribe` | Fabricates its **own throwaway call** (`offer` + `answer`, call-id `mss-preflight-<pid>-<epoch>`), then `subscribe request` + `subscribe answer`. | This rtpengine cannot feed MSS a tap at all — the one capability there is no workaround for. |
| `ng_tap_media` | Pumps ~1 s of PCMU into that call and counts the datagrams that come back on the subscription socket. | rtpengine cannot reach the address MSS advertises (`--advertise-ip`), or the media range is blocked inbound. |
| `ng_cleanup` | `unsubscribe`, `delete`, then `query` to prove the call is gone. | The probe may have left state on rtpengine; the line names the call-id to hunt. |
| `rtpengine_version` | Reports the version **you** supply with `--rtpengine-version`. | Below 11, which predates `subscribe request`/`answer`. `ng_subscribe` above is the real authority. |
| `kernel_forwarding` | Runs `kernel_probe.sh` and reports its verdict verbatim. | rtpengine unreachable. Userspace-only forwarding is a **PASS** that says so: MSS taps either way, the difference is rtpengine's CPU (architecture §8.1). |
| `redis` | Raw RESP over a socket: `SET <key> NX EX 30`, `TTL`, `DEL` — the registry lease, exactly. | No Redis, wrong auth, or a server that does not expire keys. |
| `kafka` | TCP connect to every bootstrap broker. | No broker accepted a connection. |
| `kafka_topic` | Produces one probe record to the topic and reads it back — **only if a `kafka` python client is importable** (`kafka-python-ng`). | The topic will not take a record. Without the library this is a `SKIP` naming the `rpk`/`kafka-topics.sh` command to run on a broker host: hand-rolling the Kafka wire protocol here would be a second implementation of what MSS already has in rskafka. |
| `s3` | `PUT`/`HEAD`/`DELETE` of `mss-preflight/<run>.probe`, then a `HEAD` to prove it is gone — SigV4 signed in the standard library (path style). With no `--access-key`/`--secret-key` it falls back to the `aws` CLI if on PATH; the line always says which path was used. | Wrong bucket, endpoint, region or credentials — the S3 error code is in the line (`NoSuchBucket`, `SignatureDoesNotMatch`, …). |
| `media_ports` | Validates the range, reports its real capacity (**even ports only**), and binds both ends locally. | Half a range set, no even port in it, or something on this host already holds it. Only meaningful when run **on** the host MSS will run on. |
| `media_udp` | With `--ssh <rtpengine-host>`: opens a listener on the range's first even port and has the rtpengine host send it 5 datagrams (`python3`, else `nc -u`). | A firewall between rtpengine and MSS, or the wrong advertise address. Without `--ssh` it is a `SKIP`. |
| `clock` | `chronyc tracking` (100 ms tolerance) or `timedatectl`. | The clock is not NTP-synchronised, so recording timestamps, event order and recording-group alignment cannot be trusted across pods. |

### Verified in the lab (2026-08-26)

The NG port is not published to the WSL host, so the tool runs inside the lab
network — the same way it would run on a jump host inside a deployment:

```sh
docker run --rm --network mss-microsip_lab -v "$PWD/lab":/lab:ro -w /lab \
  python:3-slim sh /lab/preflight.sh \
  --ng 172.31.99.10:22222 --redis redis://172.31.99.61:6379 \
  --kafka 172.31.99.60:9092 --s3-endpoint http://172.31.99.62:9000 \
  --bucket lab-recordings --access-key minioadmin --secret-key minioadmin \
  --media-ports 40100-40139 --rtpengine-version 14.1.1.8
```

**Green run — everything reachable (exit 0):**

```
PASS  ng_ping             rtpengine at 172.31.99.10:22222 answered pong in 1 ms
PASS  ng_subscribe        'subscribe request'/'subscribe answer' accepted on a throwaway call (1 stream(s), to-tag f09bda99029e...)
PASS  ng_tap_media        49 tapped RTP datagrams arrived at 172.31.99.2:36965 from 98 pumped into the call
PASS  ng_cleanup          the throwaway call mss-preflight-1-1787735746 was unsubscribed, deleted, and is gone from rtpengine
PASS  rtpengine_version   14.1.1.8 (caller-supplied; MSS is verified against 14.1.1.8, and ng_subscribe above proves this build's support)
PASS  kernel_forwarding   kernel_probe.sh: the kernel module is NOT in play -- every packet this node relayed went through userspace -- MSS taps a userspace relay just as well, at a higher CPU cost per call on rtpengine (architecture 8.1)
PASS  redis               SET NX / TTL 30s / DEL round-tripped on 172.31.99.61:6379 db 0
PASS  kafka               1/1 broker(s) accepted a TCP connection
SKIP  kafka_topic         no kafka client library here, so nothing produced to 'mss.events'; install kafka-python-ng, or check from a broker host with `rpk topic describe mss.events`
PASS  s3                  put/head/delete of lab-recordings/mss-preflight/1-1787735746.probe on http://172.31.99.62:9000 via stdlib SigV4 (region us-east-1, path style), and it is gone afterwards (HEAD -> 404)
PASS  media_ports         40100-40139 gives 20 even ports = 20 tapped legs (10 two-party calls); both ends bind free on 0.0.0.0
SKIP  media_udp           no --ssh <rtpengine-host> given, so nothing proved the rtpengine host can reach the media range inbound (ng_tap_media above covers the path this host sees)
SKIP  clock               neither chronyc nor timedatectl here, so NTP offset is unknown -- check it on the host MSS will run on, not from a container

preflight: 10 PASS, 0 FAIL, 3 SKIP
preflight: nothing failed; every SKIP above is an unchecked assumption
```

**Red run — same command with `--ng 172.31.99.10:22223` (wrong port) and
`--bucket wrong-bucket-name` (exit 1):**

```
FAIL  ng_ping             no reply from 172.31.99.10:22223 in 3 tries over 6 s -- wrong port, a firewall, or not an rtpengine NG socket
SKIP  ng_subscribe        rtpengine did not answer ping
SKIP  ng_tap_media        rtpengine did not answer ping
SKIP  rtpengine_version   cannot be asked over NG -- rtpengine has no NG 'version' command; read it on the rtpengine host with `rtpengine --version`, `dpkg -l ngcp-rtpengine-daemon` / `rpm -q rtpengine`, or over its CLI socket (`rtpengine-ctl` with --listen-cli), then re-run with --rtpengine-version
FAIL  kernel_forwarding   kernel_probe.sh could not reach rtpengine: kernel_probe: no reply from 172.31.99.10:22223; is this an NG port?
PASS  redis               SET NX / TTL 30s / DEL round-tripped on 172.31.99.61:6379 db 0
PASS  kafka               1/1 broker(s) accepted a TCP connection
SKIP  kafka_topic         no kafka client library here, so nothing produced to 'mss.events'; ...
FAIL  s3                  PUT wrong-bucket-name/mss-preflight/1-1787735763.probe on http://172.31.99.62:9000 -> HTTP 404 NoSuchBucket (stdlib SigV4)
PASS  media_ports         40100-40139 gives 20 even ports = 20 tapped legs (10 two-party calls); both ends bind free on 0.0.0.0
SKIP  media_udp           no --ssh <rtpengine-host> given, ...
SKIP  clock               neither chronyc nor timedatectl here, ...

preflight: 3 PASS, 3 FAIL, 6 SKIP
preflight: FAILED ng_ping: no reply from 172.31.99.10:22223 ...
preflight: FAILED kernel_forwarding: kernel_probe.sh could not reach rtpengine ...
preflight: FAILED s3: PUT wrong-bucket-name/... -> HTTP 404 NoSuchBucket (stdlib SigV4)
preflight: this environment is NOT ready for mediaserverd
```

A third run with `kafka-python-ng` installed and an `ssh` shim proved the two
paths the lab container cannot exercise on its own: `kafka_topic` **PASS** ("a
probe record produced to 'mss.preflight.probe' partition 0 at offset 0 was read
back") and `media_udp` **PASS** ("a UDP datagram sent from the rtpengine host
arrived on 172.31.99.2:40100"). Afterwards rtpengine answered `Unknown call-id`
for all three fabricated call-ids and the bucket held no probe object: the tool
leaves the environment as it found it. The probe record is the one thing it
cannot take back — it lands on the **configured** topic, so pass
`--topic mss.preflight` if a probe record on `mss.events` would confuse a
consumer.

## First day on real gear — an ordered runbook

Do these in order. Each step's failure is diagnosable on its own; skipping ahead
turns one unknown into three. Everything before step 3 is read-only.

**0. Preflight, from a jump host in the target network.**

```sh
./lab/preflight.sh --ng <rtpengine>:22222 --redis <url> --kafka <brokers> \
  --s3-endpoint <endpoint> --bucket <bucket> --media-ports <min>-<max> \
  --advertise-ip <the address rtpengine must reach> --ssh <rtpengine-host>
```

Exit 0 before going further. `ng_subscribe` FAIL stops the whole exercise —
that rtpengine cannot feed MSS a tap, and handoff **H2** (read the version on
the host; it cannot be asked over NG) is the next thing to do. Pass `--ssh` if
you possibly can: `media_udp` is the check that catches the firewall, and it is
a `SKIP` without it. Note every `SKIP` — each is an unchecked assumption, and
each names the command that would settle it.

**1. Deploy, with no calls pointed at it.** Apply an overlay, then read the
startup log and the two probes before anything else:

```sh
kubectl -n mediaserver logs deploy/mediaserverd | head -40
kubectl -n mediaserver port-forward deploy/mediaserverd 9464:9464 &
curl -s localhost:9464/healthz; curl -sS localhost:9464/readyz
```

Every `MSS_*` value is echoed at startup as a value or as "unset" — read that
list against the ConfigMap; a typo'd variable name is silently "unset". Confirm
`rtpengine node capabilities on first contact` names your node, and that
`/readyz` is **200**. A 503 names the dependency on its first line.

**2. One tapped call, `MSS_TAP_TRANSCODE=off`.** Have the proxy anchor one real
call, then create a session and attach one consumer through the API. Transcoding
off from the start, so the first call also answers the codec question: MSS sees
the carrier's own codec, and one it cannot decode is refused **by name** instead
of silently dropped. Check, in this order: the consumer receives audio; the
packet rate is what the codec implies (~50 pkt/s per leg at 20 ms — a rate far
below that is rtpengine's transcoder, not MSS); `mss_media_ports_in_use` shows
two ports for a two-party call; `mss.events` carries the session's events with a
gapless `seq`. **No audio at the consumer with a healthy subscribe is almost
always `MSS_MEDIA_ADVERTISE_IP` or the inbound media range** — that pair is the
single most common first-day failure.

**3. `kernel_probe.sh` again, with taps running.** Baseline was step 0; now
compare. On the rtpengine host add the read-only evidence NG cannot expose —
`/proc/rtpengine/<table>/list` for `num_destinations` on the target entries, and
`currentstatistics.media_kernel` / `media_userspace` / `transcodedmedia` — at
baseline, with a transcoding tap, and with a transcode-off tap. That comparison
**is** handoff **H3**, the rtpengine-side per-tap cost, and it sets the rtpengine
capacity plan. The full read-only checklist is
[architecture §8.1](architecture.md#81-running-mss-against-a-kernel-module-rtpengine).

**4. Recording.** Attach a `file-s3` consumer to one call, hang up, and check the
object landed at exactly `${accountID}/${recordingID}.${format}` — that identity
is frozen, and anything reading recordings downstream depends on it. Then pause
and resume mid-call and confirm the segmenting. Then kill the container mid-call
and confirm the spill salvage on restart. Note the residual honestly: a
**cross-pod** adopter cannot read the dead pod's spill directory (defect D9), and
byte-parity against a FreeSWITCH recording is handoff **H6**.

**5. Drain and adopt, on purpose, before it happens by accident.** With a tapped
call up: `kubectl -n mediaserver delete pod <pod>`. Expect the drain sequence in
the log, exit 0, another pod adopting, and a consumer gap of a couple of seconds
— not fifteen. If the pod is SIGKILLed instead, `terminationGracePeriodSeconds`
is below `MSS_DRAIN_TIMEOUT_SECS`.

**6. Inline, through the proxy's B2B.** Last, because it is the only step whose
failure can be entirely on the other side. `CreateSession{kind=INLINE,
sdp_offer}` returns a real SDP answer; the proxy or B2BUA has to bridge a SIP leg
onto it. **No inline leg in this repository has ever met a SIP endpoint** — every
inline and conference measurement here is against a bare RTP peer with no
signalling. That is handoff **H5**, and inline legs do **not** survive a pod
loss (see the HA table). Measure barge-in end to end against your own perceptual
budget while you are there — handoff **H4**.

Then, and only then, flip a tenant.

## The reference deployment, as a worked example

The project grew out of one production contact-center stack, and that stack
appears throughout the docs as a concrete stand-in for generic roles: carrier →
OpenSIPS → rtpengine → FreeSWITCH for the customer leg, a Go telephony
controller driving FreeSWITCH over ESL, and a per-call RTP↔WebSocket gateway
that MSS supersedes. The compatibility surfaces that shape exercises — the
Twilio Media Streams websocket dialect, the `mod_audio_fork` event names, the
the legacy verb API façade — are **optional adapters**, not part of the core.

Read it as an example of how the pieces fit, never as a requirement. Any
deployment whose media anchors in rtpengine can run MSS with none of it.
[lab.md](lab.md) is that shape, small enough to run on one machine.
