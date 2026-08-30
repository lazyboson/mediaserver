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
deploy/k8s/overlays/filesystem-recording/
                                  record to a shared RWX volume instead of a
                                  bucket (item 58) -- compose it with one of
                                  the two media overlays, not instead of one
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
it. Pick an overlay. The two media overlays are alternatives to each other;
`overlays/filesystem-recording` is orthogonal — it changes only where
recordings land, and a deployment wanting both composes them in one
kustomization of its own (`resources: [../hostnetwork, recordings-pvc.yaml]`
plus this overlay's two patches).

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
| `MSS_SIP_LISTEN` | unset | `ip:port` the **SIP front door** answers INVITEs on (UDP). Unset means the door stays shut and inline legs arrive over the control API only, exactly as before. A malformed value is refused at startup | `0.0.0.0:5080` where a B2BUA bridges to MSS. **This is the one port a stranger can address**, so firewall it to the signalling elements that should reach it | [architecture §7](architecture.md) |
| `MSS_SIP_ADVERTISE` | `MSS_SIP_LISTEN` | `ip:port` put in the `Contact` of every answer — where the peer sends its ACK and BYE. Defaults to the listen address, which is wrong behind NAT or a service IP | set it to the address the *peer* can reach when the pod's listen address is not routable from there | [architecture §7](architecture.md) |
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

#### What MSS keeps in Redis

Everything lives under one namespace prefix (`mss:`). Nothing here is a cache
you may clear while calls are up.

| Key | Shape | Written by | Expiry |
| --- | --- | --- | --- |
| `mss:session:<externalId>` | JSON `PersistedSession` — call-id, from-tags, rtpengine node, tap to-tag, every attachment | the owning pod's keeper tick | none; deleted when the session ends |
| `mss:lease:<externalId>` | the owner pod's name, `SET NX` | the owner, renewed every 5 s | **15 s** — its expiry is what makes a session adoptable |
| `mss:sessions` | a set of every known `externalId` | the owner | none; members are removed on destroy |
| `mss:group:<accountId>/<group>` | JSON `{recording_id, format, opened_at_unix_ms, created_by}`, created with `SET NX` so the loser of a race reads the winner back | the **first** member of a recording group, on any pod | **3 h** (`MAX_RECORDING` + 1 h), refreshed on every join |
| `mss:group:<accountId>/<group>:members` | hash `object key → owner pod`; `HSETNX` is the duplicate-participant refusal | every member, on its own pod | 3 h, refreshed on every join; both keys are deleted when the hash empties |
| `<MSS_DISCOVERY_REDIS_KEY_PREFIX><SIP Call-ID>` | the rtpengine node map below — **written by your proxy**, only read by MSS | your proxy | yours |

The two `mss:group:` keys are what make a recording group a shared record
instead of one pod's memory (item 54): a member can join from any pod, an
adopted member rejoins the group it was already in, and a group name reused on
a second pod finds the existing group instead of opening a second
half-recording under the same object prefix. Because the group's open instant
is now **wall-clock** (unix ms in the record) rather than one pod's monotonic
clock, cross-pod time alignment of a group's participant objects is only as
good as the nodes' clock sync: **a skew of *s* between two nodes misaligns
their two participants by *s***. Kubernetes nodes run NTP, so this is normally
single-digit milliseconds — but if you disable it, alignment goes with it. A
pod that has `MSS_REDIS_URL` unset keeps groups in its own memory, which is
correct for a single pod and wrong for two.

### Recording

| Variable | Default | Meaning | When to change | Added by |
| --- | --- | --- | --- | --- |
| `MSS_RECORDING_STORE` | `s3` | Which store holds the recordings: `s3` (the default, and what architecture §3 chose) or `filesystem`, a shared tree every recording pod mounts. **Any other value is a storage misconfiguration and refuses to start**, exactly as an unusable bucket does. The attachment transport stays `file-s3` either way — the wire is frozen (Constitution, Article VII); the `UploadCompleted.uri` scheme (`s3://` or `file://`) is what says which store it was | `filesystem` when there is no object store to point at, or when the pickup job downstream already watches a mounted tree | [item 58](tasks.md) |
| `MSS_RECORDING_ROOT` | unset | Read **only** under `MSS_RECORDING_STORE=filesystem`, and then required: the absolute root of the tree holding `${accountID}/${recordingID}.${format}` (and `${accountID}/${recordingID}/${participant}.${format}` for a group) **verbatim** — the layout `record_session` produced, so an existing pickup job keeps working. At startup MSS writes, renames and deletes a probe file under it and **refuses to start** if it cannot. It must be the **same** volume on every recording pod (RWX PVC, EFS, NFS — MSS does not care which), or a cross-pod adopter finds nothing. Ignored with one warning under the `s3` store | always, with `filesystem` | [item 58](tasks.md) |
| `MSS_RECORDING_BUCKET` | unset | The bucket holding `${accountID}/${recordingID}.${format}` — a **frozen** identity scheme (Constitution, Article VII). Unset = `file-s3` attachments are refused by name; unusable = **refuse to start** | always set it for recording | M5 |
| `MSS_RECORDING_S3_ENDPOINT` | unset (AWS) | Endpoint URL for a non-AWS S3 API | MinIO, Ceph, any S3-compatible store | M5 |
| `MSS_RECORDING_S3_REGION` | `us-east-1` | Region for request signing | match the bucket | M5 |
| `MSS_RECORDING_S3_ACCESS_KEY_ID` / `…_SECRET_ACCESS_KEY` | unset | Static credentials. Leave **both** out to let the object store client pick up an instance/IRSA/workload-identity role instead; a half-set pair is the failure that looks like a bug | prefer a role; use keys where there is none | M5 |
| `MSS_RECORDING_SPILL_DIR` | unset (memory only) | Closed segments spill here so a container restart does not lose them (defect D9), and this pod's leftovers are salvaged on its next start — never over an object that already exists | always set it, to a writable volume. The root filesystem is read-only and the process runs as uid 65532, so it must be a mount | [item 30](tasks.md) |
| `MSS_RECORDING_UPLOAD_CONCURRENCY` | `4` | How many finished recordings upload at once. `Detach`/`StopRecording` never waits for an upload ([item 50](tasks.md), D11): it returns as soon as the segment is closed and `RecordingStopped` is published, and the upload runs on in the background. This bounds how many run at a time — and therefore the memory, since each holds one rendered WAV. Empty = unset = the default | raise it only if `mss_recording_uploads_in_flight` sits at the cap while calls end faster than uploads finish; lower it to protect a slow object store | [item 50](tasks.md) |
| `MSS_RECORDING_SPILL_SECONDS` | `30` | How often a live recording spills. This **is** the worst-case audio loss when a pod dies mid-call — on the same pod always, and on **any** pod with `MSS_RECORDING_SPILL_TO=s3` | lower for shorter worst-case loss, at more IO | [item 30](tasks.md), [item 53](tasks.md) |
| `MSS_RECORDING_SPILL_TO` | `disk` | Where the segment journal lives. `disk` is per-pod local disk (`MSS_RECORDING_SPILL_DIR`; unset there still means "no spill"), so a **cross-pod** adopter reads nothing. `s3` puts the journal in the recording bucket itself under `MSS_RECORDING_SPILL_PREFIX`, so any pod can finish a recording a dead pod started — and it means **the recording store, whichever one it is**, so under the filesystem store the journal lands at `<root>/_spill/…` on the shared volume. `store` is the clearer synonym for `s3` and does exactly the same thing. Anything else logs a warning and means `disk` | set `s3` whenever you run more than one pod and care about recordings surviving a node loss | [item 53](tasks.md) |
| `MSS_RECORDING_SPILL_PREFIX` | `_spill/` | The **reserved** key prefix inside the recording bucket that `MSS_RECORDING_SPILL_TO=s3` writes journals under (a missing trailing `/` is added). Nothing but MSS may write there, and nothing outside it is ever written by the spill | change it only if `_spill/` collides with keys you already have | [item 53](tasks.md) |

**Which store.** S3 is the default and the recommendation: architecture §3 chose
direct-to-object-store over the legacy shared-filesystem recording ("no shared
filesystem, no SQS hop") and nothing here retracts that — a filesystem is one
more thing that can be full, stale or slow for every pod at once, and it needs
RWX, which not every cluster offers. Pick `filesystem` for one of two reasons:
there is no object store to point at, or the recording pickup downstream already
watches a mounted tree the way it watched FreeSWITCH's `record_session` output
and you would rather keep that tree than change the consumer. Everything else is
identical — the frozen identity, pause segmenting, recording groups, spill and
cross-pod adoption, the `file-s3` transport name, the events — so the choice is
one variable and is reversible for **new** recordings at any time (recordings
already written stay where they were written; nothing migrates them).
`lab/preflight.sh --recording-root DIR` runs the same startup probe from the
outside, and `deploy/k8s/overlays/filesystem-recording` is the variable plus an
RWX PVC mounted at `/var/lib/mediaserverd/recordings`.

**The store holds two namespaces, and only one of them is a contract.**
`${accountID}/${recordingID}.${format}` (and
`${accountID}/${recordingID}/${participant}.${format}` for a recording group) is
the frozen recording identity — Constitution, Article VII. Everything under
`MSS_RECORDING_SPILL_PREFIX` (`_spill/` by default) is MSS's own scratch space:
raw little-endian PCM chunks and a `manifest.json` per unfinished recording, of
no use to a consumer, deleted by MSS as soon as the recording reaches its real
key. Two operational consequences:

- **Do not point a downstream consumer at the whole bucket** (or at the whole
  recording root). Filter to the
  identity scheme, or give `_spill/` its own exclusion, or your pipeline will
  try to play headerless `.pcm` files.
- **Give `_spill/` a lifecycle rule: expire objects after 7 days.** MSS deletes a
  journal when its recording lands and skips a foreign one on startup salvage, so
  what accumulates is only the journals of recordings that never finished on any
  pod. **MSS implements no retention of its own** — this rule is the retention
  policy, and it is yours to configure. On S3 it is one lifecycle
  configuration with `Filter.Prefix: _spill/` and `Expiration.Days: 7`; on MinIO,
  `mc ilm rule add --expire-days 7 --prefix _spill/ lab/<bucket>`. Seven days is
  the number to start from: it is far past any `MAX_RECORDING` (2 h) and short
  enough that a bad week does not become a storage bill.

### Lifecycle

| Variable | Default | Meaning | When to change | Added by |
| --- | --- | --- | --- | --- |
| `MSS_DRAIN_TIMEOUT_SECS` | `30` | Ceiling on the whole shutdown drain: stop accepting, hand registry leases to an adopter, close every session politely (consumer stop frames, recordings finished, taps unsubscribed), flush the event backlog. The process exits **0** whether or not the window is used up; a second SIGTERM/SIGINT exits at once. `0` means "stop accepting and exit". Set `terminationGracePeriodSeconds` **at or above** it — with `docker stop`, whose default grace is 10 s, pass `-t` above it | raise it if long calls need longer to close politely; the grace period must follow | [item 42](tasks.md) |
| `MSS_CONFERENCE_LINGER_SECS` | `0` | How long a conference whose **room session** is open is held after its last member leaves. `0` (the default) ends the room session on the last leave — its room recording is closed and uploaded there and then. A non-zero value keeps the mix running and the room session open for that long, so a member who rejoins inside the window lands back in the same room, on the same clock, still recording into the same object; a rejoin cancels the wait. A conference with **no** room session is unaffected: its last member out always closes it | raise it a few seconds if your call flow drops the last leg and dials back in (a transfer, a re-INVITE the proxy handles by re-dialling) and you want one recording rather than two | [item 55](tasks.md) |
| `MSS_MEMBER_STATE_TTL_SECS` | `0` | The **default lease** on a conference member flag: how long a `member_mute` / `member_deaf` / `member_hold` set `on` holds without being refreshed when the request itself names no `member_state_ttl_ms`. `0` (the default) means no lease — the flag holds until an explicit `off`, exactly as it did before item 56. A request may always override this per call, and an explicit `member_state_ttl_ms=0` outranks it | set it to a little longer than your UI's refresh interval (say `30`) when a tenant drives mute from a screen, so a controller that dies costs one lease rather than the life of the conference. Leave it `0` when member state is owned by a policy engine that will reconcile it | [item 56](tasks.md) |
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
  way, which is what `MSS_RECORDING_SPILL_TO=s3` is for. Size it at (spill interval) × (concurrent recordings) × 16 kB/s per
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

`Detach` (and cigol's `StopRecording`, and `DestroySession`) answers as soon as
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

## Conference recording — record the room off the room, not off a member

A conference records two ways, and both may run at once:

- **the room, as one mono object** — `Attach{transport=FILE_S3,
  selector.only="mixed", endpoint="<account>/<recording>.wav"}`;
- **every participant, one object each** — a recording **group** over the member
  sessions with `selector.only="customer"`, which writes
  `<account>/<recording>/<label>.wav` per member.

Attach the room object to the **room session**:

```
CreateSession{external_id="room-42", kind=SESSION_KIND_MIX, group="<conference>"}
Attach{session="room-42", transport=FILE_S3, selector.only="mixed",
       endpoint="acct-7/rec-42.wav"}
```

`mss_ctl <endpoint> create room-42 --kind mix --group <conference>` does the
first line from a shell, and `mss_ctl … record room-42 acct-7/rec-42.wav room ""
mixed` the second.

A room session has no leg, no SDP, no media ports and no `call_id`; its `group`
names the conference it **is**, and it is refused by name if it carries any of
those things or no group. Whichever arrives first — the room session or the first
member — opens the conference, so the room object can be opened before anybody
joins (it records the wait as silence) or after (it is padded back to the
conference's open). `DescribeSession` on it returns `opened_at_unix_ms` — the
**conference's** open, the t=0 every recording of that room shares, including the
per-participant group — and `conference.members`; every member's own
`DescribeSession` names the room session back in `conference.room_session`.

The room session ends on `DestroySession`, or by itself once the conference has
held at least one member and then emptied (`MSS_CONFERENCE_LINGER_SECS`). Ending
it uploads the room object; members that are still mixing are left alone.
`StartPlayback` on it with no target is a **room prompt**, and an
`Attach{SINK, only="mixed"}` on it is a monitor of the room that costs no leg.

Recording the room off **a member's** session still works and is still supported
— but that object ends when that member leaves, even though the conference keeps
mixing, and its t=0 is its attach moment. That is D20, and the room session is
the way not to have it.

## Conference member state — read it back before you trust it

`DescribeSession` on a conference member's session reports that member's live
`mute`/`deaf`/`hold`, its mix routes (target, source, whether the recording feed
carries it, and the attachment that owns each) and the room it sits in — group
name, member count and every member's external id — so a UI can reconcile a whole
room from any one member, with no extra RPC.

### The lease, and the refresh loop a UI should run

By default a member flag holds until an explicit `off`: if your controller dies
between `mute on` and `mute off`, the member stays muted for the life of the
conference. Since item 56 a request may bound that with a **lease**.

- **`member_state_ttl_ms`** is a fourth metadata key on the same `Attach` /
  `UpdateAttachment` that carries the verbs. It applies to **every flag set `on`
  in that request**, and is a whole number of milliseconds; anything else is
  refused by name. Absent, or `0`, means no lease.
- **`MSS_MEMBER_STATE_TTL_SECS`** is the deployment default for a request that
  names no `member_state_ttl_ms`. An explicit `0` in the request outranks it.
- **A refresh is any request that sets the flag `on` again with a TTL** — the
  same `UpdateAttachment`, resent. There is no separate renew verb, and
  resending the identical metadata is enough: MSS moves the deadline out even
  though the declared state did not change (so no `MemberControlled` event is
  published for a refresh that changed nothing).
- **`off` clears the flag and its deadline** and is still how you lift a mute
  early.
- **Reading it back:** `DescribeSession` fills
  `MemberState.{mute,deaf,hold}_expires_in_ms` with the milliseconds left. `0`
  means no lease, which is also what an unset flag reports.
- **When a lease runs out** the pod lifts the flag itself, through exactly the
  path an explicit `off` takes, and publishes `MemberControlled` with
  `cause = MEMBER_CONTROL_CAUSE_EXPIRED` and the member's whole remaining state
  (a hold that was never leased stays `true`). `REQUESTED` is the zero value, so
  a consumer written before item 56 reads every controller-driven event
  unchanged. `mss_conference_member_state_expired_total` counts the flags lifted.

**The loop a UI should run:** pick a TTL comfortably longer than your refresh
interval — 30 s TTL refreshed every 10 s is a reasonable shape — resend the
`UpdateAttachment` on that interval while the screen holds the member muted, and
send `off` when the operator unmutes. Then a browser tab that closes, a pod that
restarts or a controller that crashes costs at most one TTL of unwanted mute
instead of the rest of the call. Watch
`mss_conference_member_state_expired_total`: in a healthy deployment it stays
near zero, because refreshes arrive before the deadlines do.

**Two limits worth knowing.** The lease is **pod-local** — it lives in the
control-world mirror beside the flag, so it does not survive a pod loss and does
not move with a member (conferences are pod-bound anyway). And the deadline is
checked by a control-world sweep every 500 ms, so a flag lifts up to half a
second after its lease runs out; nothing in the media path holds a timer.

## Attachment metadata — the `mss.` prefix is reserved

`Attach` and `UpdateAttachment` carry a free-form `metadata` map, and MSS reads
a handful of keys out of it: `accountId`, `streamSid`, `callSid`, `recordId`,
`fileFormat`, `recordingChannels`, `sipCallId`, `callerTag`, and the conference
verbs `mix_target` / `mix_monitor` / `mix_source` / `member_mute` /
`member_deaf` / `member_hold` / `member_state_ttl_ms`. Everything else is yours
and is passed through untouched — **except any key beginning `mss.`**, which is refused with
`INVALID_ARGUMENT` naming the key.

That prefix is how mediaserverd's own session registry talks to itself: when a
pod adopts a session, `RegistryKeeper::rebuild` re-issues the attachment with
`mss.recording.resumeMs` (how much audio the dead pod had recorded, which
becomes leading silence) and `mss.recording.spillOwner` (the pod whose spill
journal to read, and the fact that this attach may take back its
recording-group seat). A client that could set those could silence-pad any
recording or claim another pod's seat in a recording group, so the guard sits
on the wire: the keeper reaches the controller in-process and is unaffected,
and the same refusal covers the legacy `telsvc` façade, whose `StartStream`
copies caller metadata straight through. If you are carrying your own
namespaced keys, use anything but `mss.` — `tenant.`, your product's name,
whatever — and nothing changes for you.

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
| **Conferences** (Phase 4) | **no** | the mixer is pod-local; a room does not move. This fails like an inline leg, because a conference member *is* one |
| **A room session** (`kind=MIX`) | **no** | it owns a mix thread on the pod that opened it, so it is pod-bound exactly like an inline leg: the registry releases the record rather than adopting it (`is_rebuildable` is false), and its room recording is recovered the way any recording is (spill, below), not by moving the room |
| **Recording groups** | **yes, with `MSS_REDIS_URL` set** | the group is a record in Redis (`mss:group:…` above), not one pod's memory: a member's adopted session rejoins the group, a member may attach on any pod, and the group's wall-clock anchor keeps every participant object aligned across pods. Without Redis a group is pod-local, and a member adopted elsewhere opens an ungrouped file. What placement still does not do (D8) is *choose* one pod for a group — it no longer has to |
| **Recordings** | **yes, with `MSS_RECORDING_SPILL_TO=s3`** | closed segments spill every `MSS_RECORDING_SPILL_SECONDS` into the recording bucket, the adopting pod reads them back and pads only what the dead pod had not spilled yet, so the worst case is one spill interval on **any** pod (`mss_recording_frames_lost_on_adopt_total` says how much). With the default `disk` that guarantee holds only for a **same-pod** restart, because a cross-pod adopter cannot read the dead pod's disk — it recovers nothing and pads the whole recording. Either way the journals of recordings that never finished need a bucket lifecycle rule on `_spill/`: MSS implements no retention. Never measured on a live pod kill with a recorder attached — see [item 53](tasks.md) |
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

### Recording spill series (item 53)

Read these three together when a pod dies mid-recording:

| Series | Meaning |
| --- | --- |
| `mss_recording_spill_segments_total` | closed segments written to the journal while the call was still up. **Zero on a pod that is recording is the alarm**: nothing is spilling, so a pod death costs the whole recording |
| `mss_recording_spill_failures_total` | segments the journal refused (disk full, bucket unreachable, past the 10 s spill timeout). The audio stays in memory and the next tick retries, so a few are survivable and a rising rate is not |
| `mss_recording_frames_lost_on_adopt_total` | recorded frames an adopting pod could read from neither memory nor the journal, and turned into silence. With `MSS_RECORDING_SPILL_TO=s3` this should stay under one spill interval per adoption; with `disk` a cross-pod adoption shows up here as the whole recording |
| `mss_recording_spill_lost_ownership_total` | closed segments **not** spilled because another pod had already adopted the journal. Non-zero means a partitioned-but-alive pod was still recording a call it no longer owns — the registry's `mss_registry_lost_total` should say the same thing |
| `mss_recording_spill_foreign_manifests` | journals this pod's startup salvage left alone because their manifest names another owner. Expected and healthy after a rolling restart; a number that only grows means journals nobody is finishing, which is what the `_spill/` lifecycle rule is for |
| `mss_recording_salvaged_total` / `…_salvage_skipped_total` / `…_salvage_failures_total` | what this pod's start did with its *own* leftover journals: uploaded, left alone because the object already existed, or failed |

### Recording group series (item 54)

A recording group writes one object per participant under one prefix. These
three say whether the group is healthy; all three exist whether or not Redis is
configured, because the gauges are per pod.

| Series | Meaning |
| --- | --- |
| `mss_recording_groups_live` | recording groups with at least one member **on this pod**. Sum across pods to see a cross-pod group counted once per pod that holds a member of it |
| `mss_recording_group_members_live` | members of those groups on this pod. One per `FILE_S3` attachment that named a group |
| `mss_recording_group_joins_refused_total` | grouped attachments refused, by one of four reasons the log names: a participant label already writing that object (now including one held **by another pod**), a member naming a different `recordingID` or format inside an existing group, a non-empty group on any transport but `FILE_S3`, and — since item 54 — a **session registry MSS could not read**. That last one is deliberate: a member that cannot see the group would open a second half-recording under the same prefix, so it is refused instead |

### Conference member state series (item 56)

| Series | Meaning |
| --- | --- |
| `mss_conference_member_controls_total` | times a member was muted, deafened, put on hold or released — every write, whether a controller asked or a lease expired |
| `mss_conference_member_state_expired_total` | **member flags this pod lifted by itself** because their lease ran out unrefreshed. One per flag, not per member: a mute and a deaf expiring together count two. A rising rate means controllers are setting leases and not refreshing them — either their refresh loop is broken or `MSS_MEMBER_STATE_TTL_SECS` is shorter than the interaction it is bounding. Flat at zero on a pod whose clients send no TTL is expected |
| `mss_conference_muted_members` / `mss_conference_deaf_members` / `mss_conference_held_members` | how many members are in that state right now, on this pod. These say *how many*, never *who* — `DescribeSession` per member says who |

### rtpengine node series (item 57)

Every `/readyz` rtpengine probe (`MSS_HEALTH_PROBE_INTERVAL_SECS`) also takes an
NG `statistics` sample and exports it, so handoff **H3**'s three-moment
comparison can be read off Prometheus instead of a shell on the rtpengine host.
The series exist only for nodes this pod has actually probed, and they are the
*node's own* counters, not MSS's — two pods tapping one rtpengine report the
same numbers.

| Series | Means |
| --- | --- |
| `mss_rtpengine_tap_kernel_verdict{node,verdict}` | 1 on the one verdict that held at the last probe. Label values are the four verdicts: `TranscodedTapsAreProcessedInUserspace`, `TapsMayRideTheKernelPath`, `ThisNodeIsNotUsingTheKernelModule`, `Undetermined` |
| `mss_rtpengine_relayed_packets_kernel{node}` | packets this node has relayed in the kernel module since it started |
| `mss_rtpengine_relayed_packets_user{node}` | packets it has relayed in userspace since it started |
| `mss_rtpengine_media_kernel{node}` | media streams in the kernel module right now |
| `mss_rtpengine_media_userspace{node}` | media streams in userspace right now |
| `mss_rtpengine_media_mixed{node}` | media streams counted in both right now |
| `mss_rtpengine_transcoded_media{node}` | media streams this node is transcoding right now |
| `mss_rtpengine_sessions_live{node}` | sessions this node is managing right now |
| `mss_rtpengine_sample_age_seconds{node}` | seconds since the probe that produced the sample. It grows past the probe interval when `statistics` stops answering — read it before trusting the rest |

`MssTapsFellOutOfKernel` fires on the transcoding verdict, or on userspace media
growing for 10 min while kernel media stays flat; its runbook is
[architecture §8.1](architecture.md#81-running-mss-against-a-kernel-module-rtpengine).

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
read the daemon's own `rtpengine node capabilities on first contact` line —
or, since item 57, read the verdict and the relay split off `/metrics`
(`mss_rtpengine_tap_kernel_verdict`, `mss_rtpengine_relayed_packets_kernel`
vs `_user`), which is refreshed on every health probe rather than once.

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
`MSS_RECORDING_STORE`, `MSS_RECORDING_ROOT`, `MSS_MEDIA_PORT_MIN`/`MAX`,
`MSS_MEDIA_ADVERTISE_IP`, `MSS_TAP_LOCAL_IP`), so on
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
| `recording-root` | With `--recording-root DIR` (or `MSS_RECORDING_STORE=filesystem`): write, `fsync`, rename and delete a probe file under the root — **the same probe mediaserverd runs at startup** — and report the free space. An unknown `--recording-store` FAILs here too, because mediaserverd refuses to start on one. | The root does not exist, is not writable by uid 65532, or cannot rename (some FUSE and SMB mounts) — mediaserverd would refuse to start. A `SKIP` means the store is `s3`. It cannot tell you the volume is the **same** one on every pod: with `ReadWriteOnce` this line passes and cross-pod adoption still recovers nothing. |
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
and confirm the spill salvage on restart. Then, with
`MSS_RECORDING_SPILL_TO=s3`, kill a pod mid-recording and confirm another pod
finishes the object with at most `MSS_RECORDING_SPILL_SECONDS` missing — that is
the cross-pod half of D9, and it has never been measured on real gear or in the
lab. Byte-parity against a FreeSWITCH recording is handoff **H6**.

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
telsvc façade — are **optional adapters**, not part of the core.

Read it as an example of how the pieces fit, never as a requirement. Any
deployment whose media anchors in rtpengine can run MSS with none of it.
[lab.md](lab.md) is that shape, small enough to run on one machine.
