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
| `MSS_HEALTH_PROBE_INTERVAL_SECS` | `10` | How often the background watchers re-probe each configured dependency for `/readyz` (see [tasks.md item 44](tasks.md)): Redis `PING`, a Kafka partition-offset read, and NG `ping` to the rtpengine node. A **failing** dependency is re-probed sooner — 1 s, 2, 4, 8, then this interval — so a restarted dependency is picked up quickly; a probe that hangs is a failure after 15 s. Lower it for a faster readiness reaction, raise it to cut chatter. The request path never probes, so this value bounds only how stale a `/readyz` answer can be. |
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

## Health probes

`MSS_METRICS_LISTEN` serves three paths and nothing else (anything else is a 404,
a non-GET a 405):

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
sending new sessions to a pod before it starts closing the ones it holds — measured
in the lab (2026-08-26): `docker stop -t 60` on a live pod, `/readyz` polled every
~5 ms, 200 at t+0.028 s, **503 `not ready: draining` at t+0.035 s**, listener gone
at t+0.042 s, exit 0 at t+0.404 s. A dependency outage is measured too: stopping
the lab Redis turned `/readyz` 503 within **8 s** with
`not ready: redis unreachable: session store: timed out`, `/healthz` stayed 200,
and starting Redis again returned 200 within **3 s**.

Do not point a probe at `/metrics`: it renders the whole exposition and says
nothing about readiness.

## Rollout behavior

mediaserverd drains on **SIGTERM** as well as SIGINT, so an ordinary Kubernetes
rollout or eviction is graceful: readiness goes false first (`/readyz` answers
**503 `not ready: draining`** and the `mss_draining` gauge flips to 1), the registry lease is released so another pod adopts the call
on its next sweep rather than after the 15 s TTL, and consumers are closed with
their protocol's own stop frame instead of a dropped socket.

Measured in the lab (2026-08-26, `lab/drain_drill.sh`): a tapped live call, pod
stopped with `docker stop -t 60`, **exit 0 in 0.44 s**, drain 30 ms, the second
pod adopting **2.7 s** after the signal and the consumer's audio gap **1.96 s** —
against **14.41 s** for the same call killed with SIGKILL.

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
