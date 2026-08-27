"""The soak + impairment suite: the standing proof that hours are boring.

tasks.md item 19. The three test altitudes already exist -- replay (`cargo test`),
lab (the drills next to this file) and benchmark (`cargo bench -p media-core`).
What none of them says is whether N tapped calls stay healthy for an hour with
sessions churning underneath them. This does, and it FAILS LOUDLY, so a cron or
CI job can own it without anybody reading the log.

What it runs
------------
N concurrent synthetic calls, each a `host_test_caller.py` on its own SIP/RTP
ports with its own SIP Call-ID, dialed back to back for the whole duration so
the run exercises session churn and not just one long-lived tap. Each call is
tapped by a real `mediaserverd` pod (round robin over the three lab pods) with a
`WS_TWILIO` consumer attached, and the consumer is one `gap_consumer.py` keyed
per call, so every call gets its own continuity number.

What it asserts, every scrape (deltas from a pre-run baseline, per pod)
----------------------------------------------------------------------
  * `mss_legs_stalled` is 0                      -- the audio-flow watchdog
  * `mss_consumer_dropped_oldest_total` bounded  -- MAX_DROPPED_OLDEST
  * `mss_events_failed_total` is 0               -- and abandoned, and lost leases
  * `mss_ingest_recv_errors_total` is 0
  * the consumer is still being fed while sessions are live
and once, at the end:
  * process RSS is flat -- read from /proc inside each pod, not `docker stats`,
    because the container's number includes page cache and the cargo wrapper.

Impairment
----------
`SOAK_PHASES` walks the impairment matrix from testing.md: each phase names a
profile and how long to hold it, and new calls pick up the current profile. The
profile is applied on the tap link with `tc netem` (lab/netem.sh) when the kernel
can, and otherwise at the endpoint -- host_test_caller's own IMPAIR_* knobs,
which damage this caller's RTP upstream of rtpengine. That fallback is honest but
weaker, and the summary says which one ran. On the WSL2 kernel this lab uses,
CONFIG_NET_SCH_NETEM is unset, so it is the fallback (see docs/testing.md).

Running it
----------
    DOCKER_API_VERSION=1.43 docker compose -f lab/docker-compose.microsip.yml \
      up -d rtpengine opensips freeswitch call-watcher redpanda redis \
            minio minio-init mock-bridge mss-control mss-control-b mss-control-c
    python3 lab/soak.py

The compose `mediaserverd` service (the Phase-0 spike) must stay DOWN: it taps
whatever call it finds from its own process.

Env: SOAK_CALLS, SOAK_PHASES, CALL_SECONDS, SCRAPE_SECONDS, SOAK_NETEM
(auto|on|off), MAX_DROPPED_OLDEST, RSS_GROWTH, RSS_SLACK_KB, CONSUMER_HOST,
CONSUMER_PORT, SIP_PORT_BASE, RTP_PORT_BASE, DIAL, TALK_EVERY, OUT.
"""

import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
OUT = os.environ.get("OUT", os.path.join(HERE, "out"))
STAMP = str(int(time.time()))

CALLS = int(os.environ.get("SOAK_CALLS", "3"))
CALL_SECONDS = float(os.environ.get("CALL_SECONDS", "120"))
SCRAPE_SECONDS = float(os.environ.get("SCRAPE_SECONDS", "60"))
TEARDOWN_LEAD = float(os.environ.get("TEARDOWN_LEAD", "6"))
ANSWER_TIMEOUT = float(os.environ.get("ANSWER_TIMEOUT", "30"))
DIAL = os.environ.get("DIAL", "9000")
TALK_EVERY = os.environ.get("TALK_EVERY", "9")
SIP_PORT_BASE = int(os.environ.get("SIP_PORT_BASE", "45100"))
RTP_PORT_BASE = int(os.environ.get("RTP_PORT_BASE", "45200"))
CONSUMER_HOST = os.environ.get("CONSUMER_HOST", "host.docker.internal")
CONSUMER_PORT = int(os.environ.get("CONSUMER_PORT", "8096"))
MAX_DROPPED_OLDEST = int(os.environ.get("MAX_DROPPED_OLDEST", "0"))
RSS_GROWTH = float(os.environ.get("RSS_GROWTH", "1.25"))
RSS_SLACK_KB = int(os.environ.get("RSS_SLACK_KB", "8192"))
NETEM_MODE = os.environ.get("SOAK_NETEM", "auto")
SPIKE = os.environ.get("SPIKE", "mss-microsip-mediaserverd-1")
DEFAULT_PHASES = "clean:480,loss1:480,loss5:480,reorder:360,jitter:360"

PODS = [
    {"name": "A", "container": "mss-microsip-mss-control-1",
     "control": "http://127.0.0.1:50551", "metrics": "http://127.0.0.1:9464/metrics"},
    {"name": "B", "container": "mss-microsip-mss-control-b-1",
     "control": "http://127.0.0.1:50552", "metrics": "http://127.0.0.1:9465/metrics"},
    {"name": "C", "container": "mss-microsip-mss-control-c-1",
     "control": "http://127.0.0.1:50553", "metrics": "http://127.0.0.1:9466/metrics"},
]

# Every profile the impairment matrix names, mapped to the caller's own knobs
# for when netem is unavailable. These damage the endpoint -> rtpengine leg,
# which is the matrix's *second* injection point, not the tap link.
ENDPOINT_IMPAIRMENT = {
    "clean": {},
    "loss1": {"IMPAIR_LOSS": "0.01"},
    "loss5": {"IMPAIR_LOSS": "0.05"},
    "burst": {"IMPAIR_LOSS": "0.10"},
    "reorder": {"IMPAIR_REORDER": "0.10"},
    "reorder-far": {"IMPAIR_REORDER": "0.25", "IMPAIR_JITTER_MS": "120"},
    "duplicate": {"IMPAIR_DUPLICATE": "0.01"},
    "jitter": {"IMPAIR_JITTER_MS": "35"},
}

COUNTERS = [
    "mss_consumer_delivered_total",
    "mss_consumer_dropped_oldest_total",
    "mss_events_abandoned_total",
    "mss_events_accepted_total",
    "mss_events_dropped_oldest_total",
    "mss_events_dropped_total",
    "mss_events_failed_total",
    "mss_events_outbox_dropped_total",
    "mss_events_published_total",
    "mss_events_retried_total",
    "mss_ingest_datagrams_total",
    "mss_ingest_frames_concealed_total",
    "mss_ingest_frames_played_total",
    "mss_ingest_frames_suppressed_total",
    "mss_ingest_recv_errors_total",
    "mss_ingest_stalls_total",
    "mss_ingest_underruns_total",
    "mss_ingest_unknown_payload_type_total",
    "mss_ingest_unparsable_total",
    "mss_jitter_duplicates_total",
    "mss_jitter_late_drops_total",
    "mss_jitter_lost_total",
    "mss_jitter_resets_total",
    "mss_jitter_silence_gaps_total",
    "mss_legs_ssrc_changes_total",
    "mss_registry_adopted_total",
    "mss_registry_failed_total",
    "mss_registry_lost_total",
    "mss_registry_persisted_total",
]
GAUGES = [
    "mss_consumer_queue_depth_frames",
    "mss_consumer_queue_depth_frames_max",
    "mss_consumers_live",
    "mss_events_retry_depth",
    "mss_legs_live",
    "mss_legs_stalled",
    "mss_legs_unknown_ssrc",
    "mss_sessions_live",
]

# The counters worth attributing to the impairment profile that was running, so
# the recorded run says what each profile cost rather than only that it passed.
PER_PHASE = [
    "mss_ingest_datagrams_total",
    "mss_ingest_frames_played_total",
    "mss_ingest_frames_concealed_total",
    "mss_ingest_frames_suppressed_total",
    "mss_ingest_underruns_total",
    "mss_jitter_lost_total",
    "mss_jitter_late_drops_total",
    "mss_jitter_duplicates_total",
    "mss_jitter_silence_gaps_total",
    "mss_jitter_resets_total",
    "mss_consumer_delivered_total",
    "mss_consumer_dropped_oldest_total",
]

LOG_LOCK = threading.Lock()
LOG = None
VIOLATIONS = []
CALL_RECORDS = []
SCRAPES = []
PHASE_TALLY = {}
STOP = threading.Event()


def say(message):
    line = f"soak: {time.strftime('%H:%M:%S')} {message}"
    with LOG_LOCK:
        print(line, flush=True)
        if LOG is not None:
            LOG.write(line + "\n")
            LOG.flush()


def violation(message):
    with LOG_LOCK:
        VIOLATIONS.append(message)
    say(f"ASSERTION FAILED: {message}")


def docker(*args, **kwargs):
    environment = dict(os.environ, DOCKER_API_VERSION=os.environ.get(
        "DOCKER_API_VERSION", "1.43"))
    return subprocess.run(["docker", *args], env=environment, capture_output=True,
                          text=True, **kwargs)


def scrape(url):
    """Prometheus text into {family: summed value}. Labels are summed over
    because every assertion here is about a pod, not about one series."""
    try:
        with urllib.request.urlopen(url, timeout=5) as answer:
            body = answer.read().decode("utf-8", "replace")
    except (urllib.error.URLError, OSError) as error:
        return None, str(error)
    values = {}
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        name, _, value = line.rpartition(" ")
        name = name.split("{", 1)[0].strip()
        if not name:
            continue
        try:
            values[name] = values.get(name, 0.0) + float(value)
        except ValueError:
            continue
    return values, None


def pod_rss_kb(container):
    """VmRSS of the daemon itself, from /proc inside the container. `docker
    stats` reports the cgroup, which folds in page cache and the cargo wrapper,
    and a leak hunt cannot afford that noise."""
    script = ('for d in /proc/[0-9]*; do c=$(cat $d/comm 2>/dev/null); '
              'if [ "$c" = mediaserverd ]; then '
              'sed -n "s/^VmRSS:[[:space:]]*\\([0-9]*\\).*/\\1/p" $d/status; fi; done')
    done = docker("exec", container, "sh", "-c", script)
    for token in done.stdout.split():
        try:
            return int(token)
        except ValueError:
            continue
    return None


def mss_ctl(endpoint, *args):
    binary = os.path.join(REPO, "target", "debug", "examples", "mss_ctl")
    done = subprocess.run([binary, endpoint, *args], cwd=REPO, capture_output=True,
                          text=True, timeout=120)
    return done.returncode, (done.stdout + done.stderr).strip()


def parse_phases(text):
    phases = []
    for chunk in text.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        name, _, seconds = chunk.partition(":")
        name = name.strip()
        if name not in ENDPOINT_IMPAIRMENT:
            raise SystemExit(f"soak: unknown impairment profile {name!r}; "
                             f"known: {sorted(ENDPOINT_IMPAIRMENT)}")
        phases.append((name, float(seconds or "600")))
    if not phases:
        raise SystemExit("soak: SOAK_PHASES is empty")
    return phases


class Impairment:
    """Whichever of the two injection points this box can actually use."""

    def __init__(self, mode):
        self.netem = False
        self.reason = ""
        if mode == "off":
            self.reason = "SOAK_NETEM=off"
            return
        probe = subprocess.run([os.path.join(HERE, "netem.sh"), "probe"],
                               capture_output=True, text=True)
        if probe.returncode == 0:
            self.netem = True
            self.reason = "tc netem on the tap link"
            return
        self.reason = (probe.stdout + probe.stderr).strip().replace("\n", "; ")
        if mode == "on":
            raise SystemExit(f"soak: SOAK_NETEM=on but netem is unusable: {self.reason}")

    def apply(self, profile):
        if not self.netem:
            return
        done = subprocess.run([os.path.join(HERE, "netem.sh"), "apply", profile],
                              capture_output=True, text=True)
        say(f"netem {profile}: rc={done.returncode} {done.stdout.strip()}")

    def clear(self):
        if not self.netem:
            return
        subprocess.run([os.path.join(HERE, "netem.sh"), "clear"],
                       capture_output=True, text=True)

    def caller_env(self, profile):
        if self.netem:
            return {}
        return dict(ENDPOINT_IMPAIRMENT[profile])


class Phases:
    def __init__(self, phases):
        self.phases = phases
        self.total = sum(seconds for _name, seconds in phases)
        self.started = time.monotonic()

    def current(self):
        elapsed = time.monotonic() - self.started
        for name, seconds in self.phases:
            if elapsed < seconds:
                return name
            elapsed -= seconds
        return self.phases[-1][0]

    def finished(self):
        return time.monotonic() - self.started >= self.total


def one_call(index, sequence, pod, phase, impairment):
    """One call, cradle to grave: dial, tap, hold, untap, hang up.

    The order at each end matters. The session is created only once the call is
    answered and media is flowing, and destroyed TEARDOWN_LEAD seconds before
    the caller hangs up -- so the tap never outlives the audio, and the 10 s
    audio-flow watchdog never sees a leg it should call stalled.
    """
    call_id = f"soak-{STAMP}-{index}-{sequence}"
    from_tag = f"soak{index}x{sequence}"
    caller_log = os.path.join(OUT, f"soak-{STAMP}-caller-{index}.log")
    record = {"call_id": call_id, "pod": pod["name"], "phase": phase,
              "started": time.time(), "tapped": False, "errors": []}

    environment = dict(
        os.environ,
        SIP_PORT=str(SIP_PORT_BASE + index * 2),
        RTP_PORT=str(RTP_PORT_BASE + index * 2),
        CALL_ID=call_id,
        FROM_TAG=from_tag,
        RTP_SSRC=str(0x50000000 + index * 0x10000 + (sequence & 0xFFFF)),
        EAR_DIR=OUT,
        WRITE_EARS="0",
        CALL_SECONDS=str(CALL_SECONDS),
        TALK_EVERY=TALK_EVERY,
        DIAL=DIAL,
        IMPAIR_LOSS="0", IMPAIR_REORDER="0", IMPAIR_JITTER_MS="0",
        IMPAIR_DUPLICATE="0",
    )
    environment.update(impairment.caller_env(phase))

    with open(caller_log, "a") as sink:
        sink.write(f"\n=== {call_id} phase={phase} pod={pod['name']} ===\n")
        sink.flush()
        caller = subprocess.Popen([sys.executable, "host_test_caller.py"], cwd=HERE,
                                  env=environment, stdout=sink,
                                  stderr=subprocess.STDOUT)

    answered = False
    deadline = time.monotonic() + ANSWER_TIMEOUT
    while time.monotonic() < deadline and not STOP.is_set():
        if caller.poll() is not None:
            break
        with open(caller_log) as reading:
            if "answered; sending rtp" in reading.read().rsplit(f"=== {call_id}", 1)[-1]:
                answered = True
                break
        time.sleep(0.5)
    if not answered:
        record["errors"].append("never answered")
        violation(f"{call_id}: the call was never answered (see {caller_log})")
        caller.terminate()
        caller.wait(timeout=10)
        record["ended"] = time.time()
        return record

    time.sleep(1.0)
    created = False
    code, output = mss_ctl(pod["control"], "create", call_id, call_id, from_tag)
    if code != 0:
        record["errors"].append(f"create rc={code}: {output}")
        violation(f"{call_id}: create failed on pod {pod['name']}: {output}")
    else:
        created = True
        consumer = f"ws://{CONSUMER_HOST}:{CONSUMER_PORT}/ws"
        code, output = mss_ctl(pod["control"], "attach", call_id, consumer,
                               "soakmeter", "authoritative")
        if code != 0:
            record["errors"].append(f"attach rc={code}: {output}")
            violation(f"{call_id}: attach failed on pod {pod['name']}: {output}")
        else:
            record["tapped"] = True

    hold = max(0.0, CALL_SECONDS - TEARDOWN_LEAD - (time.time() - record["started"]))
    STOP.wait(hold)

    if created:
        code, output = mss_ctl(pod["control"], "destroy", call_id)
        if code != 0:
            record["errors"].append(f"destroy rc={code}: {output}")
            violation(f"{call_id}: destroy failed on pod {pod['name']}: {output}")

    try:
        caller.wait(timeout=CALL_SECONDS + 30)
    except subprocess.TimeoutExpired:
        caller.terminate()
        record["errors"].append("caller had to be terminated")
    record["ended"] = time.time()
    return record


def slot(index, phases, impairment):
    pod = PODS[index % len(PODS)]
    sequence = 0
    while not STOP.is_set() and not phases.finished():
        sequence += 1
        phase = phases.current()
        say(f"slot {index}: call {sequence} on pod {pod['name']}, profile {phase}")
        record = one_call(index, sequence, pod, phase, impairment)
        with LOG_LOCK:
            CALL_RECORDS.append(record)
        if STOP.is_set():
            break
        time.sleep(2.0)


def assert_scrape(pod, baseline, previous, now_values, phase):
    label = f"pod {pod['name']}"
    delta = {name: now_values.get(name, 0.0) - baseline.get(name, 0.0)
             for name in COUNTERS}

    if now_values.get("mss_legs_stalled", 0.0) != 0.0:
        violation(f"{label}: mss_legs_stalled = "
                  f"{now_values['mss_legs_stalled']:.0f} during phase {phase}")
    if delta["mss_consumer_dropped_oldest_total"] > MAX_DROPPED_OLDEST:
        violation(f"{label}: dropped_oldest grew by "
                  f"{delta['mss_consumer_dropped_oldest_total']:.0f} "
                  f"(bound {MAX_DROPPED_OLDEST})")
    for name in ("mss_events_failed_total", "mss_events_abandoned_total",
                 "mss_events_dropped_oldest_total", "mss_events_dropped_total",
                 "mss_events_outbox_dropped_total", "mss_ingest_recv_errors_total",
                 "mss_registry_lost_total", "mss_registry_failed_total",
                 "mss_ingest_unparsable_total"):
        if delta[name] > 0:
            violation(f"{label}: {name} grew by {delta[name]:.0f}")
    if previous is not None and now_values.get("mss_sessions_live", 0.0) > 0:
        moved = (now_values.get("mss_consumer_delivered_total", 0.0)
                 - previous.get("mss_consumer_delivered_total", 0.0))
        if moved <= 0:
            violation(f"{label}: {now_values['mss_sessions_live']:.0f} session(s) "
                      f"live but the consumer was fed 0 frames this interval")
    return delta


def scrape_all(baseline, previous, phase):
    row = {"at": time.time(), "phase": phase, "pods": {}}
    for pod in PODS:
        values, error = scrape(pod["metrics"])
        if values is None:
            violation(f"pod {pod['name']}: /metrics did not answer ({error})")
            continue
        rss = pod_rss_kb(pod["container"])
        if rss is None:
            violation(f"pod {pod['name']}: no mediaserverd process found in "
                      f"{pod['container']}")
        delta = assert_scrape(pod, baseline[pod["name"]],
                              previous.get(pod["name"]), values, phase)
        # Attributed to the phase this scrape closed. A call that started in the
        # previous phase bleeds a little into this tally; the phases are minutes
        # long and the calls are minutes long, so the bleed is one call's worth.
        interval = previous.get(pod["name"], baseline[pod["name"]])
        tally = PHASE_TALLY.setdefault(phase, {name: 0.0 for name in PER_PHASE})
        for name in PER_PHASE:
            tally[name] += values.get(name, 0.0) - interval.get(name, 0.0)
        row["pods"][pod["name"]] = {
            "rss_kb": rss,
            "gauges": {name: values.get(name, 0.0) for name in GAUGES},
            "delta": delta,
        }
        previous[pod["name"]] = values
        say(f"pod {pod['name']} [{phase}] rss={rss}kB "
            f"sessions={values.get('mss_sessions_live', 0):.0f} "
            f"legs={values.get('mss_legs_live', 0):.0f} "
            f"stalled={values.get('mss_legs_stalled', 0):.0f} "
            f"delivered+{delta['mss_consumer_delivered_total']:.0f} "
            f"datagrams+{delta['mss_ingest_datagrams_total']:.0f} "
            f"lost+{delta['mss_jitter_lost_total']:.0f} "
            f"concealed+{delta['mss_ingest_frames_concealed_total']:.0f} "
            f"late+{delta['mss_jitter_late_drops_total']:.0f} "
            f"dup+{delta['mss_jitter_duplicates_total']:.0f} "
            f"silence_gaps+{delta['mss_jitter_silence_gaps_total']:.0f} "
            f"underruns+{delta['mss_ingest_underruns_total']:.0f} "
            f"dropped_oldest+{delta['mss_consumer_dropped_oldest_total']:.0f} "
            f"events {delta['mss_events_accepted_total']:.0f}/"
            f"{delta['mss_events_published_total']:.0f} "
            f"failed+{delta['mss_events_failed_total']:.0f}")
    return row


def preflight():
    listing = docker("ps", "--format", "{{.Names}}").stdout.split()
    if SPIKE in listing:
        raise SystemExit(f"soak: the Phase-0 spike container {SPIKE} is running; "
                         "it would tap the same calls. Stop it first.")
    for pod in PODS:
        if pod["container"] not in listing:
            raise SystemExit(f"soak: {pod['container']} is not running")
        values, error = scrape(pod["metrics"])
        if values is None:
            raise SystemExit(f"soak: {pod['metrics']} does not answer ({error}); "
                             "is MSS_METRICS_LISTEN published?")
        if values.get("mss_sessions_live", 0.0) != 0.0:
            say(f"warning: pod {pod['name']} already holds "
                f"{values['mss_sessions_live']:.0f} session(s)")
    binary = os.path.join(REPO, "target", "debug", "examples", "mss_ctl")
    say("building mss_ctl so nothing compiles mid-soak")
    build = subprocess.run(["cargo", "build", "--quiet", "-p", "control-api",
                            "--examples"], cwd=REPO, capture_output=True, text=True)
    if build.returncode != 0 or not os.path.exists(binary):
        raise SystemExit(f"soak: cannot build mss_ctl: {build.stderr.strip()}")


def main():
    global LOG
    os.makedirs(OUT, exist_ok=True)
    LOG = open(os.path.join(OUT, f"soak-{STAMP}.log"), "w")
    phases = parse_phases(os.environ.get("SOAK_PHASES", DEFAULT_PHASES))
    total = sum(seconds for _n, seconds in phases)

    def interrupt(_signum, _frame):
        say("interrupted; winding down")
        STOP.set()

    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGTERM, interrupt)

    preflight()
    impairment = Impairment(NETEM_MODE)
    if impairment.netem:
        say("impairment injection: tc netem on the tap link")
    else:
        say("impairment injection: endpoint-side, upstream of rtpengine "
            f"(netem unusable: {impairment.reason})")
    say(f"{CALLS} concurrent calls of {CALL_SECONDS:.0f}s, "
        f"{total:.0f}s total, phases {phases}, scraping every {SCRAPE_SECONDS:.0f}s")

    consumer_log = os.path.join(OUT, f"soak-{STAMP}-consumer.log")
    consumer = subprocess.Popen(
        [sys.executable, os.path.join(HERE, "gap_consumer.py")],
        env=dict(os.environ, GAP_STAMP=f"soak-{STAMP}", OUT_DIR=OUT,
                 GAP_PORT=str(CONSUMER_PORT), GAP_BY_CALL="1",
                 GAP_KEEP_AUDIO="0", GAP_JOURNAL="0"),
        stdout=open(consumer_log, "w"), stderr=subprocess.STDOUT)
    time.sleep(1.5)
    if consumer.poll() is not None:
        raise SystemExit(f"soak: the gap consumer died at once; see {consumer_log}")

    baseline = {}
    for pod in PODS:
        values, _error = scrape(pod["metrics"])
        baseline[pod["name"]] = values or {}
        say(f"pod {pod['name']} baseline rss={pod_rss_kb(pod['container'])}kB")
    rss_first = {pod["name"]: pod_rss_kb(pod["container"]) for pod in PODS}

    walk = Phases(phases)
    applied = None
    workers = []
    for index in range(CALLS):
        worker = threading.Thread(target=slot, args=(index, walk, impairment),
                                  daemon=True)
        worker.start()
        workers.append(worker)
        time.sleep(3.0)

    previous = {}
    next_scrape = time.monotonic() + SCRAPE_SECONDS
    try:
        while not walk.finished() and not STOP.is_set():
            phase = walk.current()
            if phase != applied:
                impairment.apply(phase)
                applied = phase
                say(f"=== phase {phase} ===")
            if time.monotonic() >= next_scrape:
                SCRAPES.append(scrape_all(baseline, previous, phase))
                next_scrape += SCRAPE_SECONDS
            time.sleep(1.0)
    finally:
        # Not STOP.set() on a clean finish: the slots already know the phases are
        # over and will stop after the call each is holding, and letting that call
        # end the way every other one did keeps the last teardown honest.
        say("phases done; waiting for the last calls to hang up")
        for worker in workers:
            worker.join(timeout=CALL_SECONDS + 60)
        STOP.set()
        impairment.clear()
        SCRAPES.append(scrape_all(baseline, previous, "drain"))
        consumer.send_signal(signal.SIGTERM)
        try:
            consumer.wait(timeout=180)
        except subprocess.TimeoutExpired:
            consumer.kill()

    for pod in PODS:
        values, _error = scrape(pod["metrics"])
        if values is None:
            continue
        for name in ("mss_sessions_live", "mss_legs_live", "mss_consumers_live"):
            if values.get(name, 0.0) != 0.0:
                violation(f"pod {pod['name']}: {name} = {values[name]:.0f} after "
                          "every call hung up -- a session or leg leaked")
    left = docker("exec", os.environ.get("REDIS", "mss-microsip-redis-1"),
                  "redis-cli", "--raw", "smembers", "mss:sessions").stdout.split()
    if left:
        violation(f"the Redis registry still holds {len(left)} session(s): "
                  f"{' '.join(left[:8])}")
    else:
        say("the Redis registry is empty, as it was before the run")

    for phase, tally in PHASE_TALLY.items():
        say(f"phase {phase}: " + " ".join(
            f"{name.removeprefix('mss_').removesuffix('_total')}="
            f"{value:.0f}" for name, value in tally.items()))

    rss_last = {pod["name"]: pod_rss_kb(pod["container"]) for pod in PODS}
    for pod in PODS:
        first, last = rss_first[pod["name"]], rss_last[pod["name"]]
        if first is None or last is None:
            continue
        ceiling = first * RSS_GROWTH + RSS_SLACK_KB
        say(f"pod {pod['name']} rss {first}kB -> {last}kB "
            f"(ceiling {ceiling:.0f}kB)")
        if last > ceiling:
            violation(f"pod {pod['name']}: RSS grew from {first}kB to {last}kB, "
                      f"past the {ceiling:.0f}kB ceiling -- suspect a leak")

    gaps = []
    summary_path = os.path.join(OUT, f"gap-soak-{STAMP}-summary.json")
    if os.path.exists(summary_path):
        with open(summary_path) as reading:
            for track in json.load(reading).get("tracks", []):
                gaps.append((track["longest_arrival_gap_ms"], track["track"],
                             track["frames"]))
        gaps.sort(reverse=True)
        for value, track, frames in gaps[:5]:
            say(f"widest arrival gaps: {track} {value:.0f}ms over {frames} frames")
    else:
        say(f"no consumer summary at {summary_path}")

    tapped = sum(1 for record in CALL_RECORDS if record["tapped"])
    summary = {
        "stamp": STAMP,
        "calls_concurrent": CALLS,
        "call_seconds": CALL_SECONDS,
        "phases": phases,
        "impairment": "netem" if impairment.netem else "endpoint",
        "impairment_reason": impairment.reason,
        "sessions_attempted": len(CALL_RECORDS),
        "sessions_tapped": tapped,
        "rss_first_kb": rss_first,
        "rss_last_kb": rss_last,
        "per_phase": PHASE_TALLY,
        "widest_arrival_gaps_ms": gaps[:10],
        "scrapes": SCRAPES,
        "calls": CALL_RECORDS,
        "violations": VIOLATIONS,
    }
    path = os.path.join(OUT, f"soak-{STAMP}-summary.json")
    with open(path, "w") as writing:
        json.dump(summary, writing, indent=1)
    say(f"{tapped}/{len(CALL_RECORDS)} calls tapped; summary in {path}")

    if VIOLATIONS:
        say(f"=== SOAK FAILED: {len(VIOLATIONS)} assertion(s) ===")
        for entry in VIOLATIONS:
            say(f"  * {entry}")
        return 1
    say("=== SOAK GREEN: every assertion held ===")
    return 0


sys.exit(main())
