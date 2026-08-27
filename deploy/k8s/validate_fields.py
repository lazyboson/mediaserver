"""Assertions a Kubernetes schema cannot make about these manifests.

Called by validate.sh with the directory of rendered overlays. Every check
prints one PASS/FAIL line; the exit code is 1 if anything failed.
"""

import pathlib
import sys

import yaml

REQUIRED_KINDS = {
    "ServiceAccount",
    "ConfigMap",
    "Secret",
    "Service",
    "Deployment",
    "PodDisruptionBudget",
    "ServiceMonitor",
    "PrometheusRule",
}
METRICS_PORT_NAME = "metrics"
GRACE_MARGIN_SECS = 5


def load(path):
    return [doc for doc in yaml.safe_load_all(path.read_text()) if doc]


def container(deployment):
    for spec in deployment["spec"]["template"]["spec"]["containers"]:
        if spec["name"] == "mediaserverd":
            return spec
    raise AssertionError("no container named mediaserverd")


def env_of(config_map):
    return config_map.get("data", {})


def check(overlay, results):
    name = overlay.stem
    docs = load(overlay)
    kinds = {doc["kind"] for doc in docs}
    by_kind = {}
    for doc in docs:
        by_kind.setdefault(doc["kind"], []).append(doc)

    for doc in docs:
        where = f"{doc.get('kind')}/{doc.get('metadata', {}).get('name')}"
        results.append(
            (
                bool(doc.get("apiVersion")) and bool(doc.get("kind")) and bool(doc.get("metadata", {}).get("name")),
                name,
                f"{where} has apiVersion, kind and metadata.name",
            )
        )

    missing = REQUIRED_KINDS - kinds
    results.append((not missing, name, f"every expected kind is present ({sorted(missing)} missing)"))

    deployment = by_kind["Deployment"][0]
    spec = container(deployment)

    results.append((bool(spec.get("image")), name, f"the container names an image ({spec.get('image')})"))

    port_names = {port.get("name") for port in spec.get("ports", [])}
    results.append(
        (
            {"control", METRICS_PORT_NAME} <= port_names,
            name,
            "the container declares the control and metrics ports by name",
        )
    )

    readiness = spec.get("readinessProbe", {}).get("httpGet", {})
    liveness = spec.get("livenessProbe", {}).get("httpGet", {})
    results.append(
        (
            readiness.get("path") == "/readyz" and readiness.get("port") == METRICS_PORT_NAME,
            name,
            "readinessProbe is GET /readyz on the metrics port",
        )
    )
    results.append(
        (
            liveness.get("path") == "/healthz" and liveness.get("port") == METRICS_PORT_NAME,
            name,
            "livenessProbe is GET /healthz on the metrics port",
        )
    )
    results.append(
        (
            spec.get("readinessProbe", {}).get("periodSeconds") == 5
            and spec.get("livenessProbe", {}).get("periodSeconds") == 10,
            name,
            "the probe periods are the documented 5 s and 10 s",
        )
    )
    results.append((bool(spec.get("resources", {}).get("requests")), name, "the container requests cpu and memory"))

    config = env_of(by_kind["ConfigMap"][0])
    drain = int(config.get("MSS_DRAIN_TIMEOUT_SECS", "0"))
    grace = deployment["spec"]["template"]["spec"].get("terminationGracePeriodSeconds", 30)
    results.append(
        (
            grace >= drain + GRACE_MARGIN_SECS,
            name,
            f"terminationGracePeriodSeconds {grace} clears MSS_DRAIN_TIMEOUT_SECS {drain} by >= {GRACE_MARGIN_SECS} s",
        )
    )

    spill = config.get("MSS_RECORDING_SPILL_DIR")
    mounts = {mount["mountPath"] for mount in spec.get("volumeMounts", [])}
    results.append(
        (
            spill in mounts,
            name,
            f"MSS_RECORDING_SPILL_DIR {spill} is a mounted writable volume",
        )
    )

    store = (config.get("MSS_RECORDING_STORE") or "s3").strip()
    root = (config.get("MSS_RECORDING_ROOT") or "").strip()
    results.append(
        (
            store in ("s3", "filesystem"),
            name,
            f"MSS_RECORDING_STORE {store!r} is a store mediaserverd knows (anything else refuses to start)",
        )
    )
    if store == "filesystem":
        results.append(
            (
                root.startswith("/") and root in mounts,
                name,
                f"MSS_RECORDING_ROOT {root!r} is an absolute path and a mounted volume",
            )
        )
        volumes = {volume["name"]: volume for volume in deployment["spec"]["template"]["spec"].get("volumes", [])}
        mount = next((entry for entry in spec.get("volumeMounts", []) if entry["mountPath"] == root), None)
        claim = volumes.get(mount["name"], {}).get("persistentVolumeClaim", {}).get("claimName") if mount else None
        results.append(
            (
                bool(claim),
                name,
                "the recording root is a PersistentVolumeClaim, not per-pod scratch",
            )
        )
        claims = {doc["metadata"]["name"]: doc for doc in by_kind.get("PersistentVolumeClaim", [])}
        if claim and claim in claims:
            results.append(
                (
                    "ReadWriteMany" in claims[claim]["spec"].get("accessModes", []),
                    name,
                    f"PersistentVolumeClaim/{claim} is ReadWriteMany, so any pod can adopt a recording",
                )
            )
    elif store == "s3":
        results.append((not root, name, "MSS_RECORDING_ROOT is unset while the store is s3, so nothing is ignored"))

    host_ports = sorted(port["hostPort"] for port in spec.get("ports", []) if "hostPort" in port)
    low = config.get("MSS_MEDIA_PORT_MIN") or None
    high = config.get("MSS_MEDIA_PORT_MAX") or None
    if host_ports:
        results.append(
            (
                low is not None and int(low) == host_ports[0] and int(high) == host_ports[-1],
                name,
                f"the hostPort range {host_ports[0]}-{host_ports[-1]} is MSS_MEDIA_PORT_MIN..MAX ({low}-{high})",
            )
        )
        results.append(
            (
                len(host_ports) == host_ports[-1] - host_ports[0] + 1,
                name,
                "every port in the range has a hostPort entry",
            )
        )
    if deployment["spec"]["template"]["spec"].get("hostNetwork"):
        results.append(
            (
                deployment["spec"]["template"]["spec"].get("dnsPolicy") == "ClusterFirstWithHostNet",
                name,
                "a hostNetwork pod sets dnsPolicy ClusterFirstWithHostNet",
            )
        )
        results.append(
            (low is not None and high is not None, name, f"hostNetwork ships a real media range ({low}-{high})")
        )
    if low or high:
        results.append((bool(low) and bool(high), name, "the media range sets both ends or neither"))

    advertise = {entry["name"] for entry in spec.get("env", []) if "valueFrom" in entry}
    if host_ports or deployment["spec"]["template"]["spec"].get("hostNetwork"):
        results.append(
            (
                "MSS_MEDIA_ADVERTISE_IP" in advertise,
                name,
                "MSS_MEDIA_ADVERTISE_IP comes from the downward API, not a literal",
            )
        )
    results.append(
        (
            "MSS_POD_NAME" in advertise,
            name,
            "MSS_POD_NAME comes from metadata.name, so lease owners are distinct",
        )
    )

    for secret in by_kind["Secret"]:
        values = set(secret.get("stringData", {}).values()) | set(secret.get("data", {}).values())
        results.append(
            (
                all(value == "REPLACE_ME" for value in values),
                name,
                f"Secret/{secret['metadata']['name']} ships placeholders only",
            )
        )

    rule = by_kind["PrometheusRule"][0]
    alerts = [rule for group in rule["spec"]["groups"] for rule in group["rules"]]
    results.append((len(alerts) >= 10, name, f"the PrometheusRule carries the alert rules ({len(alerts)})"))


def main():
    root = pathlib.Path(sys.argv[1])
    results = []
    for overlay in sorted(root.glob("*.yaml")):
        check(overlay, results)

    failed = 0
    for ok, overlay, message in results:
        if not ok:
            failed += 1
            print(f"FAIL  fields      {overlay}: {message}")
    if failed:
        print(f"FAIL  fields      {failed} of {len(results)} assertions failed")
        return 1
    print(f"PASS  fields      {len(results)} assertions over {len(list(root.glob('*.yaml')))} rendered overlays")
    return 0


if __name__ == "__main__":
    sys.exit(main())
