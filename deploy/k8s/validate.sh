#!/bin/sh
# Checks these manifests as far as they can be checked without a cluster.
#
#   ./validate.sh
#
# Three layers, each optional and each announced:
#   1. `kubectl kustomize` (or `kustomize build`) renders every overlay -- this
#      catches a bad patch target, a missing file, a YAML error.
#   2. `kubeconform` validates the rendered objects against the Kubernetes
#      schemas, if it is installed.
#   3. a python pass asserts the things a schema does not: that mediaserverd's
#      container has both probes on the metrics port, that
#      terminationGracePeriodSeconds clears MSS_DRAIN_TIMEOUT_SECS, that the
#      hostPort entries and MSS_MEDIA_PORT_MIN..MAX describe the same range, that
#      a filesystem recording store writes to a mounted RWX volume, and that no
#      Secret ships a real-looking value.
#
# `kubectl apply --dry-run=client` is NOT one of them: it needs a live API
# server for its schemas, so it fails with a connection error and proves nothing.
set -eu

here=$(dirname "$0")
overlays="base overlays/hostport overlays/hostnetwork overlays/filesystem-recording"
rendered=$(mktemp -d)
trap 'rm -rf "$rendered"' EXIT
status=0

if command -v kubectl >/dev/null 2>&1; then
  build="kubectl kustomize"
elif command -v kustomize >/dev/null 2>&1; then
  build="kustomize build"
else
  echo "validate: neither kubectl nor kustomize is installed; cannot render" >&2
  exit 2
fi

for overlay in $overlays; do
  out="$rendered/$(echo "$overlay" | tr / -).yaml"
  if $build "$here/$overlay" > "$out" 2>"$out.err"; then
    echo "PASS  render      $overlay ($(grep -c '^kind:' "$out") objects, $build)"
  else
    echo "FAIL  render      $overlay"
    sed 's/^/                /' "$out.err"
    status=1
  fi
done
[ "$status" -eq 0 ] || exit "$status"

if command -v kubeconform >/dev/null 2>&1; then
  for out in "$rendered"/*.yaml; do
    if kubeconform -strict -ignore-missing-schemas -summary "$out" >/dev/null 2>&1; then
      echo "PASS  schema      $(basename "$out") (kubeconform -strict)"
    else
      echo "FAIL  schema      $(basename "$out")"
      kubeconform -strict -ignore-missing-schemas "$out" 2>&1 | sed 's/^/                /'
      status=1
    fi
  done
else
  echo "SKIP  schema      no kubeconform on PATH; install it to validate against the"
  echo "                  Kubernetes schemas (kubectl --dry-run=client needs a live server)"
fi

if python3 -c 'import yaml' 2>/dev/null; then
  python3 "$here/validate_fields.py" "$rendered" || status=1
else
  echo "SKIP  fields      python3 has no yaml module; pip install pyyaml to assert"
  echo "                  probes, grace period and the port range"
fi

[ "$status" -eq 0 ] && echo "validate: everything checked above passed"
exit "$status"
