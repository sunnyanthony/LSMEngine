#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${LSM_KIND_CLUSTER:-lsm-cluster}"
NAMESPACE="lsm-cluster"
IMAGE="${LSM_KIND_IMAGE:-lsmengine-server:kind}"
KEEP="${LSM_KIND_KEEP:-0}"

pods=(lsm-cluster-0 lsm-cluster-1 lsm-cluster-2)

require_cmd() {
  if ! command -v "$1" >/dev/null; then
    echo "$1 is required" >&2
    exit 1
  fi
}

cleanup() {
  if [[ "$KEEP" == "1" ]]; then
    return
  fi
  kubectl delete namespace "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

dump_diagnostics() {
  kubectl -n "$NAMESPACE" get pods -o wide >&2 || true
  kubectl -n "$NAMESPACE" get pvc >&2 || true
  kubectl -n "$NAMESPACE" describe statefulset/lsm-cluster >&2 || true
  kubectl -n "$NAMESPACE" logs statefulset/lsm-cluster --tail=120 >&2 || true
}

kubectl_lsm() {
  local pod="$1"
  shift
  kubectl -n "$NAMESPACE" exec "pod/$pod" -- /usr/local/bin/lsmctl "$@"
}

wait_for_pod_replacement() {
  local pod="$1"
  local old_uid="$2"
  local deadline=$((SECONDS + 180))
  local uid
  while (( SECONDS < deadline )); do
    uid="$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
    if [[ -n "$uid" && "$uid" != "$old_uid" ]]; then
      kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$pod" --timeout=120s
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for $pod replacement" >&2
  dump_diagnostics
  return 1
}

wait_for_value() {
  local pod="$1"
  local key="$2"
  local value="$3"
  local deadline=$((SECONDS + 90))
  local output
  while (( SECONDS < deadline )); do
    if output="$(kubectl_lsm "$pod" get --addr "http://$pod.lsm-cluster:8080" --key "$key" 2>&1)" &&
      [[ "$output" == *"found=true"* ]] &&
      [[ "$output" == *"value=$value"* ]]; then
      return 0
    fi
    sleep 1
  done
  echo "timed out reading $key=$value from $pod" >&2
  dump_diagnostics
  return 1
}

put_until_committed() {
  local pod="$1"
  local key="$2"
  local value="$3"
  local deadline=$((SECONDS + 90))
  local output
  while (( SECONDS < deadline )); do
    if output="$(kubectl_lsm "$pod" put --addr "http://$pod.lsm-cluster:8080" --key "$key" --value "$value" 2>&1)" &&
      [[ "$output" == *"state=committed"* ]]; then
      return 0
    fi
    sleep 1
  done
  echo "timed out writing $key=$value through $pod" >&2
  dump_diagnostics
  return 1
}

require_cmd docker
require_cmd kind
require_cmd kubectl

if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind create cluster --name "$CLUSTER_NAME"
fi

docker build -f "$ROOT_DIR/docker/Dockerfile.server" -t "$IMAGE" "$ROOT_DIR"
kind load docker-image "$IMAGE" --name "$CLUSTER_NAME"

kubectl apply -k "$ROOT_DIR/examples/kind-cluster"
kubectl -n "$NAMESPACE" set image statefulset/lsm-cluster "lsm=$IMAGE"
kubectl -n "$NAMESPACE" rollout status statefulset/lsm-cluster --timeout=180s

put_until_committed lsm-cluster-0 restart durable

for pod in "${pods[@]}"; do
  wait_for_value "$pod" restart durable
done

for pod in "${pods[@]}"; do
  old_uid="$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.metadata.uid}')"
  kubectl -n "$NAMESPACE" delete pod "$pod" --wait=false
  wait_for_pod_replacement "$pod" "$old_uid"
  wait_for_value "$pod" restart durable
done

echo "kind persistent restart smoke passed"
