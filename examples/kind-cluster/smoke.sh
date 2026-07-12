#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${LSM_KIND_CLUSTER:-lsm-cluster}"
NAMESPACE="lsm-cluster"
IMAGE="${LSM_KIND_IMAGE:-lsmengine-server:kind}"
KEEP="${LSM_KIND_KEEP:-0}"

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

kubectl_lsm() {
  kubectl -n "$NAMESPACE" exec pod/lsm-cluster-0 -- /usr/local/bin/lsmctl "$@"
}

require_contains() {
  local haystack="$1"
  local needle="$2"
  if [[ "$haystack" != *"$needle"* ]]; then
    echo "expected output to contain: $needle" >&2
    echo "$haystack" >&2
    return 1
  fi
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
kubectl -n "$NAMESPACE" set image statefulset/lsm-cluster lsm="$IMAGE"
kubectl -n "$NAMESPACE" rollout status statefulset/lsm-cluster --timeout=180s

put_output="$(kubectl_lsm put --addr http://lsm-cluster-0.lsm-cluster:8080 --key kind --value ok)"
require_contains "$put_output" "state=committed"

get_output="$(kubectl_lsm get --addr http://lsm-cluster-1.lsm-cluster:8080 --key kind)"
require_contains "$get_output" "found=true"
require_contains "$get_output" "value=ok"

range_output="$(kubectl_lsm range --addr http://lsm-cluster-1.lsm-cluster:8080 --start kind --end kine --limit 1)"
require_contains "$range_output" "key=kind"
require_contains "$range_output" "value=ok"

delete_output="$(kubectl_lsm delete --addr http://lsm-cluster-0.lsm-cluster:8080 --key kind)"
require_contains "$delete_output" "state=committed"

missing_output="$(kubectl_lsm get --addr http://lsm-cluster-2.lsm-cluster:8080 --key kind)"
require_contains "$missing_output" "found=false"

echo "kind cluster smoke passed"
