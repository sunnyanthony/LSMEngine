#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${LSM_KIND_CLUSTER:-lsm-cluster}"
NAMESPACE="lsm-cluster"
IMAGE="${LSM_KIND_IMAGE:-lsmengine-server:kind}"
KEEP="${LSM_KIND_KEEP:-0}"
GATEWAY_URL="http://lsm-gateway:8090"

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
  kubectl -n "$NAMESPACE" get svc >&2 || true
  kubectl -n "$NAMESPACE" logs statefulset/lsm-cluster --tail=100 >&2 || true
  kubectl -n "$NAMESPACE" logs deployment/lsm-gateway --tail=100 >&2 || true
}

kubectl_lsm() {
  kubectl -n "$NAMESPACE" exec pod/lsm-cluster-0 -- /usr/local/bin/lsmctl "$@"
}

wait_for_gateway_status() {
  local deadline=$((SECONDS + 90))
  local output=""
  until output="$(kubectl_lsm gateway-status --addr "$GATEWAY_URL")" \
    && [[ "$output" == *"ready=true"* ]] \
    && [[ "$output" == *"reachable_nodes=3"* ]] \
    && [[ "$output" == *"write_leader=lsm-cluster-"* ]]; do
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for gateway-status at $GATEWAY_URL" >&2
      if [[ -n "$output" ]]; then
        echo "$output" >&2
      fi
      dump_diagnostics
      return 1
    fi
    sleep 1
  done
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
kubectl -n "$NAMESPACE" set image deployment/lsm-gateway gateway="$IMAGE"
kubectl -n "$NAMESPACE" rollout status statefulset/lsm-cluster --timeout=180s
kubectl -n "$NAMESPACE" rollout status deployment/lsm-gateway --timeout=180s
wait_for_gateway_status

put_output="$(kubectl_lsm put --addr "$GATEWAY_URL" --key kind --value ok)"
require_contains "$put_output" "state=committed"

get_output="$(kubectl_lsm get --addr "$GATEWAY_URL" --key kind)"
require_contains "$get_output" "found=true"
require_contains "$get_output" "value=ok"

follower_output="$(kubectl_lsm get --addr http://lsm-cluster-1.lsm-cluster.lsm-cluster.svc.cluster.local:8080 --key kind)"
require_contains "$follower_output" "found=true"
require_contains "$follower_output" "value=ok"

range_output="$(kubectl_lsm range --addr "$GATEWAY_URL" --start kind --end kine --limit 1)"
require_contains "$range_output" "key=kind"
require_contains "$range_output" "value=ok"

delete_output="$(kubectl_lsm delete --addr "$GATEWAY_URL" --key kind)"
require_contains "$delete_output" "state=committed"

missing_output="$(kubectl_lsm get --addr "$GATEWAY_URL" --key kind)"
require_contains "$missing_output" "found=false"

follower_missing_output="$(kubectl_lsm get --addr http://lsm-cluster-2.lsm-cluster.lsm-cluster.svc.cluster.local:8080 --key kind)"
require_contains "$follower_missing_output" "found=false"

echo "kind gateway cluster smoke passed"
