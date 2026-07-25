#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${LSM_KIND_CLUSTER:-lsm-cluster}"
NAMESPACE="lsm-cluster"
IMAGE="${LSM_KIND_IMAGE:-lsmengine-server:kind}"
KEEP="${LSM_KIND_KEEP:-0}"
GATEWAY_URL="http://lsm-gateway:8090"

kubectl() {
  command kubectl --context "kind-$CLUSTER_NAME" "$@"
}

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

retry_kubectl_lsm_contains() {
  local needle="$1"
  shift
  local deadline=$((SECONDS + 60))
  local output=""
  until output="$(kubectl_lsm "$@" 2>&1)" && [[ "$output" == *"$needle"* ]]; do
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for lsmctl output to contain: $needle" >&2
      echo "$output" >&2
      dump_diagnostics
      return 1
    fi
    sleep 1
  done
  printf '%s\n' "$output"
}

node_endpoint_args() {
  printf '%s\n' \
    --node-endpoint "lsm-cluster-0=http://lsm-cluster-0.lsm-cluster:8080" \
    --node-endpoint "lsm-cluster-1=http://lsm-cluster-1.lsm-cluster:8080" \
    --node-endpoint "lsm-cluster-2=http://lsm-cluster-2.lsm-cluster:8080"
}

wait_for_gateway_status() {
  local deadline=$((SECONDS + 90))
  local output=""
  until output="$(kubectl_lsm gateway-status --addr "$GATEWAY_URL")" \
    && [[ "$output" == *"ready=true"* ]] \
    && [[ "$output" == *"reachable_nodes=3"* ]] \
    && [[ "$output" == *"read_mode=leader"* ]] \
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
  printf '%s\n' "$output"
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

seq_from_output() {
  local output="$1"
  local seq
  seq="$(awk -F= '/^seq=/{print $2; exit}' <<<"$output")"
  if [[ ! "$seq" =~ ^[1-9][0-9]*$ ]]; then
    echo "write did not return a positive sequence" >&2
    echo "$output" >&2
    return 1
  fi
  printf '%s\n' "$seq"
}

wait_cluster_applied() {
  local seq="$1"
  local output
  output="$(kubectl_lsm wait-cluster $(node_endpoint_args) --timeout 90s --min-applied-index "$seq")"
  require_contains "$output" "ready=true"
  require_contains "$output" "ready_nodes=3"
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

put_output="$(retry_kubectl_lsm_contains "state=committed" put --addr "$GATEWAY_URL" --key kind --value ok)"
require_contains "$put_output" "state=committed"
put_seq="$(seq_from_output "$put_output")"
wait_cluster_applied "$put_seq"

get_output="$(retry_kubectl_lsm_contains "found=true" get --addr "$GATEWAY_URL" --key kind)"
require_contains "$get_output" "found=true"
require_contains "$get_output" "value=ok"

follower_output="$(retry_kubectl_lsm_contains "found=true" get --addr http://lsm-cluster-1.lsm-cluster:8080 --key kind)"
require_contains "$follower_output" "found=true"
require_contains "$follower_output" "value=ok"

range_output="$(retry_kubectl_lsm_contains "key=kind" range --addr "$GATEWAY_URL" --start kind --end kine --limit 1)"
require_contains "$range_output" "key=kind"
require_contains "$range_output" "value=ok"

delete_output="$(retry_kubectl_lsm_contains "state=committed" delete --addr "$GATEWAY_URL" --key kind)"
require_contains "$delete_output" "state=committed"
delete_seq="$(seq_from_output "$delete_output")"
wait_cluster_applied "$delete_seq"

missing_output="$(retry_kubectl_lsm_contains "found=false" get --addr "$GATEWAY_URL" --key kind)"
require_contains "$missing_output" "found=false"

follower_missing_output="$(retry_kubectl_lsm_contains "found=false" get --addr http://lsm-cluster-2.lsm-cluster:8080 --key kind)"
require_contains "$follower_missing_output" "found=false"

echo "kind gateway cluster smoke passed"
