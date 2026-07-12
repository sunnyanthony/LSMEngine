#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${LSM_KIND_CLUSTER:-lsm-cluster}"
NAMESPACE="lsm-cluster"
IMAGE="${LSM_KIND_IMAGE:-lsmengine-server:kind}"
KEEP="${LSM_KIND_KEEP:-0}"
NODE_URLS=(
  "http://lsm-cluster-0.lsm-cluster:8080"
  "http://lsm-cluster-1.lsm-cluster:8080"
  "http://lsm-cluster-2.lsm-cluster:8080"
)

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

eventually_lsmctl_write() {
  local command="$1"
  local key="$2"
  local value="${3:-}"
  local deadline=$((SECONDS + 60))
  local output=""
  while (( SECONDS < deadline )); do
    for url in "${NODE_URLS[@]}"; do
      if [[ "$command" == "put" ]]; then
        if output="$(kubectl_lsm put --addr "$url" --key "$key" --value "$value" 2>&1)" && [[ "$output" == *"state=committed"* ]]; then
          echo "$url"
          return
        fi
      elif [[ "$command" == "delete" ]]; then
        if output="$(kubectl_lsm delete --addr "$url" --key "$key" 2>&1)" && [[ "$output" == *"state=committed"* ]]; then
          echo "$url"
          return
        fi
      else
        echo "unsupported write command: $command" >&2
        return 1
      fi
    done
    sleep 1
  done
  echo "timed out waiting for lsmctl $command $key to commit" >&2
  echo "$output" >&2
  kubectl -n "$NAMESPACE" get pods >&2 || true
  kubectl -n "$NAMESPACE" logs statefulset/lsm-cluster --tail=80 >&2 || true
  return 1
}

eventually_lsmctl_get_contains() {
  local url="$1"
  local key="$2"
  local first="$3"
  local second="${4:-}"
  local deadline=$((SECONDS + 60))
  local output=""
  while (( SECONDS < deadline )); do
    if output="$(kubectl_lsm get --addr "$url" --key "$key" 2>&1)"; then
      if [[ "$output" == *"$first"* && ( -z "$second" || "$output" == *"$second"* ) ]]; then
        return
      fi
    fi
    sleep 1
  done
  echo "timed out waiting for lsmctl get $key from $url to contain: $first $second" >&2
  echo "$output" >&2
  kubectl -n "$NAMESPACE" get pods >&2 || true
  kubectl -n "$NAMESPACE" logs statefulset/lsm-cluster --tail=80 >&2 || true
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
kubectl -n "$NAMESPACE" set image statefulset/lsm-cluster lsm="$IMAGE"
kubectl -n "$NAMESPACE" rollout status statefulset/lsm-cluster --timeout=180s

write_url="$(eventually_lsmctl_write put kind ok)"
echo "committed put through $write_url"

eventually_lsmctl_get_contains "http://lsm-cluster-1.lsm-cluster:8080" kind "found=true" "value=ok"

range_output="$(kubectl_lsm range --addr http://lsm-cluster-1.lsm-cluster:8080 --start kind --end kine --limit 1)"
require_contains "$range_output" "key=kind"
require_contains "$range_output" "value=ok"

delete_url="$(eventually_lsmctl_write delete kind)"
echo "committed delete through $delete_url"

eventually_lsmctl_get_contains "http://lsm-cluster-2.lsm-cluster:8080" kind "found=false"

echo "kind cluster smoke passed"
