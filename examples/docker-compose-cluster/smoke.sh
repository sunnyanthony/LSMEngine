#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/docker-compose-cluster/docker-compose.yml"
PROJECT="${LSM_COMPOSE_PROJECT:-lsmengine-cluster}"
KEEP="${LSM_COMPOSE_KEEP:-0}"
LSMCTL_BIN="${LSMCTL_BIN:-}"

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

lsmctl() {
  if [[ -n "$LSMCTL_BIN" ]]; then
    "$LSMCTL_BIN" "$@"
    return
  fi
  (cd "$ROOT_DIR" && go run ./cmd/lsmctl "$@")
}

cleanup() {
  if [[ "$KEEP" == "1" ]]; then
    return
  fi
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_health() {
  local url="$1"
  local deadline=$((SECONDS + 60))
  until curl -fsS "$url/healthz" >/dev/null; do
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for $url/healthz" >&2
      compose ps >&2 || true
      compose logs --tail=80 >&2 || true
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

wait_for_cluster_missing() {
  local key="$1"
  local deadline=$((SECONDS + 60))
  local output
  while (( SECONDS < deadline )); do
    if output="$(lsmctl get --cluster $(node_endpoint_args) --key "$key" 2>&1)" &&
      [[ "$output" == *"found=false"* ]]; then
      printf '%s\n' "$output"
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for $key to be absent from cluster" >&2
  compose ps >&2 || true
  compose logs --tail=80 >&2 || true
  return 1
}

seq_from_output() {
  local output="$1"
  local seq
  seq="$(awk -F= '/^seq=/{print $2; exit}' <<<"$output")"
  if [[ -z "$seq" ]]; then
    echo "write did not return seq" >&2
    echo "$output" >&2
    return 1
  fi
  printf '%s\n' "$seq"
}

wait_cluster_applied() {
  local seq="$1"
  local output
  output="$(lsmctl wait-cluster $(node_endpoint_args) --timeout 60s --min-applied-index "$seq" --require-compatible)"
  require_contains "$output" "ready=true"
  require_contains "$output" "ready_nodes=3"
  require_contains "$output" "compatible=true"
}

node_endpoint_args() {
  printf '%s\n' \
    --node-endpoint "node-a=http://127.0.0.1:8080" \
    --node-endpoint "node-b=http://127.0.0.1:8081" \
    --node-endpoint "node-c=http://127.0.0.1:8082"
}

compose up -d --build

wait_for_health "http://127.0.0.1:8080"
wait_for_health "http://127.0.0.1:8081"
wait_for_health "http://127.0.0.1:8082"

wait_output="$(lsmctl wait-cluster $(node_endpoint_args) --timeout 60s --require-compatible)"
require_contains "$wait_output" "ready=true"
require_contains "$wait_output" "ready_nodes=3"
require_contains "$wait_output" "compatible=true"

status_output="$(lsmctl cluster-status $(node_endpoint_args))"
require_contains "$status_output" "compatibility=cluster_status:1,control_state:1,state_snapshot:1,raft_peer_message:1"

put_output="$(lsmctl put --cluster $(node_endpoint_args) --key compose --value ok)"
require_contains "$put_output" "state=committed"
put_seq="$(seq_from_output "$put_output")"
wait_cluster_applied "$put_seq"

cdc_status_output="$(lsmctl cdc-status --cluster $(node_endpoint_args))"
require_contains "$cdc_status_output" "node=node-a"
require_contains "$cdc_status_output" "node=node-b"
require_contains "$cdc_status_output" "node=node-c"
require_contains "$cdc_status_output" "source=memory"
require_contains "$cdc_status_output" "replay_on_restart=false"
require_contains "$cdc_status_output" "shard=users"

put_cdc_offset=$((put_seq > 0 ? put_seq - 1 : 0))
put_cdc_output="$(lsmctl cdc-events --addr http://127.0.0.1:8081 --shard users --offset "$put_cdc_offset" --limit 10)"
require_contains "$put_cdc_output" "offset=$put_seq"
require_contains "$put_cdc_output" "operation=put"
require_contains "$put_cdc_output" "key=compose"
require_contains "$put_cdc_output" "value=ok"

get_output="$(lsmctl get --addr http://127.0.0.1:8081 --key compose)"
require_contains "$get_output" "found=true"
require_contains "$get_output" "value=ok"

range_output="$(lsmctl range --addr http://127.0.0.1:8081 --start compose --end composf --limit 1)"
require_contains "$range_output" "key=compose"
require_contains "$range_output" "value=ok"

cluster_get_output="$(lsmctl get --cluster $(node_endpoint_args) --key compose)"
require_contains "$cluster_get_output" "found=true"
require_contains "$cluster_get_output" "value=ok"

cluster_range_output="$(lsmctl range --cluster $(node_endpoint_args) --start compose --end composf --limit 1)"
require_contains "$cluster_range_output" "key=compose"
require_contains "$cluster_range_output" "value=ok"

delete_output="$(lsmctl delete --cluster $(node_endpoint_args) --key compose)"
require_contains "$delete_output" "state=committed"
delete_seq="$(seq_from_output "$delete_output")"
wait_cluster_applied "$delete_seq"

delete_cdc_offset=$((delete_seq > 0 ? delete_seq - 1 : 0))
delete_cdc_output="$(lsmctl cdc-events --addr http://127.0.0.1:8082 --shard users --offset "$delete_cdc_offset" --limit 10)"
require_contains "$delete_cdc_output" "offset=$delete_seq"
require_contains "$delete_cdc_output" "operation=delete"
require_contains "$delete_cdc_output" "key=compose"
require_contains "$delete_cdc_output" "tombstone=true"

missing_output="$(wait_for_cluster_missing compose)"
require_contains "$missing_output" "found=false"

echo "compose cluster smoke passed"
