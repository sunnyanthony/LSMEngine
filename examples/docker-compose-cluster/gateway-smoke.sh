#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/docker-compose-cluster/docker-compose.yml"
PROJECT="${LSM_COMPOSE_PROJECT:-lsmengine-cluster}"
KEEP="${LSM_COMPOSE_KEEP:-0}"
LSMCTL_BIN="${LSMCTL_BIN:-}"
GATEWAY_ADDR="${LSM_GATEWAY_ADDR:-127.0.0.1:8090}"
GATEWAY_URL="http://$GATEWAY_ADDR"

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
  if [[ "$KEEP" != "1" ]]; then
    compose --profile gateway down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

node_endpoint_args() {
  printf '%s\n' \
    --node-endpoint "node-a=http://127.0.0.1:8080" \
    --node-endpoint "node-b=http://127.0.0.1:8081" \
    --node-endpoint "node-c=http://127.0.0.1:8082"
}

wait_for_health() {
  local url="$1"
  local deadline=$((SECONDS + 60))
  until curl -fsS "$url/healthz" >/dev/null; do
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for $url/healthz" >&2
      compose --profile gateway ps >&2 || true
      compose --profile gateway logs --tail=100 >&2 || true
      return 1
    fi
    sleep 1
  done
}

wait_for_ready() {
  local url="$1"
  local deadline=$((SECONDS + 60))
  until curl -fsS "$url/readyz" >/dev/null; do
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for $url/readyz" >&2
      compose --profile gateway ps >&2 || true
      compose --profile gateway logs --tail=100 >&2 || true
      return 1
    fi
    sleep 1
  done
}

wait_for_gateway_status() {
  local deadline=$((SECONDS + 60))
  local output=""
  until output="$(lsmctl gateway-status --addr "$GATEWAY_URL")" \
    && [[ "$output" == *"ready=true"* ]] \
    && [[ "$output" == *"reachable_nodes=3"* ]] \
    && [[ "$output" == *"write_leader=node-"* ]]; do
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for gateway-status at $GATEWAY_URL" >&2
      if [[ -n "$output" ]]; then
        echo "$output" >&2
      fi
      compose --profile gateway ps >&2 || true
      compose --profile gateway logs --tail=100 >&2 || true
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

request_id_from_output() {
  local output="$1"
  local request_id
  request_id="$(awk -F= '/^request_id=/{print $2; exit}' <<<"$output")"
  if [[ -z "$request_id" ]]; then
    echo "write did not return request_id" >&2
    echo "$output" >&2
    return 1
  fi
  printf '%s\n' "$request_id"
}

compose --profile gateway up -d --build node-a node-b node-c
wait_for_health "http://127.0.0.1:8080"
wait_for_health "http://127.0.0.1:8081"
wait_for_health "http://127.0.0.1:8082"

wait_output="$(lsmctl wait-cluster $(node_endpoint_args) --timeout 60s)"
require_contains "$wait_output" "ready=true"

compose --profile gateway up -d --build gateway
wait_for_health "$GATEWAY_URL"
wait_for_ready "$GATEWAY_URL"
wait_for_gateway_status

put_output="$(lsmctl put --addr "$GATEWAY_URL" --key gateway-smoke --value ok)"
require_contains "$put_output" "state=committed"

get_output="$(lsmctl get --addr "$GATEWAY_URL" --key gateway-smoke)"
require_contains "$get_output" "found=true"
require_contains "$get_output" "value=ok"

range_output="$(lsmctl range --addr "$GATEWAY_URL" --start gateway --end gateway~ --limit 10)"
require_contains "$range_output" "key=gateway-smoke"
require_contains "$range_output" "value=ok"

async_output="$(lsmctl async-put --addr "$GATEWAY_URL" --key gateway-async --value ok)"
require_contains "$async_output" "state=pending"
request_id="$(request_id_from_output "$async_output")"

status_output="$(lsmctl write-status --addr "$GATEWAY_URL" --request-id "$request_id")"
require_contains "$status_output" "state=committed"

async_delete_output="$(lsmctl async-delete --addr "$GATEWAY_URL" --key gateway-async)"
require_contains "$async_delete_output" "state=pending"
delete_request_id="$(request_id_from_output "$async_delete_output")"

delete_status_output="$(lsmctl write-status --addr "$GATEWAY_URL" --request-id "$delete_request_id")"
require_contains "$delete_status_output" "state=committed"

async_missing_output="$(lsmctl get --addr "$GATEWAY_URL" --key gateway-async)"
require_contains "$async_missing_output" "found=false"

delete_output="$(lsmctl delete --addr "$GATEWAY_URL" --key gateway-smoke)"
require_contains "$delete_output" "state=committed"

missing_output="$(lsmctl get --addr "$GATEWAY_URL" --key gateway-smoke)"
require_contains "$missing_output" "found=false"

echo "compose gateway smoke passed"
