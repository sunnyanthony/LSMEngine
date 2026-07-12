#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/docker-compose-cluster/docker-compose.yml"
PROJECT="${LSM_COMPOSE_PROJECT:-lsmengine-cluster}"
KEEP="${LSM_COMPOSE_KEEP:-0}"
LSMCTL_BIN="${LSMCTL_BIN:-}"
NODE_URLS=(
  "http://127.0.0.1:8080"
  "http://127.0.0.1:8081"
  "http://127.0.0.1:8082"
)

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

eventually_lsmctl_write() {
  local command="$1"
  local key="$2"
  local value="${3:-}"
  local deadline=$((SECONDS + 60))
  local output=""
  local last=""
  until (( SECONDS >= deadline )); do
    for url in "${NODE_URLS[@]}"; do
      if [[ "$command" == "put" ]]; then
        if output="$(lsmctl put --addr "$url" --key "$key" --value "$value" 2>&1)"; then
          require_contains "$output" "state=committed"
          echo "$url"
          return
        fi
      else
        if output="$(lsmctl delete --addr "$url" --key "$key" 2>&1)"; then
          require_contains "$output" "state=committed"
          echo "$url"
          return
        fi
      fi
      last="$url: $output"
    done
    sleep 1
  done
  echo "timed out waiting for lsmctl $command to commit; last=$last" >&2
  compose ps >&2 || true
  compose logs --tail=80 >&2 || true
  return 1
}

eventually_lsmctl_get_contains() {
  local url="$1"
  local key="$2"
  local first="$3"
  local second="${4:-}"
  local deadline=$((SECONDS + 60))
  local output=""
  until (( SECONDS >= deadline )); do
    if output="$(lsmctl get --addr "$url" --key "$key" 2>&1)"; then
      if [[ "$output" == *"$first"* && ( -z "$second" || "$output" == *"$second"* ) ]]; then
        return
      fi
    fi
    sleep 1
  done
  echo "timed out waiting for lsmctl get $key from $url to contain: $first $second" >&2
  echo "$output" >&2
  compose ps >&2 || true
  compose logs --tail=80 >&2 || true
  return 1
}

node_endpoint_args() {
  printf '%s\n' \
    --node-endpoint "node-a=http://127.0.0.1:8080" \
    --node-endpoint "node-b=http://127.0.0.1:8081" \
    --node-endpoint "node-c=http://127.0.0.1:8082"
}

compose up -d --build

for url in "${NODE_URLS[@]}"; do
  wait_for_health "$url"
done

write_url="$(eventually_lsmctl_write put compose ok)"
echo "committed put through $write_url"

eventually_lsmctl_get_contains "http://127.0.0.1:8081" compose "found=true" "value=ok"

range_output="$(lsmctl range --addr http://127.0.0.1:8081 --start compose --end composf --limit 1)"
require_contains "$range_output" "key=compose"
require_contains "$range_output" "value=ok"

cluster_get_output="$(lsmctl get --cluster $(node_endpoint_args) --key compose)"
require_contains "$cluster_get_output" "found=true"
require_contains "$cluster_get_output" "value=ok"

cluster_range_output="$(lsmctl range --cluster $(node_endpoint_args) --start compose --end composf --limit 1)"
require_contains "$cluster_range_output" "key=compose"
require_contains "$cluster_range_output" "value=ok"

delete_url="$(eventually_lsmctl_write delete compose)"
echo "committed delete through $delete_url"

eventually_lsmctl_get_contains "http://127.0.0.1:8082" compose "found=false"

echo "compose cluster smoke passed"
