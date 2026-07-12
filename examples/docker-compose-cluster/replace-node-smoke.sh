#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/docker-compose-cluster/docker-compose.yml"
PROJECT="${LSM_COMPOSE_PROJECT:-lsmengine-cluster}"
KEEP="${LSM_COMPOSE_KEEP:-0}"
LSMCTL_BIN="${LSMCTL_BIN:-}"

initial_services=(node-a node-b node-c)
all_services=(node-a node-b node-c node-d)

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
  compose --profile replacement down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

url_for_service() {
  case "$1" in
    node-a) echo "http://127.0.0.1:8080" ;;
    node-b) echo "http://127.0.0.1:8081" ;;
    node-c) echo "http://127.0.0.1:8082" ;;
    node-d) echo "http://127.0.0.1:8083" ;;
    *) echo "unknown service: $1" >&2; return 1 ;;
  esac
}

dump_diagnostics() {
  compose --profile replacement ps >&2 || true
  compose --profile replacement logs --tail=160 >&2 || true
}

wait_for_health() {
  local url="$1"
  local deadline=$((SECONDS + 60))
  until curl -fsS "$url/healthz" >/dev/null; do
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for $url/healthz" >&2
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

node_endpoint_args() {
  local service
  for service in "${all_services[@]}"; do
    printf '%s\n' --node-endpoint "$service=$(url_for_service "$service")"
  done
}

put_cluster() {
  local key="$1"
  local value="$2"
  local deadline=$((SECONDS + 60))
  local output
  while (( SECONDS < deadline )); do
    if output="$(lsmctl put --cluster $(node_endpoint_args) --key "$key" --value "$value" 2>&1)" &&
      [[ "$output" == *"state=committed"* ]]; then
      return 0
    fi
    sleep 1
  done
  echo "timed out writing $key=$value" >&2
  dump_diagnostics
  return 1
}

wait_for_value() {
  local service="$1"
  local key="$2"
  local value="$3"
  local deadline=$((SECONDS + 60))
  local output
  local url
  url="$(url_for_service "$service")"
  while (( SECONDS < deadline )); do
    if output="$(lsmctl get --addr "$url" --key "$key" 2>&1)" &&
      [[ "$output" == *"found=true"* ]] &&
      [[ "$output" == *"value=$value"* ]]; then
      return 0
    fi
    sleep 1
  done
  echo "timed out reading $key=$value from $service" >&2
  dump_diagnostics
  return 1
}

compose up -d --build "${initial_services[@]}"
for service in "${initial_services[@]}"; do
  wait_for_health "$(url_for_service "$service")"
done

put_cluster "replace-before" "old-cluster"
for service in "${initial_services[@]}"; do
  wait_for_value "$service" "replace-before" "old-cluster"
done

compose --profile replacement up -d --build node-d
wait_for_health "$(url_for_service node-d)"

replace_output="$(lsmctl replace-node \
  --old-node node-a \
  --new-node node-d \
  --operation-prefix compose-replace-node-a-node-d \
  $(node_endpoint_args) 2>&1)"
require_contains "$replace_output" "old_node=node-a"
require_contains "$replace_output" "new_node=node-d"
require_contains "$replace_output" "step=raft-add"
require_contains "$replace_output" "step=raft-remove"

wait_for_value node-d "replace-before" "old-cluster"

compose stop node-a >/dev/null
put_cluster "replace-after" "new-cluster"
for service in node-b node-c node-d; do
  wait_for_value "$service" "replace-after" "new-cluster"
done

echo "compose replace-node smoke passed"
