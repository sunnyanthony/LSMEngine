#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/docker-compose-cluster/docker-compose.yml"
PROJECT="${LSM_COMPOSE_PROJECT:-lsmengine-cluster}"
KEEP="${LSM_COMPOSE_KEEP:-0}"
LSMCTL_BIN="${LSMCTL_BIN:-}"
TMP_DIR="$(mktemp -d)"
LSMCTL_PEER_URL_FILE="$TMP_DIR/peer-urls.yaml"
LSMCTL_CONFIG="$TMP_DIR/lsmctl.yaml"

services=(node-a node-b node-c)

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
  rm -rf "$TMP_DIR"
  if [[ "$KEEP" == "1" ]]; then
    return
  fi
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

write_lsmctl_config() {
  cat >"$LSMCTL_PEER_URL_FILE" <<EOF
node-a: "$(url_for_service node-a)"
node-b: "$(url_for_service node-b)"
node-c: "$(url_for_service node-c)"
EOF
  cat >"$LSMCTL_CONFIG" <<EOF
raft:
  peer_url_file: "$LSMCTL_PEER_URL_FILE"
EOF
}

url_for_service() {
  case "$1" in
    node-a) echo "http://127.0.0.1:8080" ;;
    node-b) echo "http://127.0.0.1:8081" ;;
    node-c) echo "http://127.0.0.1:8082" ;;
    *) echo "unknown service: $1" >&2; return 1 ;;
  esac
}

dump_diagnostics() {
  compose ps >&2 || true
  compose logs --tail=120 >&2 || true
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

wait_for_cluster() {
  local service
  for service in "${services[@]}"; do
    wait_for_health "$(url_for_service "$service")"
  done
}

cluster_config_args() {
  printf '%s\n' --config "$LSMCTL_CONFIG"
}

wait_cluster_ready() {
  local min_ready="$1"
  lsmctl wait-cluster \
    $(cluster_config_args) \
    --min-ready "$min_ready" \
    --timeout 60s >/dev/null
}

drain_service() {
  local service="$1"
  lsmctl drain-node \
    --node "$service" \
    --operation-id "rolling-drain-$service" \
    $(cluster_config_args)
}

resume_service() {
  local service="$1"
  lsmctl resume-node \
    --node "$service" \
    --operation-id "rolling-resume-$service" \
    $(cluster_config_args)
}

put_on_any_live_node() {
  local skipped_service="$1"
  local key="$2"
  local value="$3"
  local deadline=$((SECONDS + 60))
  local output
  while (( SECONDS < deadline )); do
    if output="$(lsmctl put \
      --cluster \
      $(cluster_config_args) \
      --key "$key" \
      --value "$value" 2>&1)" &&
      [[ "$output" == *"state=committed"* ]]; then
      return 0
    fi
    sleep 1
  done
  echo "timed out writing $key while $skipped_service was stopped" >&2
  dump_diagnostics
  return 1
}

wait_for_value() {
  local service="$1"
  local key="$2"
  local value="$3"
  local url
  local deadline=$((SECONDS + 60))
  local output
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

write_lsmctl_config
compose up -d --build
wait_for_cluster
wait_cluster_ready 3

put_on_any_live_node "" "rolling-before" "all-up"

for service in "${services[@]}"; do
  key="rolling-$service"
  value="ok-$service"
  drain_service "$service"
  compose stop "$service" >/dev/null
  wait_cluster_ready 2
  put_on_any_live_node "$service" "$key" "$value"
  compose start "$service" >/dev/null
  wait_for_health "$(url_for_service "$service")"
  resume_service "$service"
  wait_cluster_ready 3
  for read_service in "${services[@]}"; do
    wait_for_value "$read_service" "$key" "$value"
  done
done

echo "compose rolling restart smoke passed"
