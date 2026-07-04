# Server API Draft

This document describes the external server API and CLI modes that sit on top of
the LSM engine. It is intentionally separate from the engine internals.

## Goals
- Provide a stable RPC surface for KV operations.
- Support async writes with webhook callbacks.
- Keep transport choices pluggable (Phase 1: gRPC/HTTP2, Phase 2: HTTP/3/QUIC).
- Enable CLI usage for single-run operations.

## Phase 1: gRPC/HTTP2

### Phase 1 tasks
- Expose server-mode health + stats endpoints (`/healthz`, `/stats`) over HTTP.
- Expose M1 control-plane endpoints for fixed shard operations.
- Provide a CLI that can run in server mode or do single-run stats/health queries.
- Document async webhook semantics and the `GetStatus` fallback.

### Core RPCs
- `Get(key) -> entry`
- `Put(key, value, options) -> ack`
- `Delete(key) -> ack`
- `Range(start, end, limit) -> stream<entry>`

### M1 control-plane HTTP
- `GET /cluster/status`: node id, cluster id, storage mode, commit log provider, commit-log runtime (`mode/index/term/leader/replicas`), raft, shard count, draining, `revision`.
- `GET /cluster/shards`: shard ids, key ranges, leader and replica roles.
- `POST /cluster/shards/{id}/transfer-leader` with `{ "target": "node-x", "operation_id": "...", "expected_revision": 12 }`.
- `POST /cluster/shards/{id}/split` with `{ "split_key_base64": "<base64>", "operation_id": "...", "expected_revision": 12 }`.
- `POST /cluster/shards/{id}/rebalance` with `{ "target": "node-x", "operation_id": "...", "expected_revision": 12 }`.
- `POST /cluster/nodes/{id}/drain` with optional `{ "operation_id": "...", "expected_revision": 12 }`.
  - `operation_id` is optional; if reused with the same operation while retained in the server's recent-operation window, server returns success as an idempotent retry. The current window is bounded to the most recent 256 applied control mutations.
  - `expected_revision` is optional; mismatch returns `409 Conflict`.
- Control mutations are executed through a commit-log adapter (`commitlog.provider`).
  - Stage-1 default: `local` (single-node ordered commit, then deterministic local apply).
  - Stage-1 foundation: `etcd-raft` is wired for cluster-of-one propose/commit/apply; multi-node transport and membership lifecycle are deferred.
  - In this phase the revision / operation-id checks are node-local control-plane safeguards. Cluster-wide replicated control authority is deferred to later commitlog / raft work.
  - If a provider does not implement control write options, requests that send `operation_id` or `expected_revision` are rejected with `400 Bad Request`.

### CDC HTTP (foundation)
- `GET /cdc/events?shard=<id>&offset=<n>&limit=<n>`
  - Returns per-shard ordered events after `offset`.
  - Response includes `next_offset`, `oldest_offset`, and `dropped_before` (retention signal).
  - Delivery contract at this stage: node-local and in-memory only; events are readable while retained and are not rebuilt from WAL on restart.

### Async writes (webhook callback)
- `AsyncPut(key, value, callback_url, callback_token, request_id?) -> request_id`
- `AsyncDelete(key, callback_url, callback_token, request_id?) -> request_id`
- `GetStatus(request_id) -> status` (fallback if webhook fails)

### Webhook callback contract
- HTTP POST to `callback_url`.
- Payload fields:
  - `request_id`
  - `status` (`committed`, `rejected`, `failed`)
  - `error` (optional string)
  - `seq` (commit sequence, optional)
  - `committed_at` (RFC3339, optional)
- HMAC signature using `callback_token`.
- Retries: exponential backoff + max retry count.
- Dead-letter: after max retries, status remains queryable via `GetStatus`.
- Minimal local mode (monitoring): LSM can emit a best-effort webhook on
  `Put/Delete` success or failure without blocking the write path.
- Webhook routing can be resolved per-event (ex: different endpoints for write ops).
- Optionally emit write events to a Unix domain socket for a sidecar to handle
  webhooks/streaming out of process.

## Phase 2: HTTP/3 / QUIC
- Maintain the same API schema and semantics.
- Swap transport (gRPC-H3 or custom QUIC streams).
- Target: high RTT / mobile / cross-region deployments.

## CLI Mode
- `lsmctl get/put/delete/range`
- `lsmctl async-put`
- `lsmctl status <request_id>`
- Support `--addr` for remote server, `--data-dir` for local single-run access.

## Config and deployment
- Provide a minimal YAML config for server mode (addr, data dir, timeouts, auth hooks).
- Example config: `examples/server-config.yaml`.
- Control-plane persistence config:
  - `node_id`, `cluster_id`, `storage_mode`.
  - `control_state_path` (optional, defaults to `<data_dir>/control_state.json`).
  - `shards` must be declared in route order with non-overlapping ranges; open-ended range is only allowed on the last shard.
  - Startup validates persisted identity; mismatch fails startup to prevent cross-cluster state reuse.
- Allow bundling an L7 proxy (Envoy/Nginx) in the same pod for TLS/mTLS, auth, and rate limits.
- Keep the app server thin; let the proxy handle most ingress concerns.
- End-to-end example (Envoy + kind): `examples/k8s-envoy/`.

## Zero-copy and latency goals
- Use the proxy for TLS termination to avoid extra app-layer overhead.
- Prefer gRPC/HTTP2 initially; consider HTTP/3/QUIC in Phase 2 when server libs mature.
- For data plane reads, plan for sendfile/splice or similar kernel-assisted paths.

## Design Considerations
- Idempotency: repeated `request_id` must return the same result.
- Backpressure: async requests should accept quickly and report eventual status.
- Security: callbacks must be authenticated (HMAC token).
- Observability: track callback success rate, retries, and pending queue depth.
