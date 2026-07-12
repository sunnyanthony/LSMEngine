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
- Provide a CLI that can run in server mode and perform point KV operations plus stats/health queries.
- Document async webhook semantics and the `GetStatus` fallback.

### Core RPCs
- `Get(key) -> entry`
- `Put(key, value, options) -> ack`
- `Delete(key) -> ack`
- `Range(start, end, limit) -> stream<entry>`

### M1 control-plane HTTP
- `GET /kv/get?key_base64=<base64>`: point read from the local node; returns `200` with `found/key_base64/value_base64/seq` when present and `404` with `found=false` when absent.
- `GET /kv/range?start_key_base64=<base64>&end_key_base64=<base64>&limit=<n>`: bounded local snapshot range scan; `start_key_base64` and `end_key_base64` are optional, `limit` defaults to 100 and is capped at 1000.
- `POST /kv/put` with `{ "key_base64": "<base64>", "value_base64": "<base64>", "consistency": "accepted|local_committed" }`.
- `POST /kv/delete` with `{ "key_base64": "<base64>", "consistency": "accepted|local_committed" }`.
- `GET /kv/write-status/{request_id}`: async write lifecycle state for `accepted` writes.
- `GET /cluster/status`: node id, cluster id, storage mode, commit log provider, commit-log runtime (`mode/index/term/leader/replicas/write_available/leader_known/health/last_error_*`), raft, shard count, draining, `revision`.
- `GET /cluster/shards`: shard ids, key ranges, leader and replica roles.
- `POST /cluster/shards/{id}/transfer-leader` with `{ "target": "node-x", "operation_id": "...", "expected_revision": 12 }`.
- `POST /cluster/shards/{id}/add-replica` with `{ "target": "node-x", "operation_id": "...", "expected_revision": 12 }`.
- `POST /cluster/shards/{id}/remove-replica` with `{ "target": "node-x", "operation_id": "...", "expected_revision": 12 }`; removing the current leader is rejected until leadership is transferred.
- `POST /cluster/shards/{id}/split` with `{ "split_key_base64": "<base64>", "operation_id": "...", "expected_revision": 12 }`.
- `POST /cluster/shards/{id}/rebalance` with `{ "target": "node-x", "operation_id": "...", "expected_revision": 12 }`.
- `POST /cluster/nodes/{id}/drain` with optional `{ "operation_id": "...", "expected_revision": 12 }`.
  - `operation_id` is optional; if reused with the same operation while retained in the server's recent-operation window, server returns success as an idempotent retry. The current window is bounded to the most recent 256 applied control mutations.
  - `expected_revision` is optional; mismatch returns `409 Conflict`.
- Control mutations are executed through a commit-log adapter (`commitlog.provider`).
  - Stage-1 default: `local` (single-node ordered commit, then deterministic local apply).
  - Stage-1 foundation: `etcd-raft` is wired for cluster-of-one propose/commit/apply.
  - Static multi-peer bootstrap can use server-mode HTTP peer delivery with `raft.peer_urls`, or embedded callers can inject `CommitLogOptions.Transport`. Inbound peer-message handling is available via `POST /cluster/raft/messages` and `HandlePeerMessages`. Both use LSM-owned `RaftPeerMessage` envelopes; etcd raftpb payloads remain a builtin provider implementation detail. The builtin provider persists raft hard state, snapshots, and segmented log entries under `<data>/raft/commitlog-<node-id>/`; provider-owned raft log snapshot/compaction can be enabled with `commitlog.snapshot_policy`, but full LSM state-machine snapshot transfer, quorum-backed commits, dynamic raft ConfChange membership, and node bootstrap/join are deferred.
  - Shard replica membership has a foundation: `add-replica` and `remove-replica` are committed control-plane mutations, persisted in `control_state.json`, replicated through the commit-log path, and protected by the same `operation_id` / `expected_revision` controls as other shard mutations.
  - Follower committed-entry apply exists as a foundation: committed entries received without a local pending proposal are applied to local control/data state after the commit-log provider reports them. Three-node smoke tests cover in-process delivery, HTTP peer delivery, and a real multi-process `lsmctl serve` cluster, but full LSM state-machine snapshot transfer, membership lifecycle, and higher-level service discovery/load balancing remain deferred.
  - Write errors are route-aware where possible: known non-local raft leaders return `409` with code `not_leader` plus retryable route hints, while leader-election/apply timeouts return retryable `503` with code `commit_log_unavailable`. `server.Gateway` consumes these hints with bounded retries and route-refresh fallback.
  - In this phase the revision / operation-id checks are node-local control-plane safeguards. Cluster-wide replicated control authority is deferred to later commitlog / raft work.
  - If a provider does not implement control write options, requests that send `operation_id` or `expected_revision` are rejected with `400 Bad Request`.
  - Embedded mode can inject a custom commit-log provider via `CommitLogOptions.Factory`; the provider contract is committed-entry first, not apply-callback based.

### CDC HTTP (foundation)
- `GET /cdc/events?shard=<id>&offset=<n>&limit=<n>`
  - Returns per-shard ordered events after `offset`.
  - Response includes `next_offset`, `oldest_offset`, and `dropped_before` (retention signal).
  - Delivery contract at this stage: node-local and in-memory only; events are readable while retained and are not rebuilt from WAL on restart.
  - Durability decision for the current phase: CDC is a recent-observation API, not a durable changefeed. Clients must handle `dropped_before` or restart gaps by resyncing from the KV API. WAL-backed or raft-log-backed CDC remains deferred until full state-machine snapshot/catch-up semantics are implemented.

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
- `lsmctl serve --config <path>` starts server mode.
- `lsmctl get --addr <url> --key <key>` reads from a remote server; `--key-base64` supports binary keys.
- `lsmctl range --addr <url> --start <key> --end <key> --limit <n>` scans a bounded key range; `--start-base64` / `--end-base64` support binary bounds.
- `lsmctl put --addr <url> --key <key> --value <value>` writes to a remote server; `--key-base64` / `--value-base64` support binary payloads.
- `lsmctl delete --addr <url> --key <key>` deletes from a remote server.
- `lsmctl async-put --addr <url> --key <key> --value <value>` and `lsmctl async-delete --addr <url> --key <key>` submit writes with `accepted` consistency and return a request id for `write-status`.
- `lsmctl write-status --addr <url> --request-id <id>` reads an accepted write's lifecycle status from server mode; the request id can also be passed as a positional argument.
- `lsmctl stats` and `lsmctl health` work against `--addr` or local `--data-dir`.
- `get` / `put` / `delete` also support local single-run access with `--data-dir`.
- Deferred CLI work: callback/webhook configuration flags are not exposed yet.

## Config and deployment
- Provide a minimal YAML config for server mode (addr, data dir, timeouts, auth hooks).
- Example config: `examples/server-config.yaml`.
- Control-plane persistence config:
  - `node_id`, `cluster_id`, `storage_mode`.
  - `control_state_path` (optional, defaults to `<data_dir>/control_state.json`).
  - `raft.peers` (optional): static peer list used to bootstrap etcd-raft node IDs.
  - `raft.peer_urls` (optional): node-name to server URL map used by `lsmctl serve` to build the HTTP raft peer transport when `commitlog.provider=etcd-raft` and `raft.peers` has more than one node.
    - Multi-peer etcd-raft configs are validated before server startup: `raft.peers` must include the local `node_id`, must not contain empty/duplicate names, and `raft.peer_urls` must contain an absolute URL for every configured peer with no unknown peer names.
  - `commitlog.snapshot_policy.applied_entries` (optional): when greater than zero, enables builtin etcd-raft provider-owned raft log snapshots after this many newly applied entries.
  - `commitlog.snapshot_policy.retain_entries` (optional): number of recent raft log entries to keep after provider-owned snapshot compaction.
  - `shards` must be declared in route order with non-overlapping ranges; open-ended range is only allowed on the last shard.
  - Startup validates persisted identity; mismatch fails startup to prevent cross-cluster state reuse.
- Allow bundling an L7 proxy (Envoy/Nginx) in the same pod for TLS/mTLS, auth, and rate limits.
- Keep the app server thin; let the proxy handle most ingress concerns.
- End-to-end example (Envoy + kind): `examples/k8s-envoy/`.
- Static three-node local smoke (Docker Compose): `examples/docker-compose-cluster/`.
- Static three-node Kubernetes smoke (kind): `examples/kind-cluster/`.

## Zero-copy and latency goals
- Use the proxy for TLS termination to avoid extra app-layer overhead.
- Prefer gRPC/HTTP2 initially; consider HTTP/3/QUIC in Phase 2 when server libs mature.
- For data plane reads, plan for sendfile/splice or similar kernel-assisted paths.

## Design Considerations
- Idempotency: repeated `request_id` must return the same result.
- Backpressure: async requests should accept quickly and report eventual status.
- Security: callbacks must be authenticated (HMAC token).
- Observability: track callback success rate, retries, and pending queue depth.
