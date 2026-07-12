# Module Map

Goal: make tracing and onboarding fast without flattening the layout.

## Start here (core flows)
- `pkg/lsm/engine/lsm.go`: options + core struct.
- `pkg/lsm/engine/new.go`: wiring and startup.
- `pkg/lsm/engine/write.go`: write path entry points (Put/Delete).
- `pkg/lsm/engine/write_service.go`: write mutation execution via commit-log adapter then local materialization.
- `pkg/lsm/engine/cdc.go`: per-shard retained change stream buffer and read API (`ReadCDCEvents`).
- `pkg/lsm/engine/read.go`: point reads.
- `pkg/lsm/engine/snapshot.go`: snapshot + range scans.
- `pkg/lsm/engine/compaction.go`: compaction wiring + state snapshots.
- `pkg/lsm/engine/replay.go`: WAL replay + recovery.
- `pkg/lsm/engine/commitlog.go`: control/data commit-log adapter and provider selection.
  - `local`: single-node ordered commit, then local apply.
  - `etcd-raft`: real Raft propose/commit foundation for cluster-of-one, plus static peer bootstrap, outbound transport scaffolding, and inbound peer-message handling.
  - `factory`: optional injected provider factory (`CommitLogOptions.Factory`); custom providers must return committed entries before engine apply.
  - code layout: public contracts in `pkg/lsm/engine/commitlog_types.go`; built-in provider implementations in `internal/lsm/commitlog/*`; engine adapter/factory glue in `pkg/lsm/engine/commitlog.go` and `pkg/lsm/engine/commitlog_factory.go`.
  - dependency boundary: public peer transport/ingress uses LSM-owned `RaftPeerMessage` envelopes. The builtin etcd-raft adapter encodes/decodes raftpb messages internally, so server/engine callers do not depend on etcd raft protocol structs. This is the same dependency rule as `internal/lsm/iofs`: external libraries must be hidden behind an LSM-owned adapter before they influence public/server APIs.
  - server transport: `pkg/lsm/server/raft_transport.go` provides an async HTTP adapter that posts `RaftPeerMessage` envelopes to peer server endpoints; YAML `raft.peer_urls` maps configured node names to URLs and is converted through `lsm.RaftPeerID`.
  - follower apply: `pkg/lsm/engine/commitlog_apply.go` observes builtin raft committed entries that do not belong to a local pending proposal and applies them to control state or WAL/memtable state.
  - smoke coverage: `tests/integration/server/lsm_server_etcd_raft_3node_test.go` verifies three-node leader writes replicate to followers through in-process and HTTP peer delivery; `tests/integration/server/lsm_server_etcd_raft_multiprocess_test.go` verifies the same path across real `lsmctl serve` processes.
  - error boundary: `internal/lsm/commitlog/errors.go` defines builtin provider errors; `pkg/lsm/engine/commitlog.go` maps them to public LSM errors before server write handlers translate them into retryable HTTP responses and route hints.
  - storage boundary: `internal/lsm/commitlog/raft_storage.go` persists builtin etcd-raft hard state, snapshots, and segmented log entries under `<data>/raft/commitlog-<node-id>/`; provider-owned raft log snapshot/compaction policy stays behind the same provider layer, while full LSM state-machine snapshot transfer and membership catch-up work remain deferred.
- `pkg/lsm/engine/control_plane.go`: fixed shard map and M1 control-plane operations.
  - Exposes control status including commit-log runtime progress and operational health (`mode/index/term/leader/replicas/write_available/leader_known/health/last_error_*`).
- `pkg/lsm/server/server.go`: monitoring + control APIs + point reads and write consistency endpoints (`accepted`/`local_committed`) with async request-status tracking.
  - Also exposes CDC recent-events endpoint (`/cdc/events`).
- `pkg/lsm/server/router.go`: route-aware gateway helper (metadata cache, retryable route-hint updates, refresh fallback, and bounded write attempts).
  - Persists control metadata (shards/order/leader/drain) in `control_state.json`.
  - Validates shard layout and builds deterministic route index for key-to-shard lookup.
  - Tracks node-local control `revision` and applied `operation_id` fingerprints for optimistic concurrency plus bounded idempotent retry dedupe.

## Internal modules (by responsibility)
- `internal/lsm/memory`: entry ownership, arenas, buffer pools.
- `internal/lsm/iofs`: minimal IO abstraction for WAL/SSTable.
- `internal/lsm/wal`: WAL append/replay.
- `internal/lsm/memtable`: in-memory index + iterators.
- `internal/lsm/sstable`: table reader/writer + block/index/filter/cache.
- `internal/lsm/metadata`: table metadata schema used for planning.
- `internal/lsm/tableset`: in-memory registry of tables + resolve.
- `internal/lsm/manifest`: durable metadata log/checkpoints.
- `internal/lsm/dispatch`: flush dispatcher queue.
- `internal/lsm/compaction`: planner/runner/controller/types.
- `internal/lsm/compaction/runtime`: runtime wiring for planner/runner/applier.
- `internal/lsm/tableedit`: table edits + manifest updates.
- `internal/lsm/bootstrap`: manifest load + WAL replay helpers.

## Trace tips
- Write path: `pkg/lsm/engine/write.go` -> `internal/lsm/wal` -> `internal/lsm/memtable` -> `internal/lsm/dispatch`.
- Read path: `pkg/lsm/engine/read.go` -> memtables -> `internal/lsm/sstable`.
- Range scan: `pkg/lsm/engine/snapshot.go` -> `pkg/lsm/engine/view_iterator.go`.
- Flush apply: `pkg/lsm/engine/flush_service.go` -> `internal/lsm/tableedit/service.go`.
- Compaction: `pkg/lsm/engine/compaction.go` -> `internal/lsm/compaction/runtime.go` -> `internal/lsm/tableedit/service.go`.

## Responsibilities & invariants (quick)
- `internal/lsm/wal`: append-only; record bytes must not be mutated after append.
- `internal/lsm/memtable`: ordered index; owns entry memory; writer tracking before flush.
- `internal/lsm/sstable`: immutable sorted runs; internal key order is userKey asc, seq desc.
- `internal/lsm/tableset`: snapshot returns newest-first; metadata copies are defensive.
- `internal/lsm/metadata`: single source of truth for planning metadata.
- `internal/lsm/manifest`: serialized updates; log + checkpoint durability.
- `internal/lsm/compaction`: plans from metadata only; outputs immutable tables.
- `internal/lsm/dispatch`: async flush queue; hot path stays non-blocking.
- `internal/lsm/memory`: single-copy policy; pools only for short-lived buffers.
- `internal/lsm/iofs`: minimal IO interface; no policy or scheduling.
- `pkg/lsm/engine`: orchestration only; does not own data invariants.
