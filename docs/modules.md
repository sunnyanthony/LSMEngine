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
  - dependency boundary: etcd-raft belongs behind the commit-log provider layer. Engine code should consume committed-entry contracts, not raft internals. Peer-message transport/ingress still exposes raft protocol messages in this foundation; wrap that in an LSM-owned message type before broadening multi-node APIs.
- `pkg/lsm/engine/control_plane.go`: fixed shard map and M1 control-plane operations.
  - Exposes control status including commit-log runtime progress (`mode/index/term/leader/replicas`).
- `pkg/lsm/server/server.go`: monitoring + control APIs + write consistency endpoints (`accepted`/`local_committed`) with async request-status tracking.
  - Also exposes CDC recent-events endpoint (`/cdc/events`).
- `pkg/lsm/server/router.go`: route-aware gateway helper (metadata cache + stale-route refresh/retry for writes).
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

## External Dependency Rule
- Core paths that depend on third-party libraries must introduce a small LSM-owned interface or adapter package before the dependency reaches engine/server/storage orchestration.
- Keep library-specific types near the adapter implementation. Avoid placing them in public API contracts unless the exposure is explicitly temporary and tracked as follow-up debt.
- `internal/lsm/iofs` is the reference pattern: storage code talks to an LSM-owned filesystem interface, while backend implementations can use OS files, async wrappers, or future platform-specific libraries.
- `internal/lsm/commitlog` should follow the same pattern for raft. Etcd-raft can power the built-in provider, but the rest of the engine should reason in terms of commit positions, committed control/data entries, runtime status, and transport abstractions owned by LSMEngine.
