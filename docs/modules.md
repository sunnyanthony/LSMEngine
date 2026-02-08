# Module Map

Goal: make tracing and onboarding fast without flattening the layout.

## Start here (core flows)
- `pkg/lsm/engine/lsm.go`: options + core struct.
- `pkg/lsm/engine/new.go`: wiring and startup.
- `pkg/lsm/engine/write.go`: write path (Put/Delete).
- `pkg/lsm/engine/read.go`: point reads.
- `pkg/lsm/engine/snapshot.go`: snapshot + range scans.
- `pkg/lsm/engine/compaction.go`: compaction wiring + state snapshots.
- `pkg/lsm/engine/replay.go`: WAL replay + recovery.
- `pkg/lsm/engine/control_plane.go`: fixed shard map and M1 control-plane operations.
  - Persists control metadata (shards/order/leader/drain) in `control_state.json`.

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
