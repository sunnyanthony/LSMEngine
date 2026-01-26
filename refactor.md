# LSMEngine Refactor Design Doc

## Purpose
This document tracks the current refactor plan and active decisions. For end-to-end architecture,
see `docs/architecture.md` and `docs/modules.md`. History lives in git; this file stays current.

## What The Project Provides (Core Value)
- Low-latency key/value reads and writes with WAL durability.
- Ordered iteration and range scans via memtable and SSTables.
- Background flush and compaction for storage efficiency.
- Snapshot isolation for consistent reads.
- Optional observability; distributed replication is deferred.
- Zero-copy internal data flow backed by memory arenas and pools.

## Current Pain Points
- `pkg/lsm/engine` still centralizes too many responsibilities, so changes ripple.
- Manifest edits are synchronous; async/queued apply is deferred for performance work.

## Design Goals
- Low-latency read/write on the hot path.
- Control/data separation with clean boundaries.
- High replaceability of compaction policy; replication strategy is deferred.
- Zero-copy internal flow and explicit memory ownership.
- Minimal public surface area in `pkg/lsm`.
- IO abstraction to enable io_uring without rewriting higher layers.

## Core Principles
- Data plane owns correctness and invariants.
- Control plane owns policies and scheduling.
- External API stays stable and simple.
- Ownership is explicit; no implicit copies on hot paths.
- Metadata is the single source of truth for system state.

## Maintainability & Extensibility Guidelines
- Hot path depends on shallow interfaces only (`WAL.AppendOwned`, `Memtable.ApplyOwned`, `SSTable.GetView/Range`).
- External API copies once via `EntryBuilder`; internal layers use views/arena only.
- Background work consolidated into 2 services: flush worker, compaction service. Replication loop deferred.
- Feature insertion points via interfaces (compaction planner/runner, SSTable read policy). Transport is deferred.
- Options normalization happens once in a single layer to avoid conflicting defaults.

## System Flows (End-to-End)

### Write Path
1. Public API validates input and builds an owned Entry.
2. Acquire a memtable snapshot and increment writer tracking.
3. Append to WAL (async batch); return error if WAL fails.
4. Apply Entry to memtable.
5. If memtable crosses threshold, swap to immutable and enqueue flush.
6. Decrement writer count; return success to caller.

### Read Path (Get)
1. Capture memtable snapshot (active + immutables).
2. Probe active, then immutables (view-based to avoid copies).
3. Probe SSTables via reader pipeline (index/filter/cache).
4. Return owned data to caller or not found.

### Range Scan
1. Snapshot memtables and tableset at a point-in-time.
2. Build iterators for memtables and SSTables.
3. Merge iterators by internal key order (userKey asc, seq desc).
4. Return owned entries to the caller.

### Flush Path
1. Dispatcher receives immutable memtable.
2. Wait for writers to finish before draining entries.
3. Build SSTable and write via IO layer.
4. Update manifest + tableset atomically.
5. Release memtable/arena and notify compaction.

### WAL Replay
1. Discover WAL segments and read sequentially.
2. Decode records; resync on corruption per policy.
3. Apply entries to memtable; rotate when full.
4. Persist resulting tables via flush.

### Compaction
1. Controller builds plan from metadata snapshot.
2. Runner merges input tables into new outputs.
3. Apply plan via manifest + tableset edits.
4. Delete obsolete tables and update metrics.

### Distributed/Replication (Deferred)
- Distributed replication is deferred until core LSM is complete.
- Planned reintroduction: transport + term gating as a separate package with manifest state for dedupe.

### Startup / Shutdown
- Startup: load manifest, open WAL, replay, rebuild tableset, start workers.
- Shutdown: stop background workers, drain queues, close WAL/manifest/IO.

## Package Responsibilities & Import Rules (Agreed)
- `pkg/lsm`: public API facade; depends on `pkg/lsm/engine` and `pkg/lsm/errs` only.
- `pkg/lsm/engine`: orchestration + lifecycle; wires WAL/memtable/sstable/manifest/compaction.
- `internal/lsm/wal`: durability (append/replay/segment); depends on `internal/lsm/iofs` and `internal/lsm/memory`.
- `internal/lsm/memtable`: in-memory ordered index + arena ownership; depends on `internal/lsm/memory`.
- `internal/lsm/sstable`: immutable run + read pipeline + block/index/filter/cache; depends on `internal/lsm/iofs`, `internal/lsm/memory`, `internal/lsm/metadata`.
- `internal/lsm/tableset` + `internal/lsm/metadata`: table registry + metadata snapshot for planners; leaf packages.
- `internal/lsm/manifest`: durable metadata log/checkpoint; depends on `internal/lsm/iofs` and `internal/lsm/metadata`.
- `internal/lsm/compaction`: planner/runner/applier + scheduler; depends on `internal/lsm/metadata`, `internal/lsm/tableset`, `internal/lsm/sstable`.
- `internal/lsm/dispatch`: flush queue to flusher.
- `internal/lsm/memory`: entry ownership + pools (GC reduction); leaf package.
- `internal/lsm/iofs`: IO abstraction (FS + async read); leaf package.
- Alias packages should be limited to `pkg/lsm` (public surface); avoid alias-only packages inside `internal`.

## Alias-Only Packages (Reduce/Remove)
- Keep: `pkg/lsm/aliases.go` (public surface stability).

## Multi-Level Compaction Direction
- Default policy: strict levelled compaction.
- Pluggable policy allows tiered/L0-only variants.
- Metadata must support level-aware planning before introducing multi-L compaction.

## Identified Technical Issues & Mitigation
- **Swap Race**: Current `Put/Delete` fetches the `activeMem` pointer and then releases the lock. A `swapMemtable` can occur before `ApplyOwned` completes, leading to data being written to a table that is already being flushed or recycled.
  - *Mitigation*: Introduce `IncWriter/DecWriter` on the memtable. Flusher waits for count to hit zero.
- **Visibility Gap**: `Get` reads `activeMem` then `immutables`. If swap happens between these two reads, the data in the old-active-now-immutable table might be missed.
  - *Mitigation*: The `swapMemtable` must ensure the old table is appended to `immutables` before it is no longer the `activeMem`, or `Get` must hold `memMu` for the entire pointer-acquisition phase.
- **Throttling Consistency**: `shouldThrottleWrite` checks size but doesn't reserve space or hold the pointer, leading to potential over-limit writes.
  - *Mitigation*: Move throttling check inside the same lock that acquires the `mem` pointer, or use a reservation system.

## Compatibility Constraints
- External API remains stable and minimal.
- Input buffers are always copied at the public boundary.
- WAL replay and SSTable reload remain correct across restarts.

## Non-Goals
- No changes to public API semantics.
- No immediate rewrite of storage formats.
- No requirement to expose internal observability hooks publicly.

## Open Questions
- Preferred boundary between metadata module and compaction policy.
- Exact checkpoint policy thresholds for different deployment profiles.
- How aggressive throttling should be under sustained overload.

## Roadmap (Current)
- [ ] Execute consolidation pass across internal and `pkg/lsm` (one module at a time).
- [ ] Prepare the deferred distributed/replication reintroduction as a separate package after core LSM is stable.

## Backlog (Non-blocking)

WAL:
- Tail/truncate policy and payload cap refinements.
- Faster resync scanning for corrupted blocks.
- Async writer metrics/backpressure observability.
- Codec version negotiation for future formats.
- Large replay + mixed corrupt/missing segment stress tests.

Memtable:
- Streaming iterators to avoid snapshot copying.
- Shard count auto-tuning based on workload.
- Lock contention and tail-latency benchmarks.
- Tighter immutable/flush state machine if stronger consistency is needed.

Skiplist:
- Node allocation via arena to reduce GC.
- Comparator coverage tests for varied key distributions.
- Level distribution/iterator performance benchmarks.

Read path:
- Snapshot observability (replay counts, pinned memtable metrics).

Compaction:
- Optional flush coalescing (merge multiple immutables into a single L0 SSTable to reduce file count).
- Pluggable storage interface (local FS vs object store for SSTables).
- Output size caps + multi-output split for compaction results (max table size enforcement).
- Scheduler/backpressure/priority policy for compaction runs.

Observability/ops:
- Metrics/health endpoints for runtime visibility (flush/compaction/WAL/memtable).

Distributed/Replication (deferred):
- Reintroduce transport + term/epoch gating as a separate package after core LSM.
- External term/epoch manager integration (e.g., Raft lease) with dynamic term updates.
- Replay checkpoints to avoid re-sending large histories.
