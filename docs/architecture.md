# LSMEngine Architecture

Goals:
- Cloud-native: deterministic layout for container mounts, observable background work, safe restarts.
- Async-first: WAL append can be async with group commit; flush/compaction run in background workers with backpressure.
- Pluggable policies: compaction and read behavior can be swapped without rewriting the core.

## Components
- **LSM facade**: `pkg/lsm` re-exports the public API, while `pkg/lsm/engine` holds orchestration logic.
- **Memtable**: in-memory ordered index for recent writes; drained to SSTables. See `docs/memtable.md`.
- **Snapshot**: point-in-time view that freezes the active memtable for range scans.
- **WAL**: append-only, fsync-configurable; replay on startup. See `docs/wal.md`.
- **Flush/compaction**: background flush worker turns drained memtables into SSTables; compaction is planned.
- **TableSet/Metadata**: central in-memory registry of tables with `TableMeta` (level, key range, seq bounds).
- **Compaction engine**: strict levelled policy by default, pluggable for future variants. See `docs/compaction.md`.
- **SSTable**: immutable runs with block index + meta; compression, bloom filter, cache/prefetch are configurable.
- **Manifest**: durable metadata describing current table set and WAL checkpoints.
- **Table edits**: single apply path updates TableSet + manifest (flush + compaction).
- **Observability**: event bus hooks; SSTable FlowMetrics available (cache/filter/err), deeper metrics planned.

## Current Design Snapshot
- Data plane: WAL + memtable + tableset + SSTable read pipeline; emits metadata snapshots.
- Control plane: flush + compaction scheduling; works off metadata only and never mutates data-plane state directly.
- M1 distributed surface: fixed shard metadata + manual control operations (leader transfer/split/rebalance/drain) exposed through server APIs.
- M1 control-plane persistence: control metadata is stored in `control_state.json` and restored on restart.
- M1 control-plane commit path: mutations are routed through a commit-log adapter (default provider: `local`).
- M1 etcd-raft commit-log foundation: `commitlog.provider=etcd-raft` now executes control/data mutations through a real Raft propose/commit/apply path for cluster-of-one deployments.
- M1 data write commit path: Put/Delete mutations are also routed through the same commit-log adapter before local WAL/materialization.
- M1 write consistency API surface: server supports `consistency=accepted|local_committed` for data writes with request-status tracking (`/kv/put`, `/kv/delete`, `/kv/write-status/{id}`). `local_committed` means the write is committed and applied on this node; cluster-wide linearizability is deferred until raft quorum semantics are wired.
- M1 server consistency policy: default write consistency is configurable (`write_consistency_default`) and used when request-level consistency is omitted.
- M1 routing metadata/retry surface: server exposes route table snapshots (`/cluster/routes`) and write errors include retryable route hints (`revision/shard/leader`) for stale-route refresh and retry.
- Shard routing hardening: startup validates shard ranges (ordered, non-overlapping, bounded correctness), and key routing uses a deterministic ordered route index.
- Control operation safety: mutations carry a node-local monotonic `revision` and an optional `operation_id` for bounded idempotent retries (current retention window: 256 remembered control mutations).
- Metadata: manifest log + checkpoint; table metadata carries level, key range, size, seq bounds.
- IO: shared IO layer for WAL/SSTable; OS specifics isolated in `internal/lsm/iofs`.
- Backpressure: write path stays async; on pressure return `ErrBackpressure` (no sync flush).
- Zero-copy: single copy at API boundary; internal views stay borrowed; public reads return owned data.
- Distributed transport and membership: deferred from the engine surface until a later phase. Raft integration is currently cluster-of-one to keep one log model while multi-node networking is introduced incrementally.
- Cluster-wide replicated control authority and mixed-version control-state compatibility are deferred to later commitlog / raft hardening work.

## Boundary Audit (current focus)
- `pkg/lsm/engine` is still broad; future work splits responsibilities without widening the public API.
- `internal/lsm/sstable` consolidated format logic; remaining depth is intentional (cache/bloom/config/storage).
- `internal/lsm/compaction` is centralized; planners/runners live under it.
- `internal/lsm/memtable` holds interfaces with implementations under subpackages.
- `internal/lsm/wal` exposes a root facade; codec/segment remain as leaf helpers.

## Control vs Data
- Control plane: consumes metadata snapshots and decides flush/compaction policy.
- Data plane: owns WAL, memtables, SSTable IO, and the `TableSet` registry.
- Control plane submits **table edits** (add/remove) and does not mutate data-plane structures directly.
- Cache only covers on-disk data/index/filter blocks; memtable is managed separately.
- Metrics: FlowObserver is injectable; FlowMetrics can be sampled via LSM.

### Metadata flow (Phase 0/1)
```
Data plane (TableSet) -> metadata snapshot -> Planner (control)
                                              |
                                              v
                                    Compaction plan (TableMeta IDs)
                                              |
                                              v
                                 Data plane resolves handles + runs
```

## Async model
- Write path: WAL append (batch + fsync as configured) -> memtable apply -> return.
- Sequence assignment: data writes use the committed data entry `Seq` before WAL append. The local provider derives it from the ordered commit index; replicated providers must supply the same sequence on every replica.
- Background flush: channel of drained memtables; workers write SSTables and update manifest.
- Compaction: scheduled by size/level thresholds; merges SSTables asynchronously.
- Backpressure: when flush queue is full, writes return `ErrBackpressure` and a background goroutine
  blocks until the flush queue has capacity; WAL lag tracking is planned.
- Snapshots: freezing the active memtable pins it until closed; release enqueues flush.
- Snapshots also pin SSTables to prevent compaction from deleting files still visible to readers.

## Recovery behavior
- WAL replay rehydrates memtables and will flush to SSTables when the memtable limit is reached.
- Manifest load favors checkpoints but can fall back to log replay or SSTable scanning when needed.

### Ownership model (single copy)
LSM performs a single copy of key/value into memtable-owned memory and passes the
owned entry to both WAL and memtable. This avoids double-copy while keeping the
public API safe (callers can reuse their buffers after `Put/Delete`).

Public reads (`Get`, snapshot `Range`) return owned copies; zero-copy views are
internal only (SSTable `GetView`).

```
client []byte
   |
   | (single copy into memtable arena)
   v
owned entry (stable memory)
   |                 |
   |                 +-> WAL.AppendOwned (no copy)
   v
memtable.ApplyOwned (no copy)
```

## Data flow (write + flush)
```
client
  |
  v
 WAL append (batch + fsync as configured)
  |
  v
 Active memtable
  |
  | (size threshold)
  v
 Freeze -> Immutable memtable
  |
  v
 Flush worker -> SSTable
```

## Read path
```
Get(key):
  Active memtable
    -> Immutable memtables (newest -> oldest)
      -> SSTables (newest -> oldest)
```

```
Snapshot range scan:
  Freeze active memtable
    -> Immutable memtables (newest -> oldest)
      -> Merge iterator (dedupe + tombstone filtering)
      -> SSTable range (newest -> oldest)
```

## Deferred: distributed/replication
Distributed replication is intentionally out of scope until core LSM work is finished.
See `docs/design.md` for the deferred backlog and sequencing.

## Deferred: manifest performance
Future optimization: async table-edit apply (runtime-first) with deferred WAL checkpoint
and obsolete SSTable cleanup. This improves throughput but adds crash-recovery complexity.

## Deferred: async I/O
Future optimization: async I/O/worker pools for WAL/flush/compaction to reduce blocking.
Evaluate io_uring integration paths and define fallbacks for non-Linux platforms.
Tune batching/backpressure to minimize context switches under sustained write load.
IO backend injection now exists via `IOFS`; a selectable backend can be set via
`IOBackend` (`os`, `async`, `io_uring`). The async wrapper can be enabled with
`IOAsyncMaxInFlight`, and the next step is a Linux `io_uring` backend plus
platform fallbacks. `io_uring` requires Linux kernel 5.6+ and auto-enables
SSTable mmap when no explicit setting is provided.
Goal: share IO backend abstractions across service, WAL, SSTable, and compaction
to reduce copies and enable kernel-assisted paths (page cache, sendfile, io_uring).

## Deferred: refactor backlog
Technical issues:
- Throttling Consistency: write throttling lacks reservation, risks over-limit.
  Mitigation: check under the same lock as pointer acquisition or implement reservation.

Backlog:
- WAL: tail/truncate policy and payload caps; faster resync scanning for corrupted blocks; async writer metrics/backpressure; codec versioning; large replay + mixed corrupt/missing segment tests.
- Memtable: streaming iterators to avoid snapshot copying; shard auto-tuning; lock contention and tail-latency benchmarks; tighter immutable/flush state machine.
- Skiplist: arena node allocation; comparator coverage tests; iterator performance benchmarks.
- Read path: snapshot observability (replay counts, pinned memtable metrics).
- Compaction: optional flush coalescing; pluggable storage (local vs object store); output size caps + multi-output split; scheduler/backpressure/priority policy.
- Observability/ops: metrics and health endpoints.
- Distributed/Replication: transport + term gating; external term manager integration; replay checkpoints to avoid resending histories.

## Data layout (local FS)
- `<data>/wal.log`: current WAL.
- `<data>/sstables/` for immutable runs (e.g., `sstable-<seq>-<id>.sst`).
- `<data>/manifest.json`: active table set + WAL checkpoints.
  - If the checkpoint is corrupt, the log is replayed from scratch.
  - If the manifest is missing/invalid but SSTables exist, the engine rebuilds the manifest by scanning the SSTable directory.
  - If there are no SSTables, startup proceeds and WAL replay reconstructs memtables.
- `<data>/trash/`: cyclic trash for obsolete files (SSTables, temp files), pruned by size/count.
- `<data>/control_state.json`: control-plane state snapshot (shards/order/leaders/draining + node/cluster identity).
  - Persisted atomically via temp file + rename.
  - If file is missing: bootstrap from `ShardMap` (or default shard).
  - If file is invalid or identity mismatches (`cluster_id`/`node_id`): startup fails fast.
  - If shard layout is invalid (overlap, bad bounds, open-ended shard not last): startup fails fast.

## Configuration knobs
- Memtable: `MemtableKind` (`map`, `skiplist`, `sharded-skiplist`), `MemtableConcurrency`, `MemtableShards`, `MemtableArenaBlockSize`.
- WAL: `WALBlockSize`, `WALMaxRecord`, `WALAsync`, `WALQueueDepth`, `WALBatchMax`.
- Replay: `WALAutoRepair`, `WALMissingSegmentPolicy`, `ReplayBatchSize`.
- Cleanup: `TrashDir`, `TrashMaxBytes`, `TrashMaxFiles`.
- IO: `IOFS` for custom filesystem/IO backends (e.g., io_uring on Linux),
  `IOBackend` for selecting the backend, `IOAsyncMaxInFlight` to wrap reads/writes
  with an async worker pool.

Example:
```
opts := engine.Options{
  DataDir:             "/var/lib/lsm",
  IOBackend:           "io_uring",
  IOBackendStrict:     false,
  IOAsyncMaxInFlight:  128,
}
```
- SSTable: block sizes, compression, bloom/caches/prefetch, `FlowObserver`, `PolicyOverride`.
- SSTable: `SSTable` options (block sizing, restart interval/adaptive, compression, bloom bits per key, block cache bytes, index/filter cache bytes, read buffer cap, mmap reads, prefetch blocks/bytes/budget/async, checksum).

## Next steps (implementation order)
1) Snapshot range iterator over SSTables (merge + tombstone filtering).
2) Metrics/health endpoints and basic benchmarks.
