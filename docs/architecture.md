# LSMEngine Architecture

Goals:
- Cloud-native: deterministic layout for container mounts, observable background work, safe restarts.
- Async-first: WAL append can be async with group commit; flush/compaction/replication run in background workers with backpressure.
- Pluggable sync: replication transport interface so different distributed protocols can be swapped in.

## Components
- **LSM facade**: orchestrates memtable, WAL, flush/compaction, and manifest.
- **Memtable**: in-memory ordered index for recent writes; drained to SSTables. See `docs/memtable.md`.
- **Snapshot**: point-in-time view that freezes the active memtable for range scans.
- **WAL**: append-only, fsync-configurable; replay on startup. See `docs/wal.md`.
- **Flush/compaction**: background flush worker turns drained memtables into SSTables; compaction is planned.
- **Compaction engine**: strict levelled policy by default, pluggable for future variants. See `docs/compaction.md`.
- **SSTable**: immutable runs with block index + meta; compression, bloom filter, cache/prefetch are configurable.
- **Manifest**: durable metadata describing current table set and WAL checkpoints.
- **Transport (replication)**: planned publish/subscribe interface fed by WAL tail; current event bus offers local hooks.
- **Observability**: event bus hooks; metrics for size/latency/backlog are planned.

## Async model
- Write path: WAL append (batch + fsync as configured) -> memtable apply -> return.
- Sequence assignment: LSM assigns a monotonic `Seq` before WAL append to keep ordering consistent.
- Background flush: channel of drained memtables; workers write SSTables and update manifest.
- Compaction: scheduled by size/level thresholds; merges SSTables asynchronously.
- Backpressure: when flush queue is full, LSM triggers synchronous flush; WAL lag tracking is planned.
- Snapshots: freezing the active memtable pins it until closed; release enqueues flush.

### Ownership model (single copy)
LSM performs a single copy of key/value into memtable-owned memory and passes the
owned entry to both WAL and memtable. This avoids double-copy while keeping the
public API safe (callers can reuse their buffers after `Put/Delete`).

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

## Planned replication
- Define `Transport` with `Publish` and `Subscribe` to abstract protocol (gRPC, NATS, Kafka, Raft-like RPCs).
- WAL tailer batches entries to `Publish`; subscriber feeds apply queue that replays mutations in order.
- Future: epoch/term for multi-writer safety; idempotent apply keyed by sequence + node ID.

## Data layout (local FS)
- `<data>/wal.log`: current WAL.
- `<data>/sstables/` for immutable runs (e.g., `sstable-<seq>.sst`).
- `<data>/manifest.json`: active table set + WAL checkpoints.

## Configuration knobs
- Memtable: `MemtableKind` (`map`, `skiplist`, `sharded-skiplist`), `MemtableConcurrency`, `MemtableShards`, `MemtableArenaBlockSize`.
- WAL: `WALBlockSize`, `WALMaxRecord`, `WALAsync`, `WALQueueDepth`, `WALBatchMax`.
- Replay: `WALAutoRepair`, `WALMissingSegmentPolicy`, `ReplayBatchSize`.
- SSTable: `SSTable` options (block sizing, compression, bloom bits per key, block cache bytes, prefetch blocks, checksum).

## Next steps (implementation order)
1) Snapshot range iterator over SSTables (merge + tombstone filtering).
2) Transport interface + loopback implementation to validate replication plumbing.
3) Metrics/health endpoints and basic benchmarks.
