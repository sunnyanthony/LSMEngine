# LSMEngine Design

Goals:
- Cloud-native: deterministic data layout for container mounts/PVCs, observable background work, safe restarts.
- Async-first: writes return after WAL durability; flush/compaction/replication run in background workers with backpressure.
- Pluggable sync: replication transport interface so different distributed protocols can be swapped in.

## Components
- **LSM façade**: orchestrates memtable, WAL, flush/compaction, and manifests that track active SSTables and WAL offsets.
- **Memtable**: ordered structure (future: skiplist/B-tree) for recent writes. Supports `Put/Get/Delete/Drain`. Flush triggers on size.
- **WAL**: append-only, fsync-configurable. On startup, replay to rebuild memtable. Provides a tail for replication.
- **Flush/compaction**: background workers turn drained memtables into SSTables, merge runs, and drop obsolete tombstoned data.
- **SSTable**: immutable runs with index + data blocks. Later: Bloom filters, compression, block cache.
- **Manifest**: durable metadata describing current table set and WAL checkpoints.
- **Transport (replication)**: publish/subscribe interface fed by WAL tail and applied by a replica apply loop.
- **Observability**: metrics for memtable size, flush/compaction latency, backlog depth; health endpoints to signal readiness/liveness.

## Async model
- Write path: WAL append (+fsync as configured) -> memtable insert -> return.
- Background flush: channel of drained memtables; workers write SSTables and update manifest.
- Compaction: scheduled by size/level thresholds; merges SSTables asynchronously.
- Backpressure: when flush queue is full or WAL lag grows, throttle writers or trigger synchronous flush.

## Pluggable replication
- Define `Transport` with `Publish` and `Subscribe` to abstract protocol. Examples: gRPC stream, NATS, Kafka, Raft-like RPCs.
- WAL tailer batches entries to `Publish`; subscriber feeds apply queue that replays mutations in order.
- Future: epoch/term for multi-writer safety; idempotent apply keyed by sequence + node ID.

## Data layout (local FS)
- `<data>/wal.log`: current WAL.
- `<data>/sstables/` for immutable runs (e.g., `sstable-<seq>.sst`).
- `<data>/manifest.json`: active table set + WAL checkpoints.

## Next steps (implementation order)
1) WAL replay on startup; fsync toggle in options; deterministic data dir layout.
2) Background flush worker + ordered memtable; replace placeholder SSTable dump with writer that creates indexed blocks.
3) Manifest persistence and lookup path (memtable -> newest SSTables).
4) Transport interface + loopback implementation to validate replication plumbing.
5) Metrics/health endpoints and basic benchmarks.
