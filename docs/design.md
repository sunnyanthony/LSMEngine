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

## WAL Format (draft)
Goal: enable fast replay and recovery under corruption with minimal scanning.

### Segment header
Each WAL segment starts with a fixed header:
- Magic: `LSMW` (4 bytes)
- Version: `u8`
- Segment ID: `u64` (monotonic per node)
- CreatedAt: `u64` (unix nanos)
- Header CRC: `u32` (CRC32 over header fields)

If header CRC fails, the entire segment is skipped and a warning is emitted.

### Block framing
Records are grouped into fixed-size blocks. The block size is configurable via options
and stored in the segment header (default 64KB). Each block:
- Magic: `LSMB` (4 bytes)
- Block length: `u32` (bytes of payload)
- Block CRC: `u32` (CRC32 over block payload)
- Payload: a sequence of records

Block payload length is capped by the segment `BlockSize`. If a payload length exceeds
that cap or a block is truncated, the block is treated as corrupt and replay attempts
resync to the next block magic.

Corrupt blocks are skipped; replay continues at the next block magic.

### Record format (v1)
Record payload (inside block):
- Flags: `u8` (bit0 tombstone)
- Seq: `u64`
- KeyLen: `u32`
- ValLen: `u32`
- Key bytes
- Val bytes
- Record CRC: `u32` (CRC32 over record payload)

### Resync strategy
On decode failure:
1) Skip to next block magic (`LSMB`) within the segment.
2) Validate block CRC, then continue decoding records.
3) If no further block magic is found, move to next segment.

### Error handling policy
- Missing/corrupt segments: WAL returns `ErrMissingSegment`/`ErrCorruptSegment`, LSM decides
  whether to auto-repair (log + continue) or fail startup (option-controlled).
- Empty key/value: rejected at WAL append; tombstones allowed with empty value.

### Decisions (current)
- Block size is configurable and persisted in the segment header.
- Double CRC is used (record CRC + block CRC) for defense-in-depth; may add a toggle later.

### Open questions
- Whether to add periodic index markers for faster seek.
