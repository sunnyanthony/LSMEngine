# WAL Format (draft)

Goal: enable fast replay and recovery under corruption with minimal scanning.

## Layout overview (ASCII)
Segment file:
```
+--------------------+--------------------+--------------------+-----
| Segment Header     | Block Header       | Block Payload      | ...
+--------------------+--------------------+--------------------+-----
| LSMW + fields + CRC| LSMB + len + CRC   | records...         |
+--------------------+--------------------+--------------------+-----
```

Record (inside block payload):
```
+---------+-----+--------+--------+--------------+-----+--------+
| Flags   | Seq | KeyLen | ValLen | Key bytes    | Val | CRC    |
+---------+-----+--------+--------+--------------+-----+--------+

## Code layout
- Format framing and encode/decode helpers live in `internal/lsm/wal/codec`.
- Segment discovery helpers live in `internal/lsm/wal/segment`.
- Append/replay logic lives in `internal/lsm/wal`.
```

## Segment header
Each WAL segment starts with a fixed header:
- Magic: `LSMW` (4 bytes)
- Version: `u8`
- Segment ID: `u64` (monotonic per node)
- CreatedAt: `u64` (unix nanos)
- Header CRC: `u32` (CRC32 over header fields)

If header CRC fails, the entire segment is skipped and a warning is emitted.

## Segment retention marker
Archived segment retention is node-local. When retention removes an archived
prefix such as `wal.log.1` through `wal.log.N`, it writes `wal.log.pruned`
containing the highest pruned segment ID. Segment discovery treats that marked
prefix as intentional pruning while still reporting gaps after the first
retained archived segment as `ErrMissingSegment`.

Retention only removes a contiguous archived prefix after each segment has been
fully scanned and every entry in that segment is covered by the manifest WAL
checkpoint. The active `wal.log` is never removed by retention.

## Readiness gate
`WALReadyMaxCheckpointLag` / `wal_ready_max_checkpoint_lag` is an optional
node-local readiness gate. When greater than zero, `Health()` and `/readyz`
return `ready=false` with reason `wal_checkpoint_lag` once the latest local
sequence is more than that many entries ahead of the manifest WAL checkpoint.
This lets supervisors stop sending normal traffic to a node whose local WAL
retention debt is growing. It does not reject raft committed apply, does not
make CDC durable, and does not replace raft log retention or snapshots.

## Write-admission backpressure
`WALBackpressureMaxCheckpointLag` / `wal_backpressure_max_checkpoint_lag` is
an optional local write-admission gate. When greater than zero, new local writes
are rejected with `ErrBackpressure` before commit-log proposal once the latest
local sequence is more than that many entries ahead of the manifest WAL
checkpoint.

This is intentionally scoped to local write admission. Committed raft entries
still apply locally even while this gate is active, so a lagged node can catch up
instead of rejecting already-committed data.

## Block framing
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

## Record format (v1)
Record payload (inside block):
- Flags: `u8` (bit0 tombstone)
- Seq: `u64`
- KeyLen: `u32`
- ValLen: `u32`
- Key bytes
- Val bytes
- Record CRC: `u32` (CRC32 over record payload)

## Ownership and copying
- `AppendOwned` transfers ownership of key/value to the WAL; callers must not
  mutate or reuse those slices after the call. Violating this contract can
  corrupt the WAL because CRCs are computed at append time.
- LSM performs a single internal copy into memtable-owned memory and then calls
  `AppendOwned`, so external callers do not need to manage ownership.

## Resync strategy
On decode failure:
1) Skip to next block magic (`LSMB`) within the segment.
2) Validate block CRC, then continue decoding records.
3) If no further block magic is found, move to next segment.

## Error handling policy
- Missing/corrupt segments: WAL returns `ErrMissingSegment`/`ErrCorruptSegment`, LSM decides
  whether to ignore missing segments (policy) and whether to auto-repair corrupt tails.
- Empty key/value: rejected at WAL append; tombstones allowed with empty value.
  LSM validates empty key/value before WAL append for fast fail.

## Durability notes
- `WALSync=true`: append returns only after the WAL block is flushed and `fsync` completes.
- `WALSync=false`: append still flushes the WAL block but does **not** call `fsync`.
  This survives process crashes but may lose data on power loss or OS crash.

## Replay behavior
- WAL replay rehydrates memtables and flushes to SSTables once the memtable limit is reached.
