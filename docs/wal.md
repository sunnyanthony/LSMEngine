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
- Format framing and encode/decode helpers live in `pkg/lsm/wal/codec`.
- `pkg/lsm/wal` owns append/replay behavior and policy decisions.
```

## Segment header
Each WAL segment starts with a fixed header:
- Magic: `LSMW` (4 bytes)
- Version: `u8`
- Segment ID: `u64` (monotonic per node)
- CreatedAt: `u64` (unix nanos)
- Header CRC: `u32` (CRC32 over header fields)

If header CRC fails, the entire segment is skipped and a warning is emitted.

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
- `Append` copies key/value, so callers can reuse or mutate buffers after the call.
- `AppendOwned` transfers ownership of key/value to the WAL; callers must not
  mutate or reuse those slices after the call. Violating this contract can
  corrupt the WAL because CRCs are computed at append time.
- LSM uses a single internal copy into memtable-owned memory and then calls
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
