# SSTable Design (draft)

Goal: immutable, crash-safe, and portable on-disk tables with fast point lookups
and range scans. The design favors streaming writes and bounded memory.

## Format overview (ASCII)
```
+------------------+----------------+----------------+--------------+
| Data Blocks ...  | Index Block(s) | Meta Block     | Footer       |
+------------------+----------------+----------------+--------------+
| header+payload+trailer            | stats+filters  | magic+offset |
+------------------+----------------+----------------+--------------+
```

## Package layout (internal/lsm/sstable)
- `sstable/`: Reader/Writer controllers and public SSTable API surface.
- `config/`: Options, policy snapshots, flow observers, metrics.
- `flow_pipeline.go`: Read pipeline DAG (filter/index/data/decode/prefetch).
- `bloom/`: Bloom filter + partitioned filter writer.
- `format/`: Block/index/meta encoding, compression IDs, checksums.
- `cache/`: Block/index/filter caches.
- `storage/`: File/mmap block source + buffer pool.

## Read/Write flow (ASCII)
```
Write:
  sorted entries
    -> data block builder
    -> [block header + payload + trailer]
    -> index entries
    -> (optional) filter partitions + filter index
    -> meta
    -> footer

Read (Get):
  footer
    -> top index (or index partition)
    -> data block
    -> restart search + seq/tombstone resolution
```

## Control vs Data
- Reader/Writer act as controllers: they assemble index -> (filter) -> data -> decode; policy (prefetch/corruption/cache) is supplied via PolicySnapshot/Options.
- FlowObserver receives node events; MetricsObserver aggregates cache/filter hits and errors by default.
- Cache stores on-disk data/index/filter blocks only; memtable is not cached here.
- Prefetch is driven by a single budget (bytes or blocks); async/queue/worker settings flow through PolicySnapshot.

Control plane vs data plane (conceptual):
```
Controller
  | (plan)
  v
Meta -> Index -> DataBlock -> Decode -> Entry
       \-> Filter (optional)
       \-> Prefetch (async)
```

## Data block
Data blocks are sorted by key and appended sequentially. Each block is
checksummed for fault tolerance. Block sizing is dynamic:
- `BlockTargetBytes` guides when to seal a block.
- `BlockMaxBytes` is a hard cap; a single large entry may form its own block.

Block layout (v1):
```
| blockMagic | compression | uncompressedLen | payload | blockType | crc32c |
```
`blockMagic` enables resync; `blockType` distinguishes data/index/meta/filter.

Entry encoding (v1, prefix-compressed):
```
| shared u32 | unshared u32 | valLen u32 | seq u64 | flags u8 | unsharedKey | value |
```
- flags: bit0 tombstone
- keys are prefix-compressed against the previous key in the block
- restart points store `shared=0` (full key)

Restart array (block tail):
```
| restartOffset u32 ... | restartCount u32 |
```
- restart interval is configurable (default: 16)
- block trailer (shared with other blocks): `header+payload | blockType | crc32c(header+payload+blockType)`

## Index block
Index block maps `minKey` of each data block to its file offset + length.
This lets readers binary-search the index, then seek to the target data block.

Index entry (v1):
```
| keyLen u32 | key | blockOffset u64 | blockLen u32 |
```
Index blocks are wrapped with the same block header + trailer as data blocks.

### Partitioned index (two-level)
For large tables, the index can be partitioned:
```
Top index (small):  key -> (partition offset, len)
Index partition:    key -> (data block offset, len)
```
`IndexPartitionEntries` controls partition size. When enabled, the footer points
to the top index, which in turn locates the index partition covering a key.

### Partitioned filter
When partitioning is enabled, filters can be split to match index partitions:
```
Filter index:    key -> (filter offset, len)
Filter block:    bloom bits for the partition range
```
`FilterPartitioned` toggles this behavior. A partitioned filter avoids loading a
single large bloom filter for big tables.

## Meta block
Meta block holds table-level statistics:
- minKey / maxKey
- entryCount
- seqMin / seqMax
- bloom filter offset + parameters (bits-per-key)
Meta is written as a block with the same header + trailer as data/index blocks.

## Internal key ordering
Entries are ordered by:
1) `userKey` ascending
2) `seq` descending (newest first)
3) `tombstone` before value (for identical seq)

This ensures the newest version is the first match in a block, so Get can
early-stop on the first `userKey` hit. Range scans and merge iterators can
drop older versions once a key is consumed.

## Footer
Footer is a fixed-size trailer with:
- magic
- version
- offsets/lengths for index + meta
- footer checksum

## Format evolution plan
Planned compatibility steps (no v2 yet):
- Keep v1 fixed-width fields in little-endian.
- Introduce block/entry version tags when adding varint lengths.
- Move meta to TLV so new fields can be ignored by old readers.
- Use footer flags to advertise optional features (partitioned index/filter, new codecs).

## Writer flow
1) Write data blocks in sorted order.
2) Emit index block + meta block.
3) Emit footer.
4) fsync file, rename temp -> final, fsync directory.

Crash safety: incomplete temp files can be deleted on startup.

## Reader flow
1) Read footer, validate checksum.
2) Load index block (and meta).
3) For Get(key): binary-search index (top index if partitioned) -> read index partition (if any) -> read data block -> binary-search via restart points.
4) For Range: iterate index partitions (if any) in order and scan data blocks.
5) Internal scans can use entry views with scratch key materialization; external APIs return safe copies.

## Fault tolerance
- CRC on data/index/meta blocks and footer (CRC32C by default; uses hardware acceleration when available).
- Block trailer stores type + checksum for uniform validation.
- Footer allows detection of truncation and corrupted tail writes.
- Index and meta parsing must fail fast on malformed lengths.
- Corruption handling is configurable (fail-fast, skip-block, drop-table).

## WAL/SSTable convergence (design notes)
Combine WAL-style safety with SSTable performance so both layers share benefits.
Current:
- Block magic + header for data/index/meta/filter (enables resync).
- Unified block trailer: `header+payload | blockType | crc32c(header+payload+blockType)`.
- Payload caps: `ReadBlockMaxBytes` to guard oversized reads from poisoned indexes.
- Corruption policy: fail-fast / skip-block / drop-table.
Next:
- Resync scanning to recover from mid-file corruption.
- Explicit versioning for block/entry evolution.

## Cloud-native / distributed considerations (deferred)
These notes apply to post-core LSM work and remain in the backlog.
- SSTable is immutable and portable; safe to copy between nodes.
- Storage interface should be pluggable (local FS, object store).
- Writes are staged to temp paths to avoid partial visibility.
- Tail layout keeps top index, filter index, meta, and footer contiguous for single range reads.

## Cache and prefetch
- Data blocks are cached in an LRU (configurable size, can be disabled).
- Range scans prefetch using a single budget (bytes or blocks). For compatibility,
  `PrefetchBlocks`/`PrefetchBytes` are folded into the budget when explicit
  budgets are not set.

## Production readiness (latency-focused plan)
Improvements (execution order):
1) Read buffer pooling for index/filter partitions to reduce short-lived allocations. (done)
2) Bounded LRU caches for index partitions and filter partitions (separate caps). (done)
3) Optional `GetView` for zero-copy point lookups. (done)
4) Async prefetch with bounded queue/worker count (opt-in). (done)
5) Read-budget prefetch tuning (bytes vs blocks) and adaptive restart interval. (done)

## Configuration (all user-configurable)
Recommended defaults:
- `BlockTargetBytes`: 64KB
- `BlockMaxBytes`: 256KB
- `RestartInterval`: 16
- `RestartIntervalAdaptive`: false (if true, adjusts within min/max based on prefix similarity)
- `RestartIntervalMin`: defaults to `RestartInterval`
- `RestartIntervalMax`: defaults to `RestartInterval`
- `IndexPartitionEntries`: 256 (0 disables partitioning)
- `FilterPartitioned`: true (partitioned bloom filter aligned with index partitions)
- `ReadBlockMaxBytes`: 4x `BlockMaxBytes` (guardrail; configurable)
- `Compression`: snappy
- `BloomBitsPerKey`: 10 (enabled)
- `BlockCacheBytes`: 64MB
- `IndexCacheBytes`: 1/8 of `BlockCacheBytes` (derived; set to -1 to disable)
- `FilterCacheBytes`: 1/8 of `BlockCacheBytes` (derived; set to -1 to disable)
- `PrefetchBlocks`: 2
- `PrefetchBytes`: 0 (sliding window in bytes)
- `PrefetchBudgetBlocks`: 0 (total blocks prefetch budget for range iterators)
- `PrefetchBudgetBytes`: 0 (total bytes prefetch budget for range iterators)
- `PrefetchAsync`: false
- `PrefetchQueueDepth`: 64
- `PrefetchWorkers`: 1
- `ReadBufferMaxBytes`: defaults to `ReadBlockMaxBytes` (pool cap for index/filter reads; set to -1 to disable)
- `UseMmap`: false (optional zero-copy reads via memory mapping)
- `Checksum`: CRC32C
- `CorruptionPolicy`: fail-fast (skip-block/drop-table optional)

## Future extensions (non-blocking)
- Additional compression codecs (zstd).
- Hardware-accelerated hash options for stronger checksums.
- Optional compression for index/meta/filter blocks (configurable; default off).
