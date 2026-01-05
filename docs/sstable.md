# SSTable Design (draft)

Goal: immutable, crash-safe, and portable on-disk tables with fast point lookups
and range scans. The design favors streaming writes and bounded memory.

## Format overview (ASCII)
```
+------------------+----------------+----------------+--------------+
| Data Blocks ...  | Index Block(s) | Meta Block     | Footer       |
+------------------+----------------+----------------+--------------+
| entries + CRC    | keys+offsets   | stats+filters  | magic+offset |
+------------------+----------------+----------------+--------------+
```

## Data block
Data blocks are sorted by key and appended sequentially. Each block is
checksummed for fault tolerance. Block sizing is dynamic:
- `BlockTargetBytes` guides when to seal a block.
- `BlockMaxBytes` is a hard cap; a single large entry may form its own block.

Entry encoding (v1):
```
| keyLen u32 | valLen u32 | seq u64 | flags u8 | key | value |
```
- flags: bit0 tombstone
- data blocks can be compressed (default: snappy)

## Index block
Index block maps `minKey` of each data block to its file offset + length.
This lets readers binary-search the index, then seek to the target data block.

Index entry (v1):
```
| keyLen u32 | key | blockOffset u64 | blockLen u32 |
```

## Meta block
Meta block holds table-level statistics:
- minKey / maxKey
- entryCount
- seqMin / seqMax
- bloom filter offset + parameters (bits-per-key)

## Footer
Footer is a fixed-size trailer with:
- magic
- version
- offsets/lengths for index + meta
- footer checksum

## Writer flow
1) Write data blocks in sorted order.
2) Emit index block + meta block.
3) Emit footer.
4) fsync file, rename temp -> final, fsync directory.

Crash safety: incomplete temp files can be deleted on startup.

## Reader flow
1) Read footer, validate checksum.
2) Load index block (and meta).
3) For Get(key): binary-search index -> read block -> binary-search inside block.
4) For Range: iterate index blocks in order and scan data blocks.

## Fault tolerance
- CRC on data blocks and footer (CRC32C by default; uses hardware acceleration when available).
- Footer allows detection of truncation and corrupted tail writes.
- Index and meta parsing must fail fast on malformed lengths.

## Cloud-native / distributed considerations
- SSTable is immutable and portable; safe to copy between nodes.
- Storage interface should be pluggable (local FS, object store).
- Writes are staged to temp paths to avoid partial visibility.

## Cache and prefetch
- Data blocks are cached in an LRU (configurable size, can be disabled).
- Range scans can prefetch the next N blocks (configurable, can be disabled).

## Configuration (all user-configurable)
Recommended defaults:
- `BlockTargetBytes`: 64KB
- `BlockMaxBytes`: 256KB
- `Compression`: snappy
- `BloomBitsPerKey`: 10 (enabled)
- `BlockCacheBytes`: 64MB
- `PrefetchBlocks`: 2
- `Checksum`: CRC32C

## Future extensions (non-blocking)
- Prefix-compressed keys within blocks.
- Additional compression codecs (zstd).
- Hardware-accelerated hash options for stronger checksums.
