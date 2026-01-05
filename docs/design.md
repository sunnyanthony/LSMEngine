# Design Index

Start here for the overall architecture and component relationships:
- `docs/architecture.md`

Focused specs:
- `docs/memtable.md`
- `docs/sstable.md`
- `docs/wal.md`
- `docs/compaction.md`

## Backlog (non-blocking)

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
- SSTable range scans for snapshots (iterator + merge, tombstone filtering).
- Snapshot observability (replay counts, pinned memtable metrics).

SSTable:
- Snapshot range scan integration (merge iterator across memtable + SSTable).
- Crash-safety: fsync parent directory after rename.
- Corruption coverage: CRC/compression/bloom error handling tests.
- Data block prefix compression + restart array.
- Block trailer checksum for data/index/meta (uniform block type + CRC).
- Partitioned index + tiny top index (two-level index).
- Partitioned filter (Bloom now, optional Ribbon later).
- InternalKey ordering spec (userKey asc, seq desc, type) and early-stop rules.
- Object store read amplification controls (footer/meta/top index co-located, fewer larger index partitions, byte-based prefetch).
- Format evolution plan (varint lengths, explicit endianness, TLV meta).

Compaction:
- Compaction runner (k-way merge + manifest apply + obsolete cleanup).
- Pluggable storage interface (local FS vs object store for SSTables).
