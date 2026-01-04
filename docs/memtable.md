# Memtable Design

Goal: low-latency writes with ordered iteration for flush and range scans.

## Role
- **Active memtable** absorbs new writes.
- **Immutable memtables** are frozen snapshots queued for flush.
- **SSTables** are the durable ordered layer written from immutables.

## Default implementation: sharded skiplist
We use a sharded skiplist so writes only lock one shard while reads/flush
retain ordered iteration.

```
shards = nextPow2(GOMAXPROCS * MemtableConcurrency)

key -> hash -> shard i
                     +--------------------+
                     | shard i            |
                     |  RWMutex           |
                     |  SkipList (ordered)|
                     +--------------------+
```

If `MemtableShards` is set, it overrides the computed shard count. The default
`MemtableConcurrency` is 2.

Other implementations (select via `MemtableKind`):
- `skiplist`: single skiplist, no sharding.
- `map`: hash-based table with sorted snapshots for iteration.

## Skiplist structure (per shard)
Implementation lives in `internal/lsm/memtable/skiplist`.
```
level N:  head -> node -> node -> ...
level 1:  head -> node -> node -> ...
level 0:  head -> node -> node -> ...
```

Operations:
- **Upsert**: expected O(log n) search + insert/update.
- **IterFrom(start)**: finds first node >= start, then walks level 0 in order.

## k-way merge iterator (across shards)
Each shard yields an ordered iterator. The merge picks the smallest key from the
front of each shard iterator.

```
shard0: a -> d -> g
shard1: b -> e -> h
shard2: c -> f -> i

merge:
  step1: pick a
  step2: pick b
  step3: pick c
  ...
  result: a b c d e f g h i
```

## Interfaces
- `Get` for point lookups.
- `ApplyOwned` for entries already owned by the caller (avoids extra copies)
- `Iter()` for ordered full scan (used for flush)
- `Range(start,end)` for ordered range scan
- `CopyEntry` for creating table-owned slices without insertion (used by LSM).

## Concurrency model
- Writes lock only the target shard.
- Iter/Range take a snapshot per table (no long-lived locks during iteration).
- Reads check active, then immutables, then SSTables.
- Flush iterates immutable memtables only (no writes on those tables).

## Memory allocation
- Keys/values are copied into a per-memtable arena to reduce GC pressure.
- Sharded tables use one arena per shard to keep allocations local to the lock.
- Arena block size is configurable via `MemtableArenaBlockSize` (default 256KB).
- After a memtable flush completes, the table is reset and returned to a pool for reuse.

LSM uses `CopyEntry` to create an owned entry once, then feeds that entry to
WAL and memtable without further copies.

## Snapshot range semantics

Snapshots capture a point-in-time view by freezing the active memtable and
pinning it until the snapshot is closed. Range scans merge immutable memtables
from newest to oldest, de-duplicate keys, and hide tombstones. SSTable range
scans are not yet supported, so snapshot range iterators will return an error
once SSTables are involved.

## Data flow (write + flush)
```
client
  |
  v
 WAL append
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
