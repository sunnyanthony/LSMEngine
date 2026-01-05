# Compaction Engine (draft)

Conclusion:
Tombstone is not "keep until Lmax"; it is "keep until we can prove all older
versions are covered." TTL is only an acceleration tool, not a correctness
requirement. Correctness comes from compaction invariants; reclamation speed
comes from TTL/GC policy.

## Goals
- Strict levelled compaction by default (production-safe, predictable).
- Pluggable policy and scheduling for future variants.
- Fault tolerant: crash-safe outputs, idempotent retries.
- Cloud-native: immutable outputs, portable across nodes.

## Components
```
CompactionPlanner -> CompactionRunner -> CompactionApplier
         |                    |                |
         |                    v                v
      Policy            SSTable Writer     Manifest update
```

## Interfaces (conceptual)
- Planner: selects input tables and output level based on policy.
- Runner: performs k-way merge and writes new SSTables.
- Scheduler: enqueues plans based on pressure/backlog signals.
- Applier: atomically swaps manifest and deletes obsolete tables.

## Strict levelled policy (default)
Invariants:
- L0 may overlap; levels L1+ are non-overlapping by key range.
- Each level has a size limit; when exceeded, compact into next level.
- Output tables are sorted, non-overlapping within their target level.

Selection:
- L0: compact when file count exceeds threshold.
- L1+: compact when level size exceeds limit; pick overlapping ranges.

## Configuration (all user-configurable)
- L0 file count threshold.
- Per-level size targets.
- Max output table size.
- Optional TTL policy for tombstone acceleration.

## Tombstone handling
- Tombstones are kept until we can prove no older versions exist in lower
  levels or remote replicas that still need them.
- Once a tombstone is "covered" by compaction invariants, it can be dropped.
- TTL can optionally speed this up (policy), but does not replace invariants.

## Fault tolerance
- Compaction outputs are written to temp files.
- Manifest update is atomic (write temp + fsync + rename).
- Old files are only deleted after manifest commit.
- Retry is safe because inputs are immutable.

## Async scheduling and backpressure
- Scheduler runs in background.
- Backpressure triggers when flush queue is saturated or compaction backlog grows.
- Metrics should surface pending compactions and bytes-per-level.

## Distributed considerations
- Compaction is local; replication relies on WAL/apply order.
- SSTables are immutable and transferable for catch-up or bulk sync.
