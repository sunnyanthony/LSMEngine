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
Coordinator (optional)
   |
   v
Planner -> Runner -> Applier
   |         |         |
   |         v         v
Policy   SSTable IO  TableSet/Manifest
```

## Current implementation
- `StrictLevelledPlanner`: L0 file-count threshold only (metadata-driven).
- `SimpleRunner`: k-way merge across input SSTables, newest version per key,
  optional tombstone drop.
- `Applier`: implemented by the LSM engine; applies table edits (TableSet + manifest) and removes obsolete files.
- `Coordinator` (optional): orchestrates planner/runner/applier and collects metrics.

## Interfaces (conceptual)
- Planner: selects input tables and output level based on metadata only.
- Runner: performs k-way merge over resolved table handles and writes outputs.
- Scheduler: enqueues plans based on pressure/backlog signals.
- Applier: applies table edits (add/remove) to TableSet + manifest.

## Strict levelled policy (default)
Invariants:
- L0 may overlap; L1+ non-overlap is planned.
- Level size targets and L1+ selection are planned.
- Output tables are sorted; single-level (L0->L1) is currently implemented.

Selection:
- L0: compact when file count exceeds threshold.
- L1+: planned (size-based selection and overlapping ranges).

## Metadata inputs
Compaction decisions are made on `TableMeta` only (level, key range, seq bounds,
size). The data plane resolves table handles at execution time to keep the
control plane decoupled from IO.

## Configuration (all user-configurable)
- L0 file count threshold.
- Per-level size targets.
- Max output table size.
- Optional TTL policy for tombstone acceleration.
- `CompactionL0Threshold` (LSM option; enables background compaction when > 0).
- `CompactionDropTombstones` (drop tombstones during compaction when safe).

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
- Compaction is triggered on flush events and runs in the background.
- Scheduler/backpressure policies are planned.
