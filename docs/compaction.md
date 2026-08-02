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
- Operators can manually wake the node-local compaction runtime with
  `POST /compact` or `lsmctl compact --addr <url>`. This is a scheduling trigger,
  not a forced rewrite: the configured planner still decides whether current
  table metadata satisfies compaction policy.
- `CompactionCheckInterval` / `compaction_check_interval` can also wake the same
  runtime periodically for long-running nodes. `0` disables periodic checks.
- `CompactionAdaptiveCheck` / `compaction_adaptive_check` keeps the configured
  interval as the baseline but shortens the next periodic wake when L0 table
  pressure reaches the compaction threshold. This is a local scheduling policy;
  it does not change planner correctness or force compaction when metadata does
  not satisfy the configured policy.
- `Stats()` and `/stats` report L0 table count/bytes and whether the configured
  L0 threshold has been reached. They also report whether adaptive checks are
  enabled and the current pressure-adjusted effective check interval. These are
  pressure signals for operators and tests, not a complete durable debt
  scheduler.
- The compaction runtime also reports process-local trigger, coalesced trigger,
  run, step, successful-step, error, and running counters. These counters help
  distinguish "threshold reached" from "runtime is executing or failing" without
  introducing a durable compaction job model.
- Basic write admission can reject new local writes with `ErrBackpressure` when
  configured flush queue or L0 pressure thresholds are reached. This admission
  happens before commit-log proposal; committed entries still apply locally even
  when the node is under local compaction pressure.
- Richer durable debt scheduling, priority policy, and write throttling remain
  planned.
