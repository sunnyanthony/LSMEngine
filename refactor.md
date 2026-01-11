# LSMEngine Refactor Design Doc

## Purpose
This document defines a refactor direction that preserves existing capabilities while restoring
low coupling, low latency, and a clean control/data separation. It avoids implementation details
and focuses on design decisions, tradeoffs, and a phased plan.

## What The Project Provides (Core Value)
- Low-latency key/value reads and writes with WAL durability.
- Ordered iteration and range scans via memtable and SSTables.
- Background flush and compaction for storage efficiency.
- Snapshot isolation for consistent reads.
- Optional replication and observability.
- Zero-copy internal data flow backed by memory arenas and pools.

## Current Pain Points
- Control logic and data logic are interleaved, increasing coupling and change risk.
- I/O pathways are bound to OS files, blocking future io_uring adoption.
- Manifest updates are spread across multiple paths, increasing state drift risk.
- Backpressure paths can introduce tail-latency spikes when synchronous flushes occur.
- Compaction policy is pluggable in name, but the metadata model is not yet multi-level.

## Design Goals
- Low-latency read/write on the hot path.
- Control/data separation with clean boundaries.
- High replaceability of compaction policy and replication strategy.
- Zero-copy internal flow and explicit memory ownership.
- Minimal public surface area in `pkg/lsm`.
- IO abstraction to enable io_uring without rewriting higher layers.

## Core Principles
- Data plane owns correctness and invariants.
- Control plane owns policies and scheduling.
- External API stays stable and simple.
- Ownership is explicit; no implicit copies on hot paths.
- Metadata is the single source of truth for system state.

## Proposed Architecture Direction

### Data Plane (Internal Only)
- Responsibilities: WAL, memtable, tableset, read path, snapshots.
- Guarantees: correct ordering, atomic table set updates, stable read semantics.
- Emits events and exposes snapshots of table metadata.

### Control Plane (Internal Only)
- Responsibilities: flush scheduling, compaction planning, replication workflows, backpressure policy.
- Consumes data-plane events and metadata snapshots.
- Never mutates data-plane state directly; it submits edits and plans.

### Metadata Model
- Tables are described by minimal metadata: level, key range, size, sequence bounds.
- Table metadata is persisted and replayed at startup.
- Control plane operates on metadata, not raw data structures.

### IO Abstraction
- WAL and SSTable use a shared IO layer.
- The IO layer defines minimal read/write/flush interfaces and owns OS-specific details.
- io_uring support can be added by swapping the IO implementation.

## Backpressure Strategy
- The write path should not perform synchronous flushes.
- On pressure, return a throttle-style error to the caller.
- Throttling can be policy-driven (retry guidance and optional delay), but no implicit blocking.

## Zero-Copy Policy
- External API always returns owned data.
- Internal iteration and read paths use borrowed views with strict lifetime rules.
- Memory pools are used for short-lived buffers only; long-lived data is arena-owned.

## Design Options And Tradeoffs

### Option A: Internal Engine/Control Split
- Pros: cleanest separation, easiest long-term evolution.
- Cons: larger refactor, more internal interfaces.

### Option B: Keep Single LSM Type, Enforce Internal Boundaries
- Pros: minimal public changes, smaller refactor.
- Cons: requires discipline to avoid boundary leakage.

### Option C: Manifest Snapshot Only
- Pros: simple recovery logic.
- Cons: more write amplification, higher latency during frequent updates.

### Option D: Manifest Log + Checkpoint
- Pros: low-latency updates, bounded recovery time.
- Cons: more moving parts, requires checkpoint policy.

## Recommended Direction
- Use Option B as a near-term step and evolve toward Option A internally.
- Use Option D for metadata durability with periodic checkpoints.
- Keep public API stable; refactor internally only.

## Multi-Level Compaction Direction
- Default policy: strict levelled compaction.
- Pluggable policy allows tiered/L0-only variants.
- Metadata must support level-aware planning before introducing multi-L compaction.

## Roadmap (Phased)

### Phase 0: Design Alignment
- [x] Finalize data/control boundary and metadata schema.
- [x] Document ownership rules and zero-copy contract.
- [x] Backpressure is a caller-visible error; no sync flush in the write path.
- [x] Drain is not used on the hot path; only safe when there are no readers.

### Phase 1: Internal Boundaries
- [x] Split public API from engine internals (`pkg/lsm` -> `pkg/lsm/engine`).
- [x] Split compaction into model/data/controller with a single-worker service.
- [x] Consolidate table metadata + tableset into dedicated modules.
- [x] Centralize manifest updates with a single entry point + locking.
- [x] Split memtable into core/table/arena packages.
- [x] Split WAL into core/segment packages.
- [x] Split SSTable into block/bloom/cache/flow/format/index/meta/storage/config.
- [x] Route compaction triggers via flush events (no inline loop in LSM).
- [x] Public API returns owned data; internal flows remain zero-copy.
- [x] Integration: verify SSTable reads work without WAL (reopen after WAL removal).

### Phase 1.5: Process Boundaries (Optional)
- [x] Define a compaction service boundary suitable for external process/RPC.
- [x] Add a compaction scheduler interface (policy + trigger).

### Phase 2: Metadata Durability
- [x] Implement append-only manifest log.
- [x] Add periodic checkpoints and bounded replay time.

### Phase 3: Multi-Level Compaction
- [x] Expand metadata to include level and key ranges.
- [x] Add strict levelled compaction policy and validation.

### Phase 4: IO Abstraction
- [ ] Extract shared IO layer for WAL and SSTable.
- [ ] Provide a path for io_uring-backed implementation.

## Compatibility Constraints
- External API remains stable and minimal.
- Input buffers are always copied at the public boundary.
- WAL replay and SSTable reload remain correct across restarts.

## Non-Goals
- No changes to public API semantics.
- No immediate rewrite of storage formats.
- No requirement to expose internal observability hooks publicly.

## Open Questions
- Preferred boundary between metadata module and compaction policy.
- Exact checkpoint policy thresholds for different deployment profiles.
- How aggressive throttling should be under sustained overload.
