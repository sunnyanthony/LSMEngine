# CloudNativeLSM Alignment & High-Level TODO

## Purpose
This document aligns three sources into one executable baseline:
- The original CloudNativeLSM vision document.
- The current LSMEngine codebase reality.
- Recent architecture decisions from ongoing discussion.

The goal is to keep implementation direction clear and avoid design drift.

## 1) Original Vision (What We Started With)
From the initial CloudNativeLSM plan, the intended north star is:
- Cloud-native distributed KV foundation first (not SQL-first).
- Range-sharded architecture with strong consistency via Raft.
- Separation of correctness layer vs materialization layer.
  - Correctness: replicated log, ordering, durability, failover.
  - Materialization: LSM memtable/SSTable/compaction/indexing/CDC.
- Kubernetes deep integration with Operator-led lifecycle.
- Extensibility toward document/column/vector workloads via plugin/CDC style expansion.
- Storage policy flexibility across local disk, block storage, distributed file systems, and object storage tiers.

## 2) Current Codebase Reality (As-Is)
As of this branch, the system is a strong single-node LSM engine plus an M1 control surface.

### 2.1 Implemented Today
- LSM data path foundation:
  - WAL/memtable/SSTable/manifest/compaction/recovery.
  - Main wiring: `pkg/lsm/engine/new.go`.
- Control surface (M1):
  - Fixed shard map and admin actions:
    - transfer leader, split, rebalance, drain.
  - Core: `pkg/lsm/engine/control_plane.go`.
- Control state persistence:
  - `control_state.json` with shard/runtime state and validation.
- Shard route hardening:
  - Ordered, non-overlapping range validation and deterministic key routing.
- Control operation safety:
  - Monotonic control `revision`.
  - Optional `operation_id` and `expected_revision`.
  - Conflict semantics (`409`) surfaced via server API.
  - Server handler: `pkg/lsm/server/server.go`.
- Test posture:
  - Unit + integration suites exist.
  - Standard full run: `scripts/docker-test.sh`.
- Kubernetes examples:
  - Envoy and UDS sidecar examples under `examples/`.

### 2.2 Not Implemented Yet
- No real Raft consensus runtime (only config model fields like `RaftOptions`).
- No distributed data plane:
  - no quorum write path, no follower replication/catch-up/snapshot install.
- No Operator/CRD control loop for lifecycle automation.
- No storage-tiering backend abstraction for object/distributed FS sync.
- No plugin runtime/SDK in-repo yet (extensibility remains design intent).

## 3) Agreed Decisions (From Discussion)
1. Do not self-implement Raft; use a mature open-source Raft library.
2. Next milestone is Raft adapter foundation, not full distributed data plane in one step.
3. Log ownership model:
   - Single-node mode: LSM WAL is source of truth.
   - Cluster mode: Raft log is source of truth; avoid default double-write with LSM WAL.
4. Storage strategy is profile-based and flexible:
   - Low-latency log path for correctness writes.
   - Tiered/cost-optimized path for colder SSTables/snapshots.
5. Delivery process discipline:
   - One branch = one feature/chore/fix theme.
   - Each feature includes tests and doc updates.

## 4) Target High-Level Architecture

### 4.1 Correctness Layer
- Cluster mode:
  - Raft commit order is authoritative.
  - Control and data mutations become committed log entries before apply.
- Single mode:
  - Existing WAL path remains authoritative.

### 4.2 Materialization Layer
- Reuse current LSM pipeline:
  - memtable -> flush -> SSTable -> compaction.
- Apply committed log entries into LSM state machine.

### 4.3 Control Layer
- Keep existing control APIs and revision/idempotency semantics.
- Move mutation execution behind consensus commit in cluster mode.

### 4.4 Storage Layer
- Introduce storage profiles with policy knobs:
  - local-ssd
  - block-pvc
  - distributed-fs
  - object-tiered
- Keep the correctness log on low-latency storage.
- Tier colder SSTables/snapshots by policy.

### 4.5 Extensibility Layer
- Plugin/CDC should stay off the write commit critical path.
- Event-driven extension model for document/column/vector capabilities.

## 5) High-Level TODO (Phased)

## Phase A: Consensus Foundation (Next)
- [ ] `feature/raft-adapter-foundation`
  - [ ] Integrate open-source raft adapter and local test harness.
  - [ ] Route control mutations through propose -> commit -> apply.
  - [ ] Preserve existing `revision` + `operation_id` semantics.
  - [ ] Add unit/integration tests for commit/apply and conflict behavior.

- [ ] `feature/log-ownership-mode-switch`
  - [ ] Add explicit single/cluster log ownership modes.
  - [ ] Cluster mode: Raft log primary; define LSM WAL behavior.
  - [ ] Document crash/restart contract for each mode.

## Phase B: Storage Policy Foundation
- [ ] `feature/storage-profile-foundation`
  - [ ] Add storage profile config and validation.
  - [ ] Wire compaction and placement defaults by profile.
  - [ ] Add profile matrix docs and tests.

- [ ] `feature/compaction-policy-by-profile`
  - [ ] Tune compaction thresholds/concurrency per profile.
  - [ ] Add guardrails for tail latency and backlog limits.

## Phase C: Tiered Storage & Sync
- [ ] `feature/object-tiered-sstable-sync`
  - [ ] Introduce SSTable location metadata in manifest.
  - [ ] Add async upload/download flow and reconciliation.
  - [ ] Keep log path local/low-latency; do not place correctness log on object storage.

## Phase D: K8s Deep Integration
- [ ] `feature/operator-crd-foundation`
  - [ ] Define `LSMCluster` and policy CRDs.
  - [ ] Implement initial reconcile loop for scale/upgrade/drain orchestration.
  - [ ] Add kind e2e lifecycle tests and runbook docs.

## Phase E: Extensibility
- [ ] `feature/plugin-runtime-foundation`
  - [ ] Define plugin API and execution model.
  - [ ] Keep plugin execution asynchronous and isolated from commit path.
  - [ ] Add reference plugin examples and failure isolation tests.

## 6) Storage Strategy Matrix (High-Level)
| Profile | Correctness Log Path | Hot Read/Write Path | Cold Data Path | Compaction Strategy |
|---|---|---|---|---|
| `local-ssd` | Local SSD/NVMe | Local SSD | Optional object tier | Higher concurrency, tail-latency guardrails |
| `block-pvc` | PVC block volume | PVC block volume | Optional object tier | Balanced concurrency, IOPS-aware throttling |
| `distributed-fs` | Prefer local block for log | Distributed FS + cache | Distributed FS / object | Conservative compaction, reduce metadata churn |
| `object-tiered` | Local block for log | Local cache tiers | Object storage | Tier-aware compaction and upload scheduling |

## 7) Definition of Done (Feature-Level)
Every feature branch should include:
- Code changes scoped to one feature/chore/fix theme.
- Unit tests and integration tests as appropriate.
- Documentation updates (design + API/config if changed).
- Full validation run via `scripts/docker-test.sh`.

## 8) Open Questions to Resolve Early
- Raft library final selection and adapter boundary.
- Cluster-mode WAL policy details (disabled, minimal, or compatibility mode).
- Manifest schema evolution for tiered storage locations.
- Operator scope split between desired-state orchestration and runtime metadata ownership.
