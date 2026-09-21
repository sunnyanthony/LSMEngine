# Follower Apply Implementation Gate

Status: work in progress on the dedicated follower-apply PR. This document is an
implementation checklist, not a claim of supported multi-node operation.

## Observed Gap

HTTP peer delivery can complete a real two-node election and data commit. The
provider retains committed entries, but a running follower does not apply them
to its LSM. Restart recovery is not a substitute for live application.

## Required Invariants

- One ordered committed-entry apply path must serve local proposals and inbound
  replication. Local proposal completion must not let later entries overtake an
  earlier unapplied entry.
- Provider interfaces use LSM-owned committed-entry types, never raft protobuf
  or an apply callback supplied with a proposal.
- Engine application must not run while holding a provider mutex. Applying a
  control operation must not reacquire a proposal lock held by a caller waiting
  for the same committed operation.
- Data application uses the committed sequence, writes the local WAL, applies
  the memtable, and emits node-local CDC once. Duplicate delivery is idempotent.
- Control application uses the same deterministic mutation and operation-ID
  bookkeeping on leader, follower, and restart recovery. Local node identity and
  node-specific draining state remain local.
- A failed apply cannot be hidden by advancing the applied index past the failed
  entry. Subsequent application must retry in order or fail closed, and restart
  must retain the failed entry for recovery.
- Startup recovery retains synchronous flushing before dispatcher startup;
  runtime apply respects backpressure without dropping committed entries.
- Apply-worker shutdown, if used, must participate in engine lifecycle rather
  than leaving untracked goroutines using a closed WAL or control store.

## Verification Before Merge

1. Real HTTP peers: leader put, overwrite, delete are visible on followers with
   matching committed sequences, without restarting the follower.
2. Duplicate inbound delivery does not duplicate CDC or control revisions.
3. Replicated control operations preserve operation identity and deterministic
   rejection semantics.
4. Interleaved local proposals and inbound entries preserve commit order.
5. Inject WAL, control-save, and backpressure failures; verify no skipped entry
   and correct recovery after reopening.
6. Existing PR #23 recovery and PR #24 transport regressions remain passing.
7. Full unit, tagged integration, relevant race suites, current-head CI, and a
   fresh independent review must pass before merge.

Tick-driven elections, membership changes, snapshots, and linearizable reads are
separate remaining distributed-database milestones, not implied by this feature.
