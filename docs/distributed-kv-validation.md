# Distributed KV Validation

## Tested Revision

Runtime: `251bb77` on `codex/feature/replacement-apply-retry`.
The kind restart script in this document's commit additionally uses cluster-aware
CLI writes instead of relying on the elected Raft leader matching the manifest's
initial shard leader. No runtime changes were needed for this acceptance round.

These results cover the reviewed runtime stack, not the current `main` release
or the unreviewed plugin/control WIP branches.

## Acceptance Matrix

| Scenario | Entry Point | Result |
| --- | --- | --- |
| Three-node rolling restart, writes while one node is down, resumed reads | `examples/docker-compose-cluster/rolling-restart.sh` | Passed |
| Planned replacement, catch-up and membership transition | `examples/docker-compose-cluster/replace-node-smoke.sh` | Passed |
| Replacement after node failure | `examples/docker-compose-cluster/failed-replacement-smoke.sh` | Passed in the runtime verification round |
| Gateway any/adaptive reads, three read-ready backends, max lag 2 | `examples/docker-compose-cluster/gateway-smoke.sh` | Passed |
| Each StatefulSet pod replaced with its existing PVC; committed value retained | `examples/kind-cluster/restart-smoke.sh` | Passed after script correction |
| Kubernetes DNS gateway, leader reads, put/range/delete and readiness gates | `examples/kind-cluster/smoke.sh` | Passed |

Successful rows require both exit status zero and the script's final success
marker. Compose tests used separate project names. The kind namespace was absent
before deployment and removed afterward; the existing kind cluster was retained.
Gateway acceptance uses the explicitly configured read policy, not an assumption
that all reachable followers are current.

## Regression Evidence

- Full `go test ./...` and uncached
  `go test -count=1 -tags test ./tests/integration/...` passed on the runtime.
- Delivery/election focused race tests passed 20 repetitions.
- Long-outage catch-up and deliberately mismatched shard/Raft leader restart
  integration tests each passed five repetitions.
- Snapshot tests cover delayed local apply acknowledgements, membership snapshot
  refresh, failed transmission/retry, stale callbacks, engine restart, newer
  durable state preservation, and partial snapshot materialization.

## Remaining Scope

This is evidence for a small manually operated replicated KV cluster, not a
production certification. Leader read routing is not ReadIndex/lease-based
linearizability. CDC remains node-local and non-durable. Automatic membership
orchestration, bounded transport concurrency, a broader network fault matrix,
and plugin lifecycle/security review remain separate work.

Adaptive compaction scheduling and subsequent WAL checkpoint readiness and
backpressure branches still need review against this runtime base. PR publication
and merge are separate from test completion. See the [operator runbook](distributed-kv-runbook.md)
for commands and operating limitations.
