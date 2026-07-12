# Distributed KV Runbook

This runbook is the current operator-facing path for running LSMEngine as a
small static distributed key/value cluster.

It describes what is usable now, how to verify it, and which production
responsibilities are still outside the current foundation.

## Current Contract

The supported distributed shape is a static three-node cluster:

- each node runs `lsmctl serve`;
- `commitlog.provider` is `etcd-raft`;
- all raft peers are declared at startup with `raft.peers`;
- peer transport uses HTTP URLs from `raft.peer_urls`, `raft.join_peer_urls`, or
  a reloaded `raft.peer_url_file`;
- the shard replicas list contains the same three node ids;
- writes use `local_committed` consistency when the caller needs the write to
  be committed and locally applied before the response.

This is enough for a simple replicated KV smoke:

- write through the current raft write leader;
- read the value from any follower after commit delivery;
- stop one node and keep writing through the remaining quorum;
- restart the stopped node with the same data directory and verify it catches up.

## Fast Local Path

Use Docker Compose for the shortest operator loop:

```bash
examples/docker-compose-cluster/smoke.sh
```

The script builds the server image, starts node-a/node-b/node-c, waits for
`/healthz`, writes with `local_committed`, verifies follower reads and range
reads, deletes the key, then tears the cluster down.

Keep the cluster running for manual inspection:

```bash
LSM_COMPOSE_KEEP=1 examples/docker-compose-cluster/smoke.sh
```

Then inspect runtime state:

```bash
go run ./cmd/lsmctl cluster-status \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082
```

The useful fields are:

- `commit_log_runtime.mode`: should be `raft_transport_foundation` for the
  current static multi-peer foundation;
- `commit_log_runtime.replicas`: should be `3`;
- `commit_log_runtime.leader`: true only on the current raft write leader;
- `commit_log_runtime.write_available`: true only where local committed writes
  can currently be proposed;
- `commit_log_runtime.health`: `ready` on the leader, `follower` on healthy
  followers, and `no_leader` or `unavailable` during election or quorum loss.

## Manual KV Commands

Use `lsmctl` against the running Compose cluster:

```bash
go run ./cmd/lsmctl put --addr http://127.0.0.1:8080 --key user:1 --value alice
go run ./cmd/lsmctl get --addr http://127.0.0.1:8081 --key user:1
go run ./cmd/lsmctl range --addr http://127.0.0.1:8082 --start user: --end user~ --limit 10
go run ./cmd/lsmctl delete --addr http://127.0.0.1:8080 --key user:1
```

For cluster-aware writes, provide the node endpoint map:

```bash
go run ./cmd/lsmctl put --cluster \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082 \
  --key user:1 --value alice
```

`--cluster` polls the configured node endpoints, finds the current
`commit_log_runtime.write_available` node, transfers shard leadership to that
node if needed, and then sends the write there. Without `--cluster`, direct CLI
users should retry against the current leader shown by `lsmctl cluster-status`
or `/cluster/status` if a write is sent to a follower.

## Rolling Restart Check

The integration suite covers this workflow with real `lsmctl serve` processes:

```bash
go test -tags test ./tests/integration/server \
  -run TestEtcdRaftThreeProcessRollingRestartSmoke \
  -count=1 -timeout 180s
```

Use the Compose rolling restart smoke for a repeatable local check:

```bash
examples/docker-compose-cluster/rolling-restart.sh
```

For manual Compose validation:

1. Start the cluster with `LSM_COMPOSE_KEEP=1`.
2. Drain the node that will be restarted:

   ```bash
   go run ./cmd/lsmctl drain-node --node node-b \
     --node-endpoint node-a=http://127.0.0.1:8080 \
     --node-endpoint node-b=http://127.0.0.1:8081 \
     --node-endpoint node-c=http://127.0.0.1:8082
   ```

3. Restart one drained node:

   ```bash
   docker compose -p lsmengine-cluster \
     -f examples/docker-compose-cluster/docker-compose.yml restart node-b
   ```

4. Wait for `curl -fsS http://127.0.0.1:8081/healthz`.
5. Resume the restarted node:

   ```bash
   go run ./cmd/lsmctl resume-node --node node-b \
     --node-endpoint node-a=http://127.0.0.1:8080 \
     --node-endpoint node-b=http://127.0.0.1:8081 \
     --node-endpoint node-c=http://127.0.0.1:8082
   ```

6. Write through the current write leader and read from every node.
7. Repeat one node at a time. Keep two nodes online throughout the operation.

The current foundation now includes CLI-assisted drain/resume for static peers.
It still does not include automatic service discovery, process supervision, or
full raft membership replacement orchestration. Operator tooling should restart
one node at a time and verify quorum health between steps.

## Kubernetes Path

Use kind to verify the same static cluster through Kubernetes DNS and
StatefulSet identity:

```bash
examples/kind-cluster/smoke.sh
```

The pod names are the raft node ids:

- `lsm-cluster-0`;
- `lsm-cluster-1`;
- `lsm-cluster-2`.

The smoke runs `lsmctl` inside the first pod and verifies committed writes from
the other pods. The StatefulSet mounts a per-pod `ReadWriteOnce` PVC at `/data`,
so committed raft state, WAL, SSTables, and control state survive pod
replacement.

Use the persistent restart smoke to verify pod replacement:

```bash
examples/kind-cluster/restart-smoke.sh
```

This is still a static bootstrap path. `lsmctl raft-add-node`,
`lsmctl raft-remove-node`, `lsmctl shard-add-replica`, and
`lsmctl shard-remove-replica` provide manual membership primitives for
operators. `lsmctl replace-node` composes those primitives for a planned
replacement when the replacement node is already running and reachable.
`raft.peer_url_file` can provide operator-managed endpoint updates for future
joiners without restarting existing nodes. Automated membership repair and
process supervision remain outside this path.

Supervisor/operator preflight:

```bash
go run ./cmd/lsmctl replacement-plan \
  --new-node node-d \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082 \
  --node-endpoint node-d=http://127.0.0.1:8083
```

`replacement-plan` only reads status and shard metadata. If `--old-node` is not
provided, it selects exactly one endpoint that is unreachable, missing status,
or reporting `commit_log_runtime.health=unavailable`; multiple candidates are
rejected. It reuses the same replacement preflight as `replace-node --dry-run`
and prints suggested dry-run/apply commands. It does not submit raft membership,
shard replica, or drain mutations.

One-shot supervisor/operator execution:

```bash
go run ./cmd/lsmctl replacement-apply \
  --new-node node-d \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082 \
  --node-endpoint node-d=http://127.0.0.1:8083
```

`replacement-apply` runs the same planning step and then executes the replacement
sequence once. It still rejects zero or multiple unavailable old-node candidates
unless `--old-node` is provided. It is intentionally not a background repair
loop; an external supervisor remains responsible for starting the replacement
process, writing endpoint discovery data, choosing retry policy, and deciding
when to invoke the command.

Manual replacement workflow:

```bash
go run ./cmd/lsmctl replace-node \
  --old-node node-a \
  --new-node node-d \
  --dry-run \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082 \
  --node-endpoint node-d=http://127.0.0.1:8083

go run ./cmd/lsmctl replace-node \
  --old-node node-a \
  --new-node node-d \
  --allow-unavailable-old-node \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082 \
  --node-endpoint node-d=http://127.0.0.1:8083
```

The dry run checks endpoint wiring, discovers the current commit-log write
leader, verifies the replacement endpoint reports the expected node id, and
prints the shard replacement plan without submitting mutations. The real command
uses the same preflight before it adds `--new-node` as a raft voter, adds it as a
shard replica for those shards, drains the old node, removes the old shard
replicas, and removes the old raft voter. Use repeated `--shard` flags to
constrain the replacement to specific shards. Use `--allow-unavailable-old-node`
only for failed-node replacement; ordinary maintenance drains should keep waiting
for the target node to report `draining=true`.

Use the Compose replacement smoke for a repeatable local check:

```bash
examples/docker-compose-cluster/replace-node-smoke.sh
```

The script starts the static three-node cluster, starts node-d as a join-mode
replacement service, runs `lsmctl replace-node --dry-run`, runs the real
`lsmctl replace-node --old-node node-a --new-node node-d`, stops node-a, and
verifies node-b/node-c/node-d can still commit and read data.

## Failure Expectations

Expected behavior during common failures:

- one follower down: the remaining quorum can continue accepting
  `local_committed` writes through the current raft write leader;
- leader down: the remaining quorum can elect a new raft leader and continue
  accepting writes after election;
- two nodes down in a three-node cluster: writes must fail with retryable
  commit-log unavailability and must not become locally visible;
- restarted follower: catches up from retained raft entries or provider-owned
  raft snapshot/LSM snapshot data, depending on how far it lagged.

These behaviors are covered by:

```bash
go test -tags test ./tests/integration/server \
  -run 'TestEtcdRaftThreeProcess(Smoke|LeaderRestartSmoke|FollowerLongOutageCatchupSmoke|MinorityPartitionRejectsWrites|RollingRestartSmoke|DynamicJoinSmoke)' \
  -count=1 -timeout 360s
```

## Boundaries

Do not claim production-grade distributed operation yet. The remaining work is:

- service discovery and automatic peer URL reconciliation;
- process supervision and automatic replacement triggers;
- mixed-version compatibility tests;
- richer policy for raft/shard membership lifecycle around node replacement;
- stronger chaos and upgrade coverage.

The external dependency rule also applies here: etcd-raft remains behind the
builtin commit-log provider. Operator-facing APIs and docs should use
LSM-owned concepts such as committed entries, runtime status, raft peer message
envelopes, and shard replica metadata rather than etcd raft protocol types.
