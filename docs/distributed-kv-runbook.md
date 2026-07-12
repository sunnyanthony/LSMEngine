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
- peer transport uses HTTP URLs from `raft.peer_urls`;
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
curl -s http://127.0.0.1:8080/cluster/status
curl -s http://127.0.0.1:8081/cluster/status
curl -s http://127.0.0.1:8082/cluster/status
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

If the command is sent to a follower, the server may return a retryable
`not_leader` route hint. `server.Gateway` consumes those hints for embedded
clients; direct CLI users should retry against the current leader shown by
`/cluster/status`.

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
2. Find the current write leader from `/cluster/status`.
3. Restart one non-leader first:

   ```bash
   docker compose -p lsmengine-cluster \
     -f examples/docker-compose-cluster/docker-compose.yml restart node-b
   ```

4. Wait for `curl -fsS http://127.0.0.1:8081/healthz`.
5. Write through the current write leader and read from every node.
6. Repeat one node at a time. Keep two nodes online throughout the operation.

The current foundation does not include automatic leader drain or automated
raft leadership transfer. Operator tooling should therefore restart one node at
a time and verify quorum health between steps.

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
the other pods. This is still a static bootstrap path; it does not provide
service discovery, automated membership repair, or persistent volumes.

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
- orchestrated drain/restart workflows;
- persistent volumes in Kubernetes examples;
- mixed-version compatibility tests;
- automatic raft membership lifecycle around node replacement;
- stronger chaos and upgrade coverage.

The external dependency rule also applies here: etcd-raft remains behind the
builtin commit-log provider. Operator-facing APIs and docs should use
LSM-owned concepts such as committed entries, runtime status, raft peer message
envelopes, and shard replica metadata rather than etcd raft protocol types.
