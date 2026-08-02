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
`/healthz`, waits for cluster readiness, writes with cluster-aware
`local_committed` routing, verifies follower reads and range reads, deletes the
key, then tears the cluster down.

The Compose server configs use `raft.peer_url_file` mounted at
`/etc/lsm/peer-urls.yaml` for peer transport. The rolling restart and
replacement smoke scripts also generate a host-side `lsmctl --config` with a
`raft.peer_url_file`, so operator commands do not need repeated
`--node-endpoint` flags in that path.

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

Wait until the local cluster is ready for committed writes:

```bash
go run ./cmd/lsmctl wait-cluster \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082 \
  --timeout 60s
```

By default, `wait-cluster` requires every configured endpoint to report a
healthy `ready` or `follower` runtime state and requires one write-available
raft leader. Use `--min-ready 2` for a planned degraded-quorum operation, or
`--write-leader=false` when only endpoint/status reachability matters. For a
specific committed write, use the write response's `seq` as
`--min-applied-index <seq>` so counted ready nodes must have materialized that
commit into local LSM/control state. `--max-apply-lag <n>` is also available for
raw runtime gap checks, but that gap can include provider/internal raft entries
and is not a precise pending-KV-mutation count. During rolling upgrades, add
`--require-compatible` so counted ready nodes must report the same LSM-owned
compatibility versions in `/cluster/status`.

The useful fields are:

- `commit_log_runtime.mode`: should be `raft_transport_foundation` for the
  current static multi-peer foundation;
- `commit_log_runtime.index`: latest commit-log index observed by this node's
  provider;
- `commit_log_runtime.applied_index`: latest commit-log index materialized into
  this node's LSM/control state;
- `commit_log_runtime.apply_lag`: provider index minus local applied index; a
  non-zero value means the node may still be catching up even if it is reachable;
- `commit_log_runtime.replicas`: should be `3`;
- `commit_log_runtime.leader`: true only on the current raft write leader;
- `commit_log_runtime.write_available`: true only where local committed writes
  can currently be proposed;
- `commit_log_runtime.health`: `ready` on the leader, `follower` on healthy
  followers, and `no_leader` or `unavailable` during election or quorum loss.
- `lsmctl gateway-status` `routing_*` fields: process-local gateway routing
  counters. Rising `routing_write_retries` with successful writes usually means
  route hints or refreshes are masking stale metadata. Rising
  `routing_write_failures` or `routing_route_refresh_failures` means the gateway
  is no longer hiding the backend or metadata failure. Each backend line also
  includes process-local `backend_*` counters for read, write, and status-probe
  attempts/successes/failures so operators can identify which endpoint is
  unstable before adding richer weighting or external supervision.
  Tune bounded write retry behavior with `lsmctl gateway --max-write-attempts`,
  `lsmctl gateway --write-retry-backoff`, or the matching
  `gateway_max_write_attempts` / `gateway_write_retry_backoff` config keys.
- `Stats.WAL.SegmentCount`, `Stats.WAL.TotalBytes`, and related
  `lsmctl stats` `wal_*` fields: local WAL segment pressure for one node. Use
  them with SSTable and compaction stats to understand whether a node is
  accumulating write-ahead log data faster than flush/compaction can keep up.
  `wal_checkpoint_seq` and `wal_checkpoint_lag` show how far the local manifest
  WAL checkpoint trails the latest committed local sequence; high checkpoint
  lag means archived WAL may not yet be safe to prune. Configure
  `wal_ready_max_checkpoint_lag` to make `/readyz` report
  `reason=wal_checkpoint_lag` when this local retention debt exceeds the
  supervisor threshold. Configure `wal_backpressure_max_checkpoint_lag` to
  reject new local writes before commit-log proposal when the same debt exceeds
  the write-admission threshold. That gate does not reject committed raft apply.
  `wal_max_segment_bytes` rotates the active WAL segment after flushed bytes
  reach the configured threshold, but archived segment retention remains a
  separate policy decision.
  `wal_retain_archived_segments` can prune checkpointed archived WAL prefixes
  while retaining the configured number of newest archived segments; this is
  node-local storage cleanup, not a distributed replication health signal.
- Server-mode `GET /metrics`: text scrape surface for the same node-local LSM,
  WAL, compaction, write-backpressure, and point-read counters exposed by
  `Stats()`. It includes the configured compaction/backpressure/WAL thresholds
  and periodic compaction check interval so alerts can compare pressure against
  policy. Use it for per-node monitoring;
  it is not a cluster-wide durability or replication health summary.
- `lsmctl cdc-status --addr <node-url>` prints the node-local CDC source,
  durability, retention capacity, `start_offset`, and per-shard retained
  windows. Current CDC is `source=memory`, `durable=false`, and
  `replay_on_restart=false`.
- `lsmctl cdc-events --addr <node-url> --shard <id> --offset <n>` prints
  retained CDC events after an offset. Use `start_offset` and
  `dropped_before` to detect retention overflow, restart, or state-snapshot
  restore gaps; resync through normal KV reads when a gap is reported.
- `POST /compact` and `lsmctl compact --addr <node-url>` wake one node's local
  compaction runtime. This is useful after inspecting L0/table pressure, but it
  does not bypass the planner's configured policy and is not a cluster-wide
  rebalance or repair operation.
- `compaction_check_interval` can perform the same node-local wakeup
  periodically for long-running nodes. Keep it disabled with `0` when flush and
  manual triggers are enough.

## Manual KV Commands

Use `lsmctl` against the running Compose cluster:

```bash
go run ./cmd/lsmctl put --addr http://127.0.0.1:8080 --key user:1 --value alice
go run ./cmd/lsmctl get --addr http://127.0.0.1:8081 --key user:1
go run ./cmd/lsmctl range --addr http://127.0.0.1:8082 --start user: --end user~ --limit 10
go run ./cmd/lsmctl delete --addr http://127.0.0.1:8080 --key user:1
go run ./cmd/lsmctl cdc-status --addr http://127.0.0.1:8080
go run ./cmd/lsmctl cdc-events --addr http://127.0.0.1:8080 --shard default --offset 0 --limit 10
go run ./cmd/lsmctl compact --addr http://127.0.0.1:8080
```

For cluster-aware reads and writes, provide the node endpoint map:

```bash
go run ./cmd/lsmctl get --cluster \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082 \
  --key user:1

go run ./cmd/lsmctl put --cluster \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082 \
  --key user:1 --value alice
```

For reads, `--cluster` tries the configured endpoints in stable node order and
returns the first successful response. For writes, `--cluster` polls the
configured node endpoints, finds the current `commit_log_runtime.write_available`
node, transfers shard leadership to that node if needed, and then sends the
write there. Without `--cluster`, direct CLI users should retry against the
current leader shown by `lsmctl cluster-status` or `/cluster/status` if a write
is sent to a follower.

Cluster/operator commands can also load node endpoints from `--config` when the
server config contains `raft.peer_url_file`, `raft.peer_urls`, or
`raft.join_peer_urls`. Endpoint files are useful when an operator or supervisor
updates node-to-URL discovery data independently of the running server process;
explicit `--addr` and repeated `--node-endpoint` flags still override config
values for one-off commands.

Internally, `lsmctl` cluster commands and the route-aware `server.Gateway` use
the LSM-owned `NodeEndpointResolver` contract. Static maps and a reloaded
node-endpoint file resolver are available behind that layer for long-running
gateways or supervisors. Gateways can also use DNS SRV discovery through
`--endpoint-dns-name`, `--endpoint-dns-service`, and `--endpoint-dns-proto`.
The same gateway endpoint discovery settings can live in server config as
`gateway_endpoint_file` or `gateway_endpoint_dns_*`; explicit gateway CLI flags
override config for one-off processes.
The DNS SRV resolver keeps platform lookup behavior behind the same LSM-owned
resolver layer instead of adding provider-specific lookups directly to gateway
routing. Future service-registry discovery should plug into the same contract.

For a single client-facing endpoint, run `lsmctl gateway`:

```bash
go run ./cmd/lsmctl gateway \
  --listen 127.0.0.1:8090 \
  --bootstrap-url http://127.0.0.1:8080 \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082 \
  --read-mode leader \
  --write-consistency-default local_committed
```

Clients can then use normal non-cluster commands against the gateway:

```bash
go run ./cmd/lsmctl put --addr http://127.0.0.1:8090 --key user:1 --value alice
go run ./cmd/lsmctl get --addr http://127.0.0.1:8090 --key user:1
go run ./cmd/lsmctl range --addr http://127.0.0.1:8090 --start user: --end user~ --limit 10
go run ./cmd/lsmctl async-put --addr http://127.0.0.1:8090 --key user:2 --value bob
go run ./cmd/lsmctl write-status --addr http://127.0.0.1:8090 --request-id <request_id>
go run ./cmd/lsmctl async-delete --addr http://127.0.0.1:8090 --key user:2
go run ./cmd/lsmctl gateway-status --addr http://127.0.0.1:8090
go run ./cmd/lsmctl wait-gateway --addr http://127.0.0.1:8090 --min-reachable 3 --read-mode leader
```

The gateway exposes `/kv/put`, `/kv/delete`, `/kv/get`, `/kv/range`,
`/kv/write-status/{request_id}`, `/healthz`, `/readyz`, and `/gateway/status`.
`/healthz` reports gateway process liveness; `/readyz` reports whether the
gateway can currently see a backend commit-log write leader. Writes are
route-aware and retry stale leader metadata through `server.Gateway`.

Gateway reads default to `--read-mode any`, which uses best-effort endpoint
fallback and `--read-balance-policy round_robin` healthy-endpoint rotation. Use
`--read-balance-policy ordered` or `gateway_read_balance_policy: "ordered"` when
the gateway should keep sorted endpoint order while still deferring recently
failed endpoints behind healthy ones. Use `--read-balance-policy freshest` or
`gateway_read_balance_policy: "freshest"` when any-mode KV reads should first
probe backend `/cluster/status` and prefer the reachable backend with the lowest
reported `commit_log_runtime.apply_lag`. The Compose and kind gateway examples use
`--read-mode leader`, which sends `/kv/get` and `/kv/range` only to the current
commit-log write leader and returns unavailable when no leader can be
identified. This avoids stale follower reads for clients that want to prefer the
node accepting committed writes, but it is not raft ReadIndex, lease-read, or a
complete linearizable read protocol. Accepted write status lookups keep
best-effort endpoint fallback because the request-status tracker is node-local.
Use `--read-balance-policy adaptive` or
`gateway_read_balance_policy: "adaptive"` when any-mode reads should prefer
backends with fewer process-local read, status-probe, and write failures, using
lower read-attempt counts as a tie-breaker among equally healthy endpoints. This
policy uses only the current gateway process's counters; it is not a durable or
cluster-wide load balancer.
For `any` mode, `--max-read-apply-lag <n>` or
`gateway_max_read_apply_lag: <n>` makes the gateway probe backend
`/cluster/status` before `/kv/get` and `/kv/range`, then skip endpoints whose
reported `commit_log_runtime.apply_lag` is above the configured bound. `-1`
disables this gate. This is a freshness guard for obviously lagged followers,
not a linearizable read protocol.

Smoke tests and operator rollouts can add the same freshness bound to gateway
readiness:

```bash
go run ./cmd/lsmctl wait-gateway --addr http://127.0.0.1:8090 --min-reachable 3 --read-mode any --max-read-apply-lag 2 --min-read-ready 2
```

`--max-read-apply-lag` makes `wait-gateway` count reachable backends whose latest
reported `commit_log_runtime.health` is `ready` or `follower` and whose
`commit_log_runtime.apply_lag` is within the bound. `--min-read-ready` sets the
required count; when the max lag gate is enabled and `--min-read-ready` is
omitted or `0`, the wait requires at least one read-ready backend. The same
checks can be applied to the gateway `/readyz` endpoint with:

```bash
go run ./cmd/lsmctl gateway --ready-min-reachable <n> --ready-max-read-apply-lag <n> --ready-min-read-ready <n>
```

The matching `gateway_ready_*` config keys provide the same behavior, so external
supervisors can use one healthcheck instead of a separate wait loop.
These are operator/status gates only and do not change gateway read routing.

The gateway keeps short-lived backend endpoint health state: transport failures
and 5xx responses put an endpoint behind healthy endpoints for a cooldown
window, while successful probes clear that state. Tune the window with
`lsmctl gateway --endpoint-failure-cooldown` or
`gateway_endpoint_failure_cooldown` in server config; `0` uses the gateway
default. Healthy read endpoints rotate by default so a single stable gateway
does not always send reads to the same backend in `any` mode. `/gateway/status`
includes `read_mode`, `read_balance_policy`, `max_read_apply_lag`, per-backend
`degraded`, and `degraded_until` fields so operators can see how reads are
configured and when gateway routing is temporarily avoiding an endpoint. The
status routing block also includes process-local `read_attempts`, `read_fallbacks`, and
`read_failures` counters so operators can see whether fallback is doing useful
work or masking backend instability. Per-backend status lines and metrics expose
process-local read/write/status-probe attempts, successes, and failures for the
same reason; these counters are diagnostic hints, not durable cluster-wide
history.
`/gateway/metrics` exposes the same gateway readiness, backend health, apply
lag, and routing counters in Prometheus text format for scraping. It is still
process-local gateway telemetry, not a durable cluster-wide metrics store.
`/gateway/status` is the gateway's aggregated backend-node view, separate from a
node server's local `/cluster/status`; `lsmctl gateway-status` prints that view
from the single gateway endpoint. `lsmctl wait-gateway` polls that same view for
deployment scripts that need a bounded wait for reachable backends, the expected
read mode, and a visible write leader. Use the Compose gateway smoke for a
repeatable local check:

```bash
examples/docker-compose-cluster/gateway-smoke.sh
```

The smoke runs gateway as a Docker Compose service using the `gateway` profile
and exposes it at `http://127.0.0.1:8090`, so the local client talks to one
stable endpoint while raft peer traffic stays inside the Compose network. The
Compose gateway mounts the same `peer-urls.yaml` endpoint file as the server
containers and passes it to `lsmctl gateway --endpoint-file`; production-style
gateway processes can put the same path in `gateway_endpoint_file` when they
want endpoint discovery to come from config. The smoke also covers the
file-backed node endpoint resolver used by long-running gateways. It verifies
`/readyz` reports backend write readiness and uses
`lsmctl wait-gateway` to wait until `/gateway/status` sees all three backend
nodes, leader read mode, a write leader, and at least one read-ready backend
within the default apply-lag bound. Override that smoke gate with
`LSM_GATEWAY_READ_READY_MIN` and `LSM_GATEWAY_READ_READY_MAX_LAG` when testing
slower environments; set `LSM_GATEWAY_READ_READY_MAX_LAG=-1` to disable the
smoke read-ready gate. The Compose gateway service healthcheck uses
`lsmctl health --ready` so container health follows backend write readiness, not
just process liveness. The smoke covers point reads, range scans, committed
writes/deletes, accepted writes/deletes, and accepted write-status lookup through
the single gateway endpoint.

The Compose gateway defaults to leader read mode, but the same smoke can run the
gateway in any-mode adaptive reads:

```bash
LSM_GATEWAY_READ_MODE=any \
LSM_GATEWAY_READ_BALANCE_POLICY=adaptive \
LSM_GATEWAY_MAX_READ_APPLY_LAG=2 \
examples/docker-compose-cluster/gateway-smoke.sh
```

The smoke waits for committed writes to apply on all three nodes before reading,
so this validates deploy-time read policy wiring without claiming linearizable
follower reads.

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

The Compose and kind smokes run their cluster waits with
`--require-compatible`, so they fail if ready nodes report different
LSM-owned compatibility versions during restart or catch-up checks.

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
StatefulSet identity, with a single in-cluster gateway Service for client
traffic:

```bash
examples/kind-cluster/smoke.sh
```

The pod names are the raft node ids:

- `lsm-cluster-0`;
- `lsm-cluster-1`;
- `lsm-cluster-2`.

The smoke runs `lsmctl` inside the first server pod, sends client writes and
reads through `http://lsm-gateway:8090`, verifies `lsmctl gateway-status` can see
all three backend nodes plus a write leader, and reads from follower pods
directly to prove the gateway write reached replicated cluster state. The
gateway discovers backend node endpoints from Kubernetes DNS SRV records through
the LSM-owned DNS node endpoint resolver. The gateway smoke wait also requires
at least one read-ready backend within the configured apply-lag bound before
client traffic; follower catch-up is still verified later with
`wait-cluster --min-applied-index`. Set `LSM_GATEWAY_READ_READY_MAX_LAG=-1` to
disable only the smoke read-ready gate. The StatefulSet mounts a per-pod
`ReadWriteOnce` PVC at `/data`, so committed raft state, WAL, SSTables, and
control state survive pod replacement.

Use the persistent restart smoke to verify pod replacement:

```bash
examples/kind-cluster/restart-smoke.sh
```

This is still a static bootstrap path. `lsmctl raft-add-node`,
`lsmctl raft-remove-node`, `lsmctl shard-add-replica`, and
`lsmctl shard-remove-replica` provide manual membership primitives for
operators. `lsmctl replace-node` composes those primitives for a planned
replacement when the replacement node is already running and reachable.
`raft.peer_url_file` can provide operator-managed endpoint updates for both
server peer transport and `lsmctl` operator commands without restarting existing
nodes. Automated membership repair and process supervision remain outside this
path.

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
When the plan command uses `--config`, the suggested commands preserve that
config path instead of expanding every resolved endpoint, so endpoint-file based
operator workflows remain copyable.

Replacement preflight also enforces a quorum policy per affected shard: existing
replicas other than the old node must still have a healthy majority according to
`/cluster/shards` and `/cluster/status`. For the default three-node shape,
node-b and node-c must both be healthy before replacing node-a. This prevents
the replacement workflow from converting a degraded cluster into a non-quorum
membership change.

One-shot supervisor/operator execution:

```bash
go run ./cmd/lsmctl replacement-apply \
  --new-node node-d \
  --retry-attempts 3 \
  --retry-backoff 1s \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082 \
  --node-endpoint node-d=http://127.0.0.1:8083
```

`replacement-apply` runs the same planning step and then executes the
replacement sequence. `--retry-attempts` and `--retry-backoff` provide bounded
operator-level retries around the whole plan/apply sequence, using the same
idempotency key prefix for committed shard mutations. After `raft-add`, it waits
for the replacement node to report healthy commit-log status,
`commit_log_runtime.apply_lag <= --max-catchup-apply-lag` (default `0`), and
`applied_index` at least as high as the current healthy existing replicas
observed during the wait. It still rejects zero or multiple unavailable old-node
candidates unless `--old-node` is provided. It is intentionally not a background
repair loop; an external supervisor remains responsible for starting the
replacement process, writing endpoint discovery data, choosing retry policy, and
deciding when to invoke the command.

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
uses the same preflight before it adds `--new-node` as a raft voter, waits for
the replacement node catch-up gate, adds it as a shard replica for those shards,
drains the old node, removes the old shard replicas, and removes the old raft
voter. Use repeated `--shard` flags to constrain the replacement to specific
shards. `--catchup-timeout` controls the post-`raft-add` wait and `0` disables
it for explicit emergency operation; `--max-catchup-apply-lag` controls the
accepted replacement-node lag. Use `--allow-unavailable-old-node` only for
failed-node replacement; ordinary maintenance drains should keep waiting for the
target node to report `draining=true`.

Use the Compose replacement smoke for a repeatable local check:

```bash
examples/docker-compose-cluster/replace-node-smoke.sh
```

The script starts the static three-node cluster, starts node-d as a join-mode
replacement service, runs `lsmctl replace-node --dry-run`, runs the real
`lsmctl replace-node --old-node node-a --new-node node-d`, verifies node-d can
read committed data, stops node-a, waits for the final node-b/node-c/node-d
quorum with `lsmctl wait-cluster`, and verifies node-b/node-c/node-d can still
commit and read data.

Use the failed-node replacement smoke to verify the one-shot supervisor path:

```bash
examples/docker-compose-cluster/failed-replacement-smoke.sh
```

The script stops node-a before replacement, verifies node-b/node-c can still
commit through quorum, starts node-d, runs `lsmctl replacement-plan`, runs
`lsmctl replacement-apply`, waits for the final node-b/node-c/node-d quorum with
`lsmctl wait-cluster`, and verifies node-d catches up to committed values from
before and after the old node failed.

## Local State Snapshots

For offline embedded or single-node recovery workflows, export the local
LSM-owned state-machine payload while no other process owns the data directory:

```bash
go run ./cmd/lsmctl snapshot-export --data-dir ./data --out ./state-snapshot.json
```

Restore that payload into a fresh empty data directory:

```bash
go run ./cmd/lsmctl snapshot-restore --data-dir ./restore-data --in ./state-snapshot.json
```

Restore is intentionally empty-engine only. It fails if the target already has
local data, a committed log position, or non-empty control state. In a raft
cluster, lagging follower reset still happens through provider-owned raft
snapshot delivery, where the builtin provider applies the same LSM-owned
payload after the matching raft snapshot index. Do not treat
`snapshot-export` / `snapshot-restore` as an online cluster backup protocol or
as a replacement for raft log retention, raft snapshots, or quorum.

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
- broader mixed-version compatibility tests beyond the current compatibility
  status/wait gate and control-state future-version fail-fast guard;
- richer policy for raft/shard membership lifecycle around node replacement;
- stronger chaos and upgrade coverage.

The external dependency rule also applies here: third-party libraries must sit
behind LSM-owned adapter layers before they influence public, server, or
operator-facing APIs. `internal/lsm/iofs` is the IO example; etcd-raft follows
the same rule through the builtin commit-log provider and peer transport
envelopes. Operator-facing APIs and docs should use LSM-owned concepts such as
committed entries, runtime status, compatibility versions, raft peer message
envelopes, and shard replica metadata rather than etcd raft protocol types.
