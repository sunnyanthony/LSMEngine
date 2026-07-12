# Docker Compose Cluster Smoke

This example starts a static three-node LSMEngine cluster with the builtin
`etcd-raft` commit-log provider and HTTP peer transport.

It is a packaging smoke test for the current raft foundation:

- three `lsmctl serve` processes run in separate containers;
- node peer traffic uses an operator-managed `peer-urls.yaml` file with Compose
  service names;
- writes are issued through cluster-aware `lsmctl` routing with
  `local_committed` consistency;
- reads verify that committed data is applied on followers.
- optional node-d replacement coverage is available through the `replacement`
  profile.

This is not full dynamic cluster management yet. The CLI has manual raft and
shard membership commands plus a planned `replace-node` workflow, but automated
replacement triggers, full LSM state-machine snapshot transfer orchestration,
and service discovery remain future work.

The server configs mount `peer-urls.yaml` as `raft.peer_url_file`. The gateway
service mounts the same file through `lsmctl gateway --endpoint-file`, while the
operator smoke scripts generate a separate host-side endpoint file and pass it
through `lsmctl --config`. That keeps server peer transport, gateway routing,
and CLI operator commands on the same endpoint-file contract.

## Run

```bash
examples/docker-compose-cluster/smoke.sh
```

The script builds the server image, waits for all three health endpoints, writes
and deletes a key through cluster-aware `lsmctl`, and tears the cluster down unless
`LSM_COMPOSE_KEEP=1` is set.

Useful environment overrides:

- `LSM_COMPOSE_PROJECT`: Compose project name, default `lsmengine-cluster`.
- `LSM_COMPOSE_KEEP=1`: leave containers and volumes running after the smoke.
- `LSMCTL_BIN=/path/to/lsmctl`: use an existing CLI binary instead of
  `go run ./cmd/lsmctl`.

## Gateway smoke

```bash
examples/docker-compose-cluster/gateway-smoke.sh
```

This starts node-a/node-b/node-c, waits for cluster readiness, starts
the Compose `gateway` service on `127.0.0.1:8090`, then verifies ordinary
`lsmctl put/get/delete --addr http://127.0.0.1:8090` calls work through the
single gateway endpoint. The gateway routes writes to the current raft write
leader, loads node endpoints from the mounted `peer-urls.yaml`, and uses
best-effort endpoint fallback for reads. The smoke also verifies
`/gateway/status` reports all three backend nodes and the current write leader.

## Rolling restart smoke

```bash
examples/docker-compose-cluster/rolling-restart.sh
```

This starts the same static three-node cluster, drains one node at a time with
`lsmctl drain-node`, stops it, uses `lsmctl wait-cluster --min-ready 2` to
verify the remaining quorum, uses `lsmctl put --cluster` to commit a write,
restarts the stopped node with its existing volume, resumes it with
`lsmctl resume-node`, and verifies all three nodes can read the write before the
next node is restarted.

## Replacement smoke

```bash
examples/docker-compose-cluster/replace-node-smoke.sh
```

This starts node-a/node-b/node-c, writes a committed value, starts node-d with
`raft.join: true`, preflights `lsmctl replace-node --dry-run`, runs
`lsmctl replace-node --old-node node-a --new-node node-d`, verifies node-d can
read the committed value, stops node-a, waits for the node-b/node-c/node-d
quorum, then verifies it can accept and read a new committed write.

## Failed replacement smoke

```bash
examples/docker-compose-cluster/failed-replacement-smoke.sh
```

This starts node-a/node-b/node-c, writes committed values, stops node-a before
replacement, uses `lsmctl wait-cluster --min-ready 2` to verify the surviving
quorum, starts node-d with `raft.join: true`, runs `lsmctl replacement-plan`,
then runs `lsmctl replacement-apply`. It waits for the node-b/node-c/node-d
quorum, verifies node-d catches up to values committed before and after node-a
stopped, then verifies the new cluster can accept and read a committed write.

## Manual commands

```bash
docker compose -f examples/docker-compose-cluster/docker-compose.yml up -d --build
go run ./cmd/lsmctl wait-cluster \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082
go run ./cmd/lsmctl put --cluster \
  --node-endpoint node-a=http://127.0.0.1:8080 \
  --node-endpoint node-b=http://127.0.0.1:8081 \
  --node-endpoint node-c=http://127.0.0.1:8082 \
  --key compose --value ok
go run ./cmd/lsmctl get --addr http://127.0.0.1:8081 --key compose
docker compose -f examples/docker-compose-cluster/docker-compose.yml down -v
```
