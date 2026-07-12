# Docker Compose Cluster Smoke

This example starts a static three-node LSMEngine cluster with the builtin
`etcd-raft` commit-log provider and HTTP peer transport.

It is a packaging smoke test for the current raft foundation:

- three `lsmctl serve` processes run in separate containers;
- node peer traffic uses Compose service names;
- writes are issued through node-a with `local_committed` consistency;
- reads verify that committed data is applied on followers.

This is not full dynamic cluster management yet. The CLI has manual raft and
shard membership commands, but automated replacement policy, full LSM
state-machine snapshot transfer orchestration, and service discovery remain
future work.

## Run

```bash
examples/docker-compose-cluster/smoke.sh
```

The script builds the server image, waits for all three health endpoints, writes
and deletes a key through `lsmctl`, and tears the cluster down unless
`LSM_COMPOSE_KEEP=1` is set.

Useful environment overrides:

- `LSM_COMPOSE_PROJECT`: Compose project name, default `lsmengine-cluster`.
- `LSM_COMPOSE_KEEP=1`: leave containers and volumes running after the smoke.
- `LSMCTL_BIN=/path/to/lsmctl`: use an existing CLI binary instead of
  `go run ./cmd/lsmctl`.

## Rolling restart smoke

```bash
examples/docker-compose-cluster/rolling-restart.sh
```

This starts the same static three-node cluster, drains one node at a time with
`lsmctl drain-node`, stops it, uses `lsmctl put --cluster` to commit a write
through the remaining quorum, restarts the stopped node with its existing
volume, resumes it with `lsmctl resume-node`, and verifies all three nodes can
read the write before the next node is restarted.

## Manual commands

```bash
docker compose -f examples/docker-compose-cluster/docker-compose.yml up -d --build
go run ./cmd/lsmctl put --addr http://127.0.0.1:8080 --key compose --value ok
go run ./cmd/lsmctl get --addr http://127.0.0.1:8081 --key compose
docker compose -f examples/docker-compose-cluster/docker-compose.yml down -v
```
