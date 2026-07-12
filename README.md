# LSMEngine

Lightweight LSM tree skeleton in Go. This is a starter layout for a custom NoSQL store and focuses on clarity and hackability over completeness.

## What is here
- LSM facade with memtable, WAL replay, and async flush dispatcher.
- Sharded skiplist memtable (plus map) with ordered iterators.
- Snapshot range scans across memtables + SSTables with merge + tombstone filtering.
- WAL append/replay with corruption repair policy hooks.
- SSTable writer/reader with block index, bloom filter, cache/prefetch, and manifest store.
- Compaction engine skeleton with strict levelled policy (pluggable).
- Event bus for async signals.
- SSTable FlowObserver + FlowMetrics hooks for read-path visibility.
- M1 control-plane surface: fixed shard map metadata + management APIs.
- M1 control-plane state persistence: shard layout/leader/drain state survive restart via `control_state.json`.
- Static three-node distributed KV foundation: etcd-raft commit log, HTTP peer transport, follower apply, CLI KV commands, Docker Compose/kind smokes, and restart/partition integration coverage.
- Shard layout guardrails: ranges are validated at startup (ordered, non-overlapping, open-ended range only as last shard).
- Backpressure returns `ErrBackpressure` instead of synchronous flush when the flush queue is full.
- Close drains flush queues best-effort within `CloseTimeout`; new writes return `ErrClosed`.

## Quick start
```bash
go test ./...
```

## Testing
- Default tests: `go test ./...`
- Integration + test-only hooks: `go test -tags test ./tests/integration`
- Docker (cached fast path, runs tests with `-tags test`): `scripts/docker-test.sh`

Design docs:
- `docs/design.md` (index)
- `docs/architecture.md`
- `docs/distributed-kv-runbook.md`
- `docs/memtable.md`
- `docs/sstable.md`
- `docs/wal.md`
- `docs/compaction.md`

## Package layout
Public surface (for users building a KV/NoSQL on top):
- `pkg/lsm`: LSM facade and options.
- `pkg/lsm/types`: entry and shared types.
- `pkg/lsm/errs`: error definitions.
- `pkg/lsm/bus`: event bus (optional).

Internal engine components (subject to change):
- `internal/lsm/memtable`: in-memory table implementations; `internal/lsm/memtable/skiplist`: ordered index.
- `internal/lsm/wal`: write-ahead log append/replay; `internal/lsm/wal/codec`: WAL framing/codec.
- `internal/lsm/sstable`: SSTable writer/reader with block index, bloom, and cache/prefetch.
- `internal/lsm/dispatch`: flush dispatcher.
- `internal/lsm/manifest`: manifest store.
- `internal/lsm/logging`: logger helpers.

## Docker
- Build test image (runs verbose tests during build): `docker build -f docker/Dockerfile.test -t lsmengine-test .`
- Run tests via image (verbose): `docker run --rm lsmengine-test`
- Run a static three-node server smoke with Docker Compose: `examples/docker-compose-cluster/smoke.sh`
- Run a static three-node rolling restart smoke with Docker Compose: `examples/docker-compose-cluster/rolling-restart.sh`
- Run a static three-node server smoke in kind: `examples/kind-cluster/smoke.sh`

## Scripts
- `scripts/docker-test.sh`: default mode runs `go test -v -tags test ./...` inside Docker with mounted module/build caches.
  - Legacy image build mode: `LSM_DOCKER_TEST_MODE=build scripts/docker-test.sh`
  - Optional package override: `LSM_DOCKER_TEST_PKGS=./tests/integration/... scripts/docker-test.sh`

## Monitoring
- Embed HTTP handlers: `pkg/lsm/server` exposes `/healthz`, `/stats`, and control endpoints.
- Control endpoints: `/cluster/status`, `/cluster/shards`, `/cluster/shards/{id}/{transfer-leader|split|rebalance}`, `/cluster/nodes/{id}/drain`.
- CLI (single-run or via server): `cmd/lsmctl` with `serve`, `get`, `range`, `put`, `delete`, `async-put`, `async-delete`, `write-status`, `stats`, `health`.
- Control writes support optional optimistic concurrency + idempotency (`expected_revision`, `operation_id`).

## Control-plane state file (M1)
- Default path: `<data_dir>/control_state.json` (override with `control_state_path` in server config).
- Persisted fields: shard ranges/order, current leaders/replicas, drain flag, cluster/node identity.
- Safety rule: if persisted `cluster_id`/`node_id` does not match runtime config, startup fails fast.
- Routing rule: shard lookup uses validated shard order; keys outside all shard ranges return `ErrShardNotFound`.

## Benchmarks
- Memtable: `go test ./internal/lsm/memtable -bench=Memtable -benchmem`

## Next steps
- Add benchmarks and micro-bench tools for writes/reads.
- Add metrics/health endpoints.
