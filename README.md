# LSMEngine

Lightweight LSM tree skeleton in Go. This is a starter layout for a custom NoSQL store and focuses on clarity and hackability over completeness.

## What is here
- LSM facade with memtable, WAL replay, and async flush dispatcher.
- Sharded skiplist memtable (plus map) with ordered iterators.
- Snapshot range scans over memtables with merge + tombstone filtering.
- WAL append/replay with corruption repair policy hooks.
- SSTable writer placeholder and manifest store.
- Event bus for async signals.

## Quick start
```bash
go test ./...
```

Design docs:
- `docs/design.md` (index)
- `docs/architecture.md`
- `docs/memtable.md`
- `docs/wal.md`

## Package layout
Public surface (for users building a distributed KV/NoSQL on top):
- `pkg/lsm`: LSM facade and options.
- `pkg/lsm/types`: entry and shared types.
- `pkg/lsm/errs`: error definitions.
- `pkg/lsm/bus`: event bus (optional).

Internal engine components (subject to change):
- `internal/lsm/memtable`: in-memory table implementations; `internal/lsm/memtable/skiplist`: ordered index.
- `internal/lsm/wal`: write-ahead log append/replay; `internal/lsm/wal/codec`: WAL framing/codec.
- `internal/lsm/sstable`: placeholder SSTable writer/reader.
- `internal/lsm/dispatch`: flush dispatcher.
- `internal/lsm/manifest`: manifest store.
- `internal/lsm/logging`: logger helpers.

## Docker
- Build test image (runs verbose tests during build): `docker build -f docker/Dockerfile.test -t lsmengine-test .`
- Run tests via image (verbose): `docker run --rm lsmengine-test`

## Scripts
- `scripts/docker-test.sh`: builds the test image with plain progress and no cache to show full test logs.

## Benchmarks
- Memtable: `go test ./internal/lsm/memtable -bench=Memtable -benchmem`

## Next steps
- Implement SSTable block format (index + data blocks).
- Add SSTable range scan iterator for snapshot merges.
- Add benchmarks and micro-bench tools for writes/reads.
- Add metrics/health endpoints and replication transport.
