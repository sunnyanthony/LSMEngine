# LSMEngine

Lightweight LSM tree skeleton in Go. This is a starter layout for a custom NoSQL store and focuses on clarity and hackability over completeness.

## What is here
- LSM facade with memtable, WAL replay, and async flush dispatcher.
- Sharded skiplist memtable (plus map) with ordered iterators.
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
- `pkg/lsm`: LSM facade and options; wires subpackages.
- `pkg/lsm/memtable`: in-memory table implementations; `pkg/lsm/memtable/skiplist`: ordered index.
- `pkg/lsm/wal`: write-ahead log append/replay; `pkg/lsm/wal/codec`: WAL framing/codec.
- `pkg/lsm/sstable`: placeholder SSTable writer/reader.
- `pkg/lsm/dispatch`: flush dispatcher; `pkg/lsm/bus`: event bus.
- `pkg/lsm/manifest`: manifest store; `pkg/lsm/logging`: logger helpers.
- `pkg/lsm/types`: entry and shared types; `pkg/lsm/errs`: error definitions.

## Docker
- Build test image (runs verbose tests during build): `docker build -f docker/Dockerfile.test -t lsmengine-test .`
- Run tests via image (verbose): `docker run --rm lsmengine-test`

## Scripts
- `scripts/docker-test.sh`: builds the test image with plain progress and no cache to show full test logs.

## Benchmarks
- Memtable: `go test ./pkg/lsm/memtable -bench=Memtable -benchmem`

## Next steps
- Implement SSTable flush and block format (index + data blocks).
- Add compaction pipeline and TTL tombstone expiry.
- Add benchmarks and micro-bench tools for writes/reads.
- Add metrics/health endpoints and replication transport.
