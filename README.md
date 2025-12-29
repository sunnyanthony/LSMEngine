# LSMEngine

Lightweight LSM tree skeleton in Go. This is a starter layout for a custom NoSQL store and focuses on clarity and hackability over completeness.

## What is here
- Minimal LSM façade with in-memory memtable and WAL hooks.
- SSTable and compaction placeholders to be filled in.
- Demo command to show basic Put/Get/Delete calls.
- Tests around the memtable to keep the core API stable.

## Quick start
```bash
go test ./...
```

Design doc: `docs/design.md`.

## Package layout
- `pkg/lsm`: façade and options; wires subpackages.
- `pkg/lsm/memtable`: in-memory table implementation.
- `pkg/lsm/wal`: write-ahead log append/replay.
- `pkg/lsm/sstable`: placeholder SSTable writer/reader.
- `pkg/lsm/dispatch`: flush dispatcher; `pkg/lsm/bus`: event bus.
- `pkg/lsm/manifest`: manifest store; `pkg/lsm/logging`: logger helpers.

## Docker
- Build test image (runs verbose tests during build): `docker build -f docker/Dockerfile.test -t lsmengine-test .`
- Run tests via image (verbose): `docker run --rm lsmengine-test`

## Scripts
- `scripts/docker-test.sh`: builds the test image with plain progress and no cache to show full test logs.

## Project layout
- `internal/lsm`: Core types (memtable, WAL, LSM façade) and helpers.
- `cmd/demo`: Small example to exercise the API.

## Next steps
- Persist WAL records to disk and replay on startup.
- Implement SSTable flush and block format (index + data blocks).
- Add compaction pipeline and TTL tombstone expiry.
- Replace in-memory map with skiplist or B-Tree to preserve ordering.
- Add benchmarks and micro-bench tools for writes/reads.
