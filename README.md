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
go run ./cmd/demo
```

Design doc: `docs/design.md`.

## Project layout
- `internal/lsm`: Core types (memtable, WAL, LSM façade) and helpers.
- `cmd/demo`: Small example to exercise the API.

## Next steps
- Persist WAL records to disk and replay on startup.
- Implement SSTable flush and block format (index + data blocks).
- Add compaction pipeline and TTL tombstone expiry.
- Replace in-memory map with skiplist or B-Tree to preserve ordering.
- Add benchmarks and micro-bench tools for writes/reads.
