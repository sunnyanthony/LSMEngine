# Design Index

Start here for the overall architecture and component relationships:
- `docs/architecture.md`

Focused specs:
- `docs/memtable.md`
- `docs/wal.md`

## Backlog (non-blocking)

WAL:
- Tail/truncate policy and payload cap refinements.
- Faster resync scanning for corrupted blocks.
- Async writer metrics/backpressure observability.
- Codec version negotiation for future formats.
- Large replay + mixed corrupt/missing segment stress tests.

Memtable:
- Streaming iterators to avoid snapshot copying.
- Shard count auto-tuning based on workload.
- Lock contention and tail-latency benchmarks.
- Tighter immutable/flush state machine if stronger consistency is needed.

Skiplist:
- Node allocation via arena to reduce GC.
- Comparator coverage tests for varied key distributions.
- Level distribution/iterator performance benchmarks.
