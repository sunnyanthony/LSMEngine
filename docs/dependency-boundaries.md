# Dependency Boundary Rules

LSMEngine can use third-party libraries for hard infrastructure problems, but
public and server-facing contracts must stay LSM-owned. A dependency should be
replaceable without changing `pkg/lsm` APIs, HTTP request/response types, stored
control metadata contracts, or operator documentation.

## Rule

Do not let third-party core library types cross into public or server APIs.
Introduce an LSM-owned adapter layer first, then convert at the edge.

This applies to libraries that own core behavior or data formats, including
consensus, storage backends, filesystems, codecs, schedulers, networking, and
future object-store or kernel-assisted IO integrations.

## Existing Examples

- IO: `internal/lsm/iofs` hides OS and future backend differences behind
  LSM-owned filesystem interfaces. WAL, SSTable, and compaction code depend on
  that interface instead of direct backend-specific APIs.
- Commit log / raft: the builtin etcd-raft integration lives behind the
  commit-log provider layer. Public and server callers use LSM-owned concepts
  such as `CommitLogOptions`, committed entries, `RaftPeerMessage`, runtime
  status, and shard metadata. etcd `raftpb` messages, raft storage details, and
  ConfChange mechanics stay inside `internal/lsm/commitlog`.

## Checklist For New Dependencies

Before adding or widening use of a third-party core library:

1. Define the smallest LSM-owned interface or data type needed by callers.
2. Keep the concrete adapter in `internal/lsm/...` unless there is a deliberate
   public extension point.
3. Convert third-party structs to LSM-owned structs at the adapter boundary.
4. Keep persisted and wire-visible formats expressed in LSM-owned terms.
5. Add tests at the adapter boundary that prove callers observe LSM semantics,
   not dependency-specific behavior.
6. Document why the dependency is behind that layer and what would be required
   to replace it.

If adding a call site would make a dependency hard to replace without public API
churn, stop and tighten the adapter first.
