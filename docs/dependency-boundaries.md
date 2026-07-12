# Dependency Boundary Rules

LSMEngine can use third-party libraries for infrastructure-heavy pieces, but
core contracts should stay expressed in LSM-owned terms. A dependency should be
replaceable without forcing changes through public APIs, server request/response
types, persisted control metadata, or operator-facing documentation.

## Rule

Do not let third-party core library types cross into public, server, storage, or
orchestration boundaries without an explicit adapter layer.

Introduce the smallest LSM-owned interface or data type first, then convert to
the concrete dependency at the edge. This applies to libraries that own storage,
consensus, filesystems, codecs, schedulers, networking, object-store access, or
kernel-assisted IO behavior.

## Existing Examples

- IO: `internal/lsm/iofs` is the reference pattern. WAL, SSTable, manifest, and
  compaction code talk to LSM-owned filesystem interfaces while concrete
  backends can use OS files, async wrappers, or future platform-specific IO.
- Commit log / raft: the builtin etcd-raft integration lives behind the
  commit-log provider layer. Public and server callers use LSM-owned concepts
  such as `CommitLogOptions`, committed entries, `CommitLogPeerMessage`,
  runtime status, shard metadata, and state-machine snapshot contracts. etcd
  `raftpb` messages, raft storage details, and ConfChange mechanics stay inside
  `internal/lsm/commitlog`.
- Cluster endpoints: raft peer transport uses the LSM-owned
  `RaftPeerResolver`, while client/operator traffic and route-aware writes use
  `NodeEndpointResolver`. Static config, reloaded endpoint files, Kubernetes,
  DNS, or another service registry should plug in through those contracts
  instead of leaking provider SDK types into `lsmctl`, gateway routing, or HTTP
  request/response types.

## Boundary Status

- Raft peer-message transport and ingress use the LSM-owned
  `CommitLogPeerMessage` envelope. The payload remains provider-defined and
  opaque to engine/server callers; the builtin etcd-raft provider owns raftpb
  encoding and decoding internally.
- Future raft hardening should keep ConfChange, raft storage, snapshot policy,
  and catch-up details behind the same provider boundary before widening
  operator or public APIs.

## Checklist For New Dependencies

Before adding or expanding a third-party core dependency:

1. Define the LSM-owned concept callers should use.
2. Keep concrete dependency usage in an adapter package, preferably under
   `internal/lsm/...` unless it is an intentional public extension point.
3. Convert dependency structs to LSM-owned structs at the adapter boundary.
4. Keep persisted and wire-visible formats expressed in LSM-owned terms.
5. Add adapter-boundary tests that prove callers observe LSM semantics rather
   than dependency-specific behavior.
6. Document replacement requirements and any boundary exceptions.

If a call site would make the dependency hard to replace without API churn,
tighten the adapter before adding that call site.
