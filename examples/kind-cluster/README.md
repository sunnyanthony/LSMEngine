# kind Cluster Smoke

This example runs a static three-node LSMEngine cluster in kind. It mirrors the
Docker Compose smoke but exercises Kubernetes pod DNS, StatefulSet identity, and
headless-service peer routing.

It is still a static raft foundation:

- node ids are the StatefulSet pod names: `lsm-cluster-0`, `lsm-cluster-1`,
  and `lsm-cluster-2`;
- peer URLs use the headless service DNS names;
- all three pods are configured as shard replicas with `lsm-cluster-0` as the
  shard leader;
- each pod mounts a `ReadWriteOnce` PVC at `/data`, so raft state, WAL, SSTables,
  and control state survive pod replacement;
- dynamic raft membership, node bootstrap/join, and full state-machine snapshot
  catch-up remain future work.

## Run

```bash
examples/kind-cluster/smoke.sh
```

The script creates or reuses a kind cluster, builds and loads the server image,
waits for the StatefulSet, and verifies writes/read/range/delete through
`kubectl exec` using the `lsmctl` binary inside the image.

## Persistent restart smoke

```bash
examples/kind-cluster/restart-smoke.sh
```

This uses the same StatefulSet, writes a committed value, deletes each pod one
at a time, waits for Kubernetes to recreate it with the same PVC, and verifies
the restarted pod can still read the committed value.

Useful environment overrides:

- `LSM_KIND_CLUSTER`: kind cluster name, default `lsm-cluster`.
- `LSM_KIND_IMAGE`: image tag loaded into kind, default `lsmengine-server:kind`.
- `LSM_KIND_KEEP=1`: keep the namespace and pods after the smoke.
