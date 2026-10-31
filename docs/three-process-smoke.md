# Three-process cluster smoke test

Run from the repository root on macOS or Linux with Go installed:

```sh
go test -tags test ./tests/integration/server -run '^TestThreeProcessClusterRestart$' -count=1 -v
```

The test builds the real `lsmctl` executable and generates three temporary YAML
configurations with distinct data directories and loopback ports, the same static
peer list, HTTP peer URLs, and a shared shard. It uses the built-in `etcd-raft`
provider. It does not modify an existing deployment or require Docker.

## Acceptance checks

1. All three server processes agree on an elected leader.
2. Real `cluster-put` commands return committed acknowledgments.
3. The leader is stopped with SIGINT and the remaining two nodes elect a leader.
4. CLI put and delete commands commit while the old leader is offline.
5. The old leader restarts using its original directory and catches up.
6. All three processes stop, restart from their original directories, and accept
   another committed CLI write.
7. After every process exits, each directory is reopened through LSMEngine. All
   three nodes must contain the retained, offline-period, and post-restart values,
   and the deleted key must be absent.

Sequence polling is a synchronization aid, not the data correctness assertion.
The last check reads actual stored values. The server currently has no key-read
HTTP endpoint; this test does not imply online or linearizable reads.

## Scope and limits

This is a small static-membership, single-Raft-group smoke test with graceful
shutdowns. It does not prove crash/power-loss durability, arbitrary partitions,
snapshot transfer, dynamic membership, exactly-once retries, or production
security. Writes are attempted after election convergence, not during an election.
Ports are reserved during configuration, then released for the child executable;
a competing process taking a port can cause the test to fail.

Each subprocess has a bounded shutdown with forced cleanup on timeout. Temporary
directories are removed after process cleanup; failed tests include server logs.
Windows is excluded because the lifecycle uses SIGINT. `go test -short` skips
this process test. The recursive tagged integration suite includes it normally.
