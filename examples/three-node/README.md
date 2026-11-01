# Three-node local database

Run from the repository root on macOS or Linux with Go 1.23 or newer. Ports
8080-8082 must be free. This is an experimental, trusted-loopback deployment:
one Raft group, static membership, no authentication/TLS, no snapshots or log
retention. Keep real or sensitive data elsewhere.

Build once:

```sh
go build -o bin/lsmctl ./cmd/lsmctl
```

Start each node in a separate terminal, all at the repository root:

```sh
bin/lsmctl serve --config examples/three-node/node-a.yaml
bin/lsmctl serve --config examples/three-node/node-b.yaml
bin/lsmctl serve --config examples/three-node/node-c.yaml
```

In a fourth terminal, inspect `/cluster/status` on each port and wait until all
three report the same nonempty `commit_log_runtime.leader_node_id` before writes.
The static shard leader is bootstrap metadata, not an election result.

```sh
curl -fsS http://127.0.0.1:8080/cluster/status
curl -fsS http://127.0.0.1:8081/cluster/status
curl -fsS http://127.0.0.1:8082/cluster/status
NODES='{"node-a":"http://127.0.0.1:8080","node-b":"http://127.0.0.1:8081","node-c":"http://127.0.0.1:8082"}'
bin/lsmctl cluster-put --bootstrap-url http://127.0.0.1:8080 --node-endpoints "$NODES" --key example --value hello
bin/lsmctl get --url http://127.0.0.1:8080 --key example
bin/lsmctl get --url http://127.0.0.1:8081 --key example
bin/lsmctl get --url http://127.0.0.1:8082 --key example
bin/lsmctl cluster-delete --bootstrap-url http://127.0.0.1:8080 --node-endpoints "$NODES" --key example
```

Writes default to `local_committed`. Reads report `local_applied` and can lag on
any contacted node; poll until followers catch up instead of assuming an
immediate read-your-writes guarantee. Values are base64 in JSON (`hello` is
`aGVsbG8=`). After delete catches up, reads return `found: false` with exit code
zero. A write timeout has an unknown outcome: do not blindly retry.

Stop the elected leader with Ctrl-C, leaving the other two running. Wait for
them to agree on a new leader, then repeat writes. Restart the stopped node with
the same command and data directory. Stop all nodes with Ctrl-C before a full
restart; never start two processes using the same directory. Persistent state
is under `data/three-node/`, ignored by Git. These settings do not validate
power-loss or kill -9 recovery, network partitions, or mixed-version upgrades.

## Automated acceptance

```sh
go test -tags test ./tests/integration/server -run '^TestThreeProcessClusterRestart$' -count=1 -v
```

The test loads these configs, substituting temporary directories and available
ports. It verifies CLI reads on every live node after explicit catch-up, leader
replacement, and full restart, then checks each stopped replica's stored data.
