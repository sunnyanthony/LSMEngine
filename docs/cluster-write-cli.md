# Cluster Write CLI

`lsmctl cluster-put` and `lsmctl cluster-delete` use the gateway to discover the
effective write leader. They do not open or modify local data directories.
Run existing node servers before issuing these commands.

```sh
go build -o /tmp/lsmctl ./cmd/lsmctl
NODES='{"node-a":"http://127.0.0.1:8081","node-b":"http://127.0.0.1:8082","node-c":"http://127.0.0.1:8083"}'
/tmp/lsmctl cluster-put --bootstrap-url http://127.0.0.1:8081 \
  --node-endpoints "$NODES" --key greeting --value hello
/tmp/lsmctl cluster-delete --bootstrap-url http://127.0.0.1:8081 \
  --node-endpoints "$NODES" --key greeting
```

The default `--consistency local_committed` requires a committed acknowledgment.
Successful commands print the server's JSON write status to stdout; failures exit
nonzero. `--consistency accepted` explicitly requests asynchronous submission,
whose pending response is not a commit acknowledgment. `--timeout` defaults to
10 seconds and bounds discovery and the request.

Use `--key-base64` and `--value-base64` for binary bytes, instead of the respective
text flags. Empty keys/values, conflicting representations, invalid endpoints,
and unsupported consistency modes are rejected before network access.

The gateway may use configured node endpoints when bootstrap is unavailable.
It retries only certified unsubmitted writes, as described in
`docs/gateway-discovery.md`. A timeout or missing response can still mean the
mutation committed; do not blindly rerun failed commands. The CLI does not add
idempotency or a new read-consistency guarantee.
