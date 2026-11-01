# Node-local online key reads

## CLI

```sh
lsmctl get --url http://127.0.0.1:8080 --key example --timeout 5s
lsmctl get --url http://127.0.0.1:8080 --key-base64 AP8=
```

The command reads exactly the specified node, with no discovery, redirects,
or automatic retry. It emits the validated JSON response, including base64
values. A missing key is a successful query with `found: false` and exit code
zero; transport, protocol and server failures produce a nonzero exit code.
Provide exactly one nonempty `--key` or `--key-base64`. The deadline covers the
request and response body. This command does not provide read-your-writes or
linearizable semantics across nodes.

## HTTP

`GET /kv/get?key_base64=<URL-escaped standard base64>` reads the contacted
node's locally applied data. It does not route to a leader or perform a quorum
read barrier. A follower can be stale; even a leader response is not a
linearizability guarantee. Unsupported consistency parameters are rejected.

Responses are non-cacheable. HTTP 200 returns `found: true`, `key_base64`,
`value_base64`, the entry's `sequence`, and `consistency: "local_applied"`.
HTTP 404 returns `found: false` with empty value and zero sequence. The
sequence describes that entry, not an applied-index watermark or read barrier.

Malformed, missing, empty, duplicate or unknown parameters return HTTP 400.
Methods other than GET return 405. Closed engines return 503; storage failures
return 500 rather than falsely reporting a missing key. Internal error details
are not included in the HTTP response. Providers without the optional
`lsm.ReadProvider` interface do not expose this endpoint.

The new `GetContext` engine path preserves storage errors and cancellation,
pins SSTables while reading, and coordinates with engine shutdown. The legacy
`Get` API remains unchanged. Tombstones are reported as absent by the engine.

This endpoint inherits the server's existing trust model: there is no new
authentication or TLS layer. Keep experimental nodes on a trusted network.
