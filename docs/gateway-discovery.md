# Gateway Discovery and Safe Retry

`server.NewGateway` uses the bootstrap URL as its preferred route source and all
configured node endpoints as deterministic, deduplicated read-only fallbacks.
Each route probe is bounded by `GatewayOptions.RouteProbeTimeout` (default 500 ms)
and the caller context. Failed probes, unknown leaders, and a rejected old leader
do not prevent trying another source. Leadership changes need not advance control
metadata revision; the gateway refreshes after a certified rejection regardless.

Put/Delete make at most one explicit retry. It is allowed only after:

- The gateway-owned HTTP transport reports a dial failure before the request
  reaches a server. Caller-supplied clients, including clients with nil transport
  or registered alternate protocols, cannot certify this proof through errors.
- A server explicitly returns both `retryable` and `safe_to_retry`. The latter is
  set only for a direct engine-certified rejection before proposal. Generic
  error sentinels and nested causes do not certify the current operation.

The gateway never follows redirects, and clones rather than mutates a supplied
HTTP client. Lost responses, read/write timeouts, EOF after sending, generic 5xx,
and backpressure without a safety certificate are not automatically retried.
They may represent an already committed mutation. This deliberately tightens
behavior with older servers that only expose `retryable`.

Use `WriteConsistencyLocalCommitted` when the caller needs confirmation that the
write committed and applied. `accepted` remains asynchronous and is not a commit
acknowledgment; a 202 response is not retried. This change does not provide global
request deduplication or exactly-once writes.

The real three-node HTTP regression seeds a gateway route cache, stops its
bootstrap/leader, waits for an independent election, then performs Put/Delete
through the same gateway without changing its configuration. Unit regressions
cover slow/unknown bootstrap fallback, unchanged-revision refresh, lost responses,
uncertified rejection and forbidden redirects. A write attempted before election
converges can still fail safely; retry loops across election windows, partition
read policy, and durable idempotency remain later work.
