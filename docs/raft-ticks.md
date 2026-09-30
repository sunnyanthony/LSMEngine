# Raft Clock Foundation

The engine schedules one logical consensus tick every 100 ms for the built-in
multi-peer Raft provider. The internal `Ticker` interface carries only a context;
etcd `RawNode` remains behind the commit-log adapter. Single-node and custom/local
providers retain their existing behavior.

Ticks persist and advance Ready state through the same provider path as inbound
messages. Network sends release the provider lock. Engine committed-entry draining
runs after tick processing returns, including when peer delivery returns an error.
Tick errors are logged and the scheduler continues so transient delivery failures
do not permanently stop the clock.

Tick operations participate in the proposal/ingress admission gate. Close cancels
the tick context, closes admission, waits for admitted operations, quiesces apply,
and joins the clock goroutine before returning. The clock starts only after
startup recovery and engine services are initialized.

The real three-peer HTTP regression requires election without client writes and
reelection among the remaining peers after the elected node stops. This does not
promise complete client failover. Effective write routing now follows observed
Raft leadership while static shard metadata remains unchanged; see
`docs/raft-write-routing.md`. Automatic write
failover, partition behavior, membership changes, snapshots, and linearizable
reads require separate validation and implementation.

## Proposal Admission

Data and control proposals do not campaign or advance the logical clock. A node
that is not the current Raft leader rejects the mutation before assigning a
proposal identity or appending it. The built-in engine adapter reports this as
`errs.ErrNotLeader`; this definite pre-proposal rejection does not latch the data
or control write path. A caller may retry after leadership is established without
reopening the engine. Shard-range and draining checks still apply. Multi-peer
built-in Raft writes use the effective consensus leader, not metadata leadership.

An admitted proposal waits without holding the provider mutex. Peer ingress and
scheduled ticks can complete it asynchronously. Cancellation removes the local
waiter, not the proposed Raft entry: the mutation may still commit later and must
still be retained and applied. Post-submission timeouts, delivery/storage errors,
and apply failures remain conservatively latched by the engine; they are not
converted into retry-safe leadership rejections. Single-node startup retains its
immediate election, but proposals themselves no longer drive elections.

Regression coverage includes both mutation kinds with queued asynchronous peer
delivery, independent ticks during pending proposals, cancellation followed by
late commitment, and engine-level rejection followed by successful retry after
independent election. This is proposal admission hardening, not client failover.
