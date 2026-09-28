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
promise that shard routing follows the newly elected Raft leader: the static shard
map and existing write-triggered campaign behavior are unchanged. Automatic write
failover, partition behavior, membership changes, snapshots, and linearizable
reads require separate validation and implementation.
