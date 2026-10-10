# Raft Write Routing

The built-in multi-peer provider uses one consensus group for all data and
control mutations. Its elected leader is the effective write destination for
every logical shard. Persisted shard `leader` metadata is not an independent
Raft group and changing it does not transfer consensus leadership.

The adapter exposes the configured node name as `leader_node_id` in commit-log
runtime status; etcd identifiers and types remain internal. Shard snapshots add
an optional `write_leader`: absent means use metadata routing (local/custom or
single-node providers), empty means no known multi-peer leader. Engine admission,
HTTP `/cluster/routes`, and retry hints use effective write leadership. Unknown
leadership never falls back to the static metadata leader.

Observed leadership is transient. It does not increment control revision, alter
persisted metadata, or guarantee leadership remains current by the time a client
sends its next request. The provider rechecks leadership before proposing, and
Raft quorum commitment remains required. Draining and shard-range validation
still apply. Route consumers must refresh on rejection even at the same revision.

This does not implement independent per-shard replication groups, data placement,
automatic consensus leadership transfer, linearizable reads, or complete client
failover. In particular, delivery failures involving a stopped peer still use
the conservative error handling of the current transport/proposal path. Those
failures and gateway bootstrap availability require separate work before claiming
a usable failure-tolerant cluster.
