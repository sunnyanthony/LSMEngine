# Quorum Delivery Failures

The built-in HTTP transport reports attempted but unconfirmed destination batches
using the LSM-owned `CommitLogPeerDeliveryError`. The engine adapter copies its
destination IDs into the internal commit-log contract. Only this direct error
type is recoverable; nested markers and ordinary custom transport errors remain
conservative. Configuration validation happens before sending.

The Raft adapter validates failed IDs against the current outbound batch, reports
them unreachable, and continues Ready processing. It does not fabricate replies
or commit acknowledgments. Scheduled ticks and later peer traffic drive retry.
`commit_log_runtime.delivery_failures` counts failed destination batches since
startup so delivery failures remain visible even when quorum commits succeed.

A caller succeeds only after its exact proposal is committed and engine apply
completes. With one stopped node in a three-node group, the healthy majority can
continue data/control writes, including after replacing a stopped leader. With
two nodes stopped, the remaining node cannot acknowledge or apply a new write.
Proposal timeout and local persistence/apply errors still fail conservatively;
timeout does not prove the mutation can never commit later.

The real HTTP regression covers stopped followers and leaders, repeated Put and
Delete/control progress, healthy-replica application, and subsequent quorum loss.
This is not yet a complete client failover guarantee: gateway discovery/retry,
request idempotency, stale/minority read policy, partitions, catch-up/snapshots,
membership changes and sustained slow-peer behavior require further work.
