package commitlog

import (
	"context"
	"fmt"
	"sync"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type peerDeliveryResult struct {
	peerID        uint64
	term          uint64
	snapshotIndex uint64
	generation    uint64
	err           error
}

// Transport callbacks must not acquire the provider lock: a transport may
// report synchronously while Send holds it. The advance loop consumes results.
type peerDeliveryQueue struct {
	mu      sync.Mutex
	closed  bool
	results []peerDeliveryResult
}

func (q *peerDeliveryQueue) add(result peerDeliveryResult) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.results = append(q.results, result)
	}
}

func (q *peerDeliveryQueue) take() []peerDeliveryResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	results := q.results
	q.results = nil
	return results
}

func (q *peerDeliveryQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.results = nil
}

func (c *etcdRaftConsensus) sendPeerMessagesLocked(ctx context.Context, messages []raftpb.Message) error {
	encoded, err := encodeRaftPeerMessages(messages)
	if err != nil {
		return err
	}
	results := make(map[uint64]peerDeliveryResult)
	term := c.rawNode.Status().Term
	for _, message := range messages {
		result := results[message.To]
		result.peerID, result.term = message.To, term
		if message.Type == raftpb.MsgSnap {
			c.deliverySeq++
			result.snapshotIndex = message.Snapshot.Metadata.Index
			result.generation = c.deliverySeq
			if c.snapshotSends == nil {
				c.snapshotSends = make(map[uint64]uint64)
			}
			c.snapshotSends[message.To] = result.generation
		}
		results[message.To] = result
	}
	report := func(peerID uint64, err error) {
		result, ok := results[peerID]
		if !ok || (err == nil && result.snapshotIndex == 0) {
			return
		}
		result.err = err
		c.delivery.add(result)
	}
	sendCtx, cancel := withDefaultTimeout(ctx, etcdRaftSendTimeout)
	defer cancel()
	if transport, ok := c.transport.(ReportingPeerTransport); ok {
		err = transport.SendWithResult(sendCtx, encoded, report)
	} else {
		err = c.transport.Send(sendCtx, encoded)
	}
	if err != nil {
		for peerID := range results {
			report(peerID, err)
		}
	}
	// Delivery failure must not prevent applying already committed entries or
	// acknowledging Ready. Raft retries after consuming the failure report.
	return nil
}

func (c *etcdRaftConsensus) applyDeliveryResultsLocked() {
	for _, result := range c.delivery.take() {
		if c.closed || c.rawNode == nil {
			return
		}
		status := c.rawNode.Status()
		if status.Term != result.term || status.Lead != c.nodeID {
			continue
		}
		progress, ok := status.Progress[result.peerID]
		if !ok {
			continue
		}
		if result.snapshotIndex != 0 && (c.snapshotSends[result.peerID] != result.generation || progress.PendingSnapshot != result.snapshotIndex) {
			continue
		}
		if result.err != nil {
			c.recordRuntimeErrorLocked(fmt.Errorf("%w: raft peer %d delivery: %v", ErrUnavailable, result.peerID, result.err))
			c.rawNode.ReportUnreachable(result.peerID)
		}
		if result.snapshotIndex != 0 {
			outcome := raft.SnapshotFinish
			if result.err != nil {
				outcome = raft.SnapshotFailure
			}
			c.rawNode.ReportSnapshot(result.peerID, outcome)
		}
	}
}
