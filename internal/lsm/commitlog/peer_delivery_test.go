package commitlog

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

type snapshotDelivery struct {
	message raftpb.Message
	report  func(uint64, error)
}

type inlineFailureTransport struct {
	reject bool
	sends  int
}

func (t *inlineFailureTransport) Send(context.Context, []PeerMessage) error {
	return errors.New("injected legacy transport rejection")
}

func (t *inlineFailureTransport) SendWithResult(_ context.Context, messages []PeerMessage, report func(uint64, error)) error {
	t.sends++
	if t.reject {
		return errors.New("injected batch rejection")
	}
	seen := make(map[uint64]bool)
	for _, message := range messages {
		if !seen[message.To] {
			seen[message.To] = true
			report(message.To, errors.New("injected inline failure"))
		}
	}
	return nil
}

func TestDeliveryFailureDoesNotBlockCommittedReady(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "inline-result"
		if reject {
			name = "rejected-batch"
		}
		t.Run(name, func(t *testing.T) {
			transport := &inlineFailureTransport{reject: reject}
			consensus, err := newEtcdRaftConsensus(Config{Provider: ProviderEtcdRaft, DataDir: t.TempDir(), NodeID: "node-a", Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			cleanupEtcdRaftConsensus(t, consensus)
			if err := consensus.ChangeMembership(context.Background(), MembershipChange{Type: MembershipChangeAddNode, NodeID: "node-b"}); err != nil {
				t.Fatal(err)
			}
			consensus.mu.Lock()
			defer consensus.mu.Unlock()
			if transport.sends == 0 || consensus.rawNode.Status().Applied != consensus.index {
				t.Fatalf("delivery failure prevented committed Ready advancement: sends=%d applied=%d committed=%d", transport.sends, consensus.rawNode.Status().Applied, consensus.index)
			}
		})
	}
}

type controlledDeliveryTransport struct {
	mu        sync.Mutex
	snapshots []snapshotDelivery
}

func (t *controlledDeliveryTransport) Send(ctx context.Context, messages []PeerMessage) error {
	return t.SendWithResult(ctx, messages, func(uint64, error) {})
}

func (t *controlledDeliveryTransport) SendWithResult(_ context.Context, messages []PeerMessage, report func(uint64, error)) error {
	decoded, err := decodeRaftPeerMessages(messages)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, message := range decoded {
		if message.Type == raftpb.MsgSnap {
			t.snapshots = append(t.snapshots, snapshotDelivery{message: message, report: report})
		}
	}
	return nil
}

func (t *controlledDeliveryTransport) snapshot(n int) (snapshotDelivery, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if n >= len(t.snapshots) {
		return snapshotDelivery{}, false
	}
	return t.snapshots[n], true
}

func TestSnapshotDeliveryFailureRetriesAndIgnoresOldResult(t *testing.T) {
	transport := &controlledDeliveryTransport{}
	leader, err := newEtcdRaftConsensus(Config{Provider: ProviderEtcdRaft, DataDir: t.TempDir(), NodeID: "node-a",
		Transport: transport, SnapshotPolicy: SnapshotPolicy{AppliedEntries: 1}})
	if err != nil {
		t.Fatal(err)
	}
	cleanupEtcdRaftConsensus(t, leader)
	if err := leader.SetStateSnapshotter(&recordingBoundarySnapshotter{}); err != nil {
		t.Fatal(err)
	}
	entry, err := leader.CommitData(context.Background(), DataMutation{Kind: "put", Key: []byte("key"), Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	leader.ObserveCommittedIndex(entry.Commit.Index)
	if err := leader.ChangeMembership(context.Background(), MembershipChange{Type: MembershipChangeAddNode, NodeID: "node-d"}); err != nil {
		t.Fatal(err)
	}
	peerID := stableRaftNodeID("node-d")
	leader.mu.Lock()
	status := leader.rawNode.Status()
	leader.mu.Unlock()
	step := func(message raftpb.Message) {
		t.Helper()
		message.From, message.To, message.Term = peerID, leader.nodeID, status.Term
		messages, err := encodeRaftPeerMessages([]raftpb.Message{message})
		if err != nil {
			t.Fatal(err)
		}
		if err := leader.HandlePeerMessages(context.Background(), messages); err != nil {
			t.Fatal(err)
		}
	}
	step(raftpb.Message{Type: raftpb.MsgAppResp, Reject: true, Index: status.Progress[peerID].Next - 1})
	first, ok := transport.snapshot(0)
	if !ok {
		t.Fatal("compacted log did not send a snapshot")
	}
	first.report(peerID, errors.New("injected snapshot connection failure"))
	leader.mu.Lock()
	leader.applyDeliveryResultsLocked()
	pending := leader.rawNode.Status().Progress[peerID].PendingSnapshot
	leader.mu.Unlock()
	if pending != 0 {
		t.Fatalf("failed delivery left snapshot paused at %d", pending)
	}
	step(raftpb.Message{Type: raftpb.MsgHeartbeatResp})
	second, ok := transport.snapshot(1)
	if !ok {
		t.Fatal("heartbeat after delivery failure did not retry snapshot")
	}
	first.report(peerID, nil)
	leader.mu.Lock()
	leader.applyDeliveryResultsLocked()
	pending = leader.rawNode.Status().Progress[peerID].PendingSnapshot
	leader.mu.Unlock()
	if pending != second.message.Snapshot.Metadata.Index {
		t.Fatal("stale result canceled the newer snapshot attempt")
	}
	joinTransport := &recordingRaftTransport{}
	joiner, err := newEtcdRaftConsensus(Config{Provider: ProviderEtcdRaft, DataDir: t.TempDir(), NodeID: "node-d",
		Peers: []string{"node-a", "node-d"}, Join: true, Transport: joinTransport})
	if err != nil {
		t.Fatal(err)
	}
	cleanupEtcdRaftConsensus(t, joiner)
	messages, err := encodeRaftPeerMessages([]raftpb.Message{second.message})
	if err != nil {
		t.Fatal(err)
	}
	if err := joiner.HandlePeerMessages(context.Background(), messages); err != nil {
		t.Fatal(err)
	}
	second.report(peerID, nil)
	if err := leader.HandlePeerMessages(context.Background(), joinTransport.messagesCopy()); err != nil {
		t.Fatal(err)
	}
	leader.mu.Lock()
	progress := leader.rawNode.Status().Progress[peerID]
	leader.mu.Unlock()
	if progress.PendingSnapshot != 0 || progress.Match < second.message.Snapshot.Metadata.Index {
		t.Fatalf("successful retry did not resume replication: %+v", progress)
	}
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	second.report(peerID, errors.New("late callback after close"))
	if len(leader.delivery.take()) != 0 {
		t.Fatal("closed provider retained late delivery result")
	}
}
