package commitlog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

type recordingRaftTransport struct {
	mu       sync.Mutex
	messages []raftpb.Message
}

func (r *recordingRaftTransport) Send(_ context.Context, messages []raftpb.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, messages...)
	return nil
}

func (r *recordingRaftTransport) messagesCopy() []raftpb.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]raftpb.Message, len(r.messages))
	copy(out, r.messages)
	return out
}

type recordingStateSnapshotter struct {
	indexes []uint64
}

func (r *recordingStateSnapshotter) CaptureStateSnapshot(index uint64) ([]byte, error) {
	r.indexes = append(r.indexes, index)
	return []byte(fmt.Sprintf("state-%d", index)), nil
}

type recordingStateSnapshotApplier struct {
	indexes []uint64
	data    [][]byte
}

func (r *recordingStateSnapshotApplier) ApplyStateSnapshot(index uint64, data []byte) error {
	r.indexes = append(r.indexes, index)
	r.data = append(r.data, append([]byte(nil), data...))
	return nil
}

func TestEtcdRaftConsensusSendsPeerMessagesViaTransport(t *testing.T) {
	transport := &recordingRaftTransport{}
	consensus, err := newEtcdRaftConsensus(Config{
		Provider:  ProviderEtcdRaft,
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		Peers:     []string{"node-a", "node-b"},
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("new etcd raft consensus: %v", err)
	}

	consensus.mu.Lock()
	defer consensus.mu.Unlock()
	if err := consensus.rawNode.Campaign(); err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if err := consensus.advanceUntilStableLocked(context.Background()); err != nil {
		t.Fatalf("advance: %v", err)
	}

	messages := transport.messagesCopy()
	if len(messages) == 0 {
		t.Fatalf("expected transport to receive raft peer messages")
	}
	for _, msg := range messages {
		if msg.To == consensus.nodeID || msg.To == 0 {
			t.Fatalf("expected only peer-targeted outbound messages, got To=%d", msg.To)
		}
	}
}

func TestEtcdRaftConsensusRequiresTransportForMultiPeer(t *testing.T) {
	_, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft,
		DataDir:  t.TempDir(),
		NodeID:   "node-a",
		Peers:    []string{"node-a", "node-b"},
	})
	if err == nil {
		t.Fatalf("expected transport requirement error")
	}
	if !strings.Contains(err.Error(), "transport") {
		t.Fatalf("expected transport error, got %v", err)
	}
}

func TestEtcdRaftConsensusKnownRemoteLeaderRejectsLocalCommit(t *testing.T) {
	transport := &recordingRaftTransport{}
	consensus, err := newEtcdRaftConsensus(Config{
		Provider:  ProviderEtcdRaft,
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		Peers:     []string{"node-a", "node-b", "node-c"},
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("new etcd raft consensus: %v", err)
	}
	remoteLeader := stableRaftNodeID("node-b")
	if err := consensus.HandlePeerMessages(context.Background(), []raftpb.Message{
		{
			Type:   raftpb.MsgHeartbeat,
			From:   remoteLeader,
			To:     consensus.nodeID,
			Term:   2,
			Commit: 3,
		},
	}); err != nil {
		t.Fatalf("handle heartbeat: %v", err)
	}

	_, err = consensus.CommitData(context.Background(), DataMutation{
		Kind:  "put",
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
	status := consensus.RuntimeStatus()
	if status.WriteAvailable {
		t.Fatalf("expected follower write_available=false, got %+v", status)
	}
	if !status.LeaderKnown {
		t.Fatalf("expected known remote leader, got %+v", status)
	}
	if status.Health != "follower" {
		t.Fatalf("expected follower health, got %+v", status)
	}
	if status.LastErrorCode != "not_leader" || status.LastError == "" || status.LastErrorAt.IsZero() {
		t.Fatalf("expected not leader diagnostic, got %+v", status)
	}
}

func TestEtcdRaftConsensusElectionTimeoutRecordsUnavailableStatus(t *testing.T) {
	transport := &recordingRaftTransport{}
	consensus, err := newEtcdRaftConsensus(Config{
		Provider:  ProviderEtcdRaft,
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		Peers:     []string{"node-a", "node-b", "node-c"},
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("new etcd raft consensus: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = consensus.CommitData(ctx, DataMutation{
		Kind:  "put",
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
	status := consensus.RuntimeStatus()
	if status.WriteAvailable {
		t.Fatalf("expected write_available=false, got %+v", status)
	}
	if status.Health != "no_leader" {
		t.Fatalf("expected no_leader health, got %+v", status)
	}
	if status.LastErrorCode != "unavailable" || status.LastError == "" || status.LastErrorAt.IsZero() {
		t.Fatalf("expected unavailable diagnostic, got %+v", status)
	}
}

func TestEtcdRaftConsensusPersistsLogAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	first, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft,
		DataDir:  dataDir,
		NodeID:   "node-a",
	})
	if err != nil {
		t.Fatalf("new first consensus: %v", err)
	}
	firstEntry, err := first.CommitData(context.Background(), DataMutation{
		Kind:  "put",
		Key:   []byte("k"),
		Value: []byte("v1"),
	})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if firstEntry.Commit.Index == 0 || firstEntry.Commit.Term == 0 {
		t.Fatalf("expected committed raft position, got %+v", firstEntry.Commit)
	}

	restarted, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft,
		DataDir:  dataDir,
		NodeID:   "node-a",
	})
	if err != nil {
		t.Fatalf("restart consensus: %v", err)
	}
	status := restarted.RuntimeStatus()
	if status.Index < firstEntry.Commit.Index {
		t.Fatalf("expected restored index >= %d, got %d", firstEntry.Commit.Index, status.Index)
	}
	secondEntry, err := restarted.CommitData(context.Background(), DataMutation{
		Kind:  "put",
		Key:   []byte("k"),
		Value: []byte("v2"),
	})
	if err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if secondEntry.Commit.Index <= firstEntry.Commit.Index {
		t.Fatalf("expected restarted commit index to advance past %d, got %d",
			firstEntry.Commit.Index, secondEntry.Commit.Index)
	}
}

func TestEtcdRaftConsensusSnapshotPolicyCompactsAppliedLog(t *testing.T) {
	dataDir := t.TempDir()
	consensus, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft,
		DataDir:  dataDir,
		NodeID:   "node-a",
		SnapshotPolicy: SnapshotPolicy{
			AppliedEntries: 2,
			RetainEntries:  1,
		},
	})
	if err != nil {
		t.Fatalf("new consensus: %v", err)
	}
	var last DataCommittedEntry
	for i := 0; i < 5; i++ {
		last, err = consensus.CommitData(context.Background(), DataMutation{
			Kind:  "put",
			Key:   []byte("k"),
			Value: []byte{byte('0' + i)},
		})
		if err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	status := consensus.RuntimeStatus()
	if status.SnapshotIndex == 0 {
		t.Fatalf("expected snapshot index after policy threshold, got %+v", status)
	}
	if status.SnapshotIndex >= last.Commit.Index {
		t.Fatalf("expected retained tail after snapshot, snapshot=%d last=%d",
			status.SnapshotIndex, last.Commit.Index)
	}
	snapshot, err := consensus.storage.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Metadata.Index != status.SnapshotIndex {
		t.Fatalf("expected storage snapshot index %d, got %d",
			status.SnapshotIndex, snapshot.Metadata.Index)
	}
	firstIndex, err := consensus.storage.FirstIndex()
	if err != nil {
		t.Fatalf("first index: %v", err)
	}
	if firstIndex != status.SnapshotIndex+1 {
		t.Fatalf("expected compacted first index %d, got %d",
			status.SnapshotIndex+1, firstIndex)
	}

	restarted, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft,
		DataDir:  dataDir,
		NodeID:   "node-a",
		SnapshotPolicy: SnapshotPolicy{
			AppliedEntries: 2,
			RetainEntries:  1,
		},
	})
	if err != nil {
		t.Fatalf("restart consensus: %v", err)
	}
	restartedStatus := restarted.RuntimeStatus()
	if restartedStatus.SnapshotIndex != status.SnapshotIndex {
		t.Fatalf("expected restored snapshot index %d, got %d",
			status.SnapshotIndex, restartedStatus.SnapshotIndex)
	}
}

func TestEtcdRaftConsensusStateSnapshotterUsesObservedAppliedIndex(t *testing.T) {
	consensus, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft,
		DataDir:  t.TempDir(),
		NodeID:   "node-a",
		SnapshotPolicy: SnapshotPolicy{
			AppliedEntries: 2,
			RetainEntries:  1,
		},
	})
	if err != nil {
		t.Fatalf("new consensus: %v", err)
	}
	snapshotter := &recordingStateSnapshotter{}
	if err := consensus.SetStateSnapshotter(snapshotter); err != nil {
		t.Fatalf("set state snapshotter: %v", err)
	}

	entry, err := consensus.CommitData(context.Background(), DataMutation{
		Kind:  "put",
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if err != nil {
		t.Fatalf("commit data: %v", err)
	}
	if status := consensus.RuntimeStatus(); status.SnapshotIndex != 0 {
		t.Fatalf("expected no snapshot before applied index observation, got %+v", status)
	}
	if len(snapshotter.indexes) != 0 {
		t.Fatalf("expected snapshotter to be idle before observation, got %v", snapshotter.indexes)
	}

	consensus.ObserveCommittedIndex(entry.Commit.Index)
	if len(snapshotter.indexes) != 1 || snapshotter.indexes[0] != entry.Commit.Index {
		t.Fatalf("expected snapshotter index %d, got %v", entry.Commit.Index, snapshotter.indexes)
	}
	status := consensus.RuntimeStatus()
	if status.SnapshotIndex != entry.Commit.Index {
		t.Fatalf("expected snapshot index %d, got %+v", entry.Commit.Index, status)
	}
	snapshot, err := consensus.storage.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if got := string(snapshot.Data); got != fmt.Sprintf("state-%d", entry.Commit.Index) {
		t.Fatalf("expected LSM snapshot payload for index %d, got %q", entry.Commit.Index, got)
	}

	consensus.ObserveCommittedIndex(entry.Commit.Index)
	if len(snapshotter.indexes) != 1 {
		t.Fatalf("expected duplicate observation to avoid recapturing snapshot, got %v", snapshotter.indexes)
	}
}

func TestEtcdRaftConsensusAppliesIncomingSnapshotData(t *testing.T) {
	consensus, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft,
		DataDir:  t.TempDir(),
		NodeID:   "node-a",
	})
	if err != nil {
		t.Fatalf("new consensus: %v", err)
	}
	applier := &recordingStateSnapshotApplier{}
	if err := consensus.SetStateSnapshotApplier(applier); err != nil {
		t.Fatalf("set snapshot applier: %v", err)
	}
	snapshot := raftpb.Snapshot{
		Data: []byte("lsm-state"),
		Metadata: raftpb.SnapshotMetadata{
			Index:     8,
			Term:      2,
			ConfState: raftpb.ConfState{Voters: []uint64{consensus.nodeID}},
		},
	}

	consensus.mu.Lock()
	err = consensus.applyIncomingSnapshotLocked(snapshot)
	consensus.mu.Unlock()
	if err != nil {
		t.Fatalf("apply incoming snapshot: %v", err)
	}
	if len(applier.indexes) != 1 || applier.indexes[0] != snapshot.Metadata.Index {
		t.Fatalf("expected applier index %d, got %v", snapshot.Metadata.Index, applier.indexes)
	}
	if len(applier.data) != 1 || string(applier.data[0]) != "lsm-state" {
		t.Fatalf("expected applier data, got %q", applier.data)
	}
	status := consensus.RuntimeStatus()
	if status.SnapshotIndex != snapshot.Metadata.Index || status.Index != snapshot.Metadata.Index {
		t.Fatalf("expected runtime snapshot/index %d, got %+v", snapshot.Metadata.Index, status)
	}
}

func TestEtcdRaftConsensusChangeMembershipAddsVoter(t *testing.T) {
	transport := &recordingRaftTransport{}
	consensus, err := newEtcdRaftConsensus(Config{
		Provider:  ProviderEtcdRaft,
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("new consensus: %v", err)
	}
	if err := consensus.ChangeMembership(context.Background(), MembershipChange{
		Type:   MembershipChangeAddNode,
		NodeID: "node-b",
	}); err != nil {
		t.Fatalf("change membership: %v", err)
	}
	status := consensus.RuntimeStatus()
	if status.Replicas != 2 {
		t.Fatalf("expected 2 raft voters after add, got %+v", status)
	}

	consensus.mu.Lock()
	conf := consensus.raftConfStateLocked()
	consensus.mu.Unlock()
	if !containsRaftID(conf.Voters, stableRaftNodeID("node-b")) {
		t.Fatalf("expected node-b in raft voters, got %+v", conf.Voters)
	}

	if err := consensus.ChangeMembership(context.Background(), MembershipChange{
		Type:   MembershipChangeAddNode,
		NodeID: "node-b",
	}); err != nil {
		t.Fatalf("duplicate add should be idempotent: %v", err)
	}
}

func TestEtcdRaftConsensusHandlePeerMessagesIgnoresOtherTargets(t *testing.T) {
	transport := &recordingRaftTransport{}
	consensus, err := newEtcdRaftConsensus(Config{
		Provider:  ProviderEtcdRaft,
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		Peers:     []string{"node-a", "node-b"},
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("new etcd raft consensus: %v", err)
	}
	other := stableRaftNodeID("node-b")
	if err := consensus.HandlePeerMessages(context.Background(), []raftpb.Message{
		{
			Type: raftpb.MsgHeartbeat,
			From: other,
			To:   other,
			Term: 1,
		},
	}); err != nil {
		t.Fatalf("handle peer messages: %v", err)
	}
}

func containsRaftID(ids []uint64, target uint64) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func TestEtcdRaftConsensusHandlePeerMessagesReturnsStepError(t *testing.T) {
	consensus, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft,
		DataDir:  t.TempDir(),
		NodeID:   "node-a",
	})
	if err != nil {
		t.Fatalf("new etcd raft consensus: %v", err)
	}
	err = consensus.HandlePeerMessages(context.Background(), []raftpb.Message{
		{
			Type: raftpb.MsgHup,
			To:   consensus.nodeID,
		},
	})
	if err == nil {
		t.Fatalf("expected step error")
	}
	if !strings.Contains(err.Error(), "step") {
		t.Fatalf("expected step error, got %v", err)
	}
}
