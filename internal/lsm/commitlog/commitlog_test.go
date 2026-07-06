package commitlog

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

type recordingCommittedObserver struct {
	control []ControlCommittedEntry
	data    []DataCommittedEntry
}

func (r *recordingCommittedObserver) ObserveCommittedControl(entry ControlCommittedEntry) error {
	r.control = append(r.control, entry)
	return nil
}

func (r *recordingCommittedObserver) ObserveCommittedData(entry DataCommittedEntry) error {
	r.data = append(r.data, entry)
	return nil
}

func TestLocalConsensusReturnsOrderedCommittedControlEntries(t *testing.T) {
	consensus := newLocalConsensus()

	first, err := consensus.CommitControl(context.Background(), ControlMutation{
		Kind:    "split",
		ShardID: "users",
		Split:   []byte("m"),
	})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	second, err := consensus.CommitControl(context.Background(), ControlMutation{
		Kind:    "transfer-leader",
		ShardID: "users-a",
		Target:  "node-b",
	})
	if err != nil {
		t.Fatalf("second commit: %v", err)
	}

	if first.Commit.Index != 1 || second.Commit.Index != 2 {
		t.Fatalf("expected ordered indexes 1,2; got %d,%d", first.Commit.Index, second.Commit.Index)
	}
	if first.Commit.Term != 1 || second.Commit.Term != 1 {
		t.Fatalf("expected local term 1, got %d,%d", first.Commit.Term, second.Commit.Term)
	}
	if first.Mutation.Kind != "split" || first.Mutation.ShardID != "users" || string(first.Mutation.Split) != "m" {
		t.Fatalf("unexpected first committed mutation: %+v", first.Mutation)
	}
	if second.Mutation.Kind != "transfer-leader" || second.Mutation.ShardID != "users-a" || second.Mutation.Target != "node-b" {
		t.Fatalf("unexpected second committed mutation: %+v", second.Mutation)
	}
}

func TestLocalConsensusReturnsOrderedCommittedDataEntries(t *testing.T) {
	consensus := newLocalConsensus()

	first, err := consensus.CommitData(context.Background(), DataMutation{
		Kind:  "put",
		Key:   []byte("k"),
		Value: []byte("v1"),
	})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	second, err := consensus.CommitData(context.Background(), DataMutation{
		Kind: "delete",
		Key:  []byte("k"),
	})
	if err != nil {
		t.Fatalf("second commit: %v", err)
	}

	if first.Commit.Index != 1 || second.Commit.Index != 2 {
		t.Fatalf("expected ordered indexes 1,2; got %d,%d", first.Commit.Index, second.Commit.Index)
	}
	if first.Seq != first.Commit.Index || second.Seq != second.Commit.Index {
		t.Fatalf("expected seq to derive from committed index, got seq/index %d/%d and %d/%d",
			first.Seq, first.Commit.Index, second.Seq, second.Commit.Index)
	}
}

func TestLocalConsensusClonesMutationPayload(t *testing.T) {
	consensus := newLocalConsensus()
	split := []byte("m")
	controlEntry, err := consensus.CommitControl(context.Background(), ControlMutation{
		Kind:    "split",
		ShardID: "users",
		Split:   split,
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	split[0] = 'z'
	if string(controlEntry.Mutation.Split) != "m" {
		t.Fatalf("expected committed mutation split key to be immutable, got %q", controlEntry.Mutation.Split)
	}

	value := []byte("v1")
	dataEntry, err := consensus.CommitData(context.Background(), DataMutation{
		Kind:  "put",
		Key:   []byte("k"),
		Value: value,
	})
	if err != nil {
		t.Fatalf("data commit: %v", err)
	}
	value[1] = '2'
	if string(dataEntry.Mutation.Value) != "v1" {
		t.Fatalf("expected committed data value to be immutable, got %q", dataEntry.Mutation.Value)
	}
}

func TestEtcdRaftConsensusRecordsCommittedEntryWithoutPendingProposal(t *testing.T) {
	observer := &recordingCommittedObserver{}
	consensus := &etcdRaftConsensus{
		pending:  make(map[uint64]*pendingRaftProposal),
		observer: observer,
	}
	mutation := DataMutation{
		Kind:  "put",
		Key:   []byte("k"),
		Value: []byte("v"),
	}
	payload, err := json.Marshal(raftCommitProposal{
		ID:   99,
		Kind: "data",
		Data: &mutation,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if err := consensus.applyCommittedEntryLocked(raftpb.Entry{
		Type:  raftpb.EntryNormal,
		Index: 7,
		Term:  2,
		Data:  payload,
	}); err != nil {
		t.Fatalf("apply committed entry: %v", err)
	}

	if consensus.index != 7 || consensus.term != 2 {
		t.Fatalf("expected raft position 7/2, got %d/%d", consensus.index, consensus.term)
	}
	if len(consensus.committed) != 1 {
		t.Fatalf("expected committed entry to be recorded, got %d", len(consensus.committed))
	}
	committed := consensus.committed[0]
	if committed.ID != 99 || committed.Data == nil {
		t.Fatalf("unexpected committed proposal: %+v", committed)
	}
	if committed.Data.Commit.Index != 7 || committed.Data.Commit.Term != 2 || committed.Data.Seq != 7 {
		t.Fatalf("unexpected committed data position: %+v", committed.Data)
	}
	if string(committed.Data.Mutation.Key) != "k" || string(committed.Data.Mutation.Value) != "v" {
		t.Fatalf("unexpected committed data mutation: %+v", committed.Data.Mutation)
	}
	if len(observer.data) != 1 {
		t.Fatalf("expected observer to receive committed data entry, got %d", len(observer.data))
	}
	if observer.data[0].Commit.Index != 7 || observer.data[0].Seq != 7 {
		t.Fatalf("unexpected observed data entry: %+v", observer.data[0])
	}
}

func TestEtcdRaftConsensusDoesNotObservePendingLocalProposal(t *testing.T) {
	observer := &recordingCommittedObserver{}
	consensus := &etcdRaftConsensus{
		pending: map[uint64]*pendingRaftProposal{
			42: {},
		},
		observer: observer,
	}
	mutation := ControlMutation{
		Kind:    "transfer-leader",
		ShardID: "users",
		Target:  "node-b",
	}
	payload, err := json.Marshal(raftCommitProposal{
		ID:      42,
		Kind:    "control",
		Control: &mutation,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if err := consensus.applyCommittedEntryLocked(raftpb.Entry{
		Type:  raftpb.EntryNormal,
		Index: 3,
		Term:  1,
		Data:  payload,
	}); err != nil {
		t.Fatalf("apply committed entry: %v", err)
	}

	pending := consensus.pending[42]
	if pending != nil {
		t.Fatalf("expected pending proposal to be removed")
	}
	if len(observer.control) != 0 || len(observer.data) != 0 {
		t.Fatalf("expected pending local proposal not to notify observer")
	}
}

func TestEtcdRaftConsensusObserverErrorFailsCommittedApply(t *testing.T) {
	observerErr := fmt.Errorf("observer failed")
	consensus := &etcdRaftConsensus{
		pending: make(map[uint64]*pendingRaftProposal),
		observer: committedObserverFunc{
			data: func(DataCommittedEntry) error { return observerErr },
		},
	}
	mutation := DataMutation{Kind: "put", Key: []byte("k"), Value: []byte("v")}
	payload, err := json.Marshal(raftCommitProposal{
		ID:   100,
		Kind: "data",
		Data: &mutation,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	err = consensus.applyCommittedEntryLocked(raftpb.Entry{
		Type:  raftpb.EntryNormal,
		Index: 8,
		Term:  2,
		Data:  payload,
	})
	if err == nil {
		t.Fatalf("expected observer error")
	}
}

type committedObserverFunc struct {
	control func(ControlCommittedEntry) error
	data    func(DataCommittedEntry) error
}

func (f committedObserverFunc) ObserveCommittedControl(entry ControlCommittedEntry) error {
	if f.control == nil {
		return nil
	}
	return f.control(entry)
}

func (f committedObserverFunc) ObserveCommittedData(entry DataCommittedEntry) error {
	if f.data == nil {
		return nil
	}
	return f.data(entry)
}
