package engine

import (
	"context"
	"encoding/json"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestLocalCommitLogConsensusReturnsOrderedCommittedControlEntries(t *testing.T) {
	consensus := newLocalCommitLogConsensus()

	first, err := consensus.CommitControl(context.Background(), controlMutation{
		Kind:    "split",
		ShardID: "users",
		Split:   []byte("m"),
	})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	second, err := consensus.CommitControl(context.Background(), controlMutation{
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

func TestLocalCommitLogConsensusReturnsOrderedCommittedDataEntries(t *testing.T) {
	consensus := newLocalCommitLogConsensus()

	first, err := consensus.CommitData(context.Background(), dataMutation{
		Kind:  "put",
		Key:   []byte("k"),
		Value: []byte("v1"),
	})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	second, err := consensus.CommitData(context.Background(), dataMutation{
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

func TestLocalCommitLogConsensusClonesMutationPayload(t *testing.T) {
	consensus := newLocalCommitLogConsensus()
	split := []byte("m")
	controlEntry, err := consensus.CommitControl(context.Background(), controlMutation{
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
	dataEntry, err := consensus.CommitData(context.Background(), dataMutation{
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

func TestLocalCommitLogConsensusObservesRecoveredIndex(t *testing.T) {
	consensus := newLocalCommitLogConsensus()
	consensus.ObserveCommittedIndex(42)

	entry, err := consensus.CommitData(context.Background(), dataMutation{
		Kind:  "put",
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if entry.Commit.Index != 43 || entry.Seq != 43 {
		t.Fatalf("expected commit and seq after recovered index, got index=%d seq=%d", entry.Commit.Index, entry.Seq)
	}
}
func TestEtcdRaftCommitLogRecordsCommittedEntryWithoutPendingProposal(t *testing.T) {
	consensus := &etcdRaftCommitLogConsensus{
		pending: make(map[uint64]*pendingRaftProposal),
	}
	mutation := dataMutation{
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
}
