package engine

import (
	"context"
	"testing"
)

func TestLocalControlConsensusReturnsOrderedCommittedEntries(t *testing.T) {
	consensus := newLocalControlConsensus()

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

func TestLocalControlConsensusClonesMutationPayload(t *testing.T) {
	consensus := newLocalControlConsensus()
	split := []byte("m")
	entry, err := consensus.CommitControl(context.Background(), controlMutation{
		Kind:    "split",
		ShardID: "users",
		Split:   split,
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	split[0] = 'z'
	if string(entry.Mutation.Split) != "m" {
		t.Fatalf("expected committed mutation split key to be immutable, got %q", entry.Mutation.Split)
	}
}
