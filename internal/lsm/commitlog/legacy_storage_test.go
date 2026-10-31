package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestEtcdRaftReadsLegacyV3513Storage(t *testing.T) {
	data, err := os.ReadFile("testdata/raft-v3.5.13.json")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	raftDir := filepath.Join(dir, "raft")
	if err := os.MkdirAll(raftDir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(raftDir, fmt.Sprintf("commitlog-%016x.json", stableRaftNodeID("node-a")))
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := Config{DataDir: dir, NodeID: "node-a"}
	c, err := newEtcdRaftConsensus(cfg)
	if err != nil {
		t.Fatal(err)
	}
	entries := c.RecoveredEntries()
	if len(entries) != 3 {
		t.Fatalf("recovered %d entries, want 3", len(entries))
	}
	want := []DataMutation{
		{Kind: "put", Key: []byte("retained"), Value: []byte("legacy-value")},
		{Kind: "put", Key: []byte("deleted"), Value: []byte("remove-me")},
		{Kind: "delete", Key: []byte("deleted")},
	}
	for i, entry := range entries {
		got := entry.Data
		if got == nil || got.Commit.Index != uint64(i+3) || got.Commit.Term != 2 || got.Seq != uint64(i+3) {
			t.Fatalf("legacy position %d changed: %+v", i, got)
		}
		if got.Mutation.Kind != want[i].Kind || string(got.Mutation.Key) != string(want[i].Key) || string(got.Mutation.Value) != string(want[i].Value) {
			t.Fatalf("legacy mutation %d changed: %+v", i, got.Mutation)
		}
	}
	committed, err := c.CommitData(context.Background(), DataMutation{Kind: "put", Key: []byte("new"), Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	if committed.Commit.Index <= 5 || committed.Seq != committed.Commit.Index {
		t.Fatalf("new commit did not advance legacy log: %+v", committed)
	}
	reopened, err := newEtcdRaftConsensus(cfg)
	if err != nil {
		t.Fatal(err)
	}
	recovered := reopened.RecoveredEntries()
	if len(recovered) != 4 || recovered[3].Data == nil || recovered[3].Data.Commit != committed.Commit || string(recovered[3].Data.Mutation.Value) != "value" {
		t.Fatalf("upgraded log did not survive another reopen: %+v", recovered)
	}
}
