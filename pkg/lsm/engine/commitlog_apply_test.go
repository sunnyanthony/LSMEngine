package engine

import "testing"

func TestApplyCommittedDataFromLogMaterializesLocalState(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	err = store.applyCommittedDataFromLog(dataCommittedEntry{
		Commit: CommitLogCommit{Index: 5, Term: 1},
		Mutation: dataMutation{
			Kind:  "put",
			Key:   []byte("k"),
			Value: []byte("v"),
		},
		Seq: 5,
	})
	if err != nil {
		t.Fatalf("apply committed data: %v", err)
	}
	entry, ok := store.Get([]byte("k"))
	if !ok {
		t.Fatalf("expected committed follower value")
	}
	if string(entry.Value) != "v" || entry.Seq != 5 {
		t.Fatalf("unexpected entry: %+v", entry)
	}
	if got := store.Stats().Seq; got != 5 {
		t.Fatalf("expected seq 5, got %d", got)
	}
}

func TestApplyCommittedDataFromLogSkipsAlreadyAppliedIndex(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()
	store.commitLogAppliedIndex = 7

	err = store.applyCommittedDataFromLog(dataCommittedEntry{
		Commit: CommitLogCommit{Index: 6, Term: 1},
		Mutation: dataMutation{
			Kind:  "put",
			Key:   []byte("k"),
			Value: []byte("stale"),
		},
		Seq: 6,
	})
	if err != nil {
		t.Fatalf("apply stale committed data: %v", err)
	}
	if _, ok := store.Get([]byte("k")); ok {
		t.Fatalf("expected stale committed entry to be skipped")
	}
}

func TestApplyCommittedControlFromLogMaterializesControlState(t *testing.T) {
	store, err := New(Options{
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		ClusterID: "cluster-a",
		ShardMap: []ShardConfig{
			{
				ID:       "users",
				StartKey: []byte("a"),
				EndKey:   []byte("z"),
				Replicas: []string{"node-a", "node-b"},
				Leader:   "node-a",
			},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	err = store.applyCommittedControlFromLog(controlCommittedEntry{
		Commit: CommitLogCommit{Index: 4, Term: 1},
		Mutation: controlMutation{
			Kind:    "transfer-leader",
			ShardID: "users",
			Target:  "node-b",
		},
	})
	if err != nil {
		t.Fatalf("apply committed control: %v", err)
	}
	shards := store.Shards()
	if len(shards) != 1 {
		t.Fatalf("expected one shard, got %d", len(shards))
	}
	if shards[0].Leader != "node-b" {
		t.Fatalf("expected leader node-b, got %q", shards[0].Leader)
	}
	if got := store.control.commitLogApplied(); got != 4 {
		t.Fatalf("expected control commit applied index 4, got %d", got)
	}
	if got := store.commitLogAppliedIndex; got != 4 {
		t.Fatalf("expected lsm commit applied index 4, got %d", got)
	}
}

func TestControlCommitAppliedIndexRestoresAfterRestart(t *testing.T) {
	dataDir := t.TempDir()
	store, err := New(Options{
		DataDir: dataDir,
		ShardMap: []ShardConfig{
			{
				ID:       "users",
				Replicas: []string{"node-a", "node-b"},
				Leader:   "node-a",
			},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := store.applyCommittedControlFromLog(controlCommittedEntry{
		Commit: CommitLogCommit{Index: 9, Term: 1},
		Mutation: controlMutation{
			Kind:    "transfer-leader",
			ShardID: "users",
			Target:  "node-b",
		},
	}); err != nil {
		t.Fatalf("apply committed control: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	restarted, err := New(Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer restarted.Close()
	if got := restarted.commitLogAppliedIndex; got != 9 {
		t.Fatalf("expected restored commit applied index 9, got %d", got)
	}
}
