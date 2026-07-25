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
	status := store.ClusterStatus()
	if status.CommitLogRuntime.AppliedIndex != 5 || status.CommitLogRuntime.ApplyLag != 0 {
		t.Fatalf("unexpected commit-log runtime progress: %+v", status.CommitLogRuntime)
	}
}

func TestClusterStatusReportsCommitLogApplyLag(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	store.commitApplyMu.Lock()
	store.commitLogAppliedIndex = 3
	store.commitApplyMu.Unlock()

	consensus := &testCommitLogConsensus{
		runtimeStatus: CommitLogRuntimeStatus{
			Mode:     "custom",
			Index:    8,
			Term:     1,
			Leader:   true,
			Replicas: 1,
		},
	}
	store.control.mu.Lock()
	store.control.consensus = consensus
	store.control.mu.Unlock()

	status := store.ClusterStatus()
	if status.CommitLogRuntime.AppliedIndex != 3 {
		t.Fatalf("expected applied index 3, got %+v", status.CommitLogRuntime)
	}
	if status.CommitLogRuntime.ApplyLag != 5 {
		t.Fatalf("expected apply lag 5, got %+v", status.CommitLogRuntime)
	}
}

func TestApplyCommittedDataFromLogSkipsAlreadyAppliedIndex(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()
	store.seq = 7

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

func TestInitialCommitLogAppliedIndexUsesDurablePrefix(t *testing.T) {
	store, err := New(Options{
		DataDir: t.TempDir(),
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
	defer store.Close()
	store.seq = 10
	store.control.commitLogAppliedIndex = 5

	if got := store.initialCommitLogAppliedIndex(); got != 5 {
		t.Fatalf("expected durable applied prefix 5, got %d", got)
	}
}

func TestApplyCommittedControlFromLogDoesNotUseDataSeqForDedupe(t *testing.T) {
	store, err := New(Options{
		DataDir: t.TempDir(),
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
	defer store.Close()
	store.seq = 10
	store.control.commitLogAppliedIndex = 5
	store.commitLogAppliedIndex = store.initialCommitLogAppliedIndex()

	err = store.applyCommittedControlFromLog(controlCommittedEntry{
		Commit: CommitLogCommit{Index: 6, Term: 1},
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
	if shards[0].Leader != "node-b" {
		t.Fatalf("expected control entry to apply despite higher data seq, got leader %q", shards[0].Leader)
	}
	if got := store.control.commitLogApplied(); got != 6 {
		t.Fatalf("expected control applied index 6, got %d", got)
	}
	if got := store.commitLogAppliedIndex; got != 6 {
		t.Fatalf("expected global applied index 6, got %d", got)
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
	if got := restarted.control.commitLogApplied(); got != 9 {
		t.Fatalf("expected restored control applied index 9, got %d", got)
	}
	if got := restarted.commitLogAppliedIndex; got != 0 {
		t.Fatalf("expected conservative global applied prefix 0 without data proof, got %d", got)
	}
}
