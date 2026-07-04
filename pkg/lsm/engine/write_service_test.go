package engine

import "testing"

func TestDataWritesUseCommittedSequenceAndSeedAfterRestart(t *testing.T) {
	dataDir := t.TempDir()
	store, err := New(Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if err := store.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got := store.Stats().Seq; got != 1 {
		t.Fatalf("expected first committed data seq 1, got %d", got)
	}
	if err := store.Delete([]byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got := store.Stats().Seq; got != 2 {
		t.Fatalf("expected second committed data seq 2, got %d", got)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	restarted, err := New(Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer restarted.Close()

	if got := restarted.Stats().Seq; got != 2 {
		t.Fatalf("expected replayed seq 2 after restart, got %d", got)
	}
	if err := restarted.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("put after restart: %v", err)
	}
	if got := restarted.Stats().Seq; got != 3 {
		t.Fatalf("expected committed data seq to continue at 3 after restart, got %d", got)
	}
}
