package lsm_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/transport"
	"lsmengine/pkg/lsm/types"
)

func TestReplicationLoopback(t *testing.T) {
	tr := transport.NewLoopback(transport.LoopbackOptions{Buffer: 32})

	dirA := t.TempDir()
	dirB := t.TempDir()

	storeA, err := lsm.New(lsm.Options{
		DataDir:   dirA,
		NodeID:    "node-a",
		Transport: tr,
	})
	if err != nil {
		t.Fatalf("new lsm A: %v", err)
	}
	defer storeA.Close()

	storeB, err := lsm.New(lsm.Options{
		DataDir:   dirB,
		NodeID:    "node-b",
		Transport: tr,
	})
	if err != nil {
		t.Fatalf("new lsm B: %v", err)
	}
	defer storeB.Close()

	if err := storeA.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	waitForValue(t, storeB, []byte("alpha"), []byte("one"))

	if err := storeA.Delete([]byte("alpha")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitForNotFound(t, storeB, []byte("alpha"))
}

func TestReplicationTermGating(t *testing.T) {
	tr := transport.NewLoopback(transport.LoopbackOptions{Buffer: 32})
	ctx := context.Background()

	store, err := lsm.New(lsm.Options{
		DataDir:   t.TempDir(),
		NodeID:    "node-b",
		Transport: tr,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	if err := tr.Publish(ctx, transport.Message{
		Source: "node-a",
		Term:   2,
		Entries: []types.Entry{
			{Key: []byte("k"), Value: []byte("v2"), Seq: 100},
		},
	}); err != nil {
		t.Fatalf("publish term 2: %v", err)
	}
	waitForValue(t, store, []byte("k"), []byte("v2"))

	if err := tr.Publish(ctx, transport.Message{
		Source: "node-a",
		Term:   1,
		Entries: []types.Entry{
			{Key: []byte("k"), Value: []byte("old"), Seq: 200},
		},
	}); err != nil {
		t.Fatalf("publish stale term: %v", err)
	}
	waitForValue(t, store, []byte("k"), []byte("v2"))

	if err := tr.Publish(ctx, transport.Message{
		Source: "node-a",
		Term:   3,
		Entries: []types.Entry{
			{Key: []byte("k"), Value: []byte("v3"), Seq: 50},
		},
	}); err != nil {
		t.Fatalf("publish new term: %v", err)
	}
	waitForValue(t, store, []byte("k"), []byte("v3"))

	if err := tr.Publish(ctx, transport.Message{
		Source: "node-a",
		Term:   3,
		Entries: []types.Entry{
			{Key: []byte("k"), Value: []byte("dup"), Seq: 50},
		},
	}); err != nil {
		t.Fatalf("publish duplicate: %v", err)
	}
	waitForValue(t, store, []byte("k"), []byte("v3"))
}

func TestTermProviderRejectsWrites(t *testing.T) {
	provider := &staticTermProvider{term: 7, leader: false}
	store, err := lsm.New(lsm.Options{
		DataDir:               t.TempDir(),
		TermProvider:          provider,
		ReplicationQueueDepth: 4,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("k"), []byte("v")); !errors.Is(err, errs.ErrNotLeader) {
		t.Fatalf("expected not leader error, got %v", err)
	}

	provider.leader = true
	if err := store.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put after leader: %v", err)
	}
}

func waitForValue(t *testing.T, store *lsm.LSM, key []byte, value []byte) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, ok := store.Get(key)
		if ok && bytes.Equal(got.Value, value) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected key %q to replicate", key)
}

func waitForNotFound(t *testing.T, store *lsm.LSM, key []byte) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := store.Get(key); !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected key %q to be deleted", key)
}

type staticTermProvider struct {
	term   uint64
	leader bool
}

func (p *staticTermProvider) Term() uint64 {
	return p.term
}

func (p *staticTermProvider) IsLeader() bool {
	return p.leader
}
