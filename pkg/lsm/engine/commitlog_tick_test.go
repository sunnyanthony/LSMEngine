package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

type tickFunc func(context.Context) error

func (f tickFunc) Tick(ctx context.Context) error { return f(ctx) }

func TestCloseCancelsAndJoinsAdmittedTick(t *testing.T) {
	db, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	db.tickCancel = cancel
	defer cancel()
	entered := make(chan struct{})
	done := make(chan error, 1)
	db.bg.Add(1)
	go func() {
		defer db.bg.Done()
		done <- db.tickCommitLog(ctx, tickFunc(func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("tick not admitted")
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close failed to cancel/join tick")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("tick error = %v", err)
	}
}

func TestTickDrainsCommittedEntriesAfterDeliveryError(t *testing.T) {
	db, err := New(Options{DataDir: t.TempDir(), CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entry, err := db.commitLog.CommitData(context.Background(), dataMutation{Kind: "put", Key: []byte("key"), Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	deliveryErr := errors.New("injected delivery failure")
	if err := db.tickCommitLog(context.Background(), tickFunc(func(context.Context) error { return deliveryErr })); !errors.Is(err, deliveryErr) {
		t.Fatalf("tick = %v", err)
	}
	if got, ok := db.Get([]byte("key")); !ok || got.Seq != entry.Seq {
		t.Fatal("delivery failure hid committed entry")
	}
}
