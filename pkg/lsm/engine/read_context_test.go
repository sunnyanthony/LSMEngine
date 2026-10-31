package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"lsmengine/internal/lsm/memtable"
	"lsmengine/pkg/lsm/types"

	"lsmengine/pkg/lsm/errs"
)

type gatedReadTable struct {
	memtable.Table
	entered chan struct{}
	release chan struct{}
}

func (g *gatedReadTable) Get(key []byte) (types.Entry, bool) {
	close(g.entered)
	<-g.release
	return g.Table.Get(key)
}

func TestCloseWaitsForGetContext(t *testing.T) {
	l, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	gate := &gatedReadTable{Table: l.mem, entered: make(chan struct{}), release: make(chan struct{})}
	l.mem = gate
	var once sync.Once
	unblock := func() { once.Do(func() { close(gate.release) }) }
	t.Cleanup(func() { unblock(); _ = l.Close() })
	readDone := make(chan error, 1)
	go func() {
		entry, found, err := l.GetContext(context.Background(), []byte("k"))
		if err == nil && (!found || string(entry.Value) != "v") {
			err = errors.New("wrong admitted read result")
		}
		readDone <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("read not admitted")
	}
	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close passed admitted read: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read stuck")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close stuck")
	}
}

func TestGetContextLocalState(t *testing.T) {
	l, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if err := l.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	entry, found, err := l.GetContext(context.Background(), []byte("key"))
	if err != nil || !found || string(entry.Value) != "value" {
		t.Fatalf("get: %+v %v %v", entry, found, err)
	}
	entry.Value[0] = 'X'
	entry, _, _ = l.GetContext(context.Background(), []byte("key"))
	if string(entry.Value) != "value" {
		t.Fatal("result aliases stored value")
	}
	if err := l.Delete([]byte("key")); err != nil {
		t.Fatal(err)
	}
	if _, found, err := l.GetContext(context.Background(), []byte("key")); err != nil || found {
		t.Fatalf("delete: %v %v", found, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := l.GetContext(ctx, []byte("key")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.GetContext(context.Background(), []byte("key")); !errors.Is(err, errs.ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}
