package engine

import (
	"errors"
	"testing"
	"time"

	"lsmengine/pkg/lsm/errs"
)

func TestWALBackpressureFlushesPartialMemtableToRecover(t *testing.T) {
	db, err := New(Options{DataDir: t.TempDir(), MemtableLimit: 1 << 20,
		WALBackpressureMaxCheckpointLag: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rejected := false
	for i := 0; i < 20; i++ {
		err := db.Put([]byte("key"), []byte("value"))
		if errors.Is(err, errs.ErrBackpressure) {
			rejected = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !rejected {
		t.Fatal("writes did not reach WAL pressure threshold")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats := db.Stats()
		if stats.WAL.CheckpointSeq > 0 && !stats.WriteBackpressure.Active && db.Health().Ready {
			if err := db.Put([]byte("resumed"), []byte("value")); err != nil {
				t.Fatalf("write did not recover after checkpoint: %v", err)
			}
			if _, ok := db.Get([]byte("key")); !ok {
				t.Fatal("pressure flush lost earlier data")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("WAL pressure cannot recover with a partial memtable: %+v", db.Stats())
}

func TestWALReadinessFlushesWithoutAnotherWrite(t *testing.T) {
	db, err := New(Options{DataDir: t.TempDir(), MemtableLimit: 1 << 20, WALReadyMaxCheckpointLag: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 6; i++ {
		if err := db.Put([]byte("key"), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if db.Stats().WAL.CheckpointSeq > 0 && db.Health().Ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("readiness cannot recover without another write: %+v", db.Stats())
}

func TestWALPressureRecoversOnRestartWithoutTraffic(t *testing.T) {
	for _, mode := range []string{"readiness", "admission"} {
		t.Run(mode, func(t *testing.T) {
			opts := Options{DataDir: t.TempDir(), MemtableLimit: 1 << 20}
			db, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 6; i++ {
				if err := db.Put([]byte("key"), []byte("value")); err != nil {
					db.Close()
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if mode == "readiness" {
				opts.WALReadyMaxCheckpointLag = 5
			} else {
				opts.WALBackpressureMaxCheckpointLag = 5
			}
			reopened, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if reopened.Stats().WAL.CheckpointSeq > 0 && reopened.Health().Ready {
					if entry, ok := reopened.Get([]byte("key")); !ok || string(entry.Value) != "value" {
						t.Fatal("startup pressure flush lost recovered data")
					}
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Fatalf("startup WAL pressure did not recover: %+v", reopened.Stats())
		})
	}
}
