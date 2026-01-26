package bootstrap

import (
	"testing"

	"lsmengine/internal/lsm/memory"
	"lsmengine/pkg/lsm/types"
)

type fakeWAL struct {
	views []memory.EntryView
}

func (f *fakeWAL) ReplayViews(fn func(memory.EntryView) error) error {
	for _, view := range f.views {
		if err := fn(view); err != nil {
			return err
		}
	}
	return nil
}

func TestReplayWALBatchesAndBumpsSeq(t *testing.T) {
	wal := &fakeWAL{
		views: []memory.EntryView{
			{Key: []byte("a"), Value: []byte("1"), Seq: 1},
			{Key: []byte("b"), Value: []byte("2"), Seq: 2},
			{Key: []byte("c"), Value: []byte("3"), Seq: 3},
		},
	}
	var batches [][]types.Entry
	var bumped []uint64
	cfg := ReplayConfig{
		WAL:        wal,
		Checkpoint: 1,
		BatchSize:  2,
		Build: func(view memory.EntryView) types.Entry {
			return view.Entry()
		},
		Apply: func(entries []types.Entry) error {
			batch := append([]types.Entry(nil), entries...)
			batches = append(batches, batch)
			return nil
		},
		BumpSeq: func(seq uint64) {
			bumped = append(bumped, seq)
		},
	}

	if err := ReplayWAL(cfg); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if len(batches[0]) != 2 || string(batches[0][0].Key) != "b" || string(batches[0][1].Key) != "c" {
		t.Fatalf("unexpected batch: %+v", batches[0])
	}
	if len(bumped) != 2 || bumped[0] != 2 || bumped[1] != 3 {
		t.Fatalf("unexpected bumped seqs: %v", bumped)
	}
}
