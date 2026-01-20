package runner

import (
	"testing"

	"lsmengine/internal/lsm/compaction"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
	sstableconfig "lsmengine/internal/lsm/sstable/config"
	"lsmengine/pkg/lsm/types"
)

func TestSimpleRunnerMergesNewest(t *testing.T) {
	dir := t.TempDir()
	opts := sstableconfig.DefaultOptions(dir)
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 128
	writer, err := sstable.NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table1, err := writer.Flush([]types.Entry{
		{Key: []byte("a"), Value: []byte("old"), Seq: 1},
		{Key: []byte("b"), Value: []byte("one"), Seq: 1},
	})
	if err != nil {
		t.Fatalf("flush table1: %v", err)
	}
	t.Cleanup(func() {
		if err := table1.Close(); err != nil {
			t.Errorf("close table1: %v", err)
		}
	})
	table2, err := writer.Flush([]types.Entry{
		{Key: []byte("a"), Value: []byte("new"), Seq: 3},
		{Key: []byte("c"), Value: []byte("two"), Seq: 2},
	})
	if err != nil {
		t.Fatalf("flush table2: %v", err)
	}
	t.Cleanup(func() {
		if err := table2.Close(); err != nil {
			t.Errorf("close table2: %v", err)
		}
	})

	runner := &SimpleRunner{Flusher: writer}
	result, err := runner.Run(compaction.Plan{
		Inputs:      []metadata.TableMeta{{Path: table1.Path, SeqMax: table1.Seq}, {Path: table2.Path, SeqMax: table2.Seq}},
		OutputLevel: 1,
	}, []sstable.SSTable{table1, table2})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(result.Output) != 1 {
		t.Fatalf("expected 1 output, got %d", len(result.Output))
	}
	out := result.Output[0]
	t.Cleanup(func() {
		if err := out.Close(); err != nil {
			t.Errorf("close output: %v", err)
		}
	})

	got, ok := out.Get([]byte("a"))
	if !ok || string(got.Value) != "new" {
		t.Fatalf("get a: ok=%v val=%q", ok, got.Value)
	}
	got, ok = out.Get([]byte("b"))
	if !ok || string(got.Value) != "one" {
		t.Fatalf("get b: ok=%v val=%q", ok, got.Value)
	}
	got, ok = out.Get([]byte("c"))
	if !ok || string(got.Value) != "two" {
		t.Fatalf("get c: ok=%v val=%q", ok, got.Value)
	}
}

func TestSimpleRunnerDropsTombstones(t *testing.T) {
	dir := t.TempDir()
	opts := sstableconfig.DefaultOptions(dir)
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 128
	writer, err := sstable.NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table1, err := writer.Flush([]types.Entry{
		{Key: []byte("a"), Value: []byte("old"), Seq: 1},
	})
	if err != nil {
		t.Fatalf("flush table1: %v", err)
	}
	t.Cleanup(func() {
		if err := table1.Close(); err != nil {
			t.Errorf("close table1: %v", err)
		}
	})
	table2, err := writer.Flush([]types.Entry{
		{Key: []byte("a"), Tombstone: true, Seq: 2},
	})
	if err != nil {
		t.Fatalf("flush table2: %v", err)
	}
	t.Cleanup(func() {
		if err := table2.Close(); err != nil {
			t.Errorf("close table2: %v", err)
		}
	})

	runner := &SimpleRunner{Flusher: writer, DropTombstones: true}
	result, err := runner.Run(compaction.Plan{
		Inputs: []metadata.TableMeta{{Path: table1.Path, SeqMax: table1.Seq}, {Path: table2.Path, SeqMax: table2.Seq}},
	}, []sstable.SSTable{table1, table2})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(result.Output) != 0 {
		t.Fatalf("expected no output tables when all entries are tombstones")
	}
}
