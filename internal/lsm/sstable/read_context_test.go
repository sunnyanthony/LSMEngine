package sstable

import (
	"context"
	"errors"
	"testing"

	"lsmengine/internal/lsm/sstable/config"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

func TestGetViewContext(t *testing.T) {
	writer, err := NewSSTableWriter(config.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	table, err := writer.Flush([]types.Entry{
		{Key: []byte("a"), Value: []byte("value"), Seq: 1},
		{Key: []byte("c"), Tombstone: true, Seq: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupCloser(t, "table", table)
	for _, tc := range []struct {
		key              string
		found, tombstone bool
	}{
		{"a", true, false}, {"b", false, false}, {"c", true, true}, {"z", false, false},
	} {
		entry, found, err := table.GetViewContext(context.Background(), []byte(tc.key))
		if err != nil || found != tc.found || entry.Tombstone != tc.tombstone {
			t.Fatalf("%s: found=%v entry=%+v err=%v", tc.key, found, entry, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := table.GetViewContext(ctx, []byte("z")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
	if _, _, err := (SSTable{}).GetViewContext(context.Background(), []byte("a")); !errors.Is(err, errs.ErrClosed) {
		t.Fatalf("uninitialized: %v", err)
	}
}

func TestGetViewContextPreservesCorruption(t *testing.T) {
	for _, policy := range []config.CorruptionPolicy{config.CorruptionFailFast, config.CorruptionSkipBlock, config.CorruptionDropTable} {
		t.Run(string(policy), func(t *testing.T) {
			opts := config.DefaultOptions(t.TempDir())
			opts.CorruptionPolicy = policy
			writer, err := NewSSTableWriter(opts)
			if err != nil {
				t.Fatal(err)
			}
			table, err := writer.Flush([]types.Entry{{Key: []byte("a"), Value: []byte("value"), Seq: 1}})
			if err != nil {
				t.Fatal(err)
			}
			if err := table.Close(); err != nil {
				t.Fatal(err)
			}
			footer := readFooterForTest(t, table.Path)
			index := readIndexForTest(t, table.Path, footer)
			if len(index) != 1 {
				t.Fatalf("index length: %d", len(index))
			}
			corruptByte(t, table.Path, int64(index[0].Offset)+int64(index[0].Length)-1)
			table, err = LoadSSTable(table.Path, opts)
			if err != nil {
				t.Fatal(err)
			}
			cleanupCloser(t, "table", table)
			// Legacy reads may mark the table dropped; strict reads must still fail.
			_, _ = table.GetView([]byte("a"))
			for i := 0; i < 2; i++ {
				_, found, err := table.GetViewContext(context.Background(), []byte("a"))
				if found || !errors.Is(err, errs.ErrSSTableBadBlock) {
					t.Fatalf("found=%v err=%v", found, err)
				}
			}
		})
	}
}
