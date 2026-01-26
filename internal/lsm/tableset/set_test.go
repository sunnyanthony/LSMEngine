package tableset

import (
	"testing"

	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
)

func TestSetApplyAndPending(t *testing.T) {
	tableA := Table{
		Meta: metadata.TableMeta{Path: "a.sst", SeqMax: 1},
		Handle: sstable.SSTable{
			Path: "a.sst",
			Seq:  1,
		},
	}
	tableB := Table{
		Meta: metadata.TableMeta{Path: "b.sst", SeqMax: 2},
		Handle: sstable.SSTable{
			Path: "b.sst",
			Seq:  2,
		},
	}
	set := NewSet([]Table{tableA, tableB})

	pinned := set.SnapshotAndPin()
	if len(pinned) != 2 {
		t.Fatalf("expected 2 pinned tables, got %d", len(pinned))
	}

	removed := set.Apply(Edit{RemovePath: []string{"a.sst"}})
	if len(removed) != 0 {
		t.Fatalf("expected removed to be empty while pinned, got %d", len(removed))
	}
	pending := set.Pending()
	if len(pending) != 1 || pending[0].Meta.Path != "a.sst" {
		t.Fatalf("expected a.sst pending, got %+v", pending)
	}

	ready := set.Unpin([]string{"a.sst"})
	if len(ready) != 1 || ready[0].Meta.Path != "a.sst" {
		t.Fatalf("expected a.sst ready after unpin, got %+v", ready)
	}
	if len(set.Pending()) != 0 {
		t.Fatalf("expected no pending tables")
	}
}

func TestSetResolveMissing(t *testing.T) {
	set := NewSet(nil)
	_, err := set.Resolve([]metadata.TableMeta{{Path: "missing"}})
	if err == nil {
		t.Fatalf("expected missing table error")
	}
}
