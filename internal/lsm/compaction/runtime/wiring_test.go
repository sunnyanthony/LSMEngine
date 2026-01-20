package runtime

import (
	"testing"

	"lsmengine/internal/lsm/compaction"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/internal/lsm/tableset"
)

type stubTableEditor struct {
	add    []tableset.Table
	remove []metadata.TableMeta
	walSeq uint64
	calls  int
}

func (s *stubTableEditor) Apply(add []tableset.Table, remove []metadata.TableMeta, walSeq uint64) error {
	s.add = add
	s.remove = remove
	s.walSeq = walSeq
	s.calls++
	return nil
}

func TestApplyFromEditor(t *testing.T) {
	editor := &stubTableEditor{}
	apply := applyFromEditor(editor)

	result := compaction.Result{
		Output:      []sstable.SSTable{{Path: "out.sst", Seq: 7}},
		Obsolete:    []metadata.TableMeta{{Path: "old.sst", SeqMin: 1, SeqMax: 2}},
		OutputLevel: 2,
	}
	if err := apply(result); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if editor.calls != 1 {
		t.Fatalf("expected Apply to be called once, got %d", editor.calls)
	}
	if len(editor.add) != 1 || editor.add[0].Meta.Path != "out.sst" {
		t.Fatalf("unexpected add: %+v", editor.add)
	}
	if got := editor.add[0].Meta.Level; got != 2 {
		t.Fatalf("expected output level=2, got %d", got)
	}
	if editor.add[0].Meta.SeqMin != 7 || editor.add[0].Meta.SeqMax != 7 {
		t.Fatalf("expected seq bounds=7, got %+v", editor.add[0].Meta)
	}
	if len(editor.remove) != 1 || editor.remove[0].Path != "old.sst" {
		t.Fatalf("unexpected remove: %+v", editor.remove)
	}
	if editor.walSeq != 0 {
		t.Fatalf("expected walSeq=0, got %d", editor.walSeq)
	}
}
