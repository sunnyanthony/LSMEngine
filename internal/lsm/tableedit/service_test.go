package tableedit

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"lsmengine/internal/lsm/manifest"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/internal/lsm/tableset"
)

type memManifestStore struct {
	mu sync.Mutex
	m  manifest.Manifest
}

func (s *memManifestStore) Load() (manifest.Manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m, nil
}

func (s *memManifestStore) Save(m manifest.Manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m = m
	return nil
}

func (s *memManifestStore) Update(fn func(manifest.Manifest) manifest.Manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m = fn(s.m)
	return nil
}

func TestServiceApplyUpdatesState(t *testing.T) {
	dir := t.TempDir()
	removePath := filepath.Join(dir, "old.sst")
	if err := os.WriteFile(removePath, []byte("old"), 0o644); err != nil {
		t.Fatalf("write old table: %v", err)
	}
	addPath := filepath.Join(dir, "new.sst")

	removeMeta := metadata.TableMeta{
		Path:      removePath,
		Level:     0,
		SeqMin:    1,
		SeqMax:    10,
		SizeBytes: 128,
	}
	addMeta := metadata.TableMeta{
		Path:      addPath,
		Level:     1,
		SeqMin:    11,
		SeqMax:    20,
		SizeBytes: 256,
	}

	tables := tableset.NewSet([]tableset.Table{
		{Meta: removeMeta, Handle: sstable.SSTable{Path: removePath, Seq: removeMeta.SeqMax}},
	})
	store := &memManifestStore{m: manifest.Manifest{
		WALSeq: 1,
		Tables: []manifest.Entry{{
			Path:      removeMeta.Path,
			Level:     removeMeta.Level,
			SeqMin:    removeMeta.SeqMin,
			SeqMax:    removeMeta.SeqMax,
			SizeBytes: removeMeta.SizeBytes,
		}},
	}}

	svc := New(tables, store, nil)
	addTable := tableset.Table{Meta: addMeta, Handle: sstable.SSTable{Path: addPath, Seq: addMeta.SeqMax}}

	if err := svc.Apply([]tableset.Table{addTable}, []metadata.TableMeta{removeMeta}, 42); err != nil {
		t.Fatalf("apply: %v", err)
	}

	snapshot := tables.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("tableset size = %d, want 1", len(snapshot))
	}
	if snapshot[0].Path != addMeta.Path {
		t.Fatalf("tableset path = %s, want %s", snapshot[0].Path, addMeta.Path)
	}

	gotManifest, err := store.Load()
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if gotManifest.WALSeq != 42 {
		t.Fatalf("manifest WALSeq = %d, want 42", gotManifest.WALSeq)
	}
	if len(gotManifest.Tables) != 1 || gotManifest.Tables[0].Path != addMeta.Path {
		t.Fatalf("manifest tables = %+v, want %s", gotManifest.Tables, addMeta.Path)
	}

	if _, err := os.Stat(removePath); !os.IsNotExist(err) {
		t.Fatalf("obsolete table still exists: %v", err)
	}
}
