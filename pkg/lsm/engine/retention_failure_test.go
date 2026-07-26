package engine

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"lsmengine/internal/lsm/iofs"
	memtable "lsmengine/internal/lsm/memtable"
	"lsmengine/internal/lsm/wal/segment"
)

var errRetentionManifestSync = errors.New("injected manifest sync failure")

type retentionFailureFS struct {
	iofs.FS
	fail atomic.Bool
}

func (fs *retentionFailureFS) OpenFile(path string, flags int, mode os.FileMode) (iofs.File, error) {
	f, err := fs.FS.OpenFile(path, flags, mode)
	name := filepath.Base(path)
	if err != nil || (name != "manifest.log" && name != "manifest.json.tmp") {
		return f, err
	}
	return &retentionFailureFile{File: f, fs: fs}, nil
}

type retentionFailureFile struct {
	iofs.File
	fs *retentionFailureFS
}

func (f *retentionFailureFile) Sync() error {
	if f.fs.fail.Load() {
		return errRetentionManifestSync
	}
	return f.File.Sync()
}

func TestRetentionPreservesRecoveryAfterManifestFailure(t *testing.T) {
	fs := &retentionFailureFS{FS: iofs.OSFS{}}
	dir := t.TempDir()
	store, err := New(Options{DataDir: dir, IOFS: fs, MemtableLimit: 1 << 20, WALSync: true, WALBlockSize: 64, WALMaxSegmentBytes: 64, WALRetainArchivedSegments: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { store.cancel(); _ = store.Close() }()
	var frozen []memtable.Table
	for _, key := range []string{"a", "b", "c"} {
		if err := store.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
		frozen = append(frozen, store.freezeMemtableIfCurrent(store.activeMem()))
	}
	store.memMu.Lock()
	store.flushQueue = append(store.flushQueue, frozen...)
	store.memMu.Unlock()
	fs.fail.Store(true)
	for _, index := range []int{0, 2} {
		table, err := store.flusher.Flush(entriesFromTable(frozen[index]))
		if err != nil {
			t.Fatal(err)
		}
		store.flushSvc.onFlushTable(table, frozen[index])
		fs.fail.Store(false)
		if stats := store.Stats(); stats.WAL.CheckpointSeq != 0 || stats.WAL.CheckpointLag != 3 {
			t.Fatalf("failed checkpoint changed durable prefix stats: %+v", stats.WAL)
		}
		if _, err := store.manifest.Load(); !errors.Is(err, errRetentionManifestSync) {
			t.Fatalf("manifest failure was not latched: %v", err)
		}
		for _, mem := range frozen {
			if !store.flushQueued(mem) || len(entriesFromTable(mem)) != 1 {
				t.Fatal("failed publication released uncheckpointed memory")
			}
		}
		if marker, err := segment.ReadPrunedThrough(filepath.Join(dir, "wal.log")); err != nil || marker != 0 {
			t.Fatalf("failed publication authorized pruning: %d %v", marker, err)
		}
	}

	// Recovery must succeed even if the failed manifest/table publication is lost.
	recoveryDir := t.TempDir()
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.Name() != "wal.log" && !strings.HasPrefix(file.Name(), "wal.log.") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(recoveryDir, file.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	recovered, err := New(Options{DataDir: recoveryDir, WALBlockSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	for _, key := range []string{"a", "b", "c"} {
		if entry, ok := recovered.Get([]byte(key)); !ok || string(entry.Value) != "value" {
			t.Fatalf("lost %s after failed checkpoint: %+v", key, entry)
		}
	}
}
