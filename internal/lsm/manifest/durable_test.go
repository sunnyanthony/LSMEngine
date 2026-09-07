package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"lsmengine/internal/lsm/iofs"
)

var errInjectedSync = errors.New("injected sync failure")

type durabilityFS struct {
	iofs.FS
	dir    string
	fail   string
	events []string
}

func (fs *durabilityFS) record(event string) error {
	fs.events = append(fs.events, event)
	if event == fs.fail {
		return errInjectedSync
	}
	return nil
}

func (fs *durabilityFS) wrap(path string, file iofs.File, err error) (iofs.File, error) {
	if err != nil {
		return nil, err
	}
	name := filepath.Base(path)
	if path == fs.dir {
		name = "dir"
	}
	return &durabilityFile{File: file, fs: fs, name: name}, nil
}

func (fs *durabilityFS) Open(path string) (iofs.File, error) {
	f, err := fs.FS.Open(path)
	return fs.wrap(path, f, err)
}

func (fs *durabilityFS) OpenFile(path string, flags int, mode os.FileMode) (iofs.File, error) {
	f, err := fs.FS.OpenFile(path, flags, mode)
	return fs.wrap(path, f, err)
}

func (fs *durabilityFS) Rename(from, to string) error {
	if err := fs.record("rename:" + filepath.Base(to)); err != nil {
		return err
	}
	return fs.FS.Rename(from, to)
}

func (fs *durabilityFS) Truncate(path string, size int64) error {
	if err := fs.record("truncate:" + filepath.Base(path)); err != nil {
		return err
	}
	return fs.FS.Truncate(path, size)
}

type durabilityFile struct {
	iofs.File
	fs   *durabilityFS
	name string
}

func (f *durabilityFile) Sync() error {
	if err := f.fs.record("sync:" + f.name); err != nil {
		return err
	}
	return f.File.Sync()
}

func TestManifestDurabilityOrderingAndFailureLatch(t *testing.T) {
	for _, fail := range []string{"", "sync:manifest.log", "sync:dir", "sync:manifest.json.tmp", "rename:manifest.json", "truncate:manifest.log"} {
		t.Run(fail, func(t *testing.T) {
			dir := t.TempDir()
			fs := &durabilityFS{FS: iofs.OSFS{}, dir: dir, fail: fail}
			store, err := NewLogStore(LogOptions{LogPath: filepath.Join(dir, "manifest.log"), CheckpointPath: filepath.Join(dir, "manifest.json"), CheckpointEveryN: 1, FS: fs})
			if err != nil {
				t.Fatal(err)
			}
			err = store.Update(func(m Manifest) Manifest { m.WALSeq = 7; return m })
			if fail == "" {
				if err != nil {
					t.Fatal(err)
				}
				want := []string{"sync:manifest.log", "sync:dir", "sync:manifest.json.tmp", "rename:manifest.json", "sync:dir", "truncate:manifest.log", "sync:manifest.log"}
				if !reflect.DeepEqual(fs.events, want) {
					t.Fatalf("durability order: %v, want %v", fs.events, want)
				}
				return
			}
			if !errors.Is(err, errInjectedSync) {
				t.Fatalf("expected injected failure, got %v", err)
			}
			calls := len(fs.events)
			fs.fail = ""
			if _, err := store.Load(); !errors.Is(err, errInjectedSync) {
				t.Fatalf("failed state exposed by Load: %v", err)
			}
			if err := store.Update(func(m Manifest) Manifest { m.WALSeq = 8; return m }); !errors.Is(err, errInjectedSync) {
				t.Fatalf("update after failure: %v", err)
			}
			if err := store.Save(Manifest{}); !errors.Is(err, errInjectedSync) {
				t.Fatalf("save after failure: %v", err)
			}
			if len(fs.events) != calls {
				t.Fatal("failed store attempted more persistence")
			}
		})
	}
}
