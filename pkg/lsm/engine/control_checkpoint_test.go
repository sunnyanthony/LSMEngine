package engine

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"lsmengine/internal/lsm/iofs"
)

type checkpointSyncFS struct {
	iofs.OSFS
	events   []string
	failPath string
	err      error
}

type checkpointSyncFile struct {
	iofs.File
	fs   *checkpointSyncFS
	path string
}

func (f *checkpointSyncFile) Sync() error {
	f.fs.events = append(f.fs.events, "sync:"+f.path)
	if f.path == f.fs.failPath {
		return f.fs.err
	}
	return f.File.Sync()
}

func (f *checkpointSyncFS) Open(path string) (iofs.File, error) {
	file, err := f.OSFS.Open(path)
	if err != nil {
		return nil, err
	}
	return &checkpointSyncFile{File: file, fs: f, path: path}, nil
}

func (f *checkpointSyncFS) OpenFile(path string, flag int, perm os.FileMode) (iofs.File, error) {
	file, err := f.OSFS.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &checkpointSyncFile{File: file, fs: f, path: path}, nil
}

func (f *checkpointSyncFS) Rename(old, next string) error {
	f.events = append(f.events, "rename")
	return f.OSFS.Rename(old, next)
}

func TestControlCheckpointSyncOrderAndFailures(t *testing.T) {
	for _, failure := range []string{"none", "file", "directory", "ancestor"} {
		t.Run(failure, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "nested", "control_state.json")
			injected := errors.New("injected sync failure")
			fs := &checkpointSyncFS{err: injected}
			switch failure {
			case "file":
				fs.failPath = path + ".tmp"
			case "directory":
				fs.failPath = filepath.Dir(path)
			case "ancestor":
				fs.failPath = root
			}
			c := &controlPlane{fs: fs, statePath: path}
			err := c.saveLocked()
			if failure != "none" {
				if !errors.Is(err, injected) {
					t.Fatalf("save = %v", err)
				}
				if failure == "file" && !reflect.DeepEqual(fs.events, []string{"sync:" + path + ".tmp"}) {
					t.Fatalf("published before file sync: %v", fs.events)
				}
				fs.failPath = ""
				fs.events = nil
				err = c.saveLocked()
			}
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"sync:" + path + ".tmp", "rename"}
			for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
				want = append(want, "sync:"+dir)
				if filepath.Dir(dir) == dir {
					break
				}
			}
			if !reflect.DeepEqual(fs.events, want) {
				t.Fatalf("events = %v, want %v", fs.events, want)
			}
		})
	}
}
