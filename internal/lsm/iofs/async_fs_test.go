package iofs

import (
	"context"
	"os"
	"testing"
	"time"
)

type stubFS struct {
	file File
}

func (s stubFS) Open(path string) (File, error) { return s.file, nil }
func (s stubFS) OpenFile(path string, flag int, perm os.FileMode) (File, error) {
	return s.file, nil
}
func (s stubFS) MkdirAll(path string, perm os.FileMode) error { return nil }
func (s stubFS) Remove(path string) error                     { return nil }
func (s stubFS) Rename(oldpath, newpath string) error         { return nil }
func (s stubFS) Stat(path string) (os.FileInfo, error)        { return fakeInfo{}, nil }
func (s stubFS) ReadFile(path string) ([]byte, error)         { return nil, nil }
func (s stubFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	return nil
}
func (s stubFS) Truncate(path string, size int64) error { return nil }

func TestAsyncFSWrapsFile(t *testing.T) {
	base := &basicFile{data: []byte("hello")}
	fs := NewAsyncFS(stubFS{file: base}, AsyncConfig{MaxInFlight: 1})
	f, err := fs.Open("x")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	buf := make([]byte, 5)
	if _, err := ReadAtContext(ctx, f, buf, 0); err != nil && err != ioErrEOF {
		t.Fatalf("read at context: %v", err)
	}
	if !base.readAtCalled {
		t.Fatalf("expected base ReadAt to be called")
	}
}
