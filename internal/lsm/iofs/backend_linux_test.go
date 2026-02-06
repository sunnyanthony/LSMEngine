//go:build linux

package iofs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestSelectFSIoUringNonStrictLinux(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "io-uring-test")
	if err := os.WriteFile(path, []byte("hi"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	fs, err := SelectFS(BackendIOUring, OSFS{}, AsyncConfig{MaxInFlight: 1}, false)
	if err != nil {
		t.Fatalf("select fs: %v", err)
	}
	f, err := fs.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_ = f.Close()
	})

	buf := make([]byte, 2)
	if _, err := ReadAtContext(context.Background(), f, buf, 0); err != nil {
		t.Fatalf("read at context: %v", err)
	}
	if !bytes.Equal(buf, []byte("hi")) {
		t.Fatalf("expected %q, got %q", "hi", buf)
	}
}
