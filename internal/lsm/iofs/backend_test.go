//go:build !linux

package iofs

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
)

func TestSelectFSIoUringFallbackNonLinux(t *testing.T) {
	fs, err := SelectFS(BackendIOUring, OSFS{}, AsyncConfig{MaxInFlight: 1}, false)
	if err != nil {
		t.Fatalf("select fs: %v", err)
	}
	path := "test-fallback"
	f, err := fs.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() {
		_ = f.Close()
		_ = os.Remove(path)
	})
	if _, ok := f.(ReaderAtContext); !ok {
		t.Fatalf("expected context-aware reader for fallback fs")
	}
	buf := make([]byte, 1)
	if _, err := ReadAtContext(context.Background(), f, buf, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read at context: %v", err)
	}
}
