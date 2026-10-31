//go:build linux

package iofs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestIOUringStrictReadWrite(t *testing.T) {
	fs, err := newIOUringFS(OSFS{}, AsyncConfig{MaxInFlight: 8})
	if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
		t.Skipf("kernel or sandbox does not permit io_uring: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fs.(*ioUringFS).Close(); err != nil {
			t.Error(err)
		}
	})
	f, err := fs.OpenFile(filepath.Join(t.TempDir(), "data"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	want := []byte("io_uring retained")
	if n, err := WriteAtContext(ctx, f, want, 3); err != nil || n != len(want) {
		t.Fatalf("write: %d, %v", n, err)
	}
	got := make([]byte, len(want))
	if n, err := ReadAtContext(ctx, f, got, 3); err != nil || n != len(want) || !bytes.Equal(got, want) {
		t.Fatalf("read: %d, %v, %q", n, err, got)
	}
}

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
