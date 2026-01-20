// Minimal file interfaces and IO helpers.

package iofs

import (
	"context"
	"io"
	"os"
)

// File is the minimal file interface used by WAL and SSTable IO.
type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Closer
	io.Seeker
	Sync() error
	Stat() (os.FileInfo, error)
	Fd() uintptr
}

// FS abstracts filesystem operations so IO backends can be swapped.
type FS interface {
	Open(path string) (File, error)
	OpenFile(path string, flag int, perm os.FileMode) (File, error)
	MkdirAll(path string, perm os.FileMode) error
	Remove(path string) error
	Rename(oldpath, newpath string) error
	Stat(path string) (os.FileInfo, error)
	ReadFile(path string) ([]byte, error)
	WriteFile(path string, data []byte, perm os.FileMode) error
	Truncate(path string, size int64) error
}

// ReaderAtContext enables async-backed reads (e.g. io_uring) without changing callers.
type ReaderAtContext interface {
	ReadAtContext(ctx context.Context, p []byte, off int64) (int, error)
}

// WriterAtContext enables async-backed writes (e.g. io_uring) without changing callers.
type WriterAtContext interface {
	WriteAtContext(ctx context.Context, p []byte, off int64) (int, error)
}

// ReadAtContext dispatches to a context-aware reader when available.
func ReadAtContext(ctx context.Context, f File, p []byte, off int64) (int, error) {
	if f == nil {
		return 0, context.Canceled
	}
	if r, ok := f.(ReaderAtContext); ok {
		return r.ReadAtContext(ctx, p, off)
	}
	return f.ReadAt(p, off)
}

// WriteAtContext dispatches to a context-aware writer when available.
func WriteAtContext(ctx context.Context, f File, p []byte, off int64) (int, error) {
	if f == nil {
		return 0, context.Canceled
	}
	if w, ok := f.(WriterAtContext); ok {
		return w.WriteAtContext(ctx, p, off)
	}
	if wa, ok := f.(ioWriterAt); ok {
		return wa.WriteAt(p, off)
	}
	return 0, context.Canceled
}

type ioWriterAt interface {
	WriteAt(p []byte, off int64) (int, error)
}
