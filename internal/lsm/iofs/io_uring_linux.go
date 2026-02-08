//go:build linux

package iofs

import (
	"context"
	"fmt"
	"os"

	iouring "github.com/iceber/iouring-go"
)

func newIOUringFS(base FS, cfg AsyncConfig) (FS, error) {
	if base == nil {
		base = OSFS{}
	}
	entries := cfg.MaxInFlight
	if entries <= 0 {
		entries = 64
	}
	ring, err := iouring.New(uint(entries))
	if err != nil {
		return nil, fmt.Errorf("io_uring init: %w", err)
	}
	return &ioUringFS{
		base: base,
		ring: ring,
	}, nil
}

type ioUringFS struct {
	base FS
	ring *iouring.IOURing
}

func (i *ioUringFS) Open(path string) (File, error) {
	f, err := i.base.Open(path)
	if err != nil {
		return nil, err
	}
	return i.wrapFile(f)
}

func (i *ioUringFS) OpenFile(path string, flag int, perm os.FileMode) (File, error) {
	f, err := i.base.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return i.wrapFile(f)
}

func (i *ioUringFS) MkdirAll(path string, perm os.FileMode) error { return i.base.MkdirAll(path, perm) }
func (i *ioUringFS) Remove(path string) error                     { return i.base.Remove(path) }
func (i *ioUringFS) Rename(oldpath, newpath string) error         { return i.base.Rename(oldpath, newpath) }
func (i *ioUringFS) Stat(path string) (os.FileInfo, error)        { return i.base.Stat(path) }
func (i *ioUringFS) ReadFile(path string) ([]byte, error)         { return i.base.ReadFile(path) }
func (i *ioUringFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	return i.base.WriteFile(path, data, perm)
}
func (i *ioUringFS) Truncate(path string, size int64) error { return i.base.Truncate(path, size) }

func (i *ioUringFS) Close() error {
	if i.ring != nil {
		if err := i.ring.Close(); err != nil {
			return err
		}
	}
	if c, ok := i.base.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

func (i *ioUringFS) wrapFile(f File) (File, error) {
	osFile, ok := f.(*os.File)
	if !ok {
		return nil, fmt.Errorf("io_uring requires *os.File")
	}
	return &ioUringFile{
		File:   f,
		osFile: osFile,
		ring:   i.ring,
	}, nil
}

type ioUringFile struct {
	File
	osFile *os.File
	ring   *iouring.IOURing
}

func (f *ioUringFile) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ch := make(chan iouring.Result, 1)
	req, err := f.ring.Pread(f.osFile, p, uint64(off), ch)
	if err != nil {
		return 0, err
	}
	select {
	case res := <-ch:
		return res.ReturnInt()
	case <-ctx.Done():
		if _, cancelErr := req.Cancel(); cancelErr == nil {
			<-req.Done()
		}
		return 0, ctx.Err()
	}
}

func (f *ioUringFile) WriteAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ch := make(chan iouring.Result, 1)
	req, err := f.ring.Pwrite(f.osFile, p, uint64(off), ch)
	if err != nil {
		return 0, err
	}
	select {
	case res := <-ch:
		return res.ReturnInt()
	case <-ctx.Done():
		if _, cancelErr := req.Cancel(); cancelErr == nil {
			<-req.Done()
		}
		return 0, ctx.Err()
	}
}
