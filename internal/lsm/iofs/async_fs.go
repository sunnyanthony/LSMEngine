// AsyncFS wraps a base FS and adds context-aware ReadAt/WriteAt helpers.

package iofs

import (
	"context"
	"os"
)

// AsyncConfig tunes AsyncFS behavior.
type AsyncConfig struct {
	MaxInFlight int
}

// NewAsyncFS wraps the base filesystem with async-capable files.
func NewAsyncFS(base FS, cfg AsyncConfig) FS {
	if base == nil {
		base = OSFS{}
	}
	max := cfg.MaxInFlight
	if max <= 0 {
		max = 64
	}
	return &asyncFS{
		base: base,
		sem:  make(chan struct{}, max),
	}
}

type asyncFS struct {
	base FS
	sem  chan struct{}
}

func (a *asyncFS) Open(path string) (File, error) {
	f, err := a.base.Open(path)
	if err != nil {
		return nil, err
	}
	return &asyncFile{File: f, sem: a.sem}, nil
}

func (a *asyncFS) OpenFile(path string, flag int, perm os.FileMode) (File, error) {
	f, err := a.base.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &asyncFile{File: f, sem: a.sem}, nil
}

func (a *asyncFS) MkdirAll(path string, perm os.FileMode) error { return a.base.MkdirAll(path, perm) }
func (a *asyncFS) Remove(path string) error                     { return a.base.Remove(path) }
func (a *asyncFS) Rename(oldpath, newpath string) error         { return a.base.Rename(oldpath, newpath) }
func (a *asyncFS) Stat(path string) (os.FileInfo, error)        { return a.base.Stat(path) }
func (a *asyncFS) ReadFile(path string) ([]byte, error)         { return a.base.ReadFile(path) }
func (a *asyncFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	return a.base.WriteFile(path, data, perm)
}
func (a *asyncFS) Truncate(path string, size int64) error { return a.base.Truncate(path, size) }

func (a *asyncFS) Close() error {
	if c, ok := a.base.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

type asyncFile struct {
	File
	sem chan struct{}
}

func (a *asyncFile) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	return a.run(ctx, func() (int, error) {
		return a.File.ReadAt(p, off)
	})
}

func (a *asyncFile) WriteAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	wa, ok := a.File.(ioWriterAt)
	if !ok {
		return 0, context.Canceled
	}
	return a.run(ctx, func() (int, error) {
		return wa.WriteAt(p, off)
	})
}

func (a *asyncFile) run(ctx context.Context, fn func() (int, error)) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case a.sem <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	done := make(chan struct{})
	var n int
	var err error
	go func() {
		n, err = fn()
		<-a.sem
		close(done)
	}()
	select {
	case <-done:
		return n, err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}
