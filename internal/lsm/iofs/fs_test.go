package iofs

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

type testFile struct {
	data           []byte
	readAtCalled   bool
	readCtxCalled  bool
	writeAtCalled  bool
	writeCtxCalled bool
}

func (f *testFile) Read(p []byte) (int, error)  { return 0, nil }
func (f *testFile) Write(p []byte) (int, error) { return len(p), nil }
func (f *testFile) Close() error                { return nil }
func (f *testFile) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}
func (f *testFile) Sync() error                { return nil }
func (f *testFile) Stat() (os.FileInfo, error) { return fakeInfo{size: int64(len(f.data))}, nil }
func (f *testFile) Fd() uintptr                { return 0 }
func (f *testFile) ReadAt(p []byte, off int64) (int, error) {
	f.readAtCalled = true
	if off >= int64(len(f.data)) {
		return 0, ioErrEOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, ioErrEOF
	}
	return n, nil
}
func (f *testFile) WriteAt(p []byte, off int64) (int, error) {
	f.writeAtCalled = true
	end := int(off) + len(p)
	if end > len(f.data) {
		buf := make([]byte, end)
		copy(buf, f.data)
		f.data = buf
	}
	copy(f.data[off:], p)
	return len(p), nil
}
func (f *testFile) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	f.readCtxCalled = true
	if off >= int64(len(f.data)) {
		return 0, ioErrEOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, ioErrEOF
	}
	return n, nil
}
func (f *testFile) WriteAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	f.writeCtxCalled = true
	end := int(off) + len(p)
	if end > len(f.data) {
		buf := make([]byte, end)
		copy(buf, f.data)
		f.data = buf
	}
	copy(f.data[off:], p)
	return len(p), nil
}

type basicFile struct {
	data          []byte
	readAtCalled  bool
	writeAtCalled bool
}

func (f *basicFile) Read(p []byte) (int, error)  { return 0, nil }
func (f *basicFile) Write(p []byte) (int, error) { return len(p), nil }
func (f *basicFile) Close() error                { return nil }
func (f *basicFile) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}
func (f *basicFile) Sync() error                { return nil }
func (f *basicFile) Stat() (os.FileInfo, error) { return fakeInfo{size: int64(len(f.data))}, nil }
func (f *basicFile) Fd() uintptr                { return 0 }
func (f *basicFile) ReadAt(p []byte, off int64) (int, error) {
	f.readAtCalled = true
	if off >= int64(len(f.data)) {
		return 0, ioErrEOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, ioErrEOF
	}
	return n, nil
}
func (f *basicFile) WriteAt(p []byte, off int64) (int, error) {
	f.writeAtCalled = true
	end := int(off) + len(p)
	if end > len(f.data) {
		buf := make([]byte, end)
		copy(buf, f.data)
		f.data = buf
	}
	copy(f.data[off:], p)
	return len(p), nil
}

type fakeInfo struct {
	size int64
}

func (f fakeInfo) Name() string       { return "fake" }
func (f fakeInfo) Size() int64        { return f.size }
func (f fakeInfo) Mode() os.FileMode  { return 0 }
func (f fakeInfo) ModTime() time.Time { return time.Unix(0, 0) }
func (f fakeInfo) IsDir() bool        { return false }
func (f fakeInfo) Sys() any           { return nil }

var ioErrEOF = errors.New("eof")

func TestReadAtContextUsesContextReader(t *testing.T) {
	f := &testFile{data: []byte("hello")}
	buf := make([]byte, 5)
	if _, err := ReadAtContext(context.Background(), f, buf, 0); err != nil && err != ioErrEOF {
		t.Fatalf("read at context: %v", err)
	}
	if !f.readCtxCalled {
		t.Fatalf("expected ReadAtContext to be used")
	}
	if f.readAtCalled {
		t.Fatalf("expected ReadAtContext to bypass ReadAt")
	}
}

func TestReadAtContextFallsBackToReadAt(t *testing.T) {
	f := &basicFile{data: []byte("hello")}
	buf := make([]byte, 5)
	if _, err := ReadAtContext(context.Background(), f, buf, 0); err != nil && err != ioErrEOF {
		t.Fatalf("read at context: %v", err)
	}
	if f.readAtCalled != true {
		t.Fatalf("expected ReadAt to be used")
	}
}

func TestWriteAtContextUsesContextWriter(t *testing.T) {
	f := &testFile{}
	if _, err := WriteAtContext(context.Background(), f, []byte("x"), 0); err != nil {
		t.Fatalf("write at context: %v", err)
	}
	if !f.writeCtxCalled {
		t.Fatalf("expected WriteAtContext to be used")
	}
	if f.writeAtCalled {
		t.Fatalf("expected WriteAtContext to bypass WriteAt")
	}
}

func TestWriteAtContextFallsBackToWriteAt(t *testing.T) {
	f := &basicFile{}
	if _, err := WriteAtContext(context.Background(), f, []byte("x"), 0); err != nil {
		t.Fatalf("write at context: %v", err)
	}
	if !f.writeAtCalled {
		t.Fatalf("expected WriteAt to be used")
	}
}

func TestReadWriteContextNilFile(t *testing.T) {
	if _, err := ReadAtContext(context.Background(), nil, nil, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled for nil read, got %v", err)
	}
	if _, err := WriteAtContext(context.Background(), nil, nil, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled for nil write, got %v", err)
	}
}
