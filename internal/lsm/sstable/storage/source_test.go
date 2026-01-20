package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"lsmengine/internal/lsm/sstable/config"
)

func TestBlockSourceRead(t *testing.T) {
	data := []byte("hello world")
	f, err := os.CreateTemp(t.TempDir(), "sstable-source-*")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	opts := config.DefaultOptions("dir")
	opts.UseMmap = false
	source := NewBlockSource(f, info.Size(), opts)
	defer source.Close()

	desc := BlockDescriptor{
		Offset: 0,
		Length: uint32(len(data)),
	}
	view, err := source.Read(context.Background(), desc, ReadHint{})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(view.Data, data) {
		t.Fatalf("expected %q, got %q", data, view.Data)
	}
}

func TestBlockSourceReadOutOfRange(t *testing.T) {
	data := []byte("hello")
	f, err := os.CreateTemp(t.TempDir(), "sstable-source-*")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	source := NewBlockSource(f, info.Size(), config.DefaultOptions("dir"))
	defer source.Close()

	desc := BlockDescriptor{
		Offset: uint64(len(data) + 1),
		Length: 1,
	}
	_, err = source.Read(context.Background(), desc, ReadHint{})
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestBlockSourceReadCanceled(t *testing.T) {
	data := []byte("hello")
	f, err := os.CreateTemp(t.TempDir(), "sstable-source-*")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	source := NewBlockSource(f, info.Size(), config.DefaultOptions("dir"))
	defer source.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	desc := BlockDescriptor{
		Offset: 0,
		Length: uint32(len(data)),
	}
	_, err = source.Read(ctx, desc, ReadHint{})
	if err == nil {
		t.Fatalf("expected context error")
	}
}
