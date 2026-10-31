//go:build linux

package iouring

import (
	"os"
	"testing"
	"unsafe"

	iouring_syscall "lsmengine/internal/lsm/iofs/internal/iouring/syscall"
)

func TestSharedMappingUsesLargerQueue(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "mapping")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(16384); err != nil {
		t.Fatal(err)
	}
	iour := &IOURing{fd: int(f.Fd()), params: new(iouring_syscall.IOURingParams), sq: new(SubmissionQueue), cq: new(CompletionQueue)}
	iour.params.Features = iouring_syscall.IORING_FEAT_SINGLE_MMAP
	iour.params.SQEntries = 2
	iour.params.SQOffset.Array = 64
	iour.params.CQEntries = 512
	iour.params.CQOffset.Cqes = 128
	t.Cleanup(func() {
		if err := munmapIOURing(iour); err != nil {
			t.Error(err)
		}
	})
	if err := mmapSQ(iour); err != nil {
		t.Fatal(err)
	}
	want := uint32(128 + 512*16)
	if iour.sq.size != want || len(iour.sq.mapping) != int(want) {
		t.Fatalf("mapping size %d, want %d", iour.sq.size, want)
	}
	iour.cq.ptr = iour.sq.ptr
	if err := mmapCQ(iour); err != nil {
		t.Fatal(err)
	}
	if iour.cq.mapping != nil {
		t.Fatal("shared mapping has two owners")
	}
	*(*byte)(unsafe.Add(iour.sq.ptr, want-1)) = 42
	if err := munmapIOURing(iour); err != nil {
		t.Fatal(err)
	}
	if iour.sq != nil || iour.cq != nil {
		t.Fatal("queues not cleared")
	}
}

func TestMappingFailureCleanup(t *testing.T) {
	iour := &IOURing{fd: -1, params: new(iouring_syscall.IOURingParams)}
	iour.params.SQEntries = 8
	iour.params.SQOffset.Array = 64
	if err := mmapIOURing(iour); err == nil {
		t.Fatal("expected invalid descriptor failure")
	}
	if iour.sq != nil || iour.cq != nil {
		t.Fatal("failed mapping retained queues")
	}
	if err := munmapIOURing(iour); err != nil {
		t.Fatal(err)
	}
}
