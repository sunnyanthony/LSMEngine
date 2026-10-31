//go:build linux
// +build linux

package iouring

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"

	iouring_syscall "lsmengine/internal/lsm/iofs/internal/iouring/syscall"
)

const uint32Size = uint32(unsafe.Sizeof(uint32(0)))

func mmapIOURing(iour *IOURing) (err error) {
	defer func() {
		if err != nil {
			err = errors.Join(err, munmapIOURing(iour))
		}
	}()
	iour.sq = new(SubmissionQueue)
	iour.cq = new(CompletionQueue)

	if err = mmapSQ(iour); err != nil {
		return err
	}

	if (iour.params.Features & iouring_syscall.IORING_FEAT_SINGLE_MMAP) != 0 {
		iour.cq.ptr = iour.sq.ptr
	}

	if err = mmapCQ(iour); err != nil {
		return err
	}

	if err = mmapSQEs(iour); err != nil {
		return err
	}
	return nil
}

func mmapSQ(iour *IOURing) (err error) {
	sq := iour.sq
	params := iour.params

	sq.size = params.SQOffset.Array + params.SQEntries*uint32Size
	if params.Features&iouring_syscall.IORING_FEAT_SINGLE_MMAP != 0 {
		cqSize := params.CQOffset.Cqes + params.CQEntries*makeCompletionQueueRing(params.Flags).entrySz()
		if cqSize > sq.size {
			sq.size = cqSize
		}
	}
	sq.mapping, err = mmap(iour.fd, sq.size, iouring_syscall.IORING_OFF_SQ_RING)
	if err != nil {
		return fmt.Errorf("mmap sq ring: %w", err)
	}

	sq.ptr = unsafe.Pointer(&sq.mapping[0])
	sq.head = (*uint32)(unsafe.Add(sq.ptr, params.SQOffset.Head))
	sq.tail = (*uint32)(unsafe.Add(sq.ptr, params.SQOffset.Tail))
	sq.mask = (*uint32)(unsafe.Add(sq.ptr, params.SQOffset.RingMask))
	sq.entries = (*uint32)(unsafe.Add(sq.ptr, params.SQOffset.RingEntries))
	sq.flags = (*uint32)(unsafe.Add(sq.ptr, params.SQOffset.Flags))
	sq.dropped = (*uint32)(unsafe.Add(sq.ptr, params.SQOffset.Dropped))

	sq.array = unsafe.Slice((*uint32)(unsafe.Add(sq.ptr, params.SQOffset.Array)), int(params.SQEntries))

	return nil
}

func mmapCQ(iour *IOURing) (err error) {
	params := iour.params
	cq := iour.cq

	cqes := makeCompletionQueueRing(params.Flags)

	cq.size = params.CQOffset.Cqes + params.CQEntries*cqes.entrySz()
	if cq.ptr == nil {
		cq.mapping, err = mmap(iour.fd, cq.size, iouring_syscall.IORING_OFF_CQ_RING)
		if err != nil {
			return fmt.Errorf("mmap cq ring: %w", err)
		}
	}

	if cq.ptr == nil {
		cq.ptr = unsafe.Pointer(&cq.mapping[0])
	}
	cq.head = (*uint32)(unsafe.Add(cq.ptr, params.CQOffset.Head))
	cq.tail = (*uint32)(unsafe.Add(cq.ptr, params.CQOffset.Tail))
	cq.mask = (*uint32)(unsafe.Add(cq.ptr, params.CQOffset.RingMask))
	cq.entries = (*uint32)(unsafe.Add(cq.ptr, params.CQOffset.RingEntries))
	cq.flags = (*uint32)(unsafe.Add(cq.ptr, params.CQOffset.Flags))
	cq.overflow = (*uint32)(unsafe.Add(cq.ptr, params.CQOffset.Overflow))

	cqes.assignQueue(unsafe.Add(cq.ptr, params.CQOffset.Cqes), int(params.CQEntries))
	cq.cqes = cqes

	return nil
}

func mmapSQEs(iour *IOURing) error {
	params := iour.params

	sqes := makeSubmissionQueueRing(params.Flags)

	ptr, err := mmap(iour.fd, params.SQEntries*sqes.entrySz(), iouring_syscall.IORING_OFF_SQES)
	if err != nil {
		return fmt.Errorf("mmap sqe array: %w", err)
	}

	iour.sq.sqeMapping = ptr
	sqes.assignQueue(unsafe.Pointer(&ptr[0]), int(params.SQEntries))
	iour.sq.sqes = sqes

	return nil
}

func munmapIOURing(iour *IOURing) error {
	var errs []error
	unmap := func(mapping *[]byte) {
		if len(*mapping) == 0 {
			return
		}
		if err := syscall.Munmap(*mapping); err != nil {
			errs = append(errs, err)
		} else {
			*mapping = nil
		}
	}
	// A shared SQ/CQ mapping is owned only by the submission queue.
	if iour.sq != nil {
		unmap(&iour.sq.sqeMapping)
		unmap(&iour.sq.mapping)
	}
	if iour.cq != nil {
		unmap(&iour.cq.mapping)
	}
	if len(errs) == 0 {
		iour.sq, iour.cq = nil, nil
	}
	return errors.Join(errs...)
}

func mmap(fd int, length uint32, offset uint64) ([]byte, error) {
	return syscall.Mmap(fd, int64(offset), int(length), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
}
