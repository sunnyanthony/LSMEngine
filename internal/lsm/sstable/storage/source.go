// BlockSource for file/mmap reads.

package storage

import (
	"context"
	"errors"
	"io"

	"lsmengine/internal/lsm/iofs"
	"lsmengine/internal/lsm/memory"
	"lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/sstable/format"
)

type ReadHint struct {
	Pin bool
}

type BlockDescriptor struct {
	ID     uint64
	Type   format.BlockType
	Offset uint64
	Length uint32
	Key    []byte
}

type BlockView struct {
	Data    []byte
	Release func()
}

type BlockSource interface {
	Read(ctx context.Context, desc BlockDescriptor, hint ReadHint) (BlockView, error)
	Mmapped() bool
}

var errMmapUnsupported = errors.New("sstable: mmap unsupported")

type blockSource struct {
	file iofs.File
	size int64
	mmap []byte
	pool *memory.BufferPool
}

func NewBlockSource(file iofs.File, size int64, opts config.Options) *blockSource {
	source := &blockSource{
		file: file,
		size: size,
		pool: memory.NewBufferPool(opts.ReadBufferMaxBytes),
	}
	if opts.UseMmap {
		if data, err := mmapFile(file, size); err == nil {
			source.mmap = data
		}
	}
	return source
}

func (s *blockSource) Mmapped() bool {
	return s != nil && s.mmap != nil
}

func (s *blockSource) Read(ctx context.Context, desc BlockDescriptor, hint ReadHint) (BlockView, error) {
	if desc.Length == 0 {
		return BlockView{}, io.EOF
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return BlockView{}, ctx.Err()
		default:
		}
	}
	if s == nil || s.file == nil {
		return BlockView{}, io.EOF
	}
	end := int64(desc.Offset) + int64(desc.Length)
	if desc.Offset > uint64(s.size) || end > s.size {
		return BlockView{}, io.EOF
	}
	if s.mmap != nil {
		start := int(desc.Offset)
		return BlockView{Data: s.mmap[start : start+int(desc.Length)]}, nil
	}
	if hint.Pin || s.pool == nil || int(desc.Length) > s.pool.Max() {
		buf := make([]byte, desc.Length)
		if _, err := iofs.ReadAtContext(ctx, s.file, buf, int64(desc.Offset)); err != nil {
			return BlockView{}, err
		}
		return BlockView{Data: buf}, nil
	}
	buf := s.pool.Get(int(desc.Length))
	if _, err := iofs.ReadAtContext(ctx, s.file, buf[:desc.Length], int64(desc.Offset)); err != nil {
		s.pool.Put(buf)
		return BlockView{}, err
	}
	return BlockView{
		Data: buf[:desc.Length],
		Release: func() {
			s.pool.Put(buf)
		},
	}, nil
}

func (s *blockSource) Close() error {
	if s == nil {
		return nil
	}
	if s.mmap != nil {
		_ = munmap(s.mmap)
		s.mmap = nil
	}
	return nil
}
