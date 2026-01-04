package memtable

import "sync"

// DefaultArenaBlockSize is the default block size for memtable arenas.
const DefaultArenaBlockSize = 256 * 1024

// Arena is a simple bump allocator for memtable key/value storage.
// It is concurrency-safe so CopyEntry can be called outside shard locks.
type Arena struct {
	mu        sync.Mutex
	blockSize int
	blocks    [][]byte
	cur       []byte
	off       int
}

func NewArena(blockSize int) *Arena {
	if blockSize <= 0 {
		blockSize = DefaultArenaBlockSize
	}
	return &Arena{blockSize: blockSize}
}

func (a *Arena) Alloc(n int) []byte {
	if n <= 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if n > a.blockSize {
		buf := make([]byte, n)
		a.blocks = append(a.blocks, buf)
		return buf
	}
	if a.cur == nil || a.off+n > len(a.cur) {
		a.cur = make([]byte, a.blockSize)
		a.blocks = append(a.blocks, a.cur)
		a.off = 0
	}
	buf := a.cur[a.off : a.off+n]
	a.off += n
	return buf
}

func (a *Arena) AllocCopy(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := a.Alloc(len(src))
	copy(dst, src)
	return dst
}
