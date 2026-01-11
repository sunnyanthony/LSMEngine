package arena

import "sync"

// DefaultArenaBlockSize is the default block size for memtable arenas.
const DefaultArenaBlockSize = 256 * 1024

// Options control arena sizing and optional guardrails.
type Options struct {
	// BlockSize is the size of each arena block.
	BlockSize int
	// SoftLimitBytes signals the caller to flush/rotate when exceeded.
	SoftLimitBytes int64
	// HardLimitBytes denies arena allocations once exceeded.
	HardLimitBytes int64
}

// Stats is a snapshot of arena usage.
type Stats struct {
	BlockSize        int
	Blocks           int
	UsedBytes        int64
	ReservedBytes    int64
	SoftLimitBytes   int64
	HardLimitBytes   int64
	HardDeniedAllocs int64
}

// Arena is a bump allocator for memtable key/value storage.
// It is concurrency-safe so CopyEntry can be called outside shard locks.
type Arena struct {
	mu sync.Mutex

	opt Options

	blocks        [][]byte
	cur           []byte
	off           int
	usedBytes     int64
	reservedBytes int64
	hardDenied    int64
}

func NewArena(blockSize int) *Arena {
	return NewArenaWithOptions(Options{BlockSize: blockSize})
}

func NewArenaWithOptions(opt Options) *Arena {
	if opt.BlockSize <= 0 {
		opt.BlockSize = DefaultArenaBlockSize
	}
	return &Arena{opt: opt}
}

func (a *Arena) Alloc(n int) []byte {
	if n <= 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.opt.HardLimitBytes > 0 && a.usedBytes+int64(n) > a.opt.HardLimitBytes {
		a.hardDenied++
		return nil
	}
	if n > a.opt.BlockSize {
		buf := make([]byte, n)
		a.blocks = append(a.blocks, buf)
		a.usedBytes += int64(n)
		a.reservedBytes += int64(n)
		return buf
	}
	if a.cur == nil || a.off+n > len(a.cur) {
		a.cur = make([]byte, a.opt.BlockSize)
		a.blocks = append(a.blocks, a.cur)
		a.off = 0
		a.reservedBytes += int64(len(a.cur))
	}
	buf := a.cur[a.off : a.off+n]
	a.off += n
	a.usedBytes += int64(n)
	return buf
}

func (a *Arena) AllocCopy(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := a.Alloc(len(src))
	if dst == nil {
		return nil
	}
	copy(dst, src)
	return dst
}

// UsedBytes reports bytes handed out by the arena.
func (a *Arena) UsedBytes() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.usedBytes
}

// SoftLimitExceeded reports whether the soft limit is met.
func (a *Arena) SoftLimitExceeded() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.opt.SoftLimitBytes > 0 && a.usedBytes >= a.opt.SoftLimitBytes
}

// Blocks returns the number of blocks held by the arena.
func (a *Arena) Blocks() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.blocks)
}

// Stats returns a snapshot of arena usage.
func (a *Arena) Stats() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Stats{
		BlockSize:        a.opt.BlockSize,
		Blocks:           len(a.blocks),
		UsedBytes:        a.usedBytes,
		ReservedBytes:    a.reservedBytes,
		SoftLimitBytes:   a.opt.SoftLimitBytes,
		HardLimitBytes:   a.opt.HardLimitBytes,
		HardDeniedAllocs: a.hardDenied,
	}
}

// Reset clears usage and keeps a single block for reuse.
func (a *Arena) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()

	var keep []byte
	for _, block := range a.blocks {
		if len(block) == a.opt.BlockSize {
			keep = block
			break
		}
	}
	if keep != nil {
		a.blocks = a.blocks[:1]
		a.blocks[0] = keep
		a.cur = keep
		a.reservedBytes = int64(len(keep))
	} else {
		a.blocks = nil
		a.cur = nil
		a.reservedBytes = 0
	}
	a.off = 0
	a.usedBytes = 0
	a.hardDenied = 0
}
