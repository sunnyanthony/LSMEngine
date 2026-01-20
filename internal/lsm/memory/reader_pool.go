// Reusable bufio.Reader pool for replay.

package memory

import (
	"bufio"
	"io"
	"sync"
)

// ReaderPool reuses bufio.Reader instances to reduce allocations in hot paths.
type ReaderPool struct {
	size int
	pool sync.Pool
}

// NewReaderPool creates a pool of bufio.Reader with a fixed buffer size.
// Returns nil if size <= 0.
func NewReaderPool(size int) *ReaderPool {
	if size <= 0 {
		return nil
	}
	return &ReaderPool{size: size}
}

// Get returns a bufio.Reader bound to r.
func (p *ReaderPool) Get(r io.Reader) *bufio.Reader {
	if p == nil {
		return bufio.NewReader(r)
	}
	if v := p.pool.Get(); v != nil {
		br := v.(*bufio.Reader)
		br.Reset(r)
		recordReaderGet(true)
		return br
	}
	recordReaderGet(false)
	return bufio.NewReaderSize(r, p.size)
}

// Put resets and returns the reader to the pool.
func (p *ReaderPool) Put(br *bufio.Reader) {
	if p == nil || br == nil {
		return
	}
	br.Reset(nil)
	recordReaderPut()
	p.pool.Put(br)
}
