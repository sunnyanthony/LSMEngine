// Reusable buffer pool for IO reads.

package memory

import "sync"

// BufferPool keeps small reusable buffers for IO reads.
type BufferPool struct {
	max  int
	pool sync.Pool
}

// NewBufferPool creates a pool with a max buffer size. Returns nil when max <= 0.
func NewBufferPool(max int) *BufferPool {
	if max <= 0 {
		return nil
	}
	return &BufferPool{max: max}
}

// Get returns a buffer of length n.
func (p *BufferPool) Get(n int) []byte {
	if p == nil || n <= 0 {
		return make([]byte, n)
	}
	if v := p.pool.Get(); v != nil {
		buf := *v.(*[]byte)
		if cap(buf) >= n {
			recordBufferGet(true)
			return buf[:n]
		}
	}
	recordBufferGet(false)
	return make([]byte, n)
}

// Put returns a buffer to the pool when it is within the size limit.
func (p *BufferPool) Put(buf []byte) {
	if p == nil || buf == nil {
		return
	}
	if cap(buf) > p.max {
		return
	}
	recordBufferPut()
	pooled := buf[:cap(buf)]
	p.pool.Put(&pooled)
}

// Max returns the maximum buffer size allowed by the pool.
func (p *BufferPool) Max() int {
	if p == nil {
		return 0
	}
	return p.max
}
