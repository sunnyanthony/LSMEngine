package storage

import "sync"

// bufferPool keeps small reusable buffers for Read().
type bufferPool struct {
	max  int
	pool sync.Pool
}

func newBufferPool(max int) *bufferPool {
	if max <= 0 {
		return nil
	}
	return &bufferPool{max: max}
}

func (p *bufferPool) get(n int) []byte {
	if p == nil || n <= 0 {
		return make([]byte, n)
	}
	if v := p.pool.Get(); v != nil {
		buf := v.([]byte)
		if cap(buf) >= n {
			return buf[:n]
		}
	}
	return make([]byte, n)
}

func (p *bufferPool) put(buf []byte) {
	if p == nil || buf == nil {
		return
	}
	if cap(buf) > p.max {
		return
	}
	p.pool.Put(buf[:cap(buf)])
}
