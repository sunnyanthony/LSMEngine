package sstable

import (
	"encoding/binary"
	"hash/fnv"
)

type bloomFilter struct {
	bits []byte
	k    uint8
}

func newBloomFilter(keys int, bitsPerKey int) *bloomFilter {
	if keys <= 0 || bitsPerKey <= 0 {
		return nil
	}
	m := keys * bitsPerKey
	if m < 64 {
		m = 64
	}
	byteLen := (m + 7) / 8
	k := int(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return &bloomFilter{
		bits: make([]byte, byteLen),
		k:    uint8(k),
	}
}

func (b *bloomFilter) add(key []byte) {
	if b == nil {
		return
	}
	h1, h2 := hashKey(key)
	n := uint32(len(b.bits) * 8)
	for i := uint8(0); i < b.k; i++ {
		h := h1 + uint64(i)*h2
		bit := uint32(h % uint64(n))
		b.bits[bit/8] |= 1 << (bit % 8)
	}
}

func (b *bloomFilter) mayContain(key []byte) bool {
	if b == nil {
		return true
	}
	h1, h2 := hashKey(key)
	n := uint32(len(b.bits) * 8)
	for i := uint8(0); i < b.k; i++ {
		h := h1 + uint64(i)*h2
		bit := uint32(h % uint64(n))
		if b.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func (b *bloomFilter) encode() []byte {
	if b == nil {
		return nil
	}
	buf := make([]byte, 1+len(b.bits))
	buf[0] = b.k
	copy(buf[1:], b.bits)
	return buf
}

func decodeBloomFilter(data []byte) *bloomFilter {
	if len(data) < 2 {
		return nil
	}
	k := data[0]
	bits := make([]byte, len(data)-1)
	copy(bits, data[1:])
	return &bloomFilter{
		bits: bits,
		k:    k,
	}
}

func hashKey(key []byte) (uint64, uint64) {
	h := fnv.New64a()
	_, _ = h.Write(key)
	sum := h.Sum64()
	h2 := sum ^ (sum >> 33) ^ (sum << 11)
	if h2 == 0 {
		h2 = binary.LittleEndian.Uint64([]byte{0x9e, 0x37, 0x79, 0xb9, 0x7f, 0x4a, 0x7c, 0x15})
	}
	return sum, h2
}
