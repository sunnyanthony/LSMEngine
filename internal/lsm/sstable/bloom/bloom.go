package bloom

import (
	"encoding/binary"
	"hash/fnv"
	"io"

	"lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/sstable/format"
	"lsmengine/internal/lsm/sstable/index"
	"lsmengine/pkg/lsm/types"
)

type Filter struct {
	bits []byte
	k    uint8
}

func NewFilter(keys int, bitsPerKey int) *Filter {
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
	return &Filter{
		bits: make([]byte, byteLen),
		k:    uint8(k),
	}
}

func (b *Filter) Add(key []byte) {
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

func (b *Filter) MayContain(key []byte) bool {
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

func (b *Filter) Encode() []byte {
	if b == nil {
		return nil
	}
	buf := make([]byte, 1+len(b.bits))
	buf[0] = b.k
	copy(buf[1:], b.bits)
	return buf
}

func Decode(data []byte) *Filter {
	if len(data) < 2 {
		return nil
	}
	k := data[0]
	bits := make([]byte, len(data)-1)
	copy(bits, data[1:])
	return &Filter{
		bits: bits,
		k:    k,
	}
}

func (b *Filter) K() uint8 {
	if b == nil {
		return 0
	}
	return b.k
}

func (b *Filter) SizeBytes() int {
	if b == nil {
		return 0
	}
	return len(b.bits)
}

func WriteBloomBlock(w io.Writer, filter *Filter, offset *uint64) (uint64, uint32, error) {
	if filter == nil {
		return 0, 0, nil
	}
	blockOffset := *offset
	payload := filter.Encode()
	block := format.EncodeBlock(payload, format.BlockTypeFilter, config.CompressionNone, uint32(len(payload)))
	n, err := w.Write(block)
	if err != nil {
		return 0, 0, err
	}
	*offset += uint64(n)
	return blockOffset, uint32(n), nil
}

func WritePartitionedBloomFilters(w io.Writer, entries []types.Entry, blocks []index.Entry, blockEntryCounts []int, opts config.Options, offset *uint64) ([]index.Entry, error) {
	if len(blocks) == 0 || len(blockEntryCounts) == 0 || opts.IndexPartitionEntries <= 0 {
		return nil, nil
	}
	filterIndexEntries := make([]index.Entry, 0, (len(blocks)+opts.IndexPartitionEntries-1)/opts.IndexPartitionEntries)
	entryIdx := 0
	for i := 0; i < len(blockEntryCounts); {
		end := i + opts.IndexPartitionEntries
		if end > len(blockEntryCounts) {
			end = len(blockEntryCounts)
		}
		partEntries := 0
		for j := i; j < end; j++ {
			partEntries += blockEntryCounts[j]
		}
		partFilter := NewFilter(partEntries, opts.BloomBitsPerKey)
		for j := i; j < end; j++ {
			for k := 0; k < blockEntryCounts[j]; k++ {
				partFilter.Add(entries[entryIdx].Key)
				entryIdx++
			}
		}
		payload := partFilter.Encode()
		block := format.EncodeBlock(payload, format.BlockTypeFilter, config.CompressionNone, uint32(len(payload)))
		n, err := w.Write(block)
		if err != nil {
			return nil, err
		}
		filterIndexEntries = append(filterIndexEntries, index.Entry{
			Key:    append([]byte(nil), blocks[i].Key...),
			Offset: *offset,
			Length: uint32(n),
		})
		*offset += uint64(n)
		i = end
	}
	return filterIndexEntries, nil
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
