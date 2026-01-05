package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/golang/snappy"

	"lsmengine/pkg/lsm/types"
)

type blockBuilder struct {
	buf             []byte
	restartOffsets  []uint32
	restartInterval int
	entryCount      int
	first           []byte
	lastKey         []byte
}

const (
	entryHeaderSize        = 4 + 4 + 4 + 8 + 1
	blockOffsetSize        = 4
	blockCountSize         = 4
	defaultRestartInterval = 16
)

func newBlockBuilder(restartInterval int) *blockBuilder {
	if restartInterval <= 0 {
		restartInterval = defaultRestartInterval
	}
	return &blockBuilder{restartInterval: restartInterval}
}

func (b *blockBuilder) add(entry types.Entry) {
	shared, unshared, restart := b.nextEntrySizing(entry)
	if b.first == nil {
		b.first = append([]byte(nil), entry.Key...)
	}
	if restart {
		b.restartOffsets = append(b.restartOffsets, uint32(len(b.buf)))
	}
	b.buf = appendEntry(b.buf, shared, unshared, entry)
	b.lastKey = append(b.lastKey[:0], entry.Key...)
	b.entryCount++
}

func (b *blockBuilder) reset() {
	b.buf = b.buf[:0]
	b.restartOffsets = b.restartOffsets[:0]
	b.entryCount = 0
	b.first = nil
	b.lastKey = b.lastKey[:0]
}

func (b *blockBuilder) sizeBytes() int {
	if b.entryCount == 0 {
		return 0
	}
	return len(b.buf) + len(b.restartOffsets)*blockOffsetSize + blockCountSize
}

func (b *blockBuilder) estimatedSizeAfter(entry types.Entry) int {
	_, unshared, restart := b.nextEntrySizing(entry)
	entrySize := entryEncodedSize(unshared, len(entry.Value))
	restarts := len(b.restartOffsets)
	if restart {
		restarts++
	}
	return len(b.buf) + entrySize + restarts*blockOffsetSize + blockCountSize
}

func (b *blockBuilder) finish() []byte {
	out := make([]byte, 0, b.sizeBytes())
	out = append(out, b.buf...)
	for _, off := range b.restartOffsets {
		var tmp [blockOffsetSize]byte
		binary.LittleEndian.PutUint32(tmp[:], off)
		out = append(out, tmp[:]...)
	}
	var count [blockCountSize]byte
	binary.LittleEndian.PutUint32(count[:], uint32(len(b.restartOffsets)))
	out = append(out, count[:]...)
	return out
}

func (b *blockBuilder) nextEntrySizing(entry types.Entry) (int, int, bool) {
	if b.restartInterval <= 0 {
		b.restartInterval = defaultRestartInterval
	}
	restart := b.entryCount%b.restartInterval == 0
	shared := 0
	if !restart && len(b.lastKey) > 0 {
		shared = commonPrefixLen(b.lastKey, entry.Key)
	}
	if shared > len(entry.Key) {
		shared = len(entry.Key)
	}
	unshared := len(entry.Key) - shared
	return shared, unshared, restart
}

type block struct {
	data        []byte
	restarts    []uint32
	restartKeys [][]byte
}

type entryView struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	Seq       uint64
}

type blockCursor struct {
	block   *block
	offset  int
	limit   int
	lastKey []byte
	scratch []byte
}

func decodeBlock(payload []byte) (*block, error) {
	if len(payload) < blockCountSize {
		return nil, errors.New("sstable: block too small")
	}
	count := binary.LittleEndian.Uint32(payload[len(payload)-blockCountSize:])
	offsetsLen := int(count) * blockOffsetSize
	if len(payload) < blockCountSize+offsetsLen {
		return nil, errors.New("sstable: block restarts truncated")
	}
	offsetStart := len(payload) - blockCountSize - offsetsLen
	data := payload[:offsetStart]
	if count == 0 && len(data) > 0 {
		return nil, errors.New("sstable: block missing restart points")
	}
	restarts := make([]uint32, 0, count)
	for i := 0; i < int(count); i++ {
		pos := offsetStart + i*blockOffsetSize
		off := binary.LittleEndian.Uint32(payload[pos : pos+blockOffsetSize])
		if off >= uint32(len(data)) {
			return nil, errors.New("sstable: block restart offset out of range")
		}
		restarts = append(restarts, off)
	}
	restartKeys := make([][]byte, 0, count)
	for _, off := range restarts {
		pos := int(off)
		if len(data)-pos < entryHeaderSize {
			return nil, errors.New("sstable: restart entry truncated")
		}
		shared := binary.LittleEndian.Uint32(data[pos : pos+4])
		unshared := binary.LittleEndian.Uint32(data[pos+4 : pos+8])
		valLen := binary.LittleEndian.Uint32(data[pos+8 : pos+12])
		if shared != 0 {
			return nil, errors.New("sstable: restart entry has shared prefix")
		}
		keyStart := pos + entryHeaderSize
		keyEnd := keyStart + int(unshared)
		valEnd := keyEnd + int(valLen)
		if keyStart < 0 || valEnd > len(data) {
			return nil, errors.New("sstable: restart entry truncated")
		}
		restartKeys = append(restartKeys, data[keyStart:keyEnd])
	}
	return &block{
		data:        data,
		restarts:    restarts,
		restartKeys: restartKeys,
	}, nil
}

func (b *block) find(key []byte) (types.Entry, bool, error) {
	if len(b.restarts) == 0 {
		return types.Entry{}, false, nil
	}
	idx := sort.Search(len(b.restartKeys), func(i int) bool {
		return bytes.Compare(b.restartKeys[i], key) > 0
	}) - 1
	if idx < 0 {
		idx = 0
	}
	limit := len(b.data)
	if idx+1 < len(b.restarts) {
		limit = int(b.restarts[idx+1])
	}
	cursor := newBlockCursor(b, int(b.restarts[idx]), limit)
	for {
		entry, ok, err := cursor.next()
		if err != nil {
			return types.Entry{}, false, err
		}
		if !ok {
			return types.Entry{}, false, nil
		}
		cmp := bytes.Compare(entry.Key, key)
		if cmp == 0 {
			return entry.toEntry(), true, nil
		}
		if cmp > 0 {
			return types.Entry{}, false, nil
		}
	}
}

func (b *block) seek(key []byte) (*blockCursor, entryView, bool, error) {
	if len(b.restarts) == 0 {
		return nil, entryView{}, false, nil
	}
	idx := sort.Search(len(b.restartKeys), func(i int) bool {
		return bytes.Compare(b.restartKeys[i], key) > 0
	}) - 1
	if idx < 0 {
		idx = 0
	}
	cursor := newBlockCursor(b, int(b.restarts[idx]), len(b.data))
	for {
		entry, ok, err := cursor.next()
		if err != nil {
			return nil, entryView{}, false, err
		}
		if !ok {
			return nil, entryView{}, false, nil
		}
		if bytes.Compare(entry.Key, key) >= 0 {
			return cursor, entry, true, nil
		}
	}
}

func newBlockCursor(b *block, offset, limit int) *blockCursor {
	if limit <= 0 || limit > len(b.data) {
		limit = len(b.data)
	}
	if offset < 0 {
		offset = 0
	}
	if offset > limit {
		offset = limit
	}
	return &blockCursor{block: b, offset: offset, limit: limit}
}

func (c *blockCursor) next() (entryView, bool, error) {
	if c.offset >= c.limit {
		return entryView{}, false, nil
	}
	data := c.block.data
	if c.limit-c.offset < entryHeaderSize {
		return entryView{}, false, errors.New("sstable: entry truncated")
	}
	shared := binary.LittleEndian.Uint32(data[c.offset : c.offset+4])
	unshared := binary.LittleEndian.Uint32(data[c.offset+4 : c.offset+8])
	valLen := binary.LittleEndian.Uint32(data[c.offset+8 : c.offset+12])
	seq := binary.LittleEndian.Uint64(data[c.offset+12 : c.offset+20])
	flags := data[c.offset+20]

	keyStart := c.offset + entryHeaderSize
	keyEnd := keyStart + int(unshared)
	valEnd := keyEnd + int(valLen)
	if keyStart < 0 || valEnd > c.limit {
		return entryView{}, false, errors.New("sstable: entry truncated")
	}
	if int(shared) > len(c.lastKey) {
		return entryView{}, false, errors.New("sstable: entry shared prefix invalid")
	}
	need := int(shared) + int(unshared)
	if cap(c.scratch) < need {
		c.scratch = make([]byte, need)
	} else {
		c.scratch = c.scratch[:need]
	}
	copy(c.scratch, c.lastKey[:shared])
	copy(c.scratch[shared:], data[keyStart:keyEnd])
	key := c.scratch
	value := data[keyEnd:valEnd]

	c.offset = valEnd
	c.lastKey = append(c.lastKey[:0], key...)

	return entryView{
		Key:       key,
		Value:     value,
		Tombstone: flags&0x1 == 0x1,
		Seq:       seq,
	}, true, nil
}

func appendEntry(dst []byte, shared, unshared int, entry types.Entry) []byte {
	var hdr [entryHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(shared))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(unshared))
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(entry.Value)))
	binary.LittleEndian.PutUint64(hdr[12:20], entry.Seq)
	flags := byte(0)
	if entry.Tombstone {
		flags |= 0x1
	}
	hdr[20] = flags
	dst = append(dst, hdr[:]...)
	dst = append(dst, entry.Key[shared:]...)
	dst = append(dst, entry.Value...)
	return dst
}

func (e entryView) toEntry() types.Entry {
	return types.Entry{
		Key:       append([]byte(nil), e.Key...),
		Value:     append([]byte(nil), e.Value...),
		Tombstone: e.Tombstone,
		Seq:       e.Seq,
	}
}

func entryEncodedSize(unshared, valLen int) int {
	return entryHeaderSize + unshared + valLen
}

func commonPrefixLen(a, b []byte) int {
	max := len(a)
	if len(b) < max {
		max = len(b)
	}
	for i := 0; i < max; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return max
}

func compressBlock(payload []byte, compression Compression) ([]byte, uint32, error) {
	if compression == CompressionNone {
		return payload, uint32(len(payload)), nil
	}
	if compression == CompressionSnappy {
		out := snappy.Encode(nil, payload)
		return out, uint32(len(payload)), nil
	}
	return payload, uint32(len(payload)), nil
}

func decompressBlock(payload []byte, compression Compression, uncompressedLen uint32) ([]byte, error) {
	if compression == CompressionNone {
		return payload, nil
	}
	if compression == CompressionSnappy {
		out, err := snappy.Decode(nil, payload)
		if err != nil {
			return nil, err
		}
		if uncompressedLen > 0 && uint32(len(out)) != uncompressedLen {
			return nil, errors.New("sstable: decompressed length mismatch")
		}
		return out, nil
	}
	return payload, nil
}
