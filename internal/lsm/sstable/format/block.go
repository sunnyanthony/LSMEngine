// Data block builder/decoder and cursor.

package format

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"lsmengine/internal/lsm/memory"
	"lsmengine/pkg/lsm/types"
)

type Builder struct {
	buf             []byte
	restartOffsets  []uint32
	restartInterval int
	baseRestart     int
	minRestart      int
	maxRestart      int
	adaptive        bool
	entryCount      int
	first           []byte
	lastKey         []byte
	sharedSum       int
	keySum          int
}

const (
	entryHeaderSize        = 4 + 4 + 4 + 8 + 1
	blockOffsetSize        = 4
	blockCountSize         = 4
	defaultRestartInterval = 16
)

func NewBuilder(restartInterval int, adaptive bool, minRestart, maxRestart int) *Builder {
	if restartInterval <= 0 {
		restartInterval = defaultRestartInterval
	}
	if minRestart <= 0 {
		minRestart = restartInterval
	}
	if maxRestart <= 0 {
		maxRestart = restartInterval
	}
	if minRestart > maxRestart {
		minRestart = maxRestart
	}
	if restartInterval < minRestart {
		restartInterval = minRestart
	}
	if restartInterval > maxRestart {
		restartInterval = maxRestart
	}
	return &Builder{
		restartInterval: restartInterval,
		baseRestart:     restartInterval,
		minRestart:      minRestart,
		maxRestart:      maxRestart,
		adaptive:        adaptive,
	}
}

func (b *Builder) Add(entry types.Entry) {
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
	b.sharedSum += shared
	b.keySum += shared + unshared
	if b.adaptive && b.entryCount%b.restartInterval == 0 {
		b.adaptRestartInterval()
	}
}

func (b *Builder) Reset() {
	b.buf = b.buf[:0]
	b.restartOffsets = b.restartOffsets[:0]
	b.entryCount = 0
	b.first = nil
	b.lastKey = b.lastKey[:0]
	b.sharedSum = 0
	b.keySum = 0
	b.restartInterval = b.baseRestart
}

func (b *Builder) SizeBytes() int {
	if b.entryCount == 0 {
		return 0
	}
	return len(b.buf) + len(b.restartOffsets)*blockOffsetSize + blockCountSize
}

func (b *Builder) EstimatedSizeAfter(entry types.Entry) int {
	_, unshared, restart := b.nextEntrySizing(entry)
	entrySize := entryEncodedSize(unshared, len(entry.Value))
	restarts := len(b.restartOffsets)
	if restart {
		restarts++
	}
	return len(b.buf) + entrySize + restarts*blockOffsetSize + blockCountSize
}

func (b *Builder) Finish() []byte {
	out := make([]byte, 0, b.SizeBytes())
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

func (b *Builder) nextEntrySizing(entry types.Entry) (int, int, bool) {
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

func (b *Builder) adaptRestartInterval() {
	if b.keySum <= 0 || b.maxRestart == b.minRestart {
		b.sharedSum = 0
		b.keySum = 0
		return
	}
	ratio := float64(b.sharedSum) / float64(b.keySum)
	target := b.minRestart + int(ratio*float64(b.maxRestart-b.minRestart))
	if target < b.minRestart {
		target = b.minRestart
	}
	if target > b.maxRestart {
		target = b.maxRestart
	}
	b.restartInterval = target
	b.sharedSum = 0
	b.keySum = 0
}

func (b *Builder) EntryCount() int {
	if b == nil {
		return 0
	}
	return b.entryCount
}

func (b *Builder) FirstKey() []byte {
	if b == nil {
		return nil
	}
	return b.first
}

type Block struct {
	data        []byte
	restarts    []uint32
	restartKeys [][]byte
}

type Cursor struct {
	block   *Block
	offset  int
	limit   int
	lastKey []byte
	scratch []byte
}

func Decode(payload []byte) (*Block, error) {
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
	return &Block{
		data:        data,
		restarts:    restarts,
		restartKeys: restartKeys,
	}, nil
}

func (b *Block) Find(key []byte) (types.Entry, bool, error) {
	view, ok, err := b.FindView(key)
	if !ok || err != nil {
		return types.Entry{}, ok, err
	}
	return view.ToEntry(), true, nil
}

func (b *Block) FindView(key []byte) (memory.EntryView, bool, error) {
	if len(b.restarts) == 0 {
		return memory.EntryView{}, false, nil
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
	cursor := NewCursor(b, int(b.restarts[idx]), limit)
	for {
		entry, ok, err := cursor.Next()
		if err != nil {
			return memory.EntryView{}, false, err
		}
		if !ok {
			return memory.EntryView{}, false, nil
		}
		cmp := bytes.Compare(entry.Key, key)
		if cmp == 0 {
			return entry, true, nil
		}
		if cmp > 0 {
			return memory.EntryView{}, false, nil
		}
	}
}

func (b *Block) Seek(key []byte) (*Cursor, memory.EntryView, bool, error) {
	if len(b.restarts) == 0 {
		return nil, memory.EntryView{}, false, nil
	}
	idx := sort.Search(len(b.restartKeys), func(i int) bool {
		return bytes.Compare(b.restartKeys[i], key) > 0
	}) - 1
	if idx < 0 {
		idx = 0
	}
	cursor := NewCursor(b, int(b.restarts[idx]), len(b.data))
	for {
		entry, ok, err := cursor.Next()
		if err != nil {
			return nil, memory.EntryView{}, false, err
		}
		if !ok {
			return nil, memory.EntryView{}, false, nil
		}
		if bytes.Compare(entry.Key, key) >= 0 {
			return cursor, entry, true, nil
		}
	}
}

func NewCursor(b *Block, offset, limit int) *Cursor {
	if limit <= 0 || limit > len(b.data) {
		limit = len(b.data)
	}
	if offset < 0 {
		offset = 0
	}
	if offset > limit {
		offset = limit
	}
	return &Cursor{block: b, offset: offset, limit: limit}
}

func (c *Cursor) Next() (memory.EntryView, bool, error) {
	if c.offset >= c.limit {
		return memory.EntryView{}, false, nil
	}
	data := c.block.data
	if c.limit-c.offset < entryHeaderSize {
		return memory.EntryView{}, false, errors.New("sstable: entry truncated")
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
		return memory.EntryView{}, false, errors.New("sstable: entry truncated")
	}
	if int(shared) > len(c.lastKey) {
		return memory.EntryView{}, false, errors.New("sstable: entry shared prefix invalid")
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

	return memory.EntryView{
		Key:       key,
		Value:     value,
		Tombstone: flags&0x1 == 0x1,
		Seq:       seq,
	}, true, nil
}

func (b *Block) DataLen() int {
	if b == nil {
		return 0
	}
	return len(b.data)
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
