package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/golang/snappy"

	"lsmengine/pkg/lsm/types"
)

type blockBuilder struct {
	buf     []byte
	offsets []uint32
	first   []byte
	last    []byte
}

const (
	entryHeaderSize = 4 + 4 + 8 + 1
	blockOffsetSize = 4
	blockCountSize  = 4
)

func (b *blockBuilder) add(entry types.Entry) {
	if b.first == nil {
		b.first = append([]byte(nil), entry.Key...)
	}
	b.last = append(b.last[:0], entry.Key...)
	offset := uint32(len(b.buf))
	b.offsets = append(b.offsets, offset)
	b.buf = appendEntry(b.buf, entry)
}

func (b *blockBuilder) reset() {
	b.buf = b.buf[:0]
	b.offsets = b.offsets[:0]
	b.first = nil
	b.last = b.last[:0]
}

func (b *blockBuilder) sizeBytes() int {
	if len(b.offsets) == 0 {
		return 0
	}
	return len(b.buf) + len(b.offsets)*blockOffsetSize + blockCountSize
}

func (b *blockBuilder) estimatedSizeAfter(entry types.Entry) int {
	return len(b.buf) + entryEncodedSize(entry) + (len(b.offsets)+1)*blockOffsetSize + blockCountSize
}

func (b *blockBuilder) finish() []byte {
	out := make([]byte, 0, b.sizeBytes())
	out = append(out, b.buf...)
	for _, off := range b.offsets {
		var tmp [blockOffsetSize]byte
		binary.LittleEndian.PutUint32(tmp[:], off)
		out = append(out, tmp[:]...)
	}
	var count [blockCountSize]byte
	binary.LittleEndian.PutUint32(count[:], uint32(len(b.offsets)))
	out = append(out, count[:]...)
	return out
}

type block struct {
	data    []byte
	offsets []uint32
}

type entryView struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	Seq       uint64
}

func decodeBlock(payload []byte) (*block, error) {
	if len(payload) < blockCountSize {
		return nil, errors.New("sstable: block too small")
	}
	count := binary.LittleEndian.Uint32(payload[len(payload)-blockCountSize:])
	offsetsLen := int(count) * blockOffsetSize
	if len(payload) < blockCountSize+offsetsLen {
		return nil, errors.New("sstable: block offsets truncated")
	}
	offsetStart := len(payload) - blockCountSize - offsetsLen
	offsets := make([]uint32, 0, count)
	for i := 0; i < int(count); i++ {
		pos := offsetStart + i*blockOffsetSize
		offsets = append(offsets, binary.LittleEndian.Uint32(payload[pos:pos+blockOffsetSize]))
	}
	data := payload[:offsetStart]
	return &block{
		data:    data,
		offsets: offsets,
	}, nil
}

func (b *block) entryAt(idx int) (types.Entry, error) {
	view, err := b.entryAtView(idx)
	if err != nil {
		return types.Entry{}, err
	}
	return view.toEntry(), nil
}

func (b *block) entryAtView(idx int) (entryView, error) {
	if idx < 0 || idx >= len(b.offsets) {
		return entryView{}, errors.New("sstable: entry index out of range")
	}
	off := int(b.offsets[idx])
	if off >= len(b.data) {
		return entryView{}, errors.New("sstable: entry offset out of range")
	}
	return decodeEntryView(b.data[off:])
}

func (b *block) find(key []byte) (types.Entry, bool, error) {
	lo, hi := 0, len(b.offsets)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		entry, err := b.entryAtView(mid)
		if err != nil {
			return types.Entry{}, false, err
		}
		cmp := bytes.Compare(entry.Key, key)
		if cmp == 0 {
			return entry.toEntry(), true, nil
		}
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return types.Entry{}, false, nil
}

func appendEntry(dst []byte, entry types.Entry) []byte {
	var hdr [entryHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(entry.Key)))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(entry.Value)))
	binary.LittleEndian.PutUint64(hdr[8:16], entry.Seq)
	flags := byte(0)
	if entry.Tombstone {
		flags |= 0x1
	}
	hdr[16] = flags
	dst = append(dst, hdr[:]...)
	dst = append(dst, entry.Key...)
	dst = append(dst, entry.Value...)
	return dst
}

func decodeEntry(data []byte) (types.Entry, error) {
	view, err := decodeEntryView(data)
	if err != nil {
		return types.Entry{}, err
	}
	return view.toEntry(), nil
}

func decodeEntryView(data []byte) (entryView, error) {
	if len(data) < entryHeaderSize {
		return entryView{}, errors.New("sstable: entry truncated")
	}
	keyLen := binary.LittleEndian.Uint32(data[:4])
	valLen := binary.LittleEndian.Uint32(data[4:8])
	seq := binary.LittleEndian.Uint64(data[8:16])
	flags := data[16]
	need := int(entryHeaderSize + keyLen + valLen)
	if len(data) < need {
		return entryView{}, errors.New("sstable: entry truncated")
	}
	keyStart := entryHeaderSize
	keyEnd := entryHeaderSize + int(keyLen)
	valEnd := keyEnd + int(valLen)
	return entryView{
		Key:       data[keyStart:keyEnd],
		Value:     data[keyEnd:valEnd],
		Tombstone: flags&0x1 == 0x1,
		Seq:       seq,
	}, nil
}

func (e entryView) toEntry() types.Entry {
	return types.Entry{
		Key:       append([]byte(nil), e.Key...),
		Value:     append([]byte(nil), e.Value...),
		Tombstone: e.Tombstone,
		Seq:       e.Seq,
	}
}

func entryEncodedSize(entry types.Entry) int {
	return entryHeaderSize + len(entry.Key) + len(entry.Value)
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
