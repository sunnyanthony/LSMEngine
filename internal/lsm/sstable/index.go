package sstable

import (
	"encoding/binary"

	"lsmengine/pkg/lsm/errs"
)

type indexEntry struct {
	key    []byte
	offset uint64
	length uint32
}

const indexHeaderSize = 4 + 8 + 4

func encodeIndex(entries []indexEntry) []byte {
	var out []byte
	for _, e := range entries {
		var hdr [indexHeaderSize]byte
		binary.LittleEndian.PutUint32(hdr[:4], uint32(len(e.key)))
		binary.LittleEndian.PutUint64(hdr[4:12], e.offset)
		binary.LittleEndian.PutUint32(hdr[12:16], e.length)
		out = append(out, hdr[:]...)
		out = append(out, e.key...)
	}
	return out
}

func decodeIndex(data []byte) ([]indexEntry, error) {
	var entries []indexEntry
	pos := 0
	for pos < len(data) {
		if len(data)-pos < indexHeaderSize {
			return nil, errs.ErrSSTableBadIndex
		}
		keyLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		offset := binary.LittleEndian.Uint64(data[pos+4 : pos+12])
		length := binary.LittleEndian.Uint32(data[pos+12 : pos+16])
		pos += indexHeaderSize
		if len(data)-pos < int(keyLen) {
			return nil, errs.ErrSSTableBadIndex
		}
		key := append([]byte(nil), data[pos:pos+int(keyLen)]...)
		pos += int(keyLen)
		entries = append(entries, indexEntry{
			key:    key,
			offset: offset,
			length: length,
		})
	}
	return entries, nil
}
