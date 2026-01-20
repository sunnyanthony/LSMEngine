// Index entry encoding and decoding.

package format

import (
	"encoding/binary"

	"lsmengine/pkg/lsm/errs"
)

type IndexEntry struct {
	Key    []byte
	Offset uint64
	Length uint32
}

const IndexHeaderSize = 4 + 8 + 4

func EncodeIndex(entries []IndexEntry) []byte {
	var out []byte
	for _, e := range entries {
		var hdr [IndexHeaderSize]byte
		binary.LittleEndian.PutUint32(hdr[:4], uint32(len(e.Key)))
		binary.LittleEndian.PutUint64(hdr[4:12], e.Offset)
		binary.LittleEndian.PutUint32(hdr[12:16], e.Length)
		out = append(out, hdr[:]...)
		out = append(out, e.Key...)
	}
	return out
}

func DecodeIndex(data []byte) ([]IndexEntry, error) {
	var entries []IndexEntry
	pos := 0
	for pos < len(data) {
		if len(data)-pos < IndexHeaderSize {
			return nil, errs.ErrSSTableBadIndex
		}
		keyLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		offset := binary.LittleEndian.Uint64(data[pos+4 : pos+12])
		length := binary.LittleEndian.Uint32(data[pos+12 : pos+16])
		pos += IndexHeaderSize
		if len(data)-pos < int(keyLen) {
			return nil, errs.ErrSSTableBadIndex
		}
		key := append([]byte(nil), data[pos:pos+int(keyLen)]...)
		pos += int(keyLen)
		entries = append(entries, IndexEntry{
			Key:    key,
			Offset: offset,
			Length: length,
		})
	}
	return entries, nil
}
