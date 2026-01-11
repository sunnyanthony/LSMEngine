package index

import (
	"encoding/binary"

	"lsmengine/pkg/lsm/errs"
)

type Entry struct {
	Key    []byte
	Offset uint64
	Length uint32
}

const HeaderSize = 4 + 8 + 4

func Encode(entries []Entry) []byte {
	var out []byte
	for _, e := range entries {
		var hdr [HeaderSize]byte
		binary.LittleEndian.PutUint32(hdr[:4], uint32(len(e.Key)))
		binary.LittleEndian.PutUint64(hdr[4:12], e.Offset)
		binary.LittleEndian.PutUint32(hdr[12:16], e.Length)
		out = append(out, hdr[:]...)
		out = append(out, e.Key...)
	}
	return out
}

func Decode(data []byte) ([]Entry, error) {
	var entries []Entry
	pos := 0
	for pos < len(data) {
		if len(data)-pos < HeaderSize {
			return nil, errs.ErrSSTableBadIndex
		}
		keyLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		offset := binary.LittleEndian.Uint64(data[pos+4 : pos+12])
		length := binary.LittleEndian.Uint32(data[pos+12 : pos+16])
		pos += HeaderSize
		if len(data)-pos < int(keyLen) {
			return nil, errs.ErrSSTableBadIndex
		}
		key := append([]byte(nil), data[pos:pos+int(keyLen)]...)
		pos += int(keyLen)
		entries = append(entries, Entry{
			Key:    key,
			Offset: offset,
			Length: length,
		})
	}
	return entries, nil
}
