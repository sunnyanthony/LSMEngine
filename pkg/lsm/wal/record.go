package wal

import (
	"encoding/binary"
	"hash/crc32"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
	"net"
)

type recordBuffer struct {
	header [recordHeaderSize]byte
	key    []byte
	val    []byte
	crc    [recordCRCSize]byte
	total  int
}

func newRecordBuffer(e types.Entry) recordBuffer {
	var rb recordBuffer
	flags := byte(0)
	if e.Tombstone {
		flags |= 0x1
	}
	rb.header[recordFlagsOffset] = flags
	binary.LittleEndian.PutUint64(rb.header[recordSeqOffset:recordSeqOffset+recordSeqSize], e.Seq)
	binary.LittleEndian.PutUint32(rb.header[recordKeyLenOffset:recordKeyLenOffset+recordKeyLenSize], uint32(len(e.Key)))
	binary.LittleEndian.PutUint32(rb.header[recordValLenOffset:recordValLenOffset+recordValLenSize], uint32(len(e.Value)))
	rb.key = []byte(e.Key)
	rb.val = e.Value

	crc := crc32.NewIEEE()
	crc.Write(rb.header[:])
	crc.Write(rb.key)
	crc.Write(rb.val)
	binary.LittleEndian.PutUint32(rb.crc[:], crc.Sum32())

	rb.total = len(rb.header) + len(rb.key) + len(rb.val) + len(rb.crc)
	return rb
}

func (rb *recordBuffer) buffers() net.Buffers {
	return net.Buffers{rb.header[:], rb.key, rb.val, rb.crc[:]}
}

func encodeEntry(e types.Entry) []byte {
	rb := newRecordBuffer(e)
	out := make([]byte, rb.total)
	offset := copy(out, rb.header[:])
	offset += copy(out[offset:], rb.key)
	offset += copy(out[offset:], rb.val)
	copy(out[offset:], rb.crc[:])
	return out
}

func decodeRecords(payload []byte) ([]types.Entry, error) {
	var entries []types.Entry
	offset := 0
	for offset < len(payload) {
		if len(payload)-offset < recordHeaderSize+recordCRCSize {
			return entries, errs.ErrWALCorrupt
		}
		header := payload[offset : offset+recordHeaderSize]
		flags := header[recordFlagsOffset]
		seq := binary.LittleEndian.Uint64(header[recordSeqOffset : recordSeqOffset+recordSeqSize])
		keyLen := binary.LittleEndian.Uint32(header[recordKeyLenOffset : recordKeyLenOffset+recordKeyLenSize])
		valLen := binary.LittleEndian.Uint32(header[recordValLenOffset : recordValLenOffset+recordValLenSize])
		offset += recordHeaderSize

		if len(payload)-offset < int(keyLen+valLen)+recordCRCSize {
			return entries, errs.ErrWALCorrupt
		}
		key := payload[offset : offset+int(keyLen)]
		offset += int(keyLen)
		val := payload[offset : offset+int(valLen)]
		offset += int(valLen)
		crcBytes := payload[offset : offset+recordCRCSize]
		offset += recordCRCSize

		crc := crc32.NewIEEE()
		crc.Write(header)
		crc.Write(key)
		crc.Write(val)
		if binary.LittleEndian.Uint32(crcBytes) != crc.Sum32() {
			return entries, errs.ErrWALCorrupt
		}
		entries = append(entries, types.Entry{
			Key:       string(key),
			Value:     append([]byte(nil), val...),
			Tombstone: flags&0x1 == 0x1,
			Seq:       seq,
		})
	}
	return entries, nil
}
