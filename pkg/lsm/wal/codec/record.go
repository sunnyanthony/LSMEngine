package codec

import (
	"encoding/binary"
	"hash/crc32"
	"net"

	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

type RecordBuffer struct {
	header [RecordHeaderSize]byte
	key    []byte
	val    []byte
	crc    [RecordCRCSize]byte
	Total  int
}

func NewRecordBuffer(e types.Entry) RecordBuffer {
	var rb RecordBuffer
	flags := byte(0)
	if e.Tombstone {
		flags |= 0x1
	}
	rb.header[recordFlagsOffset] = flags
	binary.LittleEndian.PutUint64(rb.header[recordSeqOffset:recordSeqOffset+recordSeqSize], e.Seq)
	binary.LittleEndian.PutUint32(rb.header[recordKeyLenOffset:recordKeyLenOffset+recordKeyLenSize], uint32(len(e.Key)))
	binary.LittleEndian.PutUint32(rb.header[recordValLenOffset:recordValLenOffset+recordValLenSize], uint32(len(e.Value)))
	rb.key = append([]byte(nil), e.Key...)
	rb.val = append([]byte(nil), e.Value...)

	crc := crc32.NewIEEE()
	crc.Write(rb.header[:])
	crc.Write(rb.key)
	crc.Write(rb.val)
	binary.LittleEndian.PutUint32(rb.crc[:], crc.Sum32())

	rb.Total = len(rb.header) + len(rb.key) + len(rb.val) + len(rb.crc)
	return rb
}

func NewRecordBufferOwned(e types.Entry) RecordBuffer {
	var rb RecordBuffer
	flags := byte(0)
	if e.Tombstone {
		flags |= 0x1
	}
	rb.header[recordFlagsOffset] = flags
	binary.LittleEndian.PutUint64(rb.header[recordSeqOffset:recordSeqOffset+recordSeqSize], e.Seq)
	binary.LittleEndian.PutUint32(rb.header[recordKeyLenOffset:recordKeyLenOffset+recordKeyLenSize], uint32(len(e.Key)))
	binary.LittleEndian.PutUint32(rb.header[recordValLenOffset:recordValLenOffset+recordValLenSize], uint32(len(e.Value)))
	rb.key = e.Key
	rb.val = e.Value

	crc := crc32.NewIEEE()
	crc.Write(rb.header[:])
	crc.Write(rb.key)
	crc.Write(rb.val)
	binary.LittleEndian.PutUint32(rb.crc[:], crc.Sum32())

	rb.Total = len(rb.header) + len(rb.key) + len(rb.val) + len(rb.crc)
	return rb
}

func (rb *RecordBuffer) buffers() net.Buffers {
	return net.Buffers{rb.header[:], rb.key, rb.val, rb.crc[:]}
}

func EncodeEntry(e types.Entry) []byte {
	rb := NewRecordBuffer(e)
	out := make([]byte, rb.Total)
	offset := copy(out, rb.header[:])
	offset += copy(out[offset:], rb.key)
	offset += copy(out[offset:], rb.val)
	copy(out[offset:], rb.crc[:])
	return out
}

func DecodeRecords(payload []byte) ([]types.Entry, error) {
	var entries []types.Entry
	offset := 0
	for offset < len(payload) {
		if len(payload)-offset < RecordHeaderSize+RecordCRCSize {
			return entries, errs.ErrWALCorrupt
		}
		header := payload[offset : offset+RecordHeaderSize]
		flags := header[recordFlagsOffset]
		seq := binary.LittleEndian.Uint64(header[recordSeqOffset : recordSeqOffset+recordSeqSize])
		keyLen := binary.LittleEndian.Uint32(header[recordKeyLenOffset : recordKeyLenOffset+recordKeyLenSize])
		valLen := binary.LittleEndian.Uint32(header[recordValLenOffset : recordValLenOffset+recordValLenSize])
		offset += RecordHeaderSize

		if len(payload)-offset < int(keyLen+valLen)+RecordCRCSize {
			return entries, errs.ErrWALCorrupt
		}
		key := payload[offset : offset+int(keyLen)]
		offset += int(keyLen)
		val := payload[offset : offset+int(valLen)]
		offset += int(valLen)
		crcBytes := payload[offset : offset+RecordCRCSize]
		offset += RecordCRCSize

		crc := crc32.NewIEEE()
		crc.Write(header)
		crc.Write(key)
		crc.Write(val)
		if binary.LittleEndian.Uint32(crcBytes) != crc.Sum32() {
			return entries, errs.ErrWALCorrupt
		}
		entries = append(entries, types.Entry{
			Key:       append([]byte(nil), key...),
			Value:     append([]byte(nil), val...),
			Tombstone: flags&0x1 == 0x1,
			Seq:       seq,
		})
	}
	return entries, nil
}
