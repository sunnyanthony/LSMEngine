package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"lsmengine/pkg/lsm/errs"
)

const (
	fileMagic       = "LSM1"
	fileVersion     = 1
	footerSizeBytes = 36

	blockHeaderSize = 1 + 4 // compression + uncompressed length
	blockCRCLen     = 4

	compressionIDNone   byte = 0
	compressionIDSnappy byte = 1
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func checksum(payload []byte) uint32 {
	return crc32.Checksum(payload, crcTable)
}

func compressionID(c Compression) byte {
	switch c {
	case CompressionSnappy:
		return compressionIDSnappy
	default:
		return compressionIDNone
	}
}

func compressionFromID(id byte) (Compression, error) {
	switch id {
	case compressionIDNone:
		return CompressionNone, nil
	case compressionIDSnappy:
		return CompressionSnappy, nil
	default:
		return "", fmt.Errorf("%w %d", errs.ErrSSTableUnknownCompression, id)
	}
}

type footer struct {
	IndexOffset uint64
	IndexLen    uint32
	MetaOffset  uint64
	MetaLen     uint32
}

func encodeFooter(f footer) []byte {
	buf := make([]byte, footerSizeBytes)
	copy(buf[:4], []byte(fileMagic))
	buf[4] = fileVersion
	buf[5] = 0 // flags
	binary.LittleEndian.PutUint16(buf[6:8], 0)
	binary.LittleEndian.PutUint64(buf[8:16], f.IndexOffset)
	binary.LittleEndian.PutUint32(buf[16:20], f.IndexLen)
	binary.LittleEndian.PutUint64(buf[20:28], f.MetaOffset)
	binary.LittleEndian.PutUint32(buf[28:32], f.MetaLen)
	crc := checksum(buf[:32])
	binary.LittleEndian.PutUint32(buf[32:36], crc)
	return buf
}

func decodeFooter(buf []byte) (footer, error) {
	if len(buf) != footerSizeBytes {
		return footer{}, errs.ErrSSTableBadFooter
	}
	if string(buf[:4]) != fileMagic {
		return footer{}, errs.ErrSSTableBadMagic
	}
	expected := binary.LittleEndian.Uint32(buf[32:36])
	if checksum(buf[:32]) != expected {
		return footer{}, errs.ErrSSTableBadFooter
	}
	return footer{
		IndexOffset: binary.LittleEndian.Uint64(buf[8:16]),
		IndexLen:    binary.LittleEndian.Uint32(buf[16:20]),
		MetaOffset:  binary.LittleEndian.Uint64(buf[20:28]),
		MetaLen:     binary.LittleEndian.Uint32(buf[28:32]),
	}, nil
}
