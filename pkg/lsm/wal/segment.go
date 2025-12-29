package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"lsmengine/pkg/lsm/errs"
	"time"
)

type segmentHeader struct {
	Version   byte
	BlockSize uint32
	SegmentID uint64
	CreatedAt uint64
}

func writeSegmentHeader(w io.Writer, blockSize uint32, segmentID uint64) (int, error) {
	hdr := make([]byte, segmentHeaderSize)
	copy(hdr[segmentMagicOffset:segmentMagicOffset+segmentMagicSize], segmentMagic)
	hdr[segmentVersionOffset] = recordVersion
	binary.LittleEndian.PutUint32(hdr[segmentBlockSizeOffset:segmentBlockSizeOffset+segmentBlockSizeSize], blockSize)
	binary.LittleEndian.PutUint64(hdr[segmentIDOffset:segmentIDOffset+segmentIDSize], segmentID)
	binary.LittleEndian.PutUint64(hdr[segmentCreatedAtOffset:segmentCreatedAtOffset+segmentCreatedAtSize], uint64(time.Now().UnixNano()))
	crc := crc32.ChecksumIEEE(hdr[segmentVersionOffset:segmentCRCOffset])
	binary.LittleEndian.PutUint32(hdr[segmentCRCOffset:segmentCRCOffset+segmentCRCSize], crc)
	return w.Write(hdr)
}

func readSegmentHeader(r io.Reader) (segmentHeader, error) {
	buf := make([]byte, segmentHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return segmentHeader{}, err
	}
	if !bytes.Equal(buf[segmentMagicOffset:segmentMagicOffset+segmentMagicSize], segmentMagic) {
		return segmentHeader{}, errs.ErrWALCorrupt
	}
	ver := buf[segmentVersionOffset]
	if ver != recordVersion {
		return segmentHeader{}, errs.ErrWALCorrupt
	}
	blockSize := binary.LittleEndian.Uint32(buf[segmentBlockSizeOffset : segmentBlockSizeOffset+segmentBlockSizeSize])
	segmentID := binary.LittleEndian.Uint64(buf[segmentIDOffset : segmentIDOffset+segmentIDSize])
	createdAt := binary.LittleEndian.Uint64(buf[segmentCreatedAtOffset : segmentCreatedAtOffset+segmentCreatedAtSize])
	gotCRC := binary.LittleEndian.Uint32(buf[segmentCRCOffset : segmentCRCOffset+segmentCRCSize])
	if crc32.ChecksumIEEE(buf[segmentVersionOffset:segmentCRCOffset]) != gotCRC {
		return segmentHeader{}, errs.ErrWALCorrupt
	}
	return segmentHeader{
		Version:   ver,
		BlockSize: blockSize,
		SegmentID: segmentID,
		CreatedAt: createdAt,
	}, nil
}
