package codec

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"time"

	"lsmengine/pkg/lsm/errs"
)

type SegmentHeader struct {
	Version   byte
	BlockSize uint32
	SegmentID uint64
	CreatedAt uint64
}

func WriteSegmentHeader(w io.Writer, blockSize uint32, segmentID uint64) (int, error) {
	hdr := make([]byte, SegmentHeaderSize)
	copy(hdr[segmentMagicOffset:segmentMagicOffset+SegmentMagicSize], segmentMagic)
	hdr[segmentVersionOffset] = recordVersion
	binary.LittleEndian.PutUint32(hdr[segmentBlockSizeOffset:segmentBlockSizeOffset+segmentBlockSizeSize], blockSize)
	binary.LittleEndian.PutUint64(hdr[segmentIDOffset:segmentIDOffset+segmentIDSize], segmentID)
	binary.LittleEndian.PutUint64(hdr[segmentCreatedAtOffset:segmentCreatedAtOffset+segmentCreatedAtSize], uint64(time.Now().UnixNano()))
	crc := crc32.ChecksumIEEE(hdr[segmentVersionOffset:segmentCRCOffset])
	binary.LittleEndian.PutUint32(hdr[segmentCRCOffset:segmentCRCOffset+segmentCRCSize], crc)
	return w.Write(hdr)
}

func ReadSegmentHeader(r io.Reader) (SegmentHeader, error) {
	buf := make([]byte, SegmentHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return SegmentHeader{}, err
	}
	if !bytes.Equal(buf[segmentMagicOffset:segmentMagicOffset+SegmentMagicSize], segmentMagic) {
		return SegmentHeader{}, errs.ErrWALCorrupt
	}
	ver := buf[segmentVersionOffset]
	if ver != recordVersion {
		return SegmentHeader{}, errs.ErrWALCorrupt
	}
	blockSize := binary.LittleEndian.Uint32(buf[segmentBlockSizeOffset : segmentBlockSizeOffset+segmentBlockSizeSize])
	segmentID := binary.LittleEndian.Uint64(buf[segmentIDOffset : segmentIDOffset+segmentIDSize])
	createdAt := binary.LittleEndian.Uint64(buf[segmentCreatedAtOffset : segmentCreatedAtOffset+segmentCreatedAtSize])
	gotCRC := binary.LittleEndian.Uint32(buf[segmentCRCOffset : segmentCRCOffset+segmentCRCSize])
	if crc32.ChecksumIEEE(buf[segmentVersionOffset:segmentCRCOffset]) != gotCRC {
		return SegmentHeader{}, errs.ErrWALCorrupt
	}
	return SegmentHeader{
		Version:   ver,
		BlockSize: blockSize,
		SegmentID: segmentID,
		CreatedAt: createdAt,
	}, nil
}
