// WAL format constants and helpers.

package codec

var (
	segmentMagic  = []byte{'L', 'S', 'M', 'W'}
	blockMagic    = []byte{'L', 'S', 'M', 'B'}
	recordVersion = byte(1)
)

const (
	SegmentMagicSize     = 4
	segmentVersionSize   = 1
	segmentBlockSizeSize = 4
	segmentIDSize        = 8
	segmentCreatedAtSize = 8
	segmentCRCSize       = 4
	SegmentHeaderSize    = SegmentMagicSize + segmentVersionSize + segmentBlockSizeSize + segmentIDSize + segmentCreatedAtSize + segmentCRCSize

	blockMagicSize  = 4
	blockLenSize    = 4
	blockCRCSize    = 4
	BlockHeaderSize = blockMagicSize + blockLenSize + blockCRCSize

	recordFlagsSize  = 1
	recordSeqSize    = 8
	recordKeyLenSize = 4
	recordValLenSize = 4
	RecordCRCSize    = 4
	RecordHeaderSize = recordFlagsSize + recordSeqSize + recordKeyLenSize + recordValLenSize

	MinBlockSize = RecordHeaderSize + RecordCRCSize + 1
)

const (
	segmentMagicOffset     = 0
	segmentVersionOffset   = segmentMagicOffset + SegmentMagicSize
	segmentBlockSizeOffset = segmentVersionOffset + segmentVersionSize
	segmentIDOffset        = segmentBlockSizeOffset + segmentBlockSizeSize
	segmentCreatedAtOffset = segmentIDOffset + segmentIDSize
	segmentCRCOffset       = segmentCreatedAtOffset + segmentCreatedAtSize
)

const (
	blockMagicOffset = 0
	blockLenOffset   = blockMagicOffset + blockMagicSize
	blockCRCOffset   = blockLenOffset + blockLenSize
)

const (
	recordFlagsOffset  = 0
	recordSeqOffset    = recordFlagsOffset + recordFlagsSize
	recordKeyLenOffset = recordSeqOffset + recordSeqSize
	recordValLenOffset = recordKeyLenOffset + recordKeyLenSize
)

func BlockMagic() []byte {
	return append([]byte(nil), blockMagic...)
}
