package wal

var (
	segmentMagic  = []byte{'L', 'S', 'M', 'W'}
	blockMagic    = []byte{'L', 'S', 'M', 'B'}
	recordVersion = byte(1)
)

const (
	segmentMagicSize     = 4
	segmentVersionSize   = 1
	segmentBlockSizeSize = 4
	segmentIDSize        = 8
	segmentCreatedAtSize = 8
	segmentCRCSize       = 4
	segmentHeaderSize    = segmentMagicSize + segmentVersionSize + segmentBlockSizeSize + segmentIDSize + segmentCreatedAtSize + segmentCRCSize

	blockMagicSize  = 4
	blockLenSize    = 4
	blockCRCSize    = 4
	blockHeaderSize = blockMagicSize + blockLenSize + blockCRCSize

	recordFlagsSize  = 1
	recordSeqSize    = 8
	recordKeyLenSize = 4
	recordValLenSize = 4
	recordCRCSize    = 4
	recordHeaderSize = recordFlagsSize + recordSeqSize + recordKeyLenSize + recordValLenSize
)

const (
	segmentMagicOffset     = 0
	segmentVersionOffset   = segmentMagicOffset + segmentMagicSize
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
