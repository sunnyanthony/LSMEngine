// SSTable block and footer wire format helpers.

package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/golang/snappy"

	"lsmengine/internal/lsm/sstable/config"
	"lsmengine/pkg/lsm/errs"
)

const (
	fileMagic       = "LSM1"
	fileVersion     = 1
	FooterSizeBytes = 36

	BlockHeaderSizeLegacy = 1 + 4 // compression + uncompressed length
	BlockMagicSize        = 4
	BlockHeaderSize       = BlockMagicSize + 1 + 4 // magic + compression + uncompressed length
	BlockCRCLen           = 4
	BlockTypeSize         = 1

	compressionIDNone   byte = 0
	compressionIDSnappy byte = 1
)

type BlockType byte

const (
	BlockTypeData BlockType = iota
	BlockTypeIndex
	BlockTypeMeta
	BlockTypeFilter
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var blockMagic = []byte{'L', 'S', 'M', 'B'}

const (
	FooterFlagIndexPartitioned  = 1 << 0
	FooterFlagFilterPartitioned = 1 << 1
)

func Checksum(payload []byte) uint32 {
	return crc32.Checksum(payload, crcTable)
}

func CompressionID(c config.Compression) byte {
	switch c {
	case config.CompressionSnappy:
		return compressionIDSnappy
	default:
		return compressionIDNone
	}
}

func CompressionFromID(id byte) (config.Compression, error) {
	switch id {
	case compressionIDNone:
		return config.CompressionNone, nil
	case compressionIDSnappy:
		return config.CompressionSnappy, nil
	default:
		return "", fmt.Errorf("%w %d", errs.ErrSSTableUnknownCompression, id)
	}
}

func EncodeBlock(payload []byte, typ BlockType, compression config.Compression, uncompressedLen uint32) []byte {
	out := make([]byte, 0, BlockHeaderSize+len(payload)+BlockTypeSize+BlockCRCLen)
	var header [BlockHeaderSize]byte
	copy(header[:BlockMagicSize], blockMagic)
	header[BlockMagicSize] = CompressionID(compression)
	binary.LittleEndian.PutUint32(header[BlockMagicSize+1:BlockMagicSize+5], uncompressedLen)
	out = append(out, header[:]...)
	out = append(out, payload...)
	out = append(out, byte(typ))
	crc := Checksum(out)
	var crcBytes [4]byte
	binary.LittleEndian.PutUint32(crcBytes[:], crc)
	out = append(out, crcBytes[:]...)
	return out
}

func DecodeBlockPayload(data []byte, typ BlockType, errChecksum error) ([]byte, error) {
	if errChecksum == nil {
		errChecksum = errs.ErrSSTableBadBlock
	}
	if len(data) < BlockTypeSize+BlockCRCLen {
		return nil, errChecksum
	}
	blockTypeOffset := len(data) - BlockTypeSize - BlockCRCLen
	if data[blockTypeOffset] != byte(typ) {
		return nil, errChecksum
	}
	expected := binary.LittleEndian.Uint32(data[len(data)-BlockCRCLen:])
	if Checksum(data[:len(data)-BlockCRCLen]) != expected {
		return nil, errChecksum
	}
	headerLen := 0
	comp := config.CompressionNone
	var uncompressedLen uint32
	if len(data) >= BlockHeaderSize && bytes.Equal(data[:BlockMagicSize], blockMagic) {
		decoded, err := CompressionFromID(data[BlockMagicSize])
		if err != nil {
			return nil, err
		}
		comp = decoded
		uncompressedLen = binary.LittleEndian.Uint32(data[BlockMagicSize+1 : BlockMagicSize+5])
		headerLen = BlockHeaderSize
	} else if typ == BlockTypeData && len(data) >= BlockHeaderSizeLegacy {
		decoded, err := CompressionFromID(data[0])
		if err != nil {
			return nil, err
		}
		comp = decoded
		uncompressedLen = binary.LittleEndian.Uint32(data[1:5])
		headerLen = BlockHeaderSizeLegacy
	}
	if headerLen > blockTypeOffset {
		return nil, errChecksum
	}
	payload := data[headerLen:blockTypeOffset]
	if comp == config.CompressionNone {
		if headerLen > 0 && uncompressedLen > 0 && uint32(len(payload)) != uncompressedLen {
			return nil, errChecksum
		}
		return payload, nil
	}
	plain, err := decompressPayload(payload, comp, uncompressedLen)
	if err != nil {
		return nil, err
	}
	return plain, nil
}

type Footer struct {
	Flags       uint8
	IndexOffset uint64
	IndexLen    uint32
	MetaOffset  uint64
	MetaLen     uint32
}

func EncodeFooter(f Footer) []byte {
	buf := make([]byte, FooterSizeBytes)
	copy(buf[:4], []byte(fileMagic))
	buf[4] = fileVersion
	buf[5] = f.Flags
	binary.LittleEndian.PutUint16(buf[6:8], 0)
	binary.LittleEndian.PutUint64(buf[8:16], f.IndexOffset)
	binary.LittleEndian.PutUint32(buf[16:20], f.IndexLen)
	binary.LittleEndian.PutUint64(buf[20:28], f.MetaOffset)
	binary.LittleEndian.PutUint32(buf[28:32], f.MetaLen)
	crc := Checksum(buf[:32])
	binary.LittleEndian.PutUint32(buf[32:36], crc)
	return buf
}

func DecodeFooter(buf []byte) (Footer, error) {
	if len(buf) != FooterSizeBytes {
		return Footer{}, errs.ErrSSTableBadFooter
	}
	if string(buf[:4]) != fileMagic {
		return Footer{}, errs.ErrSSTableBadMagic
	}
	expected := binary.LittleEndian.Uint32(buf[32:36])
	if Checksum(buf[:32]) != expected {
		return Footer{}, errs.ErrSSTableBadFooter
	}
	return Footer{
		Flags:       buf[5],
		IndexOffset: binary.LittleEndian.Uint64(buf[8:16]),
		IndexLen:    binary.LittleEndian.Uint32(buf[16:20]),
		MetaOffset:  binary.LittleEndian.Uint64(buf[20:28]),
		MetaLen:     binary.LittleEndian.Uint32(buf[28:32]),
	}, nil
}

func CompressPayload(payload []byte, compression config.Compression) ([]byte, uint32, error) {
	if compression == config.CompressionNone {
		return payload, uint32(len(payload)), nil
	}
	if compression == config.CompressionSnappy {
		out := snappy.Encode(nil, payload)
		return out, uint32(len(payload)), nil
	}
	return payload, uint32(len(payload)), nil
}

func decompressPayload(payload []byte, compression config.Compression, uncompressedLen uint32) ([]byte, error) {
	if compression == config.CompressionNone {
		return payload, nil
	}
	if compression == config.CompressionSnappy {
		out, err := snappy.Decode(nil, payload)
		if err != nil {
			return nil, err
		}
		if uncompressedLen > 0 && uint32(len(out)) != uncompressedLen {
			return nil, errs.ErrSSTableBadBlock
		}
		return out, nil
	}
	return payload, nil
}
