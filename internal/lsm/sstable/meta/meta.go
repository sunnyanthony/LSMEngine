package meta

import (
	"encoding/binary"

	"lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/sstable/format"
	"lsmengine/pkg/lsm/errs"
)

type Meta struct {
	MinKey      []byte
	MaxKey      []byte
	EntryCount  uint64
	SeqMin      uint64
	SeqMax      uint64
	Compression config.Compression
	BloomBits   uint16
	BloomK      uint8
	BloomOffset uint64
	BloomLen    uint32
}

const HeaderSize = 4 + 4 + 8 + 8 + 8 + 1 + 2 + 1 + 8 + 4

func Encode(m Meta) []byte {
	var out []byte
	var hdr [HeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(m.MinKey)))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(m.MaxKey)))
	binary.LittleEndian.PutUint64(hdr[8:16], m.EntryCount)
	binary.LittleEndian.PutUint64(hdr[16:24], m.SeqMin)
	binary.LittleEndian.PutUint64(hdr[24:32], m.SeqMax)
	hdr[32] = format.CompressionID(m.Compression)
	binary.LittleEndian.PutUint16(hdr[33:35], m.BloomBits)
	hdr[35] = m.BloomK
	binary.LittleEndian.PutUint64(hdr[36:44], m.BloomOffset)
	binary.LittleEndian.PutUint32(hdr[44:48], m.BloomLen)
	out = append(out, hdr[:]...)
	out = append(out, m.MinKey...)
	out = append(out, m.MaxKey...)
	return out
}

func Decode(data []byte) (Meta, error) {
	if len(data) < HeaderSize {
		return Meta{}, errs.ErrSSTableBadMeta
	}
	minLen := binary.LittleEndian.Uint32(data[:4])
	maxLen := binary.LittleEndian.Uint32(data[4:8])
	entryCount := binary.LittleEndian.Uint64(data[8:16])
	seqMin := binary.LittleEndian.Uint64(data[16:24])
	seqMax := binary.LittleEndian.Uint64(data[24:32])
	comp, err := format.CompressionFromID(data[32])
	if err != nil {
		return Meta{}, errs.ErrSSTableBadMeta
	}
	bloomBits := binary.LittleEndian.Uint16(data[33:35])
	bloomK := data[35]
	bloomOffset := binary.LittleEndian.Uint64(data[36:44])
	bloomLen := binary.LittleEndian.Uint32(data[44:48])
	pos := HeaderSize
	if len(data)-pos < int(minLen+maxLen) {
		return Meta{}, errs.ErrSSTableBadMeta
	}
	minKey := append([]byte(nil), data[pos:pos+int(minLen)]...)
	pos += int(minLen)
	maxKey := append([]byte(nil), data[pos:pos+int(maxLen)]...)
	return Meta{
		MinKey:      minKey,
		MaxKey:      maxKey,
		EntryCount:  entryCount,
		SeqMin:      seqMin,
		SeqMax:      seqMax,
		Compression: comp,
		BloomBits:   bloomBits,
		BloomK:      bloomK,
		BloomOffset: bloomOffset,
		BloomLen:    bloomLen,
	}, nil
}
