package sstable

import (
	"encoding/binary"

	"lsmengine/pkg/lsm/errs"
)

type meta struct {
	MinKey      []byte
	MaxKey      []byte
	EntryCount  uint64
	SeqMin      uint64
	SeqMax      uint64
	Compression Compression
	BloomBits   uint16
	BloomK      uint8
	BloomOffset uint64
	BloomLen    uint32
}

const metaHeaderSize = 4 + 4 + 8 + 8 + 8 + 1 + 2 + 1 + 8 + 4

func encodeMeta(m meta) []byte {
	var out []byte
	var hdr [metaHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(m.MinKey)))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(m.MaxKey)))
	binary.LittleEndian.PutUint64(hdr[8:16], m.EntryCount)
	binary.LittleEndian.PutUint64(hdr[16:24], m.SeqMin)
	binary.LittleEndian.PutUint64(hdr[24:32], m.SeqMax)
	hdr[32] = compressionID(m.Compression)
	binary.LittleEndian.PutUint16(hdr[33:35], m.BloomBits)
	hdr[35] = m.BloomK
	binary.LittleEndian.PutUint64(hdr[36:44], m.BloomOffset)
	binary.LittleEndian.PutUint32(hdr[44:48], m.BloomLen)
	out = append(out, hdr[:]...)
	out = append(out, m.MinKey...)
	out = append(out, m.MaxKey...)
	return out
}

func decodeMeta(data []byte) (meta, error) {
	if len(data) < metaHeaderSize {
		return meta{}, errs.ErrSSTableBadMeta
	}
	minLen := binary.LittleEndian.Uint32(data[:4])
	maxLen := binary.LittleEndian.Uint32(data[4:8])
	entryCount := binary.LittleEndian.Uint64(data[8:16])
	seqMin := binary.LittleEndian.Uint64(data[16:24])
	seqMax := binary.LittleEndian.Uint64(data[24:32])
	comp, err := compressionFromID(data[32])
	if err != nil {
		return meta{}, errs.ErrSSTableBadMeta
	}
	bloomBits := binary.LittleEndian.Uint16(data[33:35])
	bloomK := data[35]
	bloomOffset := binary.LittleEndian.Uint64(data[36:44])
	bloomLen := binary.LittleEndian.Uint32(data[44:48])
	pos := metaHeaderSize
	if len(data)-pos < int(minLen+maxLen) {
		return meta{}, errs.ErrSSTableBadMeta
	}
	minKey := append([]byte(nil), data[pos:pos+int(minLen)]...)
	pos += int(minLen)
	maxKey := append([]byte(nil), data[pos:pos+int(maxLen)]...)
	return meta{
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

func encodeWithCRC(payload []byte) []byte {
	out := make([]byte, len(payload)+4)
	copy(out, payload)
	binary.LittleEndian.PutUint32(out[len(payload):], checksum(payload))
	return out
}

func decodeWithCRC(data []byte, errChecksum error) ([]byte, error) {
	if errChecksum == nil {
		errChecksum = errs.ErrSSTableBadMeta
	}
	if len(data) < 4 {
		return nil, errChecksum
	}
	payload := data[:len(data)-4]
	expected := binary.LittleEndian.Uint32(data[len(data)-4:])
	if checksum(payload) != expected {
		return nil, errChecksum
	}
	return payload, nil
}
