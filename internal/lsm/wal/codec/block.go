package codec

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net"

	"lsmengine/pkg/lsm/errs"
)

func WriteBlock(w io.Writer, records []RecordBuffer) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	payloadLen := 0
	for _, r := range records {
		payloadLen += r.Total
	}
	header := make([]byte, BlockHeaderSize)
	copy(header[blockMagicOffset:blockMagicOffset+blockMagicSize], blockMagic)
	binary.LittleEndian.PutUint32(header[blockLenOffset:blockLenOffset+blockLenSize], uint32(payloadLen))

	blockCRC := crc32.NewIEEE()
	for _, r := range records {
		blockCRC.Write(r.header[:])
		blockCRC.Write(r.key)
		blockCRC.Write(r.val)
		blockCRC.Write(r.crc[:])
	}
	binary.LittleEndian.PutUint32(header[blockCRCOffset:blockCRCOffset+blockCRCSize], blockCRC.Sum32())

	buffers := net.Buffers{header}
	for i := range records {
		buffers = append(buffers, records[i].buffers()...)
	}
	n, err := buffers.WriteTo(w)
	return int(n), err
}

func DecodeBlock(r io.Reader, maxPayload uint32) ([]byte, bool, error) {
	magic := make([]byte, blockMagicSize)
	if _, err := io.ReadFull(r, magic); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, false, errs.ErrWALCorrupt
		}
		return nil, false, err
	}
	if !bytes.Equal(magic, blockMagic) {
		return nil, false, errs.ErrWALCorrupt
	}
	return DecodeBlockAfterMagic(r, maxPayload)
}

func DecodeBlockAfterMagic(r io.Reader, maxPayload uint32) ([]byte, bool, error) {
	lenBuf := make([]byte, blockLenSize)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, false, errs.ErrWALCorrupt
		}
		return nil, false, err
	}
	crcBuf := make([]byte, blockCRCSize)
	if _, err := io.ReadFull(r, crcBuf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, false, errs.ErrWALCorrupt
		}
		return nil, false, err
	}
	payloadLen := binary.LittleEndian.Uint32(lenBuf)
	if maxPayload > 0 && payloadLen > maxPayload {
		return nil, false, errs.ErrWALCorrupt
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, false, errs.ErrWALCorrupt
		}
		return nil, false, err
	}
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(crcBuf) {
		return nil, false, errs.ErrWALCorrupt
	}
	return payload, true, nil
}

// ResyncBlock scans until the next block magic is found. ok=false means EOF.
func ResyncBlock(r *bufio.Reader) (bool, error) {
	window := make([]byte, 0, len(blockMagic))
	for {
		b, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return false, nil
			}
			return false, err
		}
		if len(window) < len(blockMagic) {
			window = append(window, b)
		} else {
			copy(window, window[1:])
			window[len(window)-1] = b
		}
		if len(window) == len(blockMagic) && bytes.Equal(window, blockMagic) {
			return true, nil
		}
	}
}
