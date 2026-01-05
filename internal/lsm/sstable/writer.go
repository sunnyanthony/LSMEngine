package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"lsmengine/pkg/lsm/types"
)

// SSTable is an immutable sorted run persisted to disk.
type SSTable struct {
	Path string
	Seq  uint64

	reader *Reader
}

func (s SSTable) Close() error {
	if s.reader == nil {
		return nil
	}
	return s.reader.Close()
}

// Flusher writes entries to SSTable storage.
type Flusher interface {
	Flush(entries []types.Entry) (SSTable, error)
}

type Writer struct {
	opts Options
}

func NewSSTableWriter(opts Options) (*Writer, error) {
	opts.normalize()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	if opts.Dir == "" {
		return nil, fmt.Errorf("sstable dir required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir sstable dir: %w", err)
	}
	return &Writer{opts: opts}, nil
}

// Flush creates a new SSTable file with entries sorted by key.
func (w *Writer) Flush(entries []types.Entry) (SSTable, error) {
	if len(entries) == 0 {
		return SSTable{}, fmt.Errorf("no entries to write")
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	seqMin := entries[0].Seq
	seqMax := entries[len(entries)-1].Seq
	minKey := append([]byte(nil), entries[0].Key...)
	maxKey := append([]byte(nil), entries[len(entries)-1].Key...)

	tempPath := filepath.Join(w.opts.Dir, fmt.Sprintf("sstable-%d.sst.tmp", seqMax))
	finalPath := filepath.Join(w.opts.Dir, fmt.Sprintf("sstable-%d.sst", seqMax))
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return SSTable{}, fmt.Errorf("create sstable: %w", err)
	}
	defer f.Close()

	var offset uint64
	builder := &blockBuilder{}
	var index []indexEntry

	filter := newBloomFilter(len(entries), w.opts.BloomBitsPerKey)
	for _, e := range entries {
		if e.Seq < seqMin {
			seqMin = e.Seq
		}
		if e.Seq > seqMax {
			seqMax = e.Seq
		}
		if filter != nil {
			filter.add(e.Key)
		}
		if builder.sizeBytes() > 0 && builder.estimatedSizeAfter(e) > w.opts.BlockTargetBytes {
			if err := w.flushBlock(f, builder, &offset, &index); err != nil {
				return SSTable{}, err
			}
		}
		if builder.sizeBytes() == 0 && builder.estimatedSizeAfter(e) > w.opts.BlockMaxBytes {
			builder.add(e)
			if err := w.flushBlock(f, builder, &offset, &index); err != nil {
				return SSTable{}, err
			}
			continue
		}
		builder.add(e)
		if builder.sizeBytes() >= w.opts.BlockMaxBytes {
			if err := w.flushBlock(f, builder, &offset, &index); err != nil {
				return SSTable{}, err
			}
		}
	}
	if builder.sizeBytes() > 0 {
		if err := w.flushBlock(f, builder, &offset, &index); err != nil {
			return SSTable{}, err
		}
	}

	var bloomOffset uint64
	var bloomLen uint32
	if filter != nil {
		bloomOffset = offset
		payload := filter.encode()
		n, err := writeBlockWithCRC(f, payload)
		if err != nil {
			return SSTable{}, err
		}
		bloomLen = uint32(n)
		offset += uint64(n)
	}

	indexOffset := offset
	indexPayload := encodeIndex(index)
	indexBlock := encodeWithCRC(indexPayload)
	if n, err := f.Write(indexBlock); err != nil {
		return SSTable{}, fmt.Errorf("write index: %w", err)
	} else {
		offset += uint64(n)
	}

	metaOffset := offset
	metaPayload := encodeMeta(meta{
		MinKey:      minKey,
		MaxKey:      maxKey,
		EntryCount:  uint64(len(entries)),
		SeqMin:      seqMin,
		SeqMax:      seqMax,
		Compression: w.opts.Compression,
		BloomBits:   uint16(w.opts.BloomBitsPerKey),
		BloomK:      filterK(filter),
		BloomOffset: bloomOffset,
		BloomLen:    bloomLen,
	})
	metaBlock := encodeWithCRC(metaPayload)
	if n, err := f.Write(metaBlock); err != nil {
		return SSTable{}, fmt.Errorf("write meta: %w", err)
	} else {
		offset += uint64(n)
	}

	footerBytes := encodeFooter(footer{
		IndexOffset: indexOffset,
		IndexLen:    uint32(len(indexBlock)),
		MetaOffset:  metaOffset,
		MetaLen:     uint32(len(metaBlock)),
	})
	if _, err := f.Write(footerBytes); err != nil {
		return SSTable{}, fmt.Errorf("write footer: %w", err)
	}
	if err := f.Sync(); err != nil {
		return SSTable{}, fmt.Errorf("sync sstable: %w", err)
	}
	if err := f.Close(); err != nil {
		return SSTable{}, fmt.Errorf("close sstable: %w", err)
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		return SSTable{}, fmt.Errorf("rename sstable: %w", err)
	}
	return LoadSSTable(finalPath, w.opts)
}

func (w *Writer) flushBlock(f *os.File, builder *blockBuilder, offset *uint64, index *[]indexEntry) error {
	payload := builder.finish()
	compressed, uncompressedLen, err := compressBlock(payload, w.opts.Compression)
	if err != nil {
		return fmt.Errorf("compress block: %w", err)
	}
	var header [blockHeaderSize]byte
	header[0] = compressionID(w.opts.Compression)
	binary.LittleEndian.PutUint32(header[1:5], uncompressedLen)
	var block bytes.Buffer
	block.Write(header[:])
	block.Write(compressed)
	crc := checksum(block.Bytes())
	var crcBytes [4]byte
	binary.LittleEndian.PutUint32(crcBytes[:], crc)
	block.Write(crcBytes[:])
	n, err := f.Write(block.Bytes())
	if err != nil {
		return fmt.Errorf("write block: %w", err)
	}
	if len(builder.first) > 0 {
		*index = append(*index, indexEntry{
			key:    append([]byte(nil), builder.first...),
			offset: *offset,
			length: uint32(n),
		})
	}
	*offset += uint64(n)
	builder.reset()
	return nil
}

func writeBlockWithCRC(f *os.File, payload []byte) (int, error) {
	block := encodeWithCRC(payload)
	n, err := f.Write(block)
	if err != nil {
		return 0, fmt.Errorf("write block: %w", err)
	}
	return n, nil
}

func filterK(filter *bloomFilter) uint8 {
	if filter == nil {
		return 0
	}
	return filter.k
}
