package sstable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"lsmengine/internal/lsm/sstable/block"
	"lsmengine/internal/lsm/sstable/bloom"
	"lsmengine/internal/lsm/sstable/format"
	"lsmengine/internal/lsm/sstable/index"
	"lsmengine/internal/lsm/sstable/meta"
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
	opts.Normalize()
	if err := opts.Validate(); err != nil {
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
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if cmp != 0 {
			return cmp < 0
		}
		if entries[i].Seq != entries[j].Seq {
			return entries[i].Seq > entries[j].Seq
		}
		if entries[i].Tombstone != entries[j].Tombstone {
			return entries[i].Tombstone
		}
		return false
	})
	seqMin := entries[0].Seq
	seqMax := entries[len(entries)-1].Seq
	minKey := append([]byte(nil), entries[0].Key...)
	maxKey := append([]byte(nil), entries[len(entries)-1].Key...)

	fileID := fmt.Sprintf("%d-%d", seqMax, time.Now().UnixNano())
	tempPath := filepath.Join(w.opts.Dir, fmt.Sprintf("sstable-%s.sst.tmp", fileID))
	finalPath := filepath.Join(w.opts.Dir, fmt.Sprintf("sstable-%s.sst", fileID))
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return SSTable{}, fmt.Errorf("create sstable: %w", err)
	}
	defer f.Close()

	var offset uint64
	builder := block.NewBuilder(w.opts.RestartInterval, w.opts.RestartIntervalAdaptive, w.opts.RestartIntervalMin, w.opts.RestartIntervalMax)
	var indexEntries []index.Entry
	var blockEntryCounts []int

	filter := bloom.NewFilter(len(entries), w.opts.BloomBitsPerKey)
	for _, e := range entries {
		if e.Seq < seqMin {
			seqMin = e.Seq
		}
		if e.Seq > seqMax {
			seqMax = e.Seq
		}
		if filter != nil {
			filter.Add(e.Key)
		}
		if builder.SizeBytes() > 0 && builder.EstimatedSizeAfter(e) > w.opts.BlockTargetBytes {
			count, err := w.flushBlock(f, builder, &offset, &indexEntries)
			if err != nil {
				return SSTable{}, err
			}
			blockEntryCounts = append(blockEntryCounts, count)
		}
		if builder.SizeBytes() == 0 && builder.EstimatedSizeAfter(e) > w.opts.BlockMaxBytes {
			builder.Add(e)
			count, err := w.flushBlock(f, builder, &offset, &indexEntries)
			if err != nil {
				return SSTable{}, err
			}
			blockEntryCounts = append(blockEntryCounts, count)
			continue
		}
		builder.Add(e)
		if builder.SizeBytes() >= w.opts.BlockMaxBytes {
			count, err := w.flushBlock(f, builder, &offset, &indexEntries)
			if err != nil {
				return SSTable{}, err
			}
			blockEntryCounts = append(blockEntryCounts, count)
		}
	}
	if builder.SizeBytes() > 0 {
		count, err := w.flushBlock(f, builder, &offset, &indexEntries)
		if err != nil {
			return SSTable{}, err
		}
		blockEntryCounts = append(blockEntryCounts, count)
	}

	partitioned := w.opts.IndexPartitionEntries > 0 && len(indexEntries) > w.opts.IndexPartitionEntries
	filterPartitioned := partitioned && filter != nil && w.opts.FilterPartitioned

	var bloomOffset uint64
	var bloomLen uint32
	var filterIndexEntries []index.Entry
	if filter != nil {
		if filterPartitioned {
			var err error
			filterIndexEntries, err = bloom.WritePartitionedBloomFilters(f, entries, indexEntries, blockEntryCounts, w.opts, &offset)
			if err != nil {
				return SSTable{}, err
			}
		} else {
			var err error
			bloomOffset, bloomLen, err = bloom.WriteBloomBlock(f, filter, &offset)
			if err != nil {
				return SSTable{}, err
			}
		}
	}

	topIndex := indexEntries
	if partitioned {
		topIndex = nil
		for i := 0; i < len(indexEntries); i += w.opts.IndexPartitionEntries {
			end := i + w.opts.IndexPartitionEntries
			if end > len(indexEntries) {
				end = len(indexEntries)
			}
			part := indexEntries[i:end]
			payload := index.Encode(part)
			block := format.EncodeBlock(payload, format.BlockTypeIndex, CompressionNone, uint32(len(payload)))
			n, err := f.Write(block)
			if err != nil {
				return SSTable{}, fmt.Errorf("write index partition: %w", err)
			}
			topIndex = append(topIndex, index.Entry{
				Key:    append([]byte(nil), part[0].Key...),
				Offset: offset,
				Length: uint32(n),
			})
			offset += uint64(n)
		}
	}

	indexOffset := offset
	indexPayload := index.Encode(topIndex)
	indexBlock := format.EncodeBlock(indexPayload, format.BlockTypeIndex, CompressionNone, uint32(len(indexPayload)))
	if n, err := f.Write(indexBlock); err != nil {
		return SSTable{}, fmt.Errorf("write index: %w", err)
	} else {
		offset += uint64(n)
	}

	if filterPartitioned && len(filterIndexEntries) > 0 {
		bloomOffset = offset
		payload := index.Encode(filterIndexEntries)
		block := format.EncodeBlock(payload, format.BlockTypeFilter, CompressionNone, uint32(len(payload)))
		n, err := f.Write(block)
		if err != nil {
			return SSTable{}, err
		}
		bloomLen = uint32(n)
		offset += uint64(n)
	}

	metaOffset := offset
	metaPayload := meta.Encode(meta.Meta{
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
	metaBlock := format.EncodeBlock(metaPayload, format.BlockTypeMeta, CompressionNone, uint32(len(metaPayload)))
	if n, err := f.Write(metaBlock); err != nil {
		return SSTable{}, fmt.Errorf("write meta: %w", err)
	} else {
		offset += uint64(n)
	}

	var footerFlags uint8
	if partitioned {
		footerFlags |= format.FooterFlagIndexPartitioned
	}
	if filterPartitioned {
		footerFlags |= format.FooterFlagFilterPartitioned
	}
	footerBytes := format.EncodeFooter(format.Footer{
		Flags:       footerFlags,
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
	if err := syncDir(w.opts.Dir); err != nil {
		return SSTable{}, fmt.Errorf("sync sstable dir: %w", err)
	}
	return LoadSSTable(finalPath, w.opts)
}

func (w *Writer) flushBlock(f *os.File, builder *block.Builder, offset *uint64, indexEntries *[]index.Entry) (int, error) {
	count := builder.EntryCount()
	payload := builder.Finish()
	compressed, uncompressedLen, err := format.CompressPayload(payload, w.opts.Compression)
	if err != nil {
		return 0, fmt.Errorf("compress block: %w", err)
	}
	block := format.EncodeBlock(compressed, format.BlockTypeData, w.opts.Compression, uncompressedLen)
	n, err := f.Write(block)
	if err != nil {
		return 0, fmt.Errorf("write block: %w", err)
	}
	if len(builder.FirstKey()) > 0 {
		*indexEntries = append(*indexEntries, index.Entry{
			Key:    append([]byte(nil), builder.FirstKey()...),
			Offset: *offset,
			Length: uint32(n),
		})
	}
	*offset += uint64(n)
	builder.Reset()
	return count, nil
}

func filterK(filter *bloom.Filter) uint8 {
	if filter == nil {
		return 0
	}
	return filter.K()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
