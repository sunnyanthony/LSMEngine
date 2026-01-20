// SSTable writer and flush logic.

package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"lsmengine/internal/lsm/iofs"
	"lsmengine/internal/lsm/sstable/bloom"
	sstableconfig "lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/sstable/format"
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

// Info summarizes immutable on-disk metadata for a table.
type Info struct {
	MinKey     []byte
	MaxKey     []byte
	SeqMin     uint64
	SeqMax     uint64
	SizeBytes  uint64
	EntryCount uint64
}

// Info returns immutable metadata for the table.
func (s SSTable) Info() Info {
	if s.reader == nil {
		return Info{SeqMax: s.Seq}
	}
	m := s.reader.meta
	return Info{
		MinKey:     append([]byte(nil), m.MinKey...),
		MaxKey:     append([]byte(nil), m.MaxKey...),
		SeqMin:     m.SeqMin,
		SeqMax:     m.SeqMax,
		EntryCount: m.EntryCount,
		SizeBytes:  uint64(s.reader.size),
	}
}

// Flusher writes entries to SSTable storage.
type Flusher interface {
	Flush(entries []types.Entry) (SSTable, error)
}

type Writer struct {
	opts sstableconfig.Options
	fs   iofs.FS
}

func NewSSTableWriter(opts sstableconfig.Options) (*Writer, error) {
	opts.Normalize()
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.Dir == "" {
		return nil, fmt.Errorf("sstable dir required")
	}
	if opts.FS == nil {
		opts.FS = iofs.OSFS{}
	}
	if err := opts.FS.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir sstable dir: %w", err)
	}
	return &Writer{opts: opts, fs: opts.FS}, nil
}

// Flush creates a new SSTable file with entries sorted by key.
func (w *Writer) Flush(entries []types.Entry) (table SSTable, err error) {
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
	f, err := w.fs.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return SSTable{}, fmt.Errorf("create sstable: %w", err)
	}
	closed := false
	defer func() {
		if closed {
			return
		}
		if cerr := f.Close(); cerr != nil {
			if err == nil {
				err = fmt.Errorf("close sstable: %w", cerr)
			} else {
				err = errors.Join(err, fmt.Errorf("close sstable: %w", cerr))
			}
		}
	}()

	var offset uint64
	builder := format.NewBuilder(w.opts.RestartInterval, w.opts.RestartIntervalAdaptive, w.opts.RestartIntervalMin, w.opts.RestartIntervalMax)
	var indexEntries []format.IndexEntry
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
	var filterIndexEntries []format.IndexEntry
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
			payload := format.EncodeIndex(part)
			block := format.EncodeBlock(payload, format.BlockTypeIndex, sstableconfig.CompressionNone, uint32(len(payload)))
			n, err := f.Write(block)
			if err != nil {
				return SSTable{}, fmt.Errorf("write index partition: %w", err)
			}
			topIndex = append(topIndex, format.IndexEntry{
				Key:    append([]byte(nil), part[0].Key...),
				Offset: offset,
				Length: uint32(n),
			})
			offset += uint64(n)
		}
	}

	indexOffset := offset
	indexPayload := format.EncodeIndex(topIndex)
	indexBlock := format.EncodeBlock(indexPayload, format.BlockTypeIndex, sstableconfig.CompressionNone, uint32(len(indexPayload)))
	if n, err := f.Write(indexBlock); err != nil {
		return SSTable{}, fmt.Errorf("write index: %w", err)
	} else {
		offset += uint64(n)
	}

	if filterPartitioned && len(filterIndexEntries) > 0 {
		bloomOffset = offset
		payload := format.EncodeIndex(filterIndexEntries)
		block := format.EncodeBlock(payload, format.BlockTypeFilter, sstableconfig.CompressionNone, uint32(len(payload)))
		n, err := f.Write(block)
		if err != nil {
			return SSTable{}, err
		}
		bloomLen = uint32(n)
		offset += uint64(n)
	}

	metaOffset := offset
	metaPayload := format.EncodeMeta(format.Meta{
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
	metaBlock := format.EncodeBlock(metaPayload, format.BlockTypeMeta, sstableconfig.CompressionNone, uint32(len(metaPayload)))
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
		closed = true
		return SSTable{}, fmt.Errorf("close sstable: %w", err)
	}
	closed = true
	if err := w.fs.Rename(tempPath, finalPath); err != nil {
		return SSTable{}, fmt.Errorf("rename sstable: %w", err)
	}
	if err := syncDir(w.fs, w.opts.Dir); err != nil {
		return SSTable{}, fmt.Errorf("sync sstable dir: %w", err)
	}
	return LoadSSTable(finalPath, w.opts)
}

func (w *Writer) flushBlock(f iofs.File, builder *format.Builder, offset *uint64, indexEntries *[]format.IndexEntry) (int, error) {
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
		*indexEntries = append(*indexEntries, format.IndexEntry{
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

func syncDir(fs iofs.FS, dir string) error {
	if fs == nil {
		fs = iofs.OSFS{}
	}
	d, err := fs.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		if cerr := d.Close(); cerr != nil {
			return errors.Join(err, fmt.Errorf("close dir: %w", cerr))
		}
		return err
	}
	if err := d.Close(); err != nil {
		return err
	}
	return nil
}
