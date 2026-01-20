// SSTable reader and range iterator.

package sstable

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"lsmengine/internal/lsm/iofs"
	"lsmengine/internal/lsm/memory"
	"lsmengine/internal/lsm/sstable/bloom"
	"lsmengine/internal/lsm/sstable/cache"
	"lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/sstable/format"
	"lsmengine/internal/lsm/sstable/storage"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

type Reader struct {
	file    iofs.File
	size    int64
	meta    format.Meta
	opts    config.Options
	dropped bool

	source storage.BlockSource
	pipe   *Pipeline
}

func LoadSSTable(path string, opts config.Options) (SSTable, error) {
	reader, seq, err := openReader(path, opts)
	if err != nil {
		return SSTable{}, err
	}
	return SSTable{
		Path:   path,
		Seq:    seq,
		reader: reader,
	}, nil
}

func openReader(path string, opts config.Options) (*Reader, uint64, error) {
	opts.Normalize()
	if err := opts.Validate(); err != nil {
		return nil, 0, err
	}
	fs := opts.FS
	if fs == nil {
		fs = iofs.OSFS{}
	}
	f, err := fs.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open sstable: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, fmt.Errorf("stat sstable: %w", err)
	}
	if info.Size() < format.FooterSizeBytes {
		f.Close()
		return nil, 0, errs.ErrSSTableBadFooter
	}
	source := storage.NewBlockSource(f, info.Size(), opts)
	footerDesc := storage.BlockDescriptor{
		ID:     uint64(info.Size() - format.FooterSizeBytes),
		Type:   format.BlockTypeMeta,
		Offset: uint64(info.Size() - format.FooterSizeBytes),
		Length: uint32(format.FooterSizeBytes),
	}
	footerBuf, err := source.Read(context.Background(), footerDesc, storage.ReadHint{})
	if err != nil {
		_ = source.Close()
		f.Close()
		return nil, 0, fmt.Errorf("read footer: %w", err)
	}
	if footerBuf.Release != nil {
		defer footerBuf.Release()
	}
	ft, err := format.DecodeFooter(footerBuf.Data)
	if err != nil {
		_ = source.Close()
		f.Close()
		return nil, 0, err
	}
	metaDesc := storage.BlockDescriptor{
		ID:     ft.MetaOffset,
		Type:   format.BlockTypeMeta,
		Offset: ft.MetaOffset,
		Length: ft.MetaLen,
	}
	metaPayload, err := ReadBlockPayload(context.Background(), source, metaDesc, errs.ErrSSTableBadMeta)
	if err != nil {
		_ = source.Close()
		f.Close()
		return nil, 0, err
	}
	m, err := format.DecodeMeta(metaPayload)
	if err != nil {
		_ = source.Close()
		f.Close()
		return nil, 0, err
	}
	indexDesc := storage.BlockDescriptor{
		ID:     ft.IndexOffset,
		Type:   format.BlockTypeIndex,
		Offset: ft.IndexOffset,
		Length: ft.IndexLen,
	}
	indexPayload, err := ReadBlockPayload(context.Background(), source, indexDesc, errs.ErrSSTableBadIndex)
	if err != nil {
		_ = source.Close()
		f.Close()
		return nil, 0, err
	}
	indexEntries, err := format.DecodeIndex(indexPayload)
	if err != nil {
		_ = source.Close()
		f.Close()
		return nil, 0, err
	}
	var filter *bloom.Filter
	var filterIndex []format.IndexEntry
	filterPartitioned := ft.Flags&format.FooterFlagFilterPartitioned != 0
	if m.BloomLen > 0 && m.BloomOffset > 0 {
		filterDesc := storage.BlockDescriptor{
			ID:     m.BloomOffset,
			Type:   format.BlockTypeFilter,
			Offset: m.BloomOffset,
			Length: m.BloomLen,
		}
		filterPayload, err := ReadBlockPayload(context.Background(), source, filterDesc, errs.ErrSSTableBadMeta)
		if err != nil {
			_ = source.Close()
			f.Close()
			return nil, 0, err
		}
		if filterPartitioned {
			filterIndex, err = format.DecodeIndex(filterPayload)
			if err != nil {
				_ = source.Close()
				f.Close()
				return nil, 0, err
			}
		} else {
			filter = bloom.Decode(filterPayload)
			if filter == nil {
				_ = source.Close()
				f.Close()
				return nil, 0, errs.ErrSSTableBadMeta
			}
		}
	}
	partitioned := ft.Flags&format.FooterFlagIndexPartitioned != 0
	blockCache := cache.NewBlockCache(opts.BlockCacheBytes)
	var indexCache *cache.IndexCache
	if partitioned {
		indexCache = cache.NewIndexCache(opts.IndexCacheBytes)
	}
	var filterCache *cache.FilterCache
	if filterPartitioned {
		filterCache = cache.NewFilterCache(opts.FilterCacheBytes)
	}
	policy := config.SnapshotFromOptions(opts, partitioned, filterPartitioned)
	pipe := NewPipeline(
		source,
		blockCache,
		indexCache,
		filterCache,
		indexEntries,
		partitioned,
		filter,
		filterIndex,
		filterPartitioned,
		info.Size(),
		policy,
	)
	if opts.FlowObserver != nil {
		pipe = pipe.WithObserver(opts.FlowObserver)
	}
	reader := &Reader{
		file:   f,
		size:   info.Size(),
		meta:   m,
		opts:   opts,
		source: source,
		pipe:   pipe,
	}
	return reader, m.SeqMax, nil
}

func (s SSTable) Get(key []byte) (types.Entry, bool) {
	if s.reader == nil {
		return types.Entry{}, false
	}
	return s.reader.Get(key)
}

func (s SSTable) GetView(key []byte) (memory.EntryView, bool) {
	if s.reader == nil {
		return memory.EntryView{}, false
	}
	return s.reader.GetView(key)
}

func (s SSTable) Range(start, end []byte) *RangeIterator {
	if s.reader == nil {
		return &RangeIterator{}
	}
	return s.reader.Range(start, end)
}

func (r *Reader) Get(key []byte) (types.Entry, bool) {
	view, ok := r.GetView(key)
	if !ok {
		return types.Entry{}, false
	}
	return view.ToEntry(), true
}

func (r *Reader) GetView(key []byte) (memory.EntryView, bool) {
	if r.dropped {
		return memory.EntryView{}, false
	}
	if len(r.meta.MinKey) > 0 && bytes.Compare(key, r.meta.MinKey) < 0 {
		return memory.EntryView{}, false
	}
	if len(r.meta.MaxKey) > 0 && bytes.Compare(key, r.meta.MaxKey) > 0 {
		return memory.EntryView{}, false
	}
	if r.pipe == nil {
		return memory.EntryView{}, false
	}
	entry, ok, err := r.pipe.Get(context.Background(), key)
	if err != nil {
		if r.opts.CorruptionPolicy == config.CorruptionDropTable {
			r.dropped = true
		}
		return memory.EntryView{}, false
	}
	if !ok {
		return memory.EntryView{}, false
	}
	return entry, true
}

func (r *Reader) Range(start, end []byte) *RangeIterator {
	return newRangeIterator(r, start, end)
}

func (r *Reader) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	if r.pipe != nil {
		r.pipe.StopPrefetcher()
	}
	if r.source != nil {
		if closer, ok := r.source.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
	return r.file.Close()
}

type RangeIterator struct {
	reader         *Reader
	start          []byte
	end            []byte
	block          *format.Block
	blockI         int
	partIndex      []format.IndexEntry
	plan           *IndexRangePlan
	cursor         *format.Cursor
	pending        memory.EntryView
	hasPending     bool
	startApplied   bool
	curr           memory.EntryView
	err            error
	prefetchBudget *PrefetchBudget
}

func newRangeIterator(r *Reader, start, end []byte) *RangeIterator {
	it := &RangeIterator{
		reader: r,
		start:  start,
		end:    end,
	}
	if r == nil || r.pipe == nil {
		return it
	}
	it.plan = r.pipe.NewRangePlan(start)
	it.prefetchBudget = r.pipe.NewPrefetchBudget()
	return it
}

func (it *RangeIterator) Next() bool {
	if it.reader == nil || it.reader.dropped || it.reader.pipe == nil {
		return false
	}
	for {
		if it.block == nil {
			if it.partIndex == nil || it.blockI >= len(it.partIndex) {
				if !it.loadPartition() {
					return false
				}
			}
			blk, err := it.reader.pipe.ReadBlockEntry(it.partIndex[it.blockI])
			if err != nil {
				if it.handleCorruption(err) {
					continue
				}
				return false
			}
			it.reader.pipe.Prefetch(it.partIndex, it.blockI, it.prefetchBudget)
			it.block = blk
			it.cursor = nil
			it.hasPending = false
		}
		if it.cursor == nil {
			if !it.startApplied && len(it.start) > 0 {
				cursor, entry, ok, err := it.block.Seek(it.start)
				it.startApplied = true
				if err != nil {
					if it.handleCorruption(err) {
						continue
					}
					return false
				}
				if !ok {
					it.block = nil
					it.blockI++
					continue
				}
				if len(it.end) > 0 && bytes.Compare(entry.Key, it.end) >= 0 {
					return false
				}
				it.cursor = cursor
				it.pending = entry
				it.hasPending = true
			} else {
				it.startApplied = true
				it.cursor = format.NewCursor(it.block, 0, it.block.DataLen())
			}
		}
		if it.hasPending {
			it.hasPending = false
			it.curr = it.pending
			return true
		}
		entry, ok, err := it.cursor.Next()
		if err != nil {
			if it.handleCorruption(err) {
				continue
			}
			return false
		}
		if !ok {
			it.block = nil
			it.blockI++
			it.cursor = nil
			continue
		}
		if len(it.end) > 0 && bytes.Compare(entry.Key, it.end) >= 0 {
			return false
		}
		it.curr = entry
		return true
	}
}

func (it *RangeIterator) loadPartition() bool {
	if it.reader == nil || it.plan == nil {
		return false
	}
	for {
		part, err := it.plan.Next(context.Background())
		if err != nil {
			if err == io.EOF {
				return false
			}
			if it.handleIndexCorruption(err) {
				continue
			}
			return false
		}
		it.partIndex = part
		it.blockI = 0
		if !it.startApplied && len(it.start) > 0 {
			it.blockI = findBlock(it.partIndex, it.start)
			if it.blockI < 0 {
				it.blockI = 0
			}
		}
		return true
	}
}

func (it *RangeIterator) Entry() types.Entry {
	return it.curr.ToEntry()
}

func (it *RangeIterator) EntryView() memory.EntryView {
	return it.curr
}

func (it *RangeIterator) Err() error {
	return it.err
}

func (it *RangeIterator) handleCorruption(err error) bool {
	switch it.reader.opts.CorruptionPolicy {
	case config.CorruptionSkipBlock:
		it.block = nil
		it.cursor = nil
		it.hasPending = false
		it.blockI++
		return true
	case config.CorruptionDropTable:
		it.reader.dropped = true
		return false
	default:
		it.err = err
		return false
	}
}

func (it *RangeIterator) handleIndexCorruption(err error) bool {
	switch it.reader.opts.CorruptionPolicy {
	case config.CorruptionSkipBlock:
		it.partIndex = nil
		it.block = nil
		it.cursor = nil
		it.hasPending = false
		return true
	case config.CorruptionDropTable:
		it.reader.dropped = true
		return false
	default:
		it.err = err
		return false
	}
}
