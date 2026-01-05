package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"

	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

type Reader struct {
	file   *os.File
	index  []indexEntry
	meta   meta
	filter *bloomFilter
	cache  *blockCache
	opts   Options
}

func LoadSSTable(path string, opts Options) (SSTable, error) {
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

func openReader(path string, opts Options) (*Reader, uint64, error) {
	opts.normalize()
	if err := opts.validate(); err != nil {
		return nil, 0, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open sstable: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, fmt.Errorf("stat sstable: %w", err)
	}
	if info.Size() < footerSizeBytes {
		f.Close()
		return nil, 0, errs.ErrSSTableBadFooter
	}
	footerBuf := make([]byte, footerSizeBytes)
	if _, err := f.ReadAt(footerBuf, info.Size()-footerSizeBytes); err != nil {
		f.Close()
		return nil, 0, fmt.Errorf("read footer: %w", err)
	}
	ft, err := decodeFooter(footerBuf)
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	metaBuf := make([]byte, ft.MetaLen)
	if _, err := f.ReadAt(metaBuf, int64(ft.MetaOffset)); err != nil {
		f.Close()
		return nil, 0, fmt.Errorf("read meta: %w", err)
	}
	metaPayload, err := decodeWithCRC(metaBuf, errs.ErrSSTableBadMeta)
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	m, err := decodeMeta(metaPayload)
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	indexBuf := make([]byte, ft.IndexLen)
	if _, err := f.ReadAt(indexBuf, int64(ft.IndexOffset)); err != nil {
		f.Close()
		return nil, 0, fmt.Errorf("read index: %w", err)
	}
	indexPayload, err := decodeWithCRC(indexBuf, errs.ErrSSTableBadIndex)
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	index, err := decodeIndex(indexPayload)
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	var filter *bloomFilter
	if m.BloomLen > 0 && m.BloomOffset > 0 {
		filterBuf := make([]byte, m.BloomLen)
		if _, err := f.ReadAt(filterBuf, int64(m.BloomOffset)); err != nil {
			f.Close()
			return nil, 0, fmt.Errorf("read bloom: %w", err)
		}
		filterPayload, err := decodeWithCRC(filterBuf, errs.ErrSSTableBadMeta)
		if err != nil {
			f.Close()
			return nil, 0, err
		}
		filter = decodeBloomFilter(filterPayload)
	}
	reader := &Reader{
		file:   f,
		index:  index,
		meta:   m,
		filter: filter,
		cache:  newBlockCache(opts.BlockCacheBytes),
		opts:   opts,
	}
	return reader, m.SeqMax, nil
}

func (s SSTable) Get(key []byte) (types.Entry, bool) {
	if s.reader == nil {
		return types.Entry{}, false
	}
	return s.reader.Get(key)
}

func (s SSTable) Range(start, end []byte) *RangeIterator {
	if s.reader == nil {
		return &RangeIterator{}
	}
	return s.reader.Range(start, end)
}

func (r *Reader) Get(key []byte) (types.Entry, bool) {
	if r.filter != nil && !r.filter.mayContain(key) {
		return types.Entry{}, false
	}
	idx := findBlock(r.index, key)
	if idx < 0 {
		return types.Entry{}, false
	}
	blk, err := r.readBlock(idx)
	if err != nil {
		return types.Entry{}, false
	}
	entry, ok, err := blk.find(key)
	if err != nil {
		return types.Entry{}, false
	}
	return entry, ok
}

func (r *Reader) Range(start, end []byte) *RangeIterator {
	return newRangeIterator(r, start, end)
}

func (r *Reader) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	return r.file.Close()
}

func findBlock(index []indexEntry, key []byte) int {
	if len(index) == 0 {
		return -1
	}
	if bytes.Compare(key, index[0].key) < 0 {
		return -1
	}
	i := sort.Search(len(index), func(i int) bool {
		return bytes.Compare(index[i].key, key) > 0
	})
	if i == 0 {
		return -1
	}
	return i - 1
}

func (r *Reader) readBlock(idx int) (*block, error) {
	entry := r.index[idx]
	if blk, ok := r.cache.get(entry.offset); ok {
		return blk, nil
	}
	data := make([]byte, entry.length)
	if _, err := r.file.ReadAt(data, int64(entry.offset)); err != nil {
		if err == io.EOF {
			return nil, errs.ErrSSTableBadBlock
		}
		return nil, err
	}
	if len(data) < blockHeaderSize+blockCRCLen {
		return nil, errs.ErrSSTableBadBlock
	}
	header := data[:blockHeaderSize]
	comp, err := compressionFromID(header[0])
	if err != nil {
		return nil, err
	}
	uncompressedLen := binary.LittleEndian.Uint32(header[1:5])
	payload := data[blockHeaderSize : len(data)-blockCRCLen]
	expected := binary.LittleEndian.Uint32(data[len(data)-blockCRCLen:])
	if checksum(data[:len(data)-blockCRCLen]) != expected {
		return nil, errs.ErrSSTableBadBlock
	}
	plain, err := decompressBlock(payload, comp, uncompressedLen)
	if err != nil {
		return nil, err
	}
	blk, err := decodeBlock(plain)
	if err != nil {
		return nil, err
	}
	r.cache.add(entry.offset, blk)
	return blk, nil
}

func (r *Reader) prefetch(start int) {
	if r.opts.PrefetchBlocks <= 0 {
		return
	}
	for i := 1; i <= r.opts.PrefetchBlocks; i++ {
		idx := start + i
		if idx >= len(r.index) {
			return
		}
		entry := r.index[idx]
		if _, ok := r.cache.get(entry.offset); ok {
			continue
		}
		data := make([]byte, entry.length)
		if _, err := r.file.ReadAt(data, int64(entry.offset)); err != nil {
			continue
		}
		if len(data) < blockHeaderSize+blockCRCLen {
			continue
		}
		header := data[:blockHeaderSize]
		comp, err := compressionFromID(header[0])
		if err != nil {
			continue
		}
		uncompressedLen := binary.LittleEndian.Uint32(header[1:5])
		payload := data[blockHeaderSize : len(data)-blockCRCLen]
		expected := binary.LittleEndian.Uint32(data[len(data)-blockCRCLen:])
		if checksum(data[:len(data)-blockCRCLen]) != expected {
			continue
		}
		plain, err := decompressBlock(payload, comp, uncompressedLen)
		if err != nil {
			continue
		}
		blk, err := decodeBlock(plain)
		if err != nil {
			continue
		}
		r.cache.add(entry.offset, blk)
	}
}

type RangeIterator struct {
	reader       *Reader
	start        []byte
	end          []byte
	block        *block
	blockI       int
	cursor       *blockCursor
	pending      entryView
	hasPending   bool
	startApplied bool
	curr         types.Entry
}

func newRangeIterator(r *Reader, start, end []byte) *RangeIterator {
	idx := 0
	if len(start) > 0 {
		idx = findBlock(r.index, start)
		if idx < 0 {
			idx = 0
		}
	}
	return &RangeIterator{
		reader: r,
		start:  start,
		end:    end,
		blockI: idx,
	}
}

func (it *RangeIterator) Next() bool {
	if it.reader == nil {
		return false
	}
	for {
		if it.block == nil {
			if it.blockI >= len(it.reader.index) {
				return false
			}
			blk, err := it.reader.readBlock(it.blockI)
			if err != nil {
				return false
			}
			it.reader.prefetch(it.blockI)
			it.block = blk
			it.cursor = nil
			it.hasPending = false
		}
		if it.cursor == nil {
			if !it.startApplied && len(it.start) > 0 {
				cursor, entry, ok, err := it.block.seek(it.start)
				it.startApplied = true
				if err != nil {
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
				it.cursor = newBlockCursor(it.block, 0, len(it.block.data))
			}
		}
		if it.hasPending {
			it.hasPending = false
			it.curr = it.pending.toEntry()
			return true
		}
		entry, ok, err := it.cursor.next()
		if err != nil {
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
		it.curr = entry.toEntry()
		return true
	}
}

func (it *RangeIterator) Entry() types.Entry {
	return it.curr
}
