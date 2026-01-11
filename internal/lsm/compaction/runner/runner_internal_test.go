package runner

import (
	"errors"
	"testing"

	"lsmengine/pkg/lsm/types"
)

type sliceIter struct {
	entries []types.Entry
	idx     int
	err     error
}

func (s *sliceIter) Next() bool {
	if s.idx >= len(s.entries) {
		return false
	}
	s.idx++
	return true
}

func (s *sliceIter) Entry() types.Entry {
	return s.entries[s.idx-1]
}

func (s *sliceIter) Err() error {
	if s.idx >= len(s.entries) {
		return s.err
	}
	return nil
}

func TestMergeEntriesPrefersHighestSeq(t *testing.T) {
	iters := []entryIterator{
		&sliceIter{entries: []types.Entry{{Key: []byte("a"), Value: []byte("old"), Seq: 1}}},
		&sliceIter{entries: []types.Entry{{Key: []byte("a"), Value: []byte("new"), Seq: 3}}},
	}
	out, err := mergeEntries(iters, false)
	if err != nil {
		t.Fatalf("mergeEntries: %v", err)
	}
	if len(out) != 1 || string(out[0].Value) != "new" {
		t.Fatalf("expected newest entry, got %+v", out)
	}
}

func TestMergeEntriesDropsTombstones(t *testing.T) {
	iters := []entryIterator{
		&sliceIter{entries: []types.Entry{{Key: []byte("a"), Value: []byte("old"), Seq: 1}}},
		&sliceIter{entries: []types.Entry{{Key: []byte("a"), Tombstone: true, Seq: 2}}},
	}
	out, err := mergeEntries(iters, true)
	if err != nil {
		t.Fatalf("mergeEntries: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected tombstone to be dropped, got %+v", out)
	}
}

func TestMergeEntriesPropagatesIteratorError(t *testing.T) {
	boom := errors.New("boom")
	iters := []entryIterator{
		&sliceIter{entries: nil, err: boom},
	}
	_, err := mergeEntries(iters, false)
	if err == nil || err != boom {
		t.Fatalf("expected error %v, got %v", boom, err)
	}
}

func TestAdvanceIteratorSkipsSameKeys(t *testing.T) {
	iter := &sliceIter{entries: []types.Entry{
		{Key: []byte("a"), Seq: 3},
		{Key: []byte("a"), Seq: 2},
		{Key: []byte("b"), Seq: 1},
	}}
	item := mergeItem{iter: iter, entry: types.Entry{Key: []byte("a"), Seq: 3}}
	next, ok, err := advanceIterator(item, []byte("a"))
	if err != nil {
		t.Fatalf("advanceIterator: %v", err)
	}
	if !ok || string(next.entry.Key) != "b" {
		t.Fatalf("expected next key b, got ok=%v entry=%+v", ok, next.entry)
	}
}
