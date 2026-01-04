package skiplist

import (
	"bytes"

	"lsmengine/pkg/lsm/types"
)

// Compare orders keys. It should return -1, 0, or 1.
type Compare func(a, b []byte) int

// DefaultCompare is lexicographic byte comparison.
var DefaultCompare Compare = bytes.Compare

const skiplistMaxLevel = 18

type skiplistNode struct {
	key   []byte
	entry types.Entry
	next  []*skiplistNode
}

type skiplistRNG struct {
	state uint64
}

func newSkiplistRNG(seed uint64) skiplistRNG {
	if seed == 0 {
		seed = 1
	}
	return skiplistRNG{state: seed}
}

func (r *skiplistRNG) next() uint64 {
	x := r.state
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	r.state = x
	return x
}

// SkipList stores entries in sorted key order.
type SkipList struct {
	head   *skiplistNode
	level  int
	length int
	rng    skiplistRNG
	cmp    Compare
}

func New() *SkipList {
	return NewWithCompare(DefaultCompare)
}

func NewWithCompare(cmp Compare) *SkipList {
	head := &skiplistNode{next: make([]*skiplistNode, skiplistMaxLevel)}
	return &SkipList{
		head:  head,
		level: 1,
		rng:   newSkiplistRNG(0xdecafbad),
		cmp:   cmp,
	}
}

func (s *SkipList) Len() int {
	return s.length
}

func (s *SkipList) Get(key []byte) (types.Entry, bool) {
	x := s.findPrev(key, nil).next[0]
	if x != nil && s.cmp(x.key, key) == 0 {
		return x.entry, true
	}
	return types.Entry{}, false
}

// Upsert inserts or updates a key.
// It returns (inserted, previousEntry, replaced).
func (s *SkipList) Upsert(entry types.Entry) (bool, types.Entry, bool) {
	update := make([]*skiplistNode, skiplistMaxLevel)
	x := s.findPrev(entry.Key, update).next[0]
	if x != nil && s.cmp(x.key, entry.Key) == 0 {
		prev := x.entry
		x.entry = entry
		return false, prev, true
	}

	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}
	node := &skiplistNode{
		key:   append([]byte(nil), entry.Key...),
		entry: entry,
		next:  make([]*skiplistNode, lvl),
	}
	for i := 0; i < lvl; i++ {
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}
	s.length++
	return true, types.Entry{}, false
}

func (s *SkipList) randomLevel() int {
	level := 1
	for level < skiplistMaxLevel && (s.rng.next()&1) == 1 {
		level++
	}
	return level
}

type SkipListIter struct {
	curr *skiplistNode
}

func (s *SkipList) Iter() *SkipListIter {
	return &SkipListIter{curr: s.head}
}

func (s *SkipList) IterFrom(start []byte) *SkipListIter {
	return &SkipListIter{curr: s.findPrev(start, nil)}
}

func (it *SkipListIter) Next() bool {
	if it.curr == nil || it.curr.next[0] == nil {
		return false
	}
	it.curr = it.curr.next[0]
	return true
}

func (it *SkipListIter) Entry() types.Entry {
	if it.curr == nil {
		return types.Entry{}
	}
	return it.curr.entry
}

func (s *SkipList) findPrev(key []byte, update []*skiplistNode) *skiplistNode {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && s.cmp(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
		if update != nil {
			update[i] = x
		}
	}
	return x
}
