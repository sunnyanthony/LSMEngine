package skiplist

import (
	"bytes"
	"testing"

	"lsmengine/pkg/lsm/types"
)

func TestSkipListGet(t *testing.T) {
	sl := New()
	_, _, _ = sl.Upsert(types.Entry{Key: []byte("b"), Value: []byte("2"), Seq: 2})
	_, _, _ = sl.Upsert(types.Entry{Key: []byte("a"), Value: []byte("1"), Seq: 1})

	got, ok := sl.Get([]byte("a"))
	if !ok || !bytes.Equal(got.Value, []byte("1")) {
		t.Fatalf("expected value 1, got %+v ok=%v", got, ok)
	}
}

func TestSkipListUpsertUpdates(t *testing.T) {
	sl := New()
	inserted, _, _ := sl.Upsert(types.Entry{Key: []byte("a"), Value: []byte("1"), Seq: 1})
	if !inserted || sl.Len() != 1 {
		t.Fatalf("expected new insert")
	}
	inserted, _, _ = sl.Upsert(types.Entry{Key: []byte("a"), Value: []byte("2"), Seq: 2})
	if inserted || sl.Len() != 1 {
		t.Fatalf("expected update without growth")
	}
	got, ok := sl.Get([]byte("a"))
	if !ok || !bytes.Equal(got.Value, []byte("2")) || got.Seq != 2 {
		t.Fatalf("expected updated entry, got %+v ok=%v", got, ok)
	}
}

func TestSkipListIterOrder(t *testing.T) {
	sl := New()
	_, _, _ = sl.Upsert(types.Entry{Key: []byte("b"), Value: []byte("2"), Seq: 2})
	_, _, _ = sl.Upsert(types.Entry{Key: []byte("a"), Value: []byte("1"), Seq: 1})
	_, _, _ = sl.Upsert(types.Entry{Key: []byte("c"), Value: []byte("3"), Seq: 3})

	it := sl.Iter()
	var keys [][]byte
	for it.Next() {
		keys = append(keys, it.Entry().Key)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if !bytes.Equal(keys[0], []byte("a")) || !bytes.Equal(keys[1], []byte("b")) || !bytes.Equal(keys[2], []byte("c")) {
		t.Fatalf("unexpected order: %v", keys)
	}
}
