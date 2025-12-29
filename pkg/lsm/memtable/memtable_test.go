package memtable

import (
	"testing"
)

func TestMemTablePutGet(t *testing.T) {
	mt := NewMemTable()
	entry := mt.Put("alpha", []byte("one"))
	got, ok := mt.Get("alpha")
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if got.Seq != entry.Seq {
		t.Fatalf("seq mismatch")
	}
	if string(got.Value) != "one" {
		t.Fatalf("value mismatch")
	}
}

func TestMemTableDelete(t *testing.T) {
	mt := NewMemTable()
	mt.Put("alpha", []byte("one"))
	del := mt.Delete("alpha")
	if !del.Tombstone {
		t.Fatalf("expected tombstone")
	}
	got, ok := mt.Get("alpha")
	if !ok || !got.Tombstone {
		t.Fatalf("expected tombstone in table, got %+v (ok=%v)", got, ok)
	}
}

func TestMemTableDrainSorted(t *testing.T) {
	mt := NewMemTable()
	mt.Put("b", []byte("2"))
	mt.Put("a", []byte("1"))
	entries := mt.Drain()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries")
	}
	if entries[0].Key != "a" || entries[1].Key != "b" {
		t.Fatalf("expected sorted keys, got %v %v", entries[0].Key, entries[1].Key)
	}
	if mt.Size() != 0 {
		t.Fatalf("expected empty after drain")
	}
}
