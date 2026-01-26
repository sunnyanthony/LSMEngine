package types

import "testing"

func TestEntryFields(t *testing.T) {
	entry := Entry{
		Key:       []byte("k"),
		Value:     []byte("v"),
		Tombstone: true,
		Seq:       10,
	}
	if string(entry.Key) != "k" || string(entry.Value) != "v" {
		t.Fatalf("unexpected entry data")
	}
	if !entry.Tombstone || entry.Seq != 10 {
		t.Fatalf("unexpected entry metadata")
	}
}
