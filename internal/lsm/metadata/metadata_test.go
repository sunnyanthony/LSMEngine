package metadata

import "testing"

func TestTableMetaCopy(t *testing.T) {
	meta := TableMeta{
		Path:   "a.sst",
		MinKey: []byte("a"),
		MaxKey: []byte("z"),
	}
	copy := meta.Copy()
	if string(copy.MinKey) != "a" || string(copy.MaxKey) != "z" {
		t.Fatalf("unexpected copy: %+v", copy)
	}
	copy.MinKey[0] = 'b'
	if string(meta.MinKey) != "a" {
		t.Fatalf("expected copy to be independent")
	}
}
