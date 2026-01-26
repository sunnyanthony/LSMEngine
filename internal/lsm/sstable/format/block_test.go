package format

import (
	"bytes"
	"testing"

	"lsmengine/pkg/lsm/types"
)

func TestBlockEncodeDecodeFind(t *testing.T) {
	builder := NewBuilder(2, false, 0, 0)
	entries := []types.Entry{
		{Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Key: []byte("b"), Value: []byte("2"), Seq: 2},
		{Key: []byte("c"), Value: []byte("3"), Seq: 3},
	}
	for _, entry := range entries {
		builder.Add(entry)
	}
	payload := builder.Finish()
	if len(payload) == 0 {
		t.Fatalf("expected payload")
	}

	block, err := Decode(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	got, ok, err := block.Find([]byte("b"))
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if !ok || !bytes.Equal(got.Value, []byte("2")) {
		t.Fatalf("expected b=2, ok=%v val=%q", ok, got.Value)
	}

	_, ok, err = block.Find([]byte("d"))
	if err != nil {
		t.Fatalf("find d: %v", err)
	}
	if ok {
		t.Fatalf("expected d missing")
	}
}
