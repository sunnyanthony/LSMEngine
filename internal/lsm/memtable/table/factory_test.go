package table

import "testing"

func TestFactoryForKind(t *testing.T) {
	if _, err := FactoryForKind("unknown", 1, 0, 0); err == nil {
		t.Fatalf("expected error for unknown kind")
	}
	factory, err := FactoryForKind("map", 1, 0, 0)
	if err != nil {
		t.Fatalf("factory map: %v", err)
	}
	if factory() == nil {
		t.Fatalf("expected table instance")
	}
}
