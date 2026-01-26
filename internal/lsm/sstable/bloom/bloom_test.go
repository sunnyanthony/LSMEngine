package bloom

import "testing"

func TestFilterAddAndEncode(t *testing.T) {
	filter := NewFilter(10, 8)
	if filter == nil {
		t.Fatalf("expected filter")
	}
	filter.Add([]byte("alpha"))
	if !filter.MayContain([]byte("alpha")) {
		t.Fatalf("expected alpha to be present")
	}

	encoded := filter.Encode()
	decoded := Decode(encoded)
	if decoded == nil || decoded.K() == 0 || decoded.SizeBytes() == 0 {
		t.Fatalf("expected decoded filter")
	}
	if !decoded.MayContain([]byte("alpha")) {
		t.Fatalf("expected decoded filter to contain alpha")
	}
}
