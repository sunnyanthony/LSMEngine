package arena

import "testing"

func TestArenaAllocStats(t *testing.T) {
	a := NewArenaWithOptions(Options{BlockSize: 16})
	if got := a.Alloc(8); got == nil {
		t.Fatalf("expected allocation")
	}
	stats := a.Stats()
	if stats.Blocks != 1 {
		t.Fatalf("expected 1 block, got %d", stats.Blocks)
	}
	if stats.UsedBytes != 8 {
		t.Fatalf("expected used=8, got %d", stats.UsedBytes)
	}
	if stats.ReservedBytes != 16 {
		t.Fatalf("expected reserved=16, got %d", stats.ReservedBytes)
	}

	if got := a.Alloc(20); got == nil {
		t.Fatalf("expected large allocation")
	}
	stats = a.Stats()
	if stats.Blocks != 2 {
		t.Fatalf("expected 2 blocks, got %d", stats.Blocks)
	}
	if stats.UsedBytes != 28 {
		t.Fatalf("expected used=28, got %d", stats.UsedBytes)
	}
	if stats.ReservedBytes != 36 {
		t.Fatalf("expected reserved=36, got %d", stats.ReservedBytes)
	}
}

func TestArenaResetKeepsBlock(t *testing.T) {
	a := NewArenaWithOptions(Options{BlockSize: 16})
	a.Alloc(8)
	a.Alloc(20)
	a.Reset()
	stats := a.Stats()
	if stats.Blocks != 1 {
		t.Fatalf("expected 1 block after reset, got %d", stats.Blocks)
	}
	if stats.UsedBytes != 0 {
		t.Fatalf("expected used=0 after reset, got %d", stats.UsedBytes)
	}
	if stats.ReservedBytes != 16 {
		t.Fatalf("expected reserved=16 after reset, got %d", stats.ReservedBytes)
	}
}

func TestArenaHardLimit(t *testing.T) {
	a := NewArenaWithOptions(Options{
		BlockSize:      16,
		HardLimitBytes: 8,
	})
	if got := a.Alloc(8); got == nil {
		t.Fatalf("expected allocation within hard limit")
	}
	if got := a.Alloc(1); got != nil {
		t.Fatalf("expected allocation denied by hard limit")
	}
	stats := a.Stats()
	if stats.HardDeniedAllocs != 1 {
		t.Fatalf("expected hard denied=1, got %d", stats.HardDeniedAllocs)
	}
}

func TestArenaSoftLimitExceeded(t *testing.T) {
	a := NewArenaWithOptions(Options{
		BlockSize:      16,
		SoftLimitBytes: 8,
	})
	if a.SoftLimitExceeded() {
		t.Fatalf("expected soft limit not exceeded")
	}
	a.Alloc(8)
	if !a.SoftLimitExceeded() {
		t.Fatalf("expected soft limit exceeded")
	}
}
