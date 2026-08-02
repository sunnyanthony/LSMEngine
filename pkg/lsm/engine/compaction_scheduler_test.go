package engine

import (
	"testing"
	"time"
)

func TestCompactionAdaptiveCheckDelay(t *testing.T) {
	tests := []struct {
		name      string
		base      time.Duration
		adaptive  bool
		l0Tables  int
		threshold int
		want      time.Duration
	}{
		{name: "disabled interval", base: 0, adaptive: true, l0Tables: 4, threshold: 2, want: 0},
		{name: "adaptive disabled", base: 40 * time.Second, adaptive: false, l0Tables: 4, threshold: 2, want: 40 * time.Second},
		{name: "below threshold", base: 40 * time.Second, adaptive: true, l0Tables: 1, threshold: 2, want: 40 * time.Second},
		{name: "pending pressure", base: 40 * time.Second, adaptive: true, l0Tables: 2, threshold: 2, want: 10 * time.Second},
		{name: "high pressure", base: 40 * time.Second, adaptive: true, l0Tables: 4, threshold: 2, want: 5 * time.Second},
		{name: "does not round to zero", base: 2 * time.Millisecond, adaptive: true, l0Tables: 4, threshold: 2, want: time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compactionAdaptiveCheckDelay(tt.base, tt.adaptive, tt.l0Tables, tt.threshold)
			if got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}
