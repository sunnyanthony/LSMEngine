package main

import (
	"testing"

	"lsmengine/pkg/lsm"
)

func TestParseWriteConsistencyDefault(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    lsm.WriteConsistency
		wantErr bool
	}{
		{name: "empty defaults to accepted", input: "", want: lsm.WriteConsistencyAccepted},
		{name: "accepted", input: "accepted", want: lsm.WriteConsistencyAccepted},
		{name: "local committed", input: "local_committed", want: lsm.WriteConsistencyLocalCommitted},
		{name: "invalid", input: "eventual", wantErr: true},
		{name: "linearizable rejected", input: "linearizable", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseWriteConsistencyDefault(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}
