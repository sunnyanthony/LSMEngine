package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
)

func TestReplacementCatchupBoundsSlowHTTP(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer slow.Close()
	start := time.Now()
	_, err := waitReplacementNodeCatchup(map[string]string{"node-a": slow.URL}, "node-d", replacementCatchupOptions{Timeout: 30 * time.Millisecond})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("deadline not enforced: elapsed=%s err=%v", time.Since(start), err)
	}
}

func TestReplacementCatchupDoesNotLowerWatermark(t *testing.T) {
	var calls atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		applied := uint64(12)
		if calls.Add(1) > 1 {
			applied = 4
		}
		json.NewEncoder(w).Encode(lsm.ClusterStatus{NodeID: "node-a", CommitLogRuntime: lsm.CommitLogRuntimeStatus{
			Health: "ready", LeaderKnown: true, AppliedIndex: applied,
		}})
	}))
	defer source.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(lsm.ClusterStatus{NodeID: "node-d", CommitLogRuntime: lsm.CommitLogRuntimeStatus{
			Health: "follower", LeaderKnown: true, AppliedIndex: 6,
		}})
	}))
	defer target.Close()
	result, err := waitReplacementNodeCatchup(map[string]string{"node-a": source.URL, "node-d": target.URL}, "node-d", replacementCatchupOptions{Timeout: 450 * time.Millisecond})
	if err == nil || result.RequiredAppliedIndex != 12 || calls.Load() < 2 {
		t.Fatalf("watermark regressed: result=%+v calls=%d err=%v", result, calls.Load(), err)
	}
	_, err = waitReplacementNodeCatchup(map[string]string{"node-d": target.URL}, "node-d", replacementCatchupOptions{Timeout: 30 * time.Millisecond})
	if err == nil {
		t.Fatal("accepted catchup without an existing healthy replica")
	}
}

func TestReplacementCommandPreservesDisabledCatchup(t *testing.T) {
	args := replaceNodeCommandArgs(map[string]string{"node-a": "http://node-a"}, replaceNodeOptions{OldNode: "node-a", NewNode: "node-d"})
	if !strings.Contains(strings.Join(args, " "), "--catchup-timeout 0s") {
		t.Fatalf("disabled catchup not preserved: %v", args)
	}
}

func TestReplacementCatchupUsesStateMachineLagWhenAvailable(t *testing.T) {
	zero, pending := uint64(0), uint64(1)
	for _, tc := range []struct {
		name string
		lag  *uint64
		pass bool
	}{{"configuration-only", &zero, true}, {"pending-mutation", &pending, false}, {"legacy-raw-lag", nil, false}} {
		t.Run(tc.name, func(t *testing.T) {
			node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(lsm.ClusterStatus{CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Health: "follower", LeaderKnown: true, AppliedIndex: 6, Index: 7, ApplyLag: 1,
					StateMachineApplyLag: tc.lag,
				}})
			}))
			defer node.Close()
			_, err := waitReplacementNodeCatchup(map[string]string{"node-a": node.URL, "node-d": node.URL}, "node-d", replacementCatchupOptions{Timeout: 50 * time.Millisecond})
			if (err == nil) != tc.pass {
				t.Fatalf("pass=%v err=%v", tc.pass, err)
			}
		})
	}
}
