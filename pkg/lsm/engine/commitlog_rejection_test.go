package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"

	internalcommitlog "lsmengine/internal/lsm/commitlog"
	"lsmengine/pkg/lsm/errs"
)

func TestCommitLogRejectionRequiresDirectAdmissionError(t *testing.T) {
	for _, err := range []error{
		fmt.Errorf("delivery: %w", internalcommitlog.ErrNotLeader),
		fmt.Errorf("delivery: %w", &commitLogNotLeader{}),
		errs.ErrNotLeader,
		context.DeadlineExceeded,
	} {
		if isPreProposalRejection(translateCommitLogError(err)) {
			t.Fatalf("ambiguous error classified as safe rejection: %v", err)
		}
	}
	if !isPreProposalRejection(translateCommitLogError(internalcommitlog.ErrNotLeader)) {
		t.Fatal("direct admission rejection was not recognized")
	}
}

func TestTransportRejectionCauseStillLatchesMutations(t *testing.T) {
	for _, kind := range []string{"data", "control"} {
		for _, failure := range []string{"captured-rejection", "timeout", "transport", "public-not-leader"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				var captured error
				stores, _, transport := newApplyPairBeforeElection(t, func(store *LSM) {
					captured = store.Put([]byte("key"), []byte("value"))
					if !errors.Is(captured, errs.ErrNotLeader) {
						t.Fatalf("capture rejection: %v", captured)
					}
				})
				var injected error
				switch failure {
				case "captured-rejection":
					injected = captured
				case "timeout":
					injected = context.DeadlineExceeded
				case "transport":
					injected = errors.New("delivery failed")
				case "public-not-leader":
					injected = errs.ErrNotLeader
				}
				transport.mu.Lock()
				transport.failErr = injected
				transport.mu.Unlock()
				mutate := func() error {
					if kind == "data" {
						return stores[0].Put([]byte("key"), []byte("value"))
					}
					return stores[0].TransferLeaderWithOptions("shared", "node-a", ControlWriteOptions{OperationID: "ambiguous"})
				}
				first := mutate()
				if first == nil || !errors.Is(first, injected) {
					t.Fatalf("missing delivery failure: %v", first)
				}
				transport.mu.Lock()
				transport.failErr = nil
				transport.mu.Unlock()
				if second := mutate(); second != first {
					t.Fatalf("ambiguous failure did not latch: first=%v second=%v", first, second)
				}
			})
		}
	}
}
