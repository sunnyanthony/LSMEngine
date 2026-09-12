package engine

import "testing"

func TestStateMachineLagDoesNotAdvanceApplyFloor(t *testing.T) {
	store := &LSM{commitLogAppliedIndex: 6}
	for _, index := range []uint64{6, 8} {
		status := store.applyCommitLogRuntimeProgress(CommitLogRuntimeStatus{Index: 9, StateMachineIndex: &index})
		if status.ApplyLag != 3 || status.AppliedIndex != 6 || store.commitLogAppliedIndex != 6 {
			t.Fatalf("raw progress changed: %+v", status)
		}
		if status.StateMachineApplyLag == nil || *status.StateMachineApplyLag != index-6 {
			t.Fatalf("wrong state-machine lag: %+v", status)
		}
	}
	status := store.applyCommitLogRuntimeProgress(CommitLogRuntimeStatus{Index: 9})
	if status.StateMachineApplyLag != nil {
		t.Fatal("unknown provider boundary became zero lag")
	}
}
