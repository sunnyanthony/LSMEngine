package engine

import (
	"encoding/json"
	"fmt"

	"lsmengine/pkg/lsm/types"
)

const lsmStateSnapshotVersion = 1

type lsmStateSnapshot struct {
	Version               int                `json:"version"`
	Seq                   uint64             `json:"seq"`
	CommitLogAppliedIndex uint64             `json:"commit_log_applied_index"`
	Entries               []types.Entry      `json:"entries,omitempty"`
	Control               *controlPlaneState `json:"control,omitempty"`
}

type lsmStateSnapshotter struct {
	l *LSM
}

func (s lsmStateSnapshotter) CaptureStateSnapshot(index uint64) ([]byte, error) {
	if s.l == nil {
		return nil, fmt.Errorf("nil lsm")
	}
	return s.l.exportStateSnapshotAt(index)
}

func (s lsmStateSnapshotter) ApplyStateSnapshot(index uint64, data []byte) error {
	if s.l == nil {
		return fmt.Errorf("nil lsm")
	}
	return s.l.applyRaftStateSnapshot(index, data)
}

func (l *LSM) exportStateSnapshot() ([]byte, error) {
	return l.exportStateSnapshotAt(0)
}

func (l *LSM) exportStateSnapshotAt(index uint64) ([]byte, error) {
	if l == nil {
		return nil, fmt.Errorf("nil lsm")
	}
	l.commitApplyMu.Lock()
	defer l.commitApplyMu.Unlock()
	if index != 0 && l.commitLogAppliedIndex != index {
		return nil, fmt.Errorf("state snapshot index %d does not match applied index %d", index, l.commitLogAppliedIndex)
	}

	snapshot := lsmStateSnapshot{
		Version:               lsmStateSnapshotVersion,
		Seq:                   l.seq,
		CommitLogAppliedIndex: l.commitLogAppliedIndex,
	}
	if l.control != nil {
		l.control.mu.RLock()
		control := l.control.snapshotStateLocked()
		l.control.mu.RUnlock()
		snapshot.Control = &control
	}
	entries, err := l.snapshotVisibleEntries()
	if err != nil {
		return nil, err
	}
	snapshot.Entries = entries
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal lsm state snapshot: %w", err)
	}
	return data, nil
}

func (l *LSM) applyStateSnapshotToEmpty(data []byte) error {
	return l.applyStateSnapshotToEmptyAt(0, data)
}

func (l *LSM) applyStateSnapshotToEmptyAt(index uint64, data []byte) error {
	snapshot, err := decodeStateSnapshotForRaftIndex(index, data)
	if err != nil {
		return err
	}
	return l.applyDecodedStateSnapshotToEmpty(snapshot)
}

func (l *LSM) applyRaftStateSnapshot(index uint64, data []byte) error {
	snapshot, err := decodeStateSnapshotForRaftIndex(index, data)
	if err != nil {
		return err
	}
	if l == nil {
		return fmt.Errorf("nil lsm")
	}
	l.commitApplyMu.Lock()
	currentApplied := l.commitLogAppliedIndex
	currentSeq := l.seq
	l.commitApplyMu.Unlock()
	if currentApplied == 0 && currentSeq == 0 {
		return l.applyDecodedStateSnapshotToEmpty(snapshot)
	}
	if snapshot.CommitLogAppliedIndex <= currentApplied {
		return nil
	}
	if snapshot.Seq < currentSeq {
		return fmt.Errorf("state snapshot seq %d is behind local seq %d", snapshot.Seq, currentSeq)
	}
	return l.resetToStateSnapshot(snapshot)
}

func decodeStateSnapshotForRaftIndex(index uint64, data []byte) (lsmStateSnapshot, error) {
	var snapshot lsmStateSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return lsmStateSnapshot{}, fmt.Errorf("decode lsm state snapshot: %w", err)
	}
	if snapshot.Version != lsmStateSnapshotVersion {
		return lsmStateSnapshot{}, fmt.Errorf("unsupported lsm state snapshot version %d", snapshot.Version)
	}
	if index != 0 && snapshot.CommitLogAppliedIndex != index {
		return lsmStateSnapshot{}, fmt.Errorf("state snapshot applied index %d does not match raft snapshot index %d", snapshot.CommitLogAppliedIndex, index)
	}
	return snapshot, nil
}

func (l *LSM) applyDecodedStateSnapshotToEmpty(snapshot lsmStateSnapshot) error {
	if l == nil {
		return fmt.Errorf("nil lsm")
	}
	l.commitApplyMu.Lock()
	defer l.commitApplyMu.Unlock()
	if l.seq != 0 || l.commitLogAppliedIndex != 0 {
		return fmt.Errorf("state snapshot restore requires an empty engine")
	}
	if l.control != nil {
		l.control.mu.RLock()
		controlApplied := l.control.commitLogAppliedIndex
		controlRevision := l.control.revision
		l.control.mu.RUnlock()
		if controlApplied != 0 || controlRevision != 0 {
			return fmt.Errorf("state snapshot restore requires empty control state")
		}
	}

	if snapshot.Control != nil && l.control != nil {
		l.control.mu.Lock()
		if err := l.control.applyState(*snapshot.Control); err != nil {
			l.control.mu.Unlock()
			return fmt.Errorf("apply control state snapshot: %w", err)
		}
		if err := l.control.saveLocked(); err != nil {
			l.control.mu.Unlock()
			return fmt.Errorf("save control state snapshot: %w", err)
		}
		l.control.mu.Unlock()
	}

	for _, entry := range snapshot.Entries {
		if entry.Tombstone {
			continue
		}
		if entry.Seq == 0 {
			return fmt.Errorf("state snapshot entry has zero sequence")
		}
		if _, err := l.writer.appendPutToLocalStore(entry.Key, entry.Value, entry.Seq); err != nil {
			return fmt.Errorf("apply state snapshot entry: %w", err)
		}
	}
	l.observeCommittedSeq(snapshot.Seq)
	l.markCommitLogAppliedLocked(snapshot.CommitLogAppliedIndex)
	return nil
}

func (l *LSM) resetToStateSnapshot(snapshot lsmStateSnapshot) error {
	if l == nil {
		return fmt.Errorf("nil lsm")
	}
	l.commitApplyMu.Lock()
	defer l.commitApplyMu.Unlock()
	if snapshot.CommitLogAppliedIndex <= l.commitLogAppliedIndex {
		return nil
	}
	if snapshot.Seq < l.seq {
		return fmt.Errorf("state snapshot seq %d is behind local seq %d", snapshot.Seq, l.seq)
	}

	currentEntries, err := l.snapshotVisibleEntries()
	if err != nil {
		return fmt.Errorf("snapshot current state before reset: %w", err)
	}
	nextKeys := make(map[string]struct{}, len(snapshot.Entries))
	if snapshot.Control != nil && l.control != nil {
		l.control.mu.Lock()
		if err := l.control.applyState(*snapshot.Control); err != nil {
			l.control.mu.Unlock()
			return fmt.Errorf("apply control state snapshot: %w", err)
		}
		if err := l.control.saveLocked(); err != nil {
			l.control.mu.Unlock()
			return fmt.Errorf("save control state snapshot: %w", err)
		}
		l.control.mu.Unlock()
	}
	for _, entry := range snapshot.Entries {
		if entry.Tombstone {
			continue
		}
		if entry.Seq == 0 {
			return fmt.Errorf("state snapshot entry has zero sequence")
		}
		nextKeys[string(entry.Key)] = struct{}{}
		if _, err := l.writer.appendPutToLocalStore(entry.Key, entry.Value, entry.Seq); err != nil {
			return fmt.Errorf("apply state snapshot entry: %w", err)
		}
	}
	for _, entry := range currentEntries {
		if _, ok := nextKeys[string(entry.Key)]; ok {
			continue
		}
		if _, err := l.writer.appendDeleteToLocalStore(entry.Key, snapshot.Seq); err != nil {
			return fmt.Errorf("delete state snapshot stale entry: %w", err)
		}
	}
	l.observeCommittedSeq(snapshot.Seq)
	l.markCommitLogAppliedLocked(snapshot.CommitLogAppliedIndex)
	return nil
}

func (l *LSM) snapshotVisibleEntries() ([]types.Entry, error) {
	snap := l.Snapshot()
	if snap == nil {
		return nil, fmt.Errorf("snapshot unavailable")
	}
	defer snap.Close()

	iter := snap.Range(nil, nil)
	var entries []types.Entry
	for iter.Next() {
		entry := iter.Entry()
		if entry.Tombstone {
			continue
		}
		entries = append(entries, types.Entry{
			Key:   append([]byte(nil), entry.Key...),
			Value: append([]byte(nil), entry.Value...),
			Seq:   entry.Seq,
		})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}
