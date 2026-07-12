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
	if l == nil {
		return fmt.Errorf("nil lsm")
	}
	var snapshot lsmStateSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode lsm state snapshot: %w", err)
	}
	if snapshot.Version != lsmStateSnapshotVersion {
		return fmt.Errorf("unsupported lsm state snapshot version %d", snapshot.Version)
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
