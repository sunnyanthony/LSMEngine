package engine

import (
	"context"
	"time"

	"lsmengine/internal/lsm/manifest"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/transport"
	"lsmengine/pkg/lsm/types"
)

const defaultReplicationQueueDepth = 256

type replicationItem struct {
	entry types.Entry
	term  uint64
}

func (l *LSM) startReplication(ctx context.Context, opts Options) error {
	if opts.Transport == nil {
		return nil
	}
	l.transport = opts.Transport
	if opts.NodeID == "" {
		l.nodeID = "local"
	} else {
		l.nodeID = opts.NodeID
	}
	if opts.NodeTerm == 0 {
		l.nodeTerm = 1
	} else {
		l.nodeTerm = opts.NodeTerm
	}
	queueDepth := opts.ReplicationQueueDepth
	if queueDepth <= 0 {
		queueDepth = defaultReplicationQueueDepth
	}
	batchMax := opts.ReplicationBatchMax
	if batchMax <= 0 {
		batchMax = 1
	}
	l.replicationCh = make(chan replicationItem, queueDepth)
	l.replicationBatchMax = batchMax
	l.replicationFlush = opts.ReplicationFlushInterval
	if l.replicationState == nil {
		l.replicationState = make(map[string]manifest.ReplicationState)
	}

	go l.replicationPublisher(ctx)
	if err := l.transport.Subscribe(ctx, l.handleReplication); err != nil {
		return err
	}
	return nil
}

func (l *LSM) enqueueReplication(entry types.Entry, term uint64) {
	if l.transport == nil || l.replicationCh == nil {
		return
	}
	l.replicationCh <- replicationItem{entry: cloneEntry(entry), term: term}
}

func (l *LSM) replicationPublisher(ctx context.Context) {
	var batch []types.Entry
	var batchTerm uint64
	var timer *time.Timer
	if l.replicationFlush > 0 {
		timer = time.NewTimer(l.replicationFlush)
	}

	flush := func() {
		if len(batch) == 0 {
			return
		}
		msg := transport.Message{
			Source:  l.nodeID,
			Term:    batchTerm,
			Entries: batch,
		}
		if err := l.transport.Publish(ctx, msg); err != nil && l.logger != nil {
			l.logger.Printf("replication publish: %v", err)
		}
		batch = nil
		batchTerm = 0
	}
	resetTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(l.replicationFlush)
	}
	for {
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case item := <-l.replicationCh:
			if item.term == 0 {
				item.term = 1
			}
			if batchTerm == 0 {
				batchTerm = item.term
			}
			if batchTerm != item.term && len(batch) > 0 {
				flush()
				batch = batch[:0]
				batchTerm = item.term
			}
			batch = append(batch, item.entry)
			if l.replicationBatchMax > 0 && len(batch) >= l.replicationBatchMax {
				flush()
			}
			resetTimer()
		case <-func() <-chan time.Time {
			if timer == nil {
				return nil
			}
			return timer.C
		}():
			flush()
			resetTimer()
		}
	}
}

func (l *LSM) handleReplication(msg transport.Message) error {
	if msg.Source != "" && msg.Source == l.nodeID {
		return nil
	}
	if len(msg.Entries) == 0 {
		return nil
	}
	source := msg.Source
	if source == "" {
		source = "unknown"
	}
	term := msg.Term
	if term == 0 {
		term = 1
	}
	if !l.ensureReplicationTerm(source, term) {
		if l.logger != nil {
			l.logger.Printf("replication drop stale term source=%s term=%d", source, term)
		}
		return nil
	}
	mem := l.activeMem()
	changed := false
	for _, entry := range msg.Entries {
		if len(entry.Key) == 0 {
			return errs.ErrWALEmptyKey
		}
		if len(entry.Value) == 0 && !entry.Tombstone {
			return errs.ErrWALEmptyValue
		}
		if !l.canApplyReplication(source, term, entry.Seq) {
			continue
		}
		l.bumpSeq(entry.Seq)
		owned := l.prepareEntry(mem, entry)
		if err := l.wal.AppendOwned(owned); err != nil {
			return err
		}
		l.applyEntryOwned(mem, owned)
		if l.markReplicationApplied(source, term, entry.Seq) {
			changed = true
		}
		if mem.Size() >= l.mtLimit {
			l.triggerFlush()
			mem = l.activeMem()
		}
	}
	if changed {
		l.persistReplicationState()
	}
	return nil
}

func cloneEntry(entry types.Entry) types.Entry {
	out := types.Entry{
		Seq:       entry.Seq,
		Tombstone: entry.Tombstone,
	}
	if len(entry.Key) > 0 {
		out.Key = append([]byte(nil), entry.Key...)
	}
	if len(entry.Value) > 0 {
		out.Value = append([]byte(nil), entry.Value...)
	}
	return out
}

func (l *LSM) ensureReplicationTerm(source string, term uint64) bool {
	l.replicationMu.Lock()
	defer l.replicationMu.Unlock()
	state := l.replicationState[source]
	if term < state.Term {
		return false
	}
	if term > state.Term {
		state.Term = term
		state.Seq = 0
		l.replicationState[source] = state
	}
	return true
}

func (l *LSM) canApplyReplication(source string, term, seq uint64) bool {
	l.replicationMu.Lock()
	defer l.replicationMu.Unlock()
	state := l.replicationState[source]
	if term < state.Term {
		return false
	}
	if term > state.Term {
		state.Term = term
		state.Seq = 0
		l.replicationState[source] = state
	}
	return seq > state.Seq
}

func (l *LSM) markReplicationApplied(source string, term, seq uint64) bool {
	l.replicationMu.Lock()
	defer l.replicationMu.Unlock()
	state := l.replicationState[source]
	if term < state.Term {
		return false
	}
	if term > state.Term {
		state.Term = term
		state.Seq = 0
	}
	if seq <= state.Seq {
		return false
	}
	state.Term = term
	state.Seq = seq
	l.replicationState[source] = state
	return true
}

func (l *LSM) persistReplicationState() {
	state := l.replicationSnapshot()
	if err := l.updateManifest(func(m manifest.Manifest) manifest.Manifest {
		m.Replication = state
		return m
	}); err != nil {
		if l.logger != nil {
			l.logger.Printf("replication manifest update: %v", err)
		}
	}
}

func (l *LSM) replicationSnapshot() map[string]manifest.ReplicationState {
	l.replicationMu.Lock()
	defer l.replicationMu.Unlock()
	out := make(map[string]manifest.ReplicationState, len(l.replicationState))
	for k, v := range l.replicationState {
		out[k] = v
	}
	return out
}

func copyReplicationState(in map[string]manifest.ReplicationState) map[string]manifest.ReplicationState {
	if len(in) == 0 {
		return make(map[string]manifest.ReplicationState)
	}
	out := make(map[string]manifest.ReplicationState, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
