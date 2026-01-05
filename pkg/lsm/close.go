package lsm

import "lsmengine/internal/lsm/sstable"

func (l *LSM) Close() error {
	l.cancel()
	if l.wal != nil {
		_ = l.wal.Close()
	}
	l.tablesMu.RLock()
	tables := append([]sstable.SSTable(nil), l.tables...)
	l.tablesMu.RUnlock()
	for _, table := range tables {
		_ = table.Close()
	}
	if l.logCloser != nil {
		_ = l.logCloser.Close()
	}
	return nil
}
