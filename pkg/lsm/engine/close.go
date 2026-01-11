package engine

func (l *LSM) Close() error {
	l.cancel()
	if l.wal != nil {
		_ = l.wal.Close()
	}
	if l.transport != nil {
		_ = l.transport.Close()
	}
	tables := l.tables.Tables()
	for _, table := range tables {
		_ = table.Close()
	}
	if l.logCloser != nil {
		_ = l.logCloser.Close()
	}
	return nil
}
