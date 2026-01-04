package lsm

func (l *LSM) Close() error {
	l.cancel()
	if l.wal != nil {
		_ = l.wal.Close()
	}
	if l.logCloser != nil {
		_ = l.logCloser.Close()
	}
	return nil
}
