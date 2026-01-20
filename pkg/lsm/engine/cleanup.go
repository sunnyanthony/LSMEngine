// Cleanup helpers for obsolete tables.

package engine

import (
	"os"

	"lsmengine/internal/lsm/tableset"
)

func (l *LSM) cleanupTables(tables []tableset.Table) {
	if l == nil || len(tables) == 0 {
		return
	}
	for _, table := range tables {
		if err := table.Handle.Close(); err != nil {
			if l.logger != nil {
				l.logger.Printf("table cleanup: close obsolete %s: %v", table.Meta.Path, err)
			}
		}
	}
	for _, table := range tables {
		if err := l.removeFile(table.Meta.Path); err != nil {
			if l.logger != nil {
				l.logger.Printf("table cleanup: remove obsolete %s: %v", table.Meta.Path, err)
			}
		}
	}
}

func (l *LSM) removeFile(path string) error {
	if l == nil {
		return nil
	}
	if l.remover != nil {
		return l.remover.Remove(path)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
