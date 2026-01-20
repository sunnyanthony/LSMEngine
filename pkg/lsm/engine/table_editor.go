// Table edit adapter for engine services.

package engine

import "lsmengine/internal/lsm/tableedit"

func (l *LSM) tableEditor() tableedit.Editor {
	if l == nil {
		return nil
	}
	if l.tableEdits == nil {
		l.tableEdits = tableedit.New(l.tables, l.manifest, l.logger)
	}
	return l.tableEdits
}
