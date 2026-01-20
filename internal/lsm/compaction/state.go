// Compaction state builders.

package compaction

import (
	"sort"

	"lsmengine/internal/lsm/metadata"
)

// StateFromMetas groups table metadata by level and sorts levels ascending.
func StateFromMetas(metas []metadata.TableMeta) State {
	levels := make(map[int][]metadata.TableMeta)
	for _, meta := range metas {
		levels[meta.Level] = append(levels[meta.Level], meta)
	}
	levelList := make([]Level, 0, len(levels))
	for level, tables := range levels {
		levelList = append(levelList, Level{Level: level, Tables: tables})
	}
	sort.Slice(levelList, func(i, j int) bool {
		return levelList[i].Level < levelList[j].Level
	})
	return State{Levels: levelList}
}
