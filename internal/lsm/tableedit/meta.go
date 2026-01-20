package tableedit

import (
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
)

// TableMetaFromSSTable builds metadata for a table at the provided level.
func TableMetaFromSSTable(table sstable.SSTable, level int) metadata.TableMeta {
	info := table.Info()
	meta := metadata.TableMeta{
		Path:      table.Path,
		Level:     level,
		MinKey:    info.MinKey,
		MaxKey:    info.MaxKey,
		SeqMin:    info.SeqMin,
		SeqMax:    info.SeqMax,
		SizeBytes: info.SizeBytes,
	}
	if meta.SeqMax == 0 {
		meta.SeqMax = table.Seq
	}
	if meta.SeqMin == 0 {
		meta.SeqMin = table.Seq
	}
	return meta
}
