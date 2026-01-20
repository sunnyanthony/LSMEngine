package bootstrap

import (
	"lsmengine/internal/lsm/manifest"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
	sstableconfig "lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/tableset"
)

// LoadManifestTables loads the manifest and returns the resolved table handles.
func LoadManifestTables(store manifest.Store, opts sstableconfig.Options) (manifest.Manifest, []tableset.Table, error) {
	if store == nil {
		return manifest.Manifest{}, nil, nil
	}
	m, err := store.Load()
	if err != nil {
		return manifest.Manifest{}, nil, err
	}
	if len(m.Tables) == 0 {
		return m, nil, nil
	}
	tables := make([]tableset.Table, 0, len(m.Tables))
	for _, t := range m.Tables {
		table, err := sstable.LoadSSTable(t.Path, opts)
		if err != nil {
			return manifest.Manifest{}, nil, err
		}
		meta := metadata.TableMeta{
			Path:      t.Path,
			Level:     t.Level,
			MinKey:    t.MinKey,
			MaxKey:    t.MaxKey,
			SeqMin:    t.SeqMin,
			SeqMax:    t.SeqMax,
			SizeBytes: t.SizeBytes,
		}
		info := table.Info()
		if meta.SeqMax == 0 {
			meta.SeqMax = info.SeqMax
		}
		if meta.SeqMin == 0 {
			meta.SeqMin = info.SeqMin
		}
		if len(meta.MinKey) == 0 {
			meta.MinKey = info.MinKey
		}
		if len(meta.MaxKey) == 0 {
			meta.MaxKey = info.MaxKey
		}
		if meta.SizeBytes == 0 {
			meta.SizeBytes = info.SizeBytes
		}
		tables = append(tables, tableset.Table{Meta: meta, Handle: table})
	}
	return m, tables, nil
}
