package engine

import (
	"sort"

	"lsmengine/internal/lsm/manifest"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/internal/lsm/tableset"
)

func tableMetaFromSSTable(table sstable.SSTable, level int) metadata.TableMeta {
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

func manifestEntryFromMeta(meta metadata.TableMeta) manifest.Entry {
	return manifest.Entry{
		Path:      meta.Path,
		Level:     meta.Level,
		MinKey:    meta.MinKey,
		MaxKey:    meta.MaxKey,
		SeqMin:    meta.SeqMin,
		SeqMax:    meta.SeqMax,
		SizeBytes: meta.SizeBytes,
	}
}

func (l *LSM) applyTableEdit(add []tableset.Table, remove []metadata.TableMeta, walSeq uint64) error {
	removePaths := make([]string, 0, len(remove))
	for _, meta := range remove {
		removePaths = append(removePaths, meta.Path)
	}
	if l.tables != nil {
		l.tables.Apply(tableset.Edit{Add: add, RemovePath: removePaths})
	}
	addEntries := make([]manifest.Entry, 0, len(add))
	for _, t := range add {
		addEntries = append(addEntries, manifestEntryFromMeta(t.Meta))
	}
	removeSet := make(map[string]struct{}, len(removePaths))
	for _, path := range removePaths {
		removeSet[path] = struct{}{}
	}
	return l.updateManifest(func(m manifest.Manifest) manifest.Manifest {
		m.Tables = mergeManifestTables(m.Tables, addEntries, removeSet)
		if walSeq > 0 {
			m.WALSeq = walSeq
		}
		if l.replicationState != nil {
			m.Replication = l.replicationSnapshot()
		}
		return m
	})
}

func mergeManifestTables(existing []manifest.Entry, add []manifest.Entry, remove map[string]struct{}) []manifest.Entry {
	addMap := make(map[string]manifest.Entry, len(add))
	for _, entry := range add {
		addMap[entry.Path] = entry
	}
	out := make([]manifest.Entry, 0, len(existing)+len(addMap))
	for _, entry := range existing {
		if _, drop := remove[entry.Path]; drop {
			continue
		}
		if _, replace := addMap[entry.Path]; replace {
			continue
		}
		out = append(out, entry)
	}
	for _, entry := range addMap {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SeqMax != out[j].SeqMax {
			return out[i].SeqMax > out[j].SeqMax
		}
		return out[i].Path < out[j].Path
	})
	return out
}
