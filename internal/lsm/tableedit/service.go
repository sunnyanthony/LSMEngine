// Table edit application and manifest updates.

package tableedit

import (
	"os"
	"sort"

	"lsmengine/internal/lsm/logging"
	"lsmengine/internal/lsm/manifest"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/tableset"
)

// Editor applies table edits and updates the manifest.
type Editor interface {
	Apply(add []tableset.Table, remove []metadata.TableMeta, walSeq uint64) error
}

// Remover abstracts file cleanup for obsolete tables.
type Remover interface {
	Remove(path string) error
}

// Service is the default table-edit implementation.
type Service struct {
	Tables   *tableset.Set
	Manifest manifest.Store
	Logger   logging.Logger
	Remover  Remover
}

// New builds a table edit service.
func New(tables *tableset.Set, store manifest.Store, logger logging.Logger, remover Remover) *Service {
	return &Service{Tables: tables, Manifest: store, Logger: logger, Remover: remover}
}

// Apply installs new tables, removes obsolete ones, and updates the manifest.
func (s *Service) Apply(add []tableset.Table, remove []metadata.TableMeta, walSeq uint64) error {
	if s == nil {
		return nil
	}
	removePaths := make([]string, 0, len(remove))
	for _, meta := range remove {
		removePaths = append(removePaths, meta.Path)
	}
	var removedTables []tableset.Table
	if s.Tables != nil {
		removedTables = s.Tables.Apply(tableset.Edit{Add: add, RemovePath: removePaths})
	}

	addEntries := make([]manifest.Entry, 0, len(add))
	for _, t := range add {
		addEntries = append(addEntries, manifestEntryFromMeta(t.Meta))
	}
	removeSet := make(map[string]struct{}, len(removePaths))
	for _, path := range removePaths {
		removeSet[path] = struct{}{}
	}
	if err := updateManifest(s.Manifest, func(m manifest.Manifest) manifest.Manifest {
		m.Tables = mergeManifestTables(m.Tables, addEntries, removeSet)
		if walSeq > 0 {
			m.WALSeq = walSeq
		}
		return m
	}); err != nil {
		return err
	}

	if len(removedTables) > 0 {
		for _, table := range removedTables {
			if err := table.Handle.Close(); err != nil && s.Logger != nil {
				s.Logger.Printf("table edit: close obsolete %s: %v", table.Meta.Path, err)
			}
		}
		for _, table := range removedTables {
			if err := s.removeFile(table.Meta.Path); err != nil {
				if s.Logger != nil {
					s.Logger.Printf("table edit: remove obsolete %s: %v", table.Meta.Path, err)
				}
			}
		}
	}
	return nil
}

func (s *Service) removeFile(path string) error {
	if s == nil {
		return nil
	}
	if s.Remover != nil {
		return s.Remover.Remove(path)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func updateManifest(store manifest.Store, mutator func(manifest.Manifest) manifest.Manifest) error {
	if store == nil || mutator == nil {
		return nil
	}
	if updater, ok := store.(manifest.UpdateStore); ok {
		return updater.Update(mutator)
	}
	m, err := store.Load()
	if err != nil {
		return err
	}
	m = mutator(m)
	return store.Save(m)
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
