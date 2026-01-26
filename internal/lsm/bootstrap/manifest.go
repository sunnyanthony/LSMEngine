package bootstrap

import (
	"errors"
	"fmt"
	"path/filepath"

	"lsmengine/internal/lsm/manifest"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
	sstableconfig "lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/tableedit"
	"lsmengine/internal/lsm/tableset"
	"lsmengine/pkg/lsm/errs"
)

// LoadManifestTables loads the manifest and returns the resolved table handles.
func LoadManifestTables(store manifest.Store, opts sstableconfig.Options) (manifest.Manifest, []tableset.Table, error) {
	if store == nil {
		return manifest.Manifest{}, nil, nil
	}
	m, err := store.Load()
	dropCorrupt := shouldDropCorrupt(opts)
	if err == nil && len(m.Tables) > 0 {
		tables, loadErr := loadTablesFromManifest(m, opts)
		if loadErr == nil {
			return m, tables, nil
		}
		if dropCorrupt && isCorruptionErr(loadErr) {
			err = nil
		} else {
			err = loadErr
		}
	}

	paths, listErr := listSSTablePaths(opts.Dir)
	if listErr != nil {
		if err != nil {
			return manifest.Manifest{}, nil, err
		}
		return manifest.Manifest{}, nil, listErr
	}
	if len(paths) == 0 {
		if err != nil {
			// No manifest and no SSTables; allow WAL-only recovery.
			return manifest.Manifest{}, nil, nil
		}
		return m, nil, nil
	}

	if hook := currentHooks(); hook != nil && hook.BeforeFallbackScan != nil {
		hook.BeforeFallbackScan()
	}
	rebuilt, tables, scanErr := scanSSTablePaths(paths, opts)
	if hook := currentHooks(); hook != nil && hook.AfterFallbackScan != nil {
		hook.AfterFallbackScan(rebuilt, scanErr)
	}
	if scanErr != nil {
		if dropCorrupt && isCorruptionErr(scanErr) {
			return m, nil, nil
		}
		return manifest.Manifest{}, nil, scanErr
	}
	if len(tables) == 0 {
		if err != nil {
			if dropCorrupt && isCorruptionErr(err) {
				return m, nil, nil
			}
			return manifest.Manifest{}, nil, err
		}
		return m, nil, nil
	}
	if hook := currentHooks(); hook != nil && hook.BeforeManifestSave != nil {
		hook.BeforeManifestSave(rebuilt)
	}
	saveErr := store.Save(rebuilt)
	if hook := currentHooks(); hook != nil && hook.AfterManifestSave != nil {
		hook.AfterManifestSave(rebuilt, saveErr)
	}
	if saveErr != nil {
		if cerr := closeTables(tables); cerr != nil {
			return manifest.Manifest{}, nil, errors.Join(saveErr, cerr)
		}
		return manifest.Manifest{}, nil, saveErr
	}
	return rebuilt, tables, nil
}

func loadTablesFromManifest(m manifest.Manifest, opts sstableconfig.Options) ([]tableset.Table, error) {
	dropCorrupt := shouldDropCorrupt(opts)
	tables := make([]tableset.Table, 0, len(m.Tables))
	for _, t := range m.Tables {
		table, err := sstable.LoadSSTable(t.Path, opts)
		if err != nil {
			if dropCorrupt && isCorruptionErr(err) {
				continue
			}
			if cerr := closeTables(tables); cerr != nil {
				return nil, errors.Join(err, cerr)
			}
			return nil, err
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
	return tables, nil
}

func listSSTablePaths(dir string) ([]string, error) {
	if dir == "" {
		return nil, nil
	}
	return filepath.Glob(filepath.Join(dir, "sstable-*.sst"))
}

func scanSSTablePaths(paths []string, opts sstableconfig.Options) (manifest.Manifest, []tableset.Table, error) {
	if len(paths) == 0 {
		return manifest.Manifest{}, nil, nil
	}
	dropCorrupt := shouldDropCorrupt(opts)
	tables := make([]tableset.Table, 0, len(paths))
	entries := make([]manifest.Entry, 0, len(paths))
	var maxSeq uint64
	for _, path := range paths {
		table, err := sstable.LoadSSTable(path, opts)
		if err != nil {
			if dropCorrupt && isCorruptionErr(err) {
				continue
			}
			if cerr := closeTables(tables); cerr != nil {
				return manifest.Manifest{}, nil, errors.Join(err, cerr)
			}
			return manifest.Manifest{}, nil, err
		}
		meta := tableedit.TableMetaFromSSTable(table, 0)
		if meta.SeqMax > maxSeq {
			maxSeq = meta.SeqMax
		}
		tables = append(tables, tableset.Table{Meta: meta, Handle: table})
		entries = append(entries, manifest.Entry{
			Path:      meta.Path,
			Level:     meta.Level,
			MinKey:    meta.MinKey,
			MaxKey:    meta.MaxKey,
			SeqMin:    meta.SeqMin,
			SeqMax:    meta.SeqMax,
			SizeBytes: meta.SizeBytes,
		})
	}
	return manifest.Manifest{WALSeq: maxSeq, Tables: entries}, tables, nil
}

func closeTables(tables []tableset.Table) error {
	var errOut error
	for _, t := range tables {
		if err := t.Handle.Close(); err != nil {
			errOut = errors.Join(errOut, fmt.Errorf("close table %s: %w", t.Meta.Path, err))
		}
	}
	return errOut
}

func shouldDropCorrupt(opts sstableconfig.Options) bool {
	if opts.CorruptionPolicy == sstableconfig.CorruptionDropTable {
		return true
	}
	if opts.PolicyOverride != nil && opts.PolicyOverride.CorruptionPolicy == sstableconfig.CorruptionDropTable {
		return true
	}
	return false
}

func isCorruptionErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errs.ErrSSTableBadMagic) ||
		errors.Is(err, errs.ErrSSTableBadFooter) ||
		errors.Is(err, errs.ErrSSTableBadBlock) ||
		errors.Is(err, errs.ErrSSTableBadMeta) ||
		errors.Is(err, errs.ErrSSTableBadIndex) ||
		errors.Is(err, errs.ErrSSTableUnknownCompression)
}
