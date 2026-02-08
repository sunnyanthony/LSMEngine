// Package engine wires internal components into a running LSM instance.
//
// This package is lower-level than pkg/lsm and is intended for advanced use.
// It owns orchestration (WAL, memtables, SSTables, manifest, compaction) but
// keeps data invariants in the internal packages. It also exposes a plugin
// extension point for custom behaviors (document/column/vector style adapters).
// Prefer pkg/lsm unless you need direct access to engine options.
package engine
