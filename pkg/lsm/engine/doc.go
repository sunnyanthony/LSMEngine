// Package engine wires internal components into a running LSM instance.
//
// This package is lower-level than pkg/lsm and is intended for advanced use.
// It owns orchestration (WAL, memtables, SSTables, manifest, compaction) but
// keeps data invariants in the internal packages. Prefer pkg/lsm unless you
// need direct access to engine options.
package engine
