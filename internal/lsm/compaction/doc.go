// Package compaction provides planning and execution for SSTable compaction.
//
// Planners operate on metadata snapshots and produce immutable input sets.
// Runners merge tables into new outputs, while callers apply results via
// manifest and TableSet edits instead of mutating state directly.
package compaction
