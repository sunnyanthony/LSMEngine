// Package wal implements write-ahead logging for durability.
//
// It appends owned entries, batches/fsyncs as configured, and replays records
// on startup. Codec and segment helpers live in subpackages.
package wal
