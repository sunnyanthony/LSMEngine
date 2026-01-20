// Package iofs defines minimal IO interfaces shared by WAL and SSTable.
//
// It isolates OS-specific filesystem details behind small read/write/flush
// interfaces to keep higher layers portable.
package iofs
