// Package sstable reads and writes immutable SSTable files.
//
// It provides block/index encoding, bloom filters, caching, and prefetching.
// Readers return views internally; public APIs return owned copies.
package sstable
