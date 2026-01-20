// Package lsm exposes the stable public API for the LSM engine.
//
// The facade owns configuration via Options and lifecycle via New. It provides
// Put/Delete/Get, snapshots, and range scans while keeping internal storage
// components encapsulated. Public reads return owned data; internal zero-copy
// views stay behind the API boundary.
package lsm
