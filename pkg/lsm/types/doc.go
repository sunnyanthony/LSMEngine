// Package types defines public data structures shared by the LSM API.
//
// Entry represents a single mutation with a sequence number and tombstone
// flag for deletes. Callers should treat returned Entry values as owned copies.
package types
