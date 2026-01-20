// Package manifest stores durable table metadata with log + checkpoint support.
//
// The engine updates the manifest atomically; readers use it to rebuild state.
package manifest
