// Package memory standardizes entry ownership and buffer reuse across layers.
//
// The entry builder performs the single-copy policy for API inputs; pools are
// used only for short-lived IO buffers.
package memory
