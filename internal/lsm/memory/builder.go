// Entry builder and copy policy at the API boundary.

package memory

import "lsmengine/pkg/lsm/types"

// Allocator provides arena-style allocation for copy-on-write buffers.
type Allocator interface {
	AllocCopy(src []byte) []byte
}

// CopyBytes copies src into the allocator when provided, otherwise heap-allocates.
func CopyBytes(alloc Allocator, src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	recordEntryCopy(len(src))
	if alloc != nil {
		if dst := alloc.AllocCopy(src); dst != nil {
			return dst
		}
	}
	return append([]byte(nil), src...)
}

// CopyEntry copies key/value into allocator-backed memory when available.
func CopyEntry(alloc Allocator, entry types.Entry) types.Entry {
	entry.Key = CopyBytes(alloc, entry.Key)
	entry.Value = CopyBytes(alloc, entry.Value)
	return entry
}

// EntryBuilder owns copy rules for entries at the API boundary.
type EntryBuilder struct {
	copyFn func(types.Entry) types.Entry
}

// NewEntryBuilder creates a builder with the provided copy function.
func NewEntryBuilder(copyFn func(types.Entry) types.Entry) EntryBuilder {
	return EntryBuilder{copyFn: copyFn}
}

// FromEntry copies the entry using the builder rules.
func (b EntryBuilder) FromEntry(entry types.Entry) types.Entry {
	if b.copyFn != nil {
		return b.copyFn(entry)
	}
	return CopyEntry(nil, entry)
}

// FromView converts a borrowed view into an owned entry.
func (b EntryBuilder) FromView(view EntryView) types.Entry {
	return b.FromEntry(view.Entry())
}

// Build constructs an entry from raw fields and applies copy rules.
func (b EntryBuilder) Build(key, value []byte, tombstone bool, seq uint64) types.Entry {
	return b.FromEntry(types.Entry{
		Key:       key,
		Value:     value,
		Tombstone: tombstone,
		Seq:       seq,
	})
}
