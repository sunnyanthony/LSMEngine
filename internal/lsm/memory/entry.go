// Borrowed EntryView type and helpers.

package memory

import "lsmengine/pkg/lsm/types"

// EntryView is a borrowed entry view. Callers must not retain Key/Value slices.
type EntryView struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	Seq       uint64
}

// Entry returns a types.Entry that references the same buffers.
func (v EntryView) Entry() types.Entry {
	return types.Entry{
		Key:       v.Key,
		Value:     v.Value,
		Tombstone: v.Tombstone,
		Seq:       v.Seq,
	}
}

// ToEntry returns an owned entry by copying key/value.
func (v EntryView) ToEntry() types.Entry {
	return types.Entry{
		Key:       append([]byte(nil), v.Key...),
		Value:     append([]byte(nil), v.Value...),
		Tombstone: v.Tombstone,
		Seq:       v.Seq,
	}
}
