// Entry type used for key/value mutations.

package types

// Entry represents a user key/value mutation with sequencing for last-write-wins.
type Entry struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	Seq       uint64
}
