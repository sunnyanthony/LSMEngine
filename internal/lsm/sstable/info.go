package sstable

// Info summarizes immutable on-disk metadata for a table.
type Info struct {
	MinKey     []byte
	MaxKey     []byte
	SeqMin     uint64
	SeqMax     uint64
	SizeBytes  uint64
	EntryCount uint64
}

// Info returns immutable metadata for the table.
func (s SSTable) Info() Info {
	if s.reader == nil {
		return Info{SeqMax: s.Seq}
	}
	m := s.reader.meta
	return Info{
		MinKey:     append([]byte(nil), m.MinKey...),
		MaxKey:     append([]byte(nil), m.MaxKey...),
		SeqMin:     m.SeqMin,
		SeqMax:     m.SeqMax,
		EntryCount: m.EntryCount,
		SizeBytes:  uint64(s.reader.size),
	}
}
