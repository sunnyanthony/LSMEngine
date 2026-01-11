package metadata

// TableMeta describes immutable table metadata used by planners and controllers.
type TableMeta struct {
	Path      string
	Level     int
	MinKey    []byte
	MaxKey    []byte
	SeqMin    uint64
	SeqMax    uint64
	SizeBytes uint64
}

// Copy returns a defensive copy of the metadata (slices are copied).
func (m TableMeta) Copy() TableMeta {
	out := m
	if m.MinKey != nil {
		out.MinKey = append([]byte(nil), m.MinKey...)
	}
	if m.MaxKey != nil {
		out.MaxKey = append([]byte(nil), m.MaxKey...)
	}
	return out
}
