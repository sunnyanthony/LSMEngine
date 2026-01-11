package engine

// TermProvider supplies the current term/epoch for outgoing replication.
type TermProvider interface {
	Term() uint64
}

// LeaderProvider reports whether the local node can accept writes.
// Implement alongside TermProvider to enable leader gating.
type LeaderProvider interface {
	IsLeader() bool
}
