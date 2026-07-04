package lsm

// CDCProvider exposes node-local retained per-shard change streams.
type CDCProvider interface {
	ReadCDCEvents(shardID string, offset uint64, limit int) (CDCReadResult, error)
}
