package config

// PolicySnapshot is a read-only set of tunables the controller can issue to the
// pipeline without touching data path logic.
type PolicySnapshot struct {
	UsePrefetch          bool
	PrefetchBudgetBytes  int
	PrefetchBudgetBlocks int
	PrefetchAsync        bool
	PrefetchQueueDepth   int
	PrefetchWorkers      int
	UsePartitionedIndex  bool
	UsePartitionedFilter bool
	UseMmap              bool
	ReadBufferMaxBytes   int
	ReadBlockMaxBytes    int
	CorruptionPolicy     CorruptionPolicy
}

func SnapshotFromOptions(opts Options, partitionedIndex bool, partitionedFilter bool) PolicySnapshot {
	budgetBytes := opts.PrefetchBudgetBytes
	budgetBlocks := opts.PrefetchBudgetBlocks
	// Backward compat: fold lookahead knobs into the budget so prefetch has a single source of truth.
	if budgetBytes == 0 && opts.PrefetchBytes > 0 {
		budgetBytes = opts.PrefetchBytes
	}
	if budgetBlocks == 0 && opts.PrefetchBlocks > 0 {
		budgetBlocks = opts.PrefetchBlocks
	}
	base := PolicySnapshot{
		UsePrefetch:          budgetBytes > 0 || budgetBlocks > 0 || opts.PrefetchAsync,
		PrefetchBudgetBytes:  budgetBytes,
		PrefetchBudgetBlocks: budgetBlocks,
		PrefetchAsync:        opts.PrefetchAsync,
		PrefetchQueueDepth:   opts.PrefetchQueueDepth,
		PrefetchWorkers:      opts.PrefetchWorkers,
		UsePartitionedIndex:  partitionedIndex,
		UsePartitionedFilter: partitionedFilter,
		UseMmap:              opts.UseMmap,
		ReadBufferMaxBytes:   opts.ReadBufferMaxBytes,
		ReadBlockMaxBytes:    opts.ReadBlockMaxBytes,
		CorruptionPolicy:     opts.CorruptionPolicy,
	}
	if opts.PolicyOverride != nil {
		return *opts.PolicyOverride
	}
	return base
}
