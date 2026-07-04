package engine

import (
	"fmt"
	"strings"
)

func newCommitLogConsensus(opts Options) (commitLogConsensus, error) {
	if opts.CommitLog != nil && opts.CommitLog.Factory != nil {
		consensus, err := opts.CommitLog.Factory.New(opts)
		if err != nil {
			return nil, fmt.Errorf("build commit log from factory: %w", err)
		}
		if consensus == nil {
			return nil, fmt.Errorf("build commit log from factory: nil consensus")
		}
		return consensus, nil
	}
	provider := CommitLogProviderLocal
	if opts.CommitLog != nil {
		if trimmed := strings.TrimSpace(string(opts.CommitLog.Provider)); trimmed != "" {
			provider = CommitLogProvider(trimmed)
		}
	}
	switch provider {
	case CommitLogProviderLocal:
		return newBuiltinCommitLogConsensus(opts, provider)
	case CommitLogProviderEtcdRaft:
		return newBuiltinCommitLogConsensus(opts, provider)
	default:
		return nil, fmt.Errorf("unknown commit log provider %q", provider)
	}
}
