package commitlog

import (
	"fmt"
	"strings"
)

func NewBuiltin(cfg Config) (Consensus, error) {
	provider := ProviderLocal
	if trimmed := strings.TrimSpace(string(cfg.Provider)); trimmed != "" {
		provider = Provider(trimmed)
	}

	switch provider {
	case ProviderLocal:
		return newLocalConsensus(), nil
	case ProviderEtcdRaft:
		return newEtcdRaftConsensus(cfg)
	default:
		return nil, fmt.Errorf("unknown commit log provider %q", provider)
	}
}
