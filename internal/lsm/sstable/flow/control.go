package flow

import (
	"context"
	"io"

	"lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/sstable/format"
	"lsmengine/internal/lsm/sstable/storage"
)

type PrefetchBudget struct {
	bytes  int
	blocks int
}

func NewPrefetchBudget(policy config.PolicySnapshot) *PrefetchBudget {
	if policy.PrefetchBudgetBytes > 0 {
		return &PrefetchBudget{bytes: policy.PrefetchBudgetBytes}
	}
	if policy.PrefetchBudgetBlocks > 0 {
		return &PrefetchBudget{blocks: policy.PrefetchBudgetBlocks}
	}
	return nil
}

// ReadBlockPayload is shared between controller and nodes.
func ReadBlockPayload(ctx context.Context, source storage.BlockSource, desc storage.BlockDescriptor, errBad error) ([]byte, error) {
	view, err := source.Read(ctx, desc, storage.ReadHint{})
	if err != nil {
		if err == io.EOF {
			return nil, errBad
		}
		return nil, err
	}
	if view.Release != nil {
		defer view.Release()
	}
	payload, err := format.DecodeBlockPayload(view.Data, desc.Type, errBad)
	if err != nil {
		return nil, err
	}
	return payload, nil
}
