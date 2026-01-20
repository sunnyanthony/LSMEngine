package runtime

import "lsmengine/internal/lsm/compaction"

type applyHooks struct {
	BeforeApply func(compaction.Result)
	AfterApply  func(compaction.Result, error)
}
