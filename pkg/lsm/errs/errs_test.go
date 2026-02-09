package errs

import "testing"

func TestErrorsAreDistinct(t *testing.T) {
	errs := []error{
		ErrNotLeader,
		ErrWALCorrupt,
		ErrWALCorruptSegment,
		ErrWALMissingSegment,
		ErrWALClosed,
		ErrWALEmptyKey,
		ErrWALEmptyValue,
		ErrBackpressure,
		ErrWALRecordTooLarge,
		ErrRangeUnsupported,
		ErrSSTableBadMagic,
		ErrSSTableBadFooter,
		ErrSSTableBadBlock,
		ErrSSTableBadMeta,
		ErrSSTableBadIndex,
		ErrSSTableUnknownCompression,
		ErrClosed,
		ErrShardNotFound,
		ErrControlRevisionConflict,
		ErrControlOperationConflict,
	}
	seen := make(map[string]struct{}, len(errs))
	for _, err := range errs {
		if err == nil {
			t.Fatalf("expected error")
		}
		if _, ok := seen[err.Error()]; ok {
			t.Fatalf("duplicate error string: %s", err)
		}
		seen[err.Error()] = struct{}{}
	}
}
