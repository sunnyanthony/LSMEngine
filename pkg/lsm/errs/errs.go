package errs

import "errors"

var (
	// ErrWALCorrupt signals a corrupted WAL record or block.
	ErrWALCorrupt = errors.New("wal corrupt")
	// ErrWALCorruptSegment indicates a segment with an invalid header.
	ErrWALCorruptSegment = errors.New("wal corrupt segment")
	// ErrWALMissingSegment indicates a gap in WAL segments.
	ErrWALMissingSegment = errors.New("wal missing segment")
	// ErrWALClosed indicates the WAL is closed.
	ErrWALClosed = errors.New("wal closed")
	// ErrWALEmptyKey indicates an empty key was provided.
	ErrWALEmptyKey = errors.New("wal append: empty key")
	// ErrWALEmptyValue indicates an empty value was provided.
	ErrWALEmptyValue = errors.New("wal append: empty value")
	// ErrWALRecordTooLarge indicates a record exceeded configured limits.
	ErrWALRecordTooLarge = errors.New("wal append: record too large")
	// ErrRangeUnsupported indicates range scans cannot include SSTables yet.
	ErrRangeUnsupported = errors.New("range scan: sstable iterator unavailable")
)
