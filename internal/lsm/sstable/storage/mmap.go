package storage

import "errors"

var errMmapUnsupported = errors.New("sstable: mmap unsupported")
