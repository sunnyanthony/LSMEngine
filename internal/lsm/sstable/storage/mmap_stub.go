//go:build !unix

// mmap stub for unsupported platforms.

package storage

func mmapFile(_ interface{ Fd() uintptr }, _ int64) ([]byte, error) {
	return nil, errMmapUnsupported
}

func munmap(_ []byte) error {
	return nil
}
