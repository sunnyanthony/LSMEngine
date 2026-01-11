//go:build !unix

package storage

import "os"

func mmapFile(_ *os.File, _ int64) ([]byte, error) {
	return nil, errMmapUnsupported
}

func munmap(_ []byte) error {
	return nil
}
