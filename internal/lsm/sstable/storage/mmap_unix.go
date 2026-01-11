//go:build unix

package storage

import (
	"os"
	"syscall"
)

func mmapFile(f *os.File, size int64) ([]byte, error) {
	if size <= 0 {
		return nil, nil
	}
	if size > int64(int(^uint(0)>>1)) {
		return nil, errMmapUnsupported
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func munmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
