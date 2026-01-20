// OS filesystem implementation of FS.

package iofs

import "os"

// OSFS implements FS using the standard library filesystem.
type OSFS struct{}

func (OSFS) Open(path string) (File, error) {
	return os.Open(path)
}

func (OSFS) OpenFile(path string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(path, flag, perm)
}

func (OSFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (OSFS) Remove(path string) error {
	return os.Remove(path)
}

func (OSFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (OSFS) Stat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}

func (OSFS) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (OSFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}

func (OSFS) Truncate(path string, size int64) error {
	return os.Truncate(path, size)
}
