package manifest

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"lsmengine/internal/lsm/iofs"
)

func syncPath(fs iofs.FS, path string) error {
	f, err := fs.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func writeDurableCheckpoint(fs iofs.FS, path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	f, err := fs.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	defer fs.Remove(tmp)
	n, err := f.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	if err := fs.Rename(tmp, path); err != nil {
		return err
	}
	return syncPath(fs, filepath.Dir(path))
}
