// Trash-based file remover with size/entry cycling.

package cleanup

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Trash moves files into a directory and prunes old entries by size/count.
type Trash struct {
	dir      string
	maxBytes int64
	maxFiles int
	mu       sync.Mutex
}

// NewTrash creates a new trash remover rooted at dir.
func NewTrash(dir string, maxBytes int64, maxFiles int) (*Trash, error) {
	if dir == "" {
		return nil, fmt.Errorf("trash dir required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("trash mkdir: %w", err)
	}
	return &Trash{dir: dir, maxBytes: maxBytes, maxFiles: maxFiles}, nil
}

// Remove moves path into the trash dir and prunes old entries.
func (t *Trash) Remove(path string) error {
	if t == nil {
		return nil
	}
	if path == "" {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	base := filepath.Base(path)
	dest := filepath.Join(t.dir, fmt.Sprintf("%s.%d", base, time.Now().UnixNano()))
	if err := os.Rename(path, dest); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	t.pruneLocked()
	return nil
}

type trashEntry struct {
	path string
	size int64
	mod  time.Time
}

func (t *Trash) pruneLocked() {
	if t.maxBytes <= 0 && t.maxFiles <= 0 {
		return
	}
	entries, err := os.ReadDir(t.dir)
	if err != nil {
		return
	}
	items := make([]trashEntry, 0, len(entries))
	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		items = append(items, trashEntry{
			path: filepath.Join(t.dir, entry.Name()),
			size: info.Size(),
			mod:  info.ModTime(),
		})
		total += info.Size()
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].mod.Equal(items[j].mod) {
			return items[i].path < items[j].path
		}
		return items[i].mod.Before(items[j].mod)
	})
	for len(items) > 0 {
		overFiles := t.maxFiles > 0 && len(items) > t.maxFiles
		overBytes := t.maxBytes > 0 && total > t.maxBytes
		if !overFiles && !overBytes {
			return
		}
		oldest := items[0]
		_ = os.Remove(oldest.path)
		total -= oldest.size
		items = items[1:]
	}
}
