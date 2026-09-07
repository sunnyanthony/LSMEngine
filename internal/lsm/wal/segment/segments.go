// WAL segment discovery helpers.

package segment

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const prunedMarkerSuffix = ".pruned"

func ListSegments(path string) ([]string, bool, error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, false, fmt.Errorf("list segments: %w", err)
	}
	prunedThrough, err := ReadPrunedThrough(path)
	if err != nil {
		return nil, false, err
	}
	var nums []int
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, base+".") {
			part := strings.TrimPrefix(name, base+".")
			if part == "" {
				continue
			}
			if n, err := strconv.Atoi(part); err == nil {
				nums = append(nums, n)
			}
		}
	}
	if len(nums) == 0 {
		return nil, false, nil
	}
	sort.Ints(nums)
	missing := false
	expected := 1
	for _, n := range nums {
		if n != expected && (n < expected || uint64(n-1) > prunedThrough) {
			missing = true
			break
		}
		expected = n + 1
	}
	segs := make([]string, 0, len(nums))
	for _, n := range nums {
		segs = append(segs, filepath.Join(dir, fmt.Sprintf("%s.%d", base, n)))
	}
	return segs, missing, nil
}

func SegmentID(path string) (uint64, bool) {
	_, name := filepath.Split(path)
	idx := strings.LastIndexByte(name, '.')
	if idx < 0 || idx == len(name)-1 {
		return 0, false
	}
	id, err := strconv.ParseUint(name[idx+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

func PrunedMarkerPath(path string) string {
	return path + prunedMarkerSuffix
}

func ReadPrunedThrough(path string) (uint64, error) {
	data, err := os.ReadFile(PrunedMarkerPath(path))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read pruned marker: %w", err)
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return 0, nil
	}
	out, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse pruned marker: %w", err)
	}
	return out, nil
}

func WritePrunedThrough(path string, segmentID uint64) error {
	previous, err := ReadPrunedThrough(path)
	if err != nil {
		return err
	}
	if previous > segmentID {
		segmentID = previous
	}
	marker := PrunedMarkerPath(path)
	tmp := marker + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open pruned marker: %w", err)
	}
	defer os.Remove(tmp)
	if _, err := fmt.Fprintf(f, "%d\n", segmentID); err != nil {
		_ = f.Close()
		return fmt.Errorf("write pruned marker: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync pruned marker: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close pruned marker: %w", err)
	}
	if err := os.Rename(tmp, marker); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename pruned marker: %w", err)
	}
	dir, err := os.Open(filepath.Dir(marker))
	if err != nil {
		return fmt.Errorf("open pruned marker directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync pruned marker directory: %w", err)
	}
	return nil
}

func NextSegmentID(path string) uint64 {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 1
	}
	max := 0
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, base+".") {
			part := strings.TrimPrefix(name, base+".")
			if n, err := strconv.Atoi(part); err == nil && n > max {
				max = n
			}
		}
	}
	return uint64(max + 1)
}
