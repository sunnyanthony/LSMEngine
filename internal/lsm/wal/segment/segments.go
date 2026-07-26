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
	if nums[0] > 1 && uint64(nums[0]-1) <= prunedThrough {
		expected = nums[0]
	}
	for _, n := range nums {
		if n != expected {
			missing = true
			break
		}
		expected++
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
	marker := PrunedMarkerPath(path)
	tmp := marker + ".tmp"
	if err := os.WriteFile(tmp, []byte(fmt.Sprintf("%d\n", segmentID)), 0o644); err != nil {
		return fmt.Errorf("write pruned marker: %w", err)
	}
	if err := os.Rename(tmp, marker); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename pruned marker: %w", err)
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
