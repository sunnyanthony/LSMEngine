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

func ListSegments(path string) ([]string, bool, error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, false, fmt.Errorf("list segments: %w", err)
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
	for i := 0; i < len(nums); i++ {
		if nums[i] != i+1 {
			missing = true
			break
		}
	}
	segs := make([]string, 0, len(nums))
	for _, n := range nums {
		segs = append(segs, filepath.Join(dir, fmt.Sprintf("%s.%d", base, n)))
	}
	return segs, missing, nil
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
