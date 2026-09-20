package commitlog

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRaftDirectorySyncAncestorsOnCreationAndRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data", "raft")
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{}
	for current := abs; ; current = filepath.Dir(current) {
		want = append(want, current)
		if filepath.Dir(current) == current {
			break
		}
	}
	failure := errors.New("parent sync failed")
	err = ensureDurableRaftDir(path, func(current string) error {
		if current == filepath.Dir(abs) {
			return failure
		}
		return nil
	})
	if !errors.Is(err, failure) {
		t.Fatalf("parent failure not propagated: %v", err)
	}
	for i := 0; i < 2; i++ {
		var got []string
		if err := ensureDurableRaftDir(path, func(current string) error {
			got = append(got, current)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("retry skipped ancestor durability: got %v want %v", got, want)
		}
	}
}
