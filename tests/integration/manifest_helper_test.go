package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type manifestView struct {
	WALSeq uint64 `json:"wal_seq"`
	Tables []struct {
		Path string `json:"path"`
		Seq  uint64 `json:"seq"`
	} `json:"tables"`
}

func waitForManifest(t *testing.T, dir string, minTables int, minWALSeq uint64) {
	t.Helper()
	path := filepath.Join(dir, "manifest.json")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			var view manifestView
			if err := json.Unmarshal(data, &view); err == nil {
				if len(view.Tables) >= minTables && view.WALSeq >= minWALSeq {
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected manifest with tables>=%d wal_seq>=%d", minTables, minWALSeq)
}
