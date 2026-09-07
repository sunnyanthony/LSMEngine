package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfiguredWALRotationRecovery(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, limit := range []uint64{0, 128} {
			t.Run(fmt.Sprintf("async=%v/limit=%d", async, limit), func(t *testing.T) {
				opts := Options{DataDir: t.TempDir(), WALSync: true, WALAsync: async, WALBlockSize: 64, WALMaxSegmentBytes: limit, MemtableLimit: 1 << 20}
				store, err := New(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				for i := 0; i < 12; i++ {
					if err := store.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("value")); err != nil {
						t.Fatal(err)
					}
				}
				if err := store.Delete([]byte("k00")); err != nil {
					t.Fatal(err)
				}
				if err := store.Put([]byte("k01"), []byte("updated")); err != nil {
					t.Fatal(err)
				}
				stats := store.Stats()
				if stats.SSTableCount != 0 || (stats.WAL.ArchivedSegmentCount > 0) != (limit > 0) {
					t.Fatalf("unexpected rotation/flush state: %+v", stats)
				}

				// Copy only completed WAL writes, before Close can flush SSTables.
				recoveryDir := t.TempDir()
				files, err := os.ReadDir(opts.DataDir)
				if err != nil {
					t.Fatal(err)
				}
				for _, file := range files {
					if file.Name() != "wal.log" && !strings.HasPrefix(file.Name(), "wal.log.") {
						continue
					}
					data, err := os.ReadFile(filepath.Join(opts.DataDir, file.Name()))
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(recoveryDir, file.Name()), data, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				opts.DataDir = recoveryDir
				recovered, err := New(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer recovered.Close()
				for i := 0; i < 12; i++ {
					entry, ok := recovered.Get([]byte(fmt.Sprintf("k%02d", i)))
					if i == 0 {
						if ok {
							t.Fatal("deleted key reappeared after WAL recovery")
						}
						continue
					}
					want := "value"
					if i == 1 {
						want = "updated"
					}
					if !ok || string(entry.Value) != want {
						t.Fatalf("key %d: got %+v, found=%v", i, entry, ok)
					}
				}
				if recovered.Stats().Seq != stats.Seq {
					t.Fatalf("recovered sequence %d, want %d", recovered.Stats().Seq, stats.Seq)
				}
				seq, err := recovered.PutWithSeq([]byte("after"), []byte("recovery"))
				if err != nil || seq != stats.Seq+1 {
					t.Fatalf("write after recovery: seq=%d err=%v, want %d", seq, err, stats.Seq+1)
				}
			})
		}
	}
}
