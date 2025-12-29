package sstable

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"lsmengine/pkg/lsm/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// SSTable is an immutable sorted run persisted to disk.
type SSTable struct {
	Path string
	Seq  uint64 // highest sequence in the file
	// index maps keys to entries for the placeholder implementation.
	index map[string]types.Entry
}

type SSTableWriter struct {
	dir string
}

func NewSSTableWriter(dir string) (*SSTableWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir sstable dir: %w", err)
	}
	return &SSTableWriter{dir: dir}, nil
}

// Flusher writes entries to SSTable storage.
type Flusher interface {
	Flush(entries []types.Entry) (SSTable, error)
}

// Flush creates a new SSTable file with entries sorted by key.
func (w *SSTableWriter) Flush(entries []types.Entry) (SSTable, error) {
	if len(entries) == 0 {
		return SSTable{}, fmt.Errorf("no entries to write")
	}
	// ensure sort by key for deterministic ordering
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	highestSeq := entries[len(entries)-1].Seq
	path := filepath.Join(w.dir, fmt.Sprintf("sstable-%d.sst", highestSeq))

	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return SSTable{}, fmt.Errorf("create sstable: %w", err)
	}
	writer := bufio.NewWriter(f)
	for _, e := range entries {
		keyHex := hex.EncodeToString([]byte(e.Key))
		valHex := hex.EncodeToString(e.Value)
		if _, err := fmt.Fprintf(writer, "%d|%t|%s|%s\n", e.Seq, e.Tombstone, keyHex, valHex); err != nil {
			f.Close()
			return SSTable{}, fmt.Errorf("write sstable: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		f.Close()
		return SSTable{}, fmt.Errorf("flush sstable: %w", err)
	}
	if err := f.Close(); err != nil {
		return SSTable{}, fmt.Errorf("close sstable: %w", err)
	}
	return LoadSSTable(path)
}

func LoadSSTable(path string) (SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return SSTable{}, fmt.Errorf("open sstable: %w", err)
	}
	defer f.Close()

	index := make(map[string]types.Entry)
	var highest uint64
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		entry, err := decodeSSTableLine(line)
		if err != nil {
			return SSTable{}, fmt.Errorf("decode sstable line: %w", err)
		}
		// last-write-wins if duplicates
		if prev, ok := index[entry.Key]; !ok || entry.Seq > prev.Seq {
			index[entry.Key] = entry
		}
		if entry.Seq > highest {
			highest = entry.Seq
		}
	}
	if err := scanner.Err(); err != nil {
		return SSTable{}, fmt.Errorf("scan sstable: %w", err)
	}
	return SSTable{Path: path, Seq: highest, index: index}, nil
}

func (s SSTable) Get(key string) (types.Entry, bool) {
	e, ok := s.index[key]
	return e, ok
}

func decodeSSTableLine(line string) (types.Entry, error) {
	parts := strings.Split(line, "|")
	if len(parts) != 4 {
		return types.Entry{}, fmt.Errorf("invalid sstable line")
	}
	var seq uint64
	var tombstone bool
	if _, err := fmt.Sscanf(parts[0], "%d", &seq); err != nil {
		return types.Entry{}, err
	}
	if _, err := fmt.Sscanf(parts[1], "%t", &tombstone); err != nil {
		return types.Entry{}, err
	}
	keyBytes, err := hex.DecodeString(parts[2])
	if err != nil {
		return types.Entry{}, err
	}
	valBytes, err := hex.DecodeString(strings.TrimSpace(parts[3]))
	if err != nil {
		return types.Entry{}, err
	}
	return types.Entry{
		Key:       string(keyBytes),
		Value:     valBytes,
		Tombstone: tombstone,
		Seq:       seq,
	}, nil
}
