package wal

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"lsmengine/pkg/lsm/types"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// WAL appends mutations for durability and supports replay.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
	sync bool
}

type Options struct {
	Path string
	Sync bool
}

func NewWAL(opts Options) (*WAL, error) {
	if opts.Path == "" {
		return nil, fmt.Errorf("wal path required")
	}
	if err := os.MkdirAll(filepath.Dir(opts.Path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir wal dir: %w", err)
	}
	f, err := os.OpenFile(opts.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{f: f, path: opts.Path, sync: opts.Sync}, nil
}

func (w *WAL) Append(entry types.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return fmt.Errorf("wal closed")
	}
	line := encodeEntry(entry)
	if _, err := w.f.WriteString(line); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	if w.sync {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("wal sync: %w", err)
		}
	}
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// Replay reads entries from the WAL in order and calls fn for each.
func (w *WAL) Replay(fn func(types.Entry) error) error {
	f, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open wal for replay: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		entry, err := decodeEntry(line)
		if err != nil {
			return fmt.Errorf("decode wal entry: %w", err)
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan wal: %w", err)
	}
	return nil
}

func encodeEntry(e types.Entry) string {
	keyHex := hex.EncodeToString([]byte(e.Key))
	valHex := hex.EncodeToString(e.Value)
	return fmt.Sprintf("%d|%t|%s|%s\n", e.Seq, e.Tombstone, keyHex, valHex)
}

func decodeEntry(line string) (types.Entry, error) {
	parts := strings.Split(line, "|")
	if len(parts) != 4 {
		return types.Entry{}, fmt.Errorf("invalid wal line")
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
