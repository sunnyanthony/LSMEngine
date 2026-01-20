// Append-only manifest log store with checkpoints.

package manifest

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// LogOptions configures the append-only manifest store.
type LogOptions struct {
	LogPath          string
	CheckpointPath   string
	CheckpointEveryN int
	CheckpointPerm   os.FileMode
	LogPerm          os.FileMode
}

// LogStore persists manifest updates to an append-only log with checkpoints.
type LogStore struct {
	opts      LogOptions
	mu        sync.Mutex
	loaded    bool
	state     Manifest
	updateCnt int
}

// NewLogStore creates a log-backed manifest store.
func NewLogStore(opts LogOptions) (*LogStore, error) {
	if opts.LogPath == "" || opts.CheckpointPath == "" {
		return nil, fmt.Errorf("manifest log: paths required")
	}
	if err := os.MkdirAll(filepath.Dir(opts.LogPath), 0o755); err != nil {
		return nil, fmt.Errorf("manifest log mkdir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(opts.CheckpointPath), 0o755); err != nil {
		return nil, fmt.Errorf("manifest checkpoint mkdir: %w", err)
	}
	if opts.CheckpointEveryN <= 0 {
		opts.CheckpointEveryN = 128
	}
	if opts.LogPerm == 0 {
		opts.LogPerm = 0o644
	}
	if opts.CheckpointPerm == 0 {
		opts.CheckpointPerm = 0o644
	}
	return &LogStore{opts: opts}, nil
}

func (s *LogStore) Load() (Manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return Manifest{}, err
	}
	return s.state, nil
}

func (s *LogStore) Save(m Manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = m
	s.loaded = true
	s.updateCnt = 0
	if err := s.writeCheckpointLocked(); err != nil {
		return err
	}
	if err := s.truncateLogLocked(); err != nil {
		return err
	}
	return nil
}

func (s *LogStore) Update(fn func(Manifest) Manifest) error {
	if fn == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return err
	}
	s.state = fn(s.state)
	if err := s.appendLocked(logRecord{Type: "snapshot", Manifest: s.state}); err != nil {
		return err
	}
	s.updateCnt++
	if s.updateCnt >= s.opts.CheckpointEveryN {
		if err := s.writeCheckpointLocked(); err != nil {
			return err
		}
		if err := s.truncateLogLocked(); err != nil {
			return err
		}
		s.updateCnt = 0
	}
	return nil
}

type logRecord struct {
	Type     string   `json:"type"`
	Manifest Manifest `json:"manifest"`
}

func (s *LogStore) loadLocked() error {
	if s.loaded {
		return nil
	}
	state := Manifest{}
	if data, err := os.ReadFile(s.opts.CheckpointPath); err == nil && len(data) > 0 {
		if err := json.Unmarshal(data, &state); err != nil {
			// Ignore corrupt checkpoints; fall back to replaying the log.
			state = Manifest{}
		}
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("manifest checkpoint read: %w", err)
	}
	logState, err := s.readLogLocked(state)
	if err != nil {
		return err
	}
	s.state = logState
	s.loaded = true
	return nil
}

func (s *LogStore) readLogLocked(state Manifest) (out Manifest, err error) {
	out = state
	f, err := os.Open(s.opts.LogPath)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return Manifest{}, fmt.Errorf("manifest log open: %w", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			if err == nil {
				err = fmt.Errorf("manifest log close: %w", cerr)
			} else {
				err = errors.Join(err, fmt.Errorf("manifest log close: %w", cerr))
			}
		}
	}()

	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) == 0 && err != nil {
			break
		}
		if len(line) == 0 {
			if err != nil {
				break
			}
			continue
		}
		var rec logRecord
		if err := json.Unmarshal(trimLine(line), &rec); err != nil {
			// Stop on corrupt tail to allow recovery.
			break
		}
		if rec.Type == "snapshot" {
			out = rec.Manifest
		}
		if err != nil {
			break
		}
	}
	return out, nil
}

func trimLine(line []byte) []byte {
	if len(line) == 0 {
		return line
	}
	if line[len(line)-1] == '\n' {
		return line[:len(line)-1]
	}
	return line
}

func (s *LogStore) appendLocked(rec logRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("manifest log marshal: %w", err)
	}
	data = append(data, '\n')
	f, err := os.OpenFile(s.opts.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, s.opts.LogPerm)
	if err != nil {
		return fmt.Errorf("manifest log open append: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		if cerr := f.Close(); cerr != nil {
			return errors.Join(fmt.Errorf("manifest log append: %w", err), fmt.Errorf("manifest log close: %w", cerr))
		}
		return fmt.Errorf("manifest log append: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("manifest log close: %w", err)
	}
	return nil
}

func (s *LogStore) writeCheckpointLocked() error {
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return fmt.Errorf("manifest checkpoint marshal: %w", err)
	}
	tmp := s.opts.CheckpointPath + ".tmp"
	if err := os.WriteFile(tmp, data, s.opts.CheckpointPerm); err != nil {
		return fmt.Errorf("manifest checkpoint write: %w", err)
	}
	if err := os.Rename(tmp, s.opts.CheckpointPath); err != nil {
		return fmt.Errorf("manifest checkpoint rename: %w", err)
	}
	return nil
}

func (s *LogStore) truncateLogLocked() error {
	if err := os.Truncate(s.opts.LogPath, 0); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("manifest log truncate: %w", err)
	}
	return nil
}
