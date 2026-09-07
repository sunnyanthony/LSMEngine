// Append-only manifest log store with checkpoints.

package manifest

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"lsmengine/internal/lsm/iofs"
)

// LogOptions configures the append-only manifest store.
type LogOptions struct {
	LogPath          string
	CheckpointPath   string
	CheckpointEveryN int
	CheckpointPerm   os.FileMode
	LogPerm          os.FileMode
	FS               iofs.FS
}

// LogStore persists manifest updates to an append-only log with checkpoints.
type LogStore struct {
	opts      LogOptions
	fs        iofs.FS
	mu        sync.Mutex
	loaded    bool
	state     Manifest
	updateCnt int
	failed    error
	repairLog bool
}

// NewLogStore creates a log-backed manifest store.
func NewLogStore(opts LogOptions) (*LogStore, error) {
	if opts.LogPath == "" || opts.CheckpointPath == "" {
		return nil, fmt.Errorf("manifest log: paths required")
	}
	fs := opts.FS
	if fs == nil {
		fs = iofs.OSFS{}
	}
	if err := fs.MkdirAll(filepath.Dir(opts.LogPath), 0o755); err != nil {
		return nil, fmt.Errorf("manifest log mkdir: %w", err)
	}
	if err := fs.MkdirAll(filepath.Dir(opts.CheckpointPath), 0o755); err != nil {
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
	return &LogStore{opts: opts, fs: fs}, nil
}

func (s *LogStore) Load() (Manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return Manifest{}, err
	}
	return s.state, nil
}

func (s *LogStore) Save(m Manifest) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failed
	}
	defer func() {
		if err != nil {
			s.failed = err
		}
	}()
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

func (s *LogStore) Update(fn func(Manifest) Manifest) (err error) {
	if fn == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() {
		if err != nil {
			s.failed = err
		}
	}()
	if err := s.loadLocked(); err != nil {
		return err
	}
	if s.repairLog {
		if err := s.writeCheckpointLocked(); err != nil {
			return err
		}
		if err := s.truncateLogLocked(); err != nil {
			return err
		}
		s.repairLog = false
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
	if s.failed != nil {
		return s.failed
	}
	if s.loaded {
		return nil
	}
	state := Manifest{}
	if data, err := s.fs.ReadFile(s.opts.CheckpointPath); err == nil && len(data) > 0 {
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
	f, err := s.fs.Open(s.opts.LogPath)
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
		if err != nil && !errors.Is(err, io.EOF) {
			return Manifest{}, fmt.Errorf("manifest log read: %w", err)
		}
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
			s.repairLog = true
			break
		}
		if rec.Type == "snapshot" {
			out = rec.Manifest
		}
		if err != nil {
			s.repairLog = true
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
	f, err := s.fs.OpenFile(s.opts.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, s.opts.LogPerm)
	if err != nil {
		return fmt.Errorf("manifest log open append: %w", err)
	}
	n, err := f.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		if cerr := f.Close(); cerr != nil {
			return errors.Join(fmt.Errorf("manifest log append: %w", err), fmt.Errorf("manifest log close: %w", cerr))
		}
		return fmt.Errorf("manifest log append: %w", err)
	}
	if err := f.Sync(); err != nil {
		return errors.Join(fmt.Errorf("manifest log sync: %w", err), f.Close())
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("manifest log close: %w", err)
	}
	return syncPath(s.fs, filepath.Dir(s.opts.LogPath))
}

func (s *LogStore) writeCheckpointLocked() error {
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return fmt.Errorf("manifest checkpoint marshal: %w", err)
	}
	return writeDurableCheckpoint(s.fs, s.opts.CheckpointPath, data, s.opts.CheckpointPerm)
}

func (s *LogStore) truncateLogLocked() error {
	if err := s.fs.Truncate(s.opts.LogPath, 0); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("manifest log truncate: %w", err)
	} else if os.IsNotExist(err) {
		return nil
	}
	return syncPath(s.fs, s.opts.LogPath)
}
