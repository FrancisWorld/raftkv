package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/FrancisWorld/raftkv/internal/raft"
)

const (
	logName  = "wal.log"
	snapName = "snapshot"
)

func dirOf(path string) string { return filepath.Dir(path) }

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

var _ raft.Storage = (*Store)(nil)

// Store is a raft.Storage persisted under one directory.
type Store struct {
	dir string

	mu     sync.Mutex
	f      *os.File // append handle on wal.log
	loaded raft.State
}

// Open opens (creating if necessary) the store in dir and recovers its
// contents. A torn final record, the signature of a crash mid-write, is
// truncated away; any other damage fails with ErrCorrupt.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s := &Store{dir: dir}
	snap, err := readSnapshot(s.snapPath())
	if err != nil {
		return nil, fmt.Errorf("wal: read snapshot: %w", err)
	}
	hs, ents, err := s.recoverLog()
	if err != nil {
		return nil, err
	}
	if ents, err = reconcile(snap, ents); err != nil {
		return nil, err
	}
	s.loaded = raft.State{HardState: hs, Snapshot: snap, Entries: ents}
	if s.f, err = os.OpenFile(s.logPath(), os.O_WRONLY|os.O_APPEND, 0); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) logPath() string  { return filepath.Join(s.dir, logName) }
func (s *Store) snapPath() string { return filepath.Join(s.dir, snapName) }

// recoverLog reads wal.log, creating it if absent and truncating a torn tail.
func (s *Store) recoverLog() (raft.HardState, []raft.Entry, error) {
	b, err := os.ReadFile(s.logPath())
	if errors.Is(err, os.ErrNotExist) {
		return raft.HardState{}, nil, s.writeLog(raft.HardState{}, nil)
	}
	if err != nil {
		return raft.HardState{}, nil, err
	}
	if len(b) < len(logMagic) {
		// Crash while the very first write was in flight.
		return raft.HardState{}, nil, s.writeLog(raft.HardState{}, nil)
	}
	if string(b[:len(logMagic)]) != logMagic {
		return raft.HardState{}, nil, fmt.Errorf("%w: bad log magic", ErrCorrupt)
	}
	recs, n, scanErr := scan(b[len(logMagic):])
	if scanErr != nil && !errors.Is(scanErr, errTorn) {
		return raft.HardState{}, nil, scanErr
	}
	hs, ents, err := replay(recs)
	if err != nil {
		return hs, nil, err
	}
	if scanErr != nil {
		if err := os.Truncate(s.logPath(), int64(len(logMagic)+n)); err != nil {
			return hs, nil, err
		}
	}
	return hs, ents, nil
}

// writeLog atomically replaces wal.log with a fresh log holding hs and ents.
func (s *Store) writeLog(hs raft.HardState, ents []raft.Entry) error {
	buf := append([]byte(nil), logMagic...)
	buf = appendRecord(buf, recHardState, encodeHardState(hs))
	for _, e := range ents {
		buf = appendRecord(buf, recEntry, encodeEntry(e))
	}
	return writeFileAtomic(s.logPath(), buf)
}

// Load implements raft.Storage.
func (s *Store) Load() (raft.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.loaded
	st.Entries = append([]raft.Entry(nil), st.Entries...)
	return st, nil
}

// Save implements raft.Storage. The records are written with a single write and
// fsynced before returning.
func (s *Store) Save(hs *raft.HardState, entries []raft.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf []byte
	if hs != nil {
		buf = appendRecord(buf, recHardState, encodeHardState(*hs))
	}
	for _, e := range entries {
		buf = appendRecord(buf, recEntry, encodeEntry(e))
	}
	if len(buf) == 0 {
		return nil
	}
	if _, err := s.f.Write(buf); err != nil {
		return err
	}
	return s.f.Sync()
}

// InstallSnapshot implements raft.Storage.
func (s *Store) InstallSnapshot(snap raft.Snapshot) error {
	return s.replaceSnapshot(snap, func([]raft.Entry) []raft.Entry { return nil })
}

// Compact implements raft.Storage.
func (s *Store) Compact(snap raft.Snapshot) error {
	return s.replaceSnapshot(snap, func(ents []raft.Entry) []raft.Entry {
		var keep []raft.Entry
		for _, e := range ents {
			if e.Index > snap.Index {
				keep = append(keep, e)
			}
		}
		return keep
	})
}

// replaceSnapshot writes the snapshot first, then rewrites the log. A crash
// between the two leaves a log that reconcile() trims on the next Open.
func (s *Store) replaceSnapshot(snap raft.Snapshot, keep func([]raft.Entry) []raft.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := writeFileAtomic(s.snapPath(), encodeSnapshot(snap)); err != nil {
		return err
	}
	b, err := os.ReadFile(s.logPath())
	if err != nil {
		return err
	}
	recs, _, err := scan(b[len(logMagic):])
	if err != nil {
		return err
	}
	hs, ents, err := replay(recs)
	if err != nil {
		return err
	}
	if err := s.f.Close(); err != nil {
		return err
	}
	if err := s.writeLog(hs, keep(ents)); err != nil {
		return err
	}
	s.f, err = os.OpenFile(s.logPath(), os.O_WRONLY|os.O_APPEND, 0)
	return err
}

// Close implements raft.Storage.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
