package raft

import (
	"fmt"
	"sync"
)

// MemStorage is an in-memory Storage. It models a disk that survives a "crash"
// of the node using it, which is what the simulator needs.
type MemStorage struct {
	mu    sync.Mutex
	state State
}

// NewMemStorage returns an empty MemStorage.
func NewMemStorage() *MemStorage { return &MemStorage{} }

// Load implements Storage.
func (m *MemStorage) Load() (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.state
	s.Entries = append([]Entry(nil), s.Entries...)
	return s, nil
}

// Save implements Storage.
func (m *MemStorage) Save(hs *HardState, entries []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hs != nil {
		m.state.HardState = *hs
	}
	if len(entries) == 0 {
		return nil
	}
	first := entries[0].Index
	base := m.state.Snapshot.Index
	last := base + uint64(len(m.state.Entries))
	if first <= base || first > last+1 {
		return fmt.Errorf("raft: mem storage: append at %d outside (%d, %d]", first, base, last+1)
	}
	m.state.Entries = append(m.state.Entries[:first-base-1], entries...)
	return nil
}

// InstallSnapshot implements Storage.
func (m *MemStorage) InstallSnapshot(snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Snapshot = snap
	m.state.Entries = nil
	return nil
}

// Compact implements Storage.
func (m *MemStorage) Compact(snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	base := m.state.Snapshot.Index
	if snap.Index <= base || snap.Index > base+uint64(len(m.state.Entries)) {
		return fmt.Errorf("raft: mem storage: compact to %d outside (%d, %d]", snap.Index, base, base+uint64(len(m.state.Entries)))
	}
	m.state.Entries = append([]Entry(nil), m.state.Entries[snap.Index-base:]...)
	m.state.Snapshot = snap
	return nil
}

// Close implements Storage.
func (m *MemStorage) Close() error { return nil }
