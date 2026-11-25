// Package kv is the replicated state machine: an in-memory map plus a client
// session table that turns at-least-once delivery into exactly-once execution.
//
// Every command carries (ClientID, Seq). The session table remembers, per
// client, the highest Seq applied and its result. A retried command (same Seq)
// is not re-executed; it gets the remembered result back. This is what lets a
// client safely retry a write whose reply it never saw, across leader changes.
package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Limits enforced at the API edge and again at apply time.
const (
	MaxKeyLen   = 1024
	MaxValueLen = 1 << 20
)

// Op is a state machine operation.
type Op string

// Operations. Reads are not commands; they are served through ReadIndex.
const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
	OpCAS    Op = "cas"
)

// Command is a client write, serialised into the Raft log.
type Command struct {
	// ClientID identifies the session; 0 disables deduplication.
	ClientID uint64 `json:"client"`
	// Seq increases by one per distinct command from a client, which must have at
	// most one command outstanding.
	Seq uint64 `json:"seq"`
	Op  Op     `json:"op"`
	Key string `json:"key"`

	Value        string `json:"value,omitempty"`
	Expected     string `json:"expected,omitempty"`      // CAS only
	ExpectAbsent bool   `json:"expect_absent,omitempty"` // CAS only
}

// Validate checks a command before it is proposed.
func (c Command) Validate() error {
	switch c.Op {
	case OpPut, OpDelete, OpCAS:
	default:
		return fmt.Errorf("unknown op %q", c.Op)
	}
	if c.Key == "" || len(c.Key) > MaxKeyLen {
		return fmt.Errorf("key must be 1..%d bytes", MaxKeyLen)
	}
	if len(c.Value) > MaxValueLen || len(c.Expected) > MaxValueLen {
		return fmt.Errorf("value larger than %d bytes", MaxValueLen)
	}
	if c.ClientID != 0 && c.Seq == 0 {
		return errors.New("seq must be >= 1 when a client id is set")
	}
	return nil
}

// Encode serialises the command for the log.
func (c Command) Encode() []byte {
	b, err := json.Marshal(c)
	if err != nil { // a struct of strings, integers and bools cannot fail to marshal
		panic(err)
	}
	return b
}

// Result is the outcome of a command.
type Result struct {
	// Applied is false only for a CAS whose precondition failed, or a delete
	// of a key that did not exist.
	Applied   bool   `json:"applied"`
	PrevFound bool   `json:"prev_found"`
	Prev      string `json:"prev,omitempty"`
	// Duplicate marks a replay answered from the session table.
	Duplicate bool `json:"duplicate,omitempty"`
	// Error is a deterministic apply-time rejection ("stale" or "malformed").
	Error string `json:"error,omitempty"`
}

// DecodeResult parses a Result produced by Apply.
func DecodeResult(b []byte) (Result, error) {
	var r Result
	err := json.Unmarshal(b, &r)
	return r, err
}

type session struct {
	Seq    uint64 `json:"seq"`
	Result Result `json:"result"`
}

// Store is the key-value state machine. Apply is called from one goroutine (the
// Raft apply loop); Get may be called concurrently.
type Store struct {
	mu       sync.RWMutex
	data     map[string]string
	sessions map[uint64]session
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{data: map[string]string{}, sessions: map[uint64]session{}}
}

// Get reads a key. Callers wanting linearizability must first pass a ReadIndex
// barrier.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Len returns the number of keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Apply executes an encoded Command and returns an encoded Result. It is
// deterministic: every replica applying the same log produces the same state.
func (s *Store) Apply(data []byte) []byte {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil || cmd.Validate() != nil {
		return encodeResult(Result{Error: "malformed"})
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if cmd.ClientID != 0 {
		if sess, ok := s.sessions[cmd.ClientID]; ok {
			switch {
			case cmd.Seq == sess.Seq:
				res := sess.Result
				res.Duplicate = true
				return encodeResult(res)
			case cmd.Seq < sess.Seq:
				// The client already moved on, so it cannot be waiting for this.
				return encodeResult(Result{Error: "stale"})
			}
		}
	}

	res := s.execute(cmd)
	if cmd.ClientID != 0 {
		s.sessions[cmd.ClientID] = session{Seq: cmd.Seq, Result: res}
	}
	return encodeResult(res)
}

func (s *Store) execute(cmd Command) Result {
	prev, found := s.data[cmd.Key]
	res := Result{Applied: true, PrevFound: found, Prev: prev}
	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
	case OpDelete:
		res.Applied = found
		delete(s.data, cmd.Key)
	case OpCAS:
		matches := (cmd.ExpectAbsent && !found) || (!cmd.ExpectAbsent && found && prev == cmd.Expected)
		res.Applied = matches
		if matches {
			s.data[cmd.Key] = cmd.Value
		}
	}
	return res
}

func encodeResult(r Result) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return b
}

type snapshotDoc struct {
	Data     map[string]string  `json:"data"`
	Sessions map[uint64]session `json:"sessions"`
}

// Snapshot serialises the whole state, sessions included. Dropping the session
// table here would silently break exactly-once after a restore.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(snapshotDoc{Data: s.data, Sessions: s.sessions})
}

// Restore replaces the state with a snapshot. An empty slice resets to empty.
func (s *Store) Restore(b []byte) error {
	doc := snapshotDoc{Data: map[string]string{}, Sessions: map[uint64]session{}}
	if len(b) > 0 {
		if err := json.Unmarshal(b, &doc); err != nil {
			return fmt.Errorf("kv: restore: %w", err)
		}
	}
	if doc.Data == nil {
		doc.Data = map[string]string{}
	}
	if doc.Sessions == nil {
		doc.Sessions = map[uint64]session{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.sessions = doc.Data, doc.Sessions
	return nil
}
