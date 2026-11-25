// Package raft is a from-scratch implementation of the Raft consensus
// algorithm. The core (Raft) is a deterministic, single-threaded state machine:
// it owns no goroutines, clocks, sockets or files. Callers feed it ticks and
// messages, then drain a Ready batch that says what to persist, send and apply.
// That separation is what makes the deterministic network simulator in
// internal/sim possible.
package raft

import (
	"errors"
	"fmt"
)

// EntryKind distinguishes client commands from the no-op a new leader appends
// to commit entries from earlier terms.
type EntryKind uint8

// Entry kinds.
const (
	EntryNormal EntryKind = iota
	EntryNoop
)

// Entry is one replicated log record.
type Entry struct {
	Index uint64    `json:"index"`
	Term  uint64    `json:"term"`
	Kind  EntryKind `json:"kind"`
	Data  []byte    `json:"data,omitempty"`
}

// HardState is the state that must survive a restart before any message that
// depends on it is sent.
type HardState struct {
	Term uint64 `json:"term"`
	Vote uint64 `json:"vote"`
}

// Snapshot is a state machine image covering every entry up to and including
// Index, whose term was Term.
type Snapshot struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data,omitempty"`
}

// MsgType identifies a protocol message.
type MsgType uint8

// Message types.
const (
	MsgPreVote MsgType = iota + 1
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgSnap
)

var msgNames = map[MsgType]string{
	MsgPreVote: "PreVote", MsgPreVoteResp: "PreVoteResp",
	MsgVote: "Vote", MsgVoteResp: "VoteResp",
	MsgApp: "App", MsgAppResp: "AppResp",
	MsgHeartbeat: "Heartbeat", MsgHeartbeatResp: "HeartbeatResp",
	MsgSnap: "Snap",
}

func (t MsgType) String() string {
	if s, ok := msgNames[t]; ok {
		return s
	}
	return fmt.Sprintf("MsgType(%d)", uint8(t))
}

// Valid reports whether t is a known message type.
func (t MsgType) Valid() bool { _, ok := msgNames[t]; return ok }

// Message is the single wire format for every Raft RPC. Which fields are
// meaningful depends on Type:
//
//	PreVote/Vote:  LogIndex, LogTerm describe the candidate's last entry.
//	App:           LogIndex, LogTerm describe the entry preceding Entries.
//	AppResp:       Index is the new match index on success, or the rejected
//	               prev index on reject (with ConflictTerm/ConflictIndex).
//	Heartbeat:     Commit and Context (the newest pending read id).
//	Snap:          Snapshot.
type Message struct {
	Type MsgType `json:"type"`
	From uint64  `json:"from"`
	To   uint64  `json:"to"`
	Term uint64  `json:"term"`

	LogIndex uint64    `json:"log_index,omitempty"`
	LogTerm  uint64    `json:"log_term,omitempty"`
	Entries  []Entry   `json:"entries,omitempty"`
	Commit   uint64    `json:"commit,omitempty"`
	Reject   bool      `json:"reject,omitempty"`
	Index    uint64    `json:"index,omitempty"`
	Context  uint64    `json:"context,omitempty"`
	Snapshot *Snapshot `json:"snapshot,omitempty"`

	// ConflictTerm and ConflictIndex let the leader skip a whole conflicting
	// term per round trip instead of one entry.
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
}

// Clone returns a copy that shares no mutable slice headers with m, so a
// simulated network can hand it to another node safely.
func (m Message) Clone() Message {
	if m.Entries != nil {
		m.Entries = append([]Entry(nil), m.Entries...)
	}
	if m.Snapshot != nil {
		s := *m.Snapshot
		m.Snapshot = &s
	}
	return m
}

// ReadState says a ReadIndex request has been confirmed: the read may be served
// once the state machine has applied at least Index.
type ReadState struct {
	ID    uint64
	Index uint64
}

// Ready is one batch of work for the caller. The caller must, in order:
// persist HardState, Snapshot and Entries; send Messages; restore the state
// machine from Snapshot; apply CommittedEntries.
type Ready struct {
	HardState        *HardState
	Snapshot         *Snapshot
	Entries          []Entry
	Messages         []Message
	CommittedEntries []Entry
	ReadStates       []ReadState
}

// State is what Storage hands back on startup.
type State struct {
	HardState HardState
	Snapshot  Snapshot
	// Entries holds the log after Snapshot.Index, contiguous and in order.
	Entries []Entry
}

// Storage is durable storage for the replicated log. Every method must be
// durable (fsynced) when it returns.
type Storage interface {
	// Load returns the persisted state. It is called once at startup.
	Load() (State, error)
	// Save persists hs (if non-nil) and entries. Any stored entry with an
	// index >= entries[0].Index is discarded first.
	Save(hs *HardState, entries []Entry) error
	// InstallSnapshot replaces the whole log with snap, discarding all entries.
	InstallSnapshot(snap Snapshot) error
	// Compact persists snap and drops entries up to snap.Index, keeping the tail.
	Compact(snap Snapshot) error
	Close() error
}

// Errors returned by Raft.
var (
	// ErrNotLeader is returned when a leader-only operation reaches a follower.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrNotReady is returned by ReadIndex when the leader has not yet committed
	// an entry of its own term, so its commit index is not trustworthy.
	ErrNotReady = errors.New("raft: leader has not committed an entry in its term")
)
