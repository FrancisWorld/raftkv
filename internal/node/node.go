// Package node drives a raft.Raft: it persists what Raft asks to persist, sends
// its messages, applies committed entries to a state machine, resolves client
// futures, and triggers log compaction. It is deliberately free of goroutines
// and clocks: callers invoke Tick and Receive, so a simulator can drive many
// nodes deterministically and a server can drive one from a ticker.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/FrancisWorld/raftkv/internal/raft"
)

// StateMachine is the application replicated by Raft.
type StateMachine interface {
	// Apply executes the committed command stored at log position index and
	// returns its result. It must be deterministic.
	Apply(index uint64, data []byte) []byte
	// Snapshot serialises the current state.
	Snapshot() ([]byte, error)
	// Restore replaces the state with a snapshot.
	Restore(data []byte) error
}

// Transport sends Raft messages to peers on a best-effort basis. Losing,
// delaying or reordering messages is allowed; blocking is not.
type Transport interface {
	Send(m raft.Message)
}

// Errors reported through futures.
var (
	// ErrLeadershipLost means the node stopped being leader while the operation
	// was in flight. A write may or may not have been committed.
	ErrLeadershipLost = errors.New("node: leadership lost, outcome unknown")
	// ErrNotReady means a new leader has not yet committed an entry of its own term.
	ErrNotReady = raft.ErrNotReady
	// ErrStopped means the node was closed.
	ErrStopped = errors.New("node: stopped")
)

// NotLeaderError says the operation must be sent to Leader (0 when unknown).
type NotLeaderError struct{ Leader uint64 }

func (e *NotLeaderError) Error() string {
	if e.Leader == 0 {
		return "node: not the leader and no leader is known"
	}
	return fmt.Sprintf("node: not the leader; leader is %d", e.Leader)
}

// Config configures a Node.
type Config struct {
	Raft      raft.Config
	Storage   raft.Storage
	SM        StateMachine
	Transport Transport
	// SnapshotThreshold compacts the log once this many entries have been
	// applied since the last snapshot. Zero disables compaction.
	SnapshotThreshold uint64
	Logger            *slog.Logger
}

// Stats are monotonic counters for observability.
type Stats struct {
	Proposals          uint64
	AppliedEntries     uint64
	SnapshotsTaken     uint64
	SnapshotsInstalled uint64
	MessagesSent       uint64
	MessagesReceived   uint64
	ReadBarriers       uint64
	LeaderChanges      uint64
}

// Status is a point-in-time view of the node.
type Status struct {
	raft.Status
	Stats Stats
}

type proposal struct {
	term uint64
	fut  *Future
}

type readWait struct {
	term  uint64
	index uint64
	ready bool
	fut   *Future
}

// Node couples a Raft state machine with storage, transport and an application.
type Node struct {
	cfg Config
	log *slog.Logger

	mu       sync.Mutex
	raft     *raft.Raft
	applied  uint64
	pending  map[uint64]*proposal // by log index
	reads    map[uint64]*readWait // by ReadIndex id
	stats    Stats
	fatal    error
	stopped  bool
	lastRole raft.Role
	lastTerm uint64
	lastLead uint64
}

// New loads persisted state, restores the state machine from the latest
// snapshot and returns a follower node.
func New(cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	st, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("node: load storage: %w", err)
	}
	r, err := raft.New(cfg.Raft, st)
	if err != nil {
		return nil, err
	}
	if st.Snapshot.Index > 0 {
		if err := cfg.SM.Restore(st.Snapshot.Data); err != nil {
			return nil, fmt.Errorf("node: restore snapshot: %w", err)
		}
	}
	n := &Node{
		cfg: cfg, log: cfg.Logger.With("node", cfg.Raft.ID), raft: r,
		applied: st.Snapshot.Index,
		pending: map[uint64]*proposal{}, reads: map[uint64]*readWait{},
	}
	s := r.Status()
	n.lastRole, n.lastTerm, n.lastLead = s.Role, s.Term, s.Leader
	n.log.Info("node started", "term", s.Term, "last_index", s.LastIndex, "snapshot_index", s.SnapshotIndex)
	return n, nil
}

// Tick advances the logical clock by one tick.
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.fatal != nil {
		return
	}
	n.raft.Tick()
	n.process()
}

// Receive delivers a message from a peer.
func (n *Node) Receive(m raft.Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.fatal != nil {
		return
	}
	n.stats.MessagesReceived++
	n.raft.Step(m)
	n.process()
}

// Propose submits a command. The future resolves with the state machine's
// result once the command is committed and applied.
func (n *Node) Propose(data []byte) *Future {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.unavailable(); err != nil {
		return failedFuture(err)
	}
	index, term, err := n.raft.Propose(data)
	if err != nil {
		return failedFuture(n.notLeader())
	}
	n.stats.Proposals++
	if old, ok := n.pending[index]; ok {
		old.fut.resolve(nil, ErrLeadershipLost)
	}
	p := &proposal{term: term, fut: newFuture()}
	n.pending[index] = p
	n.process()
	return p.fut
}

// ReadBarrier starts a linearizable read. When the future resolves without
// error, the state machine reflects every write acknowledged before the call.
func (n *Node) ReadBarrier() *Future {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.unavailable(); err != nil {
		return failedFuture(err)
	}
	id, err := n.raft.ReadIndex()
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return failedFuture(n.notLeader())
	case err != nil:
		return failedFuture(err)
	}
	n.stats.ReadBarriers++
	w := &readWait{term: n.raft.Status().Term, fut: newFuture()}
	n.reads[id] = w
	n.process()
	return w.fut
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{Status: n.raft.Status(), Stats: n.stats}
}

// Err returns the fatal error that stopped the node, if any.
func (n *Node) Err() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.fatal
}

// Close stops the node, fails outstanding operations and closes storage.
func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil
	}
	n.stopped = true
	n.failAll(ErrStopped)
	return n.cfg.Storage.Close()
}

// RunTicker calls Tick every interval until ctx ends.
func (n *Node) RunTicker(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.Tick()
		}
	}
}

func (n *Node) unavailable() error {
	switch {
	case n.stopped:
		return ErrStopped
	case n.fatal != nil:
		return n.fatal
	}
	return nil
}

func (n *Node) notLeader() error { return &NotLeaderError{Leader: n.raft.Status().Leader} }

// process drains Raft until it has no more work. The mutex is held.
func (n *Node) process() {
	for n.raft.HasReady() {
		rd := n.raft.Ready()
		if err := n.persist(rd); err != nil {
			n.failStop(fmt.Errorf("node: persist: %w", err))
			return
		}
		// Only now is it safe to tell peers anything that depends on that state.
		for _, m := range rd.Messages {
			n.stats.MessagesSent++
			n.cfg.Transport.Send(m)
		}
		if rd.Snapshot != nil {
			if err := n.cfg.SM.Restore(rd.Snapshot.Data); err != nil {
				n.failStop(fmt.Errorf("node: restore snapshot: %w", err))
				return
			}
			n.applied = rd.Snapshot.Index
			n.stats.SnapshotsInstalled++
			n.log.Info("installed snapshot", "index", rd.Snapshot.Index)
		}
		for _, e := range rd.CommittedEntries {
			n.apply(e)
		}
		for _, rs := range rd.ReadStates {
			if w, ok := n.reads[rs.ID]; ok {
				w.index, w.ready = rs.Index, true
			}
		}
		if err := n.maybeSnapshot(); err != nil {
			n.failStop(err)
			return
		}
	}
	n.releaseReads()
	n.failStale()
	n.observe()
}

func (n *Node) persist(rd raft.Ready) error {
	if rd.Snapshot != nil {
		if err := n.cfg.Storage.InstallSnapshot(*rd.Snapshot); err != nil {
			return err
		}
	}
	if rd.HardState != nil || len(rd.Entries) > 0 {
		return n.cfg.Storage.Save(rd.HardState, rd.Entries)
	}
	return nil
}

func (n *Node) apply(e raft.Entry) {
	var res []byte
	if e.Kind == raft.EntryNormal {
		res = n.cfg.SM.Apply(e.Index, e.Data)
		n.stats.AppliedEntries++
	}
	n.applied = e.Index
	if p, ok := n.pending[e.Index]; ok {
		delete(n.pending, e.Index)
		if p.term == e.Term {
			p.fut.resolve(res, nil)
		} else {
			// Our proposal at this index was overwritten by another leader's entry.
			p.fut.resolve(nil, ErrLeadershipLost)
		}
	}
}

func (n *Node) releaseReads() {
	for id, w := range n.reads {
		if w.ready && w.index <= n.applied {
			delete(n.reads, id)
			w.fut.resolve(nil, nil)
		}
	}
}

// failStale fails operations that belong to a leadership the node no longer holds.
func (n *Node) failStale() {
	s := n.raft.Status()
	for idx, p := range n.pending {
		if s.Role != raft.Leader || p.term != s.Term {
			delete(n.pending, idx)
			p.fut.resolve(nil, ErrLeadershipLost)
		}
	}
	for id, w := range n.reads {
		if s.Role != raft.Leader || w.term != s.Term {
			delete(n.reads, id)
			w.fut.resolve(nil, n.notLeader())
		}
	}
}

func (n *Node) maybeSnapshot() error {
	th := n.cfg.SnapshotThreshold
	if th == 0 || n.applied-n.raft.Status().SnapshotIndex < th {
		return nil
	}
	data, err := n.cfg.SM.Snapshot()
	if err != nil {
		return fmt.Errorf("node: snapshot state machine: %w", err)
	}
	if err := n.raft.Compact(n.applied, data); err != nil {
		return err
	}
	if err := n.cfg.Storage.Compact(n.raft.Snapshot()); err != nil {
		return fmt.Errorf("node: persist snapshot: %w", err)
	}
	n.stats.SnapshotsTaken++
	n.log.Info("compacted log", "index", n.applied)
	return nil
}

// observe logs role and leader transitions.
func (n *Node) observe() {
	s := n.raft.Status()
	if s.Role == n.lastRole && s.Term == n.lastTerm && s.Leader == n.lastLead {
		return
	}
	if s.Leader != n.lastLead && s.Leader != 0 {
		n.stats.LeaderChanges++
	}
	n.log.Info("raft state changed", "role", s.Role.String(), "term", s.Term, "leader", s.Leader)
	n.lastRole, n.lastTerm, n.lastLead = s.Role, s.Term, s.Leader
}

func (n *Node) failStop(err error) {
	n.fatal = err
	n.log.Error("node halted", "err", err)
	n.failAll(err)
}

func (n *Node) failAll(err error) {
	for idx, p := range n.pending {
		delete(n.pending, idx)
		p.fut.resolve(nil, err)
	}
	for id, w := range n.reads {
		delete(n.reads, id)
		w.fut.resolve(nil, err)
	}
}
