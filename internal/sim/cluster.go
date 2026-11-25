package sim

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"

	"github.com/FrancisWorld/raftkv/internal/kv"
	"github.com/FrancisWorld/raftkv/internal/node"
	"github.com/FrancisWorld/raftkv/internal/raft"
)

// StorageFactory returns the storage for node id. It is called again on every
// restart and must hand back the node's previously persisted data.
type StorageFactory func(id uint64) (raft.Storage, error)

// Options configures a simulated cluster.
type Options struct {
	Nodes             int
	Seed              uint64
	ElectionTick      int
	HeartbeatTick     int
	SnapshotThreshold uint64
	MinDelay          int
	MaxDelay          int
	// Storage defaults to in-memory storage that survives restarts.
	Storage StorageFactory
}

// Replica is one simulated server.
type Replica struct {
	ID          uint64
	Node        *node.Node
	Store       *kv.Store
	Up          bool
	Incarnation int
}

// Cluster is a set of replicas wired through a simulated network.
type Cluster struct {
	opts     Options
	rng      *rand.Rand
	Net      *Network
	ids      []uint64
	Replicas map[uint64]*Replica
	storage  StorageFactory
	now      int64
	events   int64

	leaders    map[uint64]uint64 // term -> leader id
	committed  map[uint64]uint64 // log index -> hash of the command applied there
	violations []string
}

// NewCluster starts every node.
func NewCluster(opts Options) (*Cluster, error) {
	if opts.ElectionTick == 0 {
		opts.ElectionTick = 10
	}
	if opts.HeartbeatTick == 0 {
		opts.HeartbeatTick = 1
	}
	if opts.MinDelay == 0 {
		opts.MinDelay = 1
	}
	if opts.MaxDelay < opts.MinDelay {
		opts.MaxDelay = opts.MinDelay + 2
	}
	rng := rand.New(rand.NewPCG(opts.Seed, 0x5eed))
	c := &Cluster{
		opts: opts, rng: rng,
		Net:       newNetwork(rng, opts.MinDelay, opts.MaxDelay),
		Replicas:  map[uint64]*Replica{},
		leaders:   map[uint64]uint64{},
		committed: map[uint64]uint64{},
		storage:   opts.Storage,
	}
	if c.storage == nil {
		stores := map[uint64]*raft.MemStorage{}
		c.storage = func(id uint64) (raft.Storage, error) {
			if stores[id] == nil {
				stores[id] = raft.NewMemStorage()
			}
			return stores[id], nil
		}
	}
	for i := 1; i <= opts.Nodes; i++ {
		c.ids = append(c.ids, uint64(i))
	}
	for _, id := range c.ids {
		c.Replicas[id] = &Replica{ID: id}
		if err := c.Restart(id); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// IDs returns the node ids in ascending order.
func (c *Cluster) IDs() []uint64 { return c.ids }

// Now returns the virtual time in ticks.
func (c *Cluster) Now() int64 { return c.now }

// Stamp returns a strictly increasing event number used to order history events.
func (c *Cluster) Stamp() int64 { c.events++; return c.events }

// Rand exposes the cluster PRNG so workloads stay on the same seed.
func (c *Cluster) Rand() *rand.Rand { return c.rng }

// Violations lists safety violations detected so far.
func (c *Cluster) Violations() []string { return c.violations }

func (c *Cluster) violate(format string, args ...any) {
	c.violations = append(c.violations, fmt.Sprintf("t=%d: ", c.now)+fmt.Sprintf(format, args...))
}

// recorder wraps the KV store to check state machine safety: every node must
// apply the same command at the same log index.
type recorder struct {
	c     *Cluster
	id    uint64
	inner *kv.Store
}

func (r *recorder) Apply(index uint64, data []byte) []byte {
	h := fnv.New64a()
	_, _ = h.Write(data)
	sum := h.Sum64()
	if prev, ok := r.c.committed[index]; ok && prev != sum {
		r.c.violate("node %d applied a different command at index %d than another node", r.id, index)
	}
	r.c.committed[index] = sum
	return r.inner.Apply(index, data)
}
func (r *recorder) Snapshot() ([]byte, error) { return r.inner.Snapshot() }
func (r *recorder) Restore(b []byte) error    { return r.inner.Restore(b) }

// Restart (re)creates node id from its persisted storage. Calling it on a
// running node first crashes it.
func (c *Cluster) Restart(id uint64) error {
	rep := c.Replicas[id]
	if rep.Up {
		c.Crash(id)
	}
	st, err := c.storage(id)
	if err != nil {
		return err
	}
	store := kv.NewStore()
	n, err := node.New(node.Config{
		Raft: raft.Config{
			ID: id, Peers: c.ids, ElectionTick: c.opts.ElectionTick, HeartbeatTick: c.opts.HeartbeatTick,
			PreVote: true, CheckQuorum: true, Seed: c.opts.Seed + uint64(rep.Incarnation)*1000,
		},
		Storage:           st,
		SM:                &recorder{c: c, id: id, inner: store},
		Transport:         netTransport{c.Net},
		SnapshotThreshold: c.opts.SnapshotThreshold,
	})
	if err != nil {
		return err
	}
	rep.Node, rep.Store, rep.Up = n, store, true
	rep.Incarnation++
	return nil
}

// Crash stops node id, losing all volatile state. Its storage survives.
func (c *Cluster) Crash(id uint64) {
	rep := c.Replicas[id]
	if !rep.Up {
		return
	}
	_ = rep.Node.Close()
	rep.Up = false
}

// Step advances virtual time by one tick: deliver every due message, then tick
// every running node.
func (c *Cluster) Step() {
	c.now++
	c.Net.now = c.now
	for {
		m, ok := c.Net.popDue()
		if !ok {
			break
		}
		if rep := c.Replicas[m.To]; rep != nil && rep.Up {
			rep.Node.Receive(m)
		}
	}
	for _, id := range c.ids {
		if rep := c.Replicas[id]; rep.Up {
			rep.Node.Tick()
		}
	}
	c.observe()
}

// Run steps n ticks.
func (c *Cluster) Run(n int) {
	for i := 0; i < n; i++ {
		c.Step()
	}
}

// observe checks election safety: at most one leader per term.
func (c *Cluster) observe() {
	for _, id := range c.ids {
		rep := c.Replicas[id]
		if !rep.Up {
			continue
		}
		s := rep.Node.Status()
		if s.Role != raft.Leader {
			continue
		}
		if prev, ok := c.leaders[s.Term]; ok && prev != id {
			c.violate("two leaders in term %d: %d and %d", s.Term, prev, id)
		}
		c.leaders[s.Term] = id
	}
}

// Leader returns the running leader with the highest term, if any.
func (c *Cluster) Leader() (uint64, bool) {
	var best uint64
	var bestTerm uint64
	for _, id := range c.ids {
		rep := c.Replicas[id]
		if !rep.Up {
			continue
		}
		if s := rep.Node.Status(); s.Role == raft.Leader && s.Term >= bestTerm {
			best, bestTerm = id, s.Term
		}
	}
	return best, best != 0
}

// WaitLeader steps until some running node is leader, or gives up.
func (c *Cluster) WaitLeader(maxTicks int) (uint64, bool) {
	for i := 0; i < maxTicks; i++ {
		c.Step()
		if id, ok := c.Leader(); ok {
			return id, true
		}
	}
	return 0, false
}

// Digest hashes a node's full state machine (data and sessions).
func (c *Cluster) Digest(id uint64) (uint64, error) {
	b, err := c.Replicas[id].Store.Snapshot()
	if err != nil {
		return 0, err
	}
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64(), nil
}

// Converged reports whether every running node has applied the same prefix and
// holds an identical state machine.
func (c *Cluster) Converged() bool {
	var digests []uint64
	var applied []uint64
	for _, id := range c.ids {
		if rep := c.Replicas[id]; rep.Up {
			d, err := c.Digest(id)
			if err != nil {
				return false
			}
			digests = append(digests, d)
			applied = append(applied, rep.Node.Status().Applied)
		}
	}
	return len(digests) > 0 &&
		!slices.ContainsFunc(digests, func(d uint64) bool { return d != digests[0] }) &&
		!slices.ContainsFunc(applied, func(a uint64) bool { return a != applied[0] })
}
