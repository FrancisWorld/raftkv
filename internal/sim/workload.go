package sim

import (
	"errors"
	"fmt"

	"github.com/FrancisWorld/raftkv/internal/kv"
	"github.com/FrancisWorld/raftkv/internal/lincheck"
	"github.com/FrancisWorld/raftkv/internal/node"
)

// Workload runs concurrent clients against a Cluster and records every
// operation as a lincheck history. Clients behave like real ones: they retry on
// not-leader, timeouts and lost leadership, and a retried write reuses its
// (client, seq) pair so the state machine deduplicates it.
type Workload struct {
	c       *Cluster
	clients []*client
	keys    []string
	history []lincheck.Operation
	stopped bool
	written map[string]string // last value any client tried to write per key

	// AttemptTimeout is how many ticks a client waits on one attempt before
	// trying another node.
	AttemptTimeout int64
	// Attempts counts every issued request, including retries.
	Attempts int
}

type client struct {
	id      int
	cid     uint64
	seq     uint64
	target  uint64
	retryAt int64
	op      *activeOp
}

type activeOp struct {
	rec    lincheck.Operation
	cmd    kv.Command
	isRead bool

	fut         *node.Future
	rep         *Replica
	incarnation int
	issuedAt    int64
}

// NewWorkload creates nClients clients operating over nKeys keys.
func NewWorkload(c *Cluster, nClients, nKeys int) *Workload {
	w := &Workload{c: c, written: map[string]string{}, AttemptTimeout: 60}
	for i := 0; i < nKeys; i++ {
		w.keys = append(w.keys, fmt.Sprintf("k%d", i))
	}
	for i := 0; i < nClients; i++ {
		w.clients = append(w.clients, &client{id: i, cid: uint64(1000 + i), target: c.ids[i%len(c.ids)]})
	}
	return w
}

// Stop prevents clients from starting new operations; in-flight ones continue.
func (w *Workload) Stop() { w.stopped = true }

// Idle reports whether no operation is in flight.
func (w *Workload) Idle() bool {
	for _, cl := range w.clients {
		if cl.op != nil {
			return false
		}
	}
	return true
}

// Completed is the number of operations with an observed result.
func (w *Workload) Completed() int { return len(w.history) }

// History returns the recorded operations. Writes still in flight are included
// as pending, since they may yet take effect; unanswered reads are dropped.
func (w *Workload) History() []lincheck.Operation {
	out := append([]lincheck.Operation(nil), w.history...)
	for _, cl := range w.clients {
		if cl.op != nil && !cl.op.isRead {
			rec := cl.op.rec
			rec.Return = lincheck.Pending
			out = append(out, rec)
		}
	}
	return out
}

// Poll advances every client by one decision. Call it once per cluster Step.
func (w *Workload) Poll() {
	for _, cl := range w.clients {
		w.pollClient(cl)
	}
}

func (w *Workload) pollClient(cl *client) {
	now := w.c.now
	if cl.op == nil {
		if w.stopped {
			return
		}
		cl.op = w.newOp(cl)
	}
	op := cl.op
	if op.fut != nil {
		select {
		case <-op.fut.Done():
			w.finishAttempt(cl, op)
		default:
			if now-op.issuedAt > w.AttemptTimeout {
				op.fut = nil // give up on this attempt; the entry may still commit
				cl.target = w.randomNode()
				cl.retryAt = now
			}
		}
		return
	}
	if now < cl.retryAt {
		return
	}
	rep := w.c.Replicas[cl.target]
	if !rep.Up {
		cl.target, cl.retryAt = w.randomNode(), now+1
		return
	}
	w.Attempts++
	op.rep, op.incarnation, op.issuedAt = rep, rep.Incarnation, now
	if op.isRead {
		op.fut = rep.Node.ReadBarrier()
	} else {
		op.fut = rep.Node.Propose(op.cmd.Encode())
	}
}

func (w *Workload) finishAttempt(cl *client, op *activeOp) {
	val, err := op.fut.Result()
	op.fut = nil
	now := w.c.now
	if err != nil {
		var nl *node.NotLeaderError
		switch {
		case errors.As(err, &nl) && nl.Leader != 0:
			cl.target = nl.Leader
		default:
			cl.target = w.randomNode()
		}
		cl.retryAt = now + 1
		return
	}
	if op.isRead {
		if !op.rep.Up || op.rep.Incarnation != op.incarnation {
			return // the node restarted since the barrier; just ask again
		}
		op.rec.Out, op.rec.Found = op.rep.Store.Get(op.rec.Key)
	} else {
		res, derr := kv.DecodeResult(val)
		if derr != nil || res.Error != "" {
			w.c.violate("client %d: write rejected by state machine: %v %+v", cl.id, derr, res)
		}
		op.rec.Found, op.rec.Out, op.rec.Applied = res.PrevFound, res.Prev, res.Applied
	}
	op.rec.Return = w.c.Stamp()
	w.history = append(w.history, op.rec)
	cl.op = nil
}

func (w *Workload) randomNode() uint64 { return w.c.ids[w.c.rng.IntN(len(w.c.ids))] }

func (w *Workload) newOp(cl *client) *activeOp {
	rng := w.c.rng
	cl.seq++
	key := w.keys[rng.IntN(len(w.keys))]
	val := fmt.Sprintf("c%d-%d", cl.id, cl.seq)
	op := &activeOp{rec: lincheck.Operation{Client: cl.id, Key: key, Call: w.c.Stamp()}}
	cmd := kv.Command{ClientID: cl.cid, Seq: cl.seq, Key: key}
	switch r := rng.IntN(100); {
	case r < 40:
		op.isRead = true
		op.rec.Kind = lincheck.Get
	case r < 75:
		op.rec.Kind, op.rec.Value = lincheck.Put, val
		cmd.Op, cmd.Value = kv.OpPut, val
	case r < 85:
		op.rec.Kind = lincheck.Delete
		cmd.Op = kv.OpDelete
	default:
		op.rec.Kind, op.rec.Value = lincheck.CAS, val
		cmd.Op, cmd.Value = kv.OpCAS, val
		switch guess := rng.IntN(10); {
		case guess < 6 && w.written[key] != "":
			cmd.Expected = w.written[key]
		case guess < 8:
			cmd.ExpectAbsent = true
		default:
			cmd.Expected = "never-written"
		}
		op.rec.Expected, op.rec.ExpectAbsent = cmd.Expected, cmd.ExpectAbsent
	}
	if !op.isRead && cmd.Op != kv.OpDelete {
		w.written[key] = val
	}
	op.cmd = cmd
	return op
}
