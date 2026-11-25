// Package sim is a deterministic simulation harness for raftkv clusters. All
// nodes run in one goroutine on a virtual clock; the only source of
// nondeterminism is a seeded PRNG, so a failing seed replays exactly.
package sim

import (
	"container/heap"
	"math/rand/v2"

	"github.com/FrancisWorld/raftkv/internal/raft"
)

type envelope struct {
	at  int64
	seq uint64
	msg raft.Message
}

type envHeap []envelope

func (h envHeap) Len() int { return len(h) }
func (h envHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h envHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *envHeap) Push(x any)   { *h = append(*h, x.(envelope)) }
func (h *envHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

// NetStats counts what the network did to traffic.
type NetStats struct {
	Sent, Delivered, Dropped, Duplicated, Blocked uint64
}

// Network is a lossy, reordering, delaying message fabric. Delivery time is
// send time plus a random delay in [MinDelay, MaxDelay] ticks, so messages
// overtake each other freely.
type Network struct {
	rng *rand.Rand
	now int64
	seq uint64
	q   envHeap

	MinDelay, MaxDelay int
	DropRate           float64
	DupRate            float64

	blocked map[[2]uint64]bool
	Stats   NetStats
}

func newNetwork(rng *rand.Rand, minDelay, maxDelay int) *Network {
	return &Network{rng: rng, MinDelay: minDelay, MaxDelay: maxDelay, blocked: map[[2]uint64]bool{}}
}

// send enqueues m subject to partitions, loss, duplication and delay.
func (n *Network) send(m raft.Message) {
	n.Stats.Sent++
	if n.blocked[[2]uint64{m.From, m.To}] {
		n.Stats.Blocked++
		return
	}
	if n.rng.Float64() < n.DropRate {
		n.Stats.Dropped++
		return
	}
	n.enqueue(m)
	if n.rng.Float64() < n.DupRate {
		n.Stats.Duplicated++
		n.enqueue(m)
	}
}

func (n *Network) enqueue(m raft.Message) {
	delay := n.MinDelay
	if span := n.MaxDelay - n.MinDelay; span > 0 {
		delay += n.rng.IntN(span + 1)
	}
	n.seq++
	heap.Push(&n.q, envelope{at: n.now + int64(delay), seq: n.seq, msg: m.Clone()})
}

// popDue returns the next message due at or before the current time.
func (n *Network) popDue() (raft.Message, bool) {
	if len(n.q) == 0 || n.q[0].at > n.now {
		return raft.Message{}, false
	}
	n.Stats.Delivered++
	return heap.Pop(&n.q).(envelope).msg, true
}

// Partition splits the given groups from each other; nodes within a group keep
// talking. Nodes not listed are cut off from everyone.
func (n *Network) Partition(ids []uint64, groups ...[]uint64) {
	n.blocked = map[[2]uint64]bool{}
	group := map[uint64]int{}
	for gi, g := range groups {
		for _, id := range g {
			group[id] = gi + 1
		}
	}
	for _, a := range ids {
		for _, b := range ids {
			if a != b && (group[a] == 0 || group[a] != group[b]) {
				n.blocked[[2]uint64{a, b}] = true
			}
		}
	}
}

// Heal removes every partition.
func (n *Network) Heal() { n.blocked = map[[2]uint64]bool{} }

type netTransport struct{ net *Network }

func (t netTransport) Send(m raft.Message) { t.net.send(m) }
