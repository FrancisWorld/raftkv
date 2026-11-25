package sim

import (
	"fmt"
	"testing"

	"github.com/FrancisWorld/raftkv/internal/lincheck"
	"github.com/FrancisWorld/raftkv/internal/raft"
	"github.com/FrancisWorld/raftkv/internal/wal"
)

type chaosReport struct {
	ops, attempts, pending int
	net                    NetStats
	elections              int
}

// runChaos drives a cluster and a concurrent workload through random
// partitions, crashes, message loss and duplication, then heals everything and
// checks safety (no two leaders per term, identical commands at each index,
// identical final state) and liveness (every client finishes), and finally that
// the recorded history is linearizable.
func runChaos(t *testing.T, seed uint64, nodes, chaosTicks int, storage StorageFactory) chaosReport {
	t.Helper()
	c, err := NewCluster(Options{Nodes: nodes, Seed: seed, SnapshotThreshold: 25, MinDelay: 1, MaxDelay: 4, Storage: storage})
	if err != nil {
		t.Fatal(err)
	}
	w := NewWorkload(c, 6, 3)
	rng := c.Rand()
	downUntil := map[uint64]int{}

	for tick := 0; tick < chaosTicks; tick++ {
		if tick%120 == 0 {
			switch rng.IntN(6) {
			case 0: // split into two random groups
				var a, b []uint64
				for _, id := range c.IDs() {
					if rng.IntN(2) == 0 {
						a = append(a, id)
					} else {
						b = append(b, id)
					}
				}
				c.Net.Partition(c.IDs(), a, b)
			case 1: // isolate the current leader
				if l, ok := c.Leader(); ok {
					var rest []uint64
					for _, id := range c.IDs() {
						if id != l {
							rest = append(rest, id)
						}
					}
					c.Net.Partition(c.IDs(), []uint64{l}, rest)
				}
			case 2:
				c.Net.Heal()
			case 3: // crash a random node for a while
				id := c.IDs()[rng.IntN(nodes)]
				if c.Replicas[id].Up {
					c.Crash(id)
					downUntil[id] = tick + 50 + rng.IntN(250)
				}
			case 4:
				c.Net.DropRate = float64(rng.IntN(25)) / 100
			case 5:
				c.Net.DupRate = float64(rng.IntN(20)) / 100
			}
		}
		for id, until := range downUntil {
			if tick >= until {
				delete(downUntil, id)
				if err := c.Restart(id); err != nil {
					t.Fatal(err)
				}
			}
		}
		c.Step()
		w.Poll()
	}

	// Heal everything and let the system drain.
	c.Net.Heal()
	c.Net.DropRate, c.Net.DupRate = 0, 0
	for id := range downUntil {
		if err := c.Restart(id); err != nil {
			t.Fatal(err)
		}
	}
	w.Stop()
	for i := 0; i < 3000 && (!w.Idle() || !c.Converged()); i++ {
		c.Step()
		w.Poll()
	}
	if !w.Idle() {
		t.Errorf("seed %d: clients still blocked after healing; the cluster lost liveness", seed)
	}
	if !c.Converged() {
		t.Errorf("seed %d: replicas did not converge after healing", seed)
	}
	for _, v := range c.Violations() {
		t.Errorf("seed %d: %s", seed, v)
	}

	hist := w.History()
	res, err := lincheck.Check(hist, 0)
	if err != nil {
		t.Fatalf("seed %d: linearizability check inconclusive: %v", seed, err)
	}
	if !res.Linearizable {
		t.Errorf("seed %d: history is NOT linearizable (key %q)", seed, res.Key)
		for _, op := range hist {
			if op.Key == res.Key {
				t.Log(op)
			}
		}
	}
	rep := chaosReport{ops: w.Completed(), attempts: w.Attempts, pending: len(hist) - w.Completed(), net: c.Net.Stats}
	for _, id := range c.IDs() {
		rep.elections += int(c.Replicas[id].Node.Status().Stats.LeaderChanges)
	}
	return rep
}

func TestChaosLinearizableAndSafe(t *testing.T) {
	seeds := 24
	if testing.Short() {
		seeds = 4
	}
	for seed := 1; seed <= seeds; seed++ {
		nodes := 3 + 2*(seed%2) // alternate 5- and 3-node clusters
		t.Run(fmt.Sprintf("seed=%d/nodes=%d", seed, nodes), func(t *testing.T) {
			t.Parallel()
			rep := runChaos(t, uint64(seed), nodes, 4000, nil)
			if rep.ops < 50 {
				t.Errorf("only %d operations completed; the workload barely ran", rep.ops)
			}
			t.Logf("ops=%d attempts=%d pending=%d net=%+v", rep.ops, rep.attempts, rep.pending, rep.net)
		})
	}
}

// The same chaos run with real on-disk WALs: crashes close the files and
// restarts recover from them, exercising fsync'd persistence end to end.
func TestChaosWithDiskWAL(t *testing.T) {
	if testing.Short() {
		t.Skip("fsync-heavy; skipped in -short mode")
	}
	for _, seed := range []uint64{101} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			dir := t.TempDir()
			factory := func(id uint64) (raft.Storage, error) {
				return wal.Open(fmt.Sprintf("%s/node-%d", dir, id))
			}
			rep := runChaos(t, seed, 3, 1200, factory)
			if rep.ops < 50 {
				t.Errorf("only %d operations completed", rep.ops)
			}
		})
	}
}
