package sim

import (
	"errors"
	"testing"

	"github.com/FrancisWorld/raftkv/internal/kv"
	"github.com/FrancisWorld/raftkv/internal/node"
)

func newCluster(t *testing.T, opts Options) *Cluster {
	t.Helper()
	if opts.Nodes == 0 {
		opts.Nodes = 3
	}
	c, err := NewCluster(opts)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustLeader(t *testing.T, c *Cluster) uint64 {
	t.Helper()
	id, ok := c.WaitLeader(500)
	if !ok {
		t.Fatal("no leader elected")
	}
	return id
}

// write proposes cmd on node id and steps the cluster until it resolves.
func write(t *testing.T, c *Cluster, id uint64, cmd kv.Command) kv.Result {
	t.Helper()
	fut := c.Replicas[id].Node.Propose(cmd.Encode())
	for i := 0; i < 300; i++ {
		select {
		case <-fut.Done():
			val, err := fut.Result()
			if err != nil {
				t.Fatalf("write on node %d: %v", id, err)
			}
			res, err := kv.DecodeResult(val)
			if err != nil {
				t.Fatal(err)
			}
			return res
		default:
			c.Step()
		}
	}
	t.Fatalf("write on node %d did not complete", id)
	return kv.Result{}
}

func settle(t *testing.T, c *Cluster) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if c.Converged() {
			return
		}
		c.Step()
	}
	t.Fatal("cluster did not converge")
}

func noViolations(t *testing.T, c *Cluster) {
	t.Helper()
	for _, v := range c.Violations() {
		t.Error(v)
	}
}

func TestReplicatesToAllNodes(t *testing.T) {
	c := newCluster(t, Options{Seed: 1})
	l := mustLeader(t, c)
	for i, v := range []string{"a", "b", "c"} {
		write(t, c, l, kv.Command{ClientID: 1, Seq: uint64(i + 1), Op: kv.OpPut, Key: "k" + v, Value: v})
	}
	settle(t, c)
	for _, id := range c.IDs() {
		if v, ok := c.Replicas[id].Store.Get("kb"); !ok || v != "b" {
			t.Fatalf("node %d: kb = %q %v", id, v, ok)
		}
	}
	noViolations(t, c)
}

func TestSameSeedReplaysIdentically(t *testing.T) {
	run := func() (uint64, int, NetStats, int64) {
		c := newCluster(t, Options{Nodes: 5, Seed: 99, SnapshotThreshold: 15})
		w := NewWorkload(c, 4, 3)
		c.Net.DropRate = 0.05
		for i := 0; i < 1500; i++ {
			if i == 400 {
				c.Net.Partition(c.IDs(), []uint64{1, 2, 3}, []uint64{4, 5})
			}
			if i == 700 {
				c.Net.Heal()
			}
			c.Step()
			w.Poll()
		}
		w.Stop()
		c.Net.DropRate = 0
		for i := 0; i < 600 && (!w.Idle() || !c.Converged()); i++ {
			c.Step()
			w.Poll()
		}
		d, err := c.Digest(1)
		if err != nil {
			t.Fatal(err)
		}
		return d, w.Completed(), c.Net.Stats, c.Now()
	}
	d1, n1, s1, t1 := run()
	d2, n2, s2, t2 := run()
	if d1 != d2 || n1 != n2 || s1 != s2 || t1 != t2 {
		t.Fatalf("runs diverged: (%x,%d,%+v,%d) vs (%x,%d,%+v,%d)", d1, n1, s1, t1, d2, n2, s2, t2)
	}
	if n1 == 0 {
		t.Fatal("workload made no progress")
	}
}

func TestLeaderCrashFailover(t *testing.T) {
	c := newCluster(t, Options{Seed: 3})
	l := mustLeader(t, c)
	write(t, c, l, kv.Command{ClientID: 1, Seq: 1, Op: kv.OpPut, Key: "x", Value: "before"})
	settle(t, c)
	c.Crash(l)
	nl := mustLeaderOtherThan(t, c, l)
	write(t, c, nl, kv.Command{ClientID: 1, Seq: 2, Op: kv.OpPut, Key: "y", Value: "after"})
	if err := c.Restart(l); err != nil {
		t.Fatal(err)
	}
	settle(t, c)
	for _, id := range c.IDs() {
		x, _ := c.Replicas[id].Store.Get("x")
		y, _ := c.Replicas[id].Store.Get("y")
		if x != "before" || y != "after" {
			t.Fatalf("node %d: x=%q y=%q", id, x, y)
		}
	}
	noViolations(t, c)
}

func mustLeaderOtherThan(t *testing.T, c *Cluster, old uint64) uint64 {
	t.Helper()
	for i := 0; i < 500; i++ {
		c.Step()
		if id, ok := c.Leader(); ok && id != old && c.Replicas[id].Up {
			return id
		}
	}
	t.Fatalf("no new leader after %d", old)
	return 0
}

// A write that committed but whose reply was lost is retried at the new leader
// with the same (client, seq). It must not execute twice.
func TestRetryAfterLostReplyIsExactlyOnce(t *testing.T) {
	c := newCluster(t, Options{Seed: 4})
	l := mustLeader(t, c)
	cmd := kv.Command{ClientID: 9, Seq: 1, Op: kv.OpCAS, Key: "once", ExpectAbsent: true, Value: "v"}
	_ = c.Replicas[l].Node.Propose(cmd.Encode()) // the reply is "lost": nobody reads this future
	for i := 0; i < 100; i++ {
		c.Step()
	}
	for _, id := range c.IDs() {
		if _, ok := c.Replicas[id].Store.Get("once"); !ok {
			t.Fatalf("node %d never applied the write", id)
		}
	}
	c.Crash(l)
	nl := mustLeaderOtherThan(t, c, l)
	res := write(t, c, nl, cmd)
	if !res.Applied || !res.Duplicate {
		t.Fatalf("retry result = %+v; want the original success replayed as a duplicate", res)
	}
	// Re-execution would have failed the absent-precondition; the value must be intact.
	if v, _ := c.Replicas[nl].Store.Get("once"); v != "v" {
		t.Fatalf("value = %q", v)
	}
}

func TestMinorityLeaderCannotCommitOrServeReads(t *testing.T) {
	c := newCluster(t, Options{Nodes: 5, Seed: 5})
	old := mustLeader(t, c)
	write(t, c, old, kv.Command{ClientID: 1, Seq: 1, Op: kv.OpPut, Key: "k", Value: "v1"})
	settle(t, c)

	var rest []uint64
	for _, id := range c.IDs() {
		if id != old {
			rest = append(rest, id)
		}
	}
	c.Net.Partition(c.IDs(), []uint64{old}, rest)
	stuck := c.Replicas[old].Node.Propose(kv.Command{ClientID: 2, Seq: 1, Op: kv.OpPut, Key: "k", Value: "lost"}.Encode())
	staleRead := c.Replicas[old].Node.ReadBarrier()

	nl := mustLeaderOtherThan(t, c, old)
	write(t, c, nl, kv.Command{ClientID: 3, Seq: 1, Op: kv.OpPut, Key: "k", Value: "v2"})

	for _, f := range []*node.Future{stuck, staleRead} {
		select {
		case <-f.Done():
			if _, err := f.Result(); err == nil {
				t.Fatal("deposed leader reported success")
			}
		default:
			t.Fatal("operation on the deposed leader is still pending after a new leader was elected")
		}
	}
	c.Net.Heal()
	settle(t, c)
	for _, id := range c.IDs() {
		if v, _ := c.Replicas[id].Store.Get("k"); v != "v2" {
			t.Fatalf("node %d: k = %q; the minority write must be discarded", id, v)
		}
	}
	noViolations(t, c)
}

func TestFollowerCatchesUpViaSnapshot(t *testing.T) {
	c := newCluster(t, Options{Seed: 6, SnapshotThreshold: 10})
	l := mustLeader(t, c)
	var lag uint64
	for _, id := range c.IDs() {
		if id != l {
			lag = id
		}
	}
	var others []uint64
	for _, id := range c.IDs() {
		if id != lag {
			others = append(others, id)
		}
	}
	c.Net.Partition(c.IDs(), []uint64{lag}, others)
	for i := 1; i <= 60; i++ {
		write(t, c, l, kv.Command{ClientID: 1, Seq: uint64(i), Op: kv.OpPut, Key: "counter", Value: string(rune('A' + i%26))})
	}
	c.Net.Heal()
	settle(t, c)
	if got := c.Replicas[lag].Node.Status().Stats.SnapshotsInstalled; got == 0 {
		t.Fatal("lagging follower caught up without a snapshot; compaction never kicked in")
	}
	if c.Replicas[l].Node.Status().SnapshotIndex == 0 {
		t.Fatal("leader never compacted its log")
	}
	// Sessions travel inside the snapshot: a retry of the last write is still a duplicate.
	res := write(t, c, l, kv.Command{ClientID: 1, Seq: 60, Op: kv.OpPut, Key: "counter", Value: "x"})
	if !res.Duplicate {
		t.Fatalf("retry after snapshot = %+v", res)
	}
	noViolations(t, c)
}

func TestFollowersRejectClientOperations(t *testing.T) {
	c := newCluster(t, Options{Seed: 7})
	l := mustLeader(t, c)
	c.Run(20)
	for _, id := range c.IDs() {
		if id == l {
			continue
		}
		var nl *node.NotLeaderError
		_, err := c.Replicas[id].Node.Propose(kv.Command{Op: kv.OpPut, Key: "k", Value: "v"}.Encode()).Result()
		if !errors.As(err, &nl) || nl.Leader != l {
			t.Fatalf("follower %d propose err = %v, want NotLeader{%d}", id, err, l)
		}
		_, err = c.Replicas[id].Node.ReadBarrier().Result()
		if !errors.As(err, &nl) {
			t.Fatalf("follower %d read err = %v", id, err)
		}
	}
}

func TestDuplicatedAndReorderedMessagesAreHarmless(t *testing.T) {
	c := newCluster(t, Options{Seed: 8, MinDelay: 1, MaxDelay: 8})
	c.Net.DupRate = 0.3
	l := mustLeader(t, c)
	for i := 1; i <= 30; i++ {
		write(t, c, l, kv.Command{ClientID: 1, Seq: uint64(i), Op: kv.OpPut, Key: "k", Value: string(rune('a' + i%26))})
	}
	settle(t, c)
	if c.Net.Stats.Duplicated == 0 {
		t.Fatal("network never duplicated anything")
	}
	noViolations(t, c)
}

func TestFullClusterRestartKeepsCommittedData(t *testing.T) {
	c := newCluster(t, Options{Seed: 10})
	l := mustLeader(t, c)
	write(t, c, l, kv.Command{ClientID: 1, Seq: 1, Op: kv.OpPut, Key: "k", Value: "persisted"})
	settle(t, c)
	for _, id := range c.IDs() {
		c.Crash(id)
	}
	for _, id := range c.IDs() {
		if err := c.Restart(id); err != nil {
			t.Fatal(err)
		}
	}
	mustLeader(t, c)
	settle(t, c)
	for _, id := range c.IDs() {
		if v, _ := c.Replicas[id].Store.Get("k"); v != "persisted" {
			t.Fatalf("node %d lost data across a full-cluster restart: %q", id, v)
		}
	}
}
