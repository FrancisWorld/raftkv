package raft

import (
	"errors"
	"fmt"
	"testing"
)

// testNet is a minimal lock-step harness: it drains every node's Ready, applies
// it to a MemStorage and delivers messages immediately unless a link is cut.
type testNet struct {
	t       *testing.T
	nodes   map[uint64]*Raft
	stores  map[uint64]*MemStorage
	applied map[uint64][]Entry
	reads   map[uint64][]ReadState
	cut     map[[2]uint64]bool
	queue   []Message
	sent    map[MsgType]int
}

func newTestNet(t *testing.T, n int, mutate func(*Config)) *testNet {
	t.Helper()
	var peers []uint64
	for i := 1; i <= n; i++ {
		peers = append(peers, uint64(i))
	}
	tn := &testNet{
		t: t, nodes: map[uint64]*Raft{}, stores: map[uint64]*MemStorage{},
		applied: map[uint64][]Entry{}, reads: map[uint64][]ReadState{},
		cut: map[[2]uint64]bool{}, sent: map[MsgType]int{},
	}
	for _, id := range peers {
		cfg := Config{ID: id, Peers: peers, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true, Seed: 7}
		if mutate != nil {
			mutate(&cfg)
		}
		tn.stores[id] = NewMemStorage()
		r, err := New(cfg, State{})
		if err != nil {
			t.Fatal(err)
		}
		tn.nodes[id] = r
	}
	return tn
}

func (tn *testNet) isolate(id uint64) {
	for other := range tn.nodes {
		if other != id {
			tn.cut[[2]uint64{id, other}] = true
			tn.cut[[2]uint64{other, id}] = true
		}
	}
}

func (tn *testNet) heal() { tn.cut = map[[2]uint64]bool{} }

// pump runs until no node has work and no message is queued.
func (tn *testNet) pump() {
	tn.t.Helper()
	for iter := 0; iter < 100000; iter++ {
		progressed := false
		for id, r := range tn.nodes {
			if !r.HasReady() {
				continue
			}
			progressed = true
			rd := r.Ready()
			if rd.Snapshot != nil {
				if err := tn.stores[id].InstallSnapshot(*rd.Snapshot); err != nil {
					tn.t.Fatal(err)
				}
				tn.applied[id] = nil
			}
			if err := tn.stores[id].Save(rd.HardState, rd.Entries); err != nil {
				tn.t.Fatal(err)
			}
			tn.applied[id] = append(tn.applied[id], rd.CommittedEntries...)
			tn.reads[id] = append(tn.reads[id], rd.ReadStates...)
			for _, m := range rd.Messages {
				tn.sent[m.Type]++
				tn.queue = append(tn.queue, m)
			}
		}
		if len(tn.queue) > 0 {
			progressed = true
			m := tn.queue[0]
			tn.queue = tn.queue[1:]
			if !tn.cut[[2]uint64{m.From, m.To}] {
				tn.nodes[m.To].Step(m.Clone())
			}
		}
		if !progressed {
			return
		}
	}
	tn.t.Fatal("pump did not quiesce")
}

func (tn *testNet) tick(n int) {
	for i := 0; i < n; i++ {
		for _, r := range tn.nodes {
			r.Tick()
		}
		tn.pump()
	}
}

func (tn *testNet) leader() *Raft {
	var l *Raft
	for _, r := range tn.nodes {
		if r.role == Leader {
			if l != nil && l.term == r.term {
				tn.t.Fatalf("two leaders in term %d", r.term)
			}
			if l == nil || r.term > l.term {
				l = r
			}
		}
	}
	return l
}

func (tn *testNet) electLeader() *Raft {
	tn.t.Helper()
	for i := 0; i < 200; i++ {
		tn.tick(1)
		if l := tn.leader(); l != nil {
			tn.pump()
			return l
		}
	}
	tn.t.Fatal("no leader elected")
	return nil
}

func TestConfigValidation(t *testing.T) {
	bad := []Config{
		{ID: 0, Peers: []uint64{1}, ElectionTick: 10, HeartbeatTick: 1},
		{ID: 1, Peers: []uint64{2, 3}, ElectionTick: 10, HeartbeatTick: 1},
		{ID: 1, Peers: []uint64{1, 1}, ElectionTick: 10, HeartbeatTick: 1},
		{ID: 1, Peers: []uint64{1}, ElectionTick: 1, HeartbeatTick: 1},
	}
	for i, c := range bad {
		if _, err := New(c, State{}); err == nil {
			t.Errorf("config %d: expected validation error", i)
		}
	}
}

func TestSingleNodeElectsItselfAndCommits(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	l := tn.electLeader()
	if _, _, err := l.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	tn.pump()
	got := tn.applied[1]
	if len(got) != 2 || got[0].Kind != EntryNoop || string(got[1].Data) != "x" {
		t.Fatalf("applied = %+v", got)
	}
}

func TestThreeNodeElectionAndReplication(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	l := tn.electLeader()
	for i := 0; i < 5; i++ {
		if _, _, err := l.Propose([]byte(fmt.Sprintf("cmd-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	tn.pump()
	tn.tick(3) // heartbeats carry the final commit index to followers
	for id := range tn.nodes {
		if n := len(tn.applied[id]); n != 6 {
			t.Fatalf("node %d applied %d entries, want 6", id, n)
		}
		if string(tn.applied[id][5].Data) != "cmd-4" {
			t.Fatalf("node %d applied wrong tail: %+v", id, tn.applied[id])
		}
	}
	for _, id := range []uint64{1, 2, 3} {
		if id != l.id {
			if _, _, err := tn.nodes[id].Propose(nil); !errors.Is(err, ErrNotLeader) {
				t.Fatalf("follower propose err = %v", err)
			}
		}
	}
}

func TestPreVoteStopsPartitionedNodeFromInflatingTerm(t *testing.T) {
	for _, preVote := range []bool{true, false} {
		t.Run(fmt.Sprintf("prevote=%v", preVote), func(t *testing.T) {
			tn := newTestNet(t, 3, func(c *Config) { c.PreVote = preVote; c.CheckQuorum = preVote })
			l := tn.electLeader()
			termBefore := l.term
			var victim uint64
			for id := range tn.nodes {
				if id != l.id {
					victim = id
				}
			}
			tn.isolate(victim)
			tn.tick(100)
			tn.heal()
			tn.tick(30)
			if preVote {
				if got := tn.nodes[victim].term; got != termBefore {
					t.Fatalf("isolated node term = %d, want unchanged %d", got, termBefore)
				}
				if cur := tn.leader(); cur == nil || cur.id != l.id || cur.term != termBefore {
					t.Fatalf("leadership was disturbed: %+v", cur)
				}
			} else if tn.nodes[victim].term <= termBefore {
				t.Fatalf("without pre-vote the isolated node should have inflated its term")
			}
		})
	}
}

func TestLeaderStepsDownWithoutQuorum(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	l := tn.electLeader()
	tn.isolate(l.id)
	tn.tick(30)
	if l.role == Leader {
		t.Fatal("isolated leader did not step down")
	}
}

func TestFastConflictBacktracking(t *testing.T) {
	mk := func(from, to, term uint64) []Entry {
		var es []Entry
		for i := from; i <= to; i++ {
			es = append(es, Entry{Index: i, Term: term})
		}
		return es
	}
	stale := append(mk(1, 50, 1), mk(51, 250, 2)...)
	fresh := append(mk(1, 50, 1), mk(51, 60, 3)...)
	peers := []uint64{1, 2, 3}
	cfg := func(id uint64) Config {
		return Config{ID: id, Peers: peers, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true, Seed: 1}
	}
	leader, err := New(cfg(1), State{HardState: HardState{Term: 3}, Entries: fresh})
	if err != nil {
		t.Fatal(err)
	}
	follower, err := New(cfg(2), State{HardState: HardState{Term: 2}, Entries: stale})
	if err != nil {
		t.Fatal(err)
	}
	leader.becomeCandidate()
	leader.becomeLeader() // term 4; the third node stays silent
	rejects := 0
	for i := 0; i < 50 && follower.log.lastIndex() != leader.log.lastIndex(); i++ {
		for _, m := range leader.Ready().Messages {
			if m.To == 2 {
				follower.Step(m)
			}
		}
		for _, m := range follower.Ready().Messages {
			if m.Type == MsgAppResp && m.Reject {
				rejects++
			}
			leader.Step(m)
		}
	}
	if follower.log.lastIndex() != leader.log.lastIndex() {
		t.Fatalf("follower did not converge: %d vs %d", follower.log.lastIndex(), leader.log.lastIndex())
	}
	if rejects != 1 {
		t.Fatalf("converged after %d rejects; term-level backtracking should need exactly 1", rejects)
	}
	for i := uint64(1); i <= leader.log.lastIndex(); i++ {
		a, _ := leader.log.term(i)
		b, _ := follower.log.term(i)
		if a != b {
			t.Fatalf("logs differ at %d: %d vs %d", i, a, b)
		}
	}
}

func TestReadIndexRequiresQuorumConfirmation(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	l := tn.electLeader()
	if _, _, err := l.Propose([]byte("a")); err != nil {
		t.Fatal(err)
	}
	tn.pump()

	// Healthy: the read is confirmed immediately by the heartbeat round.
	id, err := l.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	tn.pump()
	if rs := tn.reads[l.id]; len(rs) != 1 || rs[0].ID != id || rs[0].Index < 2 {
		t.Fatalf("read states = %+v", rs)
	}

	// Partitioned: the old leader must not release the read.
	tn.reads[l.id] = nil
	tn.isolate(l.id)
	if _, err := l.ReadIndex(); err != nil {
		t.Fatal(err)
	}
	tn.pump()
	if len(tn.reads[l.id]) != 0 {
		t.Fatal("isolated leader released a read without a quorum")
	}
	tn.tick(30) // it steps down and drops the pending read
	if len(tn.reads[l.id]) != 0 || l.role == Leader {
		t.Fatalf("stale leader served a read (role=%v)", l.role)
	}
	if _, err := l.ReadIndex(); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ReadIndex on follower err = %v", err)
	}
}

func TestReadIndexNotReadyBeforeFirstCommit(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	var l *Raft
	for l == nil {
		for _, r := range tn.nodes {
			r.Tick()
		}
		// Deliver only vote traffic so the no-op cannot be acknowledged yet.
		for iter := 0; iter < 20 && l == nil; iter++ {
			for id, r := range tn.nodes {
				if !r.HasReady() {
					continue
				}
				rd := r.Ready()
				_ = tn.stores[id].Save(rd.HardState, rd.Entries)
				for _, m := range rd.Messages {
					if m.Type == MsgApp || m.Type == MsgHeartbeat {
						continue
					}
					tn.nodes[m.To].Step(m.Clone())
				}
			}
			for _, r := range tn.nodes {
				if r.role == Leader {
					l = r
				}
			}
		}
	}
	if _, err := l.ReadIndex(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("ReadIndex before first commit err = %v, want ErrNotReady", err)
	}
}

func TestSnapshotInstallForLaggingFollower(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	l := tn.electLeader()
	var lag uint64
	for id := range tn.nodes {
		if id != l.id {
			lag = id
		}
	}
	tn.isolate(lag)
	for i := 0; i < 20; i++ {
		if _, _, err := l.Propose([]byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
		tn.pump()
	}
	tn.tick(2)
	if err := l.Compact(l.applied, []byte("state-at-applied")); err != nil {
		t.Fatal(err)
	}
	if err := tn.stores[l.id].Compact(l.snapshot); err != nil {
		t.Fatal(err)
	}
	snapAt := l.snapshot.Index
	tn.heal()
	tn.tick(25)

	f := tn.nodes[lag]
	if f.log.snapIndex != snapAt || f.applied < snapAt {
		t.Fatalf("lagging follower did not install snapshot: snap=%d applied=%d want %d", f.log.snapIndex, f.applied, snapAt)
	}
	if tn.sent[MsgSnap] == 0 {
		t.Fatal("leader never sent a snapshot")
	}
	if _, _, err := l.Propose([]byte("after")); err != nil {
		t.Fatal(err)
	}
	tn.tick(5)
	last := tn.applied[lag][len(tn.applied[lag])-1]
	if string(last.Data) != "after" {
		t.Fatalf("follower missed post-snapshot entry: %+v", last)
	}
	st, _ := tn.stores[lag].Load()
	if st.Snapshot.Index != snapAt || string(st.Snapshot.Data) != "state-at-applied" {
		t.Fatalf("snapshot not persisted: %+v", st.Snapshot)
	}
}

func TestCompactValidation(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	l := tn.electLeader()
	if err := l.Compact(l.applied+5, nil); err == nil {
		t.Fatal("compact beyond applied should fail")
	}
	if err := l.Compact(0, nil); err == nil {
		t.Fatal("compact at snapshot boundary should fail")
	}
}

func TestRestartRecoversTermVoteAndLog(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	l := tn.electLeader()
	if _, _, err := l.Propose([]byte("durable")); err != nil {
		t.Fatal(err)
	}
	tn.pump()
	st, err := tn.stores[l.id].Load()
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(l.cfg, st)
	if err != nil {
		t.Fatal(err)
	}
	if r.term != l.term || r.log.lastIndex() != l.log.lastIndex() || r.role != Follower {
		t.Fatalf("restored %+v want term %d last %d", r.Status(), l.term, l.log.lastIndex())
	}
}
