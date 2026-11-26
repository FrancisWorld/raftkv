package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/FrancisWorld/raftkv/internal/kv"
	"github.com/FrancisWorld/raftkv/internal/raft"
)

type nopTransport struct{}

func (nopTransport) Send(raft.Message) {}

// flakyStorage fails Save once armed, modelling a dying disk.
type flakyStorage struct {
	*raft.MemStorage
	failSave bool
}

func (f *flakyStorage) Save(hs *raft.HardState, ents []raft.Entry) error {
	if f.failSave {
		return errors.New("disk on fire")
	}
	return f.MemStorage.Save(hs, ents)
}

func singleNode(t *testing.T, st raft.Storage, threshold uint64) (*Node, *kv.Store) {
	t.Helper()
	store := kv.NewStore()
	n, err := New(Config{
		Raft:    raft.Config{ID: 1, Peers: []uint64{1}, ElectionTick: 5, HeartbeatTick: 1, PreVote: true, CheckQuorum: true, Seed: 1},
		Storage: st, SM: store, Transport: nopTransport{}, SnapshotThreshold: threshold,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50 && n.Status().Role != raft.Leader; i++ {
		n.Tick()
	}
	if n.Status().Role != raft.Leader {
		t.Fatal("single node did not become leader")
	}
	return n, store
}

func put(seq uint64, key, val string) []byte {
	return kv.Command{ClientID: 1, Seq: seq, Op: kv.OpPut, Key: key, Value: val}.Encode()
}

func TestProposeApplyAndReadBarrier(t *testing.T) {
	n, store := singleNode(t, raft.NewMemStorage(), 0)
	raw, err := n.Propose(put(1, "k", "v")).Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res, _ := kv.DecodeResult(raw); !res.Applied {
		t.Fatalf("result = %+v", res)
	}
	if _, err := n.ReadBarrier().Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, _ := store.Get("k"); v != "v" {
		t.Fatalf("value = %q", v)
	}
	if st := n.Status(); st.Stats.Proposals != 1 || st.Stats.AppliedEntries != 1 {
		t.Fatalf("stats = %+v", st.Stats)
	}
}

func TestStorageFailureHaltsNodeAndFailsOperations(t *testing.T) {
	st := &flakyStorage{MemStorage: raft.NewMemStorage()}
	n, _ := singleNode(t, st, 0)
	st.failSave = true
	_, err := n.Propose(put(1, "k", "v")).Wait(context.Background())
	if err == nil || n.Err() == nil {
		t.Fatalf("propose err = %v, node err = %v; a persistence failure must halt the node", err, n.Err())
	}
	if _, err := n.Propose(put(2, "k", "v")).Wait(context.Background()); err == nil {
		t.Fatal("halted node accepted a proposal")
	}
	if _, err := n.ReadBarrier().Wait(context.Background()); err == nil {
		t.Fatal("halted node served a read barrier")
	}
	n.Tick() // must be a no-op, not a panic
}

func TestCloseFailsFurtherWorkAndIsIdempotent(t *testing.T) {
	n, _ := singleNode(t, raft.NewMemStorage(), 0)
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Propose(put(1, "k", "v")).Wait(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v, want ErrStopped", err)
	}
	n.Receive(raft.Message{})
}

func TestCompactionAndRestartRestoresFromSnapshot(t *testing.T) {
	st := raft.NewMemStorage()
	n, _ := singleNode(t, st, 5)
	for i := uint64(1); i <= 12; i++ {
		if _, err := n.Propose(put(i, "k", "v")).Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if n.Status().Stats.SnapshotsTaken == 0 || n.Status().SnapshotIndex == 0 {
		t.Fatalf("no compaction happened: %+v", n.Status())
	}
	snapIdx := n.Status().SnapshotIndex
	_ = n.Close()

	n2, store2 := singleNode(t, st, 5)
	if v, _ := store2.Get("k"); v != "v" {
		t.Fatalf("state not restored: %q", v)
	}
	if n2.Status().SnapshotIndex < snapIdx {
		t.Fatalf("snapshot index went backwards: %d < %d", n2.Status().SnapshotIndex, snapIdx)
	}
	// Sessions came back with the snapshot, so the last command is a duplicate.
	raw, _ := n2.Propose(put(12, "k", "other")).Wait(context.Background())
	if res, _ := kv.DecodeResult(raw); !res.Duplicate {
		t.Fatalf("session lost across restart: %+v", res)
	}
}

func TestNewRejectsBadConfigAndCorruptSnapshot(t *testing.T) {
	if _, err := New(Config{Raft: raft.Config{}, Storage: raft.NewMemStorage(), SM: kv.NewStore(), Transport: nopTransport{}}); err == nil {
		t.Fatal("invalid raft config accepted")
	}
	st := raft.NewMemStorage()
	_ = st.InstallSnapshot(raft.Snapshot{Index: 3, Term: 1, Data: []byte("{not json")})
	_, err := New(Config{
		Raft:    raft.Config{ID: 1, Peers: []uint64{1}, ElectionTick: 5, HeartbeatTick: 1},
		Storage: st, SM: kv.NewStore(), Transport: nopTransport{},
	})
	if err == nil {
		t.Fatal("corrupt snapshot accepted")
	}
}

func TestFutureWaitHonoursContextAndNotLeaderError(t *testing.T) {
	f := newFuture()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := f.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	f.resolve([]byte("a"), nil)
	f.resolve([]byte("b"), nil) // second resolution is ignored
	if v, _ := f.Result(); string(v) != "a" {
		t.Fatalf("future resolved twice: %q", v)
	}
	if (&NotLeaderError{}).Error() == (&NotLeaderError{Leader: 2}).Error() {
		t.Fatal("NotLeaderError messages should differ with and without a known leader")
	}
}

func TestRunTickerStopsOnCancel(t *testing.T) {
	store := kv.NewStore()
	n, err := New(Config{
		Raft:    raft.Config{ID: 1, Peers: []uint64{1}, ElectionTick: 3, HeartbeatTick: 1, Seed: 1},
		Storage: raft.NewMemStorage(), SM: store, Transport: nopTransport{},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { n.RunTicker(ctx, time.Millisecond); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for n.Status().Role != raft.Leader && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if n.Status().Role != raft.Leader {
		t.Fatal("ticker never produced a leader")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunTicker did not stop")
	}
}
