package kv

import "testing"

func apply(t *testing.T, s *Store, c Command) Result {
	t.Helper()
	r, err := DecodeResult(s.Apply(0, c.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestPutGetDelete(t *testing.T) {
	s := NewStore()
	if r := apply(t, s, Command{Op: OpPut, Key: "a", Value: "1"}); !r.Applied || r.PrevFound {
		t.Fatalf("first put = %+v", r)
	}
	if r := apply(t, s, Command{Op: OpPut, Key: "a", Value: "2"}); !r.PrevFound || r.Prev != "1" {
		t.Fatalf("overwrite = %+v", r)
	}
	if v, ok := s.Get("a"); !ok || v != "2" {
		t.Fatalf("get = %q %v", v, ok)
	}
	if r := apply(t, s, Command{Op: OpDelete, Key: "a"}); !r.Applied || r.Prev != "2" {
		t.Fatalf("delete = %+v", r)
	}
	if r := apply(t, s, Command{Op: OpDelete, Key: "a"}); r.Applied {
		t.Fatalf("deleting a missing key reported applied: %+v", r)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("key still present")
	}
}

func TestCAS(t *testing.T) {
	s := NewStore()
	if r := apply(t, s, Command{Op: OpCAS, Key: "k", ExpectAbsent: true, Value: "v1"}); !r.Applied {
		t.Fatalf("create-if-absent failed: %+v", r)
	}
	if r := apply(t, s, Command{Op: OpCAS, Key: "k", ExpectAbsent: true, Value: "x"}); r.Applied {
		t.Fatalf("create-if-absent succeeded on an existing key: %+v", r)
	}
	if r := apply(t, s, Command{Op: OpCAS, Key: "k", Expected: "wrong", Value: "x"}); r.Applied || r.Prev != "v1" {
		t.Fatalf("mismatched cas = %+v", r)
	}
	if r := apply(t, s, Command{Op: OpCAS, Key: "k", Expected: "v1", Value: "v2"}); !r.Applied {
		t.Fatalf("matching cas = %+v", r)
	}
	if v, _ := s.Get("k"); v != "v2" {
		t.Fatalf("value = %q", v)
	}
}

func TestExactlyOnceDedupe(t *testing.T) {
	s := NewStore()
	cmd := Command{ClientID: 7, Seq: 1, Op: OpPut, Key: "k", Value: "first"}
	first := apply(t, s, cmd)
	if first.Duplicate {
		t.Fatal("first execution marked duplicate")
	}
	// Another client overwrites the key between the original and the retry.
	apply(t, s, Command{ClientID: 8, Seq: 1, Op: OpPut, Key: "k", Value: "other"})
	retry := apply(t, s, cmd)
	if !retry.Duplicate || retry.Prev != first.Prev || retry.Applied != first.Applied {
		t.Fatalf("retry = %+v, original = %+v", retry, first)
	}
	if v, _ := s.Get("k"); v != "other" {
		t.Fatalf("retry re-executed the write: value = %q", v)
	}
}

func TestDedupeAppliesToCASToo(t *testing.T) {
	s := NewStore()
	cmd := Command{ClientID: 1, Seq: 1, Op: OpCAS, Key: "n", ExpectAbsent: true, Value: "1"}
	apply(t, s, cmd)
	r := apply(t, s, cmd) // a re-execution would fail the precondition and report Applied=false
	if !r.Applied || !r.Duplicate {
		t.Fatalf("retried CAS = %+v; the original success must be replayed", r)
	}
}

func TestStaleSequenceIsRejectedNotExecuted(t *testing.T) {
	s := NewStore()
	apply(t, s, Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: "a"})
	apply(t, s, Command{ClientID: 1, Seq: 2, Op: OpPut, Key: "k", Value: "b"})
	r := apply(t, s, Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: "a"})
	if r.Error != "stale" {
		t.Fatalf("result = %+v", r)
	}
	if v, _ := s.Get("k"); v != "b" {
		t.Fatalf("stale command mutated state: %q", v)
	}
}

func TestClientIDZeroDisablesDedupe(t *testing.T) {
	s := NewStore()
	c := Command{Op: OpCAS, Key: "k", ExpectAbsent: true, Value: "v"}
	if r := apply(t, s, c); !r.Applied {
		t.Fatal("first cas failed")
	}
	if r := apply(t, s, c); r.Applied || r.Duplicate {
		t.Fatalf("session-less repeat = %+v", r)
	}
}

func TestMalformedCommandIsRejectedDeterministically(t *testing.T) {
	s := NewStore()
	for _, raw := range [][]byte{[]byte("not json"), []byte(`{"op":"nope","key":"k"}`), []byte(`{"op":"put","key":""}`)} {
		r, _ := DecodeResult(s.Apply(0, raw))
		if r.Error != "malformed" {
			t.Fatalf("%q -> %+v", raw, r)
		}
	}
	if s.Len() != 0 {
		t.Fatal("malformed command changed state")
	}
}

func TestSnapshotRestoreKeepsDataAndSessions(t *testing.T) {
	s := NewStore()
	apply(t, s, Command{ClientID: 3, Seq: 1, Op: OpPut, Key: "a", Value: "1"})
	apply(t, s, Command{ClientID: 3, Seq: 2, Op: OpPut, Key: "b", Value: "2"})
	blob, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := NewStore()
	if err := r.Restore(blob); err != nil {
		t.Fatal(err)
	}
	if v, _ := r.Get("b"); v != "2" || r.Len() != 2 {
		t.Fatalf("restored data wrong: len=%d b=%q", r.Len(), v)
	}
	// A retry of the last command after restoring from a snapshot is still deduplicated.
	dup := apply(t, r, Command{ClientID: 3, Seq: 2, Op: OpPut, Key: "b", Value: "2"})
	if !dup.Duplicate {
		t.Fatalf("session table lost in snapshot: %+v", dup)
	}
	if err := r.Restore(nil); err != nil || r.Len() != 0 {
		t.Fatalf("restore(nil): err=%v len=%d", err, r.Len())
	}
	if err := r.Restore([]byte("{")); err == nil {
		t.Fatal("corrupt snapshot accepted")
	}
}

func TestValidate(t *testing.T) {
	bad := []Command{
		{Op: "x", Key: "k"},
		{Op: OpPut},
		{Op: OpPut, Key: string(make([]byte, MaxKeyLen+1))},
		{Op: OpPut, Key: "k", Value: string(make([]byte, MaxValueLen+1))},
		{Op: OpPut, Key: "k", ClientID: 1},
	}
	for i, c := range bad {
		if c.Validate() == nil {
			t.Errorf("case %d should be invalid", i)
		}
	}
	if err := (Command{Op: OpPut, Key: "k", ClientID: 1, Seq: 1}).Validate(); err != nil {
		t.Fatal(err)
	}
}
