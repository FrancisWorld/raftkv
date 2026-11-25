package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/FrancisWorld/raftkv/internal/raft"
)

func entries(from, to, term uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Data: []byte(fmt.Sprintf("payload-%d", i))})
	}
	return out
}

func mustOpen(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return s
}

func mustLoad(t *testing.T, s *Store) raft.State {
	t.Helper()
	st, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestRoundTripAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	if err := s.Save(&raft.HardState{Term: 3, Vote: 2}, entries(1, 5, 3)); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(&raft.HardState{Term: 4, Vote: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	st := mustLoad(t, mustOpen(t, dir))
	if st.HardState != (raft.HardState{Term: 4, Vote: 1}) {
		t.Fatalf("hard state = %+v", st.HardState)
	}
	if !reflect.DeepEqual(st.Entries, entries(1, 5, 3)) {
		t.Fatalf("entries = %+v", st.Entries)
	}
}

func TestConflictingAppendTruncatesSuffix(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	_ = s.Save(nil, entries(1, 10, 1))
	_ = s.Save(nil, entries(6, 7, 2)) // a new leader overwrote 6..10
	_ = s.Close()
	st := mustLoad(t, mustOpen(t, dir))
	want := append(entries(1, 5, 1), entries(6, 7, 2)...)
	if !reflect.DeepEqual(st.Entries, want) {
		t.Fatalf("entries = %+v", st.Entries)
	}
}

// A crash can cut the file anywhere inside the last write. Every possible
// truncation point must reopen cleanly and yield a prefix of what was written.
func TestEveryTornTailRecovers(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	for i := uint64(1); i <= 6; i++ {
		if err := s.Save(&raft.HardState{Term: i}, entries(i, i, i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Close()
	full, err := os.ReadFile(filepath.Join(dir, logName))
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut <= len(full); cut++ {
		d := t.TempDir()
		if err := os.WriteFile(filepath.Join(d, logName), full[:cut], 0o600); err != nil {
			t.Fatal(err)
		}
		st := mustLoad(t, mustOpen(t, d))
		for i, e := range st.Entries {
			want := entries(uint64(i)+1, uint64(i)+1, uint64(i)+1)[0]
			if !reflect.DeepEqual(e, want) {
				t.Fatalf("cut %d: entry %d = %+v, want %+v", cut, i, e, want)
			}
		}
	}
}

func TestTornTailIsTruncatedSoAppendsContinueCleanly(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	_ = s.Save(nil, entries(1, 3, 1))
	_ = s.Close()
	p := filepath.Join(dir, logName)
	b, _ := os.ReadFile(p)
	if err := os.WriteFile(p, b[:len(b)-3], 0o600); err != nil {
		t.Fatal(err)
	}
	s = mustOpen(t, dir)
	if got := len(mustLoad(t, s).Entries); got != 2 {
		t.Fatalf("recovered %d entries, want 2", got)
	}
	_ = s.Save(nil, entries(3, 4, 1))
	_ = s.Close()
	st := mustLoad(t, mustOpen(t, dir))
	if !reflect.DeepEqual(st.Entries, entries(1, 4, 1)) {
		t.Fatalf("entries after re-append = %+v", st.Entries)
	}
}

func TestChecksumMismatchInLastRecordIsTornButMidFileIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	_ = s.Save(nil, entries(1, 4, 1))
	_ = s.Close()
	p := filepath.Join(dir, logName)
	orig, _ := os.ReadFile(p)

	last := bytes.Clone(orig)
	last[len(last)-1] ^= 0xff
	if err := os.WriteFile(p, last, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := len(mustLoad(t, mustOpen(t, dir)).Entries); got != 3 {
		t.Fatalf("flipped final byte: recovered %d entries, want 3", got)
	}

	mid := bytes.Clone(orig)
	mid[len(logMagic)+headerSize+20] ^= 0xff // inside the first entry record
	if err := os.WriteFile(p, mid, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("mid-file corruption: err = %v, want ErrCorrupt", err)
	}
}

func TestBadMagicAndGapsAreCorrupt(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, logName), []byte("NOTAWAL!rest"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("bad magic: err = %v", err)
	}

	dir = t.TempDir()
	s := mustOpen(t, dir)
	_ = s.Save(nil, entries(1, 2, 1))
	_ = s.Save(nil, entries(5, 5, 1)) // index 5 leaves a hole after 2
	_ = s.Close()
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("gap: err = %v", err)
	}
}

func TestCompactKeepsTailAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	_ = s.Save(&raft.HardState{Term: 2, Vote: 1}, entries(1, 10, 2))
	snap := raft.Snapshot{Index: 6, Term: 2, Data: []byte("state@6")}
	if err := s.Compact(snap); err != nil {
		t.Fatal(err)
	}
	_ = s.Save(nil, entries(11, 12, 2)) // appends continue on the rewritten file
	_ = s.Close()

	st := mustLoad(t, mustOpen(t, dir))
	if !reflect.DeepEqual(st.Snapshot, snap) {
		t.Fatalf("snapshot = %+v", st.Snapshot)
	}
	if !reflect.DeepEqual(st.Entries, entries(7, 12, 2)) {
		t.Fatalf("entries = %+v", st.Entries)
	}
	if st.HardState != (raft.HardState{Term: 2, Vote: 1}) {
		t.Fatalf("hard state lost across compaction: %+v", st.HardState)
	}
}

func TestInstallSnapshotDiscardsLog(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	_ = s.Save(&raft.HardState{Term: 2}, entries(1, 5, 1))
	snap := raft.Snapshot{Index: 40, Term: 3, Data: []byte("far ahead")}
	if err := s.InstallSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	_ = s.Save(nil, entries(41, 42, 3))
	_ = s.Close()
	st := mustLoad(t, mustOpen(t, dir))
	if st.Snapshot.Index != 40 || !reflect.DeepEqual(st.Entries, entries(41, 42, 3)) {
		t.Fatalf("state = %+v", st)
	}
}

// Crash after the snapshot file was replaced but before the log was rewritten:
// Open must trim or discard the stale log instead of trusting it.
func TestCrashBetweenSnapshotAndLogRewrite(t *testing.T) {
	cases := []struct {
		name string
		snap raft.Snapshot
		want []raft.Entry
	}{
		{"compact with matching term keeps tail", raft.Snapshot{Index: 4, Term: 1, Data: []byte("s")}, entries(5, 8, 1)},
		{"install with divergent term drops all", raft.Snapshot{Index: 6, Term: 9, Data: []byte("s")}, nil},
		{"install beyond the log drops all", raft.Snapshot{Index: 50, Term: 9, Data: []byte("s")}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			s := mustOpen(t, dir)
			_ = s.Save(&raft.HardState{Term: 1}, entries(1, 8, 1))
			_ = s.Close()
			if err := writeFileAtomic(filepath.Join(dir, snapName), encodeSnapshot(c.snap)); err != nil {
				t.Fatal(err)
			}
			st := mustLoad(t, mustOpen(t, dir))
			if st.Snapshot.Index != c.snap.Index || !reflect.DeepEqual(st.Entries, c.want) {
				t.Fatalf("state = snap %d entries %+v", st.Snapshot.Index, st.Entries)
			}
		})
	}
}

func TestCorruptSnapshotIsRejected(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	_ = s.Save(nil, entries(1, 3, 1))
	_ = s.Close()
	good := encodeSnapshot(raft.Snapshot{Index: 3, Term: 1, Data: []byte("data")})
	bad := bytes.Clone(good)
	bad[len(bad)-6] ^= 1
	if err := os.WriteFile(filepath.Join(dir, snapName), bad, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}
