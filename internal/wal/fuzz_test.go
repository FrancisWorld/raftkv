package wal

import (
	"bytes"
	"testing"

	"github.com/FrancisWorld/raftkv/internal/raft"
)

// FuzzDecode feeds arbitrary bytes to the log decoder. It must never panic, and
// whatever it accepts must re-encode to exactly the bytes it consumed.
func FuzzDecode(f *testing.F) {
	var seed []byte
	seed = appendRecord(seed, recHardState, encodeHardState(raft.HardState{Term: 3, Vote: 1}))
	seed = appendRecord(seed, recEntry, encodeEntry(raft.Entry{Index: 1, Term: 3, Data: []byte("hello")}))
	seed = appendRecord(seed, recEntry, encodeEntry(raft.Entry{Index: 2, Term: 3, Kind: raft.EntryNoop}))
	f.Add(seed)
	f.Add(seed[:len(seed)-2])
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		recs, n, _ := scan(data)
		if n > len(data) {
			t.Fatalf("consumed %d of %d bytes", n, len(data))
		}
		var again []byte
		for _, r := range recs {
			again = appendRecord(again, r.typ, r.payload)
		}
		if !bytes.Equal(again, data[:n]) {
			t.Fatal("accepted records do not re-encode to the consumed prefix")
		}
		_, _, _ = replay(recs) // arbitrary valid-CRC payloads must not panic either
	})
}

// FuzzSnapshotDecode checks the snapshot decoder never panics and round-trips.
func FuzzSnapshotDecode(f *testing.F) {
	f.Add(encodeSnapshot(raft.Snapshot{Index: 9, Term: 2, Data: []byte("x")}))
	f.Add([]byte("RKVSNP01"))
	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := decodeSnapshot(data)
		if err != nil {
			return
		}
		if !bytes.Equal(encodeSnapshot(s), data) {
			t.Fatal("snapshot did not round-trip")
		}
	})
}
