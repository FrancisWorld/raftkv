// Package wal is a crash-safe raft.Storage backed by a CRC-protected,
// append-only write-ahead log plus an atomically replaced snapshot file.
//
// Log file layout: an 8-byte magic, then records of
//
//	length  uint32 LE   payload length
//	crc     uint32 LE   CRC-32C over length, type and payload
//	type    uint8
//	payload
//
// A record type is either a hard state (term, vote) or one log entry. Writing an
// entry whose index is already present truncates the log from that index, which
// is how conflicting suffixes are replaced without rewriting the file.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/FrancisWorld/raftkv/internal/raft"
)

const (
	logMagic   = "RKVWAL01"
	headerSize = 9 // length + crc + type
	// maxPayload bounds a single record so a corrupt length field cannot trigger
	// a huge allocation or a misleading "torn tail" verdict.
	maxPayload = 256 << 20

	recHardState byte = 1
	recEntry     byte = 2
)

// ErrCorrupt is returned when the log or snapshot is damaged in a way that is
// not explained by a torn final write.
var ErrCorrupt = errors.New("wal: corrupt data")

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type record struct {
	typ     byte
	payload []byte
}

func appendRecord(dst []byte, typ byte, payload []byte) []byte {
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	hdr[8] = typ
	crc := crc32.Update(0, crcTable, hdr[0:4])
	crc = crc32.Update(crc, crcTable, hdr[8:9])
	crc = crc32.Update(crc, crcTable, payload)
	binary.LittleEndian.PutUint32(hdr[4:8], crc)
	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

func encodeHardState(hs raft.HardState) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[0:8], hs.Term)
	binary.LittleEndian.PutUint64(b[8:16], hs.Vote)
	return b
}

func decodeHardState(b []byte) (raft.HardState, error) {
	if len(b) != 16 {
		return raft.HardState{}, fmt.Errorf("%w: hard state record of %d bytes", ErrCorrupt, len(b))
	}
	return raft.HardState{
		Term: binary.LittleEndian.Uint64(b[0:8]),
		Vote: binary.LittleEndian.Uint64(b[8:16]),
	}, nil
}

func encodeEntry(e raft.Entry) []byte {
	b := make([]byte, 17, 17+len(e.Data))
	binary.LittleEndian.PutUint64(b[0:8], e.Index)
	binary.LittleEndian.PutUint64(b[8:16], e.Term)
	b[16] = byte(e.Kind)
	return append(b, e.Data...)
}

func decodeEntry(b []byte) (raft.Entry, error) {
	if len(b) < 17 {
		return raft.Entry{}, fmt.Errorf("%w: entry record of %d bytes", ErrCorrupt, len(b))
	}
	e := raft.Entry{
		Index: binary.LittleEndian.Uint64(b[0:8]),
		Term:  binary.LittleEndian.Uint64(b[8:16]),
		Kind:  raft.EntryKind(b[16]),
	}
	if e.Kind > raft.EntryNoop || e.Index == 0 {
		return raft.Entry{}, fmt.Errorf("%w: invalid entry header", ErrCorrupt)
	}
	if len(b) > 17 {
		e.Data = append([]byte(nil), b[17:]...)
	}
	return e, nil
}

// errTorn marks a final record that was cut short or garbled by a crash during
// the last write. It is recoverable by truncating the file.
var errTorn = errors.New("wal: torn tail")

// scan parses records from b (which excludes the file magic). It returns the
// records that were fully valid and the number of bytes they occupy. The error
// is nil for a clean end, errTorn if only a damaged final record follows, and
// ErrCorrupt for damage that is followed by more data. It never panics, which
// the fuzz test enforces.
func scan(b []byte) ([]record, int, error) {
	var recs []record
	off := 0
	for off < len(b) {
		rest := b[off:]
		if len(rest) < headerSize {
			return recs, off, errTorn
		}
		n := binary.LittleEndian.Uint32(rest[0:4])
		if n > maxPayload {
			return recs, off, fmt.Errorf("%w: record length %d at offset %d", ErrCorrupt, n, off)
		}
		end := headerSize + int(n)
		if len(rest) < end {
			return recs, off, errTorn
		}
		crc := crc32.Update(0, crcTable, rest[0:4])
		crc = crc32.Update(crc, crcTable, rest[8:9])
		crc = crc32.Update(crc, crcTable, rest[headerSize:end])
		if crc != binary.LittleEndian.Uint32(rest[4:8]) {
			if len(rest) == end {
				return recs, off, errTorn
			}
			return recs, off, fmt.Errorf("%w: checksum mismatch at offset %d", ErrCorrupt, off)
		}
		if t := rest[8]; t != recHardState && t != recEntry {
			return recs, off, fmt.Errorf("%w: unknown record type %d at offset %d", ErrCorrupt, t, off)
		}
		recs = append(recs, record{typ: rest[8], payload: rest[headerSize:end]})
		off += end
	}
	return recs, off, nil
}

// replay folds records into a hard state and a contiguous entry list.
func replay(recs []record) (raft.HardState, []raft.Entry, error) {
	var (
		hs   raft.HardState
		ents []raft.Entry
	)
	for _, r := range recs {
		switch r.typ {
		case recHardState:
			h, err := decodeHardState(r.payload)
			if err != nil {
				return hs, nil, err
			}
			hs = h
		case recEntry:
			e, err := decodeEntry(r.payload)
			if err != nil {
				return hs, nil, err
			}
			if len(ents) > 0 {
				first, next := ents[0].Index, ents[0].Index+uint64(len(ents))
				if e.Index < first || e.Index > next {
					return hs, nil, fmt.Errorf("%w: entry %d does not fit log [%d, %d)", ErrCorrupt, e.Index, first, next)
				}
				ents = ents[:e.Index-first]
			}
			ents = append(ents, e)
		}
	}
	return hs, ents, nil
}

// reconcile aligns the replayed log with the snapshot following Raft's rule: if
// the log holds the snapshot's last entry, keep what follows it; otherwise the
// log is stale or divergent and is discarded whole.
func reconcile(snap raft.Snapshot, ents []raft.Entry) ([]raft.Entry, error) {
	if len(ents) == 0 {
		return nil, nil
	}
	first, last := ents[0].Index, ents[len(ents)-1].Index
	if snap.Index == 0 {
		if first != 1 {
			return nil, fmt.Errorf("%w: log starts at %d with no snapshot", ErrCorrupt, first)
		}
		return ents, nil
	}
	switch {
	case last < snap.Index:
		return nil, nil
	case first > snap.Index+1:
		return nil, fmt.Errorf("%w: log starts at %d but snapshot ends at %d", ErrCorrupt, first, snap.Index)
	case first <= snap.Index:
		at := ents[snap.Index-first]
		if at.Term != snap.Term {
			return nil, nil
		}
		return ents[snap.Index-first+1:], nil
	}
	return ents, nil
}
