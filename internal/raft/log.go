package raft

// raftLog is the in-memory log. Entries up to snapIndex have been compacted
// into a snapshot; entries[i] holds the entry with index snapIndex+1+i.
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry
}

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

// term returns the term of the entry at index i. It succeeds for the snapshot
// boundary and every retained entry.
func (l *raftLog) term(i uint64) (uint64, bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.lastIndex():
		return 0, false
	default:
		return l.entries[i-l.snapIndex-1].Term, true
	}
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, ok := l.term(i)
	return ok && t == term
}

// slice returns a copy of entries [from, to] (inclusive), capped at max entries
// when max > 0. A copy is required because truncation reuses the backing array.
func (l *raftLog) slice(from, to uint64, max int) []Entry {
	if from <= l.snapIndex {
		from = l.snapIndex + 1
	}
	if to > l.lastIndex() {
		to = l.lastIndex()
	}
	if from > to {
		return nil
	}
	n := int(to - from + 1)
	if max > 0 && n > max {
		n = max
	}
	out := make([]Entry, n)
	copy(out, l.entries[from-l.snapIndex-1:])
	return out
}

// truncateAndAppend discards every entry with index >= ents[0].Index and
// appends ents. ents must continue the log contiguously.
func (l *raftLog) truncateAndAppend(ents []Entry) {
	if len(ents) == 0 {
		return
	}
	l.entries = append(l.entries[:ents[0].Index-l.snapIndex-1], ents...)
}

// compactTo drops entries up to and including index, which must be retained.
func (l *raftLog) compactTo(index, term uint64) {
	l.entries = append([]Entry(nil), l.entries[index-l.snapIndex:]...)
	l.snapIndex, l.snapTerm = index, term
}

// reset discards the whole log in favour of a snapshot boundary.
func (l *raftLog) reset(index, term uint64) {
	l.entries = nil
	l.snapIndex, l.snapTerm = index, term
}

// firstIndexOfTerm returns the first retained index whose term equals t,
// scanning backwards from index i (which must carry term t).
func (l *raftLog) firstIndexOfTerm(i, t uint64) uint64 {
	for i > l.snapIndex+1 {
		if pt, _ := l.term(i - 1); pt != t {
			break
		}
		i--
	}
	return i
}
