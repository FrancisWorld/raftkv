package raft

type progressState uint8

const (
	// stateProbe: the follower's position is unknown. One append is in flight at
	// a time until it answers.
	stateProbe progressState = iota
	// stateReplicate: the follower is in sync; appends are pipelined optimistically.
	stateReplicate
	// stateSnapshot: the follower is behind the compacted log and is being sent
	// a snapshot.
	stateSnapshot
)

// progress is the leader's view of one follower.
type progress struct {
	match        uint64
	next         uint64
	state        progressState
	paused       bool // probe: an append is in flight
	snapTicks    int  // ticks since a snapshot was sent
	recentActive bool
}
