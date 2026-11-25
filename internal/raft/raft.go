package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
)

// Role is a node's current role.
type Role uint8

// Roles.
const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// Config configures a Raft node. Time is measured in ticks.
type Config struct {
	ID    uint64
	Peers []uint64 // every voter, including ID. Membership is static.

	// ElectionTick is the base election timeout. The effective timeout is drawn
	// uniformly from [ElectionTick, 2*ElectionTick) each time it is reset.
	ElectionTick  int
	HeartbeatTick int

	// MaxEntriesPerMsg bounds the entries in one MsgApp. Zero means 64.
	MaxEntriesPerMsg int

	// PreVote makes a would-be candidate poll its peers before bumping its term,
	// so a partitioned node cannot disrupt a healthy cluster on rejoin.
	PreVote bool
	// CheckQuorum makes a leader step down when it has not heard from a quorum
	// within an election timeout, and makes followers ignore vote requests while
	// they hold a leader lease.
	CheckQuorum bool

	// Seed seeds the election timeout randomisation so runs are reproducible.
	Seed uint64
}

func (c *Config) validate() error {
	if c.ID == 0 {
		return errors.New("raft: ID must be non-zero")
	}
	if !slices.Contains(c.Peers, c.ID) {
		return errors.New("raft: Peers must include ID")
	}
	seen := map[uint64]bool{}
	for _, p := range c.Peers {
		if p == 0 || seen[p] {
			return fmt.Errorf("raft: invalid or duplicate peer %d", p)
		}
		seen[p] = true
	}
	if c.HeartbeatTick <= 0 || c.ElectionTick <= c.HeartbeatTick {
		return errors.New("raft: need 0 < HeartbeatTick < ElectionTick")
	}
	if c.MaxEntriesPerMsg == 0 {
		c.MaxEntriesPerMsg = 64
	}
	return nil
}

type readReq struct {
	id    uint64
	index uint64
	acks  map[uint64]bool
}

// Raft is the consensus state machine. It is not safe for concurrent use.
type Raft struct {
	cfg   Config
	id    uint64
	peers []uint64
	rng   *rand.Rand

	term uint64
	vote uint64
	role Role
	lead uint64

	log      raftLog
	snapshot Snapshot // newest local snapshot, kept so it can be sent to laggards
	commit   uint64
	applied  uint64
	stable   uint64 // highest log index handed to the caller for persistence

	prevHS      HardState
	pendingSnap *Snapshot

	progress map[uint64]*progress
	votes    map[uint64]bool

	electionElapsed  int
	heartbeatElapsed int
	randTimeout      int

	msgs       []Message
	reads      []readReq
	readStates []ReadState
	nextReadID uint64
}

// New restores a node from persisted state. It always starts as a follower.
func New(cfg Config, st State) (*Raft, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)
	r := &Raft{
		cfg:   cfg,
		id:    cfg.ID,
		peers: peers,
		rng:   rand.New(rand.NewPCG(cfg.Seed, cfg.ID)),
		term:  st.HardState.Term,
		vote:  st.HardState.Vote,
		log: raftLog{
			snapIndex: st.Snapshot.Index,
			snapTerm:  st.Snapshot.Term,
			entries:   slices.Clone(st.Entries),
		},
		snapshot: st.Snapshot,
		commit:   st.Snapshot.Index,
		applied:  st.Snapshot.Index,
		prevHS:   st.HardState,
	}
	for i, e := range r.log.entries {
		if e.Index != r.log.snapIndex+1+uint64(i) {
			return nil, fmt.Errorf("raft: stored entry %d has index %d, want %d", i, e.Index, r.log.snapIndex+1+uint64(i))
		}
	}
	r.stable = r.log.lastIndex()
	r.becomeFollower(r.term, 0)
	return r, nil
}

// Status is a read-only view of the node.
type Status struct {
	ID            uint64
	Role          Role
	Term          uint64
	Leader        uint64
	Commit        uint64
	Applied       uint64
	LastIndex     uint64
	SnapshotIndex uint64
}

// Status returns the node's current state.
func (r *Raft) Status() Status {
	return Status{
		ID: r.id, Role: r.role, Term: r.term, Leader: r.lead,
		Commit: r.commit, Applied: r.applied, LastIndex: r.log.lastIndex(),
		SnapshotIndex: r.log.snapIndex,
	}
}

func (r *Raft) quorum() int { return len(r.peers)/2 + 1 }

func (r *Raft) resetTimeout() {
	r.electionElapsed = 0
	r.randTimeout = r.cfg.ElectionTick + r.rng.IntN(r.cfg.ElectionTick)
}

func (r *Raft) send(m Message) {
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

// ---- role transitions ----

func (r *Raft) becomeFollower(term, lead uint64) {
	if term != r.term {
		r.term, r.vote = term, 0
	}
	r.role, r.lead = Follower, lead
	r.votes, r.progress, r.reads = nil, nil, nil
	r.heartbeatElapsed = 0
	r.resetTimeout()
}

func (r *Raft) becomePreCandidate() {
	r.role, r.lead = PreCandidate, 0
	r.votes = map[uint64]bool{r.id: true}
	r.resetTimeout()
}

func (r *Raft) becomeCandidate() {
	r.term++
	r.vote = r.id
	r.role, r.lead = Candidate, 0
	r.votes = map[uint64]bool{r.id: true}
	r.resetTimeout()
}

func (r *Raft) becomeLeader() {
	r.role, r.lead = Leader, r.id
	r.votes = nil
	r.electionElapsed, r.heartbeatElapsed = 0, 0
	r.progress = make(map[uint64]*progress, len(r.peers))
	for _, p := range r.peers {
		r.progress[p] = &progress{next: r.log.lastIndex() + 1, recentActive: true}
	}
	// A new leader cannot know which earlier-term entries are committed until it
	// commits something of its own term, so it appends a no-op immediately.
	r.appendEntries(Entry{Kind: EntryNoop})
	r.bcastAppend()
}

// ---- driving the node ----

// Tick advances the logical clock by one tick.
func (r *Raft) Tick() {
	if r.role == Leader {
		r.tickLeader()
		return
	}
	r.electionElapsed++
	if r.electionElapsed >= r.randTimeout {
		r.campaign()
	}
}

func (r *Raft) tickLeader() {
	r.electionElapsed++
	r.heartbeatElapsed++
	for id, pr := range r.progress {
		if id != r.id && pr.state == stateSnapshot {
			pr.snapTicks++
			if pr.snapTicks >= r.cfg.ElectionTick {
				// The snapshot message was probably lost; fall back to probing,
				// which re-sends it.
				pr.state, pr.next, pr.paused = stateProbe, pr.match+1, false
			}
		}
	}
	if r.electionElapsed >= r.cfg.ElectionTick {
		r.electionElapsed = 0
		if r.cfg.CheckQuorum && !r.quorumActive() {
			r.becomeFollower(r.term, 0)
			return
		}
	}
	if r.heartbeatElapsed >= r.cfg.HeartbeatTick {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
	}
}

// quorumActive reports whether a quorum answered since the last check, then
// clears the flags for the next window.
func (r *Raft) quorumActive() bool {
	n := 0
	for id, pr := range r.progress {
		if id == r.id || pr.recentActive {
			n++
		}
		pr.recentActive = false
	}
	return n >= r.quorum()
}

func (r *Raft) campaign() {
	if r.cfg.PreVote {
		r.becomePreCandidate()
		r.pollOthers(MsgPreVote, r.term+1)
		if r.countVotes() >= r.quorum() {
			r.campaignForReal()
		}
		return
	}
	r.campaignForReal()
}

func (r *Raft) campaignForReal() {
	r.becomeCandidate()
	if r.countVotes() >= r.quorum() {
		r.becomeLeader()
		return
	}
	r.pollOthers(MsgVote, r.term)
}

func (r *Raft) pollOthers(t MsgType, term uint64) {
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.send(Message{Type: t, To: p, Term: term, LogIndex: r.log.lastIndex(), LogTerm: r.log.lastTerm()})
	}
}

func (r *Raft) countVotes() int {
	n := 0
	for _, granted := range r.votes {
		if granted {
			n++
		}
	}
	return n
}

// Propose appends a client command to the leader's log and returns where it
// landed. The command is committed only once it appears in CommittedEntries.
func (r *Raft) Propose(data []byte) (index, term uint64, err error) {
	if r.role != Leader {
		return 0, 0, ErrNotLeader
	}
	r.appendEntries(Entry{Kind: EntryNormal, Data: data})
	r.bcastAppend()
	return r.log.lastIndex(), r.term, nil
}

// ReadIndex starts a linearizable read (Raft thesis section 6.4). The returned
// id appears in a later Ready.ReadStates once leadership has been confirmed by a
// heartbeat round; the read may then be served after applying ReadState.Index.
func (r *Raft) ReadIndex() (uint64, error) {
	if r.role != Leader {
		return 0, ErrNotLeader
	}
	if !r.log.matchTerm(r.commit, r.term) {
		return 0, ErrNotReady
	}
	r.nextReadID++
	req := readReq{id: r.nextReadID, index: r.commit, acks: map[uint64]bool{r.id: true}}
	if len(r.peers) == 1 {
		r.readStates = append(r.readStates, ReadState{ID: req.id, Index: req.index})
		return req.id, nil
	}
	r.reads = append(r.reads, req)
	r.bcastHeartbeat()
	return req.id, nil
}

// Compact discards log entries up to index, which must already be applied, and
// remembers data as the snapshot to send to followers that fall behind it.
func (r *Raft) Compact(index uint64, data []byte) error {
	if index > r.applied {
		return fmt.Errorf("raft: compact index %d beyond applied %d", index, r.applied)
	}
	if index <= r.log.snapIndex {
		return fmt.Errorf("raft: compact index %d not beyond snapshot %d", index, r.log.snapIndex)
	}
	term, _ := r.log.term(index)
	r.log.compactTo(index, term)
	r.snapshot = Snapshot{Index: index, Term: term, Data: data}
	return nil
}

// ---- Ready ----

// HasReady reports whether Ready would return any work.
func (r *Raft) HasReady() bool {
	return len(r.msgs) > 0 || len(r.readStates) > 0 || r.pendingSnap != nil ||
		r.hardState() != r.prevHS || r.log.lastIndex() > r.stable || r.commit > r.applied
}

func (r *Raft) hardState() HardState { return HardState{Term: r.term, Vote: r.vote} }

// Ready drains pending work. Calling it marks everything it returns as handed
// off: the caller owns persisting and applying it.
func (r *Raft) Ready() Ready {
	rd := Ready{
		Snapshot:   r.pendingSnap,
		Messages:   r.msgs,
		ReadStates: r.readStates,
	}
	if hs := r.hardState(); hs != r.prevHS {
		rd.HardState = &hs
		r.prevHS = hs
	}
	rd.Entries = r.log.slice(r.stable+1, r.log.lastIndex(), 0)
	rd.CommittedEntries = r.log.slice(r.applied+1, r.commit, 0)
	r.stable = r.log.lastIndex()
	r.applied = r.commit
	r.msgs, r.readStates, r.pendingSnap = nil, nil, nil
	return rd
}

// ---- message handling ----

// Step feeds a message received from another node into the state machine.
func (r *Raft) Step(m Message) {
	if m.To != r.id || m.From == r.id {
		return
	}
	switch {
	case m.Term > r.term:
		leaseHeld := r.cfg.CheckQuorum && r.lead != 0 && r.electionElapsed < r.cfg.ElectionTick
		if (m.Type == MsgVote || m.Type == MsgPreVote) && leaseHeld {
			return // a live leader exists; ignore the disruptive candidate
		}
		switch {
		case m.Type == MsgPreVote, m.Type == MsgPreVoteResp && !m.Reject:
			// Pre-vote traffic carries a term we have not adopted and must not adopt.
		case m.Type == MsgApp, m.Type == MsgHeartbeat, m.Type == MsgSnap:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, 0)
		}
	case m.Term < r.term:
		switch m.Type {
		case MsgApp, MsgHeartbeat, MsgSnap:
			// Tell a stale leader about the newer term so it steps down.
			r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term})
		case MsgPreVote:
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Term: r.term, Reject: true})
		}
		return
	}

	if m.Type == MsgVote || m.Type == MsgPreVote {
		r.handleVoteRequest(m)
		return
	}
	switch r.role {
	case Leader:
		r.stepLeader(m)
	case Candidate, PreCandidate:
		r.stepCandidate(m)
	default:
		r.stepFollower(m)
	}
}

func (r *Raft) handleVoteRequest(m Message) {
	resp := MsgVoteResp
	if m.Type == MsgPreVote {
		resp = MsgPreVoteResp
	}
	canVote := r.vote == m.From || (r.vote == 0 && r.lead == 0) ||
		(m.Type == MsgPreVote && m.Term > r.term)
	upToDate := m.LogTerm > r.log.lastTerm() ||
		(m.LogTerm == r.log.lastTerm() && m.LogIndex >= r.log.lastIndex())
	if canVote && upToDate {
		r.send(Message{Type: resp, To: m.From, Term: m.Term})
		if m.Type == MsgVote {
			r.vote = m.From
			r.electionElapsed = 0
		}
		return
	}
	r.send(Message{Type: resp, To: m.From, Term: r.term, Reject: true})
}

func (r *Raft) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		r.electionElapsed, r.lead = 0, m.From
		r.handleAppend(m)
	case MsgHeartbeat:
		r.electionElapsed, r.lead = 0, m.From
		r.commitTo(m.Commit)
		r.send(Message{Type: MsgHeartbeatResp, To: m.From, Term: r.term, Context: m.Context})
	case MsgSnap:
		r.electionElapsed, r.lead = 0, m.From
		r.handleSnapshot(m)
	}
}

func (r *Raft) stepCandidate(m Message) {
	switch m.Type {
	case MsgApp, MsgHeartbeat, MsgSnap:
		// A leader exists for our term (or the vote request we sent lost).
		r.becomeFollower(m.Term, m.From)
		r.stepFollower(m)
	case MsgPreVoteResp, MsgVoteResp:
		want := MsgVoteResp
		if r.role == PreCandidate {
			want = MsgPreVoteResp
		}
		if m.Type != want {
			return
		}
		if _, seen := r.votes[m.From]; !seen {
			r.votes[m.From] = !m.Reject
		}
		granted, rejected := r.countVotes(), len(r.votes)-r.countVotes()
		switch {
		case granted >= r.quorum() && r.role == PreCandidate:
			r.campaignForReal()
		case granted >= r.quorum():
			r.becomeLeader()
		case rejected >= r.quorum():
			r.becomeFollower(r.term, 0)
		}
	}
}

func (r *Raft) commitTo(index uint64) {
	if index <= r.commit {
		return
	}
	if index > r.log.lastIndex() {
		panic(fmt.Sprintf("raft %d: commit to %d beyond last index %d", r.id, index, r.log.lastIndex()))
	}
	r.commit = index
}

func (r *Raft) handleAppend(m Message) {
	reply := func(resp Message) {
		resp.Type, resp.To, resp.Term = MsgAppResp, m.From, r.term
		r.send(resp)
	}
	if m.LogIndex < r.commit {
		// Everything up to our commit index is already agreed.
		reply(Message{Index: r.commit})
		return
	}
	if !r.log.matchTerm(m.LogIndex, m.LogTerm) {
		rej := Message{Reject: true, Index: m.LogIndex}
		if last := r.log.lastIndex(); m.LogIndex > last {
			rej.ConflictIndex = last + 1
		} else {
			rej.ConflictTerm, _ = r.log.term(m.LogIndex)
			rej.ConflictIndex = r.log.firstIndexOfTerm(m.LogIndex, rej.ConflictTerm)
		}
		reply(rej)
		return
	}
	lastNew := m.LogIndex + uint64(len(m.Entries))
	for i, e := range m.Entries {
		if r.log.matchTerm(e.Index, e.Term) {
			continue
		}
		if e.Index <= r.commit {
			panic(fmt.Sprintf("raft %d: entry %d conflicts with committed index %d", r.id, e.Index, r.commit))
		}
		r.log.truncateAndAppend(m.Entries[i:])
		r.stable = min(r.stable, e.Index-1)
		break
	}
	r.commitTo(min(m.Commit, lastNew))
	reply(Message{Index: lastNew})
}

func (r *Raft) handleSnapshot(m Message) {
	snap := m.Snapshot
	reply := func(idx uint64) {
		r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: idx})
	}
	if snap == nil || snap.Index <= r.commit {
		reply(r.commit)
		return
	}
	if r.log.matchTerm(snap.Index, snap.Term) {
		// We already hold this prefix; just fast-forward the commit index.
		r.commitTo(snap.Index)
		reply(r.commit)
		return
	}
	r.log.reset(snap.Index, snap.Term)
	r.snapshot = *snap
	r.pendingSnap = snap
	r.commit, r.applied, r.stable = snap.Index, snap.Index, snap.Index
	reply(snap.Index)
}

// ---- leader ----

func (r *Raft) appendEntries(ents ...Entry) {
	last := r.log.lastIndex()
	for i := range ents {
		ents[i].Index = last + 1 + uint64(i)
		ents[i].Term = r.term
	}
	r.log.truncateAndAppend(ents)
	r.progress[r.id].match = r.log.lastIndex()
	r.progress[r.id].next = r.log.lastIndex() + 1
	r.maybeCommit()
}

// maybeCommit advances the commit index to the highest index replicated on a
// quorum, provided it belongs to the current term (Raft section 5.4.2).
func (r *Raft) maybeCommit() bool {
	matches := make([]uint64, 0, len(r.peers))
	for _, p := range r.peers {
		matches = append(matches, r.progress[p].match)
	}
	slices.Sort(matches)
	n := matches[len(matches)-r.quorum()]
	if n > r.commit && r.log.matchTerm(n, r.term) {
		r.commit = n
		return true
	}
	return false
}

func (r *Raft) bcastAppend() {
	for _, p := range r.peers {
		if p != r.id {
			r.sendAppend(p)
		}
	}
}

func (r *Raft) sendAppend(to uint64) {
	pr := r.progress[to]
	if pr.state == stateSnapshot || (pr.state == stateProbe && pr.paused) {
		return
	}
	prev := pr.next - 1
	prevTerm, ok := r.log.term(prev)
	if !ok {
		r.sendSnapshot(to, pr)
		return
	}
	ents := r.log.slice(pr.next, r.log.lastIndex(), r.cfg.MaxEntriesPerMsg)
	r.send(Message{
		Type: MsgApp, To: to, Term: r.term,
		LogIndex: prev, LogTerm: prevTerm, Entries: ents, Commit: r.commit,
	})
	switch pr.state {
	case stateReplicate:
		pr.next += uint64(len(ents))
	case stateProbe:
		pr.paused = true
	}
}

func (r *Raft) sendSnapshot(to uint64, pr *progress) {
	snap := r.snapshot
	pr.state, pr.snapTicks = stateSnapshot, 0
	r.send(Message{Type: MsgSnap, To: to, Term: r.term, Snapshot: &snap})
}

func (r *Raft) bcastHeartbeat() {
	var ctx uint64
	if n := len(r.reads); n > 0 {
		ctx = r.reads[n-1].id
	}
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		// Never claim a commit index beyond what the follower is known to hold.
		r.send(Message{Type: MsgHeartbeat, To: p, Term: r.term, Commit: min(r.progress[p].match, r.commit), Context: ctx})
	}
}

func (r *Raft) stepLeader(m Message) {
	pr, ok := r.progress[m.From]
	if !ok {
		return
	}
	pr.recentActive = true
	switch m.Type {
	case MsgAppResp:
		if m.Reject {
			r.handleAppendReject(m, pr)
			return
		}
		updated := m.Index > pr.match
		if updated {
			pr.match = m.Index
			pr.next = max(pr.next, pr.match+1)
		}
		if pr.state != stateReplicate && updated {
			pr.state, pr.next = stateReplicate, pr.match+1
		}
		pr.paused = false
		switch {
		case updated && r.maybeCommit():
			r.bcastAppend()
		case pr.next <= r.log.lastIndex():
			r.sendAppend(m.From)
		}
	case MsgHeartbeatResp:
		pr.paused = false
		if m.Context != 0 {
			r.ackReads(m.From, m.Context)
		}
		if pr.match < r.log.lastIndex() {
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) handleAppendReject(m Message, pr *progress) {
	switch pr.state {
	case stateSnapshot:
		return
	case stateReplicate:
		if m.Index <= pr.match {
			return // stale reject for an entry the follower already acknowledged
		}
		pr.state, pr.next = stateProbe, pr.match+1
	case stateProbe:
		if m.Index != pr.next-1 {
			return // reject for an older probe
		}
	}
	next := m.ConflictIndex
	if m.ConflictTerm != 0 {
		// If we hold entries of the follower's conflicting term, resume right
		// after our last one; otherwise skip the whole term.
		for i := min(m.Index, r.log.lastIndex()); i > r.log.snapIndex; i-- {
			t, _ := r.log.term(i)
			if t == m.ConflictTerm {
				next = i + 1
				break
			}
			if t < m.ConflictTerm {
				break
			}
		}
	}
	pr.next = max(min(next, m.Index), pr.match+1, 1)
	pr.paused = false
	r.sendAppend(m.From)
}

// ackReads records a heartbeat acknowledgement from `from` for every pending
// read up to ctx, then releases the leading reads that reached a quorum.
func (r *Raft) ackReads(from, ctx uint64) {
	for i := range r.reads {
		if r.reads[i].id <= ctx {
			r.reads[i].acks[from] = true
		}
	}
	n := 0
	for n < len(r.reads) && len(r.reads[n].acks) >= r.quorum() {
		r.readStates = append(r.readStates, ReadState{ID: r.reads[n].id, Index: r.reads[n].index})
		n++
	}
	r.reads = r.reads[n:]
}
