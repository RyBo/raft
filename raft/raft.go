package raft

import (
	"errors"
	"fmt"
	"math/rand"
)

// ErrProposalDropped is returned when a proposal cannot be processed (e.g. no
// known leader to forward to).
var ErrProposalDropped = errors.New("raft: proposal dropped")

const (
	defaultMaxSizePerMsg = 1 << 20 // 1 MiB worth of entries per MsgApp
)

// Config configures a raft node.
type Config struct {
	ID            uint64
	ElectionTick  int // election timeout in ticks (randomized to [et, 2et))
	HeartbeatTick int // heartbeat interval in ticks
	Storage       Storage
	Applied       uint64 // last applied index (for restart); 0 to use storage

	MaxSizePerMsg            uint64 // cap on entries per MsgApp
	MaxCommittedSizePerReady uint64 // cap on CommittedEntries per Ready

	PreVote     bool // enable the PreVote phase (avoids disruptive re-elections)
	CheckQuorum bool // leader steps down if it loses contact with a quorum

	// Rand is the (seeded) randomness source for election-timeout jitter.
	// Providing one makes runs reproducible; if nil, one seeded by ID is used.
	Rand *rand.Rand
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("raft: ID must not be None")
	}
	if c.HeartbeatTick <= 0 {
		return errors.New("raft: HeartbeatTick must be positive")
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("raft: ElectionTick must be greater than HeartbeatTick")
	}
	if c.Storage == nil {
		return errors.New("raft: Storage must not be nil")
	}
	if c.MaxSizePerMsg == 0 {
		c.MaxSizePerMsg = defaultMaxSizePerMsg
	}
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = noLimit
	}
	if c.Rand == nil {
		c.Rand = rand.New(rand.NewSource(int64(c.ID)))
	}
	return nil
}

// raft is the synchronous core state machine. It produces messages into r.msgs;
// the surrounding RawNode/driver delivers them.
type raft struct {
	id uint64

	Term uint64
	Vote uint64

	// msgs holds messages to be delivered by the driver after the next Ready.
	msgs       []Message
	readStates []ReadState

	raftLog *raftLog

	maxMsgSize uint64

	trk ProgressTracker

	state StateType

	lead uint64

	// number of ticks since reaching the last electionTimeout when in leader or
	// candidate state, or since the last electionTimeout / heartbeat received.
	electionElapsed  int
	heartbeatElapsed int

	checkQuorum bool
	preVote     bool

	heartbeatTimeout          int
	electionTimeout           int
	randomizedElectionTimeout int

	rand *rand.Rand

	// pendingConfIndex is the index of the latest pending configuration change;
	// no new conf change may be proposed until it is applied (one at a time).
	pendingConfIndex uint64

	readOnly *readOnly

	// removed is set when this node has been removed from the configuration.
	removed bool
}

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLog(c.Storage, c.MaxCommittedSizePerReady)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r := &raft{
		id:               c.ID,
		raftLog:          raftlog,
		maxMsgSize:       c.MaxSizePerMsg,
		trk:              makeProgressTracker(),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		checkQuorum:      c.CheckQuorum,
		preVote:          c.PreVote,
		rand:             c.Rand,
		readOnly:         newReadOnly(),
	}

	// Initialize membership from the stored ConfState.
	for _, id := range cs.Voters {
		r.trk.Progress[id] = &Progress{Next: 1}
	}
	for _, id := range cs.Learners {
		r.trk.Progress[id] = &Progress{Next: 1, IsLearner: true}
	}

	if !hs.isEmpty() {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)
	return r
}

func (r *raft) loadState(state HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		panic(fmt.Sprintf("raft: state.commit %d is out of range [%d, %d]",
			state.Commit, r.raftLog.committed, r.raftLog.lastIndex()))
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

func (r *raft) hardState() HardState {
	return HardState{Term: r.Term, Vote: r.Vote, Commit: r.raftLog.committed}
}

func (r *raft) softState() SoftState {
	return SoftState{Lead: r.lead, RaftState: r.state}
}

func (r *raft) confState() ConfState {
	var cs ConfState
	for _, id := range r.trk.allIDs() {
		if r.trk.Progress[id].IsLearner {
			cs.Learners = append(cs.Learners, id)
		} else {
			cs.Voters = append(cs.Voters, id)
		}
	}
	return cs
}

// send buffers a message for delivery. The caller sets m.Term only for the
// vote-family messages; for everything else send stamps the current term.
func (r *raft) send(m Message) {
	m.From = r.id
	switch m.Type {
	case MsgVote, MsgVoteResp, MsgPreVote, MsgPreVoteResp:
		if m.Term == 0 {
			panic(fmt.Sprintf("raft: term should be set for %s", m.Type))
		}
	default:
		if m.Term != 0 {
			panic(fmt.Sprintf("raft: term should not be set for %s (was %d)", m.Type, m.Term))
		}
		m.Term = r.Term
	}
	r.msgs = append(r.msgs, m)
}

func (r *raft) sendAppend(to uint64) {
	pr := r.trk.Progress[to]
	if pr == nil || pr.State == ProgressStateSnapshot && pr.ProbeSent {
		return
	}
	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)

	if errt != nil || erre != nil {
		// The follower needs entries we have already compacted; send a snapshot.
		r.sendSnapshot(to)
		return
	}

	m := Message{To: to, Type: MsgApp, Index: pr.Next - 1, LogTerm: term, Entries: ents, Commit: r.raftLog.committed}
	if n := len(ents); n != 0 {
		switch pr.State {
		case ProgressStateReplicate:
			last := ents[n-1].Index
			pr.Next = last + 1 // optimistic
		case ProgressStateProbe:
			pr.ProbeSent = true
		}
	}
	r.send(m)
}

func (r *raft) sendSnapshot(to uint64) {
	pr := r.trk.Progress[to]
	snap, err := r.raftLog.snapshot()
	if err != nil {
		// Snapshot temporarily unavailable; try again later.
		return
	}
	if snap.isEmpty() {
		panic("raft: need non-empty snapshot")
	}
	pr.becomeSnapshot(snap.Metadata.Index)
	s := snap
	r.send(Message{To: to, Type: MsgSnap, Snapshot: &s})
}

func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Commit cannot exceed the follower's matched index.
	commit := min(r.trk.Progress[to].Match, r.raftLog.committed)
	r.send(Message{To: to, Type: MsgHeartbeat, Commit: commit, Context: ctx})
}

func (r *raft) bcastAppend() {
	for _, id := range r.trk.allIDs() {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *raft) bcastHeartbeat() {
	r.bcastHeartbeatWithCtx(nil)
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	for _, id := range r.trk.allIDs() {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id, ctx)
	}
}

// reset prepares the node for a new term.
func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.trk.resetVotes()
	r.readOnly = newReadOnly()

	lastIndex := r.raftLog.lastIndex()
	for id, pr := range r.trk.Progress {
		*pr = Progress{
			Next:      lastIndex + 1,
			IsLearner: pr.IsLearner,
		}
		if id == r.id {
			pr.Match = lastIndex
		}
	}
	r.pendingConfIndex = 0
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + r.rand.Intn(r.electionTimeout)
}

// appendEntry appends entries to the leader's log and updates its own progress.
func (r *raft) appendEntry(es ...Entry) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	li = r.raftLog.append(es...)
	r.trk.Progress[r.id].maybeUpdate(li)
	r.maybeCommit()
}

// maybeCommit advances the leader's commit index based on follower progress.
func (r *raft) maybeCommit() bool {
	mci := r.trk.committed()
	return r.raftLog.maybeCommit(mci, r.Term)
}

// --- state transitions ---

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.state = StateFollower
	r.lead = lead
}

func (r *raft) becomePreCandidate() {
	if r.state == StateLeader {
		panic("raft: invalid transition leader -> pre-candidate")
	}
	// PreCandidate does NOT increment Term or change Vote.
	r.state = StatePreCandidate
	r.trk.resetVotes()
	r.lead = None
}

func (r *raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("raft: invalid transition leader -> candidate")
	}
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.state = StateCandidate
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		panic("raft: invalid transition follower -> leader")
	}
	r.reset(r.Term)
	r.lead = r.id
	r.state = StateLeader

	r.trk.Progress[r.id].becomeReplicate()

	// Append an empty entry for this term. Committing it lets the leader learn
	// the true commit index (and makes linearizable reads safe).
	r.pendingConfIndex = r.raftLog.lastIndex()
	r.appendEntry(Entry{Type: EntryNormal, Data: nil})
}

// --- ticking ---

func (r *raft) tick() {
	switch r.state {
	case StateLeader:
		r.tickHeartbeat()
	default:
		r.tickElection()
	}
}

func (r *raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.hup()
	}
}

func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			if !r.hasQuorumActive() {
				r.becomeFollower(r.Term, None)
			}
			// Mark everyone inactive until the next round of responses.
			for id, pr := range r.trk.Progress {
				if id != r.id {
					pr.RecentActive = false
				}
			}
		}
	}
	if r.state != StateLeader {
		return
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
	}
}

func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

// promotable reports whether this node is a voter eligible to become leader.
func (r *raft) promotable() bool {
	pr := r.trk.Progress[r.id]
	return pr != nil && !pr.IsLearner && !r.raftLog.hasPendingSnapshot()
}

func (r *raft) hasQuorumActive() bool {
	active := 0
	for _, id := range r.trk.voterIDs() {
		pr := r.trk.Progress[id]
		if id == r.id || pr.RecentActive {
			active++
		}
	}
	return active >= r.trk.quorum()
}

// hup starts a (pre-)campaign.
func (r *raft) hup() {
	if r.state == StateLeader {
		return
	}
	if !r.promotable() {
		return
	}
	if r.preVote {
		r.campaign(campaignPreElection)
	} else {
		r.campaign(campaignElection)
	}
}

type campaignType uint8

const (
	campaignPreElection campaignType = iota
	campaignElection
)

func (r *raft) campaign(t campaignType) {
	var voteMsg MessageType
	var term uint64
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = MsgPreVote
		// PreVote is cast for the *next* term without incrementing our own.
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = MsgVote
		term = r.Term
	}

	// Vote for ourselves.
	if _, _, res := r.poll(r.id, voteRespMsgType(voteMsg), true); res == voteWon {
		// Won outright (single-voter cluster).
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}

	lastIndex := r.raftLog.lastIndex()
	lastTerm := r.raftLog.lastTerm()
	for _, id := range r.trk.voterIDs() {
		if id == r.id {
			continue
		}
		r.send(Message{To: id, Term: term, Type: voteMsg, Index: lastIndex, LogTerm: lastTerm})
	}
}

func (r *raft) poll(id uint64, t MessageType, v bool) (granted, rejected int, result voteResult) {
	r.trk.recordVote(id, v)
	return r.trk.tallyVotes()
}

func voteRespMsgType(t MessageType) MessageType {
	if t == MsgPreVote {
		return MsgPreVoteResp
	}
	return MsgVoteResp
}

// --- Step: the single inbound-message entry point ---

func (r *raft) Step(m Message) error {
	// 1. Term handling.
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		if m.Type == MsgVote || m.Type == MsgPreVote {
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if inLease {
				// We have a healthy leader; ignore the disruptive request.
				return nil
			}
		}
		switch {
		case m.Type == MsgPreVote:
			// Never change our term in response to a PreVote request.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// We won a pre-vote; we'll bump our term when becoming candidate.
		default:
			if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}
	case m.Term < r.Term:
		if (r.checkQuorum || r.preVote) && (m.Type == MsgHeartbeat || m.Type == MsgApp) {
			// An old leader is talking to us; reply with our higher term so it
			// steps down.
			r.send(Message{To: m.From, Type: MsgAppResp})
		} else if m.Type == MsgPreVote {
			// Tell the pre-candidate the real term so it gives up.
			r.send(Message{To: m.From, Term: r.Term, Type: MsgPreVoteResp, Reject: true})
		}
		// Otherwise ignore stale messages.
		return nil
	}

	// 2. Message handling.
	switch m.Type {
	case MsgHup:
		r.hup()

	case MsgVote, MsgPreVote:
		canVote := r.Vote == m.From ||
			(r.Vote == None && r.lead == None) ||
			(m.Type == MsgPreVote && m.Term > r.Term)
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			r.send(Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == MsgVote {
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			r.send(Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		switch r.state {
		case StateLeader:
			r.stepLeader(m)
		case StateCandidate, StatePreCandidate:
			r.stepCandidate(m)
		case StateFollower:
			r.stepFollower(m)
		}
	}
	return nil
}

func (r *raft) stepLeader(m Message) {
	switch m.Type {
	case MsgBeat:
		r.bcastHeartbeat()
		return
	case MsgProp:
		if len(m.Entries) == 0 {
			return
		}
		if _, ok := r.trk.Progress[r.id]; !ok {
			// We were removed from the configuration.
			return
		}
		for i := range m.Entries {
			if m.Entries[i].Type == EntryConfChange {
				if r.pendingConfIndex > r.raftLog.applied {
					// A conf change is already in flight; refuse this one.
					m.Entries[i] = Entry{Type: EntryNormal}
				} else {
					r.pendingConfIndex = r.raftLog.lastIndex() + uint64(i) + 1
				}
			}
		}
		r.appendEntry(m.Entries...)
		r.bcastAppend()
		return
	case MsgReadIndex:
		if r.trk.quorum() <= 1 {
			// Single-voter cluster: serve immediately.
			r.appendReadState(r.raftLog.committed, m)
			return
		}
		// Only safe to serve reads once we've committed in our own term.
		if ct, _ := r.raftLog.term(r.raftLog.committed); ct != r.Term {
			return
		}
		r.readOnly.addRequest(r.raftLog.committed, m)
		r.bcastHeartbeatWithCtx(m.Context)
		return
	}

	pr := r.trk.Progress[m.From]
	if pr == nil {
		return
	}
	switch m.Type {
	case MsgAppResp:
		pr.RecentActive = true
		if m.Reject {
			if pr.maybeDecrTo(m.Index, m.RejectHint) {
				if pr.State == ProgressStateReplicate {
					pr.becomeProbe()
				}
				r.sendAppend(m.From)
			}
		} else {
			if pr.maybeUpdate(m.Index) {
				switch {
				case pr.State == ProgressStateProbe:
					pr.becomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.Match >= pr.PendingSnapshot:
					pr.becomeProbe()
					pr.becomeReplicate()
				}
				if r.maybeCommit() {
					r.bcastAppend()
				} else if pr.Match < r.raftLog.lastIndex() {
					r.sendAppend(m.From)
				}
			}
		}

	case MsgHeartbeatResp:
		pr.RecentActive = true
		pr.ProbeSent = false
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}
		// ReadIndex confirmation.
		if len(m.Context) > 0 {
			if r.readOnly.recvAck(m.From, m.Context) >= r.trk.quorum() {
				for _, rs := range r.readOnly.advance(m.Context) {
					if rs.req.From == None || rs.req.From == r.id {
						r.appendReadState(rs.index, rs.req)
					} else {
						// Forward the confirmed read index back to the follower.
						r.send(Message{To: rs.req.From, Type: MsgReadIndexResp, Index: rs.index, Context: rs.req.Context})
					}
				}
			}
		}

	case MsgSnap:
		// Stale snapshot response handling not needed in the simplified model.
	}
}

func (r *raft) stepCandidate(m Message) {
	// In a candidate/pre-candidate, only the matching vote-response type counts.
	var myVoteRespType MessageType
	if r.state == StatePreCandidate {
		myVoteRespType = MsgPreVoteResp
	} else {
		myVoteRespType = MsgVoteResp
	}
	switch m.Type {
	case MsgProp:
		// No leader; drop.
		return
	case MsgApp:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case MsgSnap:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case myVoteRespType:
		gr, rej, res := r.poll(m.From, m.Type, !m.Reject)
		switch res {
		case voteWon:
			if r.state == StatePreCandidate {
				r.campaign(campaignElection)
			} else {
				r.becomeLeader()
				r.bcastAppend()
			}
		case voteLost:
			r.becomeFollower(r.Term, None)
		}
		_ = gr
		_ = rej
	}
}

func (r *raft) stepFollower(m Message) {
	switch m.Type {
	case MsgProp:
		if r.lead == None {
			return // dropped
		}
		m.To = r.lead
		r.send(m)
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case MsgReadIndex:
		if r.lead == None {
			return
		}
		m.To = r.lead
		r.send(m)
	case MsgReadIndexResp:
		r.appendReadState(m.Index, Message{From: r.id, Context: m.Context})
	}
}

func (r *raft) handleAppendEntries(m Message) {
	if m.Index < r.raftLog.committed {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.raftLog.committed})
		return
	}
	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: mlastIndex})
	} else {
		// Reject and give a hint to speed up backtracking.
		hint := min(m.Index, r.raftLog.lastIndex())
		r.send(Message{To: m.From, Type: MsgAppResp, Index: m.Index, Reject: true, RejectHint: hint})
	}
}

func (r *raft) handleHeartbeat(m Message) {
	r.raftLog.commitTo(m.Commit)
	r.send(Message{To: m.From, Type: MsgHeartbeatResp, Context: m.Context})
}

func (r *raft) handleSnapshot(m Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(*m.Snapshot) {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.raftLog.committed})
	}
	_, _ = sindex, sterm
}

// restore applies a received snapshot to the log and reconfigures membership.
func (r *raft) restore(s Snapshot) bool {
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}
	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		// We already have everything up to the snapshot; just fast-forward commit.
		r.raftLog.commitTo(s.Metadata.Index)
		return false
	}
	r.raftLog.restore(s)

	// Reset membership from the snapshot's ConfState.
	r.trk.Progress = map[uint64]*Progress{}
	cs := s.Metadata.ConfState
	for _, id := range cs.Voters {
		r.trk.Progress[id] = &Progress{Next: r.raftLog.lastIndex() + 1}
	}
	for _, id := range cs.Learners {
		r.trk.Progress[id] = &Progress{Next: r.raftLog.lastIndex() + 1, IsLearner: true}
	}
	if pr := r.trk.Progress[r.id]; pr != nil {
		pr.Match = r.raftLog.lastIndex()
	}
	return true
}

func (r *raft) appendReadState(index uint64, req Message) {
	r.readStates = append(r.readStates, ReadState{Index: index, Ctx: req.Context})
}

// applyConfChange applies a committed configuration change and returns the new
// ConfState. Called by the driver (via RawNode.ApplyConfChange).
func (r *raft) applyConfChange(cc ConfChange) ConfState {
	switch cc.Type {
	case ConfChangeAddNode:
		r.addNode(cc.NodeID, false)
	case ConfChangeAddLearnerNode:
		r.addNode(cc.NodeID, true)
	case ConfChangePromoteLearner:
		if pr := r.trk.Progress[cc.NodeID]; pr != nil {
			pr.IsLearner = false
		}
	case ConfChangeRemoveNode:
		delete(r.trk.Progress, cc.NodeID)
		if cc.NodeID == r.id {
			r.removed = true
			r.becomeFollower(r.Term, None)
		}
	}
	// If we are the leader, a membership change may move the commit index.
	if r.state == StateLeader {
		if r.maybeCommit() {
			r.bcastAppend()
		}
	}
	return r.confState()
}

func (r *raft) addNode(id uint64, learner bool) {
	pr := r.trk.Progress[id]
	if pr == nil {
		r.trk.Progress[id] = &Progress{Next: r.raftLog.lastIndex() + 1, IsLearner: learner}
		return
	}
	if !learner {
		pr.IsLearner = false
	}
}
