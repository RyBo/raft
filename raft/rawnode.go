package raft

// Ready bundles the outputs the driver must process for one cycle, in order:
//
//  1. persist HardState (if set) + Entries + Snapshot to stable storage
//  2. send Messages
//  3. apply CommittedEntries to the state machine
//  4. serve ReadStates
//  5. call RawNode.Advance
//
// Persisting before sending is the safety contract: a message must never reflect
// state that could be lost on a crash.
type Ready struct {
	// SoftState is non-nil only when the leader/role changed. Volatile; not
	// persisted, but handy for observers like the UI.
	*SoftState

	// HardState is the state to persist before sending messages. Empty if
	// unchanged since the last Ready.
	HardState

	// ReadStates are confirmed linearizable read requests.
	ReadStates []ReadState

	// Entries are new log entries to persist (not yet committed).
	Entries []Entry

	// Snapshot, if non-empty, must be persisted to storage.
	Snapshot Snapshot

	// CommittedEntries are committed entries to apply to the state machine.
	CommittedEntries []Entry

	// Messages to send after persisting Entries/HardState/Snapshot.
	Messages []Message
}

func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil ||
		!rd.HardState.isEmpty() ||
		!rd.Snapshot.isEmpty() ||
		len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 ||
		len(rd.Messages) > 0 ||
		len(rd.ReadStates) > 0
}

// RawNode is a thread-unsafe, synchronous wrapper around the raft state machine.
// It is the primary API for drivers that own their own scheduling — including
// the single-goroutine simulation and a real-network server loop.
type RawNode struct {
	raft       *raft
	prevSoftSt SoftState
	prevHardSt HardState
}

// NewRawNode creates a RawNode. If peers is non-empty and the storage has no
// existing configuration, the node bootstraps with those voters. All nodes in a
// fresh cluster must be given the same peer list.
func NewRawNode(c *Config, peers []uint64) (*RawNode, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	r := newRaft(c)

	if len(r.trk.Progress) == 0 && len(peers) > 0 {
		for _, id := range peers {
			r.trk.Progress[id] = &Progress{Next: 1}
		}
		r.becomeFollower(r.Term, None)
	}

	rn := &RawNode{raft: r}
	rn.prevSoftSt = r.softState()
	rn.prevHardSt = r.hardState()
	return rn, nil
}

// Tick advances the logical clock by one tick.
func (rn *RawNode) Tick() { rn.raft.tick() }

// Campaign forces the node to start an election immediately.
func (rn *RawNode) Campaign() error {
	return rn.raft.Step(Message{Type: MsgHup})
}

// Propose appends a normal entry to the log (leader) or forwards it (follower).
func (rn *RawNode) Propose(data []byte) error {
	return rn.raft.Step(Message{
		Type:    MsgProp,
		From:    rn.raft.id,
		Entries: []Entry{{Type: EntryNormal, Data: data}},
	})
}

// ProposeConfChange proposes a single-server membership change.
func (rn *RawNode) ProposeConfChange(cc ConfChange) error {
	data := encodeConfChange(cc)
	return rn.raft.Step(Message{
		Type:    MsgProp,
		From:    rn.raft.id,
		Entries: []Entry{{Type: EntryConfChange, Data: data}},
	})
}

// ApplyConfChange applies a committed configuration change and returns the new
// ConfState. The driver calls this when it applies an EntryConfChange.
func (rn *RawNode) ApplyConfChange(cc ConfChange) ConfState {
	return rn.raft.applyConfChange(cc)
}

// ReadIndex requests a linearizable read. The matching ReadState (carrying the
// same ctx) appears in a later Ready once the read index is confirmed.
func (rn *RawNode) ReadIndex(ctx []byte) error {
	return rn.raft.Step(Message{Type: MsgReadIndex, From: rn.raft.id, Context: ctx})
}

// Step feeds an inbound message into the state machine.
func (rn *RawNode) Step(m Message) error {
	// Ignore local messages that should not arrive over the wire.
	if m.Type == MsgHup || m.Type == MsgBeat {
		return nil
	}
	return rn.raft.Step(m)
}

// HasReady reports whether there are updates to process.
func (rn *RawNode) HasReady() bool {
	r := rn.raft
	if !r.softState().equal(rn.prevSoftSt) {
		return true
	}
	if hs := r.hardState(); !hs.isEmpty() && !hs.equal(rn.prevHardSt) {
		return true
	}
	if r.raftLog.hasPendingSnapshot() {
		return true
	}
	if len(r.msgs) > 0 || len(r.raftLog.unstableEntries()) > 0 || r.raftLog.hasNextEnts() {
		return true
	}
	if len(r.readStates) > 0 {
		return true
	}
	return false
}

// Ready returns the current batch of updates and marks them accepted. The caller
// MUST process the batch and then call Advance.
func (rn *RawNode) Ready() Ready {
	r := rn.raft
	rd := Ready{
		Entries:          r.raftLog.unstableEntries(),
		CommittedEntries: r.raftLog.nextEnts(),
		Messages:         r.msgs,
	}
	if softSt := r.softState(); !softSt.equal(rn.prevSoftSt) {
		s := softSt
		rd.SoftState = &s
	}
	if hardSt := r.hardState(); !hardSt.equal(rn.prevHardSt) {
		rd.HardState = hardSt
	}
	if r.raftLog.hasPendingSnapshot() {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	if len(r.readStates) > 0 {
		rd.ReadStates = r.readStates
	}

	rn.acceptReady(rd)
	return rd
}

// acceptReady updates bookkeeping for a Ready that was returned to the caller.
func (rn *RawNode) acceptReady(rd Ready) {
	if rd.SoftState != nil {
		rn.prevSoftSt = *rd.SoftState
	}
	if !rd.HardState.isEmpty() {
		rn.prevHardSt = rd.HardState
	}
	rn.raft.msgs = nil
	rn.raft.readStates = nil
}

// Advance notifies the node that the previous Ready has been persisted, sent and
// applied. It moves the stable and applied watermarks forward.
func (rn *RawNode) Advance(rd Ready) {
	r := rn.raft
	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.raftLog.stableTo(e.Index, e.Term)
	}
	if !rd.Snapshot.isEmpty() {
		r.raftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
	if len(rd.CommittedEntries) > 0 {
		e := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		r.raftLog.appliedTo(e.Index)
	}
}

// Status returns a snapshot of the node's state for observers.
func (rn *RawNode) Status() Status { return getStatus(rn.raft) }

// Removed reports whether this node has been removed from the configuration.
func (rn *RawNode) Removed() bool { return rn.raft.removed }

// ConfState returns the node's current view of cluster membership.
func (rn *RawNode) ConfState() ConfState { return rn.raft.confState() }

// Term returns the term of the log entry at index i (for snapshot handoff).
func (rn *RawNode) Term(i uint64) (uint64, error) { return rn.raft.raftLog.term(i) }
