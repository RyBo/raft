package raft

// Status is a read-only snapshot of a node's state, intended for observers such
// as the visualization UI. It is safe to inspect and serialize.
type Status struct {
	ID        uint64
	Term      uint64
	Vote      uint64
	Lead      uint64
	State     StateType
	Commit    uint64
	Applied   uint64
	LastIndex uint64

	// Progress is a copy of per-follower replication progress (leader only).
	Progress map[uint64]Progress

	// Log holds all currently-retained entries (after the snapshot point).
	Log []Entry
}

func getStatus(r *raft) Status {
	s := Status{
		ID:        r.id,
		Term:      r.Term,
		Vote:      r.Vote,
		Lead:      r.lead,
		State:     r.state,
		Commit:    r.raftLog.committed,
		Applied:   r.raftLog.applied,
		LastIndex: r.raftLog.lastIndex(),
	}
	if r.state == StateLeader {
		s.Progress = make(map[uint64]Progress, len(r.trk.Progress))
		for id, pr := range r.trk.Progress {
			s.Progress[id] = *pr
		}
	}
	s.Log = r.allEntries()
	return s
}

// allEntries returns every retained log entry (from firstIndex to lastIndex).
func (r *raft) allEntries() []Entry {
	fi := r.raftLog.firstIndex()
	li := r.raftLog.lastIndex()
	if li < fi {
		return nil
	}
	ents, err := r.raftLog.slice(fi, li+1, noLimit)
	if err != nil {
		return nil
	}
	return ents
}
