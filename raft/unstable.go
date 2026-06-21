package raft

// unstable holds log entries (and possibly a snapshot) that have not yet been
// written to Storage by the driver. The leader appends here first; once the
// driver persists them (via Ready/Advance) they move into Storage.
type unstable struct {
	snapshot *Snapshot
	entries  []Entry
	offset   uint64 // index of entries[0]
}

// maybeFirstIndex returns the first index covered by a pending snapshot.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index held in unstable (entries or snapshot).
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeTerm returns the term of the entry at index i if it is held in unstable.
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}
	last, ok := u.maybeLastIndex()
	if !ok || i > last {
		return 0, false
	}
	return u.entries[i-u.offset].Term, true
}

// stableTo marks entries up to index i (with matching term t) as persisted.
func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	if gt == t && i >= u.offset {
		u.entries = u.entries[i+1-u.offset:]
		u.offset = i + 1
		u.shrink()
	}
}

// shrink reallocates the backing array when it has grown much larger than the
// live slice, to avoid pinning memory.
func (u *unstable) shrink() {
	if len(u.entries) == 0 {
		u.entries = nil
	} else if cap(u.entries) > 2*len(u.entries) {
		ne := make([]Entry, len(u.entries))
		copy(ne, u.entries)
		u.entries = ne
	}
}

// stableSnapTo marks the pending snapshot at index i as persisted.
func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

// restore replaces unstable state with a snapshot.
func (u *unstable) restore(s Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	snap := s
	u.snapshot = &snap
}

// truncateAndAppend appends entries, truncating any conflicting suffix first.
func (u *unstable) truncateAndAppend(ents []Entry) {
	after := ents[0].Index
	switch {
	case after == u.offset+uint64(len(u.entries)):
		u.entries = append(u.entries, ents...)
	case after <= u.offset:
		// Replace the whole unstable slice; the new entries take over.
		u.offset = after
		u.entries = ents
	default:
		// Keep [offset, after) and append.
		keep := append([]Entry{}, u.entries[:after-u.offset]...)
		u.entries = append(keep, ents...)
	}
}

func (u *unstable) slice(lo, hi uint64) []Entry {
	return u.entries[lo-u.offset : hi-u.offset]
}
