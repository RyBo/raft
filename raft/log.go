package raft

import "fmt"

// raftLog presents a unified view over persisted entries (in Storage) and
// not-yet-persisted entries (in unstable). Indices are global log indices.
type raftLog struct {
	storage  Storage
	unstable unstable

	committed uint64 // highest index known committed
	applied   uint64 // highest index applied to the state machine

	maxNextEntsSize uint64 // byte cap on CommittedEntries per Ready
}

func newLog(storage Storage, maxNextEntsSize uint64) *raftLog {
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	l := &raftLog{
		storage:         storage,
		maxNextEntsSize: maxNextEntsSize,
	}
	l.unstable.offset = lastIndex + 1
	l.committed = firstIndex - 1
	l.applied = firstIndex - 1
	return l
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d applied=%d unstable.offset=%d len(unstable.Entries)=%d",
		l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

// maybeAppend is the follower's append path. It checks that the entry preceding
// the new ones matches (index/term), then appends, returning the new last index.
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...Entry) (lastnewi uint64, ok bool) {
	if !l.matchTerm(index, logTerm) {
		return 0, false
	}
	lastnewi = index + uint64(len(ents))
	ci := l.findConflict(ents)
	switch {
	case ci == 0:
		// no conflict; entries already present
	case ci <= l.committed:
		panic(fmt.Sprintf("entry %d conflicts with committed entry [committed(%d)]", ci, l.committed))
	default:
		offset := index + 1
		l.append(ents[ci-offset:]...)
	}
	l.commitTo(min(committed, lastnewi))
	return lastnewi, true
}

func (l *raftLog) append(ents ...Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		panic(fmt.Sprintf("after(%d) is out of range [committed(%d)]", after, l.committed))
	}
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

// findConflict returns the index of the first entry that conflicts with the
// existing log (same index, different term), or the first new entry's index, or
// 0 if there is no conflict and all entries already exist.
func (l *raftLog) findConflict(ents []Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				// conflicting term at an existing index
			}
			return ne.Index
		}
	}
	return 0
}

func (l *raftLog) unstableEntries() []Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

// nextEnts returns committed-but-not-applied entries ready for the FSM.
func (l *raftLog) nextEnts() []Entry {
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1, l.maxNextEntsSize)
		if err != nil {
			panic(fmt.Sprintf("unexpected error when getting unapplied entries (%v)", err))
		}
		return ents
	}
	return nil
}

func (l *raftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off
}

func (l *raftLog) snapshot() (Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *raftLog) hasPendingSnapshot() bool {
	return l.unstable.snapshot != nil && !l.unstable.snapshot.isEmpty()
}

func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	i, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *raftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			panic(fmt.Sprintf("tocommit(%d) is out of range [lastIndex(%d)]", tocommit, l.lastIndex()))
		}
		l.committed = tocommit
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		panic(fmt.Sprintf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed))
	}
	l.applied = i
}

func (l *raftLog) stableTo(i, t uint64)  { l.unstable.stableTo(i, t) }
func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		panic(fmt.Sprintf("unexpected error when getting the last term (%v)", err))
	}
	return t
}

func (l *raftLog) term(i uint64) (uint64, error) {
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.lastIndex() {
		return 0, nil
	}
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

func (l *raftLog) entries(i, maxsize uint64) ([]Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxsize)
}

// slice returns entries in [lo, hi), pulling from storage and/or unstable.
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]Entry, error) {
	if err := l.mustCheckOutOfBounds(lo, hi); err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []Entry
	if lo < l.unstable.offset {
		storedhi := min(hi, l.unstable.offset)
		storedEnts, err := l.storage.Entries(lo, storedhi, maxSize)
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			panic(fmt.Sprintf("entries[%d:%d) is unavailable from storage", lo, storedhi))
		} else if err != nil {
			panic(err)
		}
		if uint64(len(storedEnts)) < storedhi-lo {
			return storedEnts, nil
		}
		ents = storedEnts
	}
	if hi > l.unstable.offset {
		unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		if len(ents) > 0 {
			combined := make([]Entry, len(ents)+len(unstable))
			copy(combined, ents)
			copy(combined[len(ents):], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		panic(fmt.Sprintf("invalid slice %d > %d", lo, hi))
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}
	length := l.lastIndex() + 1 - fi
	if hi > fi+length {
		panic(fmt.Sprintf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex()))
	}
	return nil
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

// maybeCommit advances the commit index to maxIndex if that entry is from the
// current term. Returns true if the commit index advanced.
func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed {
		if t, _ := l.term(maxIndex); t == term {
			l.commitTo(maxIndex)
			return true
		}
	}
	return false
}

// isUpToDate reports whether a log with (lasti, term) is at least as up-to-date
// as this log — the Raft voting rule.
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

func (l *raftLog) restore(s Snapshot) {
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}
