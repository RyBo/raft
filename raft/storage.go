package raft

import (
	"errors"
	"sync"
)

// ErrCompacted is returned when a requested index is older than the snapshot.
var ErrCompacted = errors.New("raft: requested index is unavailable due to compaction")

// ErrUnavailable is returned when requested entries are not yet available.
var ErrUnavailable = errors.New("raft: requested entry at index is unavailable")

// ErrSnapshotOutOfDate is returned when applying a stale snapshot.
var ErrSnapshotOutOfDate = errors.New("raft: snapshot is older than the current state")

// Storage is what the core reads from. The driver is responsible for writing to
// it (persisting HardState, entries and snapshots from Ready) before the core is
// allowed to act on them. The core only ever reads.
//
// All methods may be called concurrently with driver writes in a real
// deployment, so implementations must be safe; MemoryStorage uses a mutex. In
// the single-goroutine simulation there is no concurrency.
type Storage interface {
	// InitialState returns the saved HardState and ConfState (from a snapshot or
	// the most recent persisted configuration).
	InitialState() (HardState, ConfState, error)
	// Entries returns log entries in [lo, hi) up to maxSize bytes. lo must be
	// greater than the snapshot index.
	Entries(lo, hi, maxSize uint64) ([]Entry, error)
	// Term returns the term of the entry at index i, which must be in the range
	// [FirstIndex-1, LastIndex]. The term of the snapshot index is retrievable.
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first available entry. Older entries
	// have been incorporated into the most recent snapshot.
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.
	Snapshot() (Snapshot, error)
}

// MemoryStorage is an in-memory Storage implementation used as the reference
// store and as the basis for the simulation's per-node "disk".
type MemoryStorage struct {
	mu        sync.Mutex
	hardState HardState
	snapshot  Snapshot
	// ents[0] is a dummy entry whose Index/Term equal the snapshot's; real
	// entries start at ents[1]. This mirrors etcd/raft and simplifies the math.
	ents []Entry
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{ents: make([]Entry, 1)}
}

func (ms *MemoryStorage) InitialState() (HardState, ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState persists the HardState. Driver-side write method.
func (ms *MemoryStorage) SetHardState(st HardState) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.hardState = st
}

func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]Entry, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		return nil, ErrUnavailable
	}
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}
	ents := ms.ents[lo-offset : hi-offset]
	return limitSize(ents, maxSize), nil
}

func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.ents[0].Index + 1, nil
}

func (ms *MemoryStorage) Snapshot() (Snapshot, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.snapshot, nil
}

// Append stores new log entries. Driver-side write method. Entries must be
// contiguous and start no earlier than the current first index.
func (ms *MemoryStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	first := ms.ents[0].Index + 1
	last := entries[0].Index + uint64(len(entries)) - 1
	if last < first {
		// All entries already covered by the snapshot.
		return nil
	}
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		// Overwrite conflicting tail.
		ms.ents = append([]Entry{}, ms.ents[:offset]...)
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...)
	default:
		// Gap between the log and the new entries: shouldn't happen.
		return ErrUnavailable
	}
	return nil
}

// ApplySnapshot installs a snapshot, replacing the log. Driver-side write.
func (ms *MemoryStorage) ApplySnapshot(snap Snapshot) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.snapshot.Metadata.Index >= snap.Metadata.Index {
		return ErrSnapshotOutOfDate
	}
	ms.snapshot = snap
	ms.ents = []Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot records a snapshot at index i with the given confstate and FSM
// data, and is used by the driver during compaction.
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *ConfState, data []byte) (Snapshot, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if i <= ms.snapshot.Metadata.Index {
		return Snapshot{}, ErrSnapshotOutOfDate
	}
	offset := ms.ents[0].Index
	if i > ms.lastIndex() {
		return Snapshot{}, ErrUnavailable
	}
	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

// Compact discards log entries up to (and including) compactIndex.
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		return ErrUnavailable
	}
	i := compactIndex - offset
	ents := make([]Entry, 1, uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

func limitSize(ents []Entry, maxSize uint64) []Entry {
	if len(ents) == 0 || maxSize == noLimit {
		return ents
	}
	size := entSize(ents[0])
	limit := 1
	for ; limit < len(ents); limit++ {
		size += entSize(ents[limit])
		if size > maxSize {
			break
		}
	}
	return ents[:limit]
}

func entSize(e Entry) uint64 {
	// Approximate on-the-wire size; exact value doesn't matter for batching.
	return uint64(len(e.Data)) + 24
}
