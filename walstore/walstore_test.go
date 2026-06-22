package walstore

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/rybo/raft/raft"
)

func ent(term, index uint64, data string) raft.Entry {
	return raft.Entry{Term: term, Index: index, Type: raft.EntryNormal, Data: []byte(data)}
}

// mustOpen opens a WAL or fails the test.
func mustOpen(t *testing.T, dir string) *WAL {
	t.Helper()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return w
}

func TestRoundTrip(t *testing.T) {
	w := mustOpen(t, t.TempDir())
	defer w.Close()

	w.SetHardState(raft.HardState{Term: 3, Vote: 1, Commit: 2})
	if err := w.Append([]raft.Entry{ent(1, 1, "a"), ent(1, 2, "b"), ent(2, 3, "c")}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	hs, _, _ := w.InitialState()
	if hs != (raft.HardState{Term: 3, Vote: 1, Commit: 2}) {
		t.Fatalf("hardstate = %+v", hs)
	}
	if li, _ := w.LastIndex(); li != 3 {
		t.Fatalf("LastIndex = %d, want 3", li)
	}
	if fi, _ := w.FirstIndex(); fi != 1 {
		t.Fatalf("FirstIndex = %d, want 1", fi)
	}
	got, err := w.Entries(1, 4, noLimit)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(got) != 3 || string(got[2].Data) != "c" || got[2].Term != 2 {
		t.Fatalf("Entries = %+v", got)
	}
	if term, _ := w.Term(3); term != 2 {
		t.Fatalf("Term(3) = %d, want 2", term)
	}
}

func TestReopenRecovers(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	w.SetHardState(raft.HardState{Term: 5, Vote: 2, Commit: 4})
	if err := w.Append([]raft.Entry{ent(1, 1, "x"), ent(2, 2, "y"), ent(5, 3, "z"), ent(5, 4, "w")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2 := mustOpen(t, dir)
	defer w2.Close()

	hs, _, _ := w2.InitialState()
	if hs != (raft.HardState{Term: 5, Vote: 2, Commit: 4}) {
		t.Fatalf("recovered hardstate = %+v", hs)
	}
	if li, _ := w2.LastIndex(); li != 4 {
		t.Fatalf("recovered LastIndex = %d, want 4", li)
	}
	got, _ := w2.Entries(1, 5, noLimit)
	want := []raft.Entry{ent(1, 1, "x"), ent(2, 2, "y"), ent(5, 3, "z"), ent(5, 4, "w")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered entries = %+v, want %+v", got, want)
	}
}

// TestOverwriteRecovers checks that an append which overwrites a conflicting tail
// (same index, higher term) recovers to the final state even though the WAL is
// append-only and still physically contains the superseded records.
func TestOverwriteRecovers(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	if err := w.Append([]raft.Entry{ent(1, 1, "a"), ent(1, 2, "old"), ent(1, 3, "old3")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Leader change: index 2 and 3 are replaced at a higher term.
	if err := w.Append([]raft.Entry{ent(2, 2, "new"), ent(2, 3, "new3")}); err != nil {
		t.Fatalf("Append overwrite: %v", err)
	}
	w.Close()

	w2 := mustOpen(t, dir)
	defer w2.Close()
	got, _ := w2.Entries(1, 4, noLimit)
	want := []raft.Entry{ent(1, 1, "a"), ent(2, 2, "new"), ent(2, 3, "new3")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after overwrite recovered = %+v, want %+v", got, want)
	}
}

// TestTornTailDiscarded corrupts the trailing bytes of wal.log and verifies the
// last fully written record set still recovers.
func TestTornTailDiscarded(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	w.SetHardState(raft.HardState{Term: 1, Commit: 2})
	if err := w.Append([]raft.Entry{ent(1, 1, "a"), ent(1, 2, "b")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	w.Close()

	// Append garbage that looks like a partial record (a header claiming a
	// payload that isn't fully present).
	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	// 8-byte header + truncated body.
	if _, err := f.Write([]byte{0x40, 0, 0, 0, 0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02}); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	f.Close()

	w2 := mustOpen(t, dir)
	defer w2.Close()
	if li, _ := w2.LastIndex(); li != 2 {
		t.Fatalf("after torn tail LastIndex = %d, want 2", li)
	}
	got, _ := w2.Entries(1, 3, noLimit)
	if len(got) != 2 || string(got[1].Data) != "b" {
		t.Fatalf("after torn tail entries = %+v", got)
	}

	// The torn tail must be truncated, so a fresh append still recovers cleanly.
	if err := w2.Append([]raft.Entry{ent(1, 3, "c")}); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	w2.Close()
	w3 := mustOpen(t, dir)
	defer w3.Close()
	if li, _ := w3.LastIndex(); li != 3 {
		t.Fatalf("after re-append LastIndex = %d, want 3", li)
	}
}

func TestSnapshotInstallAndReopen(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	snap := raft.Snapshot{
		Data: []byte(`{"k":"v"}`),
		Metadata: raft.SnapshotMetadata{
			Index:     10,
			Term:      4,
			ConfState: raft.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}},
		},
	}
	if err := w.ApplySnapshot(snap); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	// Append entries past the snapshot.
	if err := w.Append([]raft.Entry{ent(4, 11, "k11"), ent(4, 12, "k12")}); err != nil {
		t.Fatalf("Append after snapshot: %v", err)
	}
	w.Close()

	w2 := mustOpen(t, dir)
	defer w2.Close()

	gotSnap, _ := w2.Snapshot()
	if !reflect.DeepEqual(gotSnap, snap) {
		t.Fatalf("recovered snapshot = %+v, want %+v", gotSnap, snap)
	}
	_, cs, _ := w2.InitialState()
	if !reflect.DeepEqual(cs, snap.Metadata.ConfState) {
		t.Fatalf("recovered confstate = %+v", cs)
	}
	if fi, _ := w2.FirstIndex(); fi != 11 {
		t.Fatalf("FirstIndex = %d, want 11", fi)
	}
	if li, _ := w2.LastIndex(); li != 12 {
		t.Fatalf("LastIndex = %d, want 12", li)
	}
	if _, err := w2.Entries(5, 11, noLimit); err != raft.ErrCompacted {
		t.Fatalf("Entries below snapshot err = %v, want ErrCompacted", err)
	}
	if term, _ := w2.Term(10); term != 4 {
		t.Fatalf("Term(10) = %d, want 4 (snapshot term)", term)
	}
}

func TestCreateSnapshotCompactReopen(t *testing.T) {
	dir := t.TempDir()

	w := mustOpen(t, dir)
	var ents []raft.Entry
	for i := uint64(1); i <= 20; i++ {
		ents = append(ents, ent(1, i, "d"))
	}
	if err := w.Append(ents); err != nil {
		t.Fatalf("Append: %v", err)
	}
	cs := raft.ConfState{Voters: []uint64{1, 2, 3}}
	if _, err := w.CreateSnapshot(15, &cs, []byte("state")); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if err := w.Compact(15); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if fi, _ := w.FirstIndex(); fi != 16 {
		t.Fatalf("FirstIndex after compact = %d, want 16", fi)
	}
	w.Close()

	w2 := mustOpen(t, dir)
	defer w2.Close()
	if fi, _ := w2.FirstIndex(); fi != 16 {
		t.Fatalf("recovered FirstIndex = %d, want 16", fi)
	}
	if li, _ := w2.LastIndex(); li != 20 {
		t.Fatalf("recovered LastIndex = %d, want 20", li)
	}
	if _, err := w2.Entries(10, 16, noLimit); err != raft.ErrCompacted {
		t.Fatalf("Entries below compact err = %v, want ErrCompacted", err)
	}
	got, _ := w2.Entries(16, 21, noLimit)
	if len(got) != 5 || got[0].Index != 16 {
		t.Fatalf("recovered tail = %+v", got)
	}
}

// storage is the common surface of *WAL and *raft.MemoryStorage, used for parity.
type storage interface {
	raft.Storage
	SetHardState(raft.HardState)
	Append([]raft.Entry) error
	ApplySnapshot(raft.Snapshot) error
	CreateSnapshot(uint64, *raft.ConfState, []byte) (raft.Snapshot, error)
	Compact(uint64) error
}

// TestParityWithMemoryStorage runs the same op sequence against both stores and
// asserts every read returns identical results — proving the drop-in contract.
func TestParityWithMemoryStorage(t *testing.T) {
	wal := mustOpen(t, t.TempDir())
	defer wal.Close()
	mem := raft.NewMemoryStorage()
	stores := []storage{wal, mem}

	apply := func(fn func(s storage)) {
		for _, s := range stores {
			fn(s)
		}
	}

	apply(func(s storage) { s.SetHardState(raft.HardState{Term: 2, Vote: 3, Commit: 5}) })
	var ents []raft.Entry
	for i := uint64(1); i <= 12; i++ {
		ents = append(ents, ent(1+i/6, i, "v"))
	}
	apply(func(s storage) {
		if err := s.Append(ents); err != nil {
			t.Fatalf("Append: %v", err)
		}
	})
	cs := raft.ConfState{Voters: []uint64{1, 2, 3}}
	apply(func(s storage) {
		if _, err := s.CreateSnapshot(8, &cs, []byte("snap")); err != nil {
			t.Fatalf("CreateSnapshot: %v", err)
		}
		if err := s.Compact(8); err != nil {
			t.Fatalf("Compact: %v", err)
		}
	})

	// Compare every read method.
	hsW, csW, _ := wal.InitialState()
	hsM, csM, _ := mem.InitialState()
	if hsW != hsM || !reflect.DeepEqual(csW, csM) {
		t.Fatalf("InitialState mismatch: %+v/%+v vs %+v/%+v", hsW, csW, hsM, csM)
	}
	fiW, _ := wal.FirstIndex()
	fiM, _ := mem.FirstIndex()
	liW, _ := wal.LastIndex()
	liM, _ := mem.LastIndex()
	if fiW != fiM || liW != liM {
		t.Fatalf("index mismatch: first %d/%d last %d/%d", fiW, fiM, liW, liM)
	}
	eW, errW := wal.Entries(fiW, liW+1, noLimit)
	eM, errM := mem.Entries(fiM, liM+1, noLimit)
	if errW != errM || !reflect.DeepEqual(eW, eM) {
		t.Fatalf("Entries mismatch: %+v(%v) vs %+v(%v)", eW, errW, eM, errM)
	}
	for i := fiW - 1; i <= liW; i++ {
		tW, _ := wal.Term(i)
		tM, _ := mem.Term(i)
		if tW != tM {
			t.Fatalf("Term(%d) mismatch: %d vs %d", i, tW, tM)
		}
	}
	sW, _ := wal.Snapshot()
	sM, _ := mem.Snapshot()
	if !reflect.DeepEqual(sW, sM) {
		t.Fatalf("Snapshot mismatch: %+v vs %+v", sW, sM)
	}
}

func TestErrorsMatchMemoryStorage(t *testing.T) {
	w := mustOpen(t, t.TempDir())
	defer w.Close()
	w.Append([]raft.Entry{ent(1, 1, "a")})

	// CreateSnapshot beyond last index.
	if _, err := w.CreateSnapshot(99, nil, nil); err != raft.ErrUnavailable {
		t.Fatalf("CreateSnapshot(99) err = %v, want ErrUnavailable", err)
	}
	// Stale snapshot.
	cs := raft.ConfState{Voters: []uint64{1}}
	if _, err := w.CreateSnapshot(1, &cs, nil); err != nil {
		t.Fatalf("CreateSnapshot(1): %v", err)
	}
	if _, err := w.CreateSnapshot(1, &cs, nil); err != raft.ErrSnapshotOutOfDate {
		t.Fatalf("stale CreateSnapshot err = %v, want ErrSnapshotOutOfDate", err)
	}
	// Compact below first index.
	if err := w.Compact(0); err != raft.ErrCompacted {
		t.Fatalf("Compact(0) err = %v, want ErrCompacted", err)
	}
}
