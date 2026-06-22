// Package walstore is a durable, stdlib-only implementation of raft.Storage. It
// persists the Raft log, HardState and snapshots to disk so a node recovers its
// committed state after a process restart or crash. It mirrors the semantics of
// raft.MemoryStorage exactly — the same dummy-index-0 log layout and the same
// driver-side writer methods (SetHardState, Append, ApplySnapshot, CreateSnapshot,
// Compact) — so it is a drop-in replacement for the simulation's in-memory store.
//
// Layout, in the data directory:
//
//	wal.log       append-only, CRC-framed records (entries, hardstate, truncations)
//	snapshot.bin  the most recent snapshot, replaced atomically
//
// Records in wal.log are framed as
//
//	payloadLen uint32 | crc32 uint32 | recType uint8 | payload...
//
// where the CRC covers recType+payload. A torn or partially written trailing
// record (a short read or a CRC mismatch) is discarded on Open. That is safe
// because Raft never relies on an un-fsynced write having survived a crash: the
// driver fsyncs HardState and entries before any message reflecting them is sent.
package walstore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/rybo/raft/raft"
)

// noLimit mirrors the raft core's sentinel for an unbounded Entries size, which
// is not exported from the raft package.
const noLimit = ^uint64(0)

// rewriteThresholdBytes is the wal.log size past which a Compact triggers a
// physical rewrite that drops records made dead by snapshotting. Snapshots keep
// the live log short, so this bounds disk usage over a long-running node.
const rewriteThresholdBytes = 1 << 20

// Record types in wal.log.
const (
	recEntry     uint8 = 1 // one log Entry
	recHardState uint8 = 2 // a HardState (Term/Vote/Commit)
	recTruncate  uint8 = 3 // a new log base (snapshot index/term) from ApplySnapshot or Compact
)

// snapMagic prefixes snapshot.bin so a stray file is rejected.
const snapMagic = "RSN1"

const (
	walName  = "wal.log"
	snapName = "snapshot.bin"
)

// WAL is a disk-backed raft.Storage. The in-memory fields mirror
// raft.MemoryStorage; the disk is the source of truth across restarts.
type WAL struct {
	mu        sync.Mutex
	dir       string
	f         *os.File
	w         *bufio.Writer
	hardState raft.HardState
	snapshot  raft.Snapshot
	// ents[0] is a dummy entry whose Index/Term equal the snapshot's; real
	// entries start at ents[1], exactly as in raft.MemoryStorage.
	ents []raft.Entry
}

var _ raft.Storage = (*WAL)(nil)

// Open opens (creating if necessary) the WAL in dir and reconstructs the
// in-memory state by loading any snapshot and replaying the log, discarding a
// torn trailing record.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("walstore: mkdir %s: %w", dir, err)
	}
	w := &WAL{dir: dir, ents: make([]raft.Entry, 1)}
	if err := w.loadSnapshot(); err != nil {
		return nil, err
	}
	if err := w.replay(); err != nil {
		return nil, err
	}
	return w, nil
}

// loadSnapshot seeds the dummy entry and conf state from snapshot.bin, if present.
// A corrupt snapshot is fatal rather than silently ignored: the atomic rename in
// writeSnapshotFile means the file is never torn, so corruption indicates real
// damage and losing it could lose committed state.
func (w *WAL) loadSnapshot() error {
	data, err := os.ReadFile(filepath.Join(w.dir, snapName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("walstore: read snapshot: %w", err)
	}
	snap, err := decodeSnapshot(data)
	if err != nil {
		return fmt.Errorf("walstore: snapshot corrupt: %w", err)
	}
	if snap.Metadata.Index == 0 {
		return nil
	}
	w.snapshot = snap
	w.ents = []raft.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// replay reads wal.log front-to-back, applying each intact record. It stops at
// the first short or CRC-failed record (a torn tail), truncates the file to the
// last good offset, and leaves the write handle positioned at the end.
func (w *WAL) replay() error {
	path := filepath.Join(w.dir, walName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("walstore: open wal: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("walstore: stat wal: %w", err)
	}
	size := fi.Size()

	r := bufio.NewReader(f)
	var good int64
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // clean EOF or torn header
		}
		payloadLen := binary.LittleEndian.Uint32(hdr[0:4])
		wantCRC := binary.LittleEndian.Uint32(hdr[4:8])
		// Guard against a garbage length claiming more than the file holds.
		if good+8+int64(payloadLen) > size {
			break
		}
		body := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, body); err != nil {
			break // torn trailing record
		}
		if crc32.ChecksumIEEE(body) != wantCRC {
			break // corrupt trailing record
		}
		if err := w.applyRecord(body); err != nil {
			f.Close()
			return err
		}
		good += 8 + int64(len(body))
	}
	if err := f.Truncate(good); err != nil {
		f.Close()
		return fmt.Errorf("walstore: truncate wal: %w", err)
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return fmt.Errorf("walstore: seek wal: %w", err)
	}
	w.f = f
	w.w = bufio.NewWriter(f)
	return nil
}

// applyRecord folds one decoded record into the in-memory state.
func (w *WAL) applyRecord(body []byte) error {
	if len(body) < 1 {
		return errors.New("walstore: empty record")
	}
	payload := body[1:]
	switch body[0] {
	case recEntry:
		e, err := decodeEntry(payload)
		if err != nil {
			return err
		}
		return w.appendInMemory([]raft.Entry{e})
	case recHardState:
		hs, err := decodeHardState(payload)
		if err != nil {
			return err
		}
		w.hardState = hs
		return nil
	case recTruncate:
		if len(payload) < 16 {
			return errors.New("walstore: short truncate record")
		}
		w.truncateInMemory(
			binary.LittleEndian.Uint64(payload[0:8]),
			binary.LittleEndian.Uint64(payload[8:16]),
		)
		return nil
	default:
		return fmt.Errorf("walstore: unknown record type %d", body[0])
	}
}

// --- read methods (copied from raft.MemoryStorage to guarantee identical semantics) ---

func (w *WAL) InitialState() (raft.HardState, raft.ConfState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.hardState, w.snapshot.Metadata.ConfState, nil
}

func (w *WAL) Entries(lo, hi, maxSize uint64) ([]raft.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	offset := w.ents[0].Index
	if lo <= offset {
		return nil, raft.ErrCompacted
	}
	if hi > w.lastIndex()+1 {
		return nil, raft.ErrUnavailable
	}
	if len(w.ents) == 1 {
		return nil, raft.ErrUnavailable
	}
	return limitSize(w.ents[lo-offset:hi-offset], maxSize), nil
}

func (w *WAL) Term(i uint64) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	offset := w.ents[0].Index
	if i < offset {
		return 0, raft.ErrCompacted
	}
	if int(i-offset) >= len(w.ents) {
		return 0, raft.ErrUnavailable
	}
	return w.ents[i-offset].Term, nil
}

func (w *WAL) LastIndex() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastIndex(), nil
}

func (w *WAL) lastIndex() uint64 {
	return w.ents[0].Index + uint64(len(w.ents)) - 1
}

func (w *WAL) FirstIndex() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ents[0].Index + 1, nil
}

func (w *WAL) Snapshot() (raft.Snapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapshot, nil
}

// --- driver-side write methods ---

// SetHardState persists the HardState. It panics on an I/O failure: the method
// signature matches MemoryStorage and so cannot return an error, and a HardState
// that is not durable would violate Raft's safety contract (a message may already
// reflect it). A crash here is recoverable by restarting from the last good state.
func (w *WAL) SetHardState(st raft.HardState) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.hardState = st
	if err := w.writeRecord(recHardState, encodeHardState(st)); err != nil {
		panic(fmt.Errorf("walstore: persist hardstate: %w", err))
	}
	if err := w.sync(); err != nil {
		panic(fmt.Errorf("walstore: fsync hardstate: %w", err))
	}
}

// Append stores new log entries and fsyncs. Entries are persisted as written; the
// replay path re-runs the same in-memory append logic, so an append-only log that
// later overwrites a conflicting tail still recovers the correct final state.
func (w *WAL) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.appendInMemory(entries); err != nil {
		return err
	}
	for _, e := range entries {
		if err := w.writeRecord(recEntry, encodeEntry(e)); err != nil {
			return err
		}
	}
	return w.sync()
}

// ApplySnapshot installs a snapshot, replacing the log, and persists it.
func (w *WAL) ApplySnapshot(snap raft.Snapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.snapshot.Metadata.Index >= snap.Metadata.Index {
		return raft.ErrSnapshotOutOfDate
	}
	if err := w.writeSnapshotFile(snap); err != nil {
		return err
	}
	w.snapshot = snap
	w.ents = []raft.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	if err := w.writeRecord(recTruncate, encodeTruncate(snap.Metadata.Index, snap.Metadata.Term)); err != nil {
		return err
	}
	return w.sync()
}

// CreateSnapshot records a snapshot at index i and persists snapshot.bin. The log
// is not trimmed here; Compact does that.
func (w *WAL) CreateSnapshot(i uint64, cs *raft.ConfState, data []byte) (raft.Snapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if i <= w.snapshot.Metadata.Index {
		return raft.Snapshot{}, raft.ErrSnapshotOutOfDate
	}
	if i > w.lastIndex() {
		return raft.Snapshot{}, raft.ErrUnavailable
	}
	offset := w.ents[0].Index
	w.snapshot.Metadata.Index = i
	w.snapshot.Metadata.Term = w.ents[i-offset].Term
	if cs != nil {
		w.snapshot.Metadata.ConfState = *cs
	}
	w.snapshot.Data = data
	if err := w.writeSnapshotFile(w.snapshot); err != nil {
		return raft.Snapshot{}, err
	}
	return w.snapshot, nil
}

// Compact discards log entries up to and including compactIndex, records the new
// base, and physically reclaims the log once it has grown past the threshold.
func (w *WAL) Compact(compactIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	offset := w.ents[0].Index
	if compactIndex <= offset {
		return raft.ErrCompacted
	}
	if compactIndex > w.lastIndex() {
		return raft.ErrUnavailable
	}
	i := compactIndex - offset
	term := w.ents[i].Term
	ents := make([]raft.Entry, 1, uint64(len(w.ents))-i)
	ents[0] = raft.Entry{Index: compactIndex, Term: term}
	ents = append(ents, w.ents[i+1:]...)
	w.ents = ents
	if err := w.writeRecord(recTruncate, encodeTruncate(compactIndex, term)); err != nil {
		return err
	}
	if err := w.sync(); err != nil {
		return err
	}
	return w.maybeRewrite()
}

// Close flushes, fsyncs and closes the log file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}

// --- in-memory mutation helpers (no locking; callers hold w.mu or run in Open) ---

// appendInMemory mirrors raft.MemoryStorage.Append on the in-memory slice.
func (w *WAL) appendInMemory(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	first := w.ents[0].Index + 1
	last := entries[0].Index + uint64(len(entries)) - 1
	if last < first {
		return nil // already covered by the snapshot
	}
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}
	offset := entries[0].Index - w.ents[0].Index
	switch {
	case uint64(len(w.ents)) > offset:
		w.ents = append([]raft.Entry{}, w.ents[:offset]...)
		w.ents = append(w.ents, entries...)
	case uint64(len(w.ents)) == offset:
		w.ents = append(w.ents, entries...)
	default:
		return raft.ErrUnavailable // gap between the log and the new entries
	}
	return nil
}

// truncateInMemory rebases the log at index/term, dropping everything before it.
func (w *WAL) truncateInMemory(index, term uint64) {
	offset := w.ents[0].Index
	if index <= offset {
		return
	}
	if index > w.lastIndex() {
		w.ents = []raft.Entry{{Index: index, Term: term}}
		return
	}
	i := index - offset
	ents := make([]raft.Entry, 1, uint64(len(w.ents))-i)
	ents[0] = raft.Entry{Index: index, Term: term}
	ents = append(ents, w.ents[i+1:]...)
	w.ents = ents
}

// --- low-level disk helpers ---

func (w *WAL) writeRecord(recType uint8, payload []byte) error {
	body := make([]byte, 1+len(payload))
	body[0] = recType
	copy(body[1:], payload)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(body))
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.w.Write(body)
	return err
}

func (w *WAL) sync() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// writeSnapshotFile writes snapshot.bin atomically: temp file, fsync, rename,
// fsync dir.
func (w *WAL) writeSnapshotFile(snap raft.Snapshot) error {
	tmp := filepath.Join(w.dir, snapName+".tmp")
	tf, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := tf.Write(encodeSnapshot(snap)); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(w.dir, snapName)); err != nil {
		return err
	}
	return fsyncDir(w.dir)
}

// maybeRewrite rewrites wal.log to drop records made dead by snapshotting, once
// the file has grown past the threshold.
func (w *WAL) maybeRewrite() error {
	fi, err := w.f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() < rewriteThresholdBytes {
		return nil
	}
	return w.rewrite()
}

// rewrite writes a fresh log holding only the live state (base, hardstate,
// surviving entries) and atomically swaps it in.
func (w *WAL) rewrite() error {
	tmp := filepath.Join(w.dir, walName+".tmp")
	tf, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(tf)
	put := func(recType uint8, payload []byte) error {
		body := make([]byte, 1+len(payload))
		body[0] = recType
		copy(body[1:], payload)
		var hdr [8]byte
		binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(body)))
		binary.LittleEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(body))
		if _, err := bw.Write(hdr[:]); err != nil {
			return err
		}
		_, err := bw.Write(body)
		return err
	}
	if err := put(recTruncate, encodeTruncate(w.ents[0].Index, w.ents[0].Term)); err != nil {
		tf.Close()
		return err
	}
	if err := put(recHardState, encodeHardState(w.hardState)); err != nil {
		tf.Close()
		return err
	}
	for _, e := range w.ents[1:] {
		if err := put(recEntry, encodeEntry(e)); err != nil {
			tf.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Close(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(w.dir, walName)); err != nil {
		return err
	}
	if err := fsyncDir(w.dir); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(w.dir, walName), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	w.f = f
	w.w = bufio.NewWriter(f)
	return nil
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

// --- encoding helpers ---

func encodeEntry(e raft.Entry) []byte {
	b := make([]byte, 21+len(e.Data))
	binary.LittleEndian.PutUint64(b[0:8], e.Term)
	binary.LittleEndian.PutUint64(b[8:16], e.Index)
	b[16] = byte(e.Type)
	binary.LittleEndian.PutUint32(b[17:21], uint32(len(e.Data)))
	copy(b[21:], e.Data)
	return b
}

func decodeEntry(p []byte) (raft.Entry, error) {
	if len(p) < 21 {
		return raft.Entry{}, errors.New("walstore: short entry record")
	}
	dl := binary.LittleEndian.Uint32(p[17:21])
	if uint64(len(p)) < 21+uint64(dl) {
		return raft.Entry{}, errors.New("walstore: truncated entry data")
	}
	e := raft.Entry{
		Term:  binary.LittleEndian.Uint64(p[0:8]),
		Index: binary.LittleEndian.Uint64(p[8:16]),
		Type:  raft.EntryType(p[16]),
	}
	if dl > 0 {
		e.Data = append([]byte(nil), p[21:21+dl]...)
	}
	return e, nil
}

func encodeHardState(hs raft.HardState) []byte {
	b := make([]byte, 24)
	binary.LittleEndian.PutUint64(b[0:8], hs.Term)
	binary.LittleEndian.PutUint64(b[8:16], hs.Vote)
	binary.LittleEndian.PutUint64(b[16:24], hs.Commit)
	return b
}

func decodeHardState(p []byte) (raft.HardState, error) {
	if len(p) < 24 {
		return raft.HardState{}, errors.New("walstore: short hardstate record")
	}
	return raft.HardState{
		Term:   binary.LittleEndian.Uint64(p[0:8]),
		Vote:   binary.LittleEndian.Uint64(p[8:16]),
		Commit: binary.LittleEndian.Uint64(p[16:24]),
	}, nil
}

func encodeTruncate(index, term uint64) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[0:8], index)
	binary.LittleEndian.PutUint64(b[8:16], term)
	return b
}

func encodeSnapshot(s raft.Snapshot) []byte {
	body := make([]byte, 0, 32+len(s.Data))
	body = appendU64(body, s.Metadata.Index)
	body = appendU64(body, s.Metadata.Term)
	body = appendU32(body, uint32(len(s.Metadata.ConfState.Voters)))
	for _, v := range s.Metadata.ConfState.Voters {
		body = appendU64(body, v)
	}
	body = appendU32(body, uint32(len(s.Metadata.ConfState.Learners)))
	for _, v := range s.Metadata.ConfState.Learners {
		body = appendU64(body, v)
	}
	body = appendU32(body, uint32(len(s.Data)))
	body = append(body, s.Data...)

	out := make([]byte, 0, 8+len(body))
	out = append(out, snapMagic...)
	out = appendU32(out, crc32.ChecksumIEEE(body))
	return append(out, body...)
}

func decodeSnapshot(data []byte) (raft.Snapshot, error) {
	if len(data) < 8 || string(data[0:4]) != snapMagic {
		return raft.Snapshot{}, errors.New("walstore: bad snapshot magic")
	}
	if crc32.ChecksumIEEE(data[8:]) != binary.LittleEndian.Uint32(data[4:8]) {
		return raft.Snapshot{}, errors.New("walstore: snapshot crc mismatch")
	}
	c := cursor{b: data[8:]}
	var s raft.Snapshot
	var ok bool
	if s.Metadata.Index, ok = c.u64(); !ok {
		return raft.Snapshot{}, errShortSnap
	}
	if s.Metadata.Term, ok = c.u64(); !ok {
		return raft.Snapshot{}, errShortSnap
	}
	voters, err := c.u64Slice()
	if err != nil {
		return raft.Snapshot{}, err
	}
	learners, err := c.u64Slice()
	if err != nil {
		return raft.Snapshot{}, err
	}
	s.Metadata.ConfState = raft.ConfState{Voters: voters, Learners: learners}
	dl, ok := c.u32()
	if !ok {
		return raft.Snapshot{}, errShortSnap
	}
	d, ok := c.bytes(int(dl))
	if !ok {
		return raft.Snapshot{}, errShortSnap
	}
	if dl > 0 {
		s.Data = append([]byte(nil), d...)
	}
	return s, nil
}

var errShortSnap = errors.New("walstore: short snapshot")

func appendU64(b []byte, v uint64) []byte {
	var x [8]byte
	binary.LittleEndian.PutUint64(x[:], v)
	return append(b, x[:]...)
}

func appendU32(b []byte, v uint32) []byte {
	var x [4]byte
	binary.LittleEndian.PutUint32(x[:], v)
	return append(b, x[:]...)
}

// cursor reads fixed-width fields from a byte slice, reporting truncation.
type cursor struct {
	b   []byte
	off int
}

func (c *cursor) u64() (uint64, bool) {
	if c.off+8 > len(c.b) {
		return 0, false
	}
	v := binary.LittleEndian.Uint64(c.b[c.off:])
	c.off += 8
	return v, true
}

func (c *cursor) u32() (uint32, bool) {
	if c.off+4 > len(c.b) {
		return 0, false
	}
	v := binary.LittleEndian.Uint32(c.b[c.off:])
	c.off += 4
	return v, true
}

func (c *cursor) bytes(n int) ([]byte, bool) {
	if n < 0 || c.off+n > len(c.b) {
		return nil, false
	}
	v := c.b[c.off : c.off+n]
	c.off += n
	return v, true
}

func (c *cursor) u64Slice() ([]uint64, error) {
	n, ok := c.u32()
	if !ok {
		return nil, errShortSnap
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]uint64, n)
	for i := range out {
		v, ok := c.u64()
		if !ok {
			return nil, errShortSnap
		}
		out[i] = v
	}
	return out, nil
}

func limitSize(ents []raft.Entry, maxSize uint64) []raft.Entry {
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

func entSize(e raft.Entry) uint64 {
	return uint64(len(e.Data)) + 24
}
