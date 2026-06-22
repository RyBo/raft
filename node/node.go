package node

import (
	"context"
	"encoding/binary"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/rybo/raft/raft"
)

// defaultCompactThreshold matches the simulation: snapshot and trim the log once
// this many entries have been applied past the last snapshot.
const defaultCompactThreshold = 24

// ErrNotLeader is returned by Propose when this node is not the leader. Callers
// should consult Status for the current leader and retry there.
var ErrNotLeader = errors.New("node: not leader")

// ErrStopped is returned by client methods after the node has stopped.
var ErrStopped = errors.New("node: stopped")

// Config configures a Node.
type Config struct {
	ID            uint64
	Peers         []uint64 // voter set; always passed to NewRawNode (safe on restart)
	ElectionTick  int
	HeartbeatTick int
	TickInterval  time.Duration
	PreVote       bool
	CheckQuorum   bool
	Rand          *rand.Rand

	Storage Storage
	FSM     FSM
	Sender  Sender
	// Inbound carries messages received from peers (shared with transport.Server).
	Inbound chan raft.Message

	CompactThreshold uint64
	Logger           *log.Logger
	// OnConfChange, if set, is called on the run goroutine after each committed
	// membership change is applied (used to update the transport peer registry).
	OnConfChange func(raft.ConfChange)
}

// Node drives one raft.RawNode. All RawNode access happens on the run goroutine
// (started by Run); client methods communicate with it over channels.
type Node struct {
	id               uint64
	rn               *raft.RawNode
	storage          Storage
	fsm              FSM
	sender           Sender
	inbound          chan raft.Message
	tickInterval     time.Duration
	compactThreshold uint64
	logger           *log.Logger
	onConfChange     func(raft.ConfChange)

	commands chan func()
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}

	// Run-goroutine-owned state.
	applied        uint64
	isLeader       bool
	readSeq        uint64
	pendingWrites  map[uint64]chan error
	pendingReads   map[string]*pendingRead
	confirmedReads []*confirmedRead
}

type pendingRead struct {
	read func()
	resp chan error
}

type confirmedRead struct {
	index uint64
	read  func()
	resp  chan error
}

// New creates a Node. If the storage holds a snapshot (a restart), the FSM is
// restored from it; the run loop then replays any committed-but-unapplied entries
// to bring the FSM fully current.
func New(cfg Config) (*Node, error) {
	if cfg.CompactThreshold == 0 {
		cfg.CompactThreshold = defaultCompactThreshold
	}
	if cfg.TickInterval == 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}
	if cfg.Inbound == nil {
		cfg.Inbound = make(chan raft.Message, 1024)
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}

	rc := &raft.Config{
		ID:            cfg.ID,
		ElectionTick:  cfg.ElectionTick,
		HeartbeatTick: cfg.HeartbeatTick,
		Storage:       cfg.Storage,
		PreVote:       cfg.PreVote,
		CheckQuorum:   cfg.CheckQuorum,
		Rand:          cfg.Rand,
	}
	rn, err := raft.NewRawNode(rc, cfg.Peers)
	if err != nil {
		return nil, err
	}

	n := &Node{
		id:               cfg.ID,
		rn:               rn,
		storage:          cfg.Storage,
		fsm:              cfg.FSM,
		sender:           cfg.Sender,
		inbound:          cfg.Inbound,
		tickInterval:     cfg.TickInterval,
		compactThreshold: cfg.CompactThreshold,
		logger:           cfg.Logger,
		onConfChange:     cfg.OnConfChange,
		commands:         make(chan func()),
		stop:             make(chan struct{}),
		done:             make(chan struct{}),
		pendingWrites:    make(map[uint64]chan error),
		pendingReads:     make(map[string]*pendingRead),
	}

	// Restore the FSM from a snapshot on restart.
	if snap, err := cfg.Storage.Snapshot(); err == nil && snap.Metadata.Index != 0 {
		if err := cfg.FSM.Restore(snap.Data); err != nil {
			return nil, err
		}
		n.applied = snap.Metadata.Index
	} else if fi, err := cfg.Storage.FirstIndex(); err == nil {
		n.applied = fi - 1
	}
	return n, nil
}

// Inbound returns the channel peers' messages are delivered on. Wire it to a
// transport.Server so received messages reach the run loop.
func (n *Node) Inbound() chan raft.Message { return n.inbound }

// Run drives the node until Stop is called. It must run in its own goroutine and
// is the only goroutine that touches the RawNode.
func (n *Node) Run() {
	defer close(n.done)
	ticker := time.NewTicker(n.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stop:
			n.failAll(ErrStopped)
			return
		case <-ticker.C:
			n.rn.Tick()
		case m := <-n.inbound:
			_ = n.rn.Step(m)
		case task := <-n.commands:
			task()
		}
		n.drainReady()
	}
}

// Stop signals the run loop to exit and waits for it.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stop) })
	<-n.done
}

// drainReady processes every pending Ready batch, following the same ordering as
// the simulation's drivePeer: persist, send, apply, serve reads, advance, compact.
func (n *Node) drainReady() {
	for n.rn.HasReady() {
		rd := n.rn.Ready()

		if rd.SoftState != nil {
			n.isLeader = rd.SoftState.RaftState == raft.StateLeader
			if !n.isLeader {
				n.failPendingWrites(ErrNotLeader)
			}
		}

		// 1. persist before sending.
		if rd.HardState != (raft.HardState{}) {
			n.storage.SetHardState(rd.HardState)
		}
		if rd.Snapshot.Metadata.Index != 0 {
			if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
				n.logger.Printf("node %d: ApplySnapshot: %v", n.id, err)
			}
			if err := n.fsm.Restore(rd.Snapshot.Data); err != nil {
				n.logger.Printf("node %d: FSM Restore: %v", n.id, err)
			}
			n.applied = rd.Snapshot.Metadata.Index
		}
		if len(rd.Entries) > 0 {
			if err := n.storage.Append(rd.Entries); err != nil {
				n.logger.Printf("node %d: Append: %v", n.id, err)
			}
		}

		// 2. send.
		if len(rd.Messages) > 0 {
			n.sender.Send(rd.Messages)
		}

		// 3. apply committed entries.
		for _, e := range rd.CommittedEntries {
			switch e.Type {
			case raft.EntryNormal:
				if len(e.Data) > 0 {
					n.fsm.Apply(e)
				}
				if ch, ok := n.pendingWrites[e.Index]; ok {
					ch <- nil
					delete(n.pendingWrites, e.Index)
				}
			case raft.EntryConfChange:
				cc := raft.DecodeConfChange(e.Data)
				n.rn.ApplyConfChange(cc)
				if n.onConfChange != nil {
					n.onConfChange(cc)
				}
			}
			if e.Index > n.applied {
				n.applied = e.Index
			}
		}

		// 4. record confirmed read requests for serving once applied.
		for _, rs := range rd.ReadStates {
			if pr, ok := n.pendingReads[string(rs.Ctx)]; ok {
				delete(n.pendingReads, string(rs.Ctx))
				n.confirmedReads = append(n.confirmedReads, &confirmedRead{index: rs.Index, read: pr.read, resp: pr.resp})
			}
		}

		// 5. acknowledge.
		n.rn.Advance(rd)

		// 6. serve reads whose index has now been applied.
		n.serveReads()

		// 7. snapshot and trim the log if it has grown enough.
		n.maybeCompact()
	}
}

func (n *Node) serveReads() {
	if len(n.confirmedReads) == 0 {
		return
	}
	kept := n.confirmedReads[:0]
	for _, c := range n.confirmedReads {
		if n.applied >= c.index {
			c.read()
			c.resp <- nil
		} else {
			kept = append(kept, c)
		}
	}
	n.confirmedReads = kept
}

func (n *Node) maybeCompact() {
	st := n.rn.Status()
	fi, err := n.storage.FirstIndex()
	if err != nil {
		return
	}
	snapIdx := fi - 1
	if st.Applied <= snapIdx+n.compactThreshold {
		return
	}
	data, err := n.fsm.Snapshot()
	if err != nil {
		return
	}
	cs := n.rn.ConfState()
	if _, err := n.storage.CreateSnapshot(st.Applied, &cs, data); err != nil {
		return
	}
	if err := n.storage.Compact(st.Applied); err != nil {
		n.logger.Printf("node %d: Compact: %v", n.id, err)
	}
}

// --- client API (called from other goroutines) ---

// Propose replicates a normal entry and returns once it is committed and applied.
// It returns ErrNotLeader if this node is not the leader.
func (n *Node) Propose(ctx context.Context, data []byte) error {
	resp := make(chan error, 1)
	if err := n.submit(ctx, func() { n.doPropose(data, resp) }); err != nil {
		return err
	}
	return n.wait(ctx, resp)
}

func (n *Node) doPropose(data []byte, resp chan error) {
	if n.rn.Status().State != raft.StateLeader {
		resp <- ErrNotLeader
		return
	}
	if err := n.rn.Propose(data); err != nil {
		resp <- err
		return
	}
	idx := n.rn.Status().LastIndex
	n.pendingWrites[idx] = resp
}

// ProposeConfChange replicates a membership change. It returns once the proposal
// is accepted by the leader (not when committed). Returns ErrNotLeader otherwise.
func (n *Node) ProposeConfChange(ctx context.Context, cc raft.ConfChange) error {
	resp := make(chan error, 1)
	if err := n.submit(ctx, func() {
		if n.rn.Status().State != raft.StateLeader {
			resp <- ErrNotLeader
			return
		}
		resp <- n.rn.ProposeConfChange(cc)
	}); err != nil {
		return err
	}
	return n.wait(ctx, resp)
}

// LinearizableRead confirms a linearizable read point, waits until the state
// machine has applied it, then runs read on the node's own goroutine (so it can
// safely touch the FSM) before returning.
func (n *Node) LinearizableRead(ctx context.Context, read func()) error {
	resp := make(chan error, 1)
	if err := n.submit(ctx, func() { n.doReadIndex(read, resp) }); err != nil {
		return err
	}
	return n.wait(ctx, resp)
}

func (n *Node) doReadIndex(read func(), resp chan error) {
	rctx := n.nextReadCtx()
	if err := n.rn.ReadIndex(rctx); err != nil {
		resp <- err
		return
	}
	n.pendingReads[string(rctx)] = &pendingRead{read: read, resp: resp}
}

// Do runs fn on the node's run goroutine and returns when it has completed. Use it
// for stale reads of FSM state, which would otherwise race with Apply.
func (n *Node) Do(ctx context.Context, fn func()) error {
	resp := make(chan error, 1)
	if err := n.submit(ctx, func() {
		fn()
		resp <- nil
	}); err != nil {
		return err
	}
	return n.wait(ctx, resp)
}

// Status returns a snapshot of the node's Raft state.
func (n *Node) Status(ctx context.Context) (raft.Status, error) {
	var st raft.Status
	err := n.Do(ctx, func() { st = n.rn.Status() })
	return st, err
}

func (n *Node) nextReadCtx() []byte {
	n.readSeq++
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], n.id)
	binary.BigEndian.PutUint64(b[8:16], n.readSeq)
	return b
}

// submit enqueues a task for the run goroutine.
func (n *Node) submit(ctx context.Context, task func()) error {
	select {
	case n.commands <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

// wait blocks for a task's response.
func (n *Node) wait(ctx context.Context, resp chan error) error {
	select {
	case err := <-resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

func (n *Node) failPendingWrites(err error) {
	for idx, ch := range n.pendingWrites {
		ch <- err
		delete(n.pendingWrites, idx)
	}
}

func (n *Node) failAll(err error) {
	n.failPendingWrites(err)
	for ctx, pr := range n.pendingReads {
		pr.resp <- err
		delete(n.pendingReads, ctx)
	}
	for _, c := range n.confirmedReads {
		c.resp <- err
	}
	n.confirmedReads = nil
}
