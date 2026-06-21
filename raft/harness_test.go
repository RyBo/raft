package raft

import (
	"math/rand"
	"sort"
	"testing"
)

// testNode bundles a RawNode with its backing storage, mimicking a driver.
type testNode struct {
	node    *RawNode
	storage *MemoryStorage
	// kv is a trivial state machine: applied normal entries appended here.
	applied []Entry
	reads   []ReadState
}

// network is a deterministic, single-threaded test harness. It drives every
// node's Ready/Advance cycle and routes messages, honoring partitions.
type network struct {
	t      *testing.T
	nodes  map[uint64]*testNode
	outbox []Message

	// partition state: nodes in different groups cannot talk. nil => fully
	// connected.
	groups map[uint64]int
}

func newNetwork(t *testing.T, ids []uint64, preVote, checkQuorum bool) *network {
	n := &network{t: t, nodes: map[uint64]*testNode{}, groups: map[uint64]int{}}
	for _, id := range ids {
		st := NewMemoryStorage()
		cfg := &Config{
			ID:            id,
			ElectionTick:  10,
			HeartbeatTick: 1,
			Storage:       st,
			PreVote:       preVote,
			CheckQuorum:   checkQuorum,
			// Deterministic but distinct per node so randomized timeouts differ.
			Rand: rand.New(rand.NewSource(int64(id) * 1000)),
		}
		rn, err := NewRawNode(cfg, ids)
		if err != nil {
			t.Fatalf("NewRawNode(%d): %v", id, err)
		}
		n.nodes[id] = &testNode{node: rn, storage: st}
	}
	return n
}

func (n *network) sortedIDs() []uint64 {
	ids := make([]uint64, 0, len(n.nodes))
	for id := range n.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (n *network) connected(a, b uint64) bool {
	ga, oka := n.groups[a]
	gb, okb := n.groups[b]
	if !oka || !okb {
		return true // unpartitioned nodes talk to everyone
	}
	return ga == gb
}

// partition splits nodes into communication groups. Each argument is a group.
func (n *network) partition(groups ...[]uint64) {
	n.groups = map[uint64]int{}
	for gi, g := range groups {
		for _, id := range g {
			n.groups[id] = gi
		}
	}
}

func (n *network) heal() { n.groups = map[uint64]int{} }

// drive runs one Ready/Advance cycle for a node, acting as its driver.
func (n *network) drive(id uint64) {
	tn := n.nodes[id]
	if !tn.node.HasReady() {
		return
	}
	rd := tn.node.Ready()
	if !rd.HardState.isEmpty() {
		tn.storage.SetHardState(rd.HardState)
	}
	if !rd.Snapshot.isEmpty() {
		_ = tn.storage.ApplySnapshot(rd.Snapshot)
	}
	if len(rd.Entries) > 0 {
		_ = tn.storage.Append(rd.Entries)
	}
	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case EntryNormal:
			if len(e.Data) > 0 {
				tn.applied = append(tn.applied, e)
			}
		case EntryConfChange:
			cc := DecodeConfChange(e.Data)
			tn.node.ApplyConfChange(cc)
		}
	}
	tn.reads = append(tn.reads, rd.ReadStates...)
	for _, m := range rd.Messages {
		n.outbox = append(n.outbox, m)
	}
	tn.node.Advance(rd)
}

// run advances the cluster until it reaches a quiescent state (no pending Ready
// and no deliverable messages), or panics on runaway loops.
func (n *network) run() {
	const maxIter = 100000
	for iter := 0; iter < maxIter; iter++ {
		progress := false
		for _, id := range n.sortedIDs() {
			if n.nodes[id].node.HasReady() {
				n.drive(id)
				progress = true
			}
		}
		msgs := n.outbox
		n.outbox = nil
		for _, m := range msgs {
			to := n.nodes[m.To]
			if to == nil || !n.connected(m.From, m.To) {
				continue // dropped
			}
			_ = to.node.Step(m)
			progress = true
		}
		if !progress {
			return
		}
	}
	n.t.Fatalf("network did not quiesce after %d iterations", maxIter)
}

// tickAll advances every node one tick and delivers messages, repeating `times`
// rounds. Interleaving delivery with ticks lets randomized election timeouts
// stagger so split votes resolve, just like wall-clock time would.
func (n *network) tickAll(times int) {
	for i := 0; i < times; i++ {
		for _, id := range n.sortedIDs() {
			n.nodes[id].node.Tick()
		}
		n.run()
	}
}

// campaign forces a node to start an election, then runs to quiescence.
func (n *network) campaign(id uint64) {
	_ = n.nodes[id].node.Campaign()
	n.run()
}

func (n *network) propose(id uint64, data []byte) {
	_ = n.nodes[id].node.Propose(data)
	n.run()
}

func (n *network) status(id uint64) Status { return n.nodes[id].node.Status() }

// leaders returns the IDs currently believing they are leader.
func (n *network) leaders() []uint64 {
	var ls []uint64
	for _, id := range n.sortedIDs() {
		if n.status(id).State == StateLeader {
			ls = append(ls, id)
		}
	}
	return ls
}

func (n *network) requireSingleLeader(t *testing.T) uint64 {
	t.Helper()
	ls := n.leaders()
	if len(ls) != 1 {
		t.Fatalf("expected exactly one leader, got %v", ls)
	}
	return ls[0]
}
