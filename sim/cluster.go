// Package sim runs a whole Raft cluster inside one process over a controllable
// in-memory network, and streams its state for visualization. Everything runs
// on a single goroutine driven by a logical clock, which keeps the simulation
// deterministic: a given seed and command sequence always produce the same run.
package sim

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"math/rand"

	"github.com/rybo/raft/kvstore"
	"github.com/rybo/raft/metrics"
	"github.com/rybo/raft/raft"
)

const (
	maxNodes         = 9
	defaultMsPerTick = 220
)

type pendingRead struct {
	reqID  string
	key    string
	nodeID uint64
}

// Cluster owns all peers, the network and the clock. Its public methods that
// mutate state are funnelled through a command channel so the simulation stays
// single-threaded.
type Cluster struct {
	peers map[uint64]*peer
	order []uint64

	net *netConfig
	bus *bus
	rng *rand.Rand

	seed   int64
	tick   uint64
	seq    uint64
	nextID uint64

	electionTick  int
	heartbeatTick int
	preVote       bool
	checkQuorum   bool

	running   bool
	msPerTick int

	cmds chan Command
	emit func([]byte)

	metrics *metrics.Metrics

	pendingReads      map[string]pendingRead
	prevRole          map[uint64]string
	prevTerm          map[uint64]uint64 // for term-change metric
	electionStartTick map[uint64]uint64 // candidate-start tick, for election-duration metric
}

// NewCluster builds a cluster of n nodes seeded for reproducibility. emit is
// called (from the cluster goroutine) with each JSON-encoded event.
func NewCluster(n int, seed int64, emit func([]byte)) *Cluster {
	if n < 1 {
		n = 1
	}
	if n > maxNodes {
		n = maxNodes
	}
	c := &Cluster{
		net:           newNetConfig(),
		bus:           newBus(),
		seed:          seed,
		electionTick:  10,
		heartbeatTick: 1,
		preVote:       true,
		checkQuorum:   true,
		running:       true,
		msPerTick:     defaultMsPerTick,
		cmds:          make(chan Command, 64),
		emit:          emit,
		pendingReads:  map[string]pendingRead{},
		prevRole:      map[uint64]string{},
	}
	c.build(n)
	return c
}

// build creates n fresh peers with ids 1..n.
func (c *Cluster) build(n int) {
	c.peers = map[uint64]*peer{}
	c.rng = rand.New(rand.NewSource(c.seed))
	c.bus.clear()
	c.net = newNetConfig()
	c.tick = 0
	c.seq = 0
	c.pendingReads = map[string]pendingRead{}
	c.prevRole = map[uint64]string{}
	c.prevTerm = map[uint64]uint64{}
	c.electionStartTick = map[uint64]uint64{}

	ids := make([]uint64, 0, n)
	for i := 1; i <= n; i++ {
		ids = append(ids, uint64(i))
	}
	for _, id := range ids {
		c.peers[id] = newPeer(peerConfig{
			id:            id,
			peers:         ids,
			electionTick:  c.electionTick,
			heartbeatTick: c.heartbeatTick,
			preVote:       c.preVote,
			checkQuorum:   c.checkQuorum,
			seed:          c.seed + int64(id)*7919,
		})
	}
	c.nextID = uint64(n) + 1
	c.recomputeOrder()
}

func (c *Cluster) recomputeOrder() {
	c.order = c.order[:0]
	for id := range c.peers {
		c.order = append(c.order, id)
	}
	sort.Slice(c.order, func(i, j int) bool { return c.order[i] < c.order[j] })
}

// SetEmit sets the event sink. Call before Run.
func (c *Cluster) SetEmit(emit func([]byte)) { c.emit = emit }

// SetMetrics attaches a Prometheus metrics recorder. Call before Run.
func (c *Cluster) SetMetrics(m *metrics.Metrics) { c.metrics = m }

// Submit queues a command from another goroutine (the WebSocket hub).
func (c *Cluster) Submit(cmd Command) {
	select {
	case c.cmds <- cmd:
	default:
		// Drop if the buffer is full; the UI can retry.
	}
}

// Run drives the cluster until stop is closed. It is the single goroutine that
// touches cluster state.
func (c *Cluster) Run(stop <-chan struct{}) {
	c.emitState() // initial snapshot
	for {
		var tickCh <-chan time.Time
		if c.running {
			tickCh = time.After(time.Duration(c.msPerTick) * time.Millisecond)
		}
		select {
		case <-stop:
			return
		case cmd := <-c.cmds:
			c.applyCommand(cmd)
		case <-tickCh:
			c.stepTick()
		}
	}
}

// stepTick advances logical time by one tick.
func (c *Cluster) stepTick() {
	c.tick++

	// 1. advance every live node's clock.
	for _, id := range c.order {
		if p := c.peers[id]; !p.crashed {
			p.node.Tick()
		}
	}
	// 2. deliver messages due this tick (before draining, so responses can be
	//    produced in the same tick's drive step).
	for _, m := range c.bus.due(c.tick) {
		dst := c.peers[m.To]
		if dst == nil || dst.crashed {
			continue
		}
		if !c.net.connected(m.From, m.To) {
			continue // partition opened while the message was in flight
		}
		_ = dst.node.Step(m)
	}
	// 3. drain each node's Ready batch (persist, send, apply).
	for _, id := range c.order {
		if p := c.peers[id]; !p.crashed {
			c.drivePeer(p)
		}
	}
	// 4. remove nodes that have been voted out of the configuration.
	c.sweepRemoved()
	// 5. publish the new state.
	c.emitState()
}

// sweepRemoved deletes peers that have applied their own removal.
func (c *Cluster) sweepRemoved() {
	var removed []uint64
	for id, p := range c.peers {
		if p.node.Removed() {
			removed = append(removed, id)
		}
	}
	if len(removed) == 0 {
		return
	}
	for _, id := range removed {
		delete(c.peers, id)
		delete(c.prevRole, id)
		c.bus.dropTo(id)
		c.logEvent("node_removed", id, 0, fmt.Sprintf("node %d left the cluster", id))
	}
	c.recomputeOrder()
}

// drivePeer processes one node's Ready batch, acting as its driver.
func (c *Cluster) drivePeer(p *peer) {
	if !p.node.HasReady() {
		return
	}
	rd := p.node.Ready()

	if rd.SoftState != nil {
		c.onRoleChange(p.id, rd.SoftState.RaftState.String(), p.node.Status().Term)
	}

	// 1. persist before sending.
	if rd.HardState != (raft.HardState{}) {
		p.storage.SetHardState(rd.HardState)
	}
	if rd.Snapshot.Metadata.Index != 0 {
		_ = p.storage.ApplySnapshot(rd.Snapshot)
		_ = p.kv.Restore(rd.Snapshot.Data)
		c.logEvent("snapshot_installed", p.id, 0,
			fmt.Sprintf("node %d installed snapshot at index %d (caught up)", p.id, rd.Snapshot.Metadata.Index))
		if c.metrics != nil {
			c.metrics.IncSnapshotInstalled(p.id)
		}
	}
	if len(rd.Entries) > 0 {
		_ = p.storage.Append(rd.Entries)
	}
	// 2. send.
	for _, m := range rd.Messages {
		c.scheduleSend(m)
	}
	// 3. apply committed entries.
	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case raft.EntryNormal:
			p.kv.Apply(e)
			if c.metrics != nil && len(e.Data) > 0 {
				c.metrics.IncCommitted()
			}
		case raft.EntryConfChange:
			cc := raft.DecodeConfChange(e.Data)
			p.node.ApplyConfChange(cc)
			c.onConfApplied(p, cc)
			if c.metrics != nil {
				c.metrics.IncConfChange(confChangeName(cc.Type))
			}
		}
	}
	// 4. serve confirmed reads.
	for _, rs := range rd.ReadStates {
		c.serveRead(p, rs)
	}
	p.node.Advance(rd)
	c.maybeCompact(p)
}

// compactThreshold is how many applied entries accumulate before a node snapshots
// its state machine and trims its log. Kept small so the snapshot/InstallSnapshot
// path is easy to trigger and watch in the demo.
const compactThreshold = 24

// maybeCompact snapshots and trims a node's log once it has applied enough
// entries. A follower that later falls behind the trimmed point is caught up by
// the leader via InstallSnapshot rather than a log replay.
func (c *Cluster) maybeCompact(p *peer) {
	st := p.node.Status()
	fi, err := p.storage.FirstIndex()
	if err != nil {
		return
	}
	snapIdx := fi - 1
	if st.Applied <= snapIdx+compactThreshold {
		return
	}
	data, err := p.kv.Snapshot()
	if err != nil {
		return
	}
	cs := p.node.ConfState()
	if _, err := p.storage.CreateSnapshot(st.Applied, &cs, data); err != nil {
		return
	}
	_ = p.storage.Compact(st.Applied)
	c.logEvent("snapshot_created", p.id, 0,
		fmt.Sprintf("node %d snapshotted & compacted log up to index %d", p.id, st.Applied))
	if c.metrics != nil {
		c.metrics.IncSnapshotCreated(p.id)
	}
}

// confChangeName gives a stable label for a configuration-change type.
func confChangeName(t raft.ConfChangeType) string {
	switch t {
	case raft.ConfChangeAddNode:
		return "add_voter"
	case raft.ConfChangeAddLearnerNode:
		return "add_learner"
	case raft.ConfChangeRemoveNode:
		return "remove"
	case raft.ConfChangePromoteLearner:
		return "promote"
	default:
		return "unknown"
	}
}

// scheduleSend applies the network model and enqueues the message for delivery.
func (c *Cluster) scheduleSend(m raft.Message) {
	c.seq++
	id := fmt.Sprintf("m%d", c.seq)
	from, to := m.From, m.To

	drop := false
	if !c.net.connected(from, to) {
		drop = true
	} else if dr := c.net.dropFor(from, to); dr > 0 && c.rng.Float64() < dr {
		drop = true
	}

	lat := c.net.latencyFor(from, to)
	if c.net.jitter > 0 {
		lat += c.rng.Intn(c.net.jitter + 1)
	}
	if lat < 1 {
		lat = 1
	}
	deliver := c.tick + uint64(lat)

	fate := "delivered"
	if drop {
		fate = "dropped"
	}
	if c.metrics != nil {
		c.metrics.IncMessageSent(from, m.Type.String())
		if drop {
			c.metrics.IncMessageDropped(m.Type.String())
		}
	}
	c.send(MessageEvent{
		Type: "message", ID: id, From: from, To: to,
		MsgType: m.Type.String(), SentTick: c.tick, DeliverTick: deliver,
		Fate: fate, Entries: len(m.Entries),
	})
	if drop {
		return
	}
	c.bus.schedule(deliver, c.seq, m)

	if c.net.dupRate > 0 && c.rng.Float64() < c.net.dupRate {
		c.seq++
		c.bus.schedule(deliver+1, c.seq, m)
		c.send(MessageEvent{
			Type: "message", ID: fmt.Sprintf("m%d", c.seq), From: from, To: to,
			MsgType: m.Type.String(), SentTick: c.tick, DeliverTick: deliver + 1,
			Fate: "duplicated", Entries: len(m.Entries),
		})
	}
}

func (c *Cluster) serveRead(p *peer, rs raft.ReadState) {
	pr, ok := c.pendingReads[string(rs.Ctx)]
	if !ok {
		return
	}
	delete(c.pendingReads, string(rs.Ctx))
	val, found := p.kv.Get(pr.key)
	c.send(KVResultEvent{
		Type: "kvResult", ReqID: pr.reqID, OK: true, Op: "get",
		Key: pr.key, Value: val, Found: found, ServedBy: p.id, Linearizable: true,
		Note: "linearizable read confirmed via ReadIndex",
	})
}

// leaderID returns the current leader, or 0 if none is known.
func (c *Cluster) leaderID() uint64 {
	for _, id := range c.order {
		p := c.peers[id]
		if !p.crashed && p.node.Status().State == raft.StateLeader {
			return id
		}
	}
	return 0
}

func (c *Cluster) logEvent(kind string, node, term uint64, text string) {
	c.send(LogEvent{Type: "event", Tick: c.tick, Kind: kind, Node: node, Term: term, Text: text})
}

func (c *Cluster) onRoleChange(id uint64, role string, term uint64) {
	if c.prevRole[id] == role {
		return
	}
	c.prevRole[id] = role
	switch role {
	case "leader":
		c.logEvent("leader_elected", id, term, fmt.Sprintf("node %d became leader for term %d", id, term))
		if c.metrics != nil {
			c.metrics.IncLeaderElected(id)
			if start, ok := c.electionStartTick[id]; ok {
				c.metrics.ObserveElectionDuration(float64(c.tick - start))
				delete(c.electionStartTick, id)
			}
		}
	case "candidate":
		c.logEvent("election_started", id, term, fmt.Sprintf("node %d started an election for term %d", id, term))
		if _, ok := c.electionStartTick[id]; !ok {
			c.electionStartTick[id] = c.tick // no preceding pre-vote
		}
		if c.metrics != nil {
			c.metrics.IncElection(id)
		}
	case "precandidate":
		c.logEvent("prevote_started", id, term, fmt.Sprintf("node %d started pre-vote", id))
		// Pre-vote precedes the candidate phase; mark the election start here so
		// the duration histogram covers the whole campaign.
		c.electionStartTick[id] = c.tick
		if c.metrics != nil {
			c.metrics.IncPreVote(id)
		}
	}
}

func (c *Cluster) onConfApplied(p *peer, cc raft.ConfChange) {
	c.recomputeOrder()
	c.logEvent("conf_change_applied", p.id, 0,
		fmt.Sprintf("node %d applied conf change %v on node %d", p.id, cc.Type, cc.NodeID))
}

// send marshals and emits an event.
func (c *Cluster) send(ev any) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	if c.emit != nil {
		c.emit(b)
	}
}

// emitState publishes the full cluster snapshot.
func (c *Cluster) emitState() {
	nodes := make([]NodeView, 0, len(c.order))
	leader := c.leaderID()
	var cfg ConfigView
	if leader != 0 {
		cfg = confView(c.peers[leader].node.ConfState())
	} else if len(c.order) > 0 {
		cfg = confView(c.peers[c.order[0]].node.ConfState())
	}
	learnerSet := map[uint64]bool{}
	for _, id := range cfg.Learners {
		learnerSet[id] = true
	}

	gauges := make([]metrics.NodeGauge, 0, len(c.order))
	for _, id := range c.order {
		p := c.peers[id]
		st := p.node.Status()
		nv := NodeView{
			ID:        id,
			Role:      st.State.String(),
			Term:      st.Term,
			Vote:      st.Vote,
			Lead:      st.Lead,
			Commit:    st.Commit,
			Applied:   st.Applied,
			LastIndex: st.LastIndex,
			Crashed:   p.crashed,
			IsLearner: learnerSet[id],
			KV:        p.kv.Pairs(),
		}
		for _, e := range st.Log {
			nv.Log = append(nv.Log, summarizeEntry(e, st.Commit))
		}
		if len(st.Progress) > 0 {
			nv.Progress = map[uint64]ProgressView{}
			for pid, pr := range st.Progress {
				nv.Progress[pid] = ProgressView{Match: pr.Match, Next: pr.Next, State: pr.State.String()}
			}
		}
		nodes = append(nodes, nv)

		if c.metrics != nil {
			if prev, ok := c.prevTerm[id]; ok && st.Term > prev {
				c.metrics.IncTermChange(id)
			}
			c.prevTerm[id] = st.Term
			gauges = append(gauges, metrics.NodeGauge{
				ID: id, Term: st.Term, Commit: st.Commit, Applied: st.Applied,
				LastIndex: st.LastIndex, LogLen: uint64(len(st.Log)),
				Crashed: p.crashed, IsLeader: id == leader,
			})
		}
	}

	c.send(StateEvent{
		Type:   "state",
		Tick:   c.tick,
		Seed:   c.seed,
		Clock:  ClockView{Running: c.running, MsPerTick: c.msPerTick},
		Nodes:  nodes,
		Config: cfg,
		Net:    c.net.view(c.order),
	})

	if c.metrics != nil {
		kvKeys := 0
		if leader != 0 {
			kvKeys = len(c.peers[leader].kv.Pairs())
		}
		c.metrics.SetSnapshot(metrics.Snapshot{
			Tick:        c.tick,
			Leader:      leader,
			Nodes:       len(c.order),
			Inflight:    c.bus.len(),
			Partitioned: len(c.net.group) > 0,
			KVKeys:      kvKeys,
			Per:         gauges,
		})
	}
}

func confView(cs raft.ConfState) ConfigView {
	return ConfigView{Voters: cs.Voters, Learners: cs.Learners}
}

func summarizeEntry(e raft.Entry, commit uint64) LogEntryView {
	lv := LogEntryView{Index: e.Index, Term: e.Term, Committed: e.Index <= commit}
	switch e.Type {
	case raft.EntryConfChange:
		cc := raft.DecodeConfChange(e.Data)
		lv.Kind = "confchange"
		lv.Summary = fmt.Sprintf("conf: %v node %d", cc.Type, cc.NodeID)
	default:
		if len(e.Data) == 0 {
			lv.Kind = "noop"
			lv.Summary = "(no-op)"
			return lv
		}
		lv.Kind = "normal"
		if cmd, ok := kvstore.DecodeCommand(e.Data); ok {
			switch cmd.Op {
			case kvstore.OpPut:
				lv.Summary = fmt.Sprintf("PUT %s=%s", cmd.Key, cmd.Value)
			case kvstore.OpDelete:
				lv.Summary = fmt.Sprintf("DEL %s", cmd.Key)
			default:
				lv.Summary = string(e.Data)
			}
		} else {
			lv.Summary = string(e.Data)
		}
	}
	return lv
}
