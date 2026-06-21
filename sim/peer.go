package sim

import (
	"math/rand"

	"github.com/rybo/raft/kvstore"
	"github.com/rybo/raft/raft"
)

// peer is one simulated Raft node: its RawNode, its "disk" (storage that
// survives a crash) and its state machine.
type peer struct {
	id      uint64
	node    *raft.RawNode
	storage *raft.MemoryStorage
	kv      *kvstore.KV
	crashed bool
	learner bool
}

// peerConfig holds the parameters for (re)constructing a peer's RawNode.
type peerConfig struct {
	id            uint64
	peers         []uint64
	electionTick  int
	heartbeatTick int
	preVote       bool
	checkQuorum   bool
	seed          int64
}

func newPeer(cfg peerConfig) *peer {
	st := raft.NewMemoryStorage()
	p := &peer{id: cfg.id, storage: st, kv: kvstore.New()}
	p.node = mustRawNode(cfg, st)
	return p
}

func mustRawNode(cfg peerConfig, st *raft.MemoryStorage) *raft.RawNode {
	rc := &raft.Config{
		ID:            cfg.id,
		ElectionTick:  cfg.electionTick,
		HeartbeatTick: cfg.heartbeatTick,
		Storage:       st,
		PreVote:       cfg.preVote,
		CheckQuorum:   cfg.checkQuorum,
		Rand:          rand.New(rand.NewSource(cfg.seed)),
	}
	rn, err := raft.NewRawNode(rc, cfg.peers)
	if err != nil {
		panic(err)
	}
	return rn
}

// newPeerFromSnapshot creates a node that joins an existing cluster by starting
// from a snapshot of committed state (carrying the membership and the KV data),
// rather than replaying the whole log from scratch.
func newPeerFromSnapshot(cfg peerConfig, snap raft.Snapshot) *peer {
	st := raft.NewMemoryStorage()
	_ = st.ApplySnapshot(snap)
	p := &peer{id: cfg.id, storage: st, kv: kvstore.New(), learner: true}
	_ = p.kv.Restore(snap.Data)
	cfg.peers = nil // configuration comes from the snapshot, not bootstrap
	p.node = mustRawNode(cfg, st)
	return p
}

// restart rebuilds the RawNode from the still-intact storage, modelling a
// process restart after a crash: volatile state is gone, the disk remains.
func (p *peer) restart(cfg peerConfig) {
	cfg.id = p.id
	// peers is ignored on restart; the configuration is recovered from storage.
	p.node = mustRawNode(cfg, p.storage)
	p.crashed = false
}
