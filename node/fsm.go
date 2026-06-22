// Package node is a single-node Raft driver: it owns one raft.RawNode on a single
// goroutine and connects it to durable storage, a network transport and a state
// machine. It runs the same persist→send→apply→advance loop the in-process
// simulation uses (sim.Cluster.drivePeer), but for one node per OS process.
//
// The driver is generic over the state machine (FSM) and the storage backend, so
// the existing kvstore can be reused unchanged and a different data model can be
// dropped in later.
package node

import "github.com/rybo/raft/raft"

// FSM is the application state machine the Raft log drives. It matches the method
// set of *kvstore.KV, so that store satisfies this interface with no changes.
//
// Apply, Snapshot and Restore are only ever called from the node's run goroutine,
// so an FSM need not be safe for concurrent use. Reads of FSM state must likewise
// be marshalled onto that goroutine — see Node.Do and Node.LinearizableRead.
type FSM interface {
	// Apply applies one committed normal entry and returns an optional result.
	Apply(entry raft.Entry) (result []byte, applied bool)
	// Snapshot serializes the entire state for log compaction.
	Snapshot() ([]byte, error)
	// Restore replaces the state from a snapshot produced by Snapshot.
	Restore([]byte) error
}

// Storage is the durable store the driver writes to. It is raft.Storage plus the
// driver-side writer methods. Both *walstore.WAL and *raft.MemoryStorage satisfy
// it, so the same driver runs against real disk or in-memory storage (handy for
// tests).
type Storage interface {
	raft.Storage
	SetHardState(raft.HardState)
	Append([]raft.Entry) error
	ApplySnapshot(raft.Snapshot) error
	CreateSnapshot(i uint64, cs *raft.ConfState, data []byte) (raft.Snapshot, error)
	Compact(compactIndex uint64) error
}

// Sender delivers outbound messages to peers. *transport.Peers satisfies it.
// Delivery is best-effort: Send must not block the caller.
type Sender interface {
	Send([]raft.Message)
}
