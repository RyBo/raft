// Package raft implements the Raft consensus protocol as a pure, transport-
// agnostic state machine, following the etcd/raft "Ready/Advance" design.
//
// The core (this package) imports only the standard library. It knows nothing
// about networks, goroutines, disks or timers. You feed it inputs (messages via
// Step, time via Tick, writes via Propose) and it produces a Ready batch
// describing what the surrounding "driver" must persist, send and apply. That
// separation lets the same code run over an in-memory simulated network (the
// visualizer) or a real network transport (as a library).
package raft

import "fmt"

// None is a placeholder node ID used when there is no leader / no vote.
const None uint64 = 0

// noLimit is used where an unbounded size is acceptable.
const noLimit = ^uint64(0)

// StateType identifies the role of a raft node.
type StateType uint8

const (
	StateFollower StateType = iota
	StatePreCandidate
	StateCandidate
	StateLeader
)

func (s StateType) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StatePreCandidate:
		return "precandidate"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// EntryType distinguishes normal log entries from configuration changes.
type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryConfChange
)

// Entry is a single record in the replicated log.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// MessageType enumerates the Raft RPCs plus a few local signals.
type MessageType uint8

const (
	MsgHup           MessageType = iota // local: trigger an election
	MsgBeat                             // local: leader should broadcast heartbeats
	MsgProp                             // local: propose data to append
	MsgApp                              // AppendEntries request
	MsgAppResp                          // AppendEntries response
	MsgVote                             // RequestVote request
	MsgVoteResp                         // RequestVote response
	MsgPreVote                          // PreVote request
	MsgPreVoteResp                      // PreVote response
	MsgHeartbeat                        // leader heartbeat
	MsgHeartbeatResp                    // heartbeat response
	MsgSnap                             // InstallSnapshot request
	MsgReadIndex                        // local/forwarded: linearizable read request
	MsgReadIndexResp                    // response carrying a confirmed read index
)

var msgTypeNames = map[MessageType]string{
	MsgHup: "MsgHup", MsgBeat: "MsgBeat", MsgProp: "MsgProp",
	MsgApp: "MsgApp", MsgAppResp: "MsgAppResp",
	MsgVote: "MsgVote", MsgVoteResp: "MsgVoteResp",
	MsgPreVote: "MsgPreVote", MsgPreVoteResp: "MsgPreVoteResp",
	MsgHeartbeat: "MsgHeartbeat", MsgHeartbeatResp: "MsgHeartbeatResp",
	MsgSnap: "MsgSnap", MsgReadIndex: "MsgReadIndex", MsgReadIndexResp: "MsgReadIndexResp",
}

func (t MessageType) String() string {
	if s, ok := msgTypeNames[t]; ok {
		return s
	}
	return fmt.Sprintf("MsgType(%d)", uint8(t))
}

// Message is the single envelope for every Raft RPC. Not all fields are used by
// every message type; the comments give the dominant meaning.
type Message struct {
	Type       MessageType
	To         uint64
	From       uint64
	Term       uint64  // sender's term (0 for local messages)
	LogTerm    uint64  // MsgApp: term of PrevLogIndex; MsgVote: candidate's last log term
	Index      uint64  // MsgApp: PrevLogIndex; MsgVote: candidate's last log index
	Entries    []Entry // MsgApp: entries to store
	Commit     uint64  // leader commit index
	Snapshot   *Snapshot
	Reject     bool   // response: request was rejected
	RejectHint uint64 // MsgAppResp: hint for fast log backtracking
	Context    []byte // MsgReadIndex/heartbeat: read request id; campaign hint
}

// HardState is the subset of state that MUST be persisted to stable storage
// before any message reflecting it is sent.
type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

func (h HardState) isEmpty() bool { return h.Term == 0 && h.Vote == 0 && h.Commit == 0 }

func (h HardState) equal(o HardState) bool {
	return h.Term == o.Term && h.Vote == o.Vote && h.Commit == o.Commit
}

// SoftState is volatile state that is never persisted. It is convenient for
// observers (such as the UI) to learn the current leader and role.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

func (s SoftState) equal(o SoftState) bool {
	return s.Lead == o.Lead && s.RaftState == o.RaftState
}

// ConfState records the current cluster membership.
type ConfState struct {
	Voters   []uint64
	Learners []uint64
}

// SnapshotMetadata describes a snapshot's position in the log.
type SnapshotMetadata struct {
	Index     uint64
	Term      uint64
	ConfState ConfState
}

// Snapshot is a point-in-time image of the state machine plus metadata.
type Snapshot struct {
	Data     []byte
	Metadata SnapshotMetadata
}

func (s *Snapshot) isEmpty() bool { return s == nil || s.Metadata.Index == 0 }

// ConfChangeType is the kind of single-server membership change.
type ConfChangeType uint8

const (
	ConfChangeAddNode        ConfChangeType = iota // add as voter
	ConfChangeAddLearnerNode                       // add as non-voting learner
	ConfChangeRemoveNode                           // remove node
	ConfChangePromoteLearner                       // promote learner to voter
)

// ConfChange is the payload of an EntryConfChange entry.
type ConfChange struct {
	Type   ConfChangeType
	NodeID uint64
}

// ReadState is produced when a linearizable read request has been confirmed.
// The driver may serve the read once the state machine has applied Index.
type ReadState struct {
	Index uint64
	Ctx   []byte
}
