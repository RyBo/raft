package sim

import "github.com/rybo/raft/kvstore"

// This file defines the wire protocol between the simulation backend and the
// browser. Events flow backend -> browser; Commands flow browser -> backend.
// Each is a JSON object discriminated by its "type" field. The TypeScript
// mirror lives in webui/src/types/protocol.ts.

// ---- Events (backend -> browser) ----

// ClockView describes the logical clock state.
type ClockView struct {
	Running   bool `json:"running"`
	MsPerTick int  `json:"msPerTick"`
}

// ProgressView is the leader's replication progress for one follower.
type ProgressView struct {
	Match uint64 `json:"match"`
	Next  uint64 `json:"next"`
	State string `json:"state"`
}

// LogEntryView is one log entry, summarized for display.
type LogEntryView struct {
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	Committed bool   `json:"committed"`
	Kind      string `json:"kind"`    // "normal" | "confchange" | "noop"
	Summary   string `json:"summary"` // human-readable payload
}

// NodeView is the full per-node state the UI renders.
type NodeView struct {
	ID        uint64                  `json:"id"`
	Role      string                  `json:"role"`
	Term      uint64                  `json:"term"`
	Vote      uint64                  `json:"vote"`
	Lead      uint64                  `json:"lead"`
	Commit    uint64                  `json:"commit"`
	Applied   uint64                  `json:"applied"`
	LastIndex uint64                  `json:"lastIndex"`
	Crashed   bool                    `json:"crashed"`
	IsLearner bool                    `json:"isLearner"`
	Log       []LogEntryView          `json:"log"`
	KV        []kvstore.Pair          `json:"kv"`
	Progress  map[uint64]ProgressView `json:"progress,omitempty"`
}

// ConfigView is the cluster membership.
type ConfigView struct {
	Voters   []uint64 `json:"voters"`
	Learners []uint64 `json:"learners"`
}

// LinkView is a per-directed-link network setting override.
type LinkView struct {
	From    uint64  `json:"from"`
	To      uint64  `json:"to"`
	Latency int     `json:"latency"`
	Drop    float64 `json:"drop"`
}

// NetView is the current network configuration.
type NetView struct {
	Partitions  [][]uint64 `json:"partitions"`
	BaseLatency int        `json:"baseLatency"`
	DropRate    float64    `json:"dropRate"`
	Links       []LinkView `json:"links"`
}

// StateEvent is the full cluster snapshot, emitted every tick and replayed to
// newly connected clients.
type StateEvent struct {
	Type   string     `json:"type"` // "state"
	Tick   uint64     `json:"tick"`
	Seed   int64      `json:"seed"`
	Clock  ClockView  `json:"clock"`
	Nodes  []NodeView `json:"nodes"`
	Config ConfigView `json:"config"`
	Net    NetView    `json:"net"`
}

// MessageEvent describes a single RPC for animation.
type MessageEvent struct {
	Type        string `json:"type"` // "message"
	ID          string `json:"id"`
	From        uint64 `json:"from"`
	To          uint64 `json:"to"`
	MsgType     string `json:"msgType"`
	SentTick    uint64 `json:"sentTick"`
	DeliverTick uint64 `json:"deliverTick"`
	Fate        string `json:"fate"` // "delivered" | "dropped" | "duplicated"
	Entries     int    `json:"entries"`
}

// LogEvent is a discrete, human-readable event for the timeline.
type LogEvent struct {
	Type string `json:"type"` // "event"
	Tick uint64 `json:"tick"`
	Kind string `json:"kind"`
	Node uint64 `json:"node"`
	Term uint64 `json:"term"`
	Text string `json:"text"`
}

// KVResultEvent answers a client KV operation.
type KVResultEvent struct {
	Type         string `json:"type"` // "kvResult"
	ReqID        string `json:"reqId"`
	OK           bool   `json:"ok"`
	Op           string `json:"op"`
	Key          string `json:"key"`
	Value        string `json:"value"`
	Found        bool   `json:"found"`
	ServedBy     uint64 `json:"servedBy"`
	Linearizable bool   `json:"linearizable"`
	Note         string `json:"note"`
}

// ---- Commands (browser -> backend) ----

// Command is the single inbound message shape. Fields are interpreted per Type.
type Command struct {
	Type string `json:"type"`

	// clock
	Action    string `json:"action,omitempty"` // run|pause|step|setSpeed
	MsPerTick int    `json:"msPerTick,omitempty"`

	// kv
	Op           string `json:"op,omitempty"` // put|delete|get
	Key          string `json:"key,omitempty"`
	Value        string `json:"value,omitempty"`
	Target       string `json:"target,omitempty"` // "leader" | node id as string
	Linearizable bool   `json:"linearizable,omitempty"`
	ReqID        string `json:"reqId,omitempty"`

	// node (crash|restart|remove|promote) + addNode/addLearner
	ID uint64 `json:"id,omitempty"`

	// partition
	Groups [][]uint64 `json:"groups,omitempty"`

	// net (global defaults or single link)
	Latency  int     `json:"latency,omitempty"`
	Drop     float64 `json:"drop,omitempty"`
	LinkFrom uint64  `json:"linkFrom,omitempty"`
	LinkTo   uint64  `json:"linkTo,omitempty"`

	// reset
	N    int   `json:"n,omitempty"`
	Seed int64 `json:"seed,omitempty"`
}
