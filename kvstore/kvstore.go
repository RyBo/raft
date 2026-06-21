// Package kvstore is a tiny replicated key-value state machine driven by the
// committed Raft log. It is the demo's "application": each committed normal
// entry is a serialized Command that mutates the map.
package kvstore

import (
	"encoding/json"
	"sort"

	"github.com/rybo/raft/raft"
)

// OpType is the kind of KV mutation.
type OpType string

const (
	OpPut    OpType = "put"
	OpDelete OpType = "delete"
	// OpGet is carried for tracing only; reads do not mutate state and are
	// usually served via ReadIndex rather than the log.
	OpGet OpType = "get"
)

// Command is the payload stored in a normal Raft log entry.
type Command struct {
	Op    OpType `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Encode serializes a command for use as Entry.Data.
func (c Command) Encode() []byte {
	b, _ := json.Marshal(c)
	return b
}

// DecodeCommand parses a command from Entry.Data.
func DecodeCommand(data []byte) (Command, bool) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, false
	}
	return c, true
}

// KV is the key-value state machine. It is not safe for concurrent use; the
// driver applies entries serially.
type KV struct {
	data map[string]string
}

// New creates an empty KV store.
func New() *KV { return &KV{data: map[string]string{}} }

// Apply applies a committed normal entry and returns the resulting value (for
// puts) or the removed key's prior value (for deletes).
func (kv *KV) Apply(entry raft.Entry) (result []byte, applied bool) {
	if entry.Type != raft.EntryNormal || len(entry.Data) == 0 {
		return nil, false
	}
	cmd, ok := DecodeCommand(entry.Data)
	if !ok {
		return nil, false
	}
	switch cmd.Op {
	case OpPut:
		kv.data[cmd.Key] = cmd.Value
		return []byte(cmd.Value), true
	case OpDelete:
		prev := kv.data[cmd.Key]
		delete(kv.data, cmd.Key)
		return []byte(prev), true
	default:
		return nil, false
	}
}

// Get returns the value for a key and whether it exists. This reads the local
// applied state — during a partition a stale follower may return old data,
// which is exactly what the CAP demo illustrates.
func (kv *KV) Get(key string) (string, bool) {
	v, ok := kv.data[key]
	return v, ok
}

// Snapshot serializes the whole store for log compaction.
func (kv *KV) Snapshot() ([]byte, error) {
	return json.Marshal(kv.data)
}

// Restore replaces the store from a snapshot produced by Snapshot.
func (kv *KV) Restore(data []byte) error {
	m := map[string]string{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
	}
	kv.data = m
	return nil
}

// Pairs returns the store contents as a sorted slice (deterministic for the UI).
func (kv *KV) Pairs() []Pair {
	keys := make([]string, 0, len(kv.data))
	for k := range kv.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]Pair, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, Pair{Key: k, Value: kv.data[k]})
	}
	return pairs
}

// Pair is a single key-value pair.
type Pair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
