package sim

import (
	"encoding/json"
	"testing"
)

// collector captures the most recent StateEvent and all LogEvents emitted.
type collector struct {
	last   *StateEvent
	events []LogEvent
}

func (col *collector) emit(b []byte) {
	var probe struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(b, &probe) != nil {
		return
	}
	switch probe.Type {
	case "state":
		var s StateEvent
		if json.Unmarshal(b, &s) == nil {
			col.last = &s
		}
	case "event":
		var e LogEvent
		if json.Unmarshal(b, &e) == nil {
			col.events = append(col.events, e)
		}
	}
}

func (col *collector) hasEvent(kind string, node uint64) bool {
	for _, e := range col.events {
		if e.Kind == kind && e.Node == node {
			return true
		}
	}
	return false
}

// stepUntil steps the cluster until cond holds or maxTicks is exceeded.
func stepUntil(c *Cluster, col *collector, maxTicks int, cond func(*StateEvent) bool) bool {
	for i := 0; i < maxTicks; i++ {
		c.stepTick()
		if col.last != nil && cond(col.last) {
			return true
		}
	}
	return false
}

func leaderCount(s *StateEvent) int {
	n := 0
	for _, nd := range s.Nodes {
		if nd.Role == "leader" {
			n++
		}
	}
	return n
}

func hasKV(nd NodeView, key, val string) bool {
	for _, p := range nd.KV {
		if p.Key == key && p.Value == val {
			return true
		}
	}
	return false
}

func TestClusterElectsAndReplicates(t *testing.T) {
	col := &collector{}
	c := NewCluster(3, 42, col.emit)

	if !stepUntil(c, col, 200, func(s *StateEvent) bool { return leaderCount(s) == 1 }) {
		t.Fatalf("no single leader emerged; leaders=%d", leaderCount(col.last))
	}

	c.applyCommand(Command{Type: "kv", Op: "put", Key: "a", Value: "1", Target: "leader", ReqID: "1"})

	ok := stepUntil(c, col, 100, func(s *StateEvent) bool {
		for _, nd := range s.Nodes {
			if !hasKV(nd, "a", "1") {
				return false
			}
		}
		return len(s.Nodes) == 3
	})
	if !ok {
		t.Fatalf("write did not replicate to all nodes: %+v", col.last.Nodes)
	}
}

func TestClusterPartitionAndHeal(t *testing.T) {
	col := &collector{}
	c := NewCluster(5, 7, col.emit)

	if !stepUntil(c, col, 200, func(s *StateEvent) bool { return leaderCount(s) == 1 }) {
		t.Fatal("no leader before partition")
	}
	var leader uint64
	for _, nd := range col.last.Nodes {
		if nd.Role == "leader" {
			leader = nd.ID
		}
	}

	// Isolate the leader with one node; the majority of 3 should re-elect.
	var minority, majority []uint64
	for _, nd := range col.last.Nodes {
		if nd.ID == leader || (len(minority) < 2 && nd.ID != leader && len(majority) >= 0 && len(minority) < 1) {
			minority = append(minority, nd.ID)
		} else {
			majority = append(majority, nd.ID)
		}
	}
	// Ensure a clean 2 vs 3 split with the old leader in the minority.
	minority = []uint64{leader}
	majority = nil
	for _, nd := range col.last.Nodes {
		if nd.ID == leader {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, nd.ID)
		} else {
			majority = append(majority, nd.ID)
		}
	}
	c.applyCommand(Command{Type: "partition", Groups: [][]uint64{minority, majority}})

	ok := stepUntil(c, col, 300, func(s *StateEvent) bool {
		majLeader := false
		for _, nd := range s.Nodes {
			if nd.Role == "leader" {
				for _, m := range majority {
					if nd.ID == m {
						majLeader = true
					}
				}
			}
		}
		return majLeader
	})
	if !ok {
		t.Fatal("majority partition did not elect a new leader")
	}

	// Heal and confirm the cluster converges to a single leader.
	c.applyCommand(Command{Type: "partition", Groups: nil})
	if !stepUntil(c, col, 300, func(s *StateEvent) bool { return leaderCount(s) == 1 }) {
		t.Fatalf("cluster did not converge after heal; leaders=%d", leaderCount(col.last))
	}
}

func TestClusterCrashRestart(t *testing.T) {
	col := &collector{}
	c := NewCluster(3, 99, col.emit)
	stepUntil(c, col, 200, func(s *StateEvent) bool { return leaderCount(s) == 1 })

	c.applyCommand(Command{Type: "kv", Op: "put", Key: "k", Value: "v", Target: "leader", ReqID: "1"})
	stepUntil(c, col, 60, func(s *StateEvent) bool {
		for _, nd := range s.Nodes {
			if !hasKV(nd, "k", "v") {
				return false
			}
		}
		return true
	})

	var leader uint64
	for _, nd := range col.last.Nodes {
		if nd.Role == "leader" {
			leader = nd.ID
		}
	}
	c.applyCommand(Command{Type: "node", Action: "crash", ID: leader})
	if !stepUntil(c, col, 200, func(s *StateEvent) bool {
		// a new leader (different node) emerges
		for _, nd := range s.Nodes {
			if nd.Role == "leader" && nd.ID != leader {
				return true
			}
		}
		return false
	}) {
		t.Fatal("no new leader after crashing the old one")
	}

	c.applyCommand(Command{Type: "node", Action: "restart", ID: leader})
	if !stepUntil(c, col, 200, func(s *StateEvent) bool {
		for _, nd := range s.Nodes {
			if nd.ID == leader {
				return !nd.Crashed && hasKV(nd, "k", "v")
			}
		}
		return false
	}) {
		t.Fatal("restarted node did not recover committed state")
	}
}

// TestClusterSnapshotCatchup crashes a follower, advances the log past the
// compaction threshold so the leader trims away the entries the follower is
// missing, then restarts it and confirms it catches up via InstallSnapshot.
func TestClusterSnapshotCatchup(t *testing.T) {
	col := &collector{}
	c := NewCluster(5, 21, col.emit)
	if !stepUntil(c, col, 200, func(s *StateEvent) bool { return leaderCount(s) == 1 }) {
		t.Fatal("no leader")
	}
	var leader, victim uint64
	for _, nd := range col.last.Nodes {
		if nd.Role == "leader" {
			leader = nd.ID
		}
	}
	for _, nd := range col.last.Nodes {
		if nd.ID != leader {
			victim = nd.ID
			break
		}
	}

	// Crash a follower so it misses everything that follows.
	c.applyCommand(Command{Type: "node", Action: "crash", ID: victim})
	c.stepTick()

	// Write well past the compaction threshold while it is down.
	for i := 0; i < compactThreshold*2; i++ {
		c.applyCommand(Command{Type: "kv", Op: "put", Key: "k", Value: itoa(i), Target: "leader", ReqID: itoa(i)})
		for j := 0; j < 3; j++ {
			c.stepTick()
		}
	}
	final := itoa(compactThreshold*2 - 1)

	// The leader should have compacted its log (length far below commit index).
	var leaderLog, leaderCommit int
	for _, nd := range col.last.Nodes {
		if nd.ID == leader {
			leaderLog = len(nd.Log)
			leaderCommit = int(nd.Commit)
		}
	}
	if leaderLog >= leaderCommit {
		t.Fatalf("expected leader log (%d) to be compacted below commit (%d)", leaderLog, leaderCommit)
	}

	// Restart the follower; it must catch up via a snapshot to the final value.
	c.applyCommand(Command{Type: "node", Action: "restart", ID: victim})
	if !stepUntil(c, col, 300, func(s *StateEvent) bool {
		for _, nd := range s.Nodes {
			if nd.ID == victim {
				return !nd.Crashed && hasKV(nd, "k", final)
			}
		}
		return false
	}) {
		t.Fatalf("restarted node did not catch up to k=%s", final)
	}
	if !col.hasEvent("snapshot_installed", victim) {
		t.Fatal("expected the rejoining node to catch up via InstallSnapshot")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
