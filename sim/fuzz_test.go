package sim

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestFuzzSafetyInvariants runs many randomized scenarios (partitions, crashes,
// restarts, writes, packet loss) against deterministic clusters and checks the
// core Raft safety invariants after every step:
//
//   - Election safety: at most one leader per term (across all of time).
//   - Log matching + state-machine safety: any entry that any node reports as
//     committed must agree (term + payload) with every other node at that index.
//
// Because the simulation is single-threaded and seeded, a failure is exactly
// reproducible from the printed trial seed.
func TestFuzzSafetyInvariants(t *testing.T) {
	const trials = 25
	for trial := 0; trial < trials; trial++ {
		seed := int64(trial*131 + 17)
		runFuzzTrial(t, seed)
	}
}

func runFuzzTrial(t *testing.T, seed int64) {
	col := &collector{}
	n := 3 + int(seed)%3 // 3..5 nodes
	c := NewCluster(n, seed, col.emit)
	rng := rand.New(rand.NewSource(seed*2654435761 + 1))

	termLeader := map[uint64]uint64{}        // term -> leader id seen
	committed := map[uint64]committedEntry{} // index -> (term, summary)

	keys := []string{"a", "b", "c", "d"}

	for step := 0; step < 250; step++ {
		s := col.last
		live := liveIDs(s)

		switch rng.Intn(12) {
		case 0, 1, 2, 3:
			c.applyCommand(Command{
				Type: "kv", Op: "put", Target: "leader",
				Key:   keys[rng.Intn(len(keys))],
				Value: fmt.Sprintf("%d", rng.Intn(1000)),
				ReqID: fmt.Sprintf("s%d", step),
			})
		case 4:
			if len(live) >= 2 {
				mid := 1 + rng.Intn(len(live)-1)
				c.applyCommand(Command{Type: "partition", Groups: [][]uint64{live[:mid], live[mid:]}})
			}
		case 5:
			c.applyCommand(Command{Type: "partition", Groups: nil}) // heal
		case 6:
			if len(live) > 0 {
				c.applyCommand(Command{Type: "node", Action: "crash", ID: live[rng.Intn(len(live))]})
			}
		case 7:
			if dead := crashedIDs(s); len(dead) > 0 {
				c.applyCommand(Command{Type: "node", Action: "restart", ID: dead[rng.Intn(len(dead))]})
			}
		case 8:
			c.applyCommand(Command{Type: "net", Latency: 1 + rng.Intn(5), Drop: float64(rng.Intn(4)) / 10})
		default:
			// idle
		}

		for i := 0; i < 1+rng.Intn(6); i++ {
			c.stepTick()
			checkInvariants(t, seed, step, col.last, termLeader, committed)
		}
	}
}

type committedEntry struct {
	term    uint64
	summary string
}

func checkInvariants(t *testing.T, seed int64, step int, s *StateEvent, termLeader map[uint64]uint64, committed map[uint64]committedEntry) {
	if s == nil {
		return
	}
	for _, n := range s.Nodes {
		// Election safety.
		if n.Role == "leader" {
			if prev, ok := termLeader[n.Term]; ok && prev != n.ID {
				t.Fatalf("seed %d step %d: two leaders for term %d: N%d and N%d",
					seed, step, n.Term, prev, n.ID)
			}
			termLeader[n.Term] = n.ID
		}
		// Log matching / state-machine safety for committed entries.
		for _, e := range n.Log {
			if !e.Committed {
				continue
			}
			ce := committedEntry{term: e.Term, summary: e.Summary}
			if prev, ok := committed[e.Index]; ok {
				if prev != ce {
					t.Fatalf("seed %d step %d: committed entry at index %d diverges: %+v vs N%d %+v",
						seed, step, e.Index, prev, n.ID, ce)
				}
			} else {
				committed[e.Index] = ce
			}
		}
	}
}

func liveIDs(s *StateEvent) []uint64 {
	if s == nil {
		return nil
	}
	var ids []uint64
	for _, n := range s.Nodes {
		if !n.Crashed {
			ids = append(ids, n.ID)
		}
	}
	return ids
}

func crashedIDs(s *StateEvent) []uint64 {
	if s == nil {
		return nil
	}
	var ids []uint64
	for _, n := range s.Nodes {
		if n.Crashed {
			ids = append(ids, n.ID)
		}
	}
	return ids
}
