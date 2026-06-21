package raft

import "testing"

func TestSingleNodeElection(t *testing.T) {
	n := newNetwork(t, []uint64{1}, false, false)
	n.campaign(1)
	if st := n.status(1); st.State != StateLeader {
		t.Fatalf("node 1 should be leader, got %s", st.State)
	}
}

func TestLeaderElection(t *testing.T) {
	n := newNetwork(t, []uint64{1, 2, 3}, false, false)
	n.campaign(1)

	leader := n.requireSingleLeader(t)
	if leader != 1 {
		t.Fatalf("expected node 1 to win, got %d", leader)
	}
	for _, id := range []uint64{2, 3} {
		if st := n.status(id); st.State != StateFollower {
			t.Fatalf("node %d should be follower, got %s", id, st.State)
		}
		if st := n.status(id); st.Lead != 1 {
			t.Fatalf("node %d should follow leader 1, got %d", id, st.Lead)
		}
	}
	if term := n.status(1).Term; term != 1 {
		t.Fatalf("expected term 1, got %d", term)
	}
}

func TestElectionByTimeout(t *testing.T) {
	// With distinct randomized timeouts, ticking everyone should still elect
	// exactly one leader deterministically.
	n := newNetwork(t, []uint64{1, 2, 3}, true, true)
	n.tickAll(40)
	n.requireSingleLeader(t)
}

func TestLogReplication(t *testing.T) {
	n := newNetwork(t, []uint64{1, 2, 3}, false, false)
	n.campaign(1)
	n.requireSingleLeader(t)

	n.propose(1, []byte("x=1"))
	n.propose(1, []byte("y=2"))

	// Index 1 = empty leader entry, 2 = x=1, 3 = y=2.
	for _, id := range n.sortedIDs() {
		st := n.status(id)
		if st.Commit != 3 {
			t.Fatalf("node %d commit=%d, want 3", id, st.Commit)
		}
		if st.LastIndex != 3 {
			t.Fatalf("node %d lastIndex=%d, want 3", id, st.LastIndex)
		}
		applied := n.nodes[id].applied
		if len(applied) != 2 {
			t.Fatalf("node %d applied %d normal entries, want 2", id, len(applied))
		}
		if string(applied[0].Data) != "x=1" || string(applied[1].Data) != "y=2" {
			t.Fatalf("node %d applied wrong data: %q, %q", id, applied[0].Data, applied[1].Data)
		}
	}
}

func TestElectionAfterLeaderPartition(t *testing.T) {
	n := newNetwork(t, []uint64{1, 2, 3, 4, 5}, true, true)
	n.campaign(1)
	if n.requireSingleLeader(t) != 1 {
		t.Fatal("node 1 should be leader")
	}
	n.propose(1, []byte("a=1"))

	// Isolate the leader with one follower; majority {3,4,5} should elect anew.
	n.partition([]uint64{1, 2}, []uint64{3, 4, 5})
	n.tickAll(40)

	// New leader must come from the majority side.
	majLeaders := 0
	var newLeader uint64
	for _, id := range []uint64{3, 4, 5} {
		if n.status(id).State == StateLeader {
			majLeaders++
			newLeader = id
		}
	}
	if majLeaders != 1 {
		t.Fatalf("expected one leader in majority partition, got %d", majLeaders)
	}

	// The minority side cannot commit new entries.
	_ = n.nodes[1].node.Propose([]byte("b=2"))
	n.run()
	if c := n.status(1).Commit; c != 2 {
		t.Fatalf("isolated old leader should not advance commit beyond 2, got %d", c)
	}

	// New leader can commit.
	n.propose(newLeader, []byte("c=3"))
	if c := n.status(newLeader).Commit; c < 3 {
		t.Fatalf("new leader should advance commit, got %d", c)
	}

	// Heal: old leader steps down, logs reconcile, single leader remains.
	n.heal()
	n.tickAll(20)
	leader := n.requireSingleLeader(t)
	if leader == 1 {
		t.Fatalf("old leader 1 should have stepped down")
	}
}
