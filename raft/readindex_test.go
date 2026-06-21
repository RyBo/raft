package raft

import (
	"bytes"
	"testing"
)

func TestReadIndexConfirmed(t *testing.T) {
	n := newNetwork(t, []uint64{1, 2, 3}, false, false)
	n.campaign(1)
	n.requireSingleLeader(t)
	n.propose(1, []byte("a=1"))

	_ = n.nodes[1].node.ReadIndex([]byte("r1"))
	n.run()

	reads := n.nodes[1].reads
	if len(reads) != 1 {
		t.Fatalf("expected 1 confirmed read, got %d", len(reads))
	}
	if !bytes.Equal(reads[0].Ctx, []byte("r1")) {
		t.Fatalf("unexpected read ctx %q", reads[0].Ctx)
	}
	if reads[0].Index < 2 {
		t.Fatalf("read index %d should be >= committed (2)", reads[0].Index)
	}
}

func TestReadIndexForwardedFromFollower(t *testing.T) {
	n := newNetwork(t, []uint64{1, 2, 3}, false, false)
	n.campaign(1)
	n.requireSingleLeader(t)
	n.propose(1, []byte("a=1"))

	// A follower requests a read; it forwards to the leader and gets a response.
	_ = n.nodes[2].node.ReadIndex([]byte("rf"))
	n.run()

	reads := n.nodes[2].reads
	if len(reads) != 1 {
		t.Fatalf("expected follower to receive 1 confirmed read, got %d", len(reads))
	}
	if !bytes.Equal(reads[0].Ctx, []byte("rf")) {
		t.Fatalf("unexpected read ctx %q", reads[0].Ctx)
	}
}
