package node

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/rybo/raft/kvstore"
	"github.com/rybo/raft/raft"
)

type nopSender struct{}

func (nopSender) Send([]raft.Message) {}

// singleNode builds a one-voter node backed by the given storage and FSM.
func singleNode(t *testing.T, storage Storage, fsm FSM) *Node {
	t.Helper()
	n, err := New(Config{
		ID:            1,
		Peers:         []uint64{1},
		ElectionTick:  10,
		HeartbeatTick: 1,
		TickInterval:  5 * time.Millisecond,
		PreVote:       true,
		CheckQuorum:   true,
		Rand:          rand.New(rand.NewSource(1)),
		Storage:       storage,
		FSM:           fsm,
		Sender:        nopSender{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return n
}

func waitLeader(t *testing.T, n *Node) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		st, err := n.Status(ctx)
		cancel()
		if err == nil && st.State == raft.StateLeader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("node did not become leader in time")
}

func putCmd(key, val string) []byte {
	return kvstore.Command{Op: kvstore.OpPut, Key: key, Value: val}.Encode()
}

func TestSingleNodeProposeAndRead(t *testing.T) {
	kv := kvstore.New()
	n := singleNode(t, raft.NewMemoryStorage(), kv)
	go n.Run()
	defer n.Stop()
	waitLeader(t, n)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := n.Propose(ctx, putCmd("x", "1")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	var val string
	var ok bool
	if err := n.LinearizableRead(ctx, func() { val, ok = kv.Get("x") }); err != nil {
		t.Fatalf("LinearizableRead: %v", err)
	}
	if !ok || val != "1" {
		t.Fatalf("read x = %q,%v; want 1,true", val, ok)
	}
}

func TestProposeOnNonLeaderRejected(t *testing.T) {
	// A two-voter cluster with no peer reachable: node 1 cannot win an election,
	// so it never becomes leader and Propose must report ErrNotLeader.
	n, err := New(Config{
		ID:            1,
		Peers:         []uint64{1, 2},
		ElectionTick:  10,
		HeartbeatTick: 1,
		TickInterval:  5 * time.Millisecond,
		PreVote:       true,
		CheckQuorum:   true,
		Rand:          rand.New(rand.NewSource(1)),
		Storage:       raft.NewMemoryStorage(),
		FSM:           kvstore.New(),
		Sender:        nopSender{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	go n.Run()
	defer n.Stop()

	// Give it time to (fail to) elect.
	time.Sleep(200 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.Propose(ctx, putCmd("x", "1")); err != ErrNotLeader {
		t.Fatalf("Propose on non-leader err = %v, want ErrNotLeader", err)
	}
}

// TestRestartRebuildsFSM proposes writes, stops the node, then starts a new node
// over the SAME storage with a FRESH (empty) FSM, modelling a process restart
// where volatile state is lost but the disk survives. The replay of committed
// entries must rebuild the FSM.
func TestRestartRebuildsFSM(t *testing.T) {
	storage := raft.NewMemoryStorage()

	kv1 := kvstore.New()
	n1 := singleNode(t, storage, kv1)
	go n1.Run()
	waitLeader(t, n1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	for _, k := range []string{"a", "b", "c"} {
		if err := n1.Propose(ctx, putCmd(k, k+k)); err != nil {
			t.Fatalf("Propose %s: %v", k, err)
		}
	}
	cancel()
	n1.Stop()

	// Restart: same storage, brand-new empty FSM.
	kv2 := kvstore.New()
	n2 := singleNode(t, storage, kv2)
	go n2.Run()
	defer n2.Stop()
	waitLeader(t, n2)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	var val string
	var ok bool
	if err := n2.LinearizableRead(ctx2, func() { val, ok = kv2.Get("b") }); err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if !ok || val != "bb" {
		t.Fatalf("restart read b = %q,%v; want bb,true", val, ok)
	}
}
