package node

import (
	"context"
	"io"
	"log"
	"math/rand"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/rybo/raft/kvstore"
	"github.com/rybo/raft/raft"
	"github.com/rybo/raft/transport"
	"github.com/rybo/raft/walstore"
)

// stopper is the subset of *grpc.Server the test needs, so the test file need not
// import grpc directly.
type stopper interface{ GracefulStop() }

// clusterNode bundles everything one node in the integration cluster owns. It can
// be stopped (modelling a crash, since walstore is already fsynced) and started
// again on the same address and data directory (modelling a restart).
type clusterNode struct {
	id      uint64
	addr    string
	dataDir string
	voters  []uint64
	addrs   map[uint64]string

	kv    *kvstore.KV
	wal   *walstore.WAL
	peers *transport.Peers
	gs    stopper
	node  *Node
}

func (cn *clusterNode) start(t *testing.T) {
	t.Helper()
	// Retry briefly: on a restart the old port may still be releasing.
	var lis net.Listener
	var err error
	for i := 0; i < 20; i++ {
		if lis, err = net.Listen("tcp", cn.addr); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("node %d listen %s: %v", cn.id, cn.addr, err)
	}
	inbound := make(chan raft.Message, 1024)
	cn.gs = transport.Serve(lis, transport.NewServer(inbound))

	peers, err := transport.NewPeers(cn.id, cn.addrs, nil)
	if err != nil {
		t.Fatalf("node %d peers: %v", cn.id, err)
	}
	cn.peers = peers

	wal, err := walstore.Open(cn.dataDir)
	if err != nil {
		t.Fatalf("node %d open wal: %v", cn.id, err)
	}
	cn.wal = wal
	cn.kv = kvstore.New()

	n, err := New(Config{
		ID:            cn.id,
		Peers:         cn.voters,
		ElectionTick:  10,
		HeartbeatTick: 1,
		TickInterval:  10 * time.Millisecond,
		PreVote:       true,
		CheckQuorum:   true,
		Rand:          rand.New(rand.NewSource(int64(cn.id) * 7919)),
		Storage:       wal,
		FSM:           cn.kv,
		Sender:        peers,
		Inbound:       inbound,
		Logger:        log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("node %d new: %v", cn.id, err)
	}
	cn.node = n
	go n.Run()
}

func (cn *clusterNode) stop() {
	cn.node.Stop()
	cn.gs.GracefulStop()
	cn.peers.Close()
	cn.wal.Close()
}

// newCluster allocates n nodes with fixed loopback addresses and data dirs.
func newCluster(t *testing.T, n int) []*clusterNode {
	t.Helper()
	addrs := make(map[uint64]string, n)
	voters := make([]uint64, 0, n)
	for i := 1; i <= n; i++ {
		addrs[uint64(i)] = freeAddr(t)
		voters = append(voters, uint64(i))
	}
	nodes := make([]*clusterNode, 0, n)
	for i := 1; i <= n; i++ {
		nodes = append(nodes, &clusterNode{
			id:      uint64(i),
			addr:    addrs[uint64(i)],
			dataDir: filepath.Join(t.TempDir(), "n"+itoa(i)),
			voters:  voters,
			addrs:   addrs,
		})
	}
	return nodes
}

func itoa(i int) string { return string(rune('0' + i)) }

// freeAddr returns a currently-free loopback address. There is a small race
// between closing the probe listener and the caller re-binding, acceptable in
// tests and necessary so a node can re-listen on the same address after a restart.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// findLeader polls until exactly one node reports itself leader, returning it.
func findLeader(t *testing.T, nodes []*clusterNode) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, cn := range nodes {
			if cn.node == nil { // a crashed node in the recovery test
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			st, err := cn.node.Status(ctx)
			cancel()
			if err == nil && st.State == raft.StateLeader {
				return cn
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

// propose finds the leader and proposes, retrying through leadership changes.
func propose(t *testing.T, nodes []*clusterNode, data []byte) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		leader := findLeader(t, nodes)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := leader.node.Propose(ctx, data)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("failed to propose within timeout")
}

// waitValue polls a node's local (stale) state until key==want.
func waitValue(t *testing.T, cn *clusterNode, key, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var val string
		var ok bool
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err := cn.node.Do(ctx, func() { val, ok = cn.kv.Get(key) })
		cancel()
		if err == nil && ok && val == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %d: key %q did not converge to %q", cn.id, key, want)
}

func TestClusterElectsLeaderAndReplicates(t *testing.T) {
	nodes := newCluster(t, 3)
	for _, cn := range nodes {
		cn.start(t)
	}
	defer func() {
		for _, cn := range nodes {
			cn.stop()
		}
	}()

	findLeader(t, nodes) // wait for election to settle

	propose(t, nodes, putCmd("color", "blue"))

	// Every node's state machine must converge on the committed write.
	for _, cn := range nodes {
		waitValue(t, cn, "color", "blue")
	}

	// A linearizable read served by a follower must also see it.
	follower := pickNonLeader(t, nodes)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var val string
	var ok bool
	if err := follower.node.LinearizableRead(ctx, func() { val, ok = follower.kv.Get("color") }); err != nil {
		t.Fatalf("linearizable read on follower %d: %v", follower.id, err)
	}
	if !ok || val != "blue" {
		t.Fatalf("follower linearizable read = %q,%v; want blue,true", val, ok)
	}
}

func TestNodeRecoversFromDisk(t *testing.T) {
	nodes := newCluster(t, 3)
	for _, cn := range nodes {
		cn.start(t)
	}
	defer func() {
		for _, cn := range nodes {
			if cn.node != nil {
				cn.stop()
			}
		}
	}()

	findLeader(t, nodes)
	propose(t, nodes, putCmd("k1", "v1"))
	for _, cn := range nodes {
		waitValue(t, cn, "k1", "v1")
	}

	// Crash a follower.
	victim := pickNonLeader(t, nodes)
	victim.stop()
	victim.node = nil

	// Commit more writes while it is down, so recovery must combine on-disk state
	// with catch-up from the leader.
	propose(t, nodes, putCmd("k2", "v2"))

	// Restart the victim on the same address and data directory.
	victim.start(t)

	// It must recover k1 (committed to its disk before the crash) and catch up k2.
	waitValue(t, victim, "k1", "v1")
	waitValue(t, victim, "k2", "v2")
}

func pickNonLeader(t *testing.T, nodes []*clusterNode) *clusterNode {
	t.Helper()
	leader := findLeader(t, nodes)
	for _, cn := range nodes {
		if cn.id != leader.id && cn.node != nil {
			return cn
		}
	}
	t.Fatal("no non-leader node found")
	return nil
}
