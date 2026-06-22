package transport

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rybo/raft/raft"
	"github.com/rybo/raft/transport/raftpb"
)

// sendQueue bounds per-peer outbound buffering; once full, messages are dropped
// (best-effort). deliverTimeout caps a single Deliver RPC.
const (
	sendQueue      = 256
	deliverTimeout = time.Second
)

// Peers is the outbound half of the transport: a registry mapping node ID to a
// gRPC connection. Send routes each message to its destination peer through a
// per-peer worker goroutine, so the caller (the driver loop) never blocks and
// per-peer ordering is preserved. It is safe for concurrent use.
type Peers struct {
	self uint64
	log  *log.Logger

	mu     sync.Mutex
	peers  map[uint64]*peerConn
	closed bool
}

// peerConn is one peer's connection and its outbound queue/worker.
type peerConn struct {
	id   uint64
	cc   *grpc.ClientConn
	cli  raftpb.RaftTransportClient
	out  chan *raftpb.Message
	done chan struct{}
	log  *log.Logger
}

// NewPeers creates a registry for the given node-ID→address map (gRPC addresses).
// The entry for self, if present, is skipped. Connections are created lazily by
// gRPC and dialed on first use, so unreachable peers do not fail construction.
func NewPeers(self uint64, addrs map[uint64]string, logger *log.Logger) (*Peers, error) {
	p := &Peers{self: self, log: logger, peers: make(map[uint64]*peerConn)}
	for id, addr := range addrs {
		if id == self {
			continue
		}
		if err := p.SetAddr(id, addr); err != nil {
			p.Close()
			return nil, err
		}
	}
	return p, nil
}

// SetAddr adds or replaces the address for a peer, (re)establishing its
// connection and worker. Used at startup and by membership changes. The
// connection is created in gRPC's idle state and dialed on first Deliver.
func (p *Peers) SetAddr(id uint64, addr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id == p.self || p.closed {
		return nil
	}
	p.removeLocked(id)
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	pc := &peerConn{
		id:   id,
		cc:   cc,
		cli:  raftpb.NewRaftTransportClient(cc),
		out:  make(chan *raftpb.Message, sendQueue),
		done: make(chan struct{}),
		log:  p.log,
	}
	go pc.run()
	p.peers[id] = pc
	return nil
}

// Send routes each message to its destination peer, dropping any with no known
// peer or whose queue is full. Never blocks.
func (p *Peers) Send(msgs []raft.Message) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	for _, m := range msgs {
		if m.To == p.self || m.To == raft.None {
			continue
		}
		pc, ok := p.peers[m.To]
		if !ok {
			continue
		}
		select {
		case pc.out <- toProto(m):
		default: // queue full: drop, best-effort
		}
	}
}

// RemovePeer tears down a peer's connection and worker.
func (p *Peers) RemovePeer(id uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removeLocked(id)
}

// Close tears down every connection and stops every worker.
func (p *Peers) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for id := range p.peers {
		p.removeLocked(id)
	}
	return nil
}

func (p *Peers) removeLocked(id uint64) {
	pc, ok := p.peers[id]
	if !ok {
		return
	}
	close(pc.done)
	if pc.cc != nil {
		pc.cc.Close()
	}
	delete(p.peers, id)
}

func (pc *peerConn) run() {
	for {
		select {
		case <-pc.done:
			return
		case m := <-pc.out:
			ctx, cancel := context.WithTimeout(context.Background(), deliverTimeout)
			_, err := pc.cli.Deliver(ctx, m)
			cancel()
			if err != nil && pc.log != nil {
				pc.log.Printf("transport: deliver to node %d failed: %v", pc.id, err)
			}
		}
	}
}
