// Package transport is a real network transport for the Raft core, carrying
// raft.Messages between separate processes over gRPC. A node runs one Server
// (the inbound side) and one Peers (the outbound side). Delivery is best-effort:
// the core retries via heartbeat and election timers, so dropped messages are
// safe and Send never blocks the driver loop.
package transport

import (
	"context"
	"net"

	"google.golang.org/grpc"

	"github.com/rybo/raft/raft"
	"github.com/rybo/raft/transport/raftpb"
)

// Server is the inbound half of the transport: it accepts Deliver RPCs from
// peers and forwards each decoded message onto inbound, where the node's driver
// loop steps it into the RawNode.
type Server struct {
	raftpb.UnimplementedRaftTransportServer
	inbound chan<- raft.Message
}

// NewServer creates a Server that forwards received messages onto inbound. The
// channel is typically the same one the node's run loop drains.
func NewServer(inbound chan<- raft.Message) *Server {
	return &Server{inbound: inbound}
}

// Deliver receives one message from a peer. If the inbound channel is full it
// drops the message rather than blocking the RPC handler — a dropped Raft message
// is harmless and keeping the handler responsive matters more.
func (s *Server) Deliver(ctx context.Context, pm *raftpb.Message) (*raftpb.Ack, error) {
	select {
	case s.inbound <- fromProto(pm):
	case <-ctx.Done():
	default:
	}
	return &raftpb.Ack{}, nil
}

// Serve registers srv on a new gRPC server and serves lis in a background
// goroutine. The returned *grpc.Server is stopped by the caller (GracefulStop).
func Serve(lis net.Listener, srv *Server) *grpc.Server {
	gs := grpc.NewServer()
	raftpb.RegisterRaftTransportServer(gs, srv)
	go gs.Serve(lis)
	return gs
}
