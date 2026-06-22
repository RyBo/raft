package transport

import (
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/rybo/raft/raft"
)

func TestConvertRoundTrip(t *testing.T) {
	cases := []raft.Message{
		{Type: raft.MsgVote, To: 2, From: 1, Term: 5, LogTerm: 4, Index: 9, Context: []byte("rid")},
		{
			Type: raft.MsgApp, To: 3, From: 1, Term: 7, Index: 10, Commit: 8,
			Entries: []raft.Entry{
				{Term: 7, Index: 11, Type: raft.EntryNormal, Data: []byte("a")},
				{Term: 7, Index: 12, Type: raft.EntryConfChange, Data: []byte{1, 2, 3}},
			},
		},
		{
			Type: raft.MsgSnap, To: 2, From: 1, Term: 9,
			Snapshot: &raft.Snapshot{
				Data: []byte("fsm-state"),
				Metadata: raft.SnapshotMetadata{
					Index:     20,
					Term:      9,
					ConfState: raft.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}},
				},
			},
		},
		{Type: raft.MsgAppResp, To: 1, From: 2, Term: 7, Index: 12, Reject: true, RejectHint: 5},
	}
	for i, m := range cases {
		got := fromProto(toProto(m))
		if !reflect.DeepEqual(got, m) {
			t.Fatalf("case %d round-trip mismatch:\n got %+v\nwant %+v", i, got, m)
		}
	}
}

// TestDeliverOverGRPC starts a real gRPC server on an ephemeral localhost port,
// sends a message through Peers, and asserts it arrives on the inbound channel.
func TestDeliverOverGRPC(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	inbound := make(chan raft.Message, 4)
	gs := Serve(lis, NewServer(inbound))
	defer gs.GracefulStop()

	// Node 1 sends to node 2 (the server above).
	peers, err := NewPeers(1, map[uint64]string{2: lis.Addr().String()}, nil)
	if err != nil {
		t.Fatalf("NewPeers: %v", err)
	}
	defer peers.Close()

	want := raft.Message{
		Type: raft.MsgApp, To: 2, From: 1, Term: 3, Index: 1, Commit: 1,
		Entries: []raft.Entry{{Term: 3, Index: 2, Type: raft.EntryNormal, Data: []byte("hi")}},
	}
	peers.Send([]raft.Message{want})

	select {
	case got := <-inbound:
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("received %+v, want %+v", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

// TestSendSkipsUnknownAndSelf verifies routing guards: a message to self or to an
// unregistered peer is silently dropped, not sent.
func TestSendSkipsUnknownAndSelf(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	inbound := make(chan raft.Message, 4)
	gs := Serve(lis, NewServer(inbound))
	defer gs.GracefulStop()

	peers, err := NewPeers(1, map[uint64]string{2: lis.Addr().String()}, nil)
	if err != nil {
		t.Fatalf("NewPeers: %v", err)
	}
	defer peers.Close()

	// To self (1) and to unknown (9): both dropped. To 2: delivered.
	peers.Send([]raft.Message{
		{Type: raft.MsgHeartbeat, To: 1, From: 1},
		{Type: raft.MsgHeartbeat, To: 9, From: 1},
		{Type: raft.MsgHeartbeat, To: 2, From: 1, Term: 1},
	})

	select {
	case got := <-inbound:
		if got.To != 2 {
			t.Fatalf("expected only the message to node 2, got To=%d", got.To)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the deliverable message")
	}
	// No further messages should arrive.
	select {
	case extra := <-inbound:
		t.Fatalf("unexpected extra message: %+v", extra)
	case <-time.After(200 * time.Millisecond):
	}
}
