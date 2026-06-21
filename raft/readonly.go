package raft

// readOnly tracks in-flight linearizable read requests. A read is confirmed once
// a quorum of followers acknowledges a heartbeat carrying its context, proving
// the leader was still leader at-or-after the recorded commit index.
type readOnly struct {
	pending map[string]*readIndexStatus
	queue   []string
}

type readIndexStatus struct {
	req   Message         // original MsgReadIndex (From, Context)
	index uint64          // commit index when the request was received
	acks  map[uint64]bool // peers that acked the heartbeat
}

func newReadOnly() *readOnly {
	return &readOnly{pending: make(map[string]*readIndexStatus)}
}

// addRequest registers a read request keyed by its context.
func (ro *readOnly) addRequest(index uint64, m Message) {
	ctx := string(m.Context)
	if _, ok := ro.pending[ctx]; ok {
		return
	}
	ro.pending[ctx] = &readIndexStatus{req: m, index: index, acks: map[uint64]bool{}}
	ro.queue = append(ro.queue, ctx)
}

// recvAck records an acknowledgement and returns the current ack count
// (including the implicit leader self-ack).
func (ro *readOnly) recvAck(id uint64, context []byte) int {
	rs, ok := ro.pending[string(context)]
	if !ok {
		return 0
	}
	rs.acks[id] = true
	return len(rs.acks) + 1 // +1 for the leader itself
}

// advance returns all read statuses up to and including the given context, in
// request order, removing them from the queue.
func (ro *readOnly) advance(context []byte) []*readIndexStatus {
	var rss []*readIndexStatus
	ctx := string(context)
	found := false
	var i int
	for idx, okctx := range ro.queue {
		rs, ok := ro.pending[okctx]
		if !ok {
			panic("raft: cannot find corresponding read state from pending map")
		}
		rss = append(rss, rs)
		if okctx == ctx {
			found = true
			i = idx
			break
		}
	}
	if !found {
		return nil
	}
	ro.queue = ro.queue[i+1:]
	for _, rs := range rss {
		delete(ro.pending, string(rs.req.Context))
	}
	return rss
}
