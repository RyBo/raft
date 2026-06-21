package sim

import (
	"container/heap"

	"github.com/rybo/raft/raft"
)

// inFlight is a message scheduled for future delivery.
type inFlight struct {
	deliverTick uint64
	seq         uint64 // monotonic tiebreaker for total deterministic ordering
	msg         raft.Message
}

// msgHeap is a min-heap ordered by (deliverTick, seq).
type msgHeap []inFlight

func (h msgHeap) Len() int { return len(h) }
func (h msgHeap) Less(i, j int) bool {
	if h[i].deliverTick != h[j].deliverTick {
		return h[i].deliverTick < h[j].deliverTick
	}
	return h[i].seq < h[j].seq
}
func (h msgHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *msgHeap) Push(x any)   { *h = append(*h, x.(inFlight)) }
func (h *msgHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// bus is the in-memory network: a priority queue of in-flight messages keyed by
// delivery tick. Total ordering by (deliverTick, seq) keeps delivery
// deterministic even when two messages share a tick.
type bus struct {
	pq msgHeap
}

func newBus() *bus { return &bus{} }

func (b *bus) schedule(deliverTick, seq uint64, msg raft.Message) {
	heap.Push(&b.pq, inFlight{deliverTick: deliverTick, seq: seq, msg: msg})
}

// due pops and returns all messages whose delivery tick is <= now, in order.
func (b *bus) due(now uint64) []raft.Message {
	var out []raft.Message
	for b.pq.Len() > 0 && b.pq[0].deliverTick <= now {
		it := heap.Pop(&b.pq).(inFlight)
		out = append(out, it.msg)
	}
	return out
}

// dropTo removes all in-flight messages addressed to a node (used on crash).
func (b *bus) dropTo(id uint64) {
	filtered := b.pq[:0]
	for _, it := range b.pq {
		if it.msg.To != id && it.msg.From != id {
			filtered = append(filtered, it)
		}
	}
	b.pq = filtered
	heap.Init(&b.pq)
}

func (b *bus) clear() { b.pq = nil }

// len returns the number of messages currently in flight.
func (b *bus) len() int { return b.pq.Len() }
