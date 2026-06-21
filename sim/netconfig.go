package sim

// linkKey identifies a directed link between two nodes.
type linkKey struct{ from, to uint64 }

// netConfig models the simulated network: partitions, base latency/drop, and
// per-directed-link overrides. It is owned by the Cluster's single goroutine.
type netConfig struct {
	// group maps a node to its partition group index; nodes in different groups
	// cannot exchange messages. Empty map => fully connected.
	group map[uint64]int

	baseLatency int     // ticks for a message to traverse a link
	jitter      int     // additional uniform jitter in [0, jitter]
	dropRate    float64 // probability a message is dropped
	dupRate     float64 // probability a delivered message is duplicated

	links map[linkKey]LinkView // per-link overrides
}

func newNetConfig() *netConfig {
	return &netConfig{
		group:       map[uint64]int{},
		baseLatency: 2,
		jitter:      1,
		dropRate:    0,
		dupRate:     0,
		links:       map[linkKey]LinkView{},
	}
}

// connected reports whether a and b are in the same partition.
func (n *netConfig) connected(a, b uint64) bool {
	ga, oka := n.group[a]
	gb, okb := n.group[b]
	if !oka || !okb {
		return true
	}
	return ga == gb
}

// setPartition assigns nodes to groups. Nodes not listed are fully connected.
func (n *netConfig) setPartition(groups [][]uint64) {
	n.group = map[uint64]int{}
	for gi, g := range groups {
		for _, id := range g {
			n.group[id] = gi
		}
	}
}

// latencyFor returns the configured base latency for a link (override or base).
func (n *netConfig) latencyFor(from, to uint64) int {
	if lv, ok := n.links[linkKey{from, to}]; ok {
		return lv.Latency
	}
	return n.baseLatency
}

// dropFor returns the configured drop rate for a link (override or base).
func (n *netConfig) dropFor(from, to uint64) float64 {
	if lv, ok := n.links[linkKey{from, to}]; ok {
		return lv.Drop
	}
	return n.dropRate
}

// view returns a serializable snapshot of the network configuration.
func (n *netConfig) view(ids []uint64) NetView {
	// Reconstruct partition groups in deterministic order.
	groupMembers := map[int][]uint64{}
	for _, id := range ids {
		if gi, ok := n.group[id]; ok {
			groupMembers[gi] = append(groupMembers[gi], id)
		}
	}
	// Initialize as non-nil so JSON emits [] rather than null (the UI iterates
	// these directly).
	parts := [][]uint64{}
	maxG := -1
	for gi := range groupMembers {
		if gi > maxG {
			maxG = gi
		}
	}
	for gi := 0; gi <= maxG; gi++ {
		if members, ok := groupMembers[gi]; ok {
			parts = append(parts, members)
		}
	}
	links := []LinkView{}
	for _, lv := range n.links {
		links = append(links, lv)
	}
	return NetView{
		Partitions:  parts,
		BaseLatency: n.baseLatency,
		DropRate:    n.dropRate,
		Links:       links,
	}
}
