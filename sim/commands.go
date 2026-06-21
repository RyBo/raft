package sim

import (
	"fmt"
	"strconv"

	"github.com/rybo/raft/kvstore"
	"github.com/rybo/raft/raft"
)

// applyCommand mutates cluster state in response to a UI command. It always runs
// on the cluster goroutine.
func (c *Cluster) applyCommand(cmd Command) {
	switch cmd.Type {
	case "clock":
		c.cmdClock(cmd)
	case "kv":
		c.cmdKV(cmd)
	case "partition":
		c.net.setPartition(cmd.Groups)
		c.logEvent("partition", 0, 0, fmt.Sprintf("network partition set: %v", cmd.Groups))
		c.emitState()
	case "net":
		c.cmdNet(cmd)
	case "node":
		c.cmdNode(cmd)
	case "addNode":
		c.addNode(false)
	case "addLearner":
		c.addNode(true)
	case "reset":
		c.cmdReset(cmd)
	}
}

func (c *Cluster) cmdClock(cmd Command) {
	switch cmd.Action {
	case "run":
		c.running = true
	case "pause":
		c.running = false
	case "step":
		c.stepTick()
		return
	case "setSpeed":
		ms := cmd.MsPerTick
		if ms < 20 {
			ms = 20
		}
		if ms > 2000 {
			ms = 2000
		}
		c.msPerTick = ms
	}
	c.emitState()
}

func (c *Cluster) cmdNet(cmd Command) {
	if cmd.Latency > 0 {
		c.net.baseLatency = cmd.Latency
	}
	drop := cmd.Drop
	if drop < 0 {
		drop = 0
	}
	if drop > 1 {
		drop = 1
	}
	c.net.dropRate = drop
	c.logEvent("net", 0, 0, fmt.Sprintf("network: latency=%d drop=%.2f", c.net.baseLatency, c.net.dropRate))
	c.emitState()
}

func (c *Cluster) cmdNode(cmd Command) {
	p := c.peers[cmd.ID]
	if p == nil {
		return
	}
	switch cmd.Action {
	case "crash":
		p.crashed = true
		c.bus.dropTo(cmd.ID)
		c.prevRole[cmd.ID] = ""
		if c.metrics != nil {
			c.metrics.IncCrash()
		}
		c.logEvent("node_crashed", cmd.ID, 0, fmt.Sprintf("node %d crashed (RAM lost, disk kept)", cmd.ID))
		c.emitState()
	case "restart":
		if !p.crashed {
			return
		}
		p.restart(peerConfig{
			electionTick:  c.electionTick,
			heartbeatTick: c.heartbeatTick,
			preVote:       c.preVote,
			checkQuorum:   c.checkQuorum,
			seed:          c.seed + int64(cmd.ID)*7919,
		})
		if c.metrics != nil {
			c.metrics.IncRestart()
		}
		c.logEvent("node_restarted", cmd.ID, 0, fmt.Sprintf("node %d restarted from disk", cmd.ID))
		c.emitState()
	case "remove":
		c.proposeConf(raft.ConfChange{Type: raft.ConfChangeRemoveNode, NodeID: cmd.ID})
	case "promote":
		c.proposeConf(raft.ConfChange{Type: raft.ConfChangePromoteLearner, NodeID: cmd.ID})
	}
}

func (c *Cluster) cmdReset(cmd Command) {
	n := cmd.N
	if n <= 0 {
		n = len(c.order)
	}
	if cmd.Seed != 0 {
		c.seed = cmd.Seed
	}
	c.running = true
	c.build(n)
	c.logEvent("reset", 0, 0, fmt.Sprintf("cluster reset to %d nodes (seed %d)", n, c.seed))
	c.emitState()
}

// cmdKV handles client key-value traffic.
func (c *Cluster) cmdKV(cmd Command) {
	target := c.resolveTarget(cmd.Target)

	switch cmd.Op {
	case "put", "delete":
		if target == 0 {
			c.kvFail(cmd, "no reachable node for write")
			return
		}
		op := kvstore.OpPut
		if cmd.Op == "delete" {
			op = kvstore.OpDelete
		}
		data := kvstore.Command{Op: op, Key: cmd.Key, Value: cmd.Value}.Encode()
		_ = c.peers[target].node.Propose(data)
		if c.metrics != nil {
			c.metrics.IncProposal()
		}
		c.send(KVResultEvent{
			Type: "kvResult", ReqID: cmd.ReqID, OK: true, Op: cmd.Op,
			Key: cmd.Key, Value: cmd.Value, ServedBy: target,
			Note: "proposed to node " + strconv.FormatUint(target, 10) + " (forwarded to leader)",
		})
		c.logEvent("client_write", target, 0, fmt.Sprintf("%s %s=%s via node %d", cmd.Op, cmd.Key, cmd.Value, target))

	case "get":
		if cmd.Linearizable {
			leader := c.leaderID()
			if leader == 0 {
				c.kvFail(cmd, "no leader for linearizable read")
				return
			}
			ctx := []byte("r:" + cmd.ReqID)
			c.pendingReads[string(ctx)] = pendingRead{reqID: cmd.ReqID, key: cmd.Key, nodeID: leader}
			_ = c.peers[leader].node.ReadIndex(ctx)
			if c.metrics != nil {
				c.metrics.IncRead("linearizable")
			}
			c.logEvent("client_read", leader, 0, fmt.Sprintf("linearizable get %s via leader %d", cmd.Key, leader))
		} else {
			// Stale read straight from the target node's local applied state.
			if target == 0 {
				c.kvFail(cmd, "no such node for read")
				return
			}
			val, found := c.peers[target].kv.Get(cmd.Key)
			if c.metrics != nil {
				c.metrics.IncRead("stale")
			}
			c.send(KVResultEvent{
				Type: "kvResult", ReqID: cmd.ReqID, OK: true, Op: "get",
				Key: cmd.Key, Value: val, Found: found, ServedBy: target, Linearizable: false,
				Note: "stale read from node " + strconv.FormatUint(target, 10) + " (may lag the leader)",
			})
			c.logEvent("stale_read", target, 0, fmt.Sprintf("stale get %s from node %d", cmd.Key, target))
		}
	}
}

func (c *Cluster) kvFail(cmd Command, note string) {
	c.send(KVResultEvent{Type: "kvResult", ReqID: cmd.ReqID, OK: false, Op: cmd.Op, Key: cmd.Key, Note: note})
}

// resolveTarget maps a target string ("leader" or a node id) to a node id,
// returning 0 if unavailable.
func (c *Cluster) resolveTarget(target string) uint64 {
	if target == "" || target == "leader" {
		return c.leaderID()
	}
	id, err := strconv.ParseUint(target, 10, 64)
	if err != nil {
		return 0
	}
	if p, ok := c.peers[id]; !ok || p.crashed {
		return 0
	}
	return id
}

func (c *Cluster) proposeConf(cc raft.ConfChange) {
	leader := c.leaderID()
	if leader == 0 {
		c.logEvent("error", 0, 0, "no leader; cannot change membership")
		return
	}
	_ = c.peers[leader].node.ProposeConfChange(cc)
	c.logEvent("conf_change_proposed", leader, 0, fmt.Sprintf("proposed %v node %d", cc.Type, cc.NodeID))
}

// addNode brings a new node online by handing it a snapshot of the current
// committed state, then proposes a configuration change to admit it. New nodes
// always join as learners first (or directly as voters if learner is false).
func (c *Cluster) addNode(learner bool) {
	if len(c.peers) >= maxNodes {
		c.logEvent("error", 0, 0, fmt.Sprintf("cannot exceed %d nodes", maxNodes))
		return
	}
	leader := c.leaderID()
	if leader == 0 {
		c.logEvent("error", 0, 0, "no leader; cannot add a node")
		return
	}
	lp := c.peers[leader]
	st := lp.node.Status()
	if st.Commit == 0 {
		c.logEvent("error", 0, 0, "nothing committed yet; try again once a leader is stable")
		return
	}
	term, err := lp.node.Term(st.Commit)
	if err != nil {
		c.logEvent("error", 0, 0, "could not snapshot leader state")
		return
	}

	newID := c.nextID
	c.nextID++

	data, _ := lp.kv.Snapshot()
	cs := lp.node.ConfState()
	cs.Learners = append(cs.Learners, newID)
	snap := raft.Snapshot{
		Data:     data,
		Metadata: raft.SnapshotMetadata{Index: st.Commit, Term: term, ConfState: cs},
	}

	c.peers[newID] = newPeerFromSnapshot(peerConfig{
		id:            newID,
		electionTick:  c.electionTick,
		heartbeatTick: c.heartbeatTick,
		preVote:       c.preVote,
		checkQuorum:   c.checkQuorum,
		seed:          c.seed + int64(newID)*7919,
	}, snap)
	c.recomputeOrder()

	t := raft.ConfChangeAddLearnerNode
	if !learner {
		t = raft.ConfChangeAddNode
	}
	_ = lp.node.ProposeConfChange(raft.ConfChange{Type: t, NodeID: newID})
	c.logEvent("node_added", newID, 0, fmt.Sprintf("node %d joining (snapshot at index %d), conf change proposed", newID, st.Commit))
	c.emitState()
}
