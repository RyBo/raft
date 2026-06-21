package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsExposition(t *testing.T) {
	m := New()

	m.IncElection(2)
	m.IncLeaderElected(2)
	m.IncMessageSent(1, "MsgHeartbeat")
	m.IncMessageSent(1, "MsgApp")
	m.IncMessageDropped("MsgApp")
	m.IncProposal()
	m.IncCommitted()
	m.IncSnapshotCreated(3)
	m.IncSnapshotInstalled(3)
	m.IncConfChange("add_learner")
	m.IncRead("linearizable")
	m.IncRead("stale")
	m.IncCrash()
	m.IncRestart()
	m.ObserveElectionDuration(7)

	m.SetSnapshot(Snapshot{
		Tick: 42, Leader: 2, Nodes: 3, Inflight: 5, Partitioned: true, KVKeys: 4,
		Per: []NodeGauge{
			{ID: 1, Term: 3, Commit: 10, Applied: 10, LastIndex: 11, LogLen: 11, IsLeader: false},
			{ID: 2, Term: 3, Commit: 10, Applied: 10, LastIndex: 11, LogLen: 11, IsLeader: true},
		},
	})

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	out := string(b)

	wants := []string{
		`raft_elections_started_total{node="2"} 1`,
		`raft_leaders_elected_total{node="2"} 1`,
		`raft_messages_sent_total{node="1",type="MsgHeartbeat"} 1`,
		`raft_messages_dropped_total{type="MsgApp"} 1`,
		`raft_proposals_total 1`,
		`raft_committed_entries_total 1`,
		`raft_snapshots_created_total{node="3"} 1`,
		`raft_snapshots_installed_total{node="3"} 1`,
		`raft_conf_changes_total{type="add_learner"} 1`,
		`raft_reads_total{mode="linearizable"} 1`,
		`raft_reads_total{mode="stale"} 1`,
		`raft_node_crashes_total 1`,
		`raft_node_restarts_total 1`,
		"raft_election_duration_ticks_bucket",
		"raft_nodes 3",
		"raft_tick 42",
		"raft_leader_id 2",
		"raft_partitioned 1",
		"raft_inflight_messages 5",
		"raft_kv_keys 4",
		`raft_node_term{node="1"} 3`,
		`raft_node_is_leader{node="2"} 1`,
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("metrics output missing %q", w)
		}
	}
}
