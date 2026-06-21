package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rybo/raft/metrics"
	"github.com/rybo/raft/sim"
)

// TestWebSocketEndToEnd drives the full stack: a real WebSocket client sends
// JSON commands, the cluster runs, and state events flow back.
func TestWebSocketEndToEnd(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)

	cluster := sim.NewCluster(3, 5, nil)
	hub := NewHub(cluster)
	m := metrics.New()
	cluster.SetEmit(hub.Emit)
	cluster.SetMetrics(m)
	go hub.Run(stop)
	go cluster.Run(stop)

	srv := httptest.NewServer(Handler(cluster, hub, m.Handler()))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Speed the clock up so the test is quick.
	send(t, conn, sim.Command{Type: "clock", Action: "setSpeed", MsPerTick: 20})

	// Wait for a leader.
	if !waitState(t, conn, 8*time.Second, func(s *sim.StateEvent) bool {
		n := 0
		for _, nd := range s.Nodes {
			if nd.Role == "leader" {
				n++
			}
		}
		return n == 1
	}) {
		t.Fatal("no leader via WebSocket")
	}

	// Write a key and confirm it replicates everywhere.
	send(t, conn, sim.Command{Type: "kv", Op: "put", Key: "hello", Value: "world", Target: "leader", ReqID: "1"})
	if !waitState(t, conn, 5*time.Second, func(s *sim.StateEvent) bool {
		if len(s.Nodes) != 3 {
			return false
		}
		for _, nd := range s.Nodes {
			ok := false
			for _, p := range nd.KV {
				if p.Key == "hello" && p.Value == "world" {
					ok = true
				}
			}
			if !ok {
				return false
			}
		}
		return true
	}) {
		t.Fatal("write did not replicate via WebSocket")
	}

	// Scrape /metrics and confirm Raft internals are exported with sane values.
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	for _, want := range []string{
		"raft_leaders_elected_total",
		"raft_messages_sent_total",
		"raft_committed_entries_total",
		"raft_node_term{node=",
		"raft_nodes 3",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("/metrics output missing %q", want)
		}
	}
}

func send(t *testing.T, conn *websocket.Conn, cmd sim.Command) {
	t.Helper()
	b, _ := json.Marshal(cmd)
	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func waitState(t *testing.T, conn *websocket.Conn, timeout time.Duration, cond func(*sim.StateEvent) bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	_ = conn.SetReadDeadline(deadline)
	for time.Now().Before(deadline) {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return false
		}
		var probe struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(msg, &probe) != nil || probe.Type != "state" {
			continue
		}
		var s sim.StateEvent
		if json.Unmarshal(msg, &s) != nil {
			continue
		}
		if cond(&s) {
			return true
		}
	}
	return false
}
