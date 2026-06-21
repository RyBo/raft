// Package metrics exposes Raft cluster internals as Prometheus metrics. It lives
// in the driver layer (alongside sim/) so the pure raft/ core stays stdlib-only:
// the simulation increments these counters as it observes events, and a custom
// collector renders per-node gauges from an atomically-swapped snapshot at scrape
// time (so removed nodes leave no stale series).
package metrics

import (
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NodeGauge is the per-node scalar state rendered as gauges.
type NodeGauge struct {
	ID        uint64
	Term      uint64
	Commit    uint64
	Applied   uint64
	LastIndex uint64
	LogLen    uint64
	Crashed   bool
	IsLeader  bool
}

// Snapshot is the current cluster state used to render gauges at scrape time.
type Snapshot struct {
	Tick        uint64
	Leader      uint64
	Nodes       int
	Inflight    int
	Partitioned bool
	KVKeys      int
	Per         []NodeGauge
}

// Metrics holds all instruments plus the latest gauge snapshot.
type Metrics struct {
	reg *prometheus.Registry

	elections          *prometheus.CounterVec
	prevotes           *prometheus.CounterVec
	leadersElected     *prometheus.CounterVec
	termChanges        *prometheus.CounterVec
	messagesSent       *prometheus.CounterVec
	messagesDropped    *prometheus.CounterVec
	confChanges        *prometheus.CounterVec
	snapshotsCreated   *prometheus.CounterVec
	snapshotsInstalled *prometheus.CounterVec
	reads              *prometheus.CounterVec
	proposals          prometheus.Counter
	committed          prometheus.Counter
	crashes            prometheus.Counter
	restarts           prometheus.Counter
	electionDuration   prometheus.Histogram

	snap atomic.Pointer[Snapshot]
}

// New creates a Metrics with its own registry and registers all instruments.
func New() *Metrics {
	m := &Metrics{reg: prometheus.NewRegistry()}

	cv := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
		m.reg.MustRegister(c)
		return c
	}
	c := func(name, help string) prometheus.Counter {
		cc := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
		m.reg.MustRegister(cc)
		return cc
	}

	m.elections = cv("raft_elections_started_total", "Elections started (node became candidate).", "node")
	m.prevotes = cv("raft_prevotes_started_total", "Pre-vote campaigns started.", "node")
	m.leadersElected = cv("raft_leaders_elected_total", "Times a node became leader.", "node")
	m.termChanges = cv("raft_term_changes_total", "Term increments observed per node.", "node")
	m.messagesSent = cv("raft_messages_sent_total", "Raft messages sent, by sender and type.", "node", "type")
	m.messagesDropped = cv("raft_messages_dropped_total", "Messages dropped by the network (partition/loss), by type.", "type")
	m.confChanges = cv("raft_conf_changes_total", "Membership changes applied, by type.", "type")
	m.snapshotsCreated = cv("raft_snapshots_created_total", "Snapshots created (log compaction), per node.", "node")
	m.snapshotsInstalled = cv("raft_snapshots_installed_total", "Snapshots installed via InstallSnapshot, per node.", "node")
	m.reads = cv("raft_reads_total", "KV reads served, by mode (linearizable|stale).", "mode")

	m.proposals = c("raft_proposals_total", "Client write proposals submitted.")
	m.committed = c("raft_committed_entries_total", "Normal log entries committed and applied to the KV store.")
	m.crashes = c("raft_node_crashes_total", "Node crashes triggered.")
	m.restarts = c("raft_node_restarts_total", "Node restarts triggered.")

	m.electionDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "raft_election_duration_ticks",
		Help:    "Ticks from becoming candidate to becoming leader.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128},
	})
	m.reg.MustRegister(m.electionDuration)

	m.reg.MustRegister(&gaugeCollector{m: m})
	return m
}

// Handler returns the Prometheus exposition handler for this registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// SetSnapshot atomically publishes the latest gauge snapshot.
func (m *Metrics) SetSnapshot(s Snapshot) { m.snap.Store(&s) }

func node(id uint64) string { return strconv.FormatUint(id, 10) }

// Increment helpers (all goroutine-safe via the prometheus client).
func (m *Metrics) IncElection(id uint64)      { m.elections.WithLabelValues(node(id)).Inc() }
func (m *Metrics) IncPreVote(id uint64)       { m.prevotes.WithLabelValues(node(id)).Inc() }
func (m *Metrics) IncLeaderElected(id uint64) { m.leadersElected.WithLabelValues(node(id)).Inc() }
func (m *Metrics) IncTermChange(id uint64)    { m.termChanges.WithLabelValues(node(id)).Inc() }
func (m *Metrics) IncMessageSent(id uint64, t string) {
	m.messagesSent.WithLabelValues(node(id), t).Inc()
}
func (m *Metrics) IncMessageDropped(t string)   { m.messagesDropped.WithLabelValues(t).Inc() }
func (m *Metrics) IncConfChange(t string)       { m.confChanges.WithLabelValues(t).Inc() }
func (m *Metrics) IncSnapshotCreated(id uint64) { m.snapshotsCreated.WithLabelValues(node(id)).Inc() }
func (m *Metrics) IncSnapshotInstalled(id uint64) {
	m.snapshotsInstalled.WithLabelValues(node(id)).Inc()
}
func (m *Metrics) IncRead(mode string)                   { m.reads.WithLabelValues(mode).Inc() }
func (m *Metrics) IncProposal()                          { m.proposals.Inc() }
func (m *Metrics) IncCommitted()                         { m.committed.Inc() }
func (m *Metrics) IncCrash()                             { m.crashes.Inc() }
func (m *Metrics) IncRestart()                           { m.restarts.Inc() }
func (m *Metrics) ObserveElectionDuration(ticks float64) { m.electionDuration.Observe(ticks) }

// --- gauge collector ---

var (
	descNodes       = prometheus.NewDesc("raft_nodes", "Number of nodes currently in the cluster.", nil, nil)
	descTick        = prometheus.NewDesc("raft_tick", "Current logical tick.", nil, nil)
	descLeader      = prometheus.NewDesc("raft_leader_id", "Current leader node id (0 if none).", nil, nil)
	descPartitioned = prometheus.NewDesc("raft_partitioned", "1 if a network partition is active, else 0.", nil, nil)
	descInflight    = prometheus.NewDesc("raft_inflight_messages", "Messages currently in flight on the bus.", nil, nil)
	descKVKeys      = prometheus.NewDesc("raft_kv_keys", "Number of keys in the replicated KV store.", nil, nil)

	nodeLabels   = []string{"node"}
	descTermG    = prometheus.NewDesc("raft_node_term", "Current term, per node.", nodeLabels, nil)
	descCommit   = prometheus.NewDesc("raft_commit_index", "Commit index, per node.", nodeLabels, nil)
	descApplied  = prometheus.NewDesc("raft_applied_index", "Applied index, per node.", nodeLabels, nil)
	descLast     = prometheus.NewDesc("raft_last_index", "Last log index, per node.", nodeLabels, nil)
	descLogLen   = prometheus.NewDesc("raft_log_length", "Retained log length, per node.", nodeLabels, nil)
	descCrashed  = prometheus.NewDesc("raft_node_crashed", "1 if the node is crashed, else 0.", nodeLabels, nil)
	descIsLeader = prometheus.NewDesc("raft_node_is_leader", "1 if the node is the leader, else 0.", nodeLabels, nil)
)

type gaugeCollector struct{ m *Metrics }

func (g *gaugeCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		descNodes, descTick, descLeader, descPartitioned, descInflight, descKVKeys,
		descTermG, descCommit, descApplied, descLast, descLogLen, descCrashed, descIsLeader,
	} {
		ch <- d
	}
}

func (g *gaugeCollector) Collect(ch chan<- prometheus.Metric) {
	s := g.m.snap.Load()
	if s == nil {
		return
	}
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	gauge(descNodes, float64(s.Nodes))
	gauge(descTick, float64(s.Tick))
	gauge(descLeader, float64(s.Leader))
	gauge(descPartitioned, b2f(s.Partitioned))
	gauge(descInflight, float64(s.Inflight))
	gauge(descKVKeys, float64(s.KVKeys))
	for _, n := range s.Per {
		id := node(n.ID)
		gauge(descTermG, float64(n.Term), id)
		gauge(descCommit, float64(n.Commit), id)
		gauge(descApplied, float64(n.Applied), id)
		gauge(descLast, float64(n.LastIndex), id)
		gauge(descLogLen, float64(n.LogLen), id)
		gauge(descCrashed, b2f(n.Crashed), id)
		gauge(descIsLeader, b2f(n.IsLeader), id)
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
