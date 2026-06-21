# Raft — implementation + live interactive visualizer

A real, from-scratch implementation of the [Raft consensus protocol](https://raft.github.io/)
in Go, paired with an interactive web UI that lets you *watch* and *poke at* a
live cluster: leader election, log replication, a replicated key-value store, and
hands-on experiments with network partitions and the CAP theorem.

Two things are true at once here:

1. The consensus core is a **genuine, importable Raft library** — transport- and
   storage-agnostic, modelled on etcd/raft's `Ready`/`Advance` design.
2. The visualizer runs a whole cluster **inside one process over a controllable,
   in-memory network**, so you can inject partitions, drop packets, crash nodes,
   add/remove nodes, and slow down or single-step logical time — and see exactly
   how Raft reacts.

The same core powers both: the demo just plugs in a simulated transport instead
of a real one.

```
┌─────────────────────────────────────────────────────────┐
│  browser (React + Canvas)   ◄── WebSocket ──►   Go server │
└─────────────────────────────────────────────────────────┘
                                    │  events / commands
                          ┌─────────▼──────────┐
                          │  sim.Cluster        │  single goroutine,
                          │  clock · bus · net  │  deterministic
                          └─────────┬──────────┘
                          N × ┌──────▼──────┐
                              │ raft.RawNode │  ← the real consensus core
                              │  + KV FSM    │     (stdlib-only, importable)
                              └─────────────┘
```

## Quick start

Requirements: Go 1.26+, Node 18+ / npm.

```sh
make run            # builds the UI, embeds it, builds the binary, serves :8080
# then open http://localhost:8080
```

Or develop with hot reload (two terminals):

```sh
make dev            # Go backend on :8080 (serves /ws)
make dev-ui         # Vite dev server on http://localhost:5173 (proxies /ws)
```

Flags: `./bin/raftdemo -addr :8080 -nodes 3 -seed 1`.

Prometheus metrics are exposed at `http://localhost:8080/metrics` (see
[Metrics](#metrics)).

## What to try in the UI

The layout is **key-value store + node detail on the left**, the **cluster graph
in the center**, and **controls + event timeline on the right**. The header shows
a live cluster-health pill (Healthy / Electing / Partitioned / No quorum /
Degraded) with leader, term and quorum, and the **ⓘ** button opens a primer on
Raft with links to the paper and production implementations.

- **Watch an election.** Hit *Run*. A follower times out, campaigns (pre-vote →
  vote), and becomes leader. Messages animate along the edges, color-coded by
  type. The event timeline narrates it.
- **Replicate a write.** In *Key-Value Store*, `PUT x=1` → leader. Watch the
  entry append, replicate, commit, and apply across every node's log and KV.
- **Break the network (CAP).** *Isolate leader* splits the cluster. The minority
  side can't reach quorum and stops committing (consistency over availability —
  Raft is **CP**); the majority elects a new leader. Do a non-`linearizable`
  *GET* against a stranded follower and watch the divergence matrix light up red.
  Then *Heal* and watch it reconcile.
- **Crash & recover.** Click a node → *Crash* (RAM is lost, its "disk" survives).
  A new leader takes over. *Restart* it and watch it rejoin and recover its
  committed prefix.
- **Grow the cluster.** *+ Add node* joins a new member as a learner via a
  snapshot of committed state, then a configuration change admits it. *Promote*
  it to a voter, or *Remove* a node.
- **Bend time.** Pause and *Step* one tick at a time, or drag the speed slider.
  Outcomes are identical at any speed — the clock is logical.

## Using it as a library

The `raft` package is the consensus core. You drive it: give it ticks, messages
and proposals; it hands back a `Ready` batch describing what to persist, send and
apply. This is the same loop the simulator and a real server would both run.

```go
storage := raft.NewMemoryStorage()
node, _ := raft.NewRawNode(&raft.Config{
    ID: 1, ElectionTick: 10, HeartbeatTick: 1,
    Storage: storage, PreVote: true, CheckQuorum: true,
}, []uint64{1, 2, 3})

for {
    select {
    case <-ticker:
        node.Tick()
    case m := <-inbound:
        node.Step(m)
    default:
        if node.HasReady() {
            rd := node.Ready()
            storage.SetHardState(rd.HardState)   // 1. persist...
            storage.Append(rd.Entries)
            transport.Send(rd.Messages)          // 2. ...then send
            for _, e := range rd.CommittedEntries {
                apply(e)                         // 3. apply to your FSM
            }
            node.Advance(rd)                     // 4. acknowledge
        }
    }
}
```

Key interfaces (in `raft/`): `Storage` (read log/snapshot), and your own
transport (send `rd.Messages`) and state machine (apply `rd.CommittedEntries`).
A ready-made KV state machine lives in `kvstore/`.

## Features

Core protocol: leader election, log replication, the commit rule, and all the
safety properties — plus:

- **Pre-vote** and **check-quorum** (no disruptive elections on partition heal; a
  partitioned leader steps down).
- **Linearizable reads** via ReadIndex (with leader confirmation), plus an
  explicit *stale read* mode to demonstrate the consistency trade-off.
- **Single-server membership changes** (add learner → promote → remove), with new
  members bootstrapped from a snapshot.
- **Crash/restart** with state surviving on a per-node "disk".
- **Snapshot install** path (`MsgSnap`) for catching up new/slow members.

## Metrics

The demo exposes Prometheus metrics at **`/metrics`** so you can scrape the
cluster with Prometheus + Grafana while you drive it in the UI. Metrics live in
the `metrics/` package (the pure `raft/` core stays dependency-free); the
simulation increments them as it observes events, and per-node gauges are
rendered from an atomic snapshot at scrape time.

```sh
curl -s localhost:8080/metrics | grep '^raft_'
```

**Event counters** — `raft_elections_started_total{node}`,
`raft_prevotes_started_total{node}`, `raft_leaders_elected_total{node}`,
`raft_term_changes_total{node}`, `raft_messages_sent_total{node,type}` (type =
`MsgHeartbeat`, `MsgApp`, `MsgVote`, `MsgPreVote`, `MsgSnap`, …),
`raft_messages_dropped_total{type}`, `raft_proposals_total`,
`raft_committed_entries_total`, `raft_conf_changes_total{type}`,
`raft_snapshots_created_total{node}`, `raft_snapshots_installed_total{node}`,
`raft_reads_total{mode}` (linearizable|stale), `raft_node_crashes_total`,
`raft_node_restarts_total`.

**Histograms (native)** — exposed as Prometheus *native (sparse) histograms*,
with classic buckets retained for the text format:
`raft_election_duration_ticks` (campaign → leader),
`raft_commit_latency_ticks` (write appended → committed),
`raft_message_delivery_ticks` (network in-flight time), and
`raft_append_batch_entries` (entries per `MsgApp`).

Native histograms are only carried over the **protobuf** exposition format, which
the endpoint serves via content negotiation — pass the protobuf `Accept` header:

```sh
curl -s -H 'Accept: application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited' \
  localhost:8080/metrics -o /dev/null -w '%{content_type}\n'
# application/vnd.google.protobuf; ...
```

Prometheus scrapes native histograms automatically when native histograms are
enabled in its config; plain `curl` (text format) still shows the classic buckets.

**Gauges** — `raft_nodes`, `raft_tick`, `raft_leader_id`, `raft_partitioned`,
`raft_inflight_messages`, `raft_kv_keys`, and per-node `raft_node_term{node}`,
`raft_commit_index{node}`, `raft_applied_index{node}`, `raft_last_index{node}`,
`raft_log_length{node}`, `raft_node_crashed{node}`, `raft_node_is_leader{node}`.

Example PromQL: heartbeat rate `rate(raft_messages_sent_total{type="MsgHeartbeat"}[1m])`,
replication lag `max(raft_commit_index) - min(raft_commit_index)`.

## Project layout

| Path           | What it is                                                       |
| -------------- | --------------------------------------------------------------- |
| `raft/`        | the consensus core — stdlib-only, importable                    |
| `kvstore/`     | the replicated key-value state machine (FSM)                    |
| `sim/`         | the single-goroutine deterministic cluster + in-memory network  |
| `metrics/`     | Prometheus instruments for Raft internals (`/metrics`)          |
| `server/`      | WebSocket hub + HTTP server + embedded UI                       |
| `webui/`       | React + TypeScript + Vite frontend (Canvas graph)               |
| `cmd/raftdemo` | the visualizer binary                                           |

## Testing

```sh
make test           # unit + integration tests across all packages
make test-race      # with the race detector
make fuzz           # 25 randomized scenarios, checking safety invariants
```

The tick-based, single-goroutine, seeded design makes everything
**deterministic and flake-free**. The fuzz test throws random partitions,
crashes, restarts and packet loss at the cluster and asserts the two pillars of
Raft safety on every tick — *at most one leader per term*, and *committed entries
never diverge across nodes*. A failure prints a seed that reproduces it exactly.

## Design notes

- **Pure core.** `raft/` has no goroutines, no I/O, no `time.Now()`. It is a
  state machine; the surrounding "driver" owns scheduling and the outside world.
  This is what makes it both reusable and testable.
- **Determinism is a feature.** One goroutine, one seeded RNG, sorted iteration,
  and a `(tick, seq)`-ordered message bus mean a given seed + command sequence
  always produces the same run.
- **Safety contract.** The driver persists `HardState`/`Entries` *before* sending
  messages and applies only committed entries — the ordering Raft relies on.

## License

MIT (or your choice).
