# Raft: a Go implementation with a live visualizer

An implementation of the [Raft consensus protocol](https://raft.github.io/) in
Go, with an interactive web UI for watching a cluster run: leader election, log
replication, a replicated key-value store, and experiments with network
partitions and the CAP theorem.

The project has two parts:

1. A consensus core that works as an importable library. It is transport and
   storage agnostic, modelled on the etcd/raft `Ready`/`Advance` design.
2. A visualizer that runs a whole cluster inside one process over a controllable
   in-memory network. You can inject partitions, drop packets, crash nodes, add
   or remove nodes, and slow down or single-step logical time.

Both use the same core. The visualizer plugs in a simulated transport in place of
a real one.

```
┌─────────────────────────────────────────────────────────┐
│  browser (React + Canvas)   <── WebSocket ──>   Go server │
└─────────────────────────────────────────────────────────┘
                                    │  events / commands
                          ┌─────────▼──────────┐
                          │  sim.Cluster        │  single goroutine,
                          │  clock · bus · net  │  deterministic
                          └─────────┬──────────┘
                          N × ┌──────▼──────┐
                              │ raft.RawNode │  the consensus core
                              │  + KV FSM    │  (stdlib-only, importable)
                              └─────────────┘
```

## Quick start

Requirements: Go 1.26+, Node 18+ and npm.

```sh
make run            # builds the UI, embeds it, builds the binary, serves :8080
# then open http://localhost:8080
```

To develop with hot reload, use two terminals:

```sh
make dev            # Go backend on :8080 (serves /ws)
make dev-ui         # Vite dev server on http://localhost:5173 (proxies /ws)
```

Flags: `./bin/raftdemo -addr :8080 -nodes 3 -seed 1`.

Prometheus metrics are served at `http://localhost:8080/metrics` (see
[Metrics](#metrics)).

## What to try in the UI

The layout puts the key-value store and node detail on the left, the cluster
graph in the center, and the controls and event timeline on the right. The header
shows a cluster-health pill (Healthy, Electing, Partitioned, No quorum, or
Degraded) with the leader, term, and quorum. The info button opens a short
explanation of Raft with links to the paper and to production implementations.

- Watch an election. Press Run. A follower times out, campaigns (pre-vote then
  vote), and becomes leader. Messages animate along the edges, colored by type,
  and the event timeline records what happens.
- Replicate a write. In the Key-Value Store panel, send `PUT x=1` to the leader
  and watch the entry append, replicate, commit, and apply across every node's
  log and KV.
- Break the network. Use Isolate leader to split the cluster. The minority side
  cannot reach a quorum and stops committing, so Raft favors consistency over
  availability (it is CP). The majority elects a new leader. Run a
  non-linearizable GET against a stranded follower and the divergence matrix
  highlights the stale value. Use Heal to reconnect and watch the logs reconcile.
- Crash and recover. Click a node and choose Crash. Its memory is lost but its
  disk survives. A new leader takes over. Restart it and watch it rejoin and
  recover its committed entries.
- Grow the cluster. Add node joins a new member as a learner using a snapshot of
  committed state, then a configuration change admits it. Promote it to a voter,
  or Remove a node.
- Control time. Pause and Step one tick at a time, or drag the speed slider. The
  clock is logical, so the outcome is the same at any speed.

## Using it as a library

The `raft` package is the consensus core. You drive it by giving it ticks,
messages, and proposals. It returns a `Ready` batch describing what to persist,
send, and apply. The simulator and a real server both run the same loop.

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
            storage.SetHardState(rd.HardState)   // 1. persist
            storage.Append(rd.Entries)
            transport.Send(rd.Messages)          // 2. then send
            for _, e := range rd.CommittedEntries {
                apply(e)                         // 3. apply to your FSM
            }
            node.Advance(rd)                     // 4. acknowledge
        }
    }
}
```

The key interfaces live in `raft/`: `Storage` reads the log and snapshot, while
your own transport sends `rd.Messages` and your own state machine applies
`rd.CommittedEntries`. A ready-made KV state machine lives in `kvstore/`.

## Features

Core protocol: leader election, log replication, the commit rule, and the safety
properties. On top of that:

- Pre-vote and check-quorum, so a node returning from a partition does not force
  a needless election and a partitioned leader steps down.
- Linearizable reads through ReadIndex with leader confirmation, plus an explicit
  stale-read mode that shows the consistency trade-off.
- Single-server membership changes (add learner, promote, remove), with new
  members bootstrapped from a snapshot.
- Crash and restart, with state surviving on a per-node disk.
- Snapshot install (`MsgSnap`) for catching up new or slow members.

## Metrics

The demo serves Prometheus metrics at `/metrics` so you can scrape the cluster
with Prometheus and Grafana while you drive it in the UI. Metrics live in the
`metrics/` package, which keeps the `raft/` core dependency-free. The simulation
increments them as it observes events, and per-node gauges are rendered from an
atomic snapshot at scrape time.

```sh
curl -s localhost:8080/metrics | grep '^raft_'
```

Event counters: `raft_elections_started_total{node}`,
`raft_prevotes_started_total{node}`, `raft_leaders_elected_total{node}`,
`raft_term_changes_total{node}`, `raft_messages_sent_total{node,type}` (type is
`MsgHeartbeat`, `MsgApp`, `MsgVote`, `MsgPreVote`, `MsgSnap`, and so on),
`raft_messages_dropped_total{type}`, `raft_proposals_total`,
`raft_committed_entries_total`, `raft_conf_changes_total{type}`,
`raft_snapshots_created_total{node}`, `raft_snapshots_installed_total{node}`,
`raft_reads_total{mode}` (linearizable or stale), `raft_node_crashes_total`,
`raft_node_restarts_total`.

Histograms are exposed as Prometheus native (sparse) histograms, with classic
buckets kept for the text format:

- `raft_election_duration_ticks`: ticks from a node starting its campaign to
  becoming leader.
- `raft_commit_latency_ticks`: ticks from a write being appended on the leader to
  it being committed.
- `raft_message_delivery_ticks`: network in-flight time for delivered messages.
- `raft_append_batch_entries`: entries carried per `MsgApp`.

Native histograms are only carried over the protobuf exposition format, which the
endpoint serves through content negotiation. Pass the protobuf Accept header to
request it:

```sh
curl -s -H 'Accept: application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited' \
  localhost:8080/metrics -o /dev/null -w '%{content_type}\n'
# application/vnd.google.protobuf; ...
```

Prometheus scrapes native histograms automatically when they are enabled in its
config. Plain curl uses the text format and still shows the classic buckets.

Gauges: `raft_nodes`, `raft_tick`, `raft_leader_id`, `raft_partitioned`,
`raft_inflight_messages`, `raft_kv_keys`, and per-node `raft_node_term{node}`,
`raft_commit_index{node}`, `raft_applied_index{node}`, `raft_last_index{node}`,
`raft_log_length{node}`, `raft_node_crashed{node}`, `raft_node_is_leader{node}`.

Example PromQL: heartbeat rate
`rate(raft_messages_sent_total{type="MsgHeartbeat"}[1m])`, replication spread
`max(raft_commit_index) - min(raft_commit_index)`.

## Project layout

| Path           | What it is                                                      |
| -------------- | -------------------------------------------------------------- |
| `raft/`        | the consensus core, stdlib-only and importable                 |
| `kvstore/`     | the replicated key-value state machine (FSM)                   |
| `sim/`         | the single-goroutine deterministic cluster and in-memory network |
| `metrics/`     | Prometheus instruments for Raft internals (`/metrics`)         |
| `server/`      | WebSocket hub, HTTP server, and embedded UI                    |
| `webui/`       | React, TypeScript, and Vite frontend (Canvas graph)            |
| `cmd/raftdemo` | the visualizer binary                                          |

## Testing

```sh
make test           # unit and integration tests across all packages
make test-race      # with the race detector
make fuzz           # 25 randomized scenarios that check safety invariants
```

The tick-based, single-goroutine, seeded design keeps tests deterministic and
free of flakes. The fuzz test throws random partitions, crashes, restarts, and
packet loss at the cluster and checks the two main safety properties on every
tick: at most one leader per term, and committed entries never diverge across
nodes. A failure prints a seed that reproduces it exactly.

## Design notes

- Pure core. `raft/` has no goroutines, no I/O, and no `time.Now()`. It is a
  state machine, and the surrounding driver owns scheduling and the outside
  world. That is what makes it both reusable and testable.
- Determinism. One goroutine, one seeded RNG, sorted iteration, and a message bus
  ordered by `(tick, seq)` mean a given seed and command sequence always produce
  the same run.
- Safety contract. The driver persists `HardState` and `Entries` before sending
  messages, and applies only committed entries. That ordering is what Raft relies
  on.

## License

Apache License 2.0. See [LICENSE](LICENSE).
