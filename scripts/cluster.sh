#!/usr/bin/env bash
# Launch a local 3-node raftnode cluster over gRPC. Each node persists to its own
# data directory under $DATA (default ./data), so stopping and re-running this
# script recovers committed state from disk. Ctrl-C stops the whole cluster.
#
#   ./scripts/cluster.sh            # start 3 nodes
#   DATA=/tmp/raft ./scripts/cluster.sh
#   rm -rf ./data && ./scripts/cluster.sh   # start fresh
set -euo pipefail

cd "$(dirname "$0")/.."

BIN=bin/raftnode
DATA=${DATA:-./data}
PEERS="1@127.0.0.1:9001,2@127.0.0.1:9002,3@127.0.0.1:9003"

go build -o "$BIN" ./cmd/raftnode

pids=()
cleanup() {
  echo
  echo "stopping cluster..."
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

start_node() {
  local id=$1 raft=$2 client=$3
  "$BIN" -id "$id" -peers "$PEERS" \
    -raft-addr ":$raft" -client-addr ":$client" -data "$DATA/n$id" &
  local pid=$!
  pids+=("$pid")
  echo "node $id: raft :$raft  client http://127.0.0.1:$client  data $DATA/n$id  (pid $pid)"
}

echo "starting 3-node raft cluster (Ctrl-C to stop)"
start_node 1 9001 8001
start_node 2 9002 8002
start_node 3 9003 8003

cat <<'EOF'

try (writes must hit the leader; a follower replies 503 with the leader's id):
  curl -X PUT 127.0.0.1:8001/kv/foo -d bar
  curl 127.0.0.1:8002/kv/foo            # linearizable read
  curl '127.0.0.1:8003/kv/foo?stale=true'
  curl 127.0.0.1:8001/status
EOF

wait
