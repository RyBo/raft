// Command raftnode runs a single Raft node as a standalone process: durable
// storage on disk (walstore), a real gRPC transport to its peers, and the
// kvstore state machine, all driven by the node package. Launch several of these
// with the same -peers list and they form a cluster, elect a leader, replicate a
// key-value store, and recover committed state from disk after a restart.
//
// Example (three nodes on one machine):
//
//	raftnode -id 1 -peers 1@127.0.0.1:9001,2@127.0.0.1:9002,3@127.0.0.1:9003 \
//	         -raft-addr :9001 -client-addr :8001 -data ./data/n1
//
// Then: curl -X PUT 127.0.0.1:8001/kv/foo -d bar ; curl 127.0.0.1:8001/kv/foo
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rybo/raft/kvstore"
	"github.com/rybo/raft/node"
	"github.com/rybo/raft/raft"
	"github.com/rybo/raft/transport"
	"github.com/rybo/raft/walstore"
)

func main() {
	id := flag.Uint64("id", 0, "this node's ID (must appear in -peers)")
	peersFlag := flag.String("peers", "", "comma-separated id@host:port gRPC addresses of all nodes, e.g. 1@127.0.0.1:9001,2@127.0.0.1:9002")
	raftAddr := flag.String("raft-addr", ":9001", "gRPC listen address for peer traffic")
	clientAddr := flag.String("client-addr", ":8001", "HTTP listen address for client commands")
	dataDir := flag.String("data", "./data", "directory for the durable log and snapshots")
	electionTick := flag.Int("election-tick", 10, "election timeout in ticks")
	heartbeatTick := flag.Int("heartbeat-tick", 1, "heartbeat interval in ticks")
	tick := flag.Duration("tick", 100*time.Millisecond, "wall-clock duration of one logical tick")
	flag.Parse()

	if *id == 0 {
		log.Fatal("-id is required and must be non-zero")
	}
	peerAddrs, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("-peers: %v", err)
	}
	if _, ok := peerAddrs[*id]; !ok {
		log.Fatalf("-peers must include this node's own id (%d)", *id)
	}
	voters := sortedIDs(peerAddrs)
	logger := log.New(os.Stderr, fmt.Sprintf("[node %d] ", *id), log.LstdFlags|log.Lmsgprefix)

	// Durable storage (survives restarts).
	wal, err := walstore.Open(*dataDir)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}

	// Transport: inbound server + outbound peer registry.
	inbound := make(chan raft.Message, 1024)
	peers, err := transport.NewPeers(*id, peerAddrs, logger)
	if err != nil {
		log.Fatalf("transport peers: %v", err)
	}
	lis, err := net.Listen("tcp", *raftAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", *raftAddr, err)
	}
	grpcServer := transport.Serve(lis, transport.NewServer(inbound))

	// The consensus node and its state machine.
	kv := kvstore.New()
	n, err := node.New(node.Config{
		ID:            *id,
		Peers:         voters,
		ElectionTick:  *electionTick,
		HeartbeatTick: *heartbeatTick,
		TickInterval:  *tick,
		PreVote:       true,
		CheckQuorum:   true,
		Rand:          rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(*id))),
		Storage:       wal,
		FSM:           kv,
		Sender:        peers,
		Inbound:       inbound,
		Logger:        logger,
	})
	if err != nil {
		log.Fatalf("create node: %v", err)
	}
	go n.Run()

	httpServer := &http.Server{Addr: *clientAddr, Handler: clientHandler(n, kv)}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("client http: %v", err)
		}
	}()

	logger.Printf("raft on %s, client API on %s, data in %s, voters %v", *raftAddr, *clientAddr, *dataDir, voters)

	// Graceful shutdown on SIGINT/SIGTERM.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc
	logger.Printf("shutting down")

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutCtx)
	n.Stop()
	grpcServer.GracefulStop()
	_ = peers.Close()
	if err := wal.Close(); err != nil {
		logger.Printf("close storage: %v", err)
	}
}

// clientHandler builds the HTTP client API. Reads of kv state run inside a
// node.Do/LinearizableRead closure, so they execute on the node's goroutine and
// never race with Apply.
func clientHandler(n *node.Node, kv *kvstore.KV) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := n.Propose(ctx, kvstore.Command{Op: kvstore.OpPut, Key: key, Value: string(body)}.Encode()); err != nil {
			writeErr(w, n, err)
			return
		}
		fmt.Fprintf(w, "OK %s=%s\n", key, body)
	})

	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := n.Propose(ctx, kvstore.Command{Op: kvstore.OpDelete, Key: key}.Encode()); err != nil {
			writeErr(w, n, err)
			return
		}
		fmt.Fprintf(w, "OK deleted %s\n", key)
	})

	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		stale := r.URL.Query().Get("stale") == "true"
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		var val string
		var ok bool
		fn := func() { val, ok = kv.Get(key) }
		var err error
		if stale {
			err = n.Do(ctx, fn)
		} else {
			err = n.LinearizableRead(ctx, fn)
		}
		if err != nil {
			writeErr(w, n, err)
			return
		}
		if !ok {
			http.Error(w, "key not found\n", http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "%s\n", val)
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		st, err := n.Status(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":        st.ID,
			"term":      st.Term,
			"state":     st.State.String(),
			"leader":    st.Lead,
			"commit":    st.Commit,
			"applied":   st.Applied,
			"lastIndex": st.LastIndex,
		})
	})

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "raftnode client API:\n"+
			"  PUT    /kv/{key}            body is the value\n"+
			"  GET    /kv/{key}            linearizable read (add ?stale=true for a local read)\n"+
			"  DELETE /kv/{key}\n"+
			"  GET    /status\n")
	})

	return mux
}

func writeErr(w http.ResponseWriter, n *node.Node, err error) {
	if errors.Is(err, node.ErrNotLeader) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		st, serr := n.Status(ctx)
		cancel()
		w.Header().Set("Content-Type", "text/plain")
		if serr == nil && st.Lead != 0 {
			w.Header().Set("X-Raft-Leader", strconv.FormatUint(st.Lead, 10))
			http.Error(w, fmt.Sprintf("not leader; current leader is node %d\n", st.Lead), http.StatusServiceUnavailable)
		} else {
			http.Error(w, "not leader; no leader known yet, retry\n", http.StatusServiceUnavailable)
		}
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func parsePeers(s string) (map[uint64]string, error) {
	out := make(map[uint64]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		at := strings.IndexByte(part, '@')
		if at < 0 {
			return nil, fmt.Errorf("bad peer %q (want id@host:port)", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(part[:at]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad peer id in %q: %w", part, err)
		}
		out[id] = strings.TrimSpace(part[at+1:])
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no peers given")
	}
	return out, nil
}

func sortedIDs(m map[uint64]string) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
