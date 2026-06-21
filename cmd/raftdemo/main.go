// Command raftdemo runs the in-process Raft cluster simulation and serves the
// interactive visualizer.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/rybo/raft/metrics"
	"github.com/rybo/raft/server"
	"github.com/rybo/raft/sim"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	nodes := flag.Int("nodes", 3, "initial number of nodes")
	seed := flag.Int64("seed", 1, "random seed for reproducible runs")
	flag.Parse()

	stop := make(chan struct{})

	cluster := sim.NewCluster(*nodes, *seed, nil)
	hub := server.NewHub(cluster)
	m := metrics.New()
	cluster.SetEmit(hub.Emit)
	cluster.SetMetrics(m)

	go hub.Run(stop)
	go cluster.Run(stop)

	log.Printf("raft visualizer: http://localhost%s  (nodes=%d seed=%d)", *addr, *nodes, *seed)
	log.Printf("prometheus metrics: http://localhost%s/metrics", *addr)
	if err := http.ListenAndServe(*addr, server.Handler(cluster, hub, m.Handler())); err != nil {
		log.Fatal(err)
	}
}
