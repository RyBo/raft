// Command raftdemo runs the in-process Raft cluster simulation and serves the
// interactive visualizer.
package main

import (
	"flag"
	"log"
	"net"
	"net/http"

	"github.com/rybo/raft/metrics"
	"github.com/rybo/raft/server"
	"github.com/rybo/raft/sim"
)

// lanURLs returns http URLs for the host's non-loopback IPv4 addresses, so the
// operator knows what other devices on the network should point their browser at.
func lanURLs(port string) []string {
	var urls []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		if ip4 := ipnet.IP.To4(); ip4 != nil {
			urls = append(urls, "http://"+net.JoinHostPort(ip4.String(), port))
		}
	}
	return urls
}

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

	host, port, err := net.SplitHostPort(*addr)
	if err != nil {
		host, port = "", *addr
	}
	shownHost := host
	allInterfaces := host == "" || host == "0.0.0.0" || host == "::"
	if allInterfaces {
		shownHost = "localhost"
	}
	base := "http://" + net.JoinHostPort(shownHost, port)
	log.Printf("raft visualizer: %s  (nodes=%d seed=%d)", base, *nodes, *seed)
	log.Printf("prometheus metrics: %s/metrics", base)
	if allInterfaces {
		for _, u := range lanURLs(port) {
			log.Printf("  reachable on your network at %s", u)
		}
	}
	if err := http.ListenAndServe(*addr, server.Handler(cluster, hub, m.Handler())); err != nil {
		log.Fatal(err)
	}
}
