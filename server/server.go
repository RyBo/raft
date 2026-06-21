// Package server exposes the simulation over HTTP: a WebSocket endpoint for
// live state/commands and the embedded React UI as static assets.
package server

import (
	"io/fs"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/rybo/raft/sim"
	"github.com/rybo/raft/webui"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1 << 16,
	// The demo is meant to be opened locally; allow any origin.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Handler wires the cluster, hub, optional Prometheus metrics and static assets
// into an http.Handler. metricsHandler may be nil to disable /metrics.
func Handler(cluster *sim.Cluster, hub *Hub, metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		c := &client{hub: hub, conn: conn, send: make(chan []byte, 256)}
		hub.register <- c
		go c.writePump()
		go c.readPump()
	})

	if metricsHandler != nil {
		mux.Handle("/metrics", metricsHandler)
	}

	mux.Handle("/", staticHandler())
	return mux
}

// staticHandler serves the embedded UI with SPA fallback to index.html.
func staticHandler() http.Handler {
	sub, err := fs.Sub(webui.DistFS, "dist")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// If the requested file exists, serve it; otherwise fall back to the SPA
		// entry point.
		path := r.URL.Path
		if path != "/" {
			if _, err := fs.Stat(sub, path[1:]); err != nil {
				r2 := new(http.Request)
				*r2 = *r
				r2.URL.Path = "/"
				fileServer.ServeHTTP(w, r2)
				return
			}
		}
		fileServer.ServeHTTP(w, r)
	})
}
