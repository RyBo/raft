package server

import (
	"encoding/json"

	"github.com/gorilla/websocket"
	"github.com/rybo/raft/sim"
)

// Hub fans cluster events out to all connected browsers and funnels their
// commands into the cluster. It runs on its own goroutine.
type Hub struct {
	cluster *sim.Cluster

	register   chan *client
	unregister chan *client
	broadcast  chan []byte

	clients map[*client]bool

	// lastState is the most recent full state event, replayed to new clients so
	// they render immediately even while the clock is paused.
	lastState []byte
}

// NewHub creates a hub bound to a cluster.
func NewHub(cluster *sim.Cluster) *Hub {
	return &Hub{
		cluster:    cluster,
		register:   make(chan *client),
		unregister: make(chan *client),
		broadcast:  make(chan []byte, 1024),
		clients:    map[*client]bool{},
	}
}

// Emit is the cluster's event sink. Safe to call from the cluster goroutine.
func (h *Hub) Emit(b []byte) {
	h.broadcast <- b
}

// Run processes registrations and broadcasts until stop is closed.
func (h *Hub) Run(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case c := <-h.register:
			h.clients[c] = true
			if h.lastState != nil {
				h.trySend(c, h.lastState)
			}
		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
		case b := <-h.broadcast:
			if isStateEvent(b) {
				h.lastState = b
			}
			for c := range h.clients {
				h.trySend(c, b)
			}
		}
	}
}

func (h *Hub) trySend(c *client, b []byte) {
	select {
	case c.send <- b:
	default:
		// Slow client: drop it rather than stalling everyone.
		delete(h.clients, c)
		close(c.send)
	}
}

func isStateEvent(b []byte) bool {
	var probe struct {
		Type string `json:"type"`
	}
	return json.Unmarshal(b, &probe) == nil && probe.Type == "state"
}

// client is one WebSocket connection.
type client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

// readPump reads commands from the browser and submits them to the cluster.
func (c *client) readPump() {
	defer func() {
		c.hub.unregister <- c
		_ = c.conn.Close()
	}()
	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var cmd sim.Command
		if json.Unmarshal(msg, &cmd) != nil {
			continue
		}
		c.hub.cluster.Submit(cmd)
	}
}

// writePump writes broadcast messages to the browser.
func (c *client) writePump() {
	defer func() { _ = c.conn.Close() }()
	for msg := range c.send {
		if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}
