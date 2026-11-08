package fakecodex

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type wsPeer struct {
	c  *websocket.Conn
	mu sync.Mutex
}

func (p *wsPeer) write(data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = p.c.WriteMessage(websocket.TextMessage, data)
}

type wsRoute struct {
	peer *wsPeer
	id   json.RawMessage
}
type wsHub struct {
	mu      sync.Mutex
	peers   map[*wsPeer]bool
	pending map[int]wsRoute
	next    int
	in      *io.PipeWriter
	writeMu sync.Mutex
}

func (h *wsHub) Write(data []byte) (int, error) {
	var msg message
	if err := json.Unmarshal(data, &msg); err != nil {
		return 0, err
	}
	h.mu.Lock()
	var peers []*wsPeer
	if len(msg.ID) > 0 && msg.Method == "" {
		var id int
		_ = json.Unmarshal(msg.ID, &id)
		if route, ok := h.pending[id]; ok {
			delete(h.pending, id)
			msg.ID = route.id
			peers = append(peers, route.peer)
		}
	} else {
		for p := range h.peers {
			peers = append(peers, p)
		}
	}
	h.mu.Unlock()
	payload, _ := json.Marshal(msg)
	for _, p := range peers {
		p.write(payload)
	}
	return len(data), nil
}

// ServeWebsocket keeps one fake engine behind independent authenticated
// connections. Request IDs belong to clients; responses are never broadcast.
func ServeWebsocket(cfg Config, address, token string) error {
	r, w := io.Pipe()
	hub := &wsHub{peers: map[*wsPeer]bool{}, pending: map[int]wsRoute{}, in: w}
	cfg.In, cfg.Out = r, hub
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer l.Close()
	upgrader := websocket.Upgrader{}
	httpServer := &http.Server{Handler: http.HandlerFunc(func(out http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer "+token {
			http.Error(out, "unauthorized", 401)
			return
		}
		c, err := upgrader.Upgrade(out, req, nil)
		if err != nil {
			return
		}
		peer := &wsPeer{c: c}
		hub.mu.Lock()
		hub.peers[peer] = true
		hub.mu.Unlock()
		defer func() {
			c.Close()
			hub.mu.Lock()
			delete(hub.peers, peer)
			for id, route := range hub.pending {
				if route.peer == peer {
					delete(hub.pending, id)
				}
			}
			hub.mu.Unlock()
		}()
		for {
			_, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			var msg message
			if json.Unmarshal(data, &msg) != nil {
				continue
			}
			if len(msg.ID) > 0 && msg.Method != "" {
				hub.mu.Lock()
				hub.next++
				id := hub.next
				hub.pending[id] = wsRoute{peer: peer, id: msg.ID}
				hub.mu.Unlock()
				msg.ID = json.RawMessage(fmt.Sprint(id))
			}
			data, _ = json.Marshal(msg)
			hub.writeMu.Lock()
			_, err = w.Write(append(data, '\n'))
			hub.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	})}
	defer httpServer.Close()
	go func() { _ = httpServer.Serve(l); w.Close() }()
	return Serve(cfg)
}
