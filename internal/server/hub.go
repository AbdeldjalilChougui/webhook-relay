package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/AbdeldjalilChougui/webhook-relay/internal/protocol"
)

// ErrNoClient means nobody is listening on the channel right now.
var ErrNoClient = errors.New("no client connected to this channel")

// Hub tracks one connected CLI client per channel. A newer connection for
// the same channel replaces the older one (the last `relay` you started
// wins), which is what you want when restarting the client.
type Hub struct {
	mu      sync.Mutex
	clients map[string]*client
}

type client struct {
	ws      *websocket.Conn
	mu      sync.Mutex
	pending map[string]chan protocol.Frame
	done    chan struct{}
}

func NewHub() *Hub { return &Hub{clients: map[string]*client{}} }

// Connected reports whether channel has a client.
func (h *Hub) Connected(channel string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.clients[channel] != nil
}

// Serve runs a client connection until it closes.
func (h *Hub) Serve(ctx context.Context, channel string, ws *websocket.Conn) {
	c := &client{ws: ws, pending: map[string]chan protocol.Frame{}, done: make(chan struct{})}
	h.mu.Lock()
	old := h.clients[channel]
	h.clients[channel] = c
	h.mu.Unlock()
	if old != nil {
		old.ws.Close(websocket.StatusPolicyViolation, "replaced by a newer client")
	}
	defer func() {
		h.mu.Lock()
		if h.clients[channel] == c {
			delete(h.clients, channel)
		}
		h.mu.Unlock()
		close(c.done)
		ws.CloseNow()
	}()

	hello, _ := json.Marshal(protocol.Frame{Type: protocol.TypeHello, Channel: channel})
	if err := ws.Write(ctx, websocket.MessageText, hello); err != nil {
		return
	}
	// Keepalive: proxies drop idle websockets.
	go func() {
		t := time.NewTicker(25 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-c.done:
				return
			case <-t.C:
				pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				err := ws.Ping(pctx)
				cancel()
				if err != nil {
					ws.CloseNow()
					return
				}
			}
		}
	}()
	ws.SetReadLimit(64 << 20)
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			return
		}
		var f protocol.Frame
		if json.Unmarshal(data, &f) != nil || f.Type != protocol.TypeResponse {
			continue
		}
		c.mu.Lock()
		ch := c.pending[f.ID]
		delete(c.pending, f.ID)
		c.mu.Unlock()
		if ch != nil {
			ch <- f
		}
	}
}

func newID() string {
	var b [8]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Forward sends a request frame to the channel's client and waits for the
// response or ctx's deadline.
func (h *Hub) Forward(ctx context.Context, channel string, f protocol.Frame) (protocol.Frame, error) {
	h.mu.Lock()
	c := h.clients[channel]
	h.mu.Unlock()
	if c == nil {
		return protocol.Frame{}, ErrNoClient
	}
	f.Type = protocol.TypeRequest
	f.ID = newID()
	ch := make(chan protocol.Frame, 1)
	c.mu.Lock()
	c.pending[f.ID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
	}()
	data, err := json.Marshal(f)
	if err != nil {
		return protocol.Frame{}, err
	}
	if err := c.ws.Write(ctx, websocket.MessageText, data); err != nil {
		return protocol.Frame{}, err
	}
	select {
	case resp := <-ch:
		return resp, nil
	case <-c.done:
		return protocol.Frame{}, errors.New("client disconnected before answering")
	case <-ctx.Done():
		return protocol.Frame{}, ctx.Err()
	}
}
