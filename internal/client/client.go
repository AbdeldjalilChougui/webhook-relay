// Package client is the CLI side: it keeps a websocket open to the relay
// server, forwards each webhook to a local URL and sends the answer back.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/AbdeldjalilChougui/webhook-relay/internal/protocol"
)

type Client struct {
	Server  string // relay base URL, e.g. https://relay.example.com
	Channel string
	Token   string
	Target  string // local base URL, e.g. http://localhost:3000
	HTTP    *http.Client
	Log     *slog.Logger
	// OnForward is called after each forwarded request (for the CLI's log line).
	OnForward func(method, path string, status int, d time.Duration, err error)
	// Connected is signalled (non-blocking) each time the server says hello.
	Connected chan struct{}
}

func (c *Client) wsURL() (string, error) {
	u, err := url.Parse(c.Server)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("server URL must be http(s):// or ws(s)://, got %q", c.Server)
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/ws/" + url.PathEscape(c.Channel)
	return u.String(), nil
}

// Run connects and reconnects (with exponential backoff up to 30s) until ctx
// is cancelled or the server rejects the credentials.
func (c *Client) Run(ctx context.Context) error {
	if c.HTTP == nil {
		c.HTTP = &http.Client{Timeout: 30 * time.Second}
	}
	if c.Log == nil {
		c.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	backoff := 500 * time.Millisecond
	for {
		start := time.Now()
		err := c.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var unauthorized *authError
		if errors.As(err, &unauthorized) {
			return err
		}
		if time.Since(start) > time.Minute {
			backoff = 500 * time.Millisecond // it was a healthy connection
		}
		c.Log.Warn("disconnected; reconnecting", "err", err, "in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

type authError struct{ status int }

func (e *authError) Error() string {
	return fmt.Sprintf("server refused the connection: HTTP %d (check -token)", e.status)
}

func (c *Client) runOnce(ctx context.Context) error {
	u, err := c.wsURL()
	if err != nil {
		return err
	}
	h := http.Header{}
	if c.Token != "" {
		h.Set("Authorization", "Bearer "+c.Token)
	}
	ws, resp, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPHeader: h})
	if err != nil {
		if resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			return &authError{resp.StatusCode}
		}
		return err
	}
	defer ws.CloseNow()
	ws.SetReadLimit(64 << 20)
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			return err
		}
		var f protocol.Frame
		if err := json.Unmarshal(data, &f); err != nil {
			continue
		}
		switch f.Type {
		case protocol.TypeHello:
			c.Log.Info("connected", "channel", f.Channel, "forwarding to", c.Target)
			if c.Connected != nil {
				select {
				case c.Connected <- struct{}{}:
				default:
				}
			}
		case protocol.TypeRequest:
			go func(f protocol.Frame) {
				resp := c.forward(ctx, f)
				b, _ := json.Marshal(resp)
				wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				ws.Write(wctx, websocket.MessageText, b)
			}(f)
		}
	}
}

// forward performs the HTTP request against the local target.
func (c *Client) forward(ctx context.Context, f protocol.Frame) protocol.Frame {
	start := time.Now()
	out := protocol.Frame{Type: protocol.TypeResponse, ID: f.ID}
	target := strings.TrimSuffix(c.Target, "/") + f.Path
	if f.Query != "" {
		target += "?" + f.Query
	}
	req, err := http.NewRequestWithContext(ctx, f.Method, target, bytes.NewReader(f.Body))
	if err == nil {
		req.Header = protocol.CleanHeaders(f.Headers)
		req.Header.Del("Host")
		req.Header.Del("Content-Length")
		var resp *http.Response
		if resp, err = c.HTTP.Do(req); err == nil {
			defer resp.Body.Close()
			out.Status = resp.StatusCode
			out.Headers = protocol.CleanHeaders(resp.Header)
			out.Body, err = io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		}
	}
	if err != nil {
		out.Error = err.Error()
	}
	if c.OnForward != nil {
		c.OnForward(f.Method, f.Path, out.Status, time.Since(start), err)
	}
	return out
}
