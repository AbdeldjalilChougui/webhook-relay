// Package server is the public relay: it captures webhooks on /h/{channel},
// hands them to the connected CLI client over a websocket, returns the
// client's response, and serves an inspector UI.
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/AbdeldjalilChougui/webhook-relay/internal/protocol"
	"github.com/AbdeldjalilChougui/webhook-relay/internal/store"
)

// Config for the server.
type Config struct {
	// Token protects the websocket, the inspector and the API (Bearer token,
	// or HTTP Basic with any user name and the token as password). Empty
	// disables auth, for local development only.
	Token          string
	MaxBody        int64         // bytes stored/forwarded per request (default 1 MiB)
	ForwardTimeout time.Duration // how long a webhook waits for the client (default 25s)
	Logger         *slog.Logger
}

type Server struct {
	cfg   Config
	store *store.Store
	hub   *Hub
	log   *slog.Logger
	mux   *http.ServeMux
}

var channelRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func New(st *store.Store, cfg Config) *Server {
	if cfg.MaxBody == 0 {
		cfg.MaxBody = 1 << 20
	}
	if cfg.ForwardTimeout == 0 {
		cfg.ForwardTimeout = 25 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &Server{cfg: cfg, store: st, hub: NewHub(), log: cfg.Logger, mux: http.NewServeMux()}
	// Public: anyone may send webhooks.
	s.mux.HandleFunc("/h/{channel}", s.handleHook)
	s.mux.HandleFunc("/h/{channel}/{rest...}", s.handleHook)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok\n") })
	// Private.
	s.mux.Handle("GET /ws/{channel}", s.auth(http.HandlerFunc(s.handleWS)))
	s.mux.Handle("GET /api/channels/{channel}/requests", s.auth(http.HandlerFunc(s.apiList)))
	s.mux.Handle("GET /api/requests/{id}", s.auth(http.HandlerFunc(s.apiGet)))
	s.mux.Handle("POST /api/requests/{id}/replay", s.auth(http.HandlerFunc(s.apiReplay)))
	s.mux.Handle("GET /{$}", s.auth(http.HandlerFunc(s.uiIndex)))
	s.mux.Handle("GET /c/{channel}", s.auth(http.HandlerFunc(s.uiChannel)))
	s.mux.Handle("GET /r/{id}", s.auth(http.HandlerFunc(s.uiRequest)))
	s.mux.Handle("POST /r/{id}/replay", s.auth(http.HandlerFunc(s.uiReplay)))
	s.mux.Handle("GET /diff", s.auth(http.HandlerFunc(s.uiDiff)))
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Hub exposes the hub (for tests and status).
func (s *Server) Hub() *Hub { return s.hub }

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.Token == "" {
			next.ServeHTTP(w, r)
			return
		}
		got := ""
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			got = strings.TrimPrefix(h, "Bearer ")
		} else if _, pw, ok := r.BasicAuth(); ok {
			got = pw
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.Token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="webhook-relay"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// handleHook captures a webhook, forwards it, and relays the answer.
func (s *Server) handleHook(w http.ResponseWriter, r *http.Request) {
	channel := r.PathValue("channel")
	if !channelRe.MatchString(channel) {
		http.Error(w, "invalid channel name", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, s.cfg.MaxBody+1))
	if err != nil {
		http.Error(w, "reading body: "+err.Error(), http.StatusBadRequest)
		return
	}
	truncated := int64(len(body)) > s.cfg.MaxBody
	if truncated {
		body = body[:s.cfg.MaxBody]
	}
	req := &store.Request{
		Channel: channel, ReceivedAt: time.Now(), Method: r.Method,
		Path: "/" + r.PathValue("rest"), Query: r.URL.RawQuery,
		Headers: protocol.CleanHeaders(r.Header), Body: body, BodyTruncated: truncated,
		RemoteAddr: r.RemoteAddr,
	}
	if err := s.store.Insert(r.Context(), req); err != nil {
		s.log.Error("store request", "err", err)
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}
	resp, err := s.forward(r.Context(), req)
	if errors.Is(err, ErrNoClient) {
		writeJSON(w, http.StatusAccepted, map[string]any{"id": req.ID, "status": "stored", "note": "no client connected; replay it from the inspector later"})
		return
	}
	if err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		http.Error(w, "relay: "+err.Error(), status)
		return
	}
	for k, vs := range protocol.CleanHeaders(resp.Headers) {
		if strings.EqualFold(k, "Content-Length") {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("X-Relay-Request-Id", strconv.FormatInt(req.ID, 10))
	w.WriteHeader(resp.Status)
	w.Write(resp.Body)
}

// forward sends a stored request to the channel's client and records the
// result. When the client reports an error reaching its target, that is
// returned as a 502 response frame.
func (s *Server) forward(ctx context.Context, req *store.Request) (protocol.Frame, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.ForwardTimeout)
	defer cancel()
	start := time.Now()
	resp, err := s.hub.Forward(ctx, req.Channel, protocol.Frame{
		Method: req.Method, Path: req.Path, Query: req.Query, Headers: req.Headers, Body: req.Body,
	})
	// Record with a fresh context: the caller may have gone away.
	rec, cancelRec := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRec()
	switch {
	case err != nil:
		s.store.SetResponse(rec, req.ID, 0, nil, nil, err.Error(), time.Since(start))
		return resp, err
	case resp.Error != "":
		s.store.SetResponse(rec, req.ID, 0, nil, nil, resp.Error, time.Since(start))
		return protocol.Frame{Status: http.StatusBadGateway, Body: []byte("relay client: " + resp.Error + "\n"),
			Headers: http.Header{"Content-Type": {"text/plain; charset=utf-8"}}}, nil
	}
	if resp.Status == 0 {
		resp.Status = http.StatusOK
	}
	s.store.SetResponse(rec, req.ID, resp.Status, resp.Headers, resp.Body, "", time.Since(start))
	return resp, nil
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	channel := r.PathValue("channel")
	if !channelRe.MatchString(channel) {
		http.Error(w, "invalid channel name", http.StatusBadRequest)
		return
	}
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	s.log.Info("client connected", "channel", channel, "remote", r.RemoteAddr)
	s.hub.Serve(r.Context(), channel, ws)
	s.log.Info("client disconnected", "channel", channel)
}

// replay re-sends a stored request as a new request (linked to the old one).
func (s *Server) replay(ctx context.Context, id int64) (*store.Request, error) {
	orig, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	cp := *orig
	cp.ID, cp.ReceivedAt, cp.ReplayOf, cp.RemoteAddr = 0, time.Now(), orig.ID, "replay"
	cp.RespStatus, cp.RespHeaders, cp.RespBody, cp.RespError, cp.Duration = 0, nil, nil, "", 0
	if err := s.store.Insert(ctx, &cp); err != nil {
		return nil, err
	}
	if _, err := s.forward(ctx, &cp); err != nil && !errors.Is(err, ErrNoClient) {
		s.log.Warn("replay failed", "id", id, "err", err)
	}
	return s.store.Get(ctx, cp.ID)
}

// ---- JSON API ----

type apiRequest struct {
	ID            int64       `json:"id"`
	Channel       string      `json:"channel"`
	ReceivedAt    time.Time   `json:"received_at"`
	Method        string      `json:"method"`
	Path          string      `json:"path"`
	Query         string      `json:"query,omitempty"`
	Headers       http.Header `json:"headers"`
	Body          string      `json:"body"`
	BodyTruncated bool        `json:"body_truncated,omitempty"`
	ReplayOf      int64       `json:"replay_of,omitempty"`
	Response      *apiResp    `json:"response,omitempty"`
}

type apiResp struct {
	Status     int         `json:"status,omitempty"`
	Headers    http.Header `json:"headers,omitempty"`
	Body       string      `json:"body,omitempty"`
	Error      string      `json:"error,omitempty"`
	DurationMs int64       `json:"duration_ms"`
}

func toAPI(r *store.Request) apiRequest {
	a := apiRequest{ID: r.ID, Channel: r.Channel, ReceivedAt: r.ReceivedAt, Method: r.Method, Path: r.Path,
		Query: r.Query, Headers: r.Headers, Body: string(r.Body), BodyTruncated: r.BodyTruncated, ReplayOf: r.ReplayOf}
	if r.RespStatus != 0 || r.RespError != "" {
		a.Response = &apiResp{Status: r.RespStatus, Headers: r.RespHeaders, Body: string(r.RespBody),
			Error: r.RespError, DurationMs: r.Duration.Milliseconds()}
	}
	return a
}

func (s *Server) apiList(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	reqs, err := s.store.List(r.Context(), r.PathValue("channel"), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]apiRequest, 0, len(reqs))
	for _, q := range reqs {
		out = append(out, toAPI(q))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) idParam(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return 0, false
	}
	return id, true
}

func (s *Server) apiGet(w http.ResponseWriter, r *http.Request) {
	id, ok := s.idParam(w, r)
	if !ok {
		return
	}
	q, err := s.store.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, toAPI(q))
}

func (s *Server) apiReplay(w http.ResponseWriter, r *http.Request) {
	id, ok := s.idParam(w, r)
	if !ok {
		return
	}
	q, err := s.replay(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, toAPI(q))
}
