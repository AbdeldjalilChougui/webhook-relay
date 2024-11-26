package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AbdeldjalilChougui/webhook-relay/internal/client"
	"github.com/AbdeldjalilChougui/webhook-relay/internal/server"
	"github.com/AbdeldjalilChougui/webhook-relay/internal/store"
)

const token = "s3cret"

type env struct {
	relay  *httptest.Server
	srv    *server.Server
	st     *store.Store
	mu     sync.Mutex
	seen   []*http.Request
	bodies []string
	target *httptest.Server
}

func setup(t *testing.T) *env {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "relay.db"), 100)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	e := &env{st: st}
	e.srv = server.New(st, server.Config{Token: token, ForwardTimeout: 3 * time.Second})
	e.relay = httptest.NewServer(e.srv)
	t.Cleanup(e.relay.Close)
	e.target = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		e.mu.Lock()
		e.seen = append(e.seen, r)
		e.bodies = append(e.bodies, string(b))
		e.mu.Unlock()
		if r.URL.Path == "/fail" {
			w.WriteHeader(http.StatusTeapot)
		}
		w.Header().Set("X-Local", "yes")
		fmt.Fprintf(w, "local got %s %s?%s %s", r.Method, r.URL.Path, r.URL.RawQuery, b)
	}))
	t.Cleanup(e.target.Close)
	return e
}

func (e *env) startClient(t *testing.T, tok string) (context.CancelFunc, chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	connected := make(chan struct{}, 1)
	c := &client.Client{Server: e.relay.URL, Channel: "demo", Token: tok, Target: e.target.URL, Connected: connected}
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()
	select {
	case <-connected:
	case err := <-errc:
		cancel()
		return cancel, func() chan error { ch := make(chan error, 1); ch <- err; return ch }()
	case <-time.After(5 * time.Second):
		t.Fatal("client did not connect")
	}
	t.Cleanup(cancel)
	return cancel, errc
}

func authed(method, url string, body io.Reader) *http.Request {
	r, _ := http.NewRequest(method, url, body)
	r.Header.Set("Authorization", "Bearer "+token)
	return r
}

func TestRoundTripThroughClient(t *testing.T) {
	e := setup(t)
	e.startClient(t, token)
	resp, err := http.Post(e.relay.URL+"/h/demo/stripe/events?x=1", "application/json", strings.NewReader(`{"type":"paid"}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != `local got POST /stripe/events?x=1 {"type":"paid"}` || resp.Header.Get("X-Local") != "yes" {
		t.Fatalf("%d %q %v", resp.StatusCode, b, resp.Header)
	}
	if e.seen[0].Header.Get("Content-Type") != "application/json" {
		t.Fatal("headers not forwarded")
	}
	id := resp.Header.Get("X-Relay-Request-Id")

	// The API shows the stored request with its response.
	r, _ := http.DefaultClient.Do(authed("GET", e.relay.URL+"/api/requests/"+id, nil))
	var got struct {
		Method   string
		Path     string
		Body     string
		Response struct {
			Status int
			Body   string
		}
	}
	json.NewDecoder(r.Body).Decode(&got)
	r.Body.Close()
	if got.Method != "POST" || got.Path != "/stripe/events" || got.Body != `{"type":"paid"}` || got.Response.Status != 200 {
		t.Fatalf("api: %+v", got)
	}

	// Non-2xx from the target is passed through as-is.
	resp, _ = http.Get(e.relay.URL + "/h/demo/fail")
	resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestNoClientStoresAndReplays(t *testing.T) {
	e := setup(t)
	resp, _ := http.Post(e.relay.URL+"/h/demo/hook", "text/plain", strings.NewReader("first"))
	var accepted struct{ ID int64 }
	json.NewDecoder(resp.Body).Decode(&accepted)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || accepted.ID == 0 {
		t.Fatalf("want 202 with id, got %d %+v", resp.StatusCode, accepted)
	}

	e.startClient(t, token)
	r, _ := http.DefaultClient.Do(authed("POST", fmt.Sprintf("%s/api/requests/%d/replay", e.relay.URL, accepted.ID), nil))
	var replay struct {
		ID       int64 `json:"id"`
		ReplayOf int64 `json:"replay_of"`
		Response struct{ Status int }
	}
	json.NewDecoder(r.Body).Decode(&replay)
	r.Body.Close()
	if replay.ReplayOf != accepted.ID || replay.Response.Status != 200 {
		t.Fatalf("replay %+v", replay)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.bodies) != 1 || e.bodies[0] != "first" {
		t.Fatalf("target saw %q", e.bodies)
	}
}

func TestAuth(t *testing.T) {
	e := setup(t)
	for _, path := range []string{"/", "/c/demo", "/api/channels/demo/requests", "/r/1"} {
		resp, _ := http.Get(e.relay.URL + path)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s without token: %d", path, resp.StatusCode)
		}
	}
	req, _ := http.NewRequest("GET", e.relay.URL+"/", nil)
	req.SetBasicAuth("anyone", token)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("basic auth: %d", resp.StatusCode)
	}
	// A client with the wrong token gives up instead of retrying forever.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := &client.Client{Server: e.relay.URL, Channel: "demo", Token: "wrong", Target: e.target.URL}
	if err := c.Run(ctx); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("want auth error, got %v", err)
	}
	// Invalid channel names are rejected.
	resp, _ = http.Post(e.relay.URL+"/h/bad.name", "text/plain", nil)
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("bad channel: %d", resp.StatusCode)
	}
}

func TestTargetDownGives502(t *testing.T) {
	e := setup(t)
	e.startClient(t, token)
	e.target.Close()
	resp, _ := http.Post(e.relay.URL+"/h/demo/x", "text/plain", strings.NewReader("y"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d", resp.StatusCode)
	}
	list, _ := e.st.List(context.Background(), "demo", 1)
	if list[0].RespError == "" {
		t.Fatal("client error not recorded")
	}
}

func TestInspectorPages(t *testing.T) {
	e := setup(t)
	e.startClient(t, token)
	for _, body := range []string{`{"amount":10,"currency":"dzd"}`, `{"amount":12,"currency":"dzd"}`} {
		resp, _ := http.Post(e.relay.URL+"/h/demo/pay", "application/json", strings.NewReader(body))
		resp.Body.Close()
	}
	list, _ := e.st.List(context.Background(), "demo", 10)
	a, b := list[1].ID, list[0].ID
	get := func(path string) string {
		r, err := http.DefaultClient.Do(authed("GET", e.relay.URL+path, nil))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		if r.StatusCode != 200 {
			t.Fatalf("%s: %d %s", path, r.StatusCode, body)
		}
		return string(body)
	}
	if page := get("/"); !strings.Contains(page, `href="/c/demo"`) || !strings.Contains(page, "connected") {
		t.Fatalf("index:\n%s", page)
	}
	if page := get("/c/demo"); !strings.Contains(page, "POST /pay") {
		t.Fatalf("channel page:\n%s", page)
	}
	if page := get(fmt.Sprintf("/r/%d", a)); !strings.Contains(page, "&#34;amount&#34;: 10") {
		t.Fatalf("detail page should pretty-print JSON:\n%s", page)
	}
	page := get(fmt.Sprintf("/diff?id=%d&id=%d", a, b))
	if !strings.Contains(page, `class="del">-   &#34;amount&#34;: 10,`) || !strings.Contains(page, `class="ins">&#43;   &#34;amount&#34;: 12,`) {
		t.Fatalf("diff page:\n%s", page)
	}
	// Replay through the UI redirects to the new request.
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	r, _ := noRedirect.Do(authed("POST", fmt.Sprintf("%s/r/%d/replay", e.relay.URL, a), nil))
	r.Body.Close()
	if r.StatusCode != http.StatusSeeOther || !strings.HasPrefix(r.Header.Get("Location"), "/r/") {
		t.Fatalf("replay redirect: %d %s", r.StatusCode, r.Header.Get("Location"))
	}
}

func TestNewerClientReplacesOlder(t *testing.T) {
	e := setup(t)
	_, errc1 := e.startClient(t, token)
	e.startClient(t, token)
	resp, _ := http.Post(e.relay.URL+"/h/demo/x", "text/plain", strings.NewReader("1"))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if !e.srv.Hub().Connected("demo") {
		t.Fatal("channel should still be connected")
	}
	select {
	case err := <-errc1:
		t.Fatalf("first client exited: %v", err) // it keeps reconnecting instead
	default:
	}
}
