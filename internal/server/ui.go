package server

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AbdeldjalilChougui/webhook-relay/internal/diff"
	"github.com/AbdeldjalilChougui/webhook-relay/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

var funcs = template.FuncMap{
	"ago": func(t time.Time) string {
		d := time.Since(t).Round(time.Second)
		switch {
		case d < time.Minute:
			return fmt.Sprintf("%ds ago", int(d.Seconds()))
		case d < time.Hour:
			return fmt.Sprintf("%dm ago", int(d.Minutes()))
		case d < 48*time.Hour:
			return fmt.Sprintf("%dh ago", int(d.Hours()))
		}
		return t.Format("2006-01-02 15:04")
	},
	"ts":         func(t time.Time) string { return t.Format("2006-01-02 15:04:05.000") },
	"body":       renderBody,
	"sortedKeys": sortedKeys,
	"statusClass": func(code int) string {
		switch {
		case code == 0:
			return "none"
		case code < 300:
			return "ok"
		case code < 500:
			return "warn"
		}
		return "err"
	},
	"opClass": func(op diff.Op) string { return [...]string{"eq", "del", "ins"}[op] },
	"opSign":  func(op diff.Op) string { return [...]string{" ", "-", "+"}[op] },
}

var pages = map[string]*template.Template{}

func init() {
	for _, name := range []string{"index", "channel", "request", "diff"} {
		pages[name] = template.Must(template.New("layout.html").Funcs(funcs).
			ParseFS(templateFS, "templates/layout.html", "templates/"+name+".html"))
	}
}

func (s *Server) render(w http.ResponseWriter, name string, data map[string]any) {
	var buf bytes.Buffer
	if err := pages[name].Execute(&buf, data); err != nil {
		s.log.Error("render", "page", name, "err", err)
		http.Error(w, "template error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(buf.Bytes())
}

func sortedKeys(h http.Header) []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// renderBody pretty-prints JSON and shows binary bodies as a hex summary.
func renderBody(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	var v any
	if json.Unmarshal(b, &v) == nil {
		var out bytes.Buffer
		if json.Indent(&out, b, "", "  ") == nil {
			return out.String()
		}
	}
	if !utf8.Valid(b) {
		n := min(len(b), 64)
		return fmt.Sprintf("(%d bytes of binary data) %x…", len(b), b[:n])
	}
	return string(b)
}

func (s *Server) uiIndex(w http.ResponseWriter, r *http.Request) {
	chs, err := s.store.Channels(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type row struct {
		store.Channel
		Connected bool
	}
	var rows []row
	for _, c := range chs {
		rows = append(rows, row{c, s.hub.Connected(c.Name)})
	}
	s.render(w, "index", map[string]any{"Title": "Channels", "Channels": rows, "Host": r.Host})
}

func (s *Server) uiChannel(w http.ResponseWriter, r *http.Request) {
	ch := r.PathValue("channel")
	reqs, err := s.store.List(r.Context(), ch, 200)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.render(w, "channel", map[string]any{"Title": ch, "Channel": ch, "Requests": reqs,
		"Connected": s.hub.Connected(ch), "Host": r.Host})
}

func (s *Server) uiRequest(w http.ResponseWriter, r *http.Request) {
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
	s.render(w, "request", map[string]any{"Title": fmt.Sprintf("#%d", q.ID), "R": q, "Connected": s.hub.Connected(q.Channel)})
}

func (s *Server) uiReplay(w http.ResponseWriter, r *http.Request) {
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
	http.Redirect(w, r, "/r/"+strconv.FormatInt(q.ID, 10), http.StatusSeeOther)
}

// describe renders a request as text lines for diffing.
func describe(q *store.Request) []string {
	line := q.Method + " " + q.Path
	if q.Query != "" {
		line += "?" + q.Query
	}
	lines := []string{line}
	for _, k := range sortedKeys(q.Headers) {
		for _, v := range q.Headers[k] {
			lines = append(lines, k+": "+v)
		}
	}
	lines = append(lines, "")
	if b := renderBody(q.Body); b != "" {
		lines = append(lines, strings.Split(b, "\n")...)
	}
	return lines
}

func (s *Server) uiDiff(w http.ResponseWriter, r *http.Request) {
	ids := r.URL.Query()["id"]
	if a, b := r.URL.Query().Get("a"), r.URL.Query().Get("b"); a != "" && b != "" {
		ids = []string{a, b}
	}
	if len(ids) != 2 {
		http.Error(w, "select exactly two requests to compare", http.StatusBadRequest)
		return
	}
	var reqs [2]*store.Request
	for i, raw := range ids {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		if reqs[i], err = s.store.Get(r.Context(), id); err != nil {
			http.Error(w, fmt.Sprintf("request %d: %v", id, err), http.StatusNotFound)
			return
		}
	}
	lines := diff.Lines(describe(reqs[0]), describe(reqs[1]))
	changed := 0
	for _, l := range lines {
		if l.Op != diff.Equal {
			changed++
		}
	}
	s.render(w, "diff", map[string]any{"Title": "diff", "A": reqs[0], "B": reqs[1], "Lines": lines, "Changed": changed})
}
