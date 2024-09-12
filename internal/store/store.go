// Package store keeps received webhook requests (and the responses the
// local target gave) in SQLite.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver, no cgo
)

// Request is one captured webhook delivery.
type Request struct {
	ID            int64
	Channel       string
	ReceivedAt    time.Time
	Method        string
	Path          string
	Query         string
	Headers       http.Header
	Body          []byte
	BodyTruncated bool
	RemoteAddr    string
	ReplayOf      int64 // 0 unless this is a replay

	// Filled in once the client answers (or fails).
	RespStatus  int
	RespHeaders http.Header
	RespBody    []byte
	RespError   string
	Duration    time.Duration
}

// Channel summarises one channel for the index page.
type Channel struct {
	Name     string
	Count    int
	LastSeen time.Time
}

type Store struct {
	db        *sql.DB
	keepPerCh int
}

var ErrNotFound = errors.New("not found")

const schema = `
CREATE TABLE IF NOT EXISTS requests (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	channel        TEXT    NOT NULL,
	received_at    INTEGER NOT NULL,
	method         TEXT    NOT NULL,
	path           TEXT    NOT NULL,
	query          TEXT    NOT NULL DEFAULT '',
	headers        TEXT    NOT NULL DEFAULT '{}',
	body           BLOB,
	body_truncated INTEGER NOT NULL DEFAULT 0,
	remote_addr    TEXT    NOT NULL DEFAULT '',
	replay_of      INTEGER NOT NULL DEFAULT 0,
	resp_status    INTEGER NOT NULL DEFAULT 0,
	resp_headers   TEXT    NOT NULL DEFAULT '{}',
	resp_body      BLOB,
	resp_error     TEXT    NOT NULL DEFAULT '',
	duration_ms    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS requests_channel ON requests(channel, id DESC);
`

// Open opens (or creates) the database. keepPerChannel bounds storage: older
// requests beyond that count are deleted on insert (0 = keep everything).
func Open(path string, keepPerChannel int) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	// SQLite allows one writer; a single connection avoids SQLITE_BUSY and
	// is plenty for this workload.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Store{db: db, keepPerCh: keepPerChannel}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func headersJSON(h http.Header) string {
	if h == nil {
		return "{}"
	}
	b, _ := json.Marshal(h)
	return string(b)
}

func parseHeaders(s string) http.Header {
	h := http.Header{}
	json.Unmarshal([]byte(s), &h)
	return h
}

// Insert stores r and sets r.ID.
func (s *Store) Insert(ctx context.Context, r *Request) error {
	res, err := s.db.ExecContext(ctx, `INSERT INTO requests
		(channel, received_at, method, path, query, headers, body, body_truncated, remote_addr, replay_of)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.Channel, r.ReceivedAt.UnixMilli(), r.Method, r.Path, r.Query, headersJSON(r.Headers),
		r.Body, r.BodyTruncated, r.RemoteAddr, r.ReplayOf)
	if err != nil {
		return err
	}
	if r.ID, err = res.LastInsertId(); err != nil {
		return err
	}
	if s.keepPerCh > 0 {
		_, err = s.db.ExecContext(ctx, `DELETE FROM requests WHERE channel = ? AND id NOT IN
			(SELECT id FROM requests WHERE channel = ? ORDER BY id DESC LIMIT ?)`,
			r.Channel, r.Channel, s.keepPerCh)
	}
	return err
}

// SetResponse records the outcome of forwarding request id.
func (s *Store) SetResponse(ctx context.Context, id int64, status int, h http.Header, body []byte, errMsg string, d time.Duration) error {
	_, err := s.db.ExecContext(ctx, `UPDATE requests SET resp_status = ?, resp_headers = ?, resp_body = ?,
		resp_error = ?, duration_ms = ? WHERE id = ?`,
		status, headersJSON(h), body, errMsg, d.Milliseconds(), id)
	return err
}

const cols = `id, channel, received_at, method, path, query, headers, body, body_truncated, remote_addr,
	replay_of, resp_status, resp_headers, resp_body, resp_error, duration_ms`

func scan(row interface{ Scan(...any) error }) (*Request, error) {
	var r Request
	var at, dur int64
	var h, rh string
	err := row.Scan(&r.ID, &r.Channel, &at, &r.Method, &r.Path, &r.Query, &h, &r.Body, &r.BodyTruncated,
		&r.RemoteAddr, &r.ReplayOf, &r.RespStatus, &rh, &r.RespBody, &r.RespError, &dur)
	if err != nil {
		return nil, err
	}
	r.ReceivedAt = time.UnixMilli(at)
	r.Headers, r.RespHeaders = parseHeaders(h), parseHeaders(rh)
	r.Duration = time.Duration(dur) * time.Millisecond
	return &r, nil
}

// Get returns one request.
func (s *Store) Get(ctx context.Context, id int64) (*Request, error) {
	r, err := scan(s.db.QueryRowContext(ctx, `SELECT `+cols+` FROM requests WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return r, err
}

// List returns the newest requests of a channel first.
func (s *Store) List(ctx context.Context, channel string, limit int) ([]*Request, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+cols+` FROM requests WHERE channel = ? ORDER BY id DESC LIMIT ?`, channel, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Request
	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Channels lists channels with their request counts, most recent first.
func (s *Store) Channels(ctx context.Context) ([]Channel, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT channel, COUNT(*), MAX(received_at) FROM requests
		GROUP BY channel ORDER BY MAX(received_at) DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Channel
	for rows.Next() {
		var c Channel
		var last int64
		if err := rows.Scan(&c.Name, &c.Count, &last); err != nil {
			return nil, err
		}
		c.LastSeen = time.UnixMilli(last)
		out = append(out, c)
	}
	return out, rows.Err()
}
