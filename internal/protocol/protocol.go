// Package protocol defines the JSON frames exchanged over the websocket
// between the relay server and the CLI client.
//
//	server → client  {"type":"request",  "id":"…", "method":"POST", "path":"/x", …}
//	client → server  {"type":"response", "id":"…", "status":200, …}
//
// Bodies are base64 (encoding/json does that for []byte).
package protocol

import "net/http"

const (
	TypeRequest  = "request"
	TypeResponse = "response"
	TypeHello    = "hello" // server → client after connecting
)

// Frame is the single message type; fields are used according to Type.
type Frame struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`

	// request
	Method  string      `json:"method,omitempty"`
	Path    string      `json:"path,omitempty"` // path below /h/{channel}, always starting with "/"
	Query   string      `json:"query,omitempty"`
	Headers http.Header `json:"headers,omitempty"`
	Body    []byte      `json:"body,omitempty"`

	// response
	Status int    `json:"status,omitempty"`
	Error  string `json:"error,omitempty"` // the client could not reach the target

	// hello
	Channel string `json:"channel,omitempty"`
}

// hopHeaders must not be forwarded by proxies (RFC 9110 §7.6.1).
var hopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

// CleanHeaders returns a copy of h without hop-by-hop headers.
func CleanHeaders(h http.Header) http.Header {
	out := h.Clone()
	if out == nil {
		out = http.Header{}
	}
	for _, k := range hopHeaders {
		out.Del(k)
	}
	return out
}
