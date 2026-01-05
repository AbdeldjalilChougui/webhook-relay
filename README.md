# webhook-relay

[![Go Reference](https://pkg.go.dev/badge/github.com/AbdeldjalilChougui/webhook-relay.svg)](https://pkg.go.dev/github.com/AbdeldjalilChougui/webhook-relay)

Receive webhooks on a public server and handle them on your laptop. A
self-hosted alternative to tunnelling services, built for webhook
development.

```
Stripe/GitHub/… ──POST──► relay-server /h/stripe ══websocket══► relay (CLI) ──► http://localhost:3000
                               │  stores every request                          (your app)
                               └─ inspector UI: list, detail, replay, diff
```

## Who it's for

Developers who need to test webhook integrations (payment providers, GitHub,
chat bots) against code running locally. Unlike a generic tunnel, every
request is **recorded**. You can look at it later, **replay** it after fixing
your handler, and **diff** two deliveries to see what changed.

## Install

Prebuilt binaries for Linux, macOS and Windows (amd64 and arm64) are on the
[releases page](https://github.com/AbdeldjalilChougui/webhook-relay/releases/latest).
Each archive contains the `relay-server` and `relay` binaries, the README and the license; `checksums.txt`
has the SHA-256 sums. For example, on Linux amd64:

```sh
curl -LO https://github.com/AbdeldjalilChougui/webhook-relay/releases/download/v0.1.0/webhook-relay_0.1.0_linux_amd64.tar.gz
tar xzf webhook-relay_0.1.0_linux_amd64.tar.gz
```

Or build from source with Go:

```sh
go install github.com/AbdeldjalilChougui/webhook-relay/cmd/...@latest
```

## Quick start

On a server with a public URL:

```sh
go build -o relay-server ./cmd/relay-server
RELAY_TOKEN=change-me ./relay-server -addr :8080 -db relay.db
```

On your machine:

```sh
go build -o relay ./cmd/relay
RELAY_TOKEN=change-me ./relay -server https://relay.example.com -channel stripe -to http://localhost:3000
```

Then point the webhook at `https://relay.example.com/h/stripe`. Anything after
the channel is kept: `/h/stripe/v1/events?x=1` arrives at
`http://localhost:3000/v1/events?x=1`. The response from your local app goes
back to the sender, so signature checks and status-code retries behave as
they would in production.

The inspector is at `https://relay.example.com/` (HTTP Basic auth: any user
name, the token as password).

### Server flags

| flag | default | |
|---|---|---|
| `-addr` | `:8080` | listen address (put TLS in front, e.g. Caddy or nginx) |
| `-db` | `relay.db` | SQLite file |
| `-keep` | `500` | requests kept per channel |
| `-max-body` | `1048576` | bytes stored and forwarded per request |
| `-timeout` | `25s` | how long a webhook waits for your local answer |

`RELAY_TOKEN` protects the websocket, the inspector and the JSON API. The
`/h/…` endpoints are public by design.

## How it works

- **Capture.** `/h/{channel}/{rest...}` reads the body (up to the limit),
  strips hop-by-hop headers and stores the request in SQLite before doing
  anything else. A delivery is never lost, even when no client is listening.
  In that case the sender gets `202 Accepted` with the request ID.
- **Relay.** Each channel has at most one connected client, and a newer
  client replaces the older one. The server sends a JSON `request` frame
  over the websocket with a random correlation ID, then waits for the
  matching `response` frame or the timeout. The client performs the HTTP
  call to the local target concurrently, so slow handlers don't block each
  other. If the target is unreachable, the sender gets a 502 and the error
  is stored. The server pings every 25 s to keep proxies from dropping the
  connection. The client reconnects with exponential backoff but stops on
  401/403.
- **Inspector.** These are server-rendered `html/template` pages embedded in
  the binary, with no JavaScript. *Replay* re-sends a stored request as a
  new request linked to the original. *Diff* renders both requests as text
  (request line, sorted headers, pretty-printed JSON body) and shows a line
  diff (LCS).
- **API.** `GET /api/channels/{c}/requests`, `GET /api/requests/{id}`,
  `POST /api/requests/{id}/replay` (Bearer token).
- **Storage.** It uses the pure-Go `modernc.org/sqlite` driver (no cgo) in
  WAL mode with one connection. Old requests are pruned per channel on
  insert.

## Tests

```sh
go test -race ./...
```

The end-to-end tests start the relay server and a real client over a
websocket in-process, with an `httptest` server as the "local app". They
cover forwarding (method, path, query, headers, body, status, response
headers), storing while no client is connected and then replaying, Bearer
and Basic auth, the client giving up on a bad token, invalid channel names,
the 502 when the target is down, the inspector pages including the diff
output and replay redirect, and a newer client replacing an older one.
There are also unit tests for the store (pruning, round trips) and the diff.

## Status and limitations

- One client per channel. There's no fan-out to several developers.
- A single shared token. There are no per-channel credentials or user
  accounts.
- Bodies over `-max-body` are truncated when stored *and* when forwarded.
  Streaming and very large payloads aren't supported, and neither is
  websocket passthrough (it's for webhooks, not general tunnelling).
- The server keeps waiting clients in memory, so it runs as a single
  instance. Scaling horizontally would need a shared pub/sub.
- No TLS termination built in. Run it behind a reverse proxy.
- The UI has no pagination beyond the latest 200 requests per channel.

## License

MIT. See [LICENSE](LICENSE).
