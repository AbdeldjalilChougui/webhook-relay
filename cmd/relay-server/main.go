// Command relay-server is the public side: it receives webhooks on
// /h/{channel}, stores them in SQLite and hands them to connected clients.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AbdeldjalilChougui/webhook-relay/internal/server"
	"github.com/AbdeldjalilChougui/webhook-relay/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "relay.db", "SQLite database file")
	keep := flag.Int("keep", 500, "requests kept per channel (0 = all)")
	maxBody := flag.Int64("max-body", 1<<20, "max stored/forwarded body size in bytes")
	timeout := flag.Duration("timeout", 25*time.Second, "how long a webhook waits for the client's answer")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	token := os.Getenv("RELAY_TOKEN")
	if token == "" {
		log.Warn("RELAY_TOKEN is not set: the inspector and websocket are open to anyone")
	}
	st, err := store.Open(*dbPath, *keep)
	if err != nil {
		log.Error("open db", "err", err)
		os.Exit(1)
	}
	defer st.Close()
	srv := server.New(st, server.Config{Token: token, MaxBody: *maxBody, ForwardTimeout: *timeout, Logger: log})
	hs := &http.Server{Addr: *addr, Handler: srv, ReadHeaderTimeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		hs.Shutdown(sctx)
	}()
	log.Info("listening", "addr", *addr, "db", *dbPath)
	if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
