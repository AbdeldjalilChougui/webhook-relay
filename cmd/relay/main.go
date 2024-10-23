// Command relay connects to a relay server and forwards a channel's webhooks
// to a local URL.
//
//	RELAY_TOKEN=… relay -server https://relay.example.com -channel stripe -to http://localhost:3000
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AbdeldjalilChougui/webhook-relay/internal/client"
)

func main() {
	srv := flag.String("server", "http://localhost:8080", "relay server URL")
	channel := flag.String("channel", "", "channel name (letters, digits, - and _)")
	to := flag.String("to", "http://localhost:3000", "local URL to forward webhooks to")
	flag.Parse()
	if *channel == "" {
		fmt.Fprintln(os.Stderr, "relay: -channel is required")
		os.Exit(2)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	c := &client.Client{
		Server: *srv, Channel: *channel, Token: os.Getenv("RELAY_TOKEN"), Target: *to, Log: log,
		OnForward: func(method, path string, status int, d time.Duration, err error) {
			if err != nil {
				fmt.Printf("%s %-6s %s → error: %v\n", time.Now().Format("15:04:05"), method, path, err)
				return
			}
			fmt.Printf("%s %-6s %s → %d (%s)\n", time.Now().Format("15:04:05"), method, path, status, d.Round(time.Millisecond))
		},
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("Forwarding %s/h/%s → %s\n", *srv, *channel, *to)
	if err := c.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("relay", "err", err)
		os.Exit(1)
	}
}
