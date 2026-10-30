// Command distributor-linux runs the Linux distributor: the dcore push
// client plus the UnifiedPush D-Bus adapter on the session bus.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/godbus/dbus/v5"

	"github.com/satomitoka/tsukuyo-push/dcore"
	"github.com/satomitoka/tsukuyo-push/dcore/transport"
	"github.com/satomitoka/tsukuyo-push/example/filestore"
	"github.com/satomitoka/tsukuyo-push/platform/dbus"
)

func main() {
	var (
		url      = flag.String("url", "http://127.0.0.1:8443", "push server URL")
		registry = flag.String("registry", "registry.json", "registry file path")
		name     = flag.String("name", dbusadapter.DefaultName, "distributor name (bus name suffix)")
		insecure = flag.Bool("fp", false, "skip TLS certificate verification (testing only)")
	)
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	tOpts := transport.Options{}
	if *insecure {
		tOpts.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in testing flag
	}
	client, err := dcore.NewClient(*url, dcore.WithTransportOptions(tOpts))
	if err != nil {
		log.Fatalf("distributor-linux: client: %v", err)
	}
	store, err := filestore.Open(*registry)
	if err != nil {
		log.Fatalf("distributor-linux: store: %v", err)
	}

	conn, err := dbus.SessionBus()
	if err != nil {
		log.Fatalf("distributor-linux: session bus: %v", err)
	}

	// The adapter delivers messages and serves registrations, so the two
	// reference each other. Building the adapter first and handing its
	// Deliver method to the distributor keeps the wiring one-directional.
	adapter, err := dbusadapter.New(conn, *name, logger)
	if err != nil {
		log.Fatalf("distributor-linux: adapter: %v", err)
	}
	dist := dcore.NewDistributor(client, dcore.DispatcherFunc(adapter.Deliver), dcore.Options{
		Logger:    logger,
		OnChanged: store.Apply,
		OnGone:    adapter.NotifyGone,
	})
	adapter.Attach(dist)
	if err := adapter.Serve(); err != nil {
		log.Fatalf("distributor-linux: serve: %v", err)
	}
	if err := dist.Restore(store.Registrations()); err != nil {
		log.Fatalf("distributor-linux: restore: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := dist.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("distributor-linux: %v", err)
	}
}
