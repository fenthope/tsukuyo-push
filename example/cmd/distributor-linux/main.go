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
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/godbus/dbus/v5"

	"github.com/satomitoka/tsukuyo-push/dcore"
	"github.com/satomitoka/tsukuyo-push/dcore/transport"
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

	rt, err := buildTransport(*url, *insecure)
	if err != nil {
		log.Fatalf("distributor-linux: transport: %v", err)
	}
	client, err := dcore.NewClient(*url, dcore.WithTransport(rt))
	if err != nil {
		log.Fatalf("distributor-linux: client: %v", err)
	}
	reg, err := dcore.OpenRegistry(*registry)
	if err != nil {
		log.Fatalf("distributor-linux: registry: %v", err)
	}

	conn, err := dbus.SessionBus()
	if err != nil {
		log.Fatalf("distributor-linux: session bus: %v", err)
	}

	d := &distributorHolder{}
	dist := dcore.NewDistributor(client, reg, d, dcore.Options{
		Logger: logger,
		OnGone: func(reg dcore.Registration) {
			if a := d.adapter; a != nil {
				a.NotifyGone(reg)
			}
		},
	})
	adapter, err := dbusadapter.New(conn, reg, *name, logger)
	if err != nil {
		log.Fatalf("distributor-linux: adapter: %v", err)
	}
	d.adapter = adapter
	adapter.Attach(dist)
	if err := adapter.Serve(); err != nil {
		log.Fatalf("distributor-linux: serve: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := dist.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("distributor-linux: %v", err)
	}
}

// distributorHolder breaks the construction cycle between the
// distributor (needs a Dispatcher) and the adapter (needs the
// distributor). Deliveries only start after Serve, by which time the
// adapter field is set.
type distributorHolder struct {
	adapter *dbusadapter.Adapter
}

func (h *distributorHolder) Deliver(ctx context.Context, token string, body []byte) error {
	if h.adapter == nil {
		return errors.New("distributor-linux: adapter not ready")
	}
	return h.adapter.Deliver(ctx, token, body)
}

func buildTransport(url string, insecure bool) (http.RoundTripper, error) {
	opts := transport.Options{}
	if len(url) >= 8 && url[:8] == "https://" {
		opts.TLSClientConfig = &tls.Config{InsecureSkipVerify: insecure} //nolint:gosec // opt-in testing flag
		return transport.New(opts)
	}
	return transport.NewH2C(opts)
}
