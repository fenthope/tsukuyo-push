// Command distributor runs a reference UnifiedPush distributor core (the D
// in the chain). It long-polls the push server over one reused HTTP/2
// connection and hands each encrypted message to the platform adapter via
// the Dispatcher interface.
//
// This binary ships a logging adapter only (no Android intent / D-Bus), so
// it demonstrates and lets you test the S<->D transport; real deployments
// embed dcore and wire a platform Dispatcher.
//
//	distributor -url https://push.example.com -registry registry.json \
//	    -register my-app-token
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

	"github.com/satomitoka/tsukuyo-push/dcore"
	"github.com/satomitoka/tsukuyo-push/dcore/transport"
)

// logAdapter is the reference Dispatcher: it records deliveries. A real
// adapter forwards the ciphertext to the end user application.
type logAdapter struct{ log *slog.Logger }

func (a logAdapter) Deliver(ctx context.Context, token string, body []byte) error {
	a.log.Info("deliver", "token", token, "bytes", len(body))
	return nil
}

func main() {
	var (
		url      = flag.String("url", "http://127.0.0.1:8443", "push server URL")
		registry = flag.String("registry", "registry.json", "registry file path")
		insecure = flag.Bool("fp", false, "skip TLS certificate verification (testing only)")
		register = flag.String("register", "", "connection token to register at startup, empty to resume existing only")
		label    = flag.String("label", "", "label for the startup registration")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	tOpts := transport.Options{}
	if *insecure {
		tOpts.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in testing flag
	}
	client, err := dcore.NewClient(*url, dcore.WithTransportOptions(tOpts))
	if err != nil {
		log.Fatalf("distributor: client: %v", err)
	}
	reg, err := dcore.OpenRegistry(*registry)
	if err != nil {
		log.Fatalf("distributor: registry: %v", err)
	}
	dist := dcore.NewDistributor(client, reg, logAdapter{logger}, dcore.Options{
		Logger: logger,
		OnGone: func(reg dcore.Registration) {
			logger.Warn("endpoint gone, application must re-register", "token", reg.ConnectionToken)
		},
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *register != "" {
		endpoint, err := dist.Register(ctx, *register, *register, *label, "")
		if err != nil {
			log.Fatalf("distributor: register: %v", err)
		}
		logger.Info("endpoint ready", "token", *register, "endpoint", endpoint)
	}

	if err := dist.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("distributor: %v", err)
	}
}
