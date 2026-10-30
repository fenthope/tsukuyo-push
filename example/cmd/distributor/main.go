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
	"github.com/satomitoka/tsukuyo-push/example/filestore"
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
		url          = flag.String("url", "http://127.0.0.1:8443", "push server URL")
		registry     = flag.String("registry", "registry.json", "registry file path")
		insecure     = flag.Bool("fp", false, "skip TLS certificate verification (testing only)")
		register     = flag.String("register", "", "connection token to register at startup, empty to resume existing only")
		label        = flag.String("label", "", "label for the startup registration")
		pollRetry    = flag.Duration("poll-retry-delay", 0, "first pause before retrying a failed poll (default 1s)")
		maxPoll      = flag.Duration("max-poll-retry-delay", 0, "cap for the poll retry backoff (default 60s)")
		deliverRetry = flag.Duration("deliver-retry-delay", 0, "pause before retrying a failed delivery (default 2s)")
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
	// The library keeps registrations in memory and reports every change;
	// filestore is the application's half of the deal.
	store, err := filestore.Open(*registry)
	if err != nil {
		log.Fatalf("distributor: store: %v", err)
	}
	dist := dcore.NewDistributor(client, logAdapter{logger}, dcore.Options{
		Logger:            logger,
		OnChanged:         store.Apply,
		PollRetryDelay:    *pollRetry,
		MaxPollRetryDelay: *maxPoll,
		DeliverRetryDelay: *deliverRetry,
		OnGone: func(reg dcore.Registration) {
			logger.Warn("endpoint gone, application must re-register", "token", reg.ConnectionToken)
		},
	})
	if err := dist.Restore(store.Registrations()); err != nil {
		log.Fatalf("distributor: restore: %v", err)
	}

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
