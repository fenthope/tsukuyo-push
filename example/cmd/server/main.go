// Command server runs the tsukuyo push server (the S in the UnifiedPush
// chain). It mounts the tsukasa-push web push service on a durable bolt
// store behind a tuned HTTP/2 listener.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fenthope/ikumi"
	"github.com/fenthope/tsukasa-push/service"
	"github.com/infinite-iroha/touka"
	"golang.org/x/time/rate"

	"github.com/satomitoka/tsukuyo-push/example/boltstore"
	"github.com/satomitoka/tsukuyo-push/h2srv"
)

func main() {
	var (
		addr     = flag.String("addr", ":8443", "listen address")
		dbPath   = flag.String("db", "tsukuyo.db", "bbolt database path")
		baseURL  = flag.String("base-url", "", "public base URL (derived from requests when empty)")
		certFile = flag.String("tls-cert", "", "TLS certificate; plaintext h2c+HTTP/1.1 when empty")
		keyFile  = flag.String("tls-key", "", "TLS private key")
		subTTL   = flag.Duration("sub-ttl", 30*24*time.Hour, "subscription lifetime (0 never expires)")
		sweep    = flag.Duration("sweep-interval", time.Minute, "expired subscription and message sweep (0 disables)")
		rps      = flag.Float64("rate-limit", 20, "per-application-server push requests per second")
		burst    = flag.Int("rate-burst", 40, "rate limiter burst size")

		// HTTP/2 tuning. max-streams must exceed the number of
		// subscriptions the biggest distributor holds open.
		streams      = flag.Int("max-streams", 512, "max concurrent HTTP/2 streams per connection")
		streamWindow = flag.Int("stream-window", 256<<10, "HTTP/2 per-stream receive window bytes")
		connWindow   = flag.Int("conn-window", 8<<20, "HTTP/2 per-connection receive window bytes")
		idleTimeout  = flag.Duration("idle-timeout", 30*time.Second, "ping after this long without inbound frames")
		pingTimeout  = flag.Duration("ping-timeout", 15*time.Second, "how long a ping may take before closing")

		// Socket and lifecycle. Zero buffers leave the kernel's autotuning
		// in charge.
		recvBuf     = flag.Int("listen-recv-buf", 4<<20, "listener SO_RCVBUF (0 keeps the system default)")
		sendBuf     = flag.Int("listen-send-buf", 4<<20, "listener SO_SNDBUF (0 keeps the system default)")
		shutdownCtx = flag.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown budget")
	)
	flag.Parse()

	store, err := boltstore.Open(*dbPath, boltstore.WithSweepInterval(*sweep))
	if err != nil {
		log.Fatalf("server: store: %v", err)
	}
	defer store.Close()

	engine := touka.Default()
	svc := service.New(
		service.WithStore(store),
		service.WithBaseURL(*baseURL),
		service.WithSubscriptionTTL(*subTTL),
		service.WithRateLimit(ikumi.TokenRateLimiterOptions{
			Limit: rate.Limit(*rps),
			Burst: *burst,
		}),
	)
	svc.Register(engine)

	opts := h2srv.Options{
		MaxConcurrentStreams: *streams,
		InitialStreamWindow:  *streamWindow,
		InitialConnWindow:    *connWindow,
		IdleTimeout:          *idleTimeout,
		PingTimeout:          *pingTimeout,
	}
	if *certFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			log.Fatalf("server: tls: %v", err)
		}
		opts.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}
	srv := h2srv.New(engine, opts)

	ln, err := h2srv.Listen(*addr, h2srv.ListenerOptions{
		RecvBuf: *recvBuf,
		SendBuf: *sendBuf,
	})
	if err != nil {
		log.Fatalf("server: listen: %v", err)
	}
	log.Printf("server: listening on %s (tls=%t)", ln.Addr(), opts.TLSConfig != nil)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	stop := make(chan os.Signal, 2)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	case sig := <-stop:
		log.Printf("server: %v, shutting down", sig)
		ctx, cancel := context.WithTimeout(context.Background(), *shutdownCtx)
		defer cancel()
		if err := srv.Close(ctx); err != nil {
			log.Printf("server: shutdown: %v", err)
		}
	}
}
