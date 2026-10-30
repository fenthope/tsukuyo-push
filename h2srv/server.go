// Package h2srv serves an http.Handler over a tuned listener with explicit
// HTTP/2 configuration via the standard library's http.HTTP2Config.
//
// In TLS mode ALPN offers "h2" next to HTTP/1.1. Without TLS the listener
// accepts prior-knowledge h2c next to HTTP/1.1, which is meant for local
// deployments behind a TLS-terminating proxy and for tests.
package h2srv

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"syscall"
	"time"
)

// Options configures the server.
type Options struct {
	// TLSConfig enables TLS mode; "h2" is offered via ALPN next to
	// HTTP/1.1. Without it the listener is plaintext (h2c + HTTP/1.1).
	TLSConfig *tls.Config

	// MaxConcurrentStreams bounds concurrent streams per connection
	// (default 512). It must exceed the number of long-poll streams a
	// distributor holds open plus headroom for its acks and
	// subscriptions, or streams get refused and re-established.
	MaxConcurrentStreams int

	// InitialStreamWindow and InitialConnWindow set the HTTP/2 receive
	// flow control windows (default 256 KiB and 8 MiB). Many concurrent
	// blocked long-poll streams share the connection window, so it should
	// be generous.
	InitialStreamWindow int
	InitialConnWindow   int

	// IdleTimeout is how long the server waits for any inbound frame
	// before pinging the connection, the half-open detector for parked
	// distributors (default 30s).
	IdleTimeout time.Duration

	// PingTimeout bounds the ping round trip of an idle probe (default
	// 15s). Together with IdleTimeout, a dead peer costs this much to
	// notice, and it should stay under the NAT lifetime you care about.
	PingTimeout time.Duration

	// ReadHeaderTimeout applies to the HTTP/1.1 path (default 10s).
	ReadHeaderTimeout time.Duration
}

// Server is a tuned HTTP/2 + HTTP/1.1 server bound to one handler.
type Server struct {
	http1 *http.Server
}

// New creates a Server for handler.
func New(handler http.Handler, opts Options) *Server {
	if opts.MaxConcurrentStreams == 0 {
		opts.MaxConcurrentStreams = 512
	}
	if opts.InitialStreamWindow == 0 {
		opts.InitialStreamWindow = 256 << 10
	}
	if opts.InitialConnWindow == 0 {
		opts.InitialConnWindow = 8 << 20
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = 30 * time.Second
	}
	if opts.PingTimeout == 0 {
		opts.PingTimeout = 15 * time.Second
	}
	if opts.ReadHeaderTimeout == 0 {
		opts.ReadHeaderTimeout = 10 * time.Second
	}

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: opts.ReadHeaderTimeout,
		HTTP2: &http.HTTP2Config{
			MaxConcurrentStreams:          opts.MaxConcurrentStreams,
			MaxReceiveBufferPerConnection: opts.InitialConnWindow,
			MaxReceiveBufferPerStream:     opts.InitialStreamWindow,
			SendPingTimeout:               opts.IdleTimeout,
			PingTimeout:                   opts.PingTimeout,
		},
	}
	p := &http.Protocols{}
	p.SetHTTP1(true)
	if opts.TLSConfig != nil {
		cfg := opts.TLSConfig.Clone()
		if len(cfg.NextProtos) == 0 {
			cfg.NextProtos = []string{"h2", "http/1.1"}
		}
		srv.TLSConfig = cfg
		p.SetHTTP2(true)
	} else {
		p.SetUnencryptedHTTP2(true)
	}
	srv.Protocols = p
	return &Server{http1: srv}
}

// Serve accepts connections on ln until Close is called.
func (s *Server) Serve(ln net.Listener) error {
	if s.http1.TLSConfig != nil {
		ln = tls.NewListener(ln, s.http1.TLSConfig)
	}
	err := s.http1.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Close shuts the server down gracefully within ctx's deadline.
func (s *Server) Close(ctx context.Context) error {
	return s.http1.Shutdown(ctx)
}

// ListenerOptions tunes the TCP listener underneath. Zero values leave the
// socket to the kernel, whose autotuning is usually the right answer; set
// these only when a measurement says otherwise.
type ListenerOptions struct {
	RecvBuf int // SO_RCVBUF
	SendBuf int // SO_SNDBUF
}

// Listen opens a tuned TCP listener on addr.
func Listen(addr string, o ListenerOptions) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			var serr error
			err := c.Control(func(fd uintptr) {
				serr = setBufferSizes(fd, o.RecvBuf, o.SendBuf)
			})
			return errors.Join(err, serr)
		},
	}
	return lc.Listen(context.Background(), "tcp", addr)
}
