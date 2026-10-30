// Package transport builds aggressively tuned HTTP/2 clients for the
// distributor-to-server channel.
//
// The whole channel runs over ONE TCP connection: HTTP/2 multiplexes the
// concurrent long-poll streams, acknowledgements and subscription calls
// over it, and MaxConnsPerHost pins that single connection. Tuning starts
// below TLS, at the socket options, and continues with http.HTTP2Config
// settings that were only reachable through x/net/http2 before Go 1.24
// (idle ping probes, receive windows, HPACK table sizes).
//
// What Options{} actually does. The only socket option written
// unconditionally is TCP_NODELAY. Keepalive runs with the dialer's 30s
// idle and the OS probe interval and count until Idle, Intvl or Cnt is
// set; TCP_USER_TIMEOUT, the buffer sizes, IP_TTL and IP_TOS stay off.
// A half-open peer is therefore detected by the HTTP/2 idle PING (20s
// without inbound frames, 10s for the pong), not by keepalive probes;
// TCP_USER_TIMEOUT adds a second, transport-level bound on unacknowledged
// data when the platform supports it. The underlying transport's Proxy is
// left nil on purpose: this channel is point to point, so HTTPS_PROXY and
// friends are deliberately ignored.
package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"syscall"
	"time"
)

// Options configures the socket and the HTTP/2 transport. Zero values keep
// the defaults noted on each field.
type Options struct {
	// Socket tuning.
	RecvBuf int   // SO_RCVBUF
	SendBuf int   // SO_SNDBUF
	NoDelay *bool // TCP_NODELAY; nil means true
	TTL     uint8 // IP_TTL; 0 keeps the default
	Tos     int   // IP_TOS; 0 keeps the default
	// Keepalive timing goes through net.Dialer.KeepAliveConfig, so it works
	// on every platform the standard library supports. Setting any of them
	// enables probes; with all three unset the dialer's 30s idle applies
	// with the OS defaults for the interval and the count.
	Idle  time.Duration // TCP_KEEPIDLE
	Intvl time.Duration // TCP_KEEPINTVL
	Cnt   int           // TCP_KEEPCNT
	// UserTimeout fails a connection whose data stays unacknowledged
	// (half-open peer) after this long; TCP_USER_TIMEOUT on Linux.
	UserTimeout time.Duration

	// HTTP/2 behaviour.
	// SendPingTimeout sends a PING after this span without inbound frames
	// (default 20s); PingTimeout closes the connection when the pong
	// stalls (default 10s). Together they detect half-open connections
	// faster than TCP keepalive.
	SendPingTimeout time.Duration
	PingTimeout     time.Duration
	// ConnWindow and StreamWindow are the per-connection and per-stream
	// receive windows (default 8 MiB / 256 KiB). Many blocked long-poll
	// streams share the connection window.
	ConnWindow   int
	StreamWindow int
	// HeaderTableSize bounds the HPACK decoding table (default 4 KiB):
	// the server pushes small headers on every poll response.
	HeaderTableSize int
	// MaxConnsPerHost pins connection reuse (default 1): HTTP/2 streams
	// multiplex, extra connections would only leak battery.
	MaxConnsPerHost int

	WriteBufferSize int // 32 KiB
	ReadBufferSize  int // 32 KiB

	// TLSClientConfig is used by New; NextProtos gets "h2" when empty
	// and client session resumption is always enabled.
	TLSClientConfig *tls.Config

	// Dial replaces the base dialer entirely (tests, proxies). Optional.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
}

// New returns a TLS transport negotiating h2 via ALPN.
func New(o Options) (*http.Transport, error) {
	rt, err := base(o)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if o.TLSClientConfig != nil {
		cfg = o.TLSClientConfig.Clone()
	}
	if len(cfg.NextProtos) == 0 {
		cfg.NextProtos = []string{"h2"}
	}
	if cfg.ClientSessionCache == nil {
		cfg.ClientSessionCache = tls.NewLRUClientSessionCache(16)
	}
	rt.TLSClientConfig = cfg
	rt.ForceAttemptHTTP2 = true
	p := &http.Protocols{}
	p.SetHTTP2(true)
	rt.Protocols = p
	return rt, nil
}

// NewH2C returns a transport speaking prior-knowledge h2c for local or
// TLS-terminated deployments.
func NewH2C(o Options) (*http.Transport, error) {
	rt, err := base(o)
	if err != nil {
		return nil, err
	}
	p := &http.Protocols{}
	p.SetUnencryptedHTTP2(true)
	rt.Protocols = p
	return rt, nil
}

func base(o Options) (*http.Transport, error) {
	dial, err := o.baseDialer()
	if err != nil {
		return nil, err
	}
	rt := &http.Transport{
		DialContext:        dial,
		DisableCompression: true,
		// One pooled connection, never idle-expired: the long-poll
		// streams keep it hot anyway.
		MaxConnsPerHost:       o.MaxConnsPerHost,
		MaxIdleConns:          1,
		MaxIdleConnsPerHost:   1,
		IdleConnTimeout:       0,
		WriteBufferSize:       orDefault(o.WriteBufferSize, 32<<10),
		ReadBufferSize:        orDefault(o.ReadBufferSize, 32<<10),
		ExpectContinueTimeout: 0,
	}
	if rt.MaxConnsPerHost == 0 {
		rt.MaxConnsPerHost = 1
	}
	rt.HTTP2 = &http.HTTP2Config{
		SendPingTimeout:               orDuration(o.SendPingTimeout, 20*time.Second),
		PingTimeout:                   orDuration(o.PingTimeout, 10*time.Second),
		MaxReceiveBufferPerConnection: orDefault(o.ConnWindow, 8<<20),
		MaxReceiveBufferPerStream:     orDefault(o.StreamWindow, 256<<10),
		MaxDecoderHeaderTableSize:     orDefault(o.HeaderTableSize, 4<<10),
		MaxEncoderHeaderTableSize:     orDefault(o.HeaderTableSize, 4<<10),
	}
	return rt, nil
}

func orDefault(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

func orDuration(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return d
}

func (o Options) baseDialer() (func(ctx context.Context, network, addr string) (net.Conn, error), error) {
	if o.Dial != nil {
		return o.Dial, nil
	}
	if o.UserTimeout < 0 {
		return nil, errors.New("transport: negative UserTimeout")
	}
	d := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	if o.Idle > 0 || o.Intvl > 0 || o.Cnt > 0 {
		// The dialer applies keepalive after Control runs, so writing
		// TCP_KEEP{IDLE,INTVL,CNT} from Control gets overwritten. Hand the
		// tuning to the dialer instead; zero fields keep its defaults.
		d.KeepAliveConfig = net.KeepAliveConfig{
			Enable:   true,
			Idle:     orDuration(o.Idle, 30*time.Second),
			Interval: o.Intvl,
			Count:    o.Cnt,
		}
	}
	opts := o
	d.Control = func(_, _ string, rc syscall.RawConn) error {
		var serr error
		if err := rc.Control(func(fd uintptr) {
			serr = setSocketOptions(int(fd), &opts)
		}); err != nil {
			return err
		}
		return serr
	}
	return d.DialContext, nil
}
