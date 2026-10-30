//go:build linux

package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// dialedFD runs the production dial path (net.Dialer plus the Control hook
// that applies Options) against a local listener and returns the client
// socket's file descriptor, so a test can read what the kernel actually
// ended up with rather than what setSocketOptions intended to write.
func dialedFD(t *testing.T, o Options) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	accepted := make(chan struct{})
	go func() {
		c, err := ln.Accept()
		if err == nil {
			c.Close()
		}
		close(accepted)
	}()
	rt, err := base(o)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := rt.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); <-accepted })
	return rawFD(t, conn)
}

func get(t *testing.T, fd, level, name int) int {
	t.Helper()
	v, err := unix.GetsockoptInt(fd, level, name)
	if err != nil {
		t.Fatalf("getsockopt(%d,%d): %v", level, name, err)
	}
	return v
}

// TestDefaultDialedSocketOptions records the socket state the default
// Options{} actually produces, so the README's tuning claims stay honest:
// only TCP_NODELAY is ours, keepalive timing comes from net.Dialer, and
// TCP_USER_TIMEOUT stays off unless explicitly asked for.
func TestDefaultDialedSocketOptions(t *testing.T) {
	fd := dialedFD(t, Options{})

	if v := get(t, fd, unix.IPPROTO_TCP, unix.TCP_NODELAY); v != 1 {
		t.Errorf("TCP_NODELAY = %d, want 1", v)
	}
	if v := get(t, fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE); v != 1 {
		t.Errorf("SO_KEEPALIVE = %d, want 1 (net.Dialer.KeepAlive)", v)
	}
	if v := get(t, fd, unix.IPPROTO_TCP, unix.TCP_KEEPIDLE); v != 30 {
		t.Errorf("TCP_KEEPIDLE = %d, want 30 (the dialer default)", v)
	}
	if v := get(t, fd, unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT); v != 0 {
		t.Errorf("TCP_USER_TIMEOUT = %d, want 0 (off by default)", v)
	}
}

// TestDialedKeepAliveTuningWins is the regression for the ordering between
// our Control hook and net.Dialer's own keepalive application: if the
// dialer writes TCP_KEEPIDLE after Control runs, every keepalive knob in
// Options is silently dead.
func TestDialedKeepAliveTuningWins(t *testing.T) {
	fd := dialedFD(t, Options{
		Idle:        15 * time.Second,
		Intvl:       5 * time.Second,
		Cnt:         4,
		UserTimeout: 25 * time.Second,
	})

	if v := get(t, fd, unix.IPPROTO_TCP, unix.TCP_KEEPIDLE); v != 15 {
		t.Errorf("TCP_KEEPIDLE = %d, want 15 (Options.Idle)", v)
	}
	if v := get(t, fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL); v != 5 {
		t.Errorf("TCP_KEEPINTVL = %d, want 5 (Options.Intvl)", v)
	}
	if v := get(t, fd, unix.IPPROTO_TCP, unix.TCP_KEEPCNT); v != 4 {
		t.Errorf("TCP_KEEPCNT = %d, want 4 (Options.Cnt)", v)
	}
	if v := get(t, fd, unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT); v != 25000 {
		t.Errorf("TCP_USER_TIMEOUT = %d, want 25000 (Options.UserTimeout)", v)
	}
}
