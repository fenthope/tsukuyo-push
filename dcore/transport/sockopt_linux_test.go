//go:build linux

package transport

import (
	"net"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// tcpFDs returns the raw file descriptors of the two ends of a loopback TCP
// connection. TCP options are rejected on unix-domain sockets, so a real
// TCP pair is required.
func tcpFDs(t *testing.T) (int, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	type res struct {
		c   net.Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		ch <- res{c, err}
	}()
	cc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	sc := <-ch
	if sc.err != nil {
		t.Fatalf("accept: %v", sc.err)
	}
	t.Cleanup(func() { cc.Close(); sc.c.Close() })
	return rawFD(t, cc), rawFD(t, sc.c)
}

func rawFD(t *testing.T, c net.Conn) int {
	t.Helper()
	rc, err := c.(interface {
		SyscallConn() (syscall.RawConn, error)
	}).SyscallConn()
	if err != nil {
		t.Fatalf("syscallconn: %v", err)
	}
	var fd int
	rc.Control(func(f uintptr) { fd = int(f) })
	return fd
}

func TestSetSocketOptions(t *testing.T) {
	fd1, _ := tcpFDs(t)

	nodelay := false // explicitly disable despite TCP default
	o := &Options{
		NoDelay:     &nodelay,
		RecvBuf:     64 << 10,
		SendBuf:     64 << 10,
		Idle:        15 * time.Second,
		Intvl:       5 * time.Second,
		Cnt:         4,
		UserTimeout: 25 * time.Second,
	}
	if err := setSocketOptions(fd1, o); err != nil {
		t.Fatalf("setSocketOptions: %v", err)
	}
	if v, err := unix.GetsockoptInt(fd1, unix.SOL_SOCKET, unix.SO_RCVBUF); err != nil || v < 64<<10 {
		t.Fatalf("SO_RCVBUF = %d, %v", v, err)
	}
	if v, err := unix.GetsockoptInt(fd1, unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT); err != nil || v != 25000 {
		t.Fatalf("TCP_USER_TIMEOUT = %d, %v", v, err)
	}
	if v, err := unix.GetsockoptInt(fd1, unix.IPPROTO_TCP, unix.TCP_KEEPIDLE); err != nil || v != 15 {
		t.Fatalf("TCP_KEEPIDLE = %d, %v", v, err)
	}
	if v, err := unix.GetsockoptInt(fd1, unix.IPPROTO_TCP, unix.TCP_KEEPCNT); err != nil || v != 4 {
		t.Fatalf("TCP_KEEPCNT = %d, %v", v, err)
	}
}

func TestSetSocketOptionsDefaultsNodelay(t *testing.T) {
	fd1, _ := tcpFDs(t)
	if err := setSocketOptions(fd1, &Options{}); err != nil {
		t.Fatalf("setSocketOptions: %v", err)
	}
	if v, err := unix.GetsockoptInt(fd1, unix.IPPROTO_TCP, unix.TCP_NODELAY); err != nil || v != 1 {
		t.Fatalf("TCP_NODELAY = %d, %v (want 1)", v, err)
	}
}
