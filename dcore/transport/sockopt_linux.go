//go:build linux

package transport

import (
	"time"

	"golang.org/x/sys/unix"
)

// setSocketOptions applies the full tuning set on Linux (servers and
// Android, which shares these socket options).
func setSocketOptions(fd int, o *Options) error {
	// Written explicitly: the kernel default for TCP_NODELAY is on, so
	// skipping the call would silently ignore an explicit false.
	val := 0
	if o.NoDelay == nil || *o.NoDelay {
		val = 1
	}
	if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, val); err != nil {
		return err
	}
	if o.RecvBuf > 0 {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, o.RecvBuf); err != nil {
			return err
		}
	}
	if o.SendBuf > 0 {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, o.SendBuf); err != nil {
			return err
		}
	}
	if o.TTL > 0 {
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_IP, unix.IP_TTL, int(o.TTL)); err != nil {
			return err
		}
	}
	if o.Tos != 0 {
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_IP, unix.IP_TOS, o.Tos); err != nil {
			return err
		}
	}
	// Keepalive probes are applied by net.Dialer.KeepAliveConfig, which
	// runs after this hook; setting them here would be overwritten.
	if o.UserTimeout > 0 {
		ms := int(uint32(o.UserTimeout / time.Millisecond))
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, ms); err != nil {
			return err
		}
	}
	return nil
}
