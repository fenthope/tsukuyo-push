//go:build !linux && !windows

package transport

import (
	"golang.org/x/sys/unix"
)

// setSocketOptions applies the portable subset. Keepalive timing, including
// the probe interval and count, goes through net.Dialer.KeepAliveConfig;
// TCP_USER_TIMEOUT, IP_TTL and IP_TOS are not set here.
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
	return nil
}
