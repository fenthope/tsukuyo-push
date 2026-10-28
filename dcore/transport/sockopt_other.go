//go:build !linux

package transport

import (
	"golang.org/x/sys/unix"
)

// setSocketOptions applies the portable subset. Kernel keepalive tuning
// beyond the interval is left to net.Dialer.KeepAlive; TCP_USER_TIMEOUT is
// Linux-only.
func setSocketOptions(fd int, o *Options) error {
	nodelay := o.NoDelay == nil || *o.NoDelay
	if nodelay {
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1); err != nil {
			return err
		}
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
