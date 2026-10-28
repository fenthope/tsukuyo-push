//go:build linux

package transport

import (
	"time"

	"golang.org/x/sys/unix"
)

// setSocketOptions applies the full tuning set on Linux (servers and
// Android, which shares these socket options).
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
	if o.Idle > 0 || o.Intvl > 0 || o.Cnt > 0 {
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPIDLE, secs(o.Idle, 30)); err != nil {
			return err
		}
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, secs(o.Intvl, 10)); err != nil {
			return err
		}
		if o.Cnt > 0 {
			if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPCNT, o.Cnt); err != nil {
				return err
			}
		}
	}
	if o.UserTimeout > 0 {
		ms := int(uint32(o.UserTimeout / time.Millisecond))
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, ms); err != nil {
			return err
		}
	}
	return nil
}

func secs(d time.Duration, def int) int {
	if d <= 0 {
		return def
	}
	return int(d / time.Second)
}
