//go:build windows

package transport

import "syscall"

// setSocketOptions applies the Windows subset through the standard library:
// TCP_NODELAY and the buffer sizes. Keepalive timing comes from
// net.Dialer.KeepAliveConfig; TCP_USER_TIMEOUT, IP_TTL and IP_TOS have no
// equivalent here and are left alone.
func setSocketOptions(fd int, o *Options) error {
	val := 0
	if o.NoDelay == nil || *o.NoDelay {
		val = 1
	}
	if err := syscall.SetsockoptInt(syscall.Handle(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, val); err != nil {
		return err
	}
	if o.RecvBuf > 0 {
		if err := syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, o.RecvBuf); err != nil {
			return err
		}
	}
	if o.SendBuf > 0 {
		if err := syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, o.SendBuf); err != nil {
			return err
		}
	}
	return nil
}
