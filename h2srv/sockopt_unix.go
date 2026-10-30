//go:build !windows

package h2srv

import (
	"errors"
	"syscall"
)

// setBufferSizes applies the listener's SO_RCVBUF / SO_SNDBUF. Zero means
// "leave it to the kernel", which is usually the right answer.
func setBufferSizes(fd uintptr, recvBuf, sendBuf int) error {
	var err error
	if recvBuf > 0 {
		err = errors.Join(err, syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, recvBuf))
	}
	if sendBuf > 0 {
		err = errors.Join(err, syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, sendBuf))
	}
	return err
}
