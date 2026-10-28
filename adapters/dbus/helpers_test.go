//go:build dbus

package dbusadapter

import (
	"context"
	"errors"
	"syscall"
	"testing"
)

func TestValidBusName(t *testing.T) {
	cases := map[string]bool{
		"org.unifiedpush.Distributor.tsukuyo": true,
		"org.example.TestApp":                 true,
		"a.b":                                 true,
		"single":                              false,
		"":                                    false,
		"org..double":                         false,
		"org.example.1bad":                    false,
		"org.example.bad char":                false,
		"org.example.ok-1_2":                  true,
	}
	for name, want := range cases {
		if got := validBusName(name); got != want {
			t.Errorf("validBusName(%q) = %v, want %v", name, got, want)
		}
	}
	long := make([]byte, 260)
	for i := range long {
		long[i] = 'a'
	}
	if validBusName(string(long) + ".a") {
		t.Error("overlong name accepted")
	}
}

func TestReason(t *testing.T) {
	if got := reason(context.DeadlineExceeded); got != "NETWORK" {
		t.Errorf("deadline: %s", got)
	}
	if got := reason(syscall.ECONNREFUSED); got != "NETWORK" {
		t.Errorf("errno (net.Error via Timeout): %s", got)
	}
	tcpErr := &netOpErr{err: syscall.ECONNREFUSED}
	if got := reason(tcpErr); got != "NETWORK" {
		t.Errorf("net.Error chain: %s", got)
	}
	if got := reason(errors.New("boom")); got != "INTERNAL_ERROR" {
		t.Errorf("generic: %s", got)
	}
}

type netOpErr struct{ err error }

func (e *netOpErr) Error() string   { return "dial: " + e.err.Error() }
func (e *netOpErr) Timeout() bool   { return false }
func (e *netOpErr) Temporary() bool { return false }
