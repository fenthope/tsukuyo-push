package h2srv_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/satomitoka/tsukuyo-push/dcore/transport"
	"github.com/satomitoka/tsukuyo-push/internal/h2srv"
)

type countingListener struct {
	net.Listener
	conns *atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.conns.Add(1)
	return c, nil
}

func startServer(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	var conns atomic.Int64
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln = &countingListener{Listener: ln, conns: &conns}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "proto=%s path=%s", r.Proto, r.URL.Path)
	})
	srv := h2srv.New(handler, h2srv.Options{})
	go srv.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Close(ctx)
	})
	return "http://" + ln.Addr().String(), &conns
}

func TestH2CMultiplexesSingleConnection(t *testing.T) {
	base, conns := startServer(t)

	rt, err := transport.NewH2C(transport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: rt}

	const n = 32
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			resp, err := client.Get(fmt.Sprintf("%s/p/%d", base, i))
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if !strings.HasPrefix(string(body), "proto=HTTP/2.0") {
				errs <- fmt.Errorf("request %d: body = %q", i, body)
				return
			}
			errs <- nil
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if got := conns.Load(); got != 1 {
		t.Fatalf("TCP connections = %d, want 1", got)
	}
}

func TestHTTP1Fallback(t *testing.T) {
	base, conns := startServer(t)
	client := &http.Client{
		Transport: &http.Transport{ForceAttemptHTTP2: false},
	}
	resp, err := client.Get(base + "/plain")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.HasPrefix(string(body), "proto=HTTP/1.1") {
		t.Fatalf("body = %q", body)
	}
	if conns.Load() < 1 {
		t.Fatal("no connection counted")
	}
}
