package dcore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/satomitoka/tsukuyo-push/dcore/transport"
	"github.com/satomitoka/tsukuyo-push/h2srv"
)

// serveH2C runs the shipped tuned server over a plaintext socket, the way a
// deployment behind a TLS-terminating proxy would.
func serveH2C(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := h2srv.New(subscribeHandler(t), h2srv.Options{})
	go srv.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Close(ctx)
	})
	return "http://" + ln.Addr().String()
}

// subscribeHandler answers POST /subscribe the way a push service must:
// 201 with the subscription resource in Location and the push resource in a
// Link header (RFC 8030 §4).
func subscribeHandler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subscribe" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Location", "/subscription/abc")
		w.Header().Set("Link", `</push/abc>; rel="urn:ietf:params:push"`)
		w.WriteHeader(http.StatusCreated)
	})
}

func trustServer(srv *httptest.Server) *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	return pool
}

// TestNewClientUsesTLSForHTTPS is the regression for the scheme blind
// constructor: an https URL has to be met with a TLS handshake, not with
// prior-knowledge h2c.
func TestNewClientUsesTLSForHTTPS(t *testing.T) {
	srv := httptest.NewUnstartedServer(subscribeHandler(t))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	client, err := NewClient(srv.URL, WithTransportOptions(transport.Options{
		TLSClientConfig: &tls.Config{RootCAs: trustServer(srv)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := client.Subscribe(t.Context(), "")
	if err != nil {
		t.Fatalf("subscribe over https: %v", err)
	}
	if want := srv.URL + "/push/abc"; sub.PushURI != want {
		t.Errorf("PushURI = %q, want %q", sub.PushURI, want)
	}
}

// TestNewClientSpeaksH2CForHTTP covers the other branch: a plaintext URL is
// spoken to with prior-knowledge h2c.
func TestNewClientSpeaksH2CForHTTP(t *testing.T) {
	base := serveH2C(t)
	client, err := NewClient(base)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := client.Subscribe(t.Context(), "")
	if err != nil {
		t.Fatalf("subscribe over h2c: %v", err)
	}
	if want := base + "/push/abc"; sub.PushURI != want {
		t.Errorf("PushURI = %q, want %q", sub.PushURI, want)
	}
}

// TestWithTransportOptionsForwarded checks that the tuning actually reaches
// the built transport instead of being dropped on the floor.
func TestWithTransportOptionsForwarded(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:1", WithTransportOptions(transport.Options{
		ConnWindow:  1 << 20,
		PingTimeout: 3 * time.Second,
	}))
	if err != nil {
		t.Fatal(err)
	}
	rt, ok := client.http.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T", client.http.Transport)
	}
	if got := rt.HTTP2.MaxReceiveBufferPerConnection; got != 1<<20 {
		t.Errorf("MaxReceiveBufferPerConnection = %d, want %d", got, 1<<20)
	}
	if got := rt.HTTP2.PingTimeout.Seconds(); got != 3 {
		t.Errorf("PingTimeout = %v, want 3s", rt.HTTP2.PingTimeout)
	}
	if rt.MaxConnsPerHost != 1 {
		t.Errorf("MaxConnsPerHost = %d, want 1 (one reused connection)", rt.MaxConnsPerHost)
	}
}
