// Package integration wires the full S and D sides together over a real
// socket and verifies a UnifiedPush message flow end to end: the push
// server, the distributor's long-poll loop, and application-server
// encryption (RFC 8291) that the distributor relays without ever seeing.
package integration

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fenthope/tsukasa-push/pushmsg"
	"github.com/fenthope/tsukasa-push/sender"
	"github.com/fenthope/tsukasa-push/service"
	"github.com/fenthope/tsukasa-push/upush"
	"github.com/fenthope/tsukasa-push/vapid"
	"github.com/infinite-iroha/touka"

	"github.com/satomitoka/tsukuyo-push/dcore"
	"github.com/satomitoka/tsukuyo-push/dcore/transport"
	"github.com/satomitoka/tsukuyo-push/internal/boltstore"
	"github.com/satomitoka/tsukuyo-push/internal/h2srv"
)

// connector plays the end user application: it owns the UA key material
// and decrypts what the distributor relays.
type connector struct {
	t    *testing.T
	keys *pushmsg.UAKeys

	mu       sync.Mutex
	failNext bool

	delivered chan []byte
}

func (c *connector) Deliver(ctx context.Context, token string, body []byte) error {
	if token == "" {
		c.t.Error("delivery without connection token")
	}
	c.mu.Lock()
	fail := c.failNext
	c.failNext = false
	c.mu.Unlock()
	if fail {
		return errors.New("connector busy")
	}
	plain, err := pushmsg.Decrypt(body, c.keys.AuthSecret, c.keys.Private)
	if err != nil {
		c.t.Errorf("decrypt: %v", err)
		return err
	}
	c.delivered <- plain
	return nil
}

func newServer(t *testing.T) (string, *boltstore.Store) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	store, err := boltstore.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	engine := touka.Default()
	svc := service.New(service.WithStore(store))
	svc.Register(engine)
	srv := h2srv.New(engine, h2srv.Options{})
	go srv.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Close(ctx)
		store.Close()
	})
	return "http://" + ln.Addr().String(), store
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestUnifiedPushFlow(t *testing.T) {
	base, store := newServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The app's connector generates the end-to-end keys.
	keys, err := pushmsg.GenerateUAKeys()
	if err != nil {
		t.Fatal(err)
	}
	cn := &connector{t: t, keys: keys, delivered: make(chan []byte, 16), failNext: true}

	// The distributor, with a dial counter to prove connection reuse.
	var dials atomic.Int64
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		dials.Add(1)
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}
	rt, err := transport.NewH2C(transport.Options{Dial: dial})
	if err != nil {
		t.Fatal(err)
	}
	client, err := dcore.NewClient(base, dcore.WithTransport(rt))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := dcore.OpenRegistry(filepath.Join(t.TempDir(), "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	dist := dcore.NewDistributor(client, registry, cn, dcore.Options{
		Logger: silentLogger(),
	})
	go dist.Run(ctx)

	// Registration hands the endpoint to the app.
	endpoint, err := dist.Register(ctx, "tok-1", "test app", "")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// The application server pushes through the upush wire format.
	vk, err := vapid.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := vapid.NewSigner(vk)
	if err != nil {
		t.Fatal(err)
	}
	app := sender.New(sender.WithVAPID(signer, "mailto:ops@example.com"))
	upSub := &upush.Subscription{
		Endpoint:  endpoint,
		P256DHB64: base64.RawURLEncoding.EncodeToString(keys.Public.Bytes()),
		AuthB64:   base64.RawURLEncoding.EncodeToString(keys.AuthSecret),
	}
	push := func(text string) {
		t.Helper()
		if err := upush.Push(ctx, app, upSub, sender.Message{
			Payload: []byte(text), TTL: 300, Urgency: pushmsg.UrgencyNormal,
		}); err != nil {
			t.Fatalf("push %q: %v", text, err)
		}
	}

	// First message; the connector fails once up front so the retry path
	// before acknowledgement runs too.
	push("hello")
	select {
	case plain := <-cn.delivered:
		if string(plain) != "hello" {
			t.Fatalf("payload = %q, want hello", plain)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for first message")
	}

	// Give the ack a moment, then a second message confirms the poll loop
	// continues and messages stay ordered.
	push("second")
	push("third")
	for _, want := range []string{"second", "third"} {
		select {
		case plain := <-cn.delivered:
			if string(plain) != want {
				t.Fatalf("payload = %q, want %s", plain, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for %s", want)
		}
	}

	// The whole distributor channel ran over ONE TCP connection.
	if got := dials.Load(); got != 1 {
		t.Fatalf("distributor TCP connections = %d, want 1", got)
	}

	// Unregistering removes the subscription; further pushes must fail.
	if err := dist.Unregister(ctx, "tok-1"); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	err = upush.Push(ctx, app, upSub, sender.Message{Payload: []byte("ghost"), TTL: 60})
	if !errors.Is(err, sender.ErrSubscriptionNotFound) {
		t.Fatalf("push after unregister = %v, want ErrSubscriptionNotFound", err)
	}
	_ = store
}

func TestRegistryRebuildAfterRestart(t *testing.T) {
	base, _ := newServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	keys, _ := pushmsg.GenerateUAKeys()
	cn := &connector{t: t, keys: keys, delivered: make(chan []byte, 4)}
	registryPath := filepath.Join(t.TempDir(), "registry.json")

	newDist := func() *dcore.Distributor {
		client, err := dcore.NewClient(base)
		if err != nil {
			t.Fatal(err)
		}
		registry, err := dcore.OpenRegistry(registryPath)
		if err != nil {
			t.Fatal(err)
		}
		return dcore.NewDistributor(client, registry, cn, dcore.Options{
			Logger: silentLogger(),
		})
	}

	dist := newDist()
	runCtx, stop := context.WithCancel(ctx)
	go dist.Run(runCtx)
	endpoint, err := dist.Register(ctx, "tok-2", "app", "")
	if err != nil {
		t.Fatal(err)
	}

	// Kill the distributor, send a message (it queues at S with TTL).
	stop()
	app := sender.New()
	upSub := &upush.Subscription{
		Endpoint:  endpoint,
		P256DHB64: base64.RawURLEncoding.EncodeToString(keys.Public.Bytes()),
		AuthB64:   base64.RawURLEncoding.EncodeToString(keys.AuthSecret),
	}
	if err := upush.Push(ctx, app, upSub, sender.Message{
		Payload: []byte("queued"), TTL: 3600,
	}); err != nil {
		t.Fatalf("push: %v", err)
	}

	// Restart the distributor from the registry file: the queued message
	// is picked up without a new registration.
	dist2 := newDist()
	runCtx2, stop2 := context.WithCancel(ctx)
	defer stop2()
	go dist2.Run(runCtx2)
	select {
	case plain := <-cn.delivered:
		if string(plain) != "queued" {
			t.Fatalf("payload = %q", plain)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("queued message not delivered after distributor restart")
	}
}
