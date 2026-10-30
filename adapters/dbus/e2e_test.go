//go:build dbus

package dbusadapter_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/fenthope/tsukasa-push/pushmsg"
	"github.com/fenthope/tsukasa-push/sender"
	"github.com/fenthope/tsukasa-push/service"
	"github.com/fenthope/tsukasa-push/upush"
	"github.com/godbus/dbus/v5"
	"github.com/infinite-iroha/touka"

	dbusadapter "github.com/satomitoka/tsukuyo-push/adapters/dbus"
	"github.com/satomitoka/tsukuyo-push/dcore"
	"github.com/satomitoka/tsukuyo-push/h2srv"
	"github.com/satomitoka/tsukuyo-push/internal/boltstore"
)

// TestMain re-executes under dbus-run-session when no session bus exists,
// so the suite runs in bare CI environments too.
func TestMain(m *testing.M) {
	if os.Getenv("DBUS_SESSION_BUS_ADDRESS") == "" {
		helper, err := exec.LookPath("dbus-run-session")
		if err != nil {
			fmt.Println("SKIP: no session bus and no dbus-run-session")
			os.Exit(0)
		}
		self, err := os.Executable()
		if err != nil {
			fmt.Println("SKIP:", err)
			os.Exit(0)
		}
		cmd := exec.Command(helper, append([]string{"--", self}, os.Args[1:]...)...)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// fakeConnector implements org.unifiedpush.Connector2 for one app.
type fakeConnector struct {
	name      string
	endpoints chan string
	messages  chan string
	gone      chan string
}

func (c *fakeConnector) NewEndpoint(args map[string]dbus.Variant) (map[string]dbus.Variant, *dbus.Error) {
	token, _ := args["token"].Value().(string)
	endpoint, _ := args["endpoint"].Value().(string)
	c.endpoints <- token + " " + endpoint
	return map[string]dbus.Variant{}, nil
}

func (c *fakeConnector) Message(args map[string]dbus.Variant) (map[string]dbus.Variant, *dbus.Error) {
	body, _ := args["message"].Value().([]byte)
	token, _ := args["token"].Value().(string)
	c.messages <- token + "|" + string(body)
	return map[string]dbus.Variant{}, nil
}

func (c *fakeConnector) Unregistered(args map[string]dbus.Variant) (map[string]dbus.Variant, *dbus.Error) {
	token, _ := args["token"].Value().(string)
	c.gone <- token
	return map[string]dbus.Variant{}, nil
}

func serveConnector(t *testing.T, conn *dbus.Conn, c *fakeConnector) {
	t.Helper()
	if _, err := conn.RequestName(c.name, dbus.NameFlagDoNotQueue); err != nil {
		t.Fatalf("connector request name: %v", err)
	}
	if err := conn.Export(c, "/org/unifiedpush/Connector", "org.unifiedpush.Connector2"); err != nil {
		t.Fatalf("connector export: %v", err)
	}
}

func startPushServer(t *testing.T) string {
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
	return "http://" + ln.Addr().String()
}

func silent() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestDistributorDBusFlow(t *testing.T) {
	conn, err := dbus.SessionBus()
	if err != nil {
		t.Fatalf("session bus: %v", err)
	}
	base := startPushServer(t)

	registry, err := dcore.OpenRegistry(filepath.Join(t.TempDir(), "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	client, err := dcore.NewClient(base)
	if err != nil {
		t.Fatal(err)
	}

	app := &fakeConnector{
		name:      "org.example.TestApp",
		endpoints: make(chan string, 8),
		messages:  make(chan string, 8),
		gone:      make(chan string, 8),
	}
	serveConnector(t, conn, app)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var dist *dcore.Distributor
	adapter, err := dbusadapter.New(conn, registry, "test", silent())
	if err != nil {
		t.Fatal(err)
	}
	dist = dcore.NewDistributor(client, registry, adapter, dcore.Options{
		Logger: silent(),
		OnGone: adapter.NotifyGone,
	})
	adapter.Attach(dist)
	if err := adapter.Serve(); err != nil {
		t.Fatalf("serve: %v", err)
	}
	go dist.Run(ctx)

	// --- connector side: Register via the Distributor2 API ------------
	token := "a3f0c9b2-7d41-4c8e-9a51-1b2c3d4e5f60"
	distributor := conn.Object(dbusadapter.DistributorNamePrefix+"test",
		dbus.ObjectPath(dbusadapter.DistributorPath))
	var reply map[string]dbus.Variant
	err = distributor.Call(dbusadapter.DistributorIface+".Register", 0,
		dbus.MakeVariant(map[string]dbus.Variant{
			"service":     dbus.MakeVariant(app.name),
			"token":       dbus.MakeVariant(token),
			"description": dbus.MakeVariant("e2e test"),
		})).Store(&reply)
	if err != nil {
		t.Fatalf("Register call: %v", err)
	}
	if got := reply["success"].Value(); got != "REGISTRATION_SUCCEEDED" {
		t.Fatalf("Register reply = %v (%v)", got, reply["reason"])
	}

	var endpoint string
	select {
	case ep := <-app.endpoints:
		var gotToken string
		if _, err := fmt.Sscanf(ep, "%s ", &gotToken); err != nil || gotToken != token {
			t.Fatalf("NewEndpoint for wrong token: %q", ep)
		}
		endpoint = ep[len(token)+1:]
	case <-time.After(10 * time.Second):
		t.Fatal("no NewEndpoint delivered")
	}

	// --- application server pushes an encrypted message ---------------
	keys, err := pushmsg.GenerateUAKeys()
	if err != nil {
		t.Fatal(err)
	}
	upSub := &upush.Subscription{
		Endpoint:  endpoint,
		P256DHB64: base64.RawURLEncoding.EncodeToString(keys.Public.Bytes()),
		AuthB64:   base64.RawURLEncoding.EncodeToString(keys.AuthSecret),
	}
	payload := []byte("hello over dbus")
	if err := upush.Push(ctx, sender.New(), upSub, sender.Message{
		Payload: payload, TTL: 300,
	}); err != nil {
		t.Fatalf("push: %v", err)
	}

	select {
	case got := <-app.messages:
		want := token + "|"
		if len(got) < len(want) || got[:len(want)] != want {
			t.Fatalf("Message token mismatch: %q", got)
		}
		// The connector (not the distributor) decrypts.
		plain, err := pushmsg.Decrypt([]byte(got[len(want):]), keys.AuthSecret, keys.Private)
		if err != nil {
			t.Fatalf("connector decrypt: %v", err)
		}
		if string(plain) != string(payload) {
			t.Fatalf("payload = %q, want %q", plain, payload)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no Message delivered")
	}

	// --- re-registration with the same token re-pushes NewEndpoint ----
	if err := distributor.Call(dbusadapter.DistributorIface+".Register", 0,
		dbus.MakeVariant(map[string]dbus.Variant{
			"service": dbus.MakeVariant(app.name),
			"token":   dbus.MakeVariant(token),
		})).Store(&reply); err != nil {
		t.Fatalf("re-Register: %v", err)
	}
	if reply["success"].Value() != "REGISTRATION_SUCCEEDED" {
		t.Fatalf("re-Register reply = %v", reply)
	}
	select {
	case ep := <-app.endpoints:
		if ep != token+" "+endpoint {
			t.Fatalf("re-register endpoint changed: %q vs %q", ep, endpoint)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("re-registration did not re-send NewEndpoint")
	}

	// --- token hijacking by another bus name is refused ---------------
	var hijack map[string]dbus.Variant
	err = distributor.Call(dbusadapter.DistributorIface+".Register", 0,
		dbus.MakeVariant(map[string]dbus.Variant{
			"service": dbus.MakeVariant("org.example.EvilApp"),
			"token":   dbus.MakeVariant(token),
		})).Store(&hijack)
	if err != nil {
		t.Fatalf("hijack Register: %v", err)
	}
	if hijack["success"].Value() != "REGISTRATION_FAILED" {
		t.Fatalf("hijack unexpectedly succeeded: %v", hijack)
	}

	// --- Unregister reaches the connector ------------------------------
	if err := distributor.Call(dbusadapter.DistributorIface+".Unregister", 0,
		dbus.MakeVariant(map[string]dbus.Variant{
			"token": dbus.MakeVariant(token),
		})).Store(&reply); err != nil {
		t.Fatalf("Unregister call: %v", err)
	}
	select {
	case tok := <-app.gone:
		if tok != token {
			t.Fatalf("Unregistered token = %q", tok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no Unregistered delivered")
	}

	// Pushes to the removed endpoint must now fail.
	err = upush.Push(ctx, sender.New(), upSub, sender.Message{
		Payload: []byte("ghost"), TTL: 60,
	})
	if !errors.Is(err, sender.ErrSubscriptionNotFound) {
		t.Fatalf("push after unregister = %v", err)
	}
}
