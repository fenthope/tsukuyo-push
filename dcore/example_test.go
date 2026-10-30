package dcore_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/satomitoka/tsukuyo-push/dcore"
	"github.com/satomitoka/tsukuyo-push/h2srv"
)

// pushService is a stand-in push server: enough of RFC 8030 for one
// subscription to be created, polled once and acknowledged.
func pushService() (url string, stop func()) {
	var polled atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/subscribe", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/subscription/1")
		w.Header().Set("Link", `</push/1>; rel="urn:ietf:params:push"`)
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("/subscription/1", func(w http.ResponseWriter, r *http.Request) {
		if polled.Swap(true) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Location", "/message/1")
		w.Write([]byte("encrypted payload"))
	})
	mux.HandleFunc("/message/1", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	srv := h2srv.New(mux, h2srv.Options{})
	go srv.Serve(ln)
	return "http://" + ln.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Close(ctx)
	}
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// Example shows the whole D-side loop against a stand-in push server:
// register an application, relay one encrypted message, persist through the
// change hook, and unregister.
func Example() {
	url, stop := pushService()
	defer stop()

	client, err := dcore.NewClient(url)
	if err != nil {
		panic(err)
	}

	// The application's storage: the library reports changes, this map is
	// where they land. Any backend works - a file, SQLite, a KV store.
	store := map[string]dcore.Registration{}
	delivered := make(chan string, 1)

	dist := dcore.NewDistributor(client, dcore.DispatcherFunc(
		func(ctx context.Context, connectionToken string, body []byte) error {
			// body is still RFC 8291 ciphertext: the distributor never
			// holds a key. Hand it to the application's connector.
			delivered <- connectionToken + ": " + string(body)
			return nil
		}), dcore.Options{
		Logger: quiet(),
		OnChanged: func(kind dcore.ChangeKind, reg dcore.Registration) error {
			if kind == dcore.Removed {
				delete(store, reg.ConnectionToken)
				return nil
			}
			store[reg.ConnectionToken] = reg
			return nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	endpoint, err := dist.Register(ctx, "tok-1", "org.example.app", "example app", "")
	if err != nil {
		panic(err)
	}
	fmt.Printf("registered %d, endpoint ends with /push/1: %t, saved: %d\n",
		len(store), len(endpoint) > 7 && endpoint[len(endpoint)-7:] == "/push/1", len(store))

	select {
	case msg := <-delivered:
		fmt.Println("relayed", msg)
	case <-ctx.Done():
		fmt.Println("no message")
	}

	if err := dist.Unregister(ctx, "tok-1"); err != nil {
		panic(err)
	}
	fmt.Println("unregistered, saved:", len(store))

	// Output:
	// registered 1, endpoint ends with /push/1: true, saved: 1
	// relayed tok-1: encrypted payload
	// unregistered, saved: 0
}

// ExampleDistributor_Restore shows the startup half of the storage contract:
// whatever the application saved goes back in, and polling resumes without
// anyone re-registering.
func ExampleDistributor_Restore() {
	saved := []dcore.Registration{{
		ConnectionToken: "tok-1",
		Application:     "org.example.app",
		WebSubscription: dcore.WebSubscription{
			SubscriptionURI: "https://push.example/subscription/1",
			PushURI:         "https://push.example/push/1",
		},
	}}

	client, err := dcore.NewClient("http://127.0.0.1:1") // nothing listening
	if err != nil {
		panic(err)
	}
	dist := dcore.NewDistributor(client, dcore.DispatcherFunc(
		func(context.Context, string, []byte) error { return nil }),
		dcore.Options{Logger: quiet()})

	if err := dist.Restore(saved); err != nil {
		panic(err)
	}
	reg, ok := dist.Registration("tok-1")
	fmt.Println("resumed:", ok, reg.PushURI)

	// What the application hands back is a copy, so mutating it cannot
	// disturb the distributor.
	reg.PushURI = "tampered"
	fresh, _ := dist.Registration("tok-1")
	fmt.Println("still:", fresh.PushURI)

	// Output:
	// resumed: true https://push.example/push/1
	// still: https://push.example/push/1
}
