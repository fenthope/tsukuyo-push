package dcore

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func newTestDistributor(t *testing.T) *Distributor {
	t.Helper()
	// A client aimed at a closed port: Restore starts poll loops, they find
	// nothing, and Run's context cancels them on cleanup.
	client, err := NewClient("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	dist := NewDistributor(client, DispatcherFunc(
		func(context.Context, string, []byte) error { return nil },
	), Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	go dist.Run(ctx)
	t.Cleanup(cancel)
	return dist
}

func saved(token, label string) Registration {
	return Registration{
		ConnectionToken: token,
		Application:     "org.example.app",
		Label:           label,
		WebSubscription: WebSubscription{
			SubscriptionURI: "https://push.invalid/subscription/" + token,
			PushURI:         "https://push.invalid/push/" + token,
		},
	}
}

// TestRestoreHandsBackCopies pins the safety half of the storage contract:
// everything crossing the boundary is a copy, so the embedding application
// can keep, mutate or re-serialise what it gets without touching the
// distributor's working set.
func TestRestoreHandsBackCopies(t *testing.T) {
	dist := newTestDistributor(t)
	if err := dist.Restore([]Registration{saved("a", "one")}); err != nil {
		t.Fatal(err)
	}

	got, ok := dist.Registration("a")
	if !ok {
		t.Fatal("restored registration is missing")
	}
	if got.PushURI != "https://push.invalid/push/a" || got.Application != "org.example.app" {
		t.Fatalf("restored registration = %+v", got)
	}

	got.Label = "mutated"
	if again, _ := dist.Registration("a"); again.Label != "one" {
		t.Error("Registration handed out the distributor's own memory")
	}
	list := dist.Registrations()
	if len(list) != 1 {
		t.Fatalf("Registrations = %d entries, want 1", len(list))
	}
	list[0].Label = "mutated"
	if again, _ := dist.Registration("a"); again.Label != "one" {
		t.Error("Registrations handed out the distributor's own memory")
	}
}

func TestRestoreRules(t *testing.T) {
	dist := newTestDistributor(t)
	if err := dist.Restore([]Registration{saved("a", "first")}); err != nil {
		t.Fatal(err)
	}
	if err := dist.Restore([]Registration{saved("a", "second")}); err != nil {
		t.Fatal(err)
	}
	if reg, _ := dist.Registration("a"); reg.Label != "first" {
		t.Errorf("an existing token was overwritten during Restore (label %q)", reg.Label)
	}
	if err := dist.Restore([]Registration{{Label: "no token"}}); err == nil {
		t.Error("a registration without a connection token must be refused")
	}
	if _, ok := dist.Registration("missing"); ok {
		t.Error("unknown token reported as present")
	}
}

func TestChangeKindString(t *testing.T) {
	if Registered.String() != "registered" || Removed.String() != "removed" {
		t.Errorf("ChangeKind strings = %q, %q", Registered, Removed)
	}
}

func TestOptionsDefaults(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	disp := DispatcherFunc(func(context.Context, string, []byte) error { return nil })

	dist := NewDistributor(client, disp, Options{})
	if got := dist.opts.PollRetryDelay; got != time.Second {
		t.Errorf("PollRetryDelay = %v, want 1s", got)
	}
	if got := dist.opts.MaxPollRetryDelay; got != time.Minute {
		t.Errorf("MaxPollRetryDelay = %v, want 1m", got)
	}
	if got := dist.opts.DeliverRetryDelay; got != 2*time.Second {
		t.Errorf("DeliverRetryDelay = %v, want 2s", got)
	}

	// A base above the default cap would make the doubling meaningless;
	// the cap follows the base.
	dist = NewDistributor(client, disp, Options{PollRetryDelay: 5 * time.Minute})
	if got := dist.opts.MaxPollRetryDelay; got != 5*time.Minute {
		t.Errorf("MaxPollRetryDelay = %v, want it raised to the 5m base", got)
	}
}
