package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/satomitoka/tsukuyo-push/dcore"
)

// TestRegisterFailsWhenStoreFails is the durability half of the storage
// contract: an endpoint may only be handed to an application if the
// embedding application managed to save the registration, otherwise a
// restart would silently drop the app's push channel.
func TestRegisterFailsWhenStoreFails(t *testing.T) {
	base := newServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := dcore.NewClient(base)
	if err != nil {
		t.Fatal(err)
	}
	persist := newPersister()
	dist := dcore.NewDistributor(client, dcore.DispatcherFunc(
		func(context.Context, string, []byte) error { return nil },
	), dcore.Options{
		Logger:    silentLogger(),
		OnChanged: persist.apply,
	})

	persist.failNextWrites(1, errDiskFull)
	endpoint, err := dist.Register(ctx, "tok-f", "org.example.app", "app", "")
	if !errors.Is(err, errDiskFull) {
		t.Fatalf("register = %v, want the store's error", err)
	}
	if endpoint != "" {
		t.Errorf("endpoint %q handed out although persistence failed", endpoint)
	}
	if _, ok := dist.Registration("tok-f"); ok {
		t.Error("distributor kept a registration the application could not save")
	}
	if persist.Writes != 1 {
		t.Errorf("store writes = %d, want 1", persist.Writes)
	}

	// The failed attempt must not poison the token: with the store healthy
	// again, registering succeeds. (The distributor also asked the push
	// server to drop the subscription it had just created.)
	if _, err := dist.Register(ctx, "tok-f", "org.example.app", "app", ""); err != nil {
		t.Fatalf("retry after a transient store failure: %v", err)
	}
	if !persist.has("tok-f") {
		t.Error("successful re-registration was not reported to the store")
	}
}

// TestUnregisterSurvivesUnpersistableRemoval pins the other half: the push
// server has already forgotten the subscription, so the distributor drops it
// anyway and only logs that the application could not record the removal.
func TestUnregisterSurvivesUnpersistableRemoval(t *testing.T) {
	base := newServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := dcore.NewClient(base)
	if err != nil {
		t.Fatal(err)
	}
	persist := newPersister()
	dist := dcore.NewDistributor(client, dcore.DispatcherFunc(
		func(context.Context, string, []byte) error { return nil },
	), dcore.Options{
		Logger:    silentLogger(),
		OnChanged: persist.apply,
	})
	if _, err := dist.Register(ctx, "tok-g", "org.example.app", "app", ""); err != nil {
		t.Fatal(err)
	}

	persist.failNextWrites(1, errDiskFull)
	if err := dist.Unregister(ctx, "tok-g"); err != nil {
		t.Fatalf("unregister = %v, want success with the removal logged", err)
	}
	if _, ok := dist.Registration("tok-g"); ok {
		t.Error("distributor still polls a subscription the push server dropped")
	}
}
