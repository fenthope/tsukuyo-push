package dcore

import (
	"path/filepath"
	"testing"
)

func TestRegistryRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	r, err := OpenRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	reg := &Registration{
		ConnectionToken: "tok",
		Label:           "app",
		WebSubscription: WebSubscription{
			SubscriptionURI: "https://s/subscription/1",
			PushURI:         "https://s/push/1",
		},
	}
	if err := r.Put(reg); err != nil {
		t.Fatal(err)
	}
	if err := r.Put(reg); err != nil { // replace is fine
		t.Fatal(err)
	}
	if got := len(r.List()); got != 1 {
		t.Fatalf("list = %d, want 1", got)
	}

	r2, err := OpenRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	got := r2.Get("tok")
	if got == nil || got.PushURI != reg.PushURI || got.Label != "app" {
		t.Fatalf("reload = %+v", got)
	}

	if err := r2.Remove("tok"); err != nil {
		t.Fatal(err)
	}
	if err := r2.Remove("tok"); err != nil { // idempotent
		t.Fatal(err)
	}
	r3, _ := OpenRegistry(path)
	if r3.Get("tok") != nil {
		t.Fatal("entry survived remove")
	}
}

func TestRegistryFreshFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	r, err := OpenRegistry(path)
	if err != nil {
		t.Fatalf("loading a non-existent registry must succeed: %v", err)
	}
	if got := len(r.List()); got != 0 {
		t.Fatalf("fresh registry = %d entries, want 0", got)
	}
	if err := r.Put(&Registration{ConnectionToken: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenRegistry(path); err != nil {
		t.Fatalf("reopen after save: %v", err)
	}
}
