package filestore

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/satomitoka/tsukuyo-push/dcore"
)

func registration(token string) dcore.Registration {
	return dcore.Registration{
		ConnectionToken: token,
		Application:     "org.example.app",
		Label:           "app",
		WebSubscription: dcore.WebSubscription{
			SubscriptionURI: "https://s/subscription/1",
			PushURI:         "https://s/push/1",
		},
	}
}

func TestApplyRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	reg := registration("tok")
	for range 2 { // replacing the same token is fine
		if err := s.Apply(dcore.Registered, reg); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(s.Registrations()); got != 1 {
		t.Fatalf("saved %d registrations, want 1", got)
	}

	reloaded, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	found := reloaded.Registrations()
	if len(found) != 1 || found[0].PushURI != reg.PushURI || found[0].Label != "app" {
		t.Fatalf("reload = %+v", found)
	}

	if err := s.Apply(dcore.Removed, reg); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(dcore.Removed, reg); err != nil { // idempotent
		t.Fatal(err)
	}
	reloaded, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(reloaded.Registrations()); got != 0 {
		t.Fatalf("entry survived removal: %d left", got)
	}
}

func TestOpenMissingFileIsEmpty(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "absent.json"))
	if err != nil {
		t.Fatalf("a missing store must open as empty: %v", err)
	}
	if got := len(s.Registrations()); got != 0 {
		t.Fatalf("fresh store has %d registrations, want 0", got)
	}
	if err := s.Apply(dcore.Registered, registration("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(filepath.Join(filepath.Dir(s.path), "absent.json")); err != nil {
		t.Fatalf("reopen after save: %v", err)
	}
}

// TestApplyReportsWriteErrors checks the store surfaces failures instead of
// swallowing them: the distributor relies on them to fail Register.
func TestApplyReportsWriteErrors(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(dcore.Registered, registration("tok")); err != nil {
		t.Fatal(err)
	}
	// Point the store at a path that cannot hold a file.
	s.path = filepath.Join(dir, "registry.json", "impossible")
	err = s.Apply(dcore.Registered, registration("other"))
	if err == nil {
		t.Fatal("want a write error")
	}
}

// BenchmarkApply is the counterpart of the old Registry.Put benchmark
// (41us/134us/791us for n=8/64/512, whole-file rewrite included): that cost
// did not vanish, it moved to the application's side, where a cheaper store
// can be chosen.
func BenchmarkApply(b *testing.B) {
	for _, n := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			s, err := Open(filepath.Join(b.TempDir(), "registry.json"))
			if err != nil {
				b.Fatal(err)
			}
			for i := range n {
				if err := s.Apply(dcore.Registered, registration(tokenFor(i))); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if err := s.Apply(dcore.Registered, registration(tokenFor(i%n))); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func tokenFor(i int) string { return fmt.Sprintf("tok-%08d", i) }
