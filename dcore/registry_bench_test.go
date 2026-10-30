package dcore

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
	"unsafe"
)

// The benchmarks below are the baseline for the registry rework: Put shows
// the cost of rewriting the whole file on every mutation, List the cost of
// copying every registration, and PollHotPath the per-message work the
// distributor's poll loop does. GetParallel exposes the fact that reads
// share one exclusive lock.

func benchRegistration(i int) *Registration {
	const host = "https://push.example.invalid"
	return &Registration{
		ConnectionToken: fmt.Sprintf("tok-%08d", i),
		Application:     "org.example.application.number" + fmt.Sprint(i%16),
		Label:           "benchmark registration",
		WebSubscription: WebSubscription{
			SubscriptionURI: fmt.Sprintf("%s/subscription/%08x", host, i),
			PushURI:         fmt.Sprintf("%s/push/%08x", host, i),
		},
		CreatedAt: time.Unix(1700000000, 0),
	}
}

func benchRegistry(b *testing.B, n int) *Registry {
	b.Helper()
	r, err := OpenRegistry(filepath.Join(b.TempDir(), "registry.json"))
	if err != nil {
		b.Fatal(err)
	}
	for i := range n {
		if err := r.Put(benchRegistration(i)); err != nil {
			b.Fatal(err)
		}
	}
	return r
}

var registrySizes = []int{8, 64, 512}

func BenchmarkRegistryGet(b *testing.B) {
	for _, n := range registrySizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			r := benchRegistry(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if r.Get(fmt.Sprintf("tok-%08d", i%n)) == nil {
					b.Fatal("missing registration")
				}
			}
		})
	}
}

func BenchmarkRegistryGetParallel(b *testing.B) {
	for _, n := range registrySizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			r := benchRegistry(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for i := 0; pb.Next(); i++ {
					if r.Get(fmt.Sprintf("tok-%08d", i%n)) == nil {
						b.Fatal("missing registration")
					}
				}
			})
		})
	}
}

func BenchmarkRegistryList(b *testing.B) {
	for _, n := range registrySizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			r := benchRegistry(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if len(r.List()) != n {
					b.Fatal("wrong registration count")
				}
			}
		})
	}
}

func BenchmarkRegistryPut(b *testing.B) {
	for _, n := range registrySizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			r := benchRegistry(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if err := r.Put(benchRegistration(i % n)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkPollHotPath measures the per-message copy the poll loop makes
// when it re-reads its own registration.
func BenchmarkPollHotPath(b *testing.B) {
	r := benchRegistry(b, 64)
	token := "tok-00000001"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		reg := r.Get(token)
		if reg == nil {
			b.Fatal("missing registration")
		}
		sink = reg.WebSubscription
	}
}

var sink WebSubscription

// TestRegistrationFootprint records the fixed cost of one registration so
// the per-subscription memory estimate does not have to be guessed.
func TestRegistrationFootprint(t *testing.T) {
	reg := benchRegistration(1)
	strBytes := len(reg.ConnectionToken) + len(reg.Application) + len(reg.Label) +
		len(reg.SubscriptionURI) + len(reg.PushURI)
	t.Logf("Registration struct = %d bytes", unsafe.Sizeof(*reg))
	t.Logf("registration strings = %d bytes (token %d, app %d, label %d, sub URI %d, push URI %d)",
		strBytes, len(reg.ConnectionToken), len(reg.Application), len(reg.Label),
		len(reg.SubscriptionURI), len(reg.PushURI))
}
