package dcore

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
	"unsafe"
)

// These benchmarks replaced the registry file benchmarks: the distributor
// now owns the table in memory and reports changes to the application, so
// the library's share of the work is measured here and the storage's share
// in example/filestore. Baseline before the rework (same machine):
// Registry.Get 187-267ns, Registry.List 817ns/8176ns/50234ns and 960B/7.7kB/
// 62kB for n=8/64/512, Registry.Put (including the whole-file rewrite)
// 41us/134us/791us.

func benchRegistration(i int) Registration {
	const host = "https://push.example.invalid"
	return Registration{
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

// silentPushServer accepts connections and then says nothing, so every
// restored poll loop parks inside Poll. Without this the loops would spin on
// a refused connection and the benchmark would measure a busy runtime
// instead of the registration table.
func silentPushServer(b *testing.B) string {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	// The client pins one connection, but keep room for a few more. The
	// channel holds the references so nothing is finalizer-closed while a
	// poll stream is parked on it.
	held := make(chan net.Conn, 16)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			held <- c
		}
	}()
	b.Cleanup(func() {
		ln.Close()
		for {
			select {
			case c := <-held:
				c.Close()
			default:
				return
			}
		}
	})
	return "http://" + ln.Addr().String()
}

func benchDistributor(b *testing.B, n int) *Distributor {
	b.Helper()
	client, err := NewClient(silentPushServer(b))
	if err != nil {
		b.Fatal(err)
	}
	dist := NewDistributor(client, DispatcherFunc(
		func(context.Context, string, []byte) error { return nil },
	), Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	regs := make([]Registration, n)
	for i := range regs {
		regs[i] = benchRegistration(i)
	}
	if err := dist.Restore(regs); err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go dist.Run(ctx)
	b.Cleanup(cancel)
	return dist
}

var registrySizes = []int{8, 64, 512}

func BenchmarkDistributorRegistration(b *testing.B) {
	for _, n := range registrySizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dist := benchDistributor(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, ok := dist.Registration(fmt.Sprintf("tok-%08d", i%n)); !ok {
					b.Fatal("missing registration")
				}
			}
		})
	}
}

func BenchmarkDistributorRegistrationParallel(b *testing.B) {
	for _, n := range registrySizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dist := benchDistributor(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for i := 0; pb.Next(); i++ {
					if _, ok := dist.Registration(fmt.Sprintf("tok-%08d", i%n)); !ok {
						b.Fatal("missing registration")
					}
				}
			})
		})
	}
}

func BenchmarkDistributorRegistrations(b *testing.B) {
	for _, n := range registrySizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dist := benchDistributor(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if len(dist.Registrations()) != n {
					b.Fatal("wrong registration count")
				}
			}
		})
	}
}

// BenchmarkDistributorRestore measures adopting a saved set at startup.
func BenchmarkDistributorRestore(b *testing.B) {
	for _, n := range registrySizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			regs := make([]Registration, n)
			for i := range regs {
				regs[i] = benchRegistration(i)
			}
			dist := benchDistributor(b, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := dist.Restore(regs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestRegistrationFootprint records the fixed cost of one registration so
// the per-subscription memory estimate does not have to be guessed.
func TestRegistrationFootprint(t *testing.T) {
	reg := benchRegistration(1)
	strBytes := len(reg.ConnectionToken) + len(reg.Application) + len(reg.Label) +
		len(reg.SubscriptionURI) + len(reg.PushURI)
	t.Logf("Registration struct = %d bytes", unsafe.Sizeof(reg))
	t.Logf("registration strings = %d bytes (token %d, app %d, label %d, sub URI %d, push URI %d)",
		strBytes, len(reg.ConnectionToken), len(reg.Application), len(reg.Label),
		len(reg.SubscriptionURI), len(reg.PushURI))
}
