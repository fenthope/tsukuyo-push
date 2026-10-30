package boltstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/fenthope/tsukasa-push/pushmsg"
	"github.com/fenthope/tsukasa-push/service"

	"github.com/satomitoka/tsukuyo-push/example/boltstore"
)

type factory struct {
	name string
	make func(t *testing.T) service.Store
}

func stores(t *testing.T) []factory {
	t.Helper()
	return []factory{
		{"memory", func(t *testing.T) service.Store {
			return service.NewMemoryStore()
		}},
		{"bolt", func(t *testing.T) service.Store {
			s, err := boltstore.Open(filepath.Join(t.TempDir(), "up.db"),
				boltstore.WithSweepInterval(time.Hour))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { s.Close() })
			return s
		}},
	}
}

func sub(id string) service.Subscription {
	return service.Subscription{ID: id, CreatedAt: time.Now()}
}

func msg(subID, id string, ttl uint32) service.Message {
	return service.Message{
		ID:             id,
		SubscriptionID: subID,
		Body:           []byte("body-" + id),
		TTL:            ttl,
		Urgency:        pushmsg.UrgencyNormal,
		CreatedAt:      time.Now(),
	}
}

func pollSoon(t *testing.T, s service.Store, subID string, min pushmsg.Urgency) (*service.Message, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.Poll(ctx, subID, min)
}

func TestCreateGetDelete(t *testing.T) {
	for _, f := range stores(t) {
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t)
			ctx := context.Background()

			created, err := s.CreateSubscription(ctx, sub("sub1"))
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			if created.ID != "sub1" || created.CreatedAt.IsZero() {
				t.Fatalf("created = %+v", created)
			}
			got, err := s.GetSubscription(ctx, "sub1")
			if err != nil || got.ID != "sub1" {
				t.Fatalf("get = %+v, %v", got, err)
			}
			if _, err := s.GetSubscription(ctx, "nope"); !errors.Is(err, service.ErrSubscriptionNotFound) {
				t.Fatalf("get missing: %v", err)
			}
			if err := s.DeleteSubscription(ctx, "sub1"); err != nil {
				t.Fatalf("delete: %v", err)
			}
			if _, err := s.GetSubscription(ctx, "sub1"); !errors.Is(err, service.ErrSubscriptionNotFound) {
				t.Fatalf("get after delete: %v", err)
			}
			if err := s.DeleteSubscription(ctx, "sub1"); !errors.Is(err, service.ErrSubscriptionNotFound) {
				t.Fatalf("delete twice: %v", err)
			}
		})
	}
}

func TestExpiredSubscription(t *testing.T) {
	for _, f := range stores(t) {
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t)
			ctx := context.Background()
			s.CreateSubscription(ctx, service.Subscription{
				ID: "old", CreatedAt: time.Now().Add(-2 * time.Hour),
				ExpiresAt: time.Now().Add(-time.Hour),
			})
			// First access reports Expired and evicts the subscription.
			if err := s.Enqueue(ctx, msg("old", "m1", 60)); !errors.Is(err, service.ErrSubscriptionExpired) {
				t.Fatalf("enqueue expired: %v", err)
			}
			if _, err := s.GetSubscription(ctx, "old"); !errors.Is(err, service.ErrSubscriptionNotFound) {
				t.Fatalf("get after eviction: %v", err)
			}
		})
	}
}

func TestFIFOOrder(t *testing.T) {
	for _, f := range stores(t) {
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t)
			ctx := context.Background()
			s.CreateSubscription(ctx, sub("sub1"))
			for _, id := range []string{"m1", "m2", "m3"} {
				if err := s.Enqueue(ctx, msg("sub1", id, 600)); err != nil {
					t.Fatalf("enqueue %s: %v", id, err)
				}
			}
			for _, id := range []string{"m1", "m2", "m3"} {
				m, err := pollSoon(t, s, "sub1", pushmsg.UrgencyVeryLow)
				if err != nil {
					t.Fatalf("poll: %v", err)
				}
				if m.ID != id {
					t.Fatalf("got %s, want %s", m.ID, id)
				}
			}
		})
	}
}

func TestUrgencyFilter(t *testing.T) {
	for _, f := range stores(t) {
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t)
			ctx := context.Background()
			s.CreateSubscription(ctx, sub("sub1"))
			low := msg("sub1", "low", 600)
			low.Urgency = pushmsg.UrgencyLow
			high := msg("sub1", "high", 600)
			high.Urgency = pushmsg.UrgencyHigh
			s.Enqueue(ctx, low)
			s.Enqueue(ctx, high)

			m, err := pollSoon(t, s, "sub1", pushmsg.UrgencyHigh)
			if err != nil || m.ID != "high" {
				t.Fatalf("poll high = %+v, %v", m, err)
			}
			m, err = pollSoon(t, s, "sub1", pushmsg.UrgencyVeryLow)
			if err != nil || m.ID != "low" {
				t.Fatalf("poll low = %+v, %v", m, err)
			}
		})
	}
}

func TestZeroTTL(t *testing.T) {
	for _, f := range stores(t) {
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t)
			ctx := context.Background()
			s.CreateSubscription(ctx, sub("sub1"))

			// No poller waiting: the message expires immediately.
			if err := s.Enqueue(ctx, msg("sub1", "gone", 0)); err != nil {
				t.Fatalf("enqueue zero ttl: %v", err)
			}
			ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			if _, err := s.Poll(ctxTimeout, "sub1", pushmsg.UrgencyVeryLow); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("poll after dropped zero ttl: %v", err)
			}

			// With a poller waiting: the message is delivered.
			type result struct {
				m   *service.Message
				err error
			}
			ch := make(chan result, 1)
			go func() {
				m, err := pollSoon(t, s, "sub1", pushmsg.UrgencyVeryLow)
				ch <- result{m, err}
			}()
			time.Sleep(150 * time.Millisecond)
			if err := s.Enqueue(ctx, msg("sub1", "live", 0)); err != nil {
				t.Fatalf("enqueue zero ttl waiting: %v", err)
			}
			r := <-ch
			if r.err != nil || r.m == nil || r.m.ID != "live" {
				t.Fatalf("waiting poll = %+v, %v", r.m, r.err)
			}
		})
	}
}

func TestAck(t *testing.T) {
	for _, f := range stores(t) {
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t)
			ctx := context.Background()
			s.CreateSubscription(ctx, sub("sub1"))
			s.Enqueue(ctx, msg("sub1", "m1", 600))
			m, err := pollSoon(t, s, "sub1", pushmsg.UrgencyVeryLow)
			if err != nil {
				t.Fatalf("poll: %v", err)
			}
			if err := s.Ack(ctx, m.ID); err != nil {
				t.Fatalf("ack: %v", err)
			}
			if err := s.Ack(ctx, m.ID); !errors.Is(err, service.ErrMessageNotFound) {
				t.Fatalf("ack twice: %v", err)
			}
			if err := s.Ack(ctx, "unknown"); !errors.Is(err, service.ErrMessageNotFound) {
				t.Fatalf("ack unknown: %v", err)
			}
		})
	}
}

func TestPollNotFoundAndBlocked(t *testing.T) {
	for _, f := range stores(t) {
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t)
			ctx := context.Background()
			if _, err := pollSoon(t, s, "ghost", pushmsg.UrgencyVeryLow); !errors.Is(err, service.ErrSubscriptionNotFound) {
				t.Fatalf("poll missing sub: %v", err)
			}
			s.CreateSubscription(ctx, sub("sub1"))
			// Blocked until woken by Enqueue.
			done := make(chan *service.Message, 1)
			go func() {
				m, err := pollSoon(t, s, "sub1", pushmsg.UrgencyVeryLow)
				if err != nil {
					t.Errorf("poll: %v", err)
				}
				done <- m
			}()
			time.Sleep(50 * time.Millisecond)
			s.Enqueue(ctx, msg("sub1", "m1", 600))
			select {
			case m := <-done:
				if m == nil || m.ID != "m1" {
					t.Fatalf("woken with %+v", m)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("poller not woken")
			}
		})
	}
}
