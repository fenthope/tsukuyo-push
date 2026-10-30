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

func TestPersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "up.db")
	ctx := context.Background()

	s, err := boltstore.Open(path, boltstore.WithSweepInterval(time.Hour))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s.CreateSubscription(ctx, sub("sub1"))
	s.CreateSubscription(ctx, sub("sub2"))
	s.Enqueue(ctx, msg("sub1", "m1", 600))
	s.Enqueue(ctx, msg("sub1", "m2", 600))
	s.Enqueue(ctx, msg("sub2", "other", 600))
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := boltstore.Open(path, boltstore.WithSweepInterval(time.Hour))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	if _, err := s2.GetSubscription(ctx, "sub1"); err != nil {
		t.Fatalf("sub1 lost: %v", err)
	}
	for _, want := range []string{"m1", "m2"} {
		m, err := pollSoon(t, s2, "sub1", pushmsg.UrgencyVeryLow)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		if m.ID != want {
			t.Fatalf("got %s want %s", m.ID, want)
		}
	}
	m, err := pollSoon(t, s2, "sub2", pushmsg.UrgencyVeryLow)
	if err != nil || m.ID != "other" {
		t.Fatalf("sub2: %+v %v", m, err)
	}
}

func TestUnackedRequeuedAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "up.db")
	ctx := context.Background()

	s, _ := boltstore.Open(path, boltstore.WithSweepInterval(time.Hour))
	s.CreateSubscription(ctx, sub("sub1"))
	s.Enqueue(ctx, msg("sub1", "m1", 600))
	m, err := pollSoon(t, s, "sub1", pushmsg.UrgencyVeryLow)
	if err != nil || m.ID != "m1" {
		t.Fatalf("poll: %+v %v", m, err)
	}
	s.Close()

	s2, _ := boltstore.Open(path, boltstore.WithSweepInterval(time.Hour))
	defer s2.Close()
	m, err = pollSoon(t, s2, "sub1", pushmsg.UrgencyVeryLow)
	if err != nil || m.ID != "m1" {
		t.Fatalf("at-least-once redelivery failed: %+v %v", m, err)
	}
	if err := s2.Ack(ctx, "m1"); err != nil {
		t.Fatalf("ack after reopen: %v", err)
	}
}

func TestDeleteRemovesMessagesFromDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "up.db")
	ctx := context.Background()

	s, _ := boltstore.Open(path, boltstore.WithSweepInterval(time.Hour))
	s.CreateSubscription(ctx, sub("sub1"))
	s.Enqueue(ctx, msg("sub1", "m1", 600))
	s.DeleteSubscription(ctx, "sub1")
	s.Close()

	s2, _ := boltstore.Open(path, boltstore.WithSweepInterval(time.Hour))
	defer s2.Close()
	if _, err := s2.GetSubscription(ctx, "sub1"); !errors.Is(err, service.ErrSubscriptionNotFound) {
		t.Fatalf("sub survived delete: %v", err)
	}
	if _, err := pollSoon(t, s2, "sub1", pushmsg.UrgencyVeryLow); !errors.Is(err, service.ErrSubscriptionNotFound) {
		t.Fatalf("message survived delete: %v", err)
	}
}
