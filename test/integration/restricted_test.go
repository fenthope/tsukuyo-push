package integration

import (
	"context"
	"encoding/base64"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/fenthope/tsukasa-push/pushmsg"
	"github.com/fenthope/tsukasa-push/sender"
	"github.com/fenthope/tsukasa-push/upush"
	"github.com/fenthope/tsukasa-push/vapid"

	"github.com/satomitoka/tsukuyo-push/dcore"
)

// TestRestrictedRegistration checks that the restricted key an application
// server passes to the distributor (RFC 8292 §4.1, the UnifiedPush register
// "vapid" argument) actually restricts the subscription: only the holder of
// the matching VAPID key pair may push to the endpoint.
func TestRestrictedRegistration(t *testing.T) {
	base, _ := newServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	keys, err := pushmsg.GenerateUAKeys()
	if err != nil {
		t.Fatal(err)
	}
	cn := &connector{t: t, keys: keys, delivered: make(chan []byte, 4)}

	client, err := dcore.NewClient(base)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := dcore.OpenRegistry(filepath.Join(t.TempDir(), "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	dist := dcore.NewDistributor(client, registry, cn, dcore.Options{
		Logger: silentLogger(),
	})
	go dist.Run(ctx)

	allowed, err := vapid.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	allowedSigner, err := vapid.NewSigner(allowed)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := vapid.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	foreignSigner, err := vapid.NewSigner(foreign)
	if err != nil {
		t.Fatal(err)
	}

	endpoint, err := dist.Register(ctx, "tok-r", "org.example.app", "restricted", allowedSigner.PublicKeyB64())
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	upSub := &upush.Subscription{
		Endpoint:  endpoint,
		P256DHB64: base64.RawURLEncoding.EncodeToString(keys.Public.Bytes()),
		AuthB64:   base64.RawURLEncoding.EncodeToString(keys.AuthSecret),
	}

	msg := sender.Message{Payload: []byte("nope"), TTL: 300}
	if err := upush.Push(ctx, sender.New(sender.WithVAPID(foreignSigner, "mailto:other@example.com")), upSub, msg); !errors.Is(err, sender.ErrVAPIDRejected) {
		t.Errorf("push from another application server = %v, want ErrVAPIDRejected", err)
	}
	if err := upush.Push(ctx, sender.New(), upSub, msg); !errors.Is(err, sender.ErrVAPIDRequired) {
		t.Errorf("push without VAPID = %v, want ErrVAPIDRequired", err)
	}

	if err := upush.Push(ctx, sender.New(sender.WithVAPID(allowedSigner, "mailto:ops@example.com")), upSub,
		sender.Message{Payload: []byte("mine"), TTL: 300, Urgency: pushmsg.UrgencyNormal}); err != nil {
		t.Fatalf("push from the allowed application server: %v", err)
	}
	select {
	case plain := <-cn.delivered:
		if string(plain) != "mine" {
			t.Fatalf("payload = %q, want mine", plain)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for the allowed message")
	}
}
