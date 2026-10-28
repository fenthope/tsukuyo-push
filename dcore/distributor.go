package dcore

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Dispatcher delivers the raw encrypted push body to the end user
// application identified by its connection token. Returning an error makes
// the distributor retry before acknowledging the message at the push
// server, so delivery stays at-least-once.
type Dispatcher interface {
	Deliver(ctx context.Context, connectionToken string, body []byte) error
}

// Options configures a Distributor.
type Options struct {
	// Logger receives lifecycle events; defaults to slog.Default.
	Logger *slog.Logger
	// OnGone is called when the push server reports a subscription is
	// gone (expired or removed). The registration is already dropped from
	// the registry; the adapter should ask the app to re-register
	// (UnifiedPush ENDPOINT_RENEWAL / REGISTRATION_FAILED).
	OnGone func(connectionToken string)
	// DeliverRetryDelay bounds the backoff between failed deliveries.
	DeliverRetryDelay time.Duration
}

// Distributor manages Web Push subscriptions for many end user
// applications over one shared, connection-reused push client.
type Distributor struct {
	client *Client
	reg    *Registry
	disp   Dispatcher
	opts   Options

	mu      sync.Mutex
	running map[string]context.CancelFunc
}

// NewDistributor creates a Distributor. Registrations already present in
// the registry resume automatically on Run.
func NewDistributor(client *Client, registry *Registry, disp Dispatcher, opts Options) *Distributor {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.DeliverRetryDelay == 0 {
		opts.DeliverRetryDelay = 2 * time.Second
	}
	return &Distributor{
		client:  client,
		reg:     registry,
		disp:    disp,
		opts:    opts,
		running: make(map[string]context.CancelFunc),
	}
}

// Register creates a push subscription for a connection token and returns
// the endpoint the app must hand to its application server.
func (d *Distributor) Register(ctx context.Context, connectionToken, label, restrictedKey string) (string, error) {
	if connectionToken == "" {
		return "", errors.New("dcore: empty connection token")
	}
	if existing := d.reg.Get(connectionToken); existing != nil {
		return existing.PushURI, nil
	}
	web, err := d.client.Subscribe(ctx, restrictedKey)
	if err != nil {
		return "", err
	}
	reg := &Registration{
		ConnectionToken: connectionToken,
		Label:           label,
		WebSubscription: *web,
		CreatedAt:       time.Now(),
	}
	if err := d.reg.Put(reg); err != nil {
		// Best effort: drop the server-side subscription we just made.
		d.client.Unsubscribe(context.WithoutCancel(ctx), web)
		return "", err
	}
	d.ensure()
	d.opts.Logger.Info("registered",
		"token", connectionToken, "endpoint", web.PushURI)
	return web.PushURI, nil
}

// Unregister removes the push subscription and stops its poll loop.
func (d *Distributor) Unregister(ctx context.Context, connectionToken string) error {
	reg := d.reg.Get(connectionToken)
	if reg == nil {
		return nil
	}
	err := d.client.Unsubscribe(ctx, &reg.WebSubscription)
	if err != nil {
		return err
	}
	d.mu.Lock()
	if cancel, ok := d.running[connectionToken]; ok {
		cancel()
		delete(d.running, connectionToken)
	}
	d.mu.Unlock()
	if err := d.reg.Remove(connectionToken); err != nil {
		return err
	}
	d.opts.Logger.Info("unregistered", "token", connectionToken)
	return nil
}

// Run resumes every registered subscription and blocks until ctx ends.
func (d *Distributor) Run(ctx context.Context) error {
	d.ensure()
	<-ctx.Done()
	d.mu.Lock()
	for _, cancel := range d.running {
		cancel()
	}
	d.running = make(map[string]context.CancelFunc)
	d.mu.Unlock()
	return context.Cause(ctx)
}

// ensure starts a poll loop for every registration not yet running.
func (d *Distributor) ensure() {
	for _, reg := range d.reg.List() {
		d.mu.Lock()
		_, live := d.running[reg.ConnectionToken]
		d.mu.Unlock()
		if live {
			continue
		}
		runCtx, cancel := context.WithCancel(context.Background())
		d.mu.Lock()
		// Recheck under the lock; another ensure may have started it.
		if _, live := d.running[reg.ConnectionToken]; live {
			d.mu.Unlock()
			cancel()
			continue
		}
		d.running[reg.ConnectionToken] = cancel
		d.mu.Unlock()
		go d.loop(runCtx, reg.ConnectionToken)
	}
}

// loop long-polls one subscription until its context ends.
func (d *Distributor) loop(ctx context.Context, token string) {
	defer func() {
		d.mu.Lock()
		delete(d.running, token)
		d.mu.Unlock()
	}()
	web := func() *WebSubscription {
		if reg := d.reg.Get(token); reg != nil {
			cp := reg.WebSubscription
			return &cp
		}
		return nil
	}()
	if web == nil {
		return
	}
	backoff := time.Second
	for ctx.Err() == nil {
		res, err := d.client.Poll(ctx, web)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, ErrSubscriptionGone) {
				d.opts.Logger.Info("subscription gone, dropping", "token", token)
				_ = d.reg.Remove(token)
				if d.opts.OnGone != nil {
					d.opts.OnGone(token)
				}
				return
			}
			d.opts.Logger.Warn("poll failed", "token", token, "err", err)
			if !sleepCtx(ctx, jitter(backoff)) {
				return
			}
			backoff = min(backoff*2, 60*time.Second)
			continue
		}
		backoff = time.Second
		if res.Outcome == PollEmpty {
			continue
		}
		if !d.deliver(ctx, token, res.Body) {
			return
		}
		if res.MessageURI != "" {
			if err := d.client.Ack(ctx, res.MessageURI); err != nil && ctx.Err() == nil {
				d.opts.Logger.Warn("ack failed", "token", token, "err", err)
			}
		}
	}
}

// deliver retries until the dispatcher succeeds or ctx ends. Reports
// whether the loop may continue.
func (d *Distributor) deliver(ctx context.Context, token string, body []byte) bool {
	for {
		err := d.disp.Deliver(ctx, token, body)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		d.opts.Logger.Warn("deliver failed, will retry", "token", token, "err", err)
		if !sleepCtx(ctx, jitter(d.opts.DeliverRetryDelay)) {
			return false
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d/2 + time.Duration(rand.Int64N(int64(d)/2))
}
