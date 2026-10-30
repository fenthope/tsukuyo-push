package dcore

import (
	"context"
	"errors"
	"fmt"
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

// DispatcherFunc adapts a function to the Dispatcher interface.
type DispatcherFunc func(ctx context.Context, connectionToken string, body []byte) error

// Deliver implements Dispatcher.
func (f DispatcherFunc) Deliver(ctx context.Context, connectionToken string, body []byte) error {
	return f(ctx, connectionToken, body)
}

// Options configures a Distributor.
type Options struct {
	// Logger receives lifecycle events; defaults to slog.Default.
	Logger *slog.Logger
	// OnGone is called when the push server reports a subscription is
	// gone (expired or removed). The registration has already been
	// dropped; the adapter should inform the app so it can re-register
	// (UnifiedPush Unregistered / ENDPOINT_RENEWAL).
	OnGone func(reg Registration)
	// OnChanged receives every change to the registration table, and is
	// how the embedding application persists them: the distributor keeps
	// registrations in memory and performs no storage of its own. Feed
	// what was saved back through Restore before Run.
	//
	// A failed Registered report fails the matching Register call, so an
	// endpoint is never handed to an application that would not survive a
	// restart. A failed Removed report is logged only: the push server
	// has already forgotten the subscription.
	//
	// It is called with no lock held and must not call back into the
	// Distributor.
	OnChanged func(kind ChangeKind, reg Registration) error
	// DeliverRetryDelay bounds the backoff between failed deliveries.
	DeliverRetryDelay time.Duration
}

// Distributor manages Web Push subscriptions for many end user
// applications over one shared, connection-reused push client.
type Distributor struct {
	client *Client
	disp   Dispatcher
	opts   Options

	mu      sync.RWMutex
	regs    map[string]*Registration
	running map[string]context.CancelFunc
}

// NewDistributor creates a Distributor. Restore the registrations saved by
// an earlier run before calling Run and they resume automatically.
func NewDistributor(client *Client, disp Dispatcher, opts Options) *Distributor {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.DeliverRetryDelay == 0 {
		opts.DeliverRetryDelay = 2 * time.Second
	}
	return &Distributor{
		client:  client,
		disp:    disp,
		opts:    opts,
		regs:    make(map[string]*Registration),
		running: make(map[string]context.CancelFunc),
	}
}

// Register creates a push subscription for a connection token and returns
// the endpoint the app must hand to its application server. The
// application identity (D-Bus name, Android package) is kept with the
// registration; re-registering the same token from a different application
// is refused.
func (d *Distributor) Register(ctx context.Context, connectionToken, application, label, restrictedKey string) (string, error) {
	if connectionToken == "" {
		return "", errors.New("dcore: empty connection token")
	}
	if existing, ok := d.Registration(connectionToken); ok {
		if existing.Application != application {
			return "", fmt.Errorf("dcore: connection token already registered by %q", existing.Application)
		}
		return existing.PushURI, nil
	}
	web, err := d.client.Subscribe(ctx, restrictedKey)
	if err != nil {
		return "", err
	}
	reg := &Registration{
		ConnectionToken: connectionToken,
		Application:     application,
		Label:           label,
		WebSubscription: *web,
		CreatedAt:       time.Now(),
	}
	if err := d.adopt(reg); err != nil {
		// Best effort: drop the server-side subscription we just made.
		d.client.Unsubscribe(context.WithoutCancel(ctx), web)
		return "", err
	}
	d.start(*web, connectionToken)
	d.opts.Logger.Info("registered",
		"token", connectionToken, "endpoint", web.PushURI)
	return web.PushURI, nil
}

// Unregister removes the push subscription and stops its poll loop.
func (d *Distributor) Unregister(ctx context.Context, connectionToken string) error {
	reg, ok := d.Registration(connectionToken)
	if !ok {
		return nil
	}
	if err := d.client.Unsubscribe(ctx, &reg.WebSubscription); err != nil {
		return err
	}
	d.stop(connectionToken)
	if err := d.forget(connectionToken); err != nil {
		d.opts.Logger.Warn("registration removal not persisted",
			"token", connectionToken, "err", err)
	}
	d.opts.Logger.Info("unregistered", "token", connectionToken)
	return nil
}

// Restore adopts registrations saved by an earlier run and starts polling
// them again; it is the counterpart of Options.OnChanged. It changes
// nothing at the push server and reports nothing through OnChanged.
// Registrations whose token is already present are left alone.
func (d *Distributor) Restore(regs []Registration) error {
	for _, reg := range regs {
		if reg.ConnectionToken == "" {
			return errors.New("dcore: registration without a connection token")
		}
		cp := reg
		d.mu.Lock()
		if _, exists := d.regs[cp.ConnectionToken]; !exists {
			d.regs[cp.ConnectionToken] = &cp
		}
		d.mu.Unlock()
	}
	d.ensure()
	return nil
}

// Registration returns a copy of one registration.
func (d *Distributor) Registration(token string) (Registration, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	reg, ok := d.regs[token]
	if !ok {
		return Registration{}, false
	}
	return *reg, true
}

// Registrations returns copies of every registration, in no particular
// order.
func (d *Distributor) Registrations() []Registration {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]Registration, 0, len(d.regs))
	for _, reg := range d.regs {
		out = append(out, *reg)
	}
	return out
}

// Run resumes every registration and blocks until ctx ends.
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

// adopt reports the change first and only then keeps the registration, so
// a caller that persists synchronously can fail Register before an endpoint
// is handed out.
func (d *Distributor) adopt(reg *Registration) error {
	if err := d.changed(Registered, *reg); err != nil {
		return err
	}
	d.mu.Lock()
	d.regs[reg.ConnectionToken] = reg
	d.mu.Unlock()
	return nil
}

// forget drops a registration and reports it. The removal stands either
// way; the error only says the caller could not persist it.
func (d *Distributor) forget(token string) error {
	d.mu.Lock()
	reg, ok := d.regs[token]
	if ok {
		delete(d.regs, token)
	}
	d.mu.Unlock()
	if !ok {
		return nil
	}
	return d.changed(Removed, *reg)
}

func (d *Distributor) changed(kind ChangeKind, reg Registration) error {
	if d.opts.OnChanged == nil {
		return nil
	}
	return d.opts.OnChanged(kind, reg)
}

// stop cancels one poll loop.
func (d *Distributor) stop(token string) {
	d.mu.Lock()
	if cancel, ok := d.running[token]; ok {
		cancel()
		delete(d.running, token)
	}
	d.mu.Unlock()
}

// start launches the poll loop for one registration unless one is already
// running for it.
func (d *Distributor) start(web WebSubscription, token string) {
	runCtx, cancel := context.WithCancel(context.Background())
	d.mu.Lock()
	// Recheck under the lock; another caller may have started it.
	if _, live := d.running[token]; live {
		d.mu.Unlock()
		cancel()
		return
	}
	d.running[token] = cancel
	d.mu.Unlock()
	go d.loop(runCtx, web, token)
}

// ensure starts a poll loop for every registration not yet running.
func (d *Distributor) ensure() {
	for _, reg := range d.Registrations() {
		d.start(reg.WebSubscription, reg.ConnectionToken)
	}
}

// loop long-polls one subscription until its context ends.
func (d *Distributor) loop(ctx context.Context, web WebSubscription, token string) {
	defer func() {
		d.mu.Lock()
		delete(d.running, token)
		d.mu.Unlock()
	}()
	backoff := time.Second
	for ctx.Err() == nil {
		res, err := d.client.Poll(ctx, &web)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, ErrSubscriptionGone) {
				d.opts.Logger.Info("subscription gone, dropping", "token", token)
				var gone Registration
				d.mu.Lock()
				if cur, ok := d.regs[token]; ok {
					gone = *cur
				}
				d.mu.Unlock()
				if err := d.forget(token); err != nil && ctx.Err() == nil {
					d.opts.Logger.Warn("gone registration removal not persisted",
						"token", token, "err", err)
				}
				if d.opts.OnGone != nil {
					d.opts.OnGone(gone)
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
