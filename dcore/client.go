// Package dcore implements the distributor side (the D in the UnifiedPush
// chain) of a Web Push transport.
//
// The distributor owns one capability per subscription: the subscription
// resource URI it long-polls at the push server. All poll loops share one
// tuned HTTP/2 client, so the whole channel — poll streams, acks,
// subscriptions — multiplexes over a single TCP connection.
//
// This is distributor mode: push messages stay end-to-end encrypted by the
// app's connector, the distributor relays the ciphertext unchanged and
// holds no decryption keys.
package dcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/fenthope/tsukasa-push/pushmsg"

	"github.com/satomitoka/tsukuyo-push/dcore/transport"
)

// ErrSubscriptionGone means the push server no longer knows the
// subscription; the registration must be renewed.
var ErrSubscriptionGone = errors.New("dcore: subscription gone")

// WebSubscription identifies the two push resources of one subscription.
type WebSubscription struct {
	// SubscriptionURI is polled by the distributor for messages.
	SubscriptionURI string `json:"subscription_uri"`
	// PushURI is the endpoint handed to the end user application.
	PushURI string `json:"push_uri"`
}

// Client talks to one push server with a tuned HTTP/2 transport.
type Client struct {
	http *http.Client
	base string
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithTransport replaces the HTTP transport (default: a single-connection
// tuned transport from dcore/transport).
func WithTransport(rt http.RoundTripper) ClientOption {
	return func(c *Client) { c.http.Transport = rt }
}

// NewClient creates a push server client for the service base URL.
func NewClient(serviceURL string, opts ...ClientOption) (*Client, error) {
	if serviceURL == "" {
		return nil, errors.New("dcore: missing push service URL")
	}
	rt, err := transport.NewH2C(transport.Options{})
	if err != nil {
		return nil, err
	}
	c := &Client{
		// No client-wide timeout: long polls hold the stream open.
		http: &http.Client{Transport: rt},
		base: strings.TrimRight(serviceURL, "/"),
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// Subscribe creates a push subscription (RFC 8030 §4). A non-empty
// restrictedKeyB64 restricts the subscription to one application server
// (RFC 8292 §4.1).
func (c *Client) Subscribe(ctx context.Context, restrictedKeyB64 string) (*WebSubscription, error) {
	var body io.Reader
	if restrictedKeyB64 != "" {
		raw, err := json.Marshal(map[string]string{"vapid": restrictedKeyB64})
		if err != nil {
			return nil, err
		}
		body = strings.NewReader(string(raw))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/subscribe", body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", pushmsg.OptionsMediaType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dcore: subscribe: %w", err)
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("dcore: subscribe: status %d", resp.StatusCode)
	}
	sub := &WebSubscription{
		SubscriptionURI: resolve(c.base, resp.Header.Get("Location")),
	}
	sub.PushURI = resolve(c.base, pushLink(resp.Header.Get("Link")))
	if sub.SubscriptionURI == "" || sub.PushURI == "" {
		return nil, errors.New("dcore: subscribe: missing Location or push Link")
	}
	return sub, nil
}

// PollOutcome distinguishes the poll results.
type PollOutcome int

const (
	// PollMessage carries a message body.
	PollMessage PollOutcome = iota
	// PollEmpty means no message was available (immediate polls only).
	PollEmpty
)

// PollResult is the outcome of one poll.
type PollResult struct {
	Outcome    PollOutcome
	Body       []byte
	MessageURI string
}

// Poll long-polls the subscription resource (RFC 8030 §6). It blocks until
// a message arrives, the subscription is gone (ErrSubscriptionGone) or ctx
// is done.
func (c *Client) Poll(ctx context.Context, sub *WebSubscription) (*PollResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sub.SubscriptionURI, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dcore: poll: %w", err)
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		if err != nil {
			return nil, fmt.Errorf("dcore: poll body: %w", err)
		}
		return &PollResult{
			Outcome:    PollMessage,
			Body:       body,
			MessageURI: resolve(c.base, resp.Header.Get("Location")),
		}, nil
	case http.StatusNoContent:
		return &PollResult{Outcome: PollEmpty}, nil
	case http.StatusNotFound, http.StatusGone:
		return nil, ErrSubscriptionGone
	default:
		return nil, fmt.Errorf("dcore: poll: status %d", resp.StatusCode)
	}
}

// Ack acknowledges delivery (RFC 8030 §6.2).
func (c *Client) Ack(ctx context.Context, messageURI string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, messageURI, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("dcore: ack: %w", err)
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK, http.StatusGone:
		return nil
	default:
		return fmt.Errorf("dcore: ack: status %d", resp.StatusCode)
	}
}

// Unsubscribe removes the subscription (RFC 8030 §7.2).
func (c *Client) Unsubscribe(ctx context.Context, sub *WebSubscription) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, sub.SubscriptionURI, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("dcore: unsubscribe: %w", err)
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK, http.StatusGone, http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("dcore: unsubscribe: status %d", resp.StatusCode)
	}
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
}

// pushLink extracts the push resource URI from a Link header (RFC 8030 §4).
func pushLink(header string) string {
	for _, link := range strings.Split(header, ",") {
		target, params, ok := strings.Cut(link, ";")
		if !ok {
			continue
		}
		target = strings.Trim(strings.TrimSpace(target), "<>")
		for _, param := range strings.Split(params, ";") {
			key, value, ok := strings.Cut(param, "=")
			if !ok || !strings.EqualFold(strings.TrimSpace(key), "rel") {
				continue
			}
			if strings.Trim(strings.TrimSpace(value), `"`) == "urn:ietf:params:push" {
				return target
			}
		}
	}
	return ""
}

// resolve turns a possibly relative URI absolute against base.
func resolve(base, ref string) string {
	if ref == "" {
		return ""
	}
	if strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://") {
		return ref
	}
	refURL, err := url.Parse(ref)
	if err != nil {
		return ref
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return ref
	}
	return baseURL.ResolveReference(refURL).String()
}
