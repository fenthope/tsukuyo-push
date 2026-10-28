// Package boltstore implements service.Store on top of bbolt with an
// in-memory hot layer, mirroring the semantics of service.MemoryStore.
//
// Subscriptions and queued messages survive a restart. Messages that were
// delivered to a user agent but not acknowledged when the process died are
// re-enqueued on Open, making delivery at-least-once across restarts.
package boltstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/fenthope/tsukasa-push/pushmsg"
	"github.com/fenthope/tsukasa-push/service"
)

const (
	bucketSubs = "subs"
	bucketMsgs = "msgs"
	bucketIdx  = "msgkey"
	bucketSeq  = "seq"

	zeroTTLLifetime = 30 * time.Second
)

type storedSub struct {
	ID            string    `json:"id"`
	CreatedAt     time.Time `json:"created_at"`
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
	RestrictedKey []byte    `json:"restricted_key,omitempty"`
}

func (s storedSub) toService() service.Subscription {
	return service.Subscription{
		ID:            s.ID,
		CreatedAt:     s.CreatedAt,
		ExpiresAt:     s.ExpiresAt,
		RestrictedKey: s.RestrictedKey,
	}
}

type storedMsg struct {
	Key            string    `json:"key"`
	ID             string    `json:"id"`
	SubscriptionID string    `json:"subscription_id"`
	Body           []byte    `json:"body"`
	TTL            uint32    `json:"ttl"`
	Urgency        int       `json:"urgency"`
	Topic          string    `json:"topic,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	ExpiresAt      time.Time `json:"expires_at,omitempty"`
	Delivered      bool      `json:"delivered,omitempty"`
}

func (m *storedMsg) toService() *service.Message {
	return &service.Message{
		ID:             m.ID,
		SubscriptionID: m.SubscriptionID,
		Body:           m.Body,
		TTL:            m.TTL,
		Urgency:        pushmsg.Urgency(m.Urgency),
		Topic:          m.Topic,
		CreatedAt:      m.CreatedAt,
		ExpiresAt:      m.ExpiresAt,
	}
}

func storeMsg(msg *service.Message) storedMsg {
	return storedMsg{
		ID:             msg.ID,
		SubscriptionID: msg.SubscriptionID,
		Body:           msg.Body,
		TTL:            msg.TTL,
		Urgency:        int(msg.Urgency),
		Topic:          msg.Topic,
		CreatedAt:      msg.CreatedAt,
		ExpiresAt:      msg.ExpiresAt,
	}
}

type subState struct {
	sub       service.Subscription
	queue     []*service.Message
	delivered map[string]*service.Message
	notify    chan struct{}
	polling   int
}

// Store is a durable service.Store backed by bbolt.
type Store struct {
	db   *bolt.DB
	mu   sync.Mutex
	subs map[string]*subState

	sweepEvery time.Duration
	now        func() time.Time
	stop       chan struct{}
	done       chan struct{}
}

// Option configures a Store.
type Option func(*Store)

// WithSweepInterval sets how often expired subscriptions and messages are
// collected. Zero disables the sweeper.
func WithSweepInterval(d time.Duration) Option {
	return func(s *Store) { s.sweepEvery = d }
}

// Open opens or creates the store at path and loads its state.
func Open(path string, opts ...Option) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("boltstore: open: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{bucketSubs, bucketMsgs, bucketIdx, bucketSeq} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("boltstore: init buckets: %w", err)
	}

	s := &Store{
		db:         db,
		subs:       make(map[string]*subState),
		sweepEvery: time.Minute,
		now:        time.Now,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	if err := s.load(); err != nil {
		db.Close()
		return nil, err
	}
	if s.sweepEvery > 0 {
		go s.sweepLoop()
	}
	return s, nil
}

// Close stops the sweeper and closes the database.
func (s *Store) Close() error {
	close(s.stop)
	<-s.done
	return s.db.Close()
}

// load restores the hot layer from disk. Keys sort by per-subscription
// sequence, so ForEach yields each subscription's queue in arrival order.
// Messages marked delivered but unacknowledged are re-enqueued
// (at-least-once across restarts).
func (s *Store) load() error {
	return s.db.View(func(tx *bolt.Tx) error {
		sb := tx.Bucket([]byte(bucketSubs))
		if err := sb.ForEach(func(k, v []byte) error {
			var sub storedSub
			if err := json.Unmarshal(v, &sub); err != nil {
				return err
			}
			s.subs[sub.ID] = &subState{
				sub:       sub.toService(),
				delivered: make(map[string]*service.Message),
				notify:    make(chan struct{}, 1),
			}
			return nil
		}); err != nil {
			return err
		}

		mb := tx.Bucket([]byte(bucketMsgs))
		return mb.ForEach(func(k, v []byte) error {
			var m storedMsg
			if err := json.Unmarshal(v, &m); err != nil {
				return err
			}
			ms, ok := s.subs[m.SubscriptionID]
			if !ok {
				return nil
			}
			ms.queue = append(ms.queue, m.toService())
			return nil
		})
	})
}

// CreateSubscription stores a new subscription.
func (s *Store) CreateSubscription(_ context.Context, sub service.Subscription) (service.Subscription, error) {
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = s.now()
	}
	rec := storedSub{
		ID:            sub.ID,
		CreatedAt:     sub.CreatedAt,
		ExpiresAt:     sub.ExpiresAt,
		RestrictedKey: sub.RestrictedKey,
	}
	if err := s.putSub(rec); err != nil {
		return service.Subscription{}, err
	}
	s.mu.Lock()
	s.subs[sub.ID] = &subState{
		sub:       sub,
		delivered: make(map[string]*service.Message),
		notify:    make(chan struct{}, 1),
	}
	s.mu.Unlock()
	return sub, nil
}

// GetSubscription returns a subscription.
func (s *Store) GetSubscription(_ context.Context, id string) (service.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ms, ok := s.subs[id]
	if !ok {
		return service.Subscription{}, service.ErrSubscriptionNotFound
	}
	if ms.sub.Expired(s.now()) {
		s.dropLocked(id)
		return service.Subscription{}, service.ErrSubscriptionExpired
	}
	return ms.sub, nil
}

// DeleteSubscription removes a subscription and its pending messages.
func (s *Store) DeleteSubscription(_ context.Context, id string) error {
	s.mu.Lock()
	ms, ok := s.subs[id]
	if ok {
		delete(s.subs, id)
	}
	err := s.deleteSubLocked(id)
	s.mu.Unlock()

	if !ok {
		if err == nil {
			return service.ErrSubscriptionNotFound
		}
		return err
	}
	if err != nil {
		return err
	}
	s.signal(ms)
	return nil
}

// Enqueue stores a message for delivery.
//
// A zero-TTL message is only kept while a user agent is waiting, otherwise
// it expires immediately (RFC 8030 §5.2).
func (s *Store) Enqueue(_ context.Context, msg service.Message) error {
	now := s.now()
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.TTL == 0 {
		msg.ExpiresAt = msg.CreatedAt.Add(zeroTTLLifetime)
	} else {
		msg.ExpiresAt = msg.CreatedAt.Add(time.Duration(msg.TTL) * time.Second)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	ms, ok := s.subs[msg.SubscriptionID]
	if !ok {
		return service.ErrSubscriptionNotFound
	}
	if ms.sub.Expired(now) {
		s.dropLocked(msg.SubscriptionID)
		return service.ErrSubscriptionExpired
	}
	if msg.TTL == 0 && ms.polling == 0 {
		return nil
	}
	stored := storeMsg(&msg)
	if err := s.putMsgLocked(&stored); err != nil {
		return err
	}
	ms.queue = append(ms.queue, &msg)
	s.signal(ms)
	return nil
}

// Poll waits for a message of the subscription that satisfies minUrgency.
func (s *Store) Poll(ctx context.Context, subscriptionID string, minUrgency pushmsg.Urgency) (*service.Message, error) {
	for {
		s.mu.Lock()
		ms, ok := s.subs[subscriptionID]
		if !ok {
			s.mu.Unlock()
			return nil, service.ErrSubscriptionNotFound
		}
		if ms.sub.Expired(s.now()) {
			s.dropLocked(subscriptionID)
			s.mu.Unlock()
			return nil, service.ErrSubscriptionExpired
		}
		ms.polling++
		msg := ms.pop(s.now(), minUrgency)
		if msg != nil {
			ms.delivered[msg.ID] = msg
			ms.polling--
			mark := storeMsg(msg)
			mark.Delivered = true
			err := s.putMsgLocked(&mark)
			s.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return msg, nil
		}
		notify := ms.notify
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			s.mu.Lock()
			if cur, ok := s.subs[subscriptionID]; ok && cur == ms {
				cur.polling--
			}
			s.mu.Unlock()
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

// Ack removes a delivered message.
func (s *Store) Ack(_ context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var found bool
	for _, ms := range s.subs {
		if _, ok := ms.delivered[messageID]; ok {
			found = true
			delete(ms.delivered, messageID)
			break
		}
	}
	if !found {
		return service.ErrMessageNotFound
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		mb := tx.Bucket([]byte(bucketMsgs))
		ib := tx.Bucket([]byte(bucketIdx))
		key := ib.Get([]byte(messageID))
		if key != nil {
			if err := mb.Delete(key); err != nil {
				return err
			}
		}
		return ib.Delete([]byte(messageID))
	})
}

// pop returns the first message satisfying the urgency filter and drops
// expired ones. The caller must hold the lock.
func (ms *subState) pop(now time.Time, minUrgency pushmsg.Urgency) *service.Message {
	kept := ms.queue[:0]
	var found *service.Message
	for _, msg := range ms.queue {
		if found == nil && !msg.ExpiresAt.IsZero() && !now.Before(msg.ExpiresAt) {
			continue
		}
		if found == nil && urgencyAtLeast(msg.Urgency, minUrgency) {
			found = msg
			continue
		}
		kept = append(kept, msg)
	}
	clear(ms.queue[len(kept):])
	ms.queue = kept
	return found
}

func urgencyAtLeast(urgency, minimum pushmsg.Urgency) bool {
	if urgency == pushmsg.UrgencyUnset {
		urgency = pushmsg.DefaultUrgency
	}
	if minimum == pushmsg.UrgencyUnset {
		minimum = pushmsg.UrgencyVeryLow
	}
	return urgency >= minimum
}

// signal wakes up a waiting poller.
func (s *Store) signal(ms *subState) {
	select {
	case ms.notify <- struct{}{}:
	default:
	}
}

// dropLocked removes a subscription from both layers. The caller must hold
// the lock.
func (s *Store) dropLocked(id string) {
	delete(s.subs, id)
	_ = s.deleteSubLocked(id)
}

func (s *Store) deleteSubLocked(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket([]byte(bucketSubs)).Delete([]byte(id)); err != nil {
			return err
		}
		if err := tx.Bucket([]byte(bucketSeq)).Delete([]byte(id)); err != nil {
			return err
		}
		prefix := []byte(id + "/")
		mb := tx.Bucket([]byte(bucketMsgs))
		ib := tx.Bucket([]byte(bucketIdx))
		c := mb.Cursor()
		var msgIDs [][]byte
		var keys [][]byte
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var m storedMsg
			if err := json.Unmarshal(v, &m); err == nil {
				msgIDs = append(msgIDs, []byte(m.ID))
			}
			keys = append(keys, append([]byte(nil), k...))
		}
		for _, k := range keys {
			if err := mb.Delete(k); err != nil {
				return err
			}
		}
		for _, id := range msgIDs {
			if err := ib.Delete(id); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) putSub(rec storedSub) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return putJSON(tx.Bucket([]byte(bucketSubs)), rec.ID, rec)
	})
}

// putMsgLocked writes a message record and its id index entry, allocating a
// per-subscription sequence number for queue ordering. The caller must hold
// the lock so that seq allocation matches the volatile append order.
func (s *Store) putMsgLocked(m *storedMsg) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		sb := tx.Bucket([]byte(bucketSeq))
		mb := tx.Bucket([]byte(bucketMsgs))
		ib := tx.Bucket([]byte(bucketIdx))

		key := ib.Get([]byte(m.ID))
		var keyStr string
		if key == nil {
			var seq uint64
			if enc := sb.Get([]byte(m.SubscriptionID)); enc != nil {
				seq = binary.BigEndian.Uint64(enc)
			}
			seq++
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, seq)
			if err := sb.Put([]byte(m.SubscriptionID), buf); err != nil {
				return err
			}
			keyStr = fmt.Sprintf("%s/%020d", m.SubscriptionID, seq)
			if err := ib.Put([]byte(m.ID), []byte(keyStr)); err != nil {
				return err
			}
		} else {
			keyStr = string(key)
		}
		m.Key = keyStr
		return putJSON(mb, keyStr, m)
	})
}

func (s *Store) sweepLoop() {
	defer close(s.done)
	t := time.NewTicker(s.sweepEvery)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.sweep()
		}
	}
}

// sweep removes expired subscriptions and queued messages.
func (s *Store) sweep() {
	now := s.now()
	type purge struct {
		subID  string
		all    bool
		msgIDs []string
	}
	var purges []purge

	s.mu.Lock()
	for id, ms := range s.subs {
		if ms.sub.Expired(now) {
			purges = append(purges, purge{subID: id, all: true})
			delete(s.subs, id)
			s.signal(ms)
			continue
		}
		var dead []string
		kept := ms.queue[:0]
		for _, msg := range ms.queue {
			if !msg.ExpiresAt.IsZero() && !now.Before(msg.ExpiresAt) {
				dead = append(dead, msg.ID)
				continue
			}
			kept = append(kept, msg)
		}
		clear(ms.queue[len(kept):])
		ms.queue = kept
		if len(dead) > 0 {
			purges = append(purges, purge{subID: id, msgIDs: dead})
		}
	}
	var errs []error
	for _, p := range purges {
		if p.all {
			errs = append(errs, s.deleteSubLocked(p.subID))
			continue
		}
		errs = append(errs, s.db.Update(func(tx *bolt.Tx) error {
			mb := tx.Bucket([]byte(bucketMsgs))
			ib := tx.Bucket([]byte(bucketIdx))
			for _, msgID := range p.msgIDs {
				if key := ib.Get([]byte(msgID)); key != nil {
					if err := mb.Delete(key); err != nil {
						return err
					}
				}
				if err := ib.Delete([]byte(msgID)); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	s.mu.Unlock()
	_ = errs
}

func putJSON(b *bolt.Bucket, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return b.Put([]byte(key), data)
}
