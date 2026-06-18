package rulestore

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

const SnapshotCacheKey = "stellpulsar:rules:snapshot"

type Cache interface {
	Get(context.Context, string) ([]byte, bool, error)
	Set(context.Context, string, []byte) error
}

type Store struct {
	current        atomic.Pointer[Snapshot]
	cache          Cache
	mu             sync.Mutex
	nextSubscriber int64
	subscribers    map[int64]chan Snapshot
}

func NewStore(initial Snapshot) *Store {
	store := &Store{}
	store.Replace(initial)
	return store
}

func NewEmptyStore(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return NewStore(EmptySnapshot(now()))
}

func (s *Store) Replace(snapshot Snapshot) {
	_ = s.ReplaceWithContext(context.Background(), snapshot)
}

func (s *Store) ReplaceWithContext(ctx context.Context, snapshot Snapshot) error {
	cloned := snapshot.Clone()
	s.current.Store(&cloned)
	err := s.cacheSnapshot(ctx, cloned)
	s.notify(cloned)
	return err
}

func (s *Store) AttachCache(ctx context.Context, cache Cache) error {
	if cache == nil {
		return nil
	}
	s.cache = cache
	if payload, ok, err := cache.Get(ctx, SnapshotCacheKey); err != nil {
		return err
	} else if ok {
		var cached Snapshot
		if err := json.Unmarshal(payload, &cached); err != nil {
			return err
		}
		s.current.Store(&cached)
		return nil
	}
	return s.cacheSnapshot(ctx, s.Snapshot())
}

func (s *Store) Snapshot() Snapshot {
	current := s.current.Load()
	if current == nil {
		return EmptySnapshot(time.Now())
	}
	return current.Clone()
}

func (s *Store) Subscribe(ctx context.Context, buffer int) (<-chan Snapshot, func()) {
	if buffer <= 0 {
		buffer = 1
	}
	ch := make(chan Snapshot, buffer)

	s.mu.Lock()
	if s.subscribers == nil {
		s.subscribers = map[int64]chan Snapshot{}
	}
	s.nextSubscriber++
	id := s.nextSubscriber
	s.subscribers[id] = ch
	s.mu.Unlock()

	var closed atomic.Bool
	unsubscribe := func() {
		if !closed.CompareAndSwap(false, true) {
			return
		}
		s.mu.Lock()
		delete(s.subscribers, id)
		close(ch)
		s.mu.Unlock()
	}
	if ctx != nil && ctx.Done() != nil {
		go func() {
			<-ctx.Done()
			unsubscribe()
		}()
	}
	return ch, unsubscribe
}

func (s *Store) FindRule(applicationCode, ruleID string) (Rule, bool) {
	current := s.current.Load()
	if current == nil {
		return Rule{}, false
	}
	app, ok := current.Apps[applicationCode]
	if !ok {
		return Rule{}, false
	}
	rule, ok := app.Rules[ruleID]
	if !ok {
		return Rule{}, false
	}
	return rule.Clone(), true
}

func (s *Store) Rules(applicationCode string, ruleIDs []string) []Rule {
	current := s.current.Load()
	if current == nil {
		return nil
	}
	app, ok := current.Apps[applicationCode]
	if !ok {
		return nil
	}
	if len(ruleIDs) == 0 {
		rules := make([]Rule, 0, len(app.Rules))
		for _, rule := range app.Rules {
			rules = append(rules, rule.Clone())
		}
		return rules
	}

	rules := make([]Rule, 0, len(ruleIDs))
	for _, id := range ruleIDs {
		if rule, ok := app.Rules[id]; ok {
			rules = append(rules, rule.Clone())
		}
	}
	return rules
}

func (s *Store) cacheSnapshot(ctx context.Context, snapshot Snapshot) error {
	if s.cache == nil {
		return nil
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	return s.cache.Set(ctx, SnapshotCacheKey, payload)
}

func (s *Store) notify(snapshot Snapshot) {
	s.mu.Lock()
	subscribers := make([]chan Snapshot, 0, len(s.subscribers))
	for _, ch := range s.subscribers {
		subscribers = append(subscribers, ch)
	}
	s.mu.Unlock()

	for _, ch := range subscribers {
		cloned := snapshot.Clone()
		select {
		case ch <- cloned:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- cloned:
			default:
			}
		}
	}
}
