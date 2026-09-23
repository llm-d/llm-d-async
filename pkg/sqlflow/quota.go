package sqlflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
)

const releaseRetryInterval = time.Second

// QuotaStore counts sql-quota gates in the transport's database. Every
// dispatcher runs at most one statement per quota key at a time; requests that
// arrive while it runs go in the next statement together.
type QuotaStore struct {
	store   *sqlqueue.Store
	ttl     time.Duration
	timeout time.Duration
	logger  logr.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	holderMu sync.Mutex
	holder   string
	renewed  time.Time

	slots *admitBatcher[slotKey]
	rates *admitBatcher[rateKey]

	releaseMu sync.Mutex
	pending   map[string]map[string]int
	releasing bool
}

type slotKey struct {
	key   string
	limit int
}

type rateKey struct {
	key    string
	limit  int
	window time.Duration
}

// NewQuotaStore starts the holder heartbeat; Close stops it. Concurrency slots
// of a holder that stops heartbeating are freed once its lease of ttl lapses.
func NewQuotaStore(store *sqlqueue.Store, ttl, timeout time.Duration, logger logr.Logger) *QuotaStore {
	ctx, cancel := context.WithCancel(context.Background())
	s := &QuotaStore{
		store:   store,
		ttl:     ttl,
		timeout: timeout,
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
		pending: map[string]map[string]int{},
	}
	s.slots = newAdmitBatcher(s.acquireSlots)
	s.rates = newAdmitBatcher(s.admitRate)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.heartbeat()
	}()
	return s
}

func (s *QuotaStore) Close() {
	s.cancel()
	s.wg.Wait()
}

func (s *QuotaStore) AcquireSlot(ctx context.Context, key string, limit int) (func(), bool, error) {
	r, err := s.slots.admit(ctx, slotKey{key: key, limit: limit}, func(r admitReply) {
		if r.ok {
			s.release(r.holder, key)
		}
	})
	if err != nil || !r.ok {
		return nil, false, err
	}
	var once sync.Once
	return func() { once.Do(func() { s.release(r.holder, key) }) }, true, nil
}

func (s *QuotaStore) Admit(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	r, err := s.rates.admit(ctx, rateKey{key: key, limit: limit, window: window}, nil)
	return r.ok, err
}

func (s *QuotaStore) acquireSlots(k slotKey, n int) (int, string, error) {
	for retried := false; ; retried = true {
		holder, err := s.currentHolder()
		if err != nil {
			return 0, "", err
		}
		ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
		granted, err := s.store.AcquireQuotaSlots(ctx, holder, k.key, n, k.limit)
		cancel()
		if errors.Is(err, sqlqueue.ErrQuotaHolderLapsed) && !retried {
			s.dropHolder(holder)
			continue
		}
		if err != nil {
			return 0, "", fmt.Errorf("acquire quota slots for %s: %w", k.key, err)
		}
		return granted, holder, nil
	}
}

func (s *QuotaStore) admitRate(k rateKey, n int) (int, string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	granted, err := s.store.AdmitQuota(ctx, k.key, n, k.limit, k.window)
	if err != nil {
		return 0, "", fmt.Errorf("admit quota for %s: %w", k.key, err)
	}
	return granted, "", nil
}

func (s *QuotaStore) currentHolder() (string, error) {
	s.holderMu.Lock()
	defer s.holderMu.Unlock()
	if s.holder != "" {
		return s.holder, nil
	}
	name, err := newOwner()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	started := time.Now()
	if err := s.store.RegisterQuotaHolder(ctx, name, s.ttl); err != nil {
		return "", fmt.Errorf("register quota holder: %w", err)
	}
	s.holder, s.renewed = name, started
	return name, nil
}

func (s *QuotaStore) dropHolder(holder string) {
	s.holderMu.Lock()
	defer s.holderMu.Unlock()
	if s.holder == holder {
		s.holder = ""
	}
}

func (s *QuotaStore) heartbeat() {
	ticker := time.NewTicker(max(100*time.Millisecond, s.ttl/3))
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		if !s.renew() {
			continue
		}
		ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
		if err := s.store.ReapQuotaHolders(ctx, s.ttl); err != nil {
			s.logger.Error(err, "Failed to reap lapsed quota holders")
		}
		cancel()
	}
}

// renew reports whether this dispatcher had a holder to renew.
func (s *QuotaStore) renew() bool {
	s.holderMu.Lock()
	holder, renewed := s.holder, s.renewed
	s.holderMu.Unlock()
	if holder == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	started := time.Now()
	live, err := s.store.RenewQuotaHolder(ctx, holder, s.ttl)
	switch {
	case err != nil && time.Since(renewed) < s.ttl:
		s.logger.Error(err, "Failed to renew quota holder", "holder", holder)
	case err != nil || !live:
		s.logger.Info("Quota holder lease lapsed; its concurrency slots no longer count", "holder", holder)
		s.dropHolder(holder)
	default:
		s.holderMu.Lock()
		if s.holder == holder {
			s.renewed = started
		}
		s.holderMu.Unlock()
	}
	return true
}

func (s *QuotaStore) release(holder, key string) {
	s.releaseMu.Lock()
	byKey := s.pending[holder]
	if byKey == nil {
		byKey = map[string]int{}
		s.pending[holder] = byKey
	}
	byKey[key]++
	start := !s.releasing
	s.releasing = true
	s.releaseMu.Unlock()
	if start {
		go s.flushReleases()
	}
}

func (s *QuotaStore) flushReleases() {
	for {
		s.releaseMu.Lock()
		batch := s.pending
		if len(batch) == 0 {
			s.releasing = false
			s.releaseMu.Unlock()
			return
		}
		s.pending = map[string]map[string]int{}
		s.releaseMu.Unlock()

		var failed map[string]map[string]int
		for holder, byKey := range batch {
			keys := make([]string, 0, len(byKey))
			counts := make([]int, 0, len(byKey))
			for k, n := range byKey {
				keys = append(keys, k)
				counts = append(counts, n)
			}
			ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
			err := s.store.ReleaseQuotaSlots(ctx, holder, keys, counts)
			cancel()
			if err != nil {
				s.logger.Error(err, "Failed to release quota slots; retrying", "holder", holder, "keys", len(keys))
				if failed == nil {
					failed = map[string]map[string]int{}
				}
				failed[holder] = byKey
			}
		}
		if failed == nil {
			continue
		}
		s.releaseMu.Lock()
		for holder, byKey := range failed {
			if s.pending[holder] == nil {
				s.pending[holder] = map[string]int{}
			}
			for k, n := range byKey {
				s.pending[holder][k] += n
			}
		}
		s.releaseMu.Unlock()
		select {
		case <-s.ctx.Done():
			s.releaseMu.Lock()
			s.releasing = false
			s.releaseMu.Unlock()
			return
		case <-time.After(releaseRetryInterval):
		}
	}
}

type admitReply struct {
	ok     bool
	holder string
	err    error
}

// admitBatcher sends one statement per key at a time; callers that arrive
// while it runs share the next one, first come first admitted.
type admitBatcher[K comparable] struct {
	run        func(key K, n int) (granted int, holder string, err error)
	mu         sync.Mutex
	queues     map[K]*admitQueue
	statements atomic.Int64
}

type admitQueue struct {
	waiting []chan admitReply
}

func newAdmitBatcher[K comparable](run func(key K, n int) (int, string, error)) *admitBatcher[K] {
	return &admitBatcher[K]{run: run, queues: map[K]*admitQueue{}}
}

// admit waits for key's next statement. If ctx ends first, abandoned receives
// the reply once it arrives so the caller can give back what it was granted.
func (b *admitBatcher[K]) admit(ctx context.Context, key K, abandoned func(admitReply)) (admitReply, error) {
	reply := make(chan admitReply, 1)
	b.mu.Lock()
	q, busy := b.queues[key]
	if !busy {
		q = &admitQueue{}
		b.queues[key] = q
	}
	q.waiting = append(q.waiting, reply)
	b.mu.Unlock()
	if !busy {
		go b.flush(key, q)
	}
	select {
	case r := <-reply:
		return r, r.err
	case <-ctx.Done():
		if abandoned != nil {
			go func() { abandoned(<-reply) }()
		}
		return admitReply{}, fmt.Errorf("await quota admission: %w", ctx.Err())
	}
}

func (b *admitBatcher[K]) flush(key K, q *admitQueue) {
	for {
		b.mu.Lock()
		batch := q.waiting
		q.waiting = nil
		if len(batch) == 0 {
			delete(b.queues, key)
			b.mu.Unlock()
			return
		}
		b.mu.Unlock()
		b.statements.Add(1)
		granted, holder, err := b.run(key, len(batch))
		for i, reply := range batch {
			reply <- admitReply{ok: err == nil && i < granted, holder: holder, err: err}
		}
	}
}
