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

const quotaReapBatch = 1000

// QuotaStore counts sql-quota gates in the transport's database. Every
// dispatcher runs at most one statement per quota key at a time; requests that
// arrive while it runs go in the next statement together.
//
// Concurrency slots are counted per holder. A statement that returns an error
// retires its holder instead of being retried. A retired holder takes no new
// grants, is renewed while any statement, grant or release names it, and is
// deleted once none does.
type QuotaStore struct {
	store   *sqlqueue.Store
	ttl     time.Duration
	timeout time.Duration
	logger  logr.Logger

	ctx    context.Context
	cancel context.CancelFunc
	lifeMu sync.Mutex
	closed bool
	wg     sync.WaitGroup

	registerMu sync.Mutex
	mu         sync.Mutex
	current    *quotaHolder
	holders    map[*quotaHolder]struct{}

	slots *admitBatcher[slotKey]
	rates *admitBatcher[rateKey]
	used  atomic.Bool

	releaseMu sync.Mutex
	pending   map[*quotaHolder]map[string]int
	releasing bool
}

// quotaHolder is one holder row; known counts the statements in flight, grants
// handed out and releases queued under it. A retired holder that nothing names
// is deleting until its row is gone. Guarded by QuotaStore.mu.
type quotaHolder struct {
	name     string
	known    int
	renewed  time.Time
	retired  bool
	deleting bool
	gone     bool
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

// NewQuotaStore starts the holder heartbeat; Close stops it. Slots of a
// holder that stops heartbeating are freed once its lease of ttl lapses.
func NewQuotaStore(store *sqlqueue.Store, ttl, timeout time.Duration, logger logr.Logger) *QuotaStore {
	ctx, cancel := context.WithCancel(context.Background())
	s := &QuotaStore{
		store:   store,
		ttl:     ttl,
		timeout: timeout,
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
		holders: map[*quotaHolder]struct{}{},
		pending: map[*quotaHolder]map[string]int{},
	}
	s.slots = newAdmitBatcher(s.acquireSlots, s.spawn)
	s.rates = newAdmitBatcher(s.admitRate, s.spawn)
	s.spawn(s.heartbeat)
	return s
}

// spawn runs fn on a goroutine Close waits for, and reports false once Close
// has begun.
func (s *QuotaStore) spawn(fn func()) bool {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	if s.closed {
		return false
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
	return true
}

// Close waits for every statement and queued release, then deletes the holders
// nothing names so their rows do not wait out the lease. A closed store admits
// nothing.
func (s *QuotaStore) Close() {
	s.lifeMu.Lock()
	s.closed = true
	s.lifeMu.Unlock()
	s.cancel()
	s.wg.Wait()
	s.mu.Lock()
	var idle []*quotaHolder
	for h := range s.holders {
		if h.known == 0 && !h.gone {
			idle = append(idle, h)
		}
	}
	s.mu.Unlock()
	for _, h := range idle {
		s.delete(h)
	}
}

func (s *QuotaStore) AcquireSlot(ctx context.Context, key string, limit int) (func(), bool, error) {
	s.used.Store(true)
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
	s.used.Store(true)
	r, err := s.rates.admit(ctx, rateKey{key: key, limit: limit, window: window}, nil)
	return r.ok, err
}

func (s *QuotaStore) acquireSlots(k slotKey, n int) (int, *quotaHolder, error) {
	for retried := false; ; retried = true {
		h, err := s.begin()
		if err != nil {
			return 0, nil, err
		}
		ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
		granted, err := s.store.AcquireQuotaSlots(ctx, h.name, k.key, n, k.limit)
		cancel()
		if err != nil {
			s.retire(h)
			s.settle(h, 1)
			if errors.Is(err, sqlqueue.ErrQuotaHolderLapsed) && !retried {
				continue
			}
			return 0, nil, fmt.Errorf("acquire quota slots for %s: %w", k.key, err)
		}
		s.settle(h, 1-granted)
		return granted, h, nil
	}
}

func (s *QuotaStore) admitRate(k rateKey, n int) (int, *quotaHolder, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	granted, err := s.store.AdmitQuota(ctx, k.key, n, k.limit, k.window)
	if err != nil {
		return 0, nil, fmt.Errorf("admit quota for %s: %w", k.key, err)
	}
	return granted, nil, nil
}

// begin counts a statement against the current holder, registering one first
// if there is none.
func (s *QuotaStore) begin() (*quotaHolder, error) {
	if h := s.countCurrent(); h != nil {
		return h, nil
	}
	s.registerMu.Lock()
	defer s.registerMu.Unlock()
	if h := s.countCurrent(); h != nil {
		return h, nil
	}
	name, err := newOwner()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	started := time.Now()
	if err := s.store.RegisterQuotaHolder(ctx, name, s.ttl); err != nil {
		return nil, fmt.Errorf("register quota holder: %w", err)
	}
	h := &quotaHolder{name: name, renewed: started, known: 1}
	s.mu.Lock()
	s.current = h
	s.holders[h] = struct{}{}
	s.mu.Unlock()
	return h, nil
}

func (s *QuotaStore) countCurrent() *quotaHolder {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return nil
	}
	s.current.known++
	return s.current
}

func (s *QuotaStore) retire(h *quotaHolder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h.retired = true
	if s.current == h {
		s.current = nil
	}
}

// settle takes n off h's known count and deletes h once it is retired and
// nothing names it.
func (s *QuotaStore) settle(h *quotaHolder, n int) {
	s.mu.Lock()
	h.known -= n
	drop := h.retired && !h.gone && !h.deleting && h.known == 0
	if drop {
		h.deleting = true
	}
	s.mu.Unlock()
	if drop {
		s.delete(h)
	}
}

// delete removes a retired holder's row; the heartbeat retries it until it lands.
func (s *QuotaStore) delete(h *quotaHolder) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	if err := s.store.DeleteQuotaHolder(ctx, h.name); err != nil {
		s.logger.Error(err, "Failed to delete retired quota holder; retrying", "holder", h.name)
		return
	}
	s.mu.Lock()
	h.gone = true
	delete(s.holders, h)
	s.mu.Unlock()
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
		if !s.used.Load() {
			continue
		}
		s.mu.Lock()
		holders := make([]*quotaHolder, 0, len(s.holders))
		for h := range s.holders {
			holders = append(holders, h)
		}
		s.mu.Unlock()
		for _, h := range holders {
			s.mu.Lock()
			deleting := h.deleting
			s.mu.Unlock()
			if deleting {
				s.delete(h)
			} else {
				s.renew(h)
			}
		}
		ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
		if err := s.store.ReapQuotaHolders(ctx, s.ttl); err != nil {
			s.logger.Error(err, "Failed to reap lapsed quota holders")
		}
		if _, err := s.store.ReapQuota(ctx, quotaReapBatch); err != nil {
			s.logger.Error(err, "Failed to reap idle quota rows")
		}
		cancel()
	}
}

func (s *QuotaStore) renew(h *quotaHolder) {
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	started := time.Now()
	live, err := s.store.RenewQuotaHolder(ctx, h.name, s.ttl)

	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err == nil && live:
		h.renewed = started
	case err == nil:
		if h.known > 0 {
			s.logger.Error(nil, "Quota holder lease lapsed with grants in use; the limit may be exceeded until they finish", "holder", h.name, "known", h.known)
		}
		h.retired, h.gone = true, true
		delete(s.holders, h)
		if s.current == h {
			s.current = nil
		}
	case time.Since(h.renewed) >= s.ttl:
		s.logger.Error(err, "Cannot renew quota holder; retiring it", "holder", h.name)
		h.retired = true
		if s.current == h {
			s.current = nil
		}
	default:
		s.logger.Error(err, "Failed to renew quota holder", "holder", h.name)
	}
}

func (s *QuotaStore) release(h *quotaHolder, key string) {
	s.releaseMu.Lock()
	byKey := s.pending[h]
	if byKey == nil {
		byKey = map[string]int{}
		s.pending[h] = byKey
	}
	byKey[key]++
	start := !s.releasing
	s.releasing = true
	s.releaseMu.Unlock()
	if start && !s.spawn(s.flushReleases) {
		s.flushReleases()
	}
}

// flushReleases sends each holder's queued releases once; a failed release
// retires its holder.
func (s *QuotaStore) flushReleases() {
	for {
		s.releaseMu.Lock()
		batch := s.pending
		if len(batch) == 0 {
			s.releasing = false
			s.releaseMu.Unlock()
			return
		}
		s.pending = map[*quotaHolder]map[string]int{}
		s.releaseMu.Unlock()

		for h, byKey := range batch {
			keys := make([]string, 0, len(byKey))
			counts := make([]int, 0, len(byKey))
			total := 0
			for k, n := range byKey {
				keys = append(keys, k)
				counts = append(counts, n)
				total += n
			}
			ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
			err := s.store.ReleaseQuotaSlots(ctx, h.name, keys, counts)
			cancel()
			if err != nil {
				s.logger.Error(err, "Failed to release quota slots; retiring the holder", "holder", h.name, "keys", len(keys))
				s.retire(h)
			}
			s.settle(h, total)
		}
	}
}

type admitReply struct {
	ok     bool
	holder *quotaHolder
	err    error
}

var errQuotaStoreClosed = errors.New("quota store closed")

// admitBatcher sends one statement per key at a time; callers that arrive
// while it runs share the next one, first come first admitted.
type admitBatcher[K comparable] struct {
	run        func(key K, n int) (granted int, holder *quotaHolder, err error)
	spawn      func(func()) bool
	mu         sync.Mutex
	queues     map[K]*admitQueue
	statements atomic.Int64
}

type admitQueue struct {
	waiting []*admitWaiter
}

const (
	waiterPending int32 = iota
	waiterAnswered
	waiterAbandoned
)

// admitWaiter is answered by the flush that serves it unless its caller gave
// up first, in which case that flush hands the reply to abandoned.
type admitWaiter struct {
	state     atomic.Int32
	reply     chan admitReply
	abandoned func(admitReply)
}

func newAdmitBatcher[K comparable](run func(key K, n int) (int, *quotaHolder, error), spawn func(func()) bool) *admitBatcher[K] {
	return &admitBatcher[K]{run: run, spawn: spawn, queues: map[K]*admitQueue{}}
}

// admit waits for key's next statement. If ctx ends first, abandoned receives
// the reply once it arrives so the caller can give back what it was granted.
func (b *admitBatcher[K]) admit(ctx context.Context, key K, abandoned func(admitReply)) (admitReply, error) {
	w := &admitWaiter{reply: make(chan admitReply, 1), abandoned: abandoned}
	b.mu.Lock()
	q, busy := b.queues[key]
	if !busy {
		q = &admitQueue{}
		if !b.spawn(func() { b.flush(key, q) }) {
			b.mu.Unlock()
			return admitReply{}, errQuotaStoreClosed
		}
		b.queues[key] = q
	}
	q.waiting = append(q.waiting, w)
	b.mu.Unlock()
	select {
	case r := <-w.reply:
		return r, r.err
	case <-ctx.Done():
		if w.state.CompareAndSwap(waiterPending, waiterAbandoned) {
			return admitReply{}, fmt.Errorf("await quota admission: %w", ctx.Err())
		}
		r := <-w.reply
		return r, r.err
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
		for i, w := range batch {
			r := admitReply{ok: err == nil && i < granted, holder: holder, err: err}
			if w.state.CompareAndSwap(waiterPending, waiterAnswered) {
				w.reply <- r
			} else if w.abandoned != nil {
				w.abandoned(r)
			}
		}
	}
}
