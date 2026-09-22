package sqlqueue

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Consumer is one dispatcher's partitions and in-flight requests on a queue.
//
//	owned ──(above share)──> draining ──(nothing in flight, or drainTimeout)──> released
//	  ^                          │
//	  └───────(below share)──────┘
type Consumer struct {
	store        *Store
	queue        string
	owner        string
	leaseTTL     time.Duration
	drainTimeout time.Duration

	cycle sync.Mutex

	mu        sync.Mutex
	seeded    bool
	stopping  bool
	reconcile bool
	beats     int
	leases    map[int]*partitionState
	inflight  map[Key]tracked
	orphans   map[Key]int64
}

type tracked struct {
	partition int
	epoch     int64
}

type partitionState struct {
	epoch      int64
	acquiredAt time.Time
	draining   bool
	drainSince time.Time
	inflight   int
}

func NewConsumer(store *Store, queue, owner string, leaseTTL, drainTimeout time.Duration) *Consumer {
	return &Consumer{
		store:        store,
		queue:        queue,
		owner:        owner,
		leaseTTL:     leaseTTL,
		drainTimeout: drainTimeout,
		leases:       map[int]*partitionState{},
		inflight:     map[Key]tracked{},
		orphans:      map[Key]int64{},
	}
}

// Poll marks the consumer for reconciliation on error, since the dispatch may have committed.
func (c *Consumer) Poll(ctx context.Context, now time.Time, limit int) ([]Request, error) {
	c.cycle.Lock()
	defer c.cycle.Unlock()
	rows, err := c.store.Dispatch(ctx, c.queue, c.owner, now, limit)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.reconcile = true
		return nil, err
	}
	for _, r := range rows {
		if prev, ok := c.inflight[r.Key()]; ok {
			if st, ok := c.leases[prev.partition]; ok && st.inflight > 0 {
				st.inflight--
			}
		}
		delete(c.orphans, r.Key())
		c.inflight[r.Key()] = tracked{partition: r.Partition, epoch: r.Epoch}
		if st, ok := c.leases[r.Partition]; ok {
			st.inflight++
		}
	}
	return rows, nil
}

func (c *Consumer) Ack(ctx context.Context, completions []Completion) ([]bool, error) {
	acked, err := c.store.Ack(ctx, c.owner, completions)
	if err != nil {
		return nil, err
	}
	stamps := make([]Stamp, len(completions))
	for i, comp := range completions {
		stamps[i] = Stamp{Key: comp.Key, Epoch: comp.Epoch}
	}
	c.doneStamps(stamps)
	return acked, nil
}

func (c *Consumer) Retry(ctx context.Context, stamp Stamp, notBefore int64, envelope string) (bool, error) {
	ok, err := c.store.Retry(ctx, c.owner, stamp, notBefore, envelope)
	if err != nil {
		return false, err
	}
	c.doneStamps([]Stamp{stamp})
	return ok, nil
}

func (c *Consumer) Undispatch(ctx context.Context, keys []Key) error {
	stamps := c.stamps(keys)
	if err := c.store.Undispatch(ctx, c.owner, stamps); err != nil {
		c.Abandon(keys...)
		return err
	}
	c.doneStamps(stamps)
	return nil
}

// Abandon hands requests whose outcome could not be written to the next Rebalance.
func (c *Consumer) Abandon(keys ...Key) {
	c.mu.Lock()
	for _, k := range keys {
		if t, ok := c.inflight[k]; ok {
			c.orphans[k] = t.epoch
		}
	}
	c.mu.Unlock()
	c.done(keys)
}

func (c *Consumer) stamps(keys []Key) []Stamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Stamp, 0, len(keys))
	for _, k := range keys {
		if t, ok := c.inflight[k]; ok {
			out = append(out, Stamp{Key: k, Epoch: t.epoch})
		}
	}
	return out
}

func (c *Consumer) done(keys []Key) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		t, ok := c.inflight[k]
		if !ok {
			continue
		}
		delete(c.inflight, k)
		if st, ok := c.leases[t.partition]; ok && st.inflight > 0 {
			st.inflight--
		}
	}
}

func (c *Consumer) doneStamps(stamps []Stamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stamp := range stamps {
		t, ok := c.inflight[stamp.Key]
		if !ok || t.epoch != stamp.Epoch {
			continue
		}
		delete(c.inflight, stamp.Key)
		if st, ok := c.leases[t.partition]; ok && st.inflight > 0 {
			st.inflight--
		}
	}
}

func (c *Consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopping = true
}

func (c *Consumer) Rebalance(ctx context.Context, now time.Time) error {
	c.cycle.Lock()
	defer c.cycle.Unlock()

	c.mu.Lock()
	seeded := c.seeded
	c.mu.Unlock()
	if !seeded {
		if err := c.store.EnsurePartitions(ctx, c.queue); err != nil {
			return err
		}
		c.mu.Lock()
		c.seeded = true
		c.mu.Unlock()
	}
	orphanErr := errors.Join(c.undispatchOrphans(ctx), c.reconcileInFlight(ctx))

	c.mu.Lock()
	stopping := c.stopping
	c.mu.Unlock()
	held, members, err := c.store.Heartbeat(ctx, c.queue, c.owner, now, c.leaseTTL, !stopping)
	if err != nil {
		return errors.Join(orphanErr, err)
	}

	c.mu.Lock()
	c.syncLeases(held, now)
	c.mu.Unlock()
	if err := c.resetStale(ctx, now); err != nil {
		return errors.Join(orphanErr, err)
	}
	if err := c.releaseDrained(ctx, now); err != nil {
		return errors.Join(orphanErr, err)
	}

	c.mu.Lock()
	target := (Partitions + max(1, members) - 1) / max(1, members)
	if stopping {
		target = 0
	}
	active, draining := c.partitionsLocked()
	c.mu.Unlock()

	switch {
	case len(active) < target:
		need := target - len(active)
		undrain := draining[:min(need, len(draining))]
		if err := c.store.SetDraining(ctx, c.queue, c.owner, undrain, false); err != nil {
			return errors.Join(orphanErr, err)
		}
		c.mu.Lock()
		for _, p := range undrain {
			if st, ok := c.leases[p]; ok {
				st.draining = false
			}
		}
		c.mu.Unlock()
		if need -= len(undrain); need == 0 {
			return orphanErr
		}
		acquired, err := c.store.AcquirePartitions(ctx, c.queue, c.owner, now, c.leaseTTL, need)
		if err != nil {
			return errors.Join(orphanErr, err)
		}
		c.mu.Lock()
		parts := make([]int, 0, len(acquired))
		for _, l := range acquired {
			c.leases[l.Partition] = &partitionState{epoch: l.Epoch, acquiredAt: now}
			parts = append(parts, l.Partition)
		}
		c.mu.Unlock()
		if err := c.store.ResetStale(ctx, c.queue, c.owner, parts); err != nil {
			return errors.Join(orphanErr, err)
		}
	case len(active) > target:
		c.mu.Lock()
		drain := c.drainCandidatesLocked(active, len(active)-target)
		c.mu.Unlock()
		if err := c.store.SetDraining(ctx, c.queue, c.owner, drain, true); err != nil {
			return errors.Join(orphanErr, err)
		}
		c.mu.Lock()
		for _, p := range drain {
			if st, ok := c.leases[p]; ok {
				st.draining, st.drainSince = true, now
			}
		}
		c.mu.Unlock()
		if err := c.releaseDrained(ctx, now); err != nil {
			return errors.Join(orphanErr, err)
		}
	}
	return orphanErr
}

func (c *Consumer) releaseDrained(ctx context.Context, now time.Time) error {
	c.mu.Lock()
	var release []int
	for p, st := range c.leases {
		if st.draining && (st.inflight == 0 || now.Sub(st.drainSince) >= c.drainTimeout) {
			release = append(release, p)
		}
	}
	c.mu.Unlock()
	sort.Ints(release)
	if err := c.store.ReleasePartitions(ctx, c.queue, c.owner, release); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range release {
		delete(c.leases, p)
	}
	return nil
}

func (c *Consumer) Close(ctx context.Context) error {
	c.cycle.Lock()
	defer c.cycle.Unlock()
	orphanErr := c.undispatchOrphans(ctx)
	if err := c.store.Leave(ctx, c.queue, c.owner); err != nil {
		return errors.Join(orphanErr, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leases = map[int]*partitionState{}
	return orphanErr
}

func (c *Consumer) undispatchOrphans(ctx context.Context) error {
	c.mu.Lock()
	stamps := make([]Stamp, 0, len(c.orphans))
	for k, epoch := range c.orphans {
		stamps = append(stamps, Stamp{Key: k, Epoch: epoch})
	}
	c.mu.Unlock()
	if len(stamps) == 0 {
		return nil
	}
	if err := c.store.Undispatch(ctx, c.owner, stamps); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, st := range stamps {
		if c.orphans[st.Key] == st.Epoch {
			delete(c.orphans, st.Key)
		}
	}
	return nil
}

func (c *Consumer) reconcileInFlight(ctx context.Context) error {
	c.mu.Lock()
	needed := c.reconcile
	c.mu.Unlock()
	if !needed {
		return nil
	}
	stamped, err := c.store.InFlight(ctx, c.queue, c.owner)
	if err != nil {
		return err
	}
	c.mu.Lock()
	var untracked []Stamp
	for _, st := range stamped {
		if t, ok := c.inflight[st.Key]; !ok || t.epoch != st.Epoch {
			untracked = append(untracked, st)
		}
	}
	c.mu.Unlock()
	if err := c.store.Undispatch(ctx, c.owner, untracked); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reconcile = false
	return nil
}

// resetStale resets partitions acquired within two lease TTLs, where stale stamps come from,
// on every heartbeat, and all owned partitions on every tenth.
func (c *Consumer) resetStale(ctx context.Context, now time.Time) error {
	c.mu.Lock()
	c.beats++
	var parts []int
	if c.beats%fullResetEvery != 0 {
		parts = []int{}
		for p, st := range c.leases {
			if now.Sub(st.acquiredAt) < 2*c.leaseTTL {
				parts = append(parts, p)
			}
		}
		sort.Ints(parts)
	}
	c.mu.Unlock()
	return c.store.ResetStale(ctx, c.queue, c.owner, parts)
}

const fullResetEvery = 10

func (c *Consumer) syncLeases(held []Lease, now time.Time) {
	next := make(map[int]*partitionState, len(held))
	for _, l := range held {
		st, ok := c.leases[l.Partition]
		if !ok || st.epoch != l.Epoch {
			st = &partitionState{epoch: l.Epoch, acquiredAt: now}
		}
		if l.Draining && !st.draining {
			st.drainSince = now
		}
		st.draining = l.Draining
		next[l.Partition] = st
	}
	c.leases = next
}

func (c *Consumer) partitionsLocked() (active, draining []int) {
	for p, st := range c.leases {
		if st.draining {
			draining = append(draining, p)
		} else {
			active = append(active, p)
		}
	}
	sort.Ints(active)
	sort.Ints(draining)
	return active, draining
}

func (c *Consumer) drainCandidatesLocked(active []int, n int) []int {
	picked := append([]int(nil), active...)
	sort.SliceStable(picked, func(i, j int) bool {
		a, b := c.leases[picked[i]].inflight, c.leases[picked[j]].inflight
		if a != b {
			return a < b
		}
		return picked[i] > picked[j]
	})
	picked = picked[:n]
	sort.Ints(picked)
	return picked
}
