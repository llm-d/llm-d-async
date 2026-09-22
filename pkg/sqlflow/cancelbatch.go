package sqlflow

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
)

type cancelQuery struct {
	key   sqlqueue.Key
	reply chan cancelReply
}

type cancelReply struct {
	cancelled bool
	err       error
}

// cancelBatcher coalesces the per-request cancellation checks every worker runs
// before dispatch. Dispatch already reports requests cancelled before they were
// claimed, so these checks only catch a cancellation racing an in-flight
// request; one query per batch keeps that guarantee without one round trip per
// request.
type cancelBatcher struct {
	store   *sqlqueue.Store
	queries chan cancelQuery
	size    int
	linger  time.Duration
	timeout time.Duration
	cancel  context.CancelFunc
	done    chan struct{}
	flushes atomic.Int64
}

// newCancelBatcher starts the coalescing goroutine; the flow stops it in
// Shutdown. Checks can arrive from any worker as soon as the flow exists, so
// the batcher runs for the flow's whole life rather than only while consuming.
func newCancelBatcher(store *sqlqueue.Store, size int, linger, timeout time.Duration) *cancelBatcher {
	ctx, cancel := context.WithCancel(context.Background())
	b := &cancelBatcher{
		store:   store,
		queries: make(chan cancelQuery),
		size:    size,
		linger:  linger,
		timeout: timeout,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go func() {
		defer close(b.done)
		b.run(ctx)
	}()
	return b
}

func (b *cancelBatcher) stop() {
	b.cancel()
	<-b.done
}

func (b *cancelBatcher) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case first := <-b.queries:
			batch := []cancelQuery{first}
			timer := time.NewTimer(b.linger)
		collect:
			for len(batch) < b.size {
				select {
				case q := <-b.queries:
					batch = append(batch, q)
				case <-timer.C:
					break collect
				case <-ctx.Done():
					break collect
				}
			}
			timer.Stop()
			b.flush(ctx, batch)
		}
	}
}

func (b *cancelBatcher) flush(ctx context.Context, batch []cancelQuery) {
	b.flushes.Add(1)
	keys := make([]sqlqueue.Key, 0, len(batch))
	seen := make(map[sqlqueue.Key]struct{}, len(batch))
	for _, q := range batch {
		if _, dup := seen[q.key]; dup {
			continue
		}
		seen[q.key] = struct{}{}
		keys = append(keys, q.key)
	}

	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	cancelled, err := b.store.CancelledKeys(ctx, keys)
	for _, q := range batch {
		q.reply <- cancelReply{cancelled: cancelled[q.key], err: err}
	}
}

// isCancelled hands the key to the batcher and waits for that batch's answer.
func (b *cancelBatcher) isCancelled(ctx context.Context, key sqlqueue.Key) (bool, error) {
	reply := make(chan cancelReply, 1)
	select {
	case b.queries <- cancelQuery{key: key, reply: reply}:
	case <-ctx.Done():
		return false, fmt.Errorf("queue cancellation check for %q: %w", key.ID, ctx.Err())
	}
	select {
	case r := <-reply:
		return r.cancelled, r.err
	case <-ctx.Done():
		return false, fmt.Errorf("await cancellation check for %q: %w", key.ID, ctx.Err())
	}
}
