package sqlflow

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
)

func cancelTestStore(t *testing.T) *sqlqueue.Store {
	t.Helper()
	pgURL := os.Getenv("TEST_POSTGRES_URL")
	if pgURL == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()
	s, err := sqlqueue.Open(ctx, pgURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.TruncateForTest(ctx))
	return s
}

func TestCancelBatcherCoalescesConcurrentChecks(t *testing.T) {
	store := cancelTestStore(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Hour).Unix()

	reqs := make([]sqlqueue.Request, 0, 20)
	for i := range 20 {
		reqs = append(reqs, sqlqueue.Request{
			ID: "req-" + string(rune('a'+i)), Token: "t", Queue: "q", Deadline: deadline, Payload: "{}",
		})
	}
	require.NoError(t, store.Enqueue(ctx, reqs...))
	require.NoError(t, store.Cancel(ctx, []string{"req-c"}))

	b := newCancelBatcher(store, 256, 5*time.Millisecond)
	defer b.stop()

	const checks = 200
	got := make([]bool, checks)
	var wg sync.WaitGroup
	for i := range checks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := sqlqueue.Key{ID: reqs[i%len(reqs)].ID, Token: "t"}
			cancelled, err := b.isCancelled(ctx, key)
			require.NoError(t, err)
			got[i] = cancelled
		}(i)
	}
	wg.Wait()

	for i, cancelled := range got {
		assert.Equal(t, reqs[i%len(reqs)].ID == "req-c", cancelled, "check %d", i)
	}

	flushes := b.flushes.Load()
	assert.Less(t, flushes, int64(checks), "concurrent checks must not be one query each")
	t.Logf("%d checks in %d queries", checks, flushes)
}

func TestCancelBatcherRespectsBatchSize(t *testing.T) {
	store := cancelTestStore(t)
	ctx := context.Background()
	require.NoError(t, store.Enqueue(ctx, sqlqueue.Request{
		ID: "solo", Token: "t", Queue: "q", Deadline: time.Now().Add(time.Hour).Unix(), Payload: "{}",
	}))

	// A batch of one still answers correctly, and every caller costs a query.
	b := newCancelBatcher(store, 1, time.Millisecond)
	defer b.stop()

	for range 3 {
		cancelled, err := b.isCancelled(ctx, sqlqueue.Key{ID: "solo", Token: "t"})
		require.NoError(t, err)
		assert.False(t, cancelled)
	}
	assert.EqualValues(t, 3, b.flushes.Load(), "a size of one cannot coalesce")
}

func TestCancelBatcherUnknownRequestIsNotCancelled(t *testing.T) {
	store := cancelTestStore(t)
	b := newCancelBatcher(store, 256, 5*time.Millisecond)
	defer b.stop()

	cancelled, err := b.isCancelled(context.Background(), sqlqueue.Key{ID: "never-enqueued", Token: "t"})
	require.NoError(t, err)
	assert.False(t, cancelled)
}

func TestCancelBatcherHonoursContext(t *testing.T) {
	// No loop is running, so the send blocks and only the context releases it.
	b := &cancelBatcher{queries: make(chan cancelQuery), size: 256, linger: time.Millisecond}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := b.isCancelled(ctx, sqlqueue.Key{ID: "r", Token: "t"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
