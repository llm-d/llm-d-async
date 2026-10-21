package sqlflow

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
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
			ID: "req-" + string(rune('a'+i)), Token: "t", Queue: "q", Deadline: deadline, Envelope: "{}", Payload: []byte("{}"),
		})
	}
	require.NoError(t, store.Enqueue(ctx, reqs...))
	require.NoError(t, store.Cancel(ctx, []string{"req-c"}))

	b := newCancelBatcher(store, 256, 5*time.Millisecond, time.Second)
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
		ID: "solo", Token: "t", Queue: "q", Deadline: time.Now().Add(time.Hour).Unix(), Envelope: "{}", Payload: []byte("{}"),
	}))

	// A batch of one still answers correctly, and every caller costs a query.
	b := newCancelBatcher(store, 1, time.Millisecond, time.Second)
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
	b := newCancelBatcher(store, 256, 5*time.Millisecond, time.Second)
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

func TestCancelBatcherBoundsAStalledQuery(t *testing.T) {
	store := cancelTestStore(t)
	ctx := context.Background()
	db, err := sql.Open("pgx", os.Getenv("TEST_POSTGRES_URL"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "LOCK TABLE async_requests IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)

	b := newCancelBatcher(store, 256, time.Millisecond, 200*time.Millisecond)
	defer b.stop()

	start := time.Now()
	_, err = b.isCancelled(ctx, sqlqueue.Key{ID: "blocked", Token: "t"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 5*time.Second)

	require.NoError(t, tx.Rollback())
	cancelled, err := b.isCancelled(ctx, sqlqueue.Key{ID: "blocked", Token: "t"})
	require.NoError(t, err, "the batcher recovers once the lock is gone")
	assert.False(t, cancelled)
}

func TestCancelBatcherKeepsCheckingWhileAFlushIsSlow(t *testing.T) {
	store := cancelTestStore(t)
	db := quotaDB(t)
	ctx := context.Background()
	b := newCancelBatcher(store, 256, time.Millisecond, 10*time.Second)
	t.Cleanup(b.stop)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	var pid int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
	_, err = tx.ExecContext(ctx, `LOCK TABLE async_requests IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)
	blocked := func() int {
		var n int
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))`, pid).Scan(&n))
		return n
	}

	errs := make(chan error, 6)
	check := func(id string) {
		_, err := b.isCancelled(ctx, sqlqueue.Key{ID: id, Token: "t"})
		errs <- err
	}
	go check("first")
	require.Eventually(t, func() bool { return blocked() == 1 }, 5*time.Second, 5*time.Millisecond)
	go check("second")
	require.Eventually(t, func() bool { return blocked() == 2 }, 2*time.Second, 5*time.Millisecond,
		"a check that arrived during a slow flush waited for it instead of starting its own")
	for i := range 4 {
		go check(fmt.Sprintf("more-%d", i))
		require.Eventually(t, func() bool { return blocked() == min(3+i, maxCancelFlushes) }, 2*time.Second, 5*time.Millisecond)
	}
	assert.Never(t, func() bool { return blocked() > maxCancelFlushes }, 300*time.Millisecond, 10*time.Millisecond,
		"more flushes ran at once than the cap")

	require.NoError(t, tx.Rollback())
	for range 6 {
		require.NoError(t, <-errs)
	}
}
