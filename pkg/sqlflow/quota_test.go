package sqlflow

import (
	"context"
	"database/sql"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestQuotaStore(t *testing.T, store *sqlqueue.Store, ttl, timeout time.Duration) *QuotaStore {
	t.Helper()
	q := NewQuotaStore(store, ttl, timeout, logr.Discard())
	t.Cleanup(q.Close)
	return q
}

func quotaDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", os.Getenv("TEST_POSTGRES_URL"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func liveSlots(t *testing.T, db *sql.DB, key string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`
SELECT coalesce(sum(s.used), 0) FROM async_quota_slots s
JOIN async_quota_holders h ON h.holder = s.holder
WHERE s.key = $1 AND h.expires_ms > (extract(epoch FROM clock_timestamp()) * 1000)::bigint`, key).Scan(&n))
	return n
}

func eventuallySlots(t *testing.T, db *sql.DB, key string, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return liveSlots(t, db, key) == want }, 5*time.Second, 20*time.Millisecond,
		"slots held for %s never reached %d", key, want)
}

func TestQuotaStoreCoalescesConcurrentAcquires(t *testing.T) {
	store := cancelTestStore(t)
	q := newTestQuotaStore(t, store, 30*time.Second, time.Second)
	ctx := context.Background()
	const callers = 64

	var wg sync.WaitGroup
	var granted atomic.Int64
	releases := make(chan func(), callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, ok, err := q.AcquireSlot(ctx, "k", 40)
			assert.NoError(t, err)
			if ok {
				granted.Add(1)
				releases <- release
			}
		}()
	}
	wg.Wait()
	close(releases)
	assert.EqualValues(t, 40, granted.Load(), "exactly the limit is granted")
	assert.Less(t, q.slots.statements.Load(), int64(callers), "concurrent acquires share statements")

	db := quotaDB(t)
	eventuallySlots(t, db, "k", 40)
	for release := range releases {
		release()
		release()
	}
	eventuallySlots(t, db, "k", 0)
}

// Several dispatchers share one key. held counts slots a caller holds and has
// not started to release; the database holds at least that many, so held
// passing the limit means the stores admitted too many together.
func TestQuotaStoresNeverExceedTheLimitTogether(t *testing.T) {
	store := cancelTestStore(t)
	ctx := context.Background()
	const (
		limit       = 5
		dispatchers = 3
		workers     = 8
		rounds      = 25
	)
	var held, peak, grants atomic.Int64
	var wg sync.WaitGroup
	for d := range dispatchers {
		q := newTestQuotaStore(t, store, 30*time.Second, time.Second)
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewPCG(uint64(d), uint64(w)))
				for range rounds {
					release, ok, err := q.AcquireSlot(ctx, "k", limit)
					if !assert.NoError(t, err) {
						return
					}
					if !ok {
						time.Sleep(time.Duration(rng.IntN(300)) * time.Microsecond)
						continue
					}
					now := held.Add(1)
					for p := peak.Load(); now > p && !peak.CompareAndSwap(p, now); p = peak.Load() {
					}
					grants.Add(1)
					time.Sleep(time.Duration(rng.IntN(2000)) * time.Microsecond)
					held.Add(-1)
					release()
				}
			}()
		}
	}
	wg.Wait()
	assert.LessOrEqual(t, peak.Load(), int64(limit))
	assert.Greater(t, grants.Load(), int64(2*limit), "the test must actually cycle slots")
	eventuallySlots(t, quotaDB(t), "k", 0)
}

func TestQuotaStoresAdmitExactlyTheRateLimitTogether(t *testing.T) {
	store := cancelTestStore(t)
	ctx := context.Background()
	const (
		limit       = 30
		dispatchers = 3
		workers     = 10
		rounds      = 10
	)
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range dispatchers {
		q := newTestQuotaStore(t, store, 30*time.Second, time.Second)
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range rounds {
					ok, err := q.Admit(ctx, "k", limit, time.Hour)
					if !assert.NoError(t, err) {
						return
					}
					if ok {
						admitted.Add(1)
					}
				}
			}()
		}
	}
	wg.Wait()
	assert.EqualValues(t, limit, admitted.Load())
}

func TestQuotaStoreRateLimitWindowSlides(t *testing.T) {
	store := cancelTestStore(t)
	q := newTestQuotaStore(t, store, 30*time.Second, time.Second)
	ctx := context.Background()
	const window = 500 * time.Millisecond
	for i := range 2 {
		ok, err := q.Admit(ctx, "k", 2, window)
		require.NoError(t, err)
		require.True(t, ok, "admit %d", i)
	}
	ok, err := q.Admit(ctx, "k", 2, window)
	require.NoError(t, err)
	require.False(t, ok)
	time.Sleep(window + 50*time.Millisecond)
	ok, err = q.Admit(ctx, "k", 2, window)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestQuotaStoreGivesBackASlotItsCallerAbandoned(t *testing.T) {
	store := cancelTestStore(t)
	q := newTestQuotaStore(t, store, 30*time.Second, 5*time.Second)
	db := quotaDB(t)
	ctx := context.Background()

	release, ok, err := q.AcquireSlot(ctx, "k", 2)
	require.NoError(t, err)
	require.True(t, ok)
	release()
	eventuallySlots(t, db, "k", 0)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `SELECT 1 FROM async_quota_keys WHERE key = 'k' FOR UPDATE`)
	require.NoError(t, err)

	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, _, err = q.AcquireSlot(waitCtx, "k", 2)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, tx.Rollback())

	time.Sleep(200 * time.Millisecond)
	eventuallySlots(t, db, "k", 0)
	for range 2 {
		_, ok, err := q.AcquireSlot(ctx, "k", 2)
		require.NoError(t, err)
		require.True(t, ok, "the abandoned grant was returned")
	}
}

func TestQuotaStoreRetriesReleasesUntilTheyLand(t *testing.T) {
	store := cancelTestStore(t)
	q := newTestQuotaStore(t, store, 30*time.Second, 200*time.Millisecond)
	db := quotaDB(t)
	ctx := context.Background()

	release, ok, err := q.AcquireSlot(ctx, "k", 1)
	require.NoError(t, err)
	require.True(t, ok)
	eventuallySlots(t, db, "k", 1)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `LOCK TABLE async_quota_slots IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)
	release()
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, tx.Rollback())

	eventuallySlots(t, db, "k", 0)
	_, ok, err = q.AcquireSlot(ctx, "k", 1)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestQuotaStoreRegistersAgainAfterItsLeaseLapses(t *testing.T) {
	store := cancelTestStore(t)
	q := newTestQuotaStore(t, store, 30*time.Second, time.Second)
	db := quotaDB(t)
	ctx := context.Background()

	oldRelease, ok, err := q.AcquireSlot(ctx, "k", 2)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = db.ExecContext(ctx, `UPDATE async_quota_holders SET expires_ms = 0`)
	require.NoError(t, err)
	require.Zero(t, liveSlots(t, db, "k"))

	for range 2 {
		_, ok, err := q.AcquireSlot(ctx, "k", 2)
		require.NoError(t, err)
		require.True(t, ok, "a new holder takes over without failing the request")
	}
	oldRelease()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 2, liveSlots(t, db, "k"), "releasing a lapsed holder's slot frees nothing the new holder holds")
}

func TestQuotaStoreHeartbeatKeepsSlotsPastTheTTL(t *testing.T) {
	store := cancelTestStore(t)
	const ttl = 300 * time.Millisecond
	holder := newTestQuotaStore(t, store, ttl, time.Second)
	other := newTestQuotaStore(t, store, ttl, time.Second)
	ctx := context.Background()

	_, ok, err := holder.AcquireSlot(ctx, "k", 1)
	require.NoError(t, err)
	require.True(t, ok)
	time.Sleep(4 * ttl)
	_, ok, err = other.AcquireSlot(ctx, "k", 1)
	require.NoError(t, err)
	assert.False(t, ok, "a heartbeating holder keeps its slot")
}

func TestQuotaStoreSlotsOfAStoppedDispatcherFree(t *testing.T) {
	store := cancelTestStore(t)
	const ttl = 300 * time.Millisecond
	crashed := NewQuotaStore(store, ttl, time.Second, logr.Discard())
	survivor := newTestQuotaStore(t, store, ttl, time.Second)
	ctx := context.Background()

	_, ok, err := crashed.AcquireSlot(ctx, "k", 1)
	require.NoError(t, err)
	require.True(t, ok)
	crashed.Close()

	_, ok, err = survivor.AcquireSlot(ctx, "k", 1)
	require.NoError(t, err)
	require.False(t, ok)
	require.Eventually(t, func() bool {
		_, ok, err := survivor.AcquireSlot(ctx, "k", 1)
		return err == nil && ok
	}, 5*ttl, 50*time.Millisecond, "the stopped dispatcher's slot frees once its lease lapses")
}
