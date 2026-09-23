package sqlflow

import (
	"context"
	"database/sql"
	"io"
	"math/rand/v2"
	"net"
	"net/url"
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

func TestQuotaStoreRetiresTheHolderOfAFailedRelease(t *testing.T) {
	store := cancelTestStore(t)
	q := newTestQuotaStore(t, store, 600*time.Millisecond, 150*time.Millisecond)
	db := quotaDB(t)
	ctx := context.Background()

	release, ok, err := q.AcquireSlot(ctx, "k", 1)
	require.NoError(t, err)
	require.True(t, ok)
	first := holderNames(t, db)
	require.Len(t, first, 1)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `LOCK TABLE async_quota_slots IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)
	release()
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, tx.Rollback())

	eventuallySlots(t, db, "k", 0)
	assert.Eventually(t, func() bool { return len(holderNames(t, db)) == 0 }, 5*time.Second, 20*time.Millisecond,
		"the retired holder is deleted once nothing names it, which frees the slot the failed release left counted")
	_, ok, err = q.AcquireSlot(ctx, "k", 1)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.NotEqual(t, first, holderNames(t, db), "grants after the failure go to a new holder")
}

// A release commits but its reply is lost. Retrying it would subtract the
// slot twice and free one a running request still holds.
func TestQuotaStoreLostReleaseReplyCannotOverAdmit(t *testing.T) {
	proxy, store := proxiedStore(t)
	q := newTestQuotaStore(t, store, 30*time.Second, time.Second)
	db := quotaDB(t)
	ctx := context.Background()

	releaseA, ok, err := q.AcquireSlot(ctx, "k", 2)
	require.NoError(t, err)
	require.True(t, ok)
	releaseB, ok, err := q.AcquireSlot(ctx, "k", 2)
	require.NoError(t, err)
	require.True(t, ok)
	eventuallySlots(t, db, "k", 2)

	proxy.dropNextReply()
	releaseA()
	require.Eventually(t, proxy.dropped, 5*time.Second, 10*time.Millisecond)
	eventuallySlots(t, db, "k", 1)

	releaseC, ok, err := q.AcquireSlot(ctx, "k", 2)
	require.NoError(t, err)
	require.True(t, ok, "A's slot is free")
	time.Sleep(200 * time.Millisecond)
	_, ok, err = q.AcquireSlot(ctx, "k", 2)
	require.NoError(t, err)
	assert.False(t, ok, "B and C hold both slots; a retried release would have freed B's")
	assert.Equal(t, 2, liveSlots(t, db, "k"))

	releaseB()
	releaseC()
	eventuallySlots(t, db, "k", 0)
}

// An acquire commits but its reply is lost, so no caller holds the grant.
func TestQuotaStoreLostGrantReplyDoesNotLeak(t *testing.T) {
	proxy, store := proxiedStore(t)
	q := newTestQuotaStore(t, store, 30*time.Second, time.Second)
	db := quotaDB(t)
	ctx := context.Background()

	release, ok, err := q.AcquireSlot(ctx, "warm", 1)
	require.NoError(t, err)
	require.True(t, ok)
	release()
	eventuallySlots(t, db, "warm", 0)

	logSlotGrants(t, db)
	proxy.dropNextReply()
	_, _, err = q.AcquireSlot(ctx, "k", 1)
	require.Error(t, err)
	require.True(t, proxy.dropped())
	var granted int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM quota_slot_grants WHERE key = 'k'`).Scan(&granted))
	require.Equal(t, 1, granted, "the lost statement committed its grant")

	eventuallySlots(t, db, "k", 0)
	_, ok, err = q.AcquireSlot(ctx, "k", 1)
	require.NoError(t, err)
	assert.True(t, ok, "the grant nobody holds went with its retired holder")
}

func TestQuotaStoreRenewsARetiredHolderUntilItsGrantsFinish(t *testing.T) {
	store := cancelTestStore(t)
	const ttl = 300 * time.Millisecond
	q := newTestQuotaStore(t, store, ttl, 100*time.Millisecond)
	db := quotaDB(t)
	ctx := context.Background()

	release, ok, err := q.AcquireSlot(ctx, "k", 2)
	require.NoError(t, err)
	require.True(t, ok)
	retiredName := holderNames(t, db)

	_, err = db.ExecContext(ctx, `INSERT INTO async_quota_keys (key) VALUES ('other')`)
	require.NoError(t, err)
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `SELECT 1 FROM async_quota_keys WHERE key = 'other' FOR UPDATE`)
	require.NoError(t, err)
	_, _, err = q.AcquireSlot(ctx, "other", 2)
	require.Error(t, err, "the statement times out behind the lock and retires the holder")
	require.NoError(t, tx.Rollback())

	time.Sleep(4 * ttl)
	assert.Equal(t, 1, liveSlots(t, db, "k"), "a retired holder with a running grant keeps renewing")
	assert.Subset(t, holderNames(t, db), retiredName)

	release()
	assert.Eventually(t, func() bool {
		for _, n := range holderNames(t, db) {
			if n == retiredName[0] {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond, "the retired holder is deleted after its last release")
	eventuallySlots(t, db, "k", 0)
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

func holderNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT holder FROM async_quota_holders ORDER BY holder`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}
	require.NoError(t, rows.Err())
	return names
}

// replyDropper is a TCP proxy to Postgres that can discard the server's next
// reply and close the client's connection while the server finishes the
// statement, so the client sees an error for a statement that committed.
type replyDropper struct {
	armed atomic.Bool
	gone  atomic.Bool
}

func (d *replyDropper) dropNextReply() {
	d.gone.Store(false)
	d.armed.Store(true)
}

func (d *replyDropper) dropped() bool { return d.gone.Load() }

func proxiedStore(t *testing.T) (*replyDropper, *sqlqueue.Store) {
	t.Helper()
	pgURL := os.Getenv("TEST_POSTGRES_URL")
	if pgURL == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	u, err := url.Parse(pgURL)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	d := &replyDropper{}
	upstream := u.Host
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", upstream)
			if err != nil {
				_ = client.Close()
				continue
			}
			go func() { _, _ = io.Copy(server, client) }()
			go func() {
				buf := make([]byte, 64<<10)
				for {
					n, err := server.Read(buf)
					if n > 0 && d.armed.CompareAndSwap(true, false) {
						d.gone.Store(true)
						_ = client.Close()
						time.Sleep(200 * time.Millisecond)
						_ = server.Close()
						return
					}
					if n > 0 {
						if _, werr := client.Write(buf[:n]); werr != nil {
							_ = server.Close()
							return
						}
					}
					if err != nil {
						_ = client.Close()
						return
					}
				}
			}()
		}
	}()
	u.Host = ln.Addr().String()
	query := u.Query()
	query.Set("default_query_exec_mode", "exec")
	u.RawQuery = query.Encode()
	ctx := context.Background()
	store, err := sqlqueue.Open(ctx, u.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.TruncateForTest(ctx))
	return d, store
}

// logSlotGrants records every committed slot grant, which outlives the holder
// row a retired holder's delete removes.
func logSlotGrants(t *testing.T, db *sql.DB) {
	t.Helper()
	teardown := []string{
		`DROP TRIGGER IF EXISTS quota_slot_grants ON async_quota_slots`,
		`DROP FUNCTION IF EXISTS quota_slot_grants()`,
		`DROP TABLE IF EXISTS quota_slot_grants`,
	}
	for _, stmt := range append(teardown,
		`CREATE TABLE quota_slot_grants (key TEXT NOT NULL, holder TEXT NOT NULL)`,
		`CREATE FUNCTION quota_slot_grants() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO quota_slot_grants (key, holder) VALUES (NEW.key, NEW.holder);
	RETURN NEW;
END $$`,
		`CREATE TRIGGER quota_slot_grants AFTER INSERT ON async_quota_slots FOR EACH ROW EXECUTE FUNCTION quota_slot_grants()`,
	) {
		_, err := db.Exec(stmt)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, stmt := range teardown {
			_, _ = db.Exec(stmt)
		}
	})
}
