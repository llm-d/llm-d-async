package sqlqueue

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const leaseTTL = 30 * time.Second

func openStore(t *testing.T) *Store {
	t.Helper()
	pgURL := os.Getenv("TEST_POSTGRES_URL")
	if pgURL == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, pgURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.TruncateForTest(ctx))
	return s
}

func withStore(t *testing.T, fn func(t *testing.T, s *Store)) {
	t.Helper()
	fn(t, openStore(t))
}

func idInPartition(t *testing.T, prefix string, p int) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		id := prefix + "-" + strconv.Itoa(i)
		if partitionOf(id) == p {
			return id
		}
	}
	t.Fatalf("no id with prefix %q hashes to partition %d", prefix, p)
	return ""
}

func req(id string, deadline int64) Request {
	return Request{ID: id, Token: "t", Queue: "q", Deadline: deadline, Payload: `{"id":"` + id + `"}`}
}

func ownAll(t *testing.T, s *Store, owner string, now time.Time) []Lease {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, s.EnsurePartitions(ctx, "q"))
	leases, err := s.AcquirePartitions(ctx, "q", owner, now, leaseTTL, Partitions)
	require.NoError(t, err)
	require.Len(t, leases, Partitions)
	return leases
}

func dispatchIDs(t *testing.T, s *Store, owner string, now time.Time, limit int) []string {
	t.Helper()
	rows, err := s.Dispatch(context.Background(), "q", owner, now, limit)
	require.NoError(t, err)
	return ids(rows)
}

func ids(rs []Request) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.ID)
	}
	return out
}

func completion(id, route string) Completion {
	return Completion{Key: Key{ID: id, Token: "t"}, Route: route, Payload: `{"id":"` + id + `"}`}
}

func TestValidateDSN(t *testing.T) {
	for _, dsn := range []string{"postgres://u:p@h/db", "postgresql://u:p@h/db"} {
		require.NoError(t, validateDSN(dsn), dsn)
	}
	for _, dsn := range []string{"sqlite:///tmp/x.db", "file:/tmp/x.db?mode=ro"} {
		require.Error(t, validateDSN(dsn), dsn)
	}
	err := validateDSN("mysql://u:secret@h/db")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secret")
}

func TestMigrateIsIdempotent(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		require.NoError(t, s.Migrate(context.Background()))
		require.NoError(t, s.Migrate(context.Background()))
	})
}

func TestPartitionOfIsStableAndCoversEveryPartition(t *testing.T) {
	assert.Equal(t, partitionOf("request-42"), partitionOf("request-42"))
	counts := make([]int, Partitions)
	for i := 0; i < 64000; i++ {
		p := partitionOf(fmt.Sprintf("batch-%d-line-%d", i/100, i))
		require.GreaterOrEqual(t, p, 0)
		require.Less(t, p, Partitions)
		counts[p]++
	}
	for p, n := range counts {
		assert.InDelta(t, 1000, n, 200, "partition %d is badly skewed", p)
	}
}

func TestEnqueueIsAtomicAndStoresPartition(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now().Unix()

		batch := make([]Request, 0, 2*enqueueChunk+5)
		for i := 0; i < cap(batch); i++ {
			batch = append(batch, req(fmt.Sprintf("bulk-%d", i), now+100))
		}
		require.NoError(t, s.Enqueue(ctx, batch...))
		var n int
		require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM async_requests WHERE queue = 'q'`).Scan(&n))
		assert.Equal(t, len(batch), n, "every chunk lands")

		rows, err := s.db.QueryContext(ctx, `SELECT id, partition_id FROM async_requests`)
		require.NoError(t, err)
		for rows.Next() {
			var id string
			var p int
			require.NoError(t, rows.Scan(&id, &p))
			require.Equal(t, partitionOf(id), p, id)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		dup := []Request{req("fresh-1", now+100), req("bulk-3", now+100), req("fresh-2", now+100)}
		require.Error(t, s.Enqueue(ctx, dup...), "a duplicate (id, token) fails the batch")
		require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM async_requests WHERE id LIKE 'fresh-%'`).Scan(&n))
		assert.Zero(t, n, "no row of a failed batch is kept")

		require.NoError(t, s.Enqueue(ctx), "empty batch is a no-op")
	})
}

func TestDispatchOrdersByDeadlineAndHidesInFlight(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)
		dl := now.Unix()
		require.NoError(t, s.Enqueue(ctx, req("late", dl+300), req("early", dl+10), req("mid", dl+100)))
		require.NoError(t, s.Enqueue(ctx, Request{ID: "other-queue", Token: "t", Queue: "q2", Deadline: dl, Payload: "{}"}))
		require.NoError(t, s.Cancel(ctx, []string{"mid"}))

		rows, err := s.Dispatch(ctx, "q", "a", now, 2)
		require.NoError(t, err)
		require.Equal(t, []string{"early", "mid"}, ids(rows))
		assert.False(t, rows[0].Cancelled)
		assert.True(t, rows[1].Cancelled, "cancel flag rides along with the dispatch")
		assert.Equal(t, partitionOf("mid"), rows[1].Partition)
		assert.Equal(t, `{"id":"mid"}`, rows[1].Payload)

		assert.Equal(t, []string{"late"}, dispatchIDs(t, s, "a", now, 10), "in-flight requests are not dispatched again")
		assert.Empty(t, dispatchIDs(t, s, "a", now, 10))
		assert.Empty(t, dispatchIDs(t, s, "nobody", now, 10), "a non-owner dispatches nothing")
	})
}

func TestDispatchIsConfinedToOwnedActivePartitions(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		require.NoError(t, s.EnsurePartitions(ctx, "q"))
		aLeases, err := s.AcquirePartitions(ctx, "q", "a", now, leaseTTL, Partitions/2)
		require.NoError(t, err)
		require.Len(t, aLeases, Partitions/2)
		bLeases, err := s.AcquirePartitions(ctx, "q", "b", now, leaseTTL, Partitions)
		require.NoError(t, err)
		require.Len(t, bLeases, Partitions/2, "b only gets what a left")
		ofA := map[int]bool{}
		for _, l := range aLeases {
			ofA[l.Partition] = true
		}

		var batch []Request
		for i := 0; i < 400; i++ {
			batch = append(batch, req(fmt.Sprintf("r%d", i), now.Unix()+int64(i)))
		}
		require.NoError(t, s.Enqueue(ctx, batch...))

		gotA := dispatchIDs(t, s, "a", now, 1000)
		gotB := dispatchIDs(t, s, "b", now, 1000)
		assert.Len(t, append(gotA, gotB...), 400, "the two owners cover every request")
		for _, id := range gotA {
			assert.True(t, ofA[partitionOf(id)], "a dispatched %s outside its partitions", id)
		}
		for _, id := range gotB {
			assert.False(t, ofA[partitionOf(id)], "b dispatched %s outside its partitions", id)
		}

		p := aLeases[0].Partition
		drainMe := idInPartition(t, "drain", p)
		require.NoError(t, s.Enqueue(ctx, req(drainMe, now.Unix())))
		require.NoError(t, s.SetDraining(ctx, "q", "a", []int{p}, true))
		assert.Empty(t, dispatchIDs(t, s, "a", now, 10), "a draining partition dispatches nothing")
		require.NoError(t, s.SetDraining(ctx, "q", "b", []int{p}, false))
		assert.Empty(t, dispatchIDs(t, s, "a", now, 10), "only the owner can undrain")
		require.NoError(t, s.SetDraining(ctx, "q", "a", []int{p}, false))
		assert.Equal(t, []string{drainMe}, dispatchIDs(t, s, "a", now, 10))
	})
}

func TestDispatchKeepsSubmissionOrderForEqualDeadlines(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)
		var batch []Request
		var want []string
		for i := 0; i < 50; i++ {
			id := fmt.Sprintf("same-%02d", 49-i)
			batch = append(batch, req(id, now.Unix()+60))
			want = append(want, id)
		}
		require.NoError(t, s.Enqueue(ctx, batch...))
		require.NoError(t, s.Enqueue(ctx, req("later-batch", now.Unix()+60)))
		assert.Equal(t, append(want, "later-batch"), dispatchIDs(t, s, "a", now, 100))
	})
}

func TestInFlightListsCurrentEpochStampsOfOwnedPartitions(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)
		require.NoError(t, s.Enqueue(ctx, req("f1", now.Unix()+60), req("f2", now.Unix()+60), req("pending", now.Unix()+600)))
		rows, err := s.Dispatch(ctx, "q", "a", now, 2)
		require.NoError(t, err)
		got, err := s.InFlight(ctx, "q", "a")
		require.NoError(t, err)
		assert.ElementsMatch(t, []Stamp{rows[0].Stamp(), rows[1].Stamp()}, got)
		got, err = s.InFlight(ctx, "q", "b")
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestConcurrentOpenCreatesSchemaOnce(t *testing.T) {
	pgURL := os.Getenv("TEST_POSTGRES_URL")
	if pgURL == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()
	admin, err := Open(ctx, pgURL)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()
	schema := fmt.Sprintf("migrate_%d", time.Now().UnixNano())
	_, err = admin.db.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	defer func() { _, _ = admin.db.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE") }()

	sep := "?"
	if strings.Contains(pgURL, "?") {
		sep = "&"
	}
	dsn := pgURL + sep + "search_path=" + schema
	const openers = 8
	errs := make(chan error, openers)
	var wg sync.WaitGroup
	for range openers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := Open(ctx, dsn)
			if err == nil {
				err = st.Close()
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var tables int
	require.NoError(t, admin.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1`, schema).Scan(&tables))
	assert.Equal(t, 4, tables)
}

func TestResetStaleIsScopedAndSkipsLockedRows(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)
		inOne, inTwo := idInPartition(t, "one", 1), idInPartition(t, "two", 2)
		require.NoError(t, s.Enqueue(ctx, req(inOne, now.Unix()+60), req(inTwo, now.Unix()+60)))
		require.Len(t, dispatchIDs(t, s, "a", now, 10), 2)
		require.NoError(t, s.ReleasePartitions(ctx, "q", "a", allPartitionIDs()))
		_, err := s.AcquirePartitions(ctx, "q", "a", now, leaseTTL, Partitions)
		require.NoError(t, err)

		require.NoError(t, s.ResetStale(ctx, "q", "a", []int{}))
		assert.Empty(t, dispatchIDs(t, s, "a", now, 10), "an empty partition list resets nothing")
		require.NoError(t, s.ResetStale(ctx, "q", "a", []int{1}))
		assert.Equal(t, []string{inOne}, dispatchIDs(t, s, "a", now, 10), "only the listed partition is reset")

		tx, err := s.db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		_, err = tx.ExecContext(ctx, `SELECT 1 FROM async_requests WHERE id = $1 FOR UPDATE`, inTwo)
		require.NoError(t, err)
		quick, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		require.NoError(t, s.ResetStale(quick, "q", "a", nil), "a locked row does not block the reset")
		require.NoError(t, tx.Rollback())
		assert.Empty(t, dispatchIDs(t, s, "a", now, 10), "the locked row was skipped")
		require.NoError(t, s.ResetStale(ctx, "q", "a", nil))
		assert.Equal(t, []string{inTwo}, dispatchIDs(t, s, "a", now, 10))
	})
}

func allPartitionIDs() []int {
	out := make([]int, Partitions)
	for i := range out {
		out[i] = i
	}
	return out
}

func TestAcquireAfterLapseRedeliversAndFencesOldOwner(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		for _, l := range ownAll(t, s, "a", now) {
			assert.EqualValues(t, 1, l.Epoch)
		}
		require.NoError(t, s.Enqueue(ctx, req("x", now.Unix()+3600)))
		require.Equal(t, []string{"x"}, dispatchIDs(t, s, "a", now, 10))

		live, err := s.AcquirePartitions(ctx, "q", "b", now.Add(leaseTTL-time.Second), leaseTTL, Partitions)
		require.NoError(t, err)
		assert.Empty(t, live, "live leases are not taken")

		later := now.Add(leaseTTL + time.Second)
		taken, err := s.AcquirePartitions(ctx, "q", "b", later, leaseTTL, Partitions)
		require.NoError(t, err)
		require.Len(t, taken, Partitions)
		for _, l := range taken {
			assert.EqualValues(t, 2, l.Epoch, "takeover bumps the epoch")
		}
		assert.Empty(t, dispatchIDs(t, s, "b", later, 10), "a stamp from the old epoch hides the request until reset")
		require.NoError(t, s.ResetStale(ctx, "q", "a", nil))
		assert.Empty(t, dispatchIDs(t, s, "b", later, 10), "only the owner resets stale stamps")
		require.NoError(t, s.ResetStale(ctx, "q", "b", nil))
		assert.Equal(t, []string{"x"}, dispatchIDs(t, s, "b", later, 10), "the new owner redelivers what a left in flight")
		require.NoError(t, s.ResetStale(ctx, "q", "b", nil))
		assert.Empty(t, dispatchIDs(t, s, "b", later, 10), "a stamp from the current epoch is not stale")

		_, err = s.db.ExecContext(ctx, `UPDATE async_requests SET dispatch_epoch = 1 WHERE id = 'x'`)
		require.NoError(t, err)
		assert.Empty(t, dispatchIDs(t, s, "b", later, 10))
		require.NoError(t, s.ResetStale(ctx, "q", "b", nil))
		assert.Equal(t, []string{"x"}, dispatchIDs(t, s, "b", later, 10))
		assert.Empty(t, dispatchIDs(t, s, "a", later, 10))

		acked, err := s.Ack(ctx, "a", []Completion{completion("x", "route")})
		require.NoError(t, err)
		assert.Equal(t, []bool{false}, acked, "old owner's ack is fenced")
		current := Stamp{Key: Key{ID: "x", Token: "t"}, Epoch: 2}
		ok, err := s.Retry(ctx, "a", current, 0, "{}")
		require.NoError(t, err)
		assert.False(t, ok, "old owner's retry is fenced")
		require.NoError(t, s.Undispatch(ctx, "a", []Stamp{current}))
		assert.Empty(t, dispatchIDs(t, s, "b", later, 10), "old owner's undispatch is fenced")

		acked, err = s.Ack(ctx, "b", []Completion{completion("x", "route")})
		require.NoError(t, err)
		assert.Equal(t, []bool{true}, acked)
		res, err := s.PopResults(ctx, "route", later.Unix(), 10)
		require.NoError(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, "x", res[0].ID)
	})
}

func TestHeartbeatRenewsLeasesAndTracksMembers(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)

		leases, members, err := s.Heartbeat(ctx, "q", "a", now.Add(leaseTTL/2), leaseTTL, true)
		require.NoError(t, err)
		assert.Len(t, leases, Partitions)
		assert.Equal(t, 1, members)
		_, members, err = s.Heartbeat(ctx, "q", "b", now.Add(leaseTTL/2), leaseTTL, true)
		require.NoError(t, err)
		assert.Equal(t, 2, members)
		_, members, err = s.Heartbeat(ctx, "q2", "c", now, leaseTTL, true)
		require.NoError(t, err)
		assert.Equal(t, 1, members, "membership is per queue")

		taken, err := s.AcquirePartitions(ctx, "q", "b", now.Add(leaseTTL+time.Second), leaseTTL, Partitions)
		require.NoError(t, err)
		assert.Empty(t, taken, "renewed leases outlive the original expiry")

		leases, members, err = s.Heartbeat(ctx, "q", "b", now.Add(2*leaseTTL), leaseTTL, true)
		require.NoError(t, err)
		assert.Empty(t, leases)
		assert.Equal(t, 1, members, "a's lapsed membership is dropped")

		leases, _, err = s.Heartbeat(ctx, "q", "a", now.Add(2*leaseTTL), leaseTTL, true)
		require.NoError(t, err)
		assert.Len(t, leases, Partitions, "a lapsed lease nobody took is still renewable")

		leases, members, err = s.Heartbeat(ctx, "q", "a", now.Add(2*leaseTTL), leaseTTL, false)
		require.NoError(t, err)
		assert.Len(t, leases, Partitions, "a withdrawing owner still renews what it holds")
		assert.Equal(t, 1, members, "a withdrawing owner no longer counts as a member")
		_, members, err = s.Heartbeat(ctx, "q", "a", now.Add(2*leaseTTL), leaseTTL, true)
		require.NoError(t, err)
		assert.Equal(t, 2, members)

		require.NoError(t, s.Leave(ctx, "q", "a"))
		_, members, err = s.Heartbeat(ctx, "q", "b", now.Add(2*leaseTTL), leaseTTL, true)
		require.NoError(t, err)
		assert.Equal(t, 1, members)
		taken, err = s.AcquirePartitions(ctx, "q", "b", now.Add(2*leaseTTL), leaseTTL, Partitions)
		require.NoError(t, err)
		assert.Len(t, taken, Partitions, "leaving releases every partition immediately")
	})
}

func TestReleasePartitionsIsFencedAndImmediate(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)
		require.NoError(t, s.ReleasePartitions(ctx, "q", "b", []int{0, 1}))
		taken, err := s.AcquirePartitions(ctx, "q", "b", now, leaseTTL, Partitions)
		require.NoError(t, err)
		assert.Empty(t, taken, "only the owner can release")

		require.NoError(t, s.ReleasePartitions(ctx, "q", "a", []int{3, 5}))
		taken, err = s.AcquirePartitions(ctx, "q", "b", now, leaseTTL, Partitions)
		require.NoError(t, err)
		got := []int{}
		for _, l := range taken {
			got = append(got, l.Partition)
			assert.EqualValues(t, 2, l.Epoch)
		}
		sort.Ints(got)
		assert.Equal(t, []int{3, 5}, got)
	})
}

func TestAckIsBatchedFencedAndDeduplicated(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)
		require.NoError(t, s.Enqueue(ctx, req("a1", now.Unix()+100), req("a2", now.Unix()+100)))
		require.Len(t, dispatchIDs(t, s, "a", now, 10), 2)

		acked, err := s.Ack(ctx, "a", []Completion{
			completion("a1", "r1"),
			completion("a1", "r1"),
			{Key: Key{ID: "a2", Token: "t"}, Route: "r2", Payload: "two", ExpiresAt: now.Unix() + 60},
			completion("unknown", "r1"),
		})
		require.NoError(t, err)
		assert.Equal(t, []bool{true, false, true, false}, acked)

		acked, err = s.Ack(ctx, "a", []Completion{completion("a1", "r1")})
		require.NoError(t, err)
		assert.Equal(t, []bool{false}, acked, "a second ack of the same request writes nothing")

		depth, err := s.ResultDepth(ctx, "r1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, depth)
		res, err := s.PopResults(ctx, "r2", now.Unix(), 10)
		require.NoError(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, Result{Seq: res[0].Seq, Route: "r2", ID: "a2", Token: "t", Payload: "two"}, res[0])
		has, err := s.HasRequests(ctx, "q")
		require.NoError(t, err)
		assert.False(t, has, "acked requests are gone")

		acked, err = s.Ack(ctx, "a", nil)
		require.NoError(t, err)
		assert.Empty(t, acked)
	})
}

func TestUndispatchReturnsRequestsToPending(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)
		require.NoError(t, s.Enqueue(ctx, req("u1", now.Unix()+20), req("u2", now.Unix()+10)))
		rows, err := s.Dispatch(ctx, "q", "a", now, 10)
		require.NoError(t, err)
		require.Len(t, rows, 2)
		stale := rows[0].Stamp()
		stale.Epoch--
		require.NoError(t, s.Undispatch(ctx, "a", []Stamp{stale}))
		assert.Empty(t, dispatchIDs(t, s, "a", now, 10), "an undispatch naming an older dispatch is fenced")
		require.NoError(t, s.Undispatch(ctx, "a", []Stamp{rows[0].Stamp(), rows[1].Stamp(), rows[0].Stamp()}))
		assert.Equal(t, []string{"u2", "u1"}, dispatchIDs(t, s, "a", now, 10), "undispatched requests keep their deadline order")
		require.NoError(t, s.Undispatch(ctx, "a", nil))
	})
}

func TestRetryParksWithUpdatedPayload(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)
		require.NoError(t, s.Enqueue(ctx, req("mid", now.Unix()+100), req("late", now.Unix()+300)))
		dispatched, err := s.Dispatch(ctx, "q", "a", now, 10)
		require.NoError(t, err)
		require.Equal(t, []string{"mid", "late"}, ids(dispatched))

		stale := dispatched[0].Stamp()
		stale.Epoch--
		ok, err := s.Retry(ctx, "a", stale, now.Unix()+5, `{"stale":true}`)
		require.NoError(t, err)
		assert.False(t, ok, "a retry naming an older dispatch is fenced")
		ok, err = s.Retry(ctx, "a", dispatched[0].Stamp(), now.Unix()+5, `{"retried":true}`)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = s.Retry(ctx, "a", Stamp{Key: Key{ID: "missing", Token: "t"}, Epoch: 1}, now.Unix()+5, `{}`)
		require.NoError(t, err)
		assert.False(t, ok)

		assert.Empty(t, dispatchIDs(t, s, "a", now, 10), "a parked retry is hidden until due")
		rows, err := s.Dispatch(ctx, "q", "a", now.Add(5*time.Second), 10)
		require.NoError(t, err)
		require.Equal(t, []string{"mid"}, ids(rows))
		assert.Equal(t, `{"retried":true}`, rows[0].Payload)
	})
}

func TestCancelFlagsLiveGenerationsOnly(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now().Unix()
		require.NoError(t, s.Enqueue(ctx,
			Request{ID: "r", Token: "gen1", Queue: "q", Deadline: now + 100, Payload: "{}"},
			Request{ID: "r", Token: "gen2", Queue: "q", Deadline: now + 100, Payload: "{}"}))
		require.NoError(t, s.Cancel(ctx, []string{"r", "unknown", ""}))
		for _, gen := range []string{"gen1", "gen2"} {
			c, err := s.IsCancelled(ctx, Key{ID: "r", Token: gen})
			require.NoError(t, err)
			assert.True(t, c, gen)
		}
		c, err := s.IsCancelled(ctx, Key{ID: "r", Token: "gen3"})
		require.NoError(t, err)
		assert.False(t, c, "unknown generation is not cancelled")
		require.NoError(t, s.Enqueue(ctx, Request{ID: "r", Token: "gen3", Queue: "q", Deadline: now + 100, Payload: "{}"}))
		c, err = s.IsCancelled(ctx, Key{ID: "r", Token: "gen3"})
		require.NoError(t, err)
		assert.False(t, c, "a resubmission after cancel starts clean")
		require.NoError(t, s.Cancel(ctx, []string{""}))
	})
}

func TestExpiredResultsAreDropped(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)
		require.NoError(t, s.Enqueue(ctx, req("a", now.Unix()+100)))
		require.Len(t, dispatchIDs(t, s, "a", now, 10), 1)
		acked, err := s.Ack(ctx, "a", []Completion{{Key: Key{ID: "a", Token: "t"}, Route: "route", Payload: "{}", ExpiresAt: now.Unix() + 10}})
		require.NoError(t, err)
		require.Equal(t, []bool{true}, acked)
		res, err := s.PopResults(ctx, "route", now.Unix()+11, 10)
		require.NoError(t, err)
		assert.Empty(t, res)
		depth, err := s.ResultDepth(ctx, "route")
		require.NoError(t, err)
		assert.Zero(t, depth, "expired results are deleted, not just skipped")
	})
}

func TestBacklogCountsPendingAndInFlight(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)
		dl := now.Unix()
		require.NoError(t, s.Enqueue(ctx, req("expired", dl-5), req("soon", dl+30), req("later", dl+3000)))
		require.Len(t, dispatchIDs(t, s, "a", now, 1), 1)
		depth, expiring, err := s.Backlog(ctx, "q", dl, []int64{0, 60, 3600})
		require.NoError(t, err)
		assert.EqualValues(t, 3, depth)
		assert.Equal(t, []int64{1, 2, 3}, expiring)
		has, err := s.HasRequests(ctx, "q")
		require.NoError(t, err)
		assert.True(t, has)
		has, err = s.HasRequests(ctx, "empty")
		require.NoError(t, err)
		assert.False(t, has)
	})
}

func TestPopResultsDrainsInOrder(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		ownAll(t, s, "a", now)
		for _, id := range []string{"a", "b", "c"} {
			require.NoError(t, s.Enqueue(ctx, req(id, now.Unix()+100)))
			require.Len(t, dispatchIDs(t, s, "a", now, 10), 1)
			acked, err := s.Ack(ctx, "a", []Completion{completion(id, "route")})
			require.NoError(t, err)
			require.Equal(t, []bool{true}, acked)
		}
		got, err := s.PopResults(ctx, "route", now.Unix(), 2)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "a", got[0].ID)
		assert.Equal(t, "b", got[1].ID)
		got, err = s.PopResults(ctx, "route", now.Unix(), 2)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "c", got[0].ID)
		got, err = s.PopResults(ctx, "route", now.Unix(), 2)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestQueueTablesVacuumOnFixedThresholds(t *testing.T) {
	s := openStore(t)
	for _, table := range []string{"async_requests", "async_results"} {
		var opts string
		require.NoError(t, s.db.QueryRowContext(context.Background(),
			`SELECT array_to_string(reloptions, ',') FROM pg_class WHERE relname = $1`, table).Scan(&opts))
		for _, want := range []string{"autovacuum_vacuum_scale_factor=0", "autovacuum_vacuum_threshold=10000", "autovacuum_vacuum_cost_delay=0"} {
			assert.Contains(t, opts, want, table)
		}
	}
}
