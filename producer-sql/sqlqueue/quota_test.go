package sqlqueue

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func quotaSlotsHeld(t *testing.T, s *Store, key string) int {
	t.Helper()
	var n int
	require.NoError(t, s.db.QueryRowContext(context.Background(), `
SELECT coalesce(sum(s.used), 0) FROM async_quota_slots s
JOIN async_quota_holders h ON h.holder = s.holder
WHERE s.key = $1 AND h.expires_ms > `+dbNowMs, key).Scan(&n))
	return n
}

func TestQuotaSlotsGrantUpToTheLimit(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		require.NoError(t, s.RegisterQuotaHolder(ctx, "a", leaseTTL))
		require.NoError(t, s.RegisterQuotaHolder(ctx, "b", leaseTTL))

		got, err := s.AcquireQuotaSlots(ctx, "a", "k", 3, 5)
		require.NoError(t, err)
		assert.Equal(t, 3, got)
		got, err = s.AcquireQuotaSlots(ctx, "b", "k", 3, 5)
		require.NoError(t, err)
		assert.Equal(t, 2, got, "only what the limit leaves")
		got, err = s.AcquireQuotaSlots(ctx, "a", "k", 1, 5)
		require.NoError(t, err)
		assert.Zero(t, got)

		got, err = s.AcquireQuotaSlots(ctx, "a", "other", 5, 5)
		require.NoError(t, err)
		assert.Equal(t, 5, got, "keys count separately")

		require.NoError(t, s.ReleaseQuotaSlots(ctx, "a", []string{"k"}, []int{2}))
		got, err = s.AcquireQuotaSlots(ctx, "b", "k", 3, 5)
		require.NoError(t, err)
		assert.Equal(t, 2, got)
		assert.Equal(t, 5, quotaSlotsHeld(t, s, "k"))

		got, err = s.AcquireQuotaSlots(ctx, "a", "k", 3, 7)
		require.NoError(t, err)
		assert.Equal(t, 2, got, "a gate with a higher limit sees the same count")
	})
}

func TestQuotaSlotsReleaseNeverGoesNegative(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		require.NoError(t, s.RegisterQuotaHolder(ctx, "a", leaseTTL))
		require.NoError(t, s.RegisterQuotaHolder(ctx, "b", leaseTTL))
		got, err := s.AcquireQuotaSlots(ctx, "a", "k", 2, 4)
		require.NoError(t, err)
		require.Equal(t, 2, got)
		got, err = s.AcquireQuotaSlots(ctx, "b", "k", 2, 4)
		require.NoError(t, err)
		require.Equal(t, 2, got)

		require.NoError(t, s.ReleaseQuotaSlots(ctx, "a", []string{"k", "never-held"}, []int{5, 1}))
		assert.Equal(t, 2, quotaSlotsHeld(t, s, "k"), "a over-release frees only the holder's own slots")
		require.NoError(t, s.ReleaseQuotaSlots(ctx, "a", nil, nil))
		require.Error(t, s.ReleaseQuotaSlots(ctx, "a", []string{"k"}, nil))

		got, err = s.AcquireQuotaSlots(ctx, "a", "k", 4, 4)
		require.NoError(t, err)
		assert.Equal(t, 2, got)
	})
}

func TestQuotaSlotsOfALapsedHolderStopCounting(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		const ttl = 300 * time.Millisecond
		require.NoError(t, s.RegisterQuotaHolder(ctx, "dead", ttl))
		require.NoError(t, s.RegisterQuotaHolder(ctx, "live", leaseTTL))
		got, err := s.AcquireQuotaSlots(ctx, "dead", "k", 4, 4)
		require.NoError(t, err)
		require.Equal(t, 4, got)
		got, err = s.AcquireQuotaSlots(ctx, "live", "k", 1, 4)
		require.NoError(t, err)
		require.Zero(t, got)

		time.Sleep(ttl + 100*time.Millisecond)

		got, err = s.AcquireQuotaSlots(ctx, "live", "k", 4, 4)
		require.NoError(t, err)
		assert.Equal(t, 4, got, "a lapsed holder's slots are free")

		_, err = s.AcquireQuotaSlots(ctx, "dead", "k", 1, 8)
		require.ErrorIs(t, err, ErrQuotaHolderLapsed)
		ok, err := s.RenewQuotaHolder(ctx, "dead", leaseTTL)
		require.NoError(t, err)
		assert.False(t, ok, "a lapsed holder cannot come back and resurrect its slots")
		assert.Equal(t, 4, quotaSlotsHeld(t, s, "k"))

		_, err = s.AcquireQuotaSlots(ctx, "unknown", "k", 1, 8)
		require.ErrorIs(t, err, ErrQuotaHolderLapsed)
	})
}

func TestQuotaHolderRenewKeepsSlots(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		const ttl = 400 * time.Millisecond
		require.NoError(t, s.RegisterQuotaHolder(ctx, "a", ttl))
		got, err := s.AcquireQuotaSlots(ctx, "a", "k", 2, 2)
		require.NoError(t, err)
		require.Equal(t, 2, got)
		for range 4 {
			time.Sleep(ttl / 2)
			ok, err := s.RenewQuotaHolder(ctx, "a", ttl)
			require.NoError(t, err)
			require.True(t, ok)
		}
		assert.Equal(t, 2, quotaSlotsHeld(t, s, "k"), "slots outlive the original ttl while the holder renews")
	})
}

func TestDeleteQuotaHolderFreesItsSlots(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		require.NoError(t, s.RegisterQuotaHolder(ctx, "a", leaseTTL))
		require.NoError(t, s.RegisterQuotaHolder(ctx, "b", leaseTTL))
		for _, h := range []string{"a", "b"} {
			got, err := s.AcquireQuotaSlots(ctx, h, "k", 2, 4)
			require.NoError(t, err)
			require.Equal(t, 2, got)
		}

		require.NoError(t, s.DeleteQuotaHolder(ctx, "a"))
		require.NoError(t, s.DeleteQuotaHolder(ctx, "a"), "deleting twice is harmless")
		assert.Equal(t, 2, quotaSlotsHeld(t, s, "k"))
		_, err := s.AcquireQuotaSlots(ctx, "a", "k", 1, 4)
		require.ErrorIs(t, err, ErrQuotaHolderLapsed, "a deleted holder cannot take slots")
		ok, err := s.RenewQuotaHolder(ctx, "a", leaseTTL)
		require.NoError(t, err)
		assert.False(t, ok)
		got, err := s.AcquireQuotaSlots(ctx, "b", "k", 4, 4)
		require.NoError(t, err)
		assert.Equal(t, 2, got)
	})
}

func TestReapQuotaHoldersDeletesLongLapsedHolders(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		require.NoError(t, s.RegisterQuotaHolder(ctx, "dead", 50*time.Millisecond))
		require.NoError(t, s.RegisterQuotaHolder(ctx, "live", leaseTTL))
		for _, h := range []string{"dead", "live"} {
			_, err := s.AcquireQuotaSlots(ctx, h, "k", 1, 4)
			require.NoError(t, err)
		}
		time.Sleep(150 * time.Millisecond)

		require.NoError(t, s.ReapQuotaHolders(ctx, time.Hour))
		var holders, slots int
		require.NoError(t, s.db.QueryRowContext(ctx, `SELECT count(*) FROM async_quota_holders`).Scan(&holders))
		assert.Equal(t, 2, holders, "grace keeps a recently lapsed holder")

		require.NoError(t, s.ReapQuotaHolders(ctx, 0))
		require.NoError(t, s.db.QueryRowContext(ctx, `SELECT count(*) FROM async_quota_holders`).Scan(&holders))
		require.NoError(t, s.db.QueryRowContext(ctx, `SELECT count(*) FROM async_quota_slots`).Scan(&slots))
		assert.Equal(t, 1, holders)
		assert.Equal(t, 1, slots, "the dead holder's slots go with it")
		assert.Equal(t, 1, quotaSlotsHeld(t, s, "k"))
	})
}

// Holders acquire and release one key concurrently. held counts slots a caller
// has been granted and not yet begun to release, so it never exceeds what the
// database holds; if it ever passes the limit, the database admitted too many.
func TestQuotaSlotsNeverExceedTheLimitUnderContention(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		const (
			limit   = 7
			holders = 4
			workers = 6
			rounds  = 40
		)
		for h := range holders {
			require.NoError(t, s.RegisterQuotaHolder(ctx, fmt.Sprintf("h%d", h), leaseTTL))
		}
		var held, peak, grants atomic.Int64
		var wg sync.WaitGroup
		errs := make(chan error, holders*workers)
		for h := range holders {
			for w := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					holder := fmt.Sprintf("h%d", h)
					rng := rand.New(rand.NewPCG(uint64(h), uint64(w)))
					for range rounds {
						got, err := s.AcquireQuotaSlots(ctx, holder, "k", 1+rng.IntN(3), limit)
						if err != nil {
							errs <- err
							return
						}
						now := held.Add(int64(got))
						for p := peak.Load(); now > p && !peak.CompareAndSwap(p, now); p = peak.Load() {
						}
						grants.Add(int64(got))
						time.Sleep(time.Duration(rng.IntN(500)) * time.Microsecond)
						held.Add(-int64(got))
						if got > 0 {
							if err := s.ReleaseQuotaSlots(ctx, holder, []string{"k"}, []int{got}); err != nil {
								errs <- err
								return
							}
						}
					}
				}()
			}
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		assert.LessOrEqual(t, peak.Load(), int64(limit))
		assert.Greater(t, grants.Load(), int64(limit), "the test must actually cycle slots")
		assert.Zero(t, quotaSlotsHeld(t, s, "k"))
	})
}

func TestAdmitQuotaSlidingWindow(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		const window = 600 * time.Millisecond

		got, err := s.AdmitQuota(ctx, "k", 3, 5, window)
		require.NoError(t, err)
		assert.Equal(t, 3, got)
		time.Sleep(window / 2)
		got, err = s.AdmitQuota(ctx, "k", 3, 5, window)
		require.NoError(t, err)
		assert.Equal(t, 2, got, "only what the limit leaves")
		got, err = s.AdmitQuota(ctx, "k", 1, 5, window)
		require.NoError(t, err)
		assert.Zero(t, got)

		got, err = s.AdmitQuota(ctx, "other", 5, 5, window)
		require.NoError(t, err)
		assert.Equal(t, 5, got, "keys count separately")

		time.Sleep(window/2 + 50*time.Millisecond)
		got, err = s.AdmitQuota(ctx, "k", 5, 5, window)
		require.NoError(t, err)
		assert.Equal(t, 3, got, "the first batch left the window, the second has not")

		time.Sleep(window)
		got, err = s.AdmitQuota(ctx, "k", 9, 5, window)
		require.NoError(t, err)
		assert.Equal(t, 5, got)
	})
}

// A short-window gate on the same key must not delete a long-window gate's
// admissions as expired for its own window.
func TestAdmitQuotaWindowsCountSeparately(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		const long, short = time.Minute, 200 * time.Millisecond

		got, err := s.AdmitQuota(ctx, "k", 2, 2, long)
		require.NoError(t, err)
		require.Equal(t, 2, got)
		time.Sleep(short + 100*time.Millisecond)

		got, err = s.AdmitQuota(ctx, "k", 1, 2, short)
		require.NoError(t, err)
		assert.Equal(t, 1, got, "the short window counts only its own admissions")
		got, err = s.AdmitQuota(ctx, "k", 2, 2, long)
		require.NoError(t, err)
		assert.Zero(t, got, "the long window still holds its two admissions")
	})
}

func TestAdmitQuotaIsExactUnderContention(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		const (
			limit   = 50
			workers = 16
			rounds  = 20
		)
		for trial := range 5 {
			key := fmt.Sprintf("k%d", trial)
			var admitted atomic.Int64
			var wg sync.WaitGroup
			errs := make(chan error, workers)
			for w := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range rounds {
						got, err := s.AdmitQuota(ctx, key, 1+w%4, limit, time.Hour)
						if err != nil {
							errs <- err
							return
						}
						admitted.Add(int64(got))
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
			require.EqualValues(t, limit, admitted.Load(), "trial %d: demand far above the limit admits exactly the limit", trial)
		}
	})
}

func quotaRows(t *testing.T, s *Store) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, table := range []string{"async_quota_keys", "async_quota_slots", "async_quota_windows", "async_quota_admits"} {
		var n int
		require.NoError(t, s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM `+table).Scan(&n))
		out[table] = n
	}
	return out
}

func ageAdmits(t *testing.T, s *Store, key string, by time.Duration) {
	t.Helper()
	_, err := s.db.ExecContext(context.Background(), `UPDATE async_quota_admits SET at_ms = at_ms - $2 WHERE key = $1`, key, by.Milliseconds())
	require.NoError(t, err)
}

func TestReapQuotaDropsEverythingAnIdleTenantLeft(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		require.NoError(t, s.RegisterQuotaHolder(ctx, "a", leaseTTL))
		got, err := s.AcquireQuotaSlots(ctx, "a", "slots", 2, 5)
		require.NoError(t, err)
		require.Equal(t, 2, got)
		require.NoError(t, s.ReleaseQuotaSlots(ctx, "a", []string{"slots"}, []int{2}))
		got, err = s.AdmitQuota(ctx, "rate", 3, 5, time.Minute)
		require.NoError(t, err)
		require.Equal(t, 3, got)
		ageAdmits(t, s, "rate", 2*time.Minute)

		_, err = s.ReapQuota(ctx, 100)
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"async_quota_keys": 0, "async_quota_slots": 0, "async_quota_windows": 0, "async_quota_admits": 0}, quotaRows(t, s))

		got, err = s.AcquireQuotaSlots(ctx, "a", "slots", 9, 5)
		require.NoError(t, err)
		assert.Equal(t, 5, got, "a reaped key counts from zero again")
		got, err = s.AdmitQuota(ctx, "rate", 9, 5, time.Minute)
		require.NoError(t, err)
		assert.Equal(t, 5, got, "a reaped window counts from zero again")
	})
}

func TestReapQuotaKeepsWhatStillCounts(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		require.NoError(t, s.RegisterQuotaHolder(ctx, "a", leaseTTL))
		got, err := s.AcquireQuotaSlots(ctx, "a", "slots", 2, 3)
		require.NoError(t, err)
		require.Equal(t, 2, got)

		got, err = s.AdmitQuota(ctx, "rate", 2, 3, time.Minute)
		require.NoError(t, err)
		require.Equal(t, 2, got)
		ageAdmits(t, s, "rate", 2*time.Minute)
		got, err = s.AdmitQuota(ctx, "rate", 1, 3, time.Minute)
		require.NoError(t, err)
		require.Equal(t, 1, got)

		_, err = s.ReapQuota(ctx, 100)
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"async_quota_keys": 1, "async_quota_slots": 1, "async_quota_windows": 1, "async_quota_admits": 1}, quotaRows(t, s))

		got, err = s.AcquireQuotaSlots(ctx, "a", "slots", 9, 3)
		require.NoError(t, err)
		assert.Equal(t, 1, got, "held slots still count after a reap")
		got, err = s.AdmitQuota(ctx, "rate", 9, 3, time.Minute)
		require.NoError(t, err)
		assert.Equal(t, 2, got, "the live admission still counts and the reaped ones no longer do")
	})
}

func TestReapQuotaIsBounded(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		for i := range 5 {
			_, err := s.AdmitQuota(ctx, fmt.Sprintf("k%d", i), 1, 1, time.Minute)
			require.NoError(t, err)
		}
		_, err := s.db.ExecContext(ctx, `UPDATE async_quota_admits SET at_ms = at_ms - 120000`)
		require.NoError(t, err)

		n, err := s.ReapQuota(ctx, 2)
		require.NoError(t, err)
		assert.Equal(t, 4, n, "two windows drained and those two dropped")
		assert.Equal(t, 3, quotaRows(t, s)["async_quota_windows"])
	})
}

func TestQuotaStaysExactWhileTheReaperRuns(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		const limit, workers = 3, 12
		for w := range workers {
			require.NoError(t, s.RegisterQuotaHolder(ctx, fmt.Sprintf("h%d", w), leaseTTL))
		}

		var reaps atomic.Int64
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if _, err := s.ReapQuota(ctx, 100); err == nil {
					reaps.Add(1)
				}
			}
		}()

		var held atomic.Int64
		var over atomic.Int64
		errs := make(chan error, workers)
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				holder := fmt.Sprintf("h%d", w)
				for ctx.Err() == nil {
					got, err := s.AcquireQuotaSlots(ctx, holder, "slots", 1, limit)
					if err != nil {
						if ctx.Err() == nil {
							errs <- err
						}
						return
					}
					if got == 1 {
						if held.Add(1) > limit {
							over.Add(1)
						}
						held.Add(-1)
						if err := s.ReleaseQuotaSlots(context.Background(), holder, []string{"slots"}, []int{1}); err != nil {
							errs <- err
							return
						}
					}
					if _, err := s.AdmitQuota(ctx, "rate", 1+rand.IntN(3), limit, time.Millisecond); err != nil {
						if ctx.Err() == nil {
							errs <- err
						}
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		assert.Zero(t, over.Load(), "more slots were held at once than the limit")
		assert.Positive(t, reaps.Load())
	})
}

// deleteUnderWaiter locks the row that where selects in table, waits until another session
// blocks on it, then deletes the row and commits.
func deleteUnderWaiter(t *testing.T, s *Store, table, where string) {
	t.Helper()
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var pid int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
	_, err = tx.ExecContext(ctx, `SELECT 1 FROM `+table+` WHERE `+where+` FOR UPDATE`)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var waiting bool
		require.NoError(t, s.db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))`, pid).Scan(&waiting))
		return waiting
	}, 5*time.Second, 5*time.Millisecond, "nothing blocked on the locked row")
	_, err = tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE `+where)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestAdmitLocksAWindowReapedUnderIt(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		_, err := s.db.ExecContext(ctx, `INSERT INTO async_quota_windows (key, window_ms) VALUES ('rate', 60000)`)
		require.NoError(t, err)

		type reply struct {
			got int
			err error
		}
		done := make(chan reply, 1)
		go func() {
			got, err := s.AdmitQuota(ctx, "rate", 2, 5, time.Minute)
			done <- reply{got, err}
		}()
		deleteUnderWaiter(t, s, "async_quota_windows", "key = 'rate'")

		r := <-done
		require.NoError(t, r.err)
		assert.Equal(t, 2, r.got)
		var used int
		require.NoError(t, s.db.QueryRowContext(ctx, `SELECT used FROM async_quota_windows WHERE key = 'rate'`).Scan(&used))
		assert.Equal(t, 2, used, "the admission is counted in a window row that exists")
	})
}

func TestAcquireLocksAKeyReapedUnderIt(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		require.NoError(t, s.RegisterQuotaHolder(ctx, "a", leaseTTL))
		_, err := s.db.ExecContext(ctx, `INSERT INTO async_quota_keys (key) VALUES ('k')`)
		require.NoError(t, err)

		type reply struct {
			got int
			err error
		}
		done := make(chan reply, 1)
		go func() {
			got, err := s.AcquireQuotaSlots(ctx, "a", "k", 2, 5)
			done <- reply{got, err}
		}()
		deleteUnderWaiter(t, s, "async_quota_keys", "key = 'k'")

		r := <-done
		require.NoError(t, r.err)
		assert.Equal(t, 2, r.got)
		assert.Equal(t, 1, quotaRows(t, s)["async_quota_keys"], "acquire held a key row it locked, not one that was gone")
	})
}
