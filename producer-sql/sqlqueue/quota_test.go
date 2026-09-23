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
