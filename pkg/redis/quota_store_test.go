package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newQuotaStore(t *testing.T, slotTTL time.Duration) (*QuotaStore, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewQuotaStore(rdb, slotTTL), s
}

func TestQuotaStoreSlots(t *testing.T) {
	store, _ := newQuotaStore(t, 10*time.Second)
	ctx := context.Background()

	release1, ok, err := store.AcquireSlot(ctx, "quota:userid:user1", 2)
	require.NoError(t, err)
	require.True(t, ok)
	release2, ok, err := store.AcquireSlot(ctx, "quota:userid:user1", 2)
	require.NoError(t, err)
	require.True(t, ok)

	release3, ok, err := store.AcquireSlot(ctx, "quota:userid:user1", 2)
	require.NoError(t, err)
	require.False(t, ok, "a third slot exceeds the limit")
	require.Nil(t, release3)

	_, ok, err = store.AcquireSlot(ctx, "quota:userid:user2", 2)
	require.NoError(t, err)
	require.True(t, ok, "another key has its own slots")

	release1()
	release4, ok, err := store.AcquireSlot(ctx, "quota:userid:user1", 2)
	require.NoError(t, err)
	require.True(t, ok, "a released slot is available again")
	release2()
	release4()
}

// TestQuotaStoreSlotsTTLRefresh is a regression test for the #311 sibling
// over-admission bug: in concurrency mode the counter's TTL was set only on the
// first acquire (new_val==1) and never refreshed, so under sustained load the
// key expired mid-flight, the counter reset to 0, and further requests were
// admitted beyond the limit. Every acquire must refresh the TTL so the counter
// survives as long as there is activity (and only expires after the TTL of
// total inactivity, as crash-orphan cleanup).
func TestQuotaStoreSlotsTTLRefresh(t *testing.T) {
	store, s := newQuotaStore(t, 10*time.Second)
	ctx := context.Background()

	// Acquire slot 1 (counter=1, TTL=10s); hold it.
	_, ok, err := store.AcquireSlot(ctx, "quota:userid:user1", 2)
	require.NoError(t, err)
	require.True(t, ok)

	// Advance below the TTL, then acquire slot 2 — this must refresh the TTL.
	s.FastForward(9 * time.Second)
	_, ok, err = store.AcquireSlot(ctx, "quota:userid:user1", 2)
	require.NoError(t, err)
	require.True(t, ok)

	// Advance past the ORIGINAL first-acquire TTL (18s total > 10s) but within the
	// refreshed TTL. Both slots are still held, so the counter must remain at the
	// limit and a 3rd acquire must be refused. Before the fix the key would have
	// expired at ~10s, resetting the counter and wrongly admitting this request.
	s.FastForward(9 * time.Second)
	_, ok, err = store.AcquireSlot(ctx, "quota:userid:user1", 2)
	require.NoError(t, err)
	require.False(t, ok, "counter reset mid-flight → over-admission (TTL not refreshed)")
}

func TestQuotaStoreAdmit(t *testing.T) {
	store, _ := newQuotaStore(t, 0)
	ctx := context.Background()

	for i := range 2 {
		ok, err := store.Admit(ctx, "quota:userid:user1", 2, time.Second)
		require.NoError(t, err)
		require.True(t, ok, "admit %d", i)
	}
	ok, err := store.Admit(ctx, "quota:userid:user1", 2, time.Second)
	require.NoError(t, err)
	require.False(t, ok, "a third request in the window exceeds the limit")

	time.Sleep(1100 * time.Millisecond)
	ok, err = store.Admit(ctx, "quota:userid:user1", 2, time.Second)
	require.NoError(t, err)
	require.True(t, ok, "the window has passed")
}
