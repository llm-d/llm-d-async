package redis

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
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

// Releasing a reservation must not report an error: the release script has to
// return a value (a script that falls off its end replies Null Bulk, which
// go-redis turns into redis.Nil) and the remaining count must be accurate.
func TestQuotaStoreReleaseSlotReturnsRemaining(t *testing.T) {
	store, s := newQuotaStore(t, time.Minute)
	ctx := context.Background()
	key := "quota:user:alice"

	for i := 0; i < 2; i++ {
		_, ok, err := store.AcquireSlot(ctx, key, 5)
		require.NoError(t, err)
		require.True(t, ok, "acquire %d", i)
	}

	remaining, err := store.releaseSlot(ctx, key, 60)
	require.NoError(t, err)
	require.EqualValues(t, 1, remaining)
	require.Positive(t, s.TTL(key), "key TTL kept alive while a reservation remains")

	remaining, err = store.releaseSlot(ctx, key, 60)
	require.NoError(t, err)
	require.EqualValues(t, 0, remaining)

	// Counter expired or never existed: not an error, reported as -1.
	s.Del(key)
	remaining, err = store.releaseSlot(ctx, key, 60)
	require.NoError(t, err)
	require.EqualValues(t, -1, remaining)
}

// dropEvalReplyConn fails the reply read of the first EVAL written on it with
// io.EOF after the server has answered, as a connection reset after the
// script ran would. go-redis then retries the EVAL.
type dropEvalReplyConn struct {
	net.Conn
	armed *atomic.Bool
	drop  bool
}

func (c *dropEvalReplyConn) Write(b []byte) (int, error) {
	if bytes.Contains(bytes.ToLower(b), []byte("\r\neval\r\n")) && c.armed.CompareAndSwap(true, false) {
		c.drop = true
	}
	return c.Conn.Write(b)
}

func (c *dropEvalReplyConn) Read(b []byte) (int, error) {
	if c.drop {
		_, _ = c.Conn.Read(b) // the reply arrived, so the script ran
		_ = c.Close()
		return 0, io.EOF
	}
	return c.Conn.Read(b)
}

// forEachClientLosingAReply runs fn for a standalone and a cluster client. The
// reply to the next EVAL after armed is set is lost.
func forEachClientLosingAReply(t *testing.T, fn func(t *testing.T, store *QuotaStore, mr *miniredis.Miniredis, armed *atomic.Bool)) {
	t.Helper()
	for name, cluster := range map[string]bool{"standalone": false, "cluster": true} {
		t.Run(name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			var armed atomic.Bool
			dialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &dropEvalReplyConn{Conn: conn, armed: &armed}, nil
			}
			var client redis.UniversalClient = redis.NewClient(&redis.Options{Addr: mr.Addr(), Dialer: dialer})
			if cluster {
				client = redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{mr.Addr()}, Dialer: dialer})
			}
			t.Cleanup(func() { _ = client.Close() })
			fn(t, NewQuotaStore(client, time.Minute), mr, &armed)
		})
	}
}

func TestQuotaStoreAcquireRetryAfterLostReplyTakesOneSlot(t *testing.T) {
	forEachClientLosingAReply(t, func(t *testing.T, store *QuotaStore, mr *miniredis.Miniredis, armed *atomic.Bool) {
		armed.Store(true)
		release, ok, err := store.AcquireSlot(context.Background(), "quota:k", 5)
		require.NoError(t, err)
		require.True(t, ok)
		require.False(t, armed.Load(), "the acquire reply was dropped")

		count, err := mr.Get("quota:k")
		require.NoError(t, err)
		require.Equal(t, "1", count)

		release()
		count, err = mr.Get("quota:k")
		require.NoError(t, err)
		require.Equal(t, "0", count, "no slot is held after the release")
	})
}

func TestQuotaStoreAcquireRetryAfterLostReplyAtTheLastSlotIsAdmitted(t *testing.T) {
	forEachClientLosingAReply(t, func(t *testing.T, store *QuotaStore, mr *miniredis.Miniredis, armed *atomic.Bool) {
		armed.Store(true)
		_, ok, err := store.AcquireSlot(context.Background(), "quota:k", 1)
		require.NoError(t, err)
		require.False(t, armed.Load(), "the acquire reply was dropped")
		require.True(t, ok, "the first attempt took the slot, so the retry must report it")

		count, err := mr.Get("quota:k")
		require.NoError(t, err)
		require.Equal(t, "1", count)
	})
}

func TestQuotaStoreReleaseRetryAfterLostReplyReturnsOneSlot(t *testing.T) {
	forEachClientLosingAReply(t, func(t *testing.T, store *QuotaStore, mr *miniredis.Miniredis, armed *atomic.Bool) {
		ctx := context.Background()
		_, ok, err := store.AcquireSlot(ctx, "quota:k", 2)
		require.NoError(t, err)
		require.True(t, ok)
		_, ok, err = store.AcquireSlot(ctx, "quota:k", 2)
		require.NoError(t, err)
		require.True(t, ok)

		armed.Store(true)
		remaining, err := store.releaseSlot(ctx, "quota:k", 60)
		require.NoError(t, err)
		require.False(t, armed.Load(), "the release reply was dropped")
		require.EqualValues(t, 1, remaining)

		count, err := mr.Get("quota:k")
		require.NoError(t, err)
		require.Equal(t, "1", count, "the other holder keeps its slot")
	})
}

func TestQuotaStoreAdmitRetryAfterLostReplyAtTheLastTokenIsAdmitted(t *testing.T) {
	forEachClientLosingAReply(t, func(t *testing.T, store *QuotaStore, mr *miniredis.Miniredis, armed *atomic.Bool) {
		ctx := context.Background()
		armed.Store(true)
		ok, err := store.Admit(ctx, "rate:k", 1, time.Minute)
		require.NoError(t, err)
		require.False(t, armed.Load(), "the admit reply was dropped")
		require.True(t, ok, "the first attempt spent the token, so the retry must report it")

		members, err := mr.ZMembers("rate:k")
		require.NoError(t, err)
		require.Len(t, members, 1)

		ok, err = store.Admit(ctx, "rate:k", 1, time.Minute)
		require.NoError(t, err)
		require.False(t, ok, "a later call is still limited")
	})
}

func TestQuotaCallGuardKeySharesTheCounterSlot(t *testing.T) {
	for _, key := range []string{"quota:userid:u1", "{tenant}quota:userid:u1", "quota:{"} {
		require.True(t, sameHashSlot(key, quotaCallGuardKey(key, "token")), key)
	}
}

func TestQuotaStoreCallGuardsExpire(t *testing.T) {
	store, mr := newQuotaStore(t, 24*time.Hour)
	release, ok, err := store.AcquireSlot(context.Background(), "quota:k", 2)
	require.NoError(t, err)
	require.True(t, ok)
	release()

	guards := 0
	for _, key := range mr.Keys() {
		if key == "quota:k" {
			continue
		}
		guards++
		require.Equal(t, quotaCallGuardTTL, mr.TTL(key), key)
	}
	require.Equal(t, 2, guards)
}
