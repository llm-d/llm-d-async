package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// QuotaStore keeps quota counters in Redis. A concurrency counter expires
// after slotTTL without an acquire or release, which frees slots a crashed
// dispatcher never returned.
type QuotaStore struct {
	rdb     redis.UniversalClient
	slotTTL time.Duration
}

func NewQuotaStore(rdb redis.UniversalClient, slotTTL time.Duration) *QuotaStore {
	return &QuotaStore{rdb: rdb, slotTTL: slotTTL}
}

// quotaCallGuardTTL outlasts the retries of one script call, which finish
// within a minute under the go-redis default timeouts. It does not follow
// slotTTL or how long a slot is held, so guards number about 10 minutes of
// calls.
const quotaCallGuardTTL = 10 * time.Minute

// quotaCallGuardKey marks that the call holding token has changed the counter
// at key. go-redis retries a command whose reply was lost, so a counter script
// can run twice for one call; the guard keeps the retry from counting again.
// The hash tag keeps it in the counter's slot on a cluster client.
func quotaCallGuardKey(key, token string) string {
	return fmt.Sprintf("quota-call:%s:%s", slotKey(key, true), token)
}

func (s *QuotaStore) AcquireSlot(ctx context.Context, key string, limit int) (func(), bool, error) {
	// Use Lua script for atomic check and increment
	script := `
		if redis.call("EXISTS", KEYS[2]) == 1 then
			return 1
		end
		local current = redis.call("GET", KEYS[1])
		if current and tonumber(current) >= tonumber(ARGV[1]) then
			return 0
		end
		redis.call("SET", KEYS[2], "1", "PX", ARGV[3])
		redis.call("INCR", KEYS[1])
		-- Refresh the TTL on every acquire so the counter cannot expire while
		-- requests are still in flight (#311 sibling). The key then expires only
		-- after ARGV[2] seconds of total inactivity (crash-orphan cleanup).
		redis.call("EXPIRE", KEYS[1], ARGV[2])
		return 1
	`
	// Default to 5m when no TTL is configured.
	ttl := int(s.slotTTL.Seconds())
	if ttl <= 0 {
		ttl = 300
	}

	token, err := newClaimToken()
	if err != nil {
		return nil, false, fmt.Errorf("acquire concurrency quota: %w", err)
	}
	keys := []string{key, quotaCallGuardKey(key, token)}
	res, err := s.rdb.Eval(ctx, script, keys, limit, ttl, quotaCallGuardTTL.Milliseconds()).Int64()
	if err != nil {
		return nil, false, fmt.Errorf("acquire concurrency quota: %w", err)
	}
	if res == 0 {
		return nil, false, nil
	}

	release := func() {
		// Use a background context for release to ensure it runs even if the request context is canceled
		remaining, err := s.releaseSlot(context.Background(), key, ttl)
		switch {
		case err != nil:
			log.Log.Error(err, "Failed to release concurrency quota", "key", key)
		case remaining < 0:
			// The counter was gone before this reservation was returned: it
			// expired (window of total inactivity) or was reset externally.
			// Nothing to decrement, so the gate under-counts for a moment at
			// worst; informational, not an error.
			log.Log.V(1).Info("Concurrency quota counter missing on release", "key", key)
		}
	}
	return release, true, nil
}

// releaseSlot returns one reservation on key and reports the count
// that remains, or -1 when the counter did not exist (or was already zero).
//
// The script always returns a value. A Lua script that falls off its end
// replies with a Null Bulk, which go-redis surfaces as redis.Nil, so a
// release that completed successfully would otherwise look like an error.
func (s *QuotaStore) releaseSlot(ctx context.Context, key string, ttl int) (int64, error) {
	releaseScript := `
		local current = redis.call("GET", KEYS[1])
		if not redis.call("SET", KEYS[2], "1", "NX", "PX", ARGV[2]) then
			return tonumber(current or "0")
		end
		if not current or tonumber(current) <= 0 then
			return -1
		end
		local remaining = redis.call("DECR", KEYS[1])
		if remaining > 0 then
			-- Keep the key alive while reservations remain in flight.
			redis.call("EXPIRE", KEYS[1], ARGV[1])
		end
		return remaining
	`
	token, err := newClaimToken()
	if err != nil {
		return 0, fmt.Errorf("release concurrency quota: %w", err)
	}
	keys := []string{key, quotaCallGuardKey(key, token)}
	res, err := s.rdb.Eval(ctx, releaseScript, keys, ttl, quotaCallGuardTTL.Milliseconds()).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, fmt.Errorf("release concurrency quota: %w", err)
	}
	return res, nil
}

func (s *QuotaStore) Admit(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	// Sliding window rate limit using Sorted Set
	now := time.Now().UnixNano()
	min := now - window.Nanoseconds()

	script := `
		redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, ARGV[1])
		local count = redis.call("ZCARD", KEYS[1])
		if count >= tonumber(ARGV[2]) then
			return 0
		end
		redis.call("ZADD", KEYS[1], ARGV[3], ARGV[3])
		redis.call("EXPIRE", KEYS[1], ARGV[4])
		return 1
	`
	// TTL is window size plus some buffer (e.g., 2x window)
	ttl := int(window.Seconds()) * 2
	if ttl <= 0 {
		ttl = 3600
	}

	res, err := s.rdb.Eval(ctx, script, []string{key}, min, limit, now, ttl).Int64()
	if err != nil {
		return false, fmt.Errorf("admit rate quota: %w", err)
	}
	return res == 1, nil
}
