package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// QuotaStore keeps quota counters in Redis. A concurrency counter expires
// after slotTTL without an acquire or release, which frees slots a crashed
// dispatcher never returned.
type QuotaStore struct {
	rdb     *redis.Client
	slotTTL time.Duration
}

func NewQuotaStore(rdb *redis.Client, slotTTL time.Duration) *QuotaStore {
	return &QuotaStore{rdb: rdb, slotTTL: slotTTL}
}

func (s *QuotaStore) AcquireSlot(ctx context.Context, key string, limit int) (func(), bool, error) {
	// Use Lua script for atomic check and increment
	script := `
		local current = redis.call("GET", KEYS[1])
		if current and tonumber(current) >= tonumber(ARGV[1]) then
			return 0
		end
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

	res, err := s.rdb.Eval(ctx, script, []string{key}, limit, ttl).Int64()
	if err != nil {
		return nil, false, fmt.Errorf("acquire concurrency quota: %w", err)
	}
	if res == 0 {
		return nil, false, nil
	}

	release := func() {
		// Use a background context for release to ensure it runs even if the request context is canceled
		releaseScript := `
			local current = redis.call("GET", KEYS[1])
			if current and tonumber(current) > 0 then
				local remaining = redis.call("DECR", KEYS[1])
				if remaining > 0 then
					-- Keep the key alive while reservations remain in flight.
					redis.call("EXPIRE", KEYS[1], ARGV[1])
				end
			end
		`
		err := s.rdb.Eval(context.Background(), releaseScript, []string{key}, ttl).Err()
		if err != nil {
			log.Log.Error(err, "Failed to release concurrency quota", "key", key)
		}
	}
	return release, true, nil
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
