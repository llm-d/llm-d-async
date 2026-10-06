package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var _ pipeline.Gate = (*RedisQuotaGate)(nil)

type QuotaMode string

const (
	QuotaModeRateLimit   QuotaMode = "rate-limit"
	QuotaModeConcurrency QuotaMode = "concurrency"
)

type GatingMode string

const (
	GatingModeBlocking    GatingMode = "blocking"
	GatingModeClassifying GatingMode = "classifying"
)

type RedisQuotaGate struct {
	rdb        *redis.Client
	attribute  string
	mode       QuotaMode
	gatingMode GatingMode
	limit      int
	window     time.Duration
	prefix     string
}

func NewRedisQuotaGate(client *redis.Client, attribute string, mode QuotaMode, limit int, window time.Duration, prefix string) *RedisQuotaGate {
	return &RedisQuotaGate{
		rdb:        client,
		attribute:  attribute,
		mode:       mode,
		gatingMode: GatingModeBlocking,
		limit:      limit,
		window:     window,
		prefix:     prefix,
	}
}

func (g *RedisQuotaGate) WithGatingMode(mode GatingMode) *RedisQuotaGate {
	g.gatingMode = mode
	return g
}

// Budget implements api.DispatchGate. For quota gates, we return 1.0 (open)
// because the actual gating happens at the message level via Acquire.
func (g *RedisQuotaGate) Budget(ctx context.Context) float64 {
	return 1.0
}

// Apply implements pipeline.Gate.
func (g *RedisQuotaGate) Apply(ctx context.Context, msg *api.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	val, ok := msg.PublicRequest.ReqMetadata()[g.attribute]
	if !ok {
		// If the attribute is missing, we allow it by default.
		return pipeline.Continue(), nil
	}

	key := fmt.Sprintf("%s%s:%s", g.prefix, g.attribute, val)

	var classification api.QuotaClassification
	var release func()
	var err error

	switch g.mode {
	case QuotaModeConcurrency:
		classification, release, err = g.acquireConcurrency(ctx, key)
	case QuotaModeRateLimit:
		classification, release, err = g.acquireRateLimit(ctx, key)
	default:
		return pipeline.Continue(), nil
	}

	if err != nil {
		return pipeline.Verdict{}, err
	}

	if g.gatingMode == GatingModeBlocking {
		if classification != api.ClassificationReserved {
			msg.SetClassification(classification)
			return pipeline.Refuse(), nil
		}
	}

	msg.SetClassification(classification)
	if release != nil && releases != nil {
		*releases = append(*releases, release)
	}

	return pipeline.Continue(), nil
}

func (g *RedisQuotaGate) acquireConcurrency(ctx context.Context, key string) (api.QuotaClassification, func(), error) {
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
	// TTL is window size, or a default 5m if window is 0
	ttl := int(g.window.Seconds())
	if ttl <= 0 {
		ttl = 300
	}

	res, err := g.rdb.Eval(ctx, script, []string{key}, g.limit, ttl).Result()
	if err != nil {
		return api.ClassificationNone, nil, err
	}

	if res.(int64) == 0 {
		return api.ClassificationOverflow, nil, nil
	}

	release := func() {
		// Use a background context for release to ensure it runs even if the request context is canceled
		remaining, err := g.releaseConcurrency(context.Background(), key, ttl)
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

	return api.ClassificationReserved, release, nil
}

// releaseConcurrency returns one reservation on key and reports the count
// that remains, or -1 when the counter did not exist (or was already zero).
//
// The script always returns a value. A Lua script that falls off its end
// replies with a Null Bulk, which go-redis surfaces as redis.Nil, so a
// release that completed successfully would otherwise look like an error.
func (g *RedisQuotaGate) releaseConcurrency(ctx context.Context, key string, ttl int) (int64, error) {
	releaseScript := `
		local current = redis.call("GET", KEYS[1])
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
	res, err := g.rdb.Eval(ctx, releaseScript, []string{key}, ttl).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, err
	}
	return res, nil
}

func (g *RedisQuotaGate) acquireRateLimit(ctx context.Context, key string) (api.QuotaClassification, func(), error) {
	// Sliding window rate limit using Sorted Set
	now := time.Now().UnixNano()
	windowNano := g.window.Nanoseconds()
	min := now - windowNano

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
	ttl := int(g.window.Seconds()) * 2
	if ttl <= 0 {
		ttl = 3600
	}

	res, err := g.rdb.Eval(ctx, script, []string{key}, min, g.limit, now, ttl).Result()
	if err != nil {
		return api.ClassificationNone, nil, err
	}

	if res.(int64) == 0 {
		return api.ClassificationOverflow, nil, nil
	}

	return api.ClassificationReserved, func() {}, nil
}
