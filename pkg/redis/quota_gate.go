package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var _ pipeline.DispatchGate = (*RedisQuotaGate)(nil)
var _ pipeline.AttributeGate = (*RedisQuotaGate)(nil)

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

// Acquire implements api.AttributeGate.
func (g *RedisQuotaGate) Acquire(ctx context.Context, attributes map[string]string) (pipeline.AcquireResult, error) {
	val, ok := attributes[g.attribute]
	if !ok {
		// If the attribute is missing, we allow it by default (or we could reject it).
		return pipeline.AcquireResult{Allowed: true, Classification: api.ClassificationNone}, nil
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
		return pipeline.AcquireResult{Allowed: true, Classification: api.ClassificationNone}, nil
	}

	if err != nil {
		return pipeline.AcquireResult{}, err
	}

	allowed := true
	if g.gatingMode == GatingModeBlocking {
		allowed = (classification == api.ClassificationReserved)
	}

	return pipeline.AcquireResult{
		Allowed:        allowed,
		Classification: classification,
		Release:        release,
	}, nil
}

func (g *RedisQuotaGate) acquireConcurrency(ctx context.Context, key string) (api.QuotaClassification, func(), error) {
	// Use Lua script for atomic check and increment
	script := `
		local current = redis.call("GET", KEYS[1])
		if current and tonumber(current) >= tonumber(ARGV[1]) then
			return 0
		end
		local new_val = redis.call("INCR", KEYS[1])
		if tonumber(new_val) == 1 then
			redis.call("EXPIRE", KEYS[1], ARGV[2])
		end
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
		releaseScript := `
			local current = redis.call("GET", KEYS[1])
			if current and tonumber(current) > 0 then
				redis.call("DECR", KEYS[1])
			end
		`
		err := g.rdb.Eval(context.Background(), releaseScript, []string{key}).Err()
		if err != nil {
			log.Log.Error(err, "Failed to release concurrency quota", "key", key)
		}
	}

	return api.ClassificationReserved, release, nil
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
