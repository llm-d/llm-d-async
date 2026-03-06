package redis

import (
	"context"
	"flag"
	"strconv"

	goredis "github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var dispatchGateBudgetKey = flag.String("redis.dispatch-gate-budget-key", "dispatch-gate-budget", "Redis key for dispatch gate budget value")

// RedisDispatchGate implements flowcontrol.DispatchGate by reading the budget
// from a Redis key. This allows external systems to dynamically control
// the dispatch rate. If the key does not exist or is invalid, it defaults
// to full capacity (1.0).
type RedisDispatchGate struct {
	rdb *goredis.Client
	key string
}

// NewRedisDispatchGate creates a new RedisDispatchGate that reads budget
// from the configured Redis key. It connects to the same Redis instance
// as the sorted set flow (--redis.ss.addr).
func NewRedisDispatchGate() *RedisDispatchGate {
	return &RedisDispatchGate{
		rdb: goredis.NewClient(&goredis.Options{Addr: *ssRedisAddr}),
		key: *dispatchGateBudgetKey,
	}
}

// Budget reads the dispatch budget from Redis. Returns a value in [0.0, 1.0].
// Defaults to 1.0 (full capacity) when the key is absent or unparseable.
func (g *RedisDispatchGate) Budget(ctx context.Context) float64 {
	val, err := g.rdb.Get(ctx, g.key).Result()
	if err == goredis.Nil {
		// Key doesn't exist; default to full capacity.
		return 1.0
	}
	if err != nil {
		// Redis error; log and fail closed.
		logger := log.FromContext(ctx)
		logger.V(logutil.DEFAULT).Error(err, "Failed to read dispatch gate budget from Redis")
		return 0.0
	}
	budget, err := strconv.ParseFloat(val, 64)
	if err != nil {
		logger := log.FromContext(ctx)
		logger.V(logutil.DEFAULT).Error(err, "Failed to parse dispatch gate budget", "value", val)
		return 1.0
	}
	if budget < 0 {
		return 0.0
	}
	if budget > 1 {
		return 1.0
	}
	return budget
}
