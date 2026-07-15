//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMiniredisClient(t *testing.T) (*goredis.Client, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{
		Addr:         s.Addr(),
		DialTimeout:  500 * time.Millisecond,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	})
	t.Cleanup(func() { rdb.Close() })
	return rdb, s
}

func TestRedisDispatchGate_KeyMissing(t *testing.T) {
	rdb, _ := newMiniredisClient(t)
	gate := redis.NewRedisDispatchGate(rdb, "nonexistent-key")

	budget := gate.Budget(context.Background())
	assert.InDelta(t, 1.0, budget, 1e-9, "missing key should default to full capacity")
}

func TestRedisDispatchGate_ValidBudget(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	require.NoError(t, s.Set("budget-key", "0.75"))

	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	budget := gate.Budget(context.Background())
	assert.InDelta(t, 0.75, budget, 1e-9)
}

func TestRedisDispatchGate_ZeroBudget(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	require.NoError(t, s.Set("budget-key", "0.0"))

	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	budget := gate.Budget(context.Background())
	assert.InDelta(t, 0.0, budget, 1e-9)
}

func TestRedisDispatchGate_ClampAboveOne(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	require.NoError(t, s.Set("budget-key", "5.0"))

	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	budget := gate.Budget(context.Background())
	assert.InDelta(t, 1.0, budget, 1e-9, "values above 1.0 should be clamped to 1.0")
}

func TestRedisDispatchGate_ClampBelowZero(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	require.NoError(t, s.Set("budget-key", "-0.5"))

	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	budget := gate.Budget(context.Background())
	assert.InDelta(t, 0.0, budget, 1e-9, "negative values should be clamped to 0.0")
}

func TestRedisDispatchGate_UnparsableValue(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	require.NoError(t, s.Set("budget-key", "not-a-number"))

	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	budget := gate.Budget(context.Background())
	assert.InDelta(t, 1.0, budget, 1e-9, "unparsable values should default to 1.0")
}

func TestRedisDispatchGate_RedisDown(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	budget := gate.Budget(ctx)
	assert.InDelta(t, 0.0, budget, 1e-9, "redis error should fail closed (0.0)")
}

func TestRedisDispatchGate_BudgetUpdated(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	require.NoError(t, s.Set("budget-key", "0.5"))
	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	require.InDelta(t, 0.5, gate.Budget(context.Background()), 1e-9)

	require.NoError(t, s.Set("budget-key", "0.9"))
	assert.InDelta(t, 0.9, gate.Budget(context.Background()), 1e-9, "budget should reflect updated Redis value")
}
