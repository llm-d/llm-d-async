//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMiniredisClient(t *testing.T) (*goredis.Client, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return rdb, s
}

func TestRedisDispatchGate_KeyMissing(t *testing.T) {
	rdb, _ := newMiniredisClient(t)
	gate := redis.NewRedisDispatchGate(rdb, "nonexistent-key")

	budget := gate.Budget(context.Background())
	assert.Equal(t, 1.0, budget, "missing key should default to full capacity")
}

func TestRedisDispatchGate_ValidBudget(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	s.Set("budget-key", "0.75")

	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	budget := gate.Budget(context.Background())
	assert.Equal(t, 0.75, budget)
}

func TestRedisDispatchGate_ZeroBudget(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	s.Set("budget-key", "0.0")

	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	budget := gate.Budget(context.Background())
	assert.Equal(t, 0.0, budget)
}

func TestRedisDispatchGate_ClampAboveOne(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	s.Set("budget-key", "5.0")

	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	budget := gate.Budget(context.Background())
	assert.Equal(t, 1.0, budget, "values above 1.0 should be clamped to 1.0")
}

func TestRedisDispatchGate_ClampBelowZero(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	s.Set("budget-key", "-0.5")

	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	budget := gate.Budget(context.Background())
	assert.Equal(t, 0.0, budget, "negative values should be clamped to 0.0")
}

func TestRedisDispatchGate_UnparsableValue(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	s.Set("budget-key", "not-a-number")

	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	budget := gate.Budget(context.Background())
	assert.Equal(t, 1.0, budget, "unparsable values should default to 1.0")
}

func TestRedisDispatchGate_RedisDown(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	s.Close()

	budget := gate.Budget(context.Background())
	assert.Equal(t, 0.0, budget, "redis error should fail closed (0.0)")
}

func TestRedisDispatchGate_BudgetUpdated(t *testing.T) {
	rdb, s := newMiniredisClient(t)
	s.Set("budget-key", "0.5")
	gate := redis.NewRedisDispatchGate(rdb, "budget-key")

	require.Equal(t, 0.5, gate.Budget(context.Background()))

	s.Set("budget-key", "0.9")
	assert.Equal(t, 0.9, gate.Budget(context.Background()), "budget should reflect updated Redis value")
}
