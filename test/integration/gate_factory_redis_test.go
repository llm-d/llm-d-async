//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGateFactory_RedisQuota_ConcurrencyParsing validates that GateFactory
// correctly parses "redis-quota" params and produces a working AttributeGate.
// Packages exercised: flowcontrol (GateFactory) -> pkg/redis (RedisQuotaGate).
func TestGateFactory_RedisQuota_ConcurrencyParsing(t *testing.T) {
	s := miniredis.RunT(t)
	factory := flowcontrol.NewGateFactory("")

	gate, err := factory.CreateGate("redis-quota", map[string]string{
		"address":   s.Addr(),
		"attribute": "model",
		"mode":      "concurrency",
		"limit":     "2",
		"window":    "30s",
		"prefix":    "test:",
	})
	require.NoError(t, err)
	require.NotNil(t, gate)

	// Budget always returns 1.0 for quota gates.
	assert.Equal(t, 1.0, gate.Budget(context.Background()))

	attrGate, ok := gate.(pipeline.AttributeGate)
	require.True(t, ok, "redis-quota gate should implement AttributeGate")

	ctx := context.Background()
	attrs := map[string]string{"model": "gpt-4"}

	// Acquire twice (limit=2) — both should succeed.
	ok1, rel1, err := attrGate.Acquire(ctx, attrs)
	require.NoError(t, err)
	assert.True(t, ok1)

	ok2, rel2, err := attrGate.Acquire(ctx, attrs)
	require.NoError(t, err)
	assert.True(t, ok2)

	// Third acquire — should fail.
	ok3, _, err := attrGate.Acquire(ctx, attrs)
	require.NoError(t, err)
	assert.False(t, ok3, "Third acquire should be denied (limit=2)")

	// Release one and retry.
	rel1()
	ok4, rel4, err := attrGate.Acquire(ctx, attrs)
	require.NoError(t, err)
	assert.True(t, ok4, "Should succeed after release")

	// Cleanup.
	rel2()
	if rel4 != nil {
		rel4()
	}
}

// TestGateFactory_RedisQuota_RateLimitParsing validates rate-limit mode parsing.
func TestGateFactory_RedisQuota_RateLimitParsing(t *testing.T) {
	s := miniredis.RunT(t)
	factory := flowcontrol.NewGateFactory("")

	gate, err := factory.CreateGate("redis-quota", map[string]string{
		"address": s.Addr(),
		"mode":    "rate-limit",
		"limit":   "3",
		"window":  "1m",
	})
	require.NoError(t, err)
	require.NotNil(t, gate)

	attrGate, ok := gate.(pipeline.AttributeGate)
	require.True(t, ok)

	ctx := context.Background()
	attrs := map[string]string{"userid": "alice"}

	for i := 0; i < 3; i++ {
		ok, _, err := attrGate.Acquire(ctx, attrs)
		require.NoError(t, err)
		assert.True(t, ok, "Request %d should be allowed", i+1)
	}

	// Fourth should be rate limited.
	ok4, _, err := attrGate.Acquire(ctx, attrs)
	require.NoError(t, err)
	assert.False(t, ok4, "Fourth request should be rate limited")
}

// TestGateFactory_RedisQuota_MissingParams validates error handling for missing
// required parameters.
func TestGateFactory_RedisQuota_MissingParams(t *testing.T) {
	factory := flowcontrol.NewGateFactory("")

	_, err := factory.CreateGate("redis-quota", map[string]string{
		"limit": "5",
	})
	assert.Error(t, err, "Should fail when address is missing")

	s := miniredis.RunT(t)
	_, err = factory.CreateGate("redis-quota", map[string]string{
		"address": s.Addr(),
	})
	assert.Error(t, err, "Should fail when limit is missing")
}

// TestGateFactory_RedisQuota_DefaultParams verifies that omitted optional params
// fall back to documented defaults (attribute=userid, mode=rate-limit, etc.).
func TestGateFactory_RedisQuota_DefaultParams(t *testing.T) {
	s := miniredis.RunT(t)
	factory := flowcontrol.NewGateFactory("")

	gate, err := factory.CreateGate("redis-quota", map[string]string{
		"address": s.Addr(),
		"limit":   "1",
	})
	require.NoError(t, err)

	attrGate, ok := gate.(pipeline.AttributeGate)
	require.True(t, ok)

	// Default attribute is "userid", default mode is "rate-limit".
	ctx := context.Background()
	ok1, _, err := attrGate.Acquire(ctx, map[string]string{"userid": "bob"})
	require.NoError(t, err)
	assert.True(t, ok1)

	ok2, _, err := attrGate.Acquire(ctx, map[string]string{"userid": "bob"})
	require.NoError(t, err)
	assert.False(t, ok2, "Second acquire should be rate limited with default params")
}
