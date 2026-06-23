//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGateFactory_RedisQuota_ConcurrencyParsing validates that GateFactory
// correctly parses "redis-quota" params and produces a working Gate.
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

	ctx := context.Background()

	// Apply twice (limit=2) — both should succeed.
	req1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"model": "gpt-4"}})
	verdict1, err := gate.Apply(ctx, req1)
	require.NoError(t, err)
	assert.False(t, verdict1.Redeliver)

	req2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"model": "gpt-4"}})
	verdict2, err := gate.Apply(ctx, req2)
	require.NoError(t, err)
	assert.False(t, verdict2.Redeliver)

	// Third apply — should block/refuse.
	req3 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"model": "gpt-4"}})
	verdict3, err := gate.Apply(ctx, req3)
	require.NoError(t, err)
	assert.True(t, verdict3.Redeliver, "Third request should be denied (limit=2)")

	// Release one and retry.
	req1.Release()

	verdict4, err := gate.Apply(ctx, req3)
	require.NoError(t, err)
	assert.False(t, verdict4.Redeliver, "Should succeed after release")

	// Cleanup.
	req2.Release()
	req3.Release()
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

	ctx := context.Background()

	for i := 0; i < 3; i++ {
		req := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"userid": "alice"}})
		verdict, err := gate.Apply(ctx, req)
		require.NoError(t, err)
		assert.False(t, verdict.Redeliver, "Request %d should be allowed", i+1)
	}

	// Fourth should be rate limited.
	req4 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"userid": "alice"}})
	verdict4, err := gate.Apply(ctx, req4)
	require.NoError(t, err)
	assert.True(t, verdict4.Redeliver, "Fourth request should be rate limited")
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

	// Default attribute is "userid", default mode is "rate-limit".
	ctx := context.Background()
	req1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"userid": "bob"}})
	verdict1, err := gate.Apply(ctx, req1)
	require.NoError(t, err)
	assert.False(t, verdict1.Redeliver)

	req2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"userid": "bob"}})
	verdict2, err := gate.Apply(ctx, req2)
	require.NoError(t, err)
	assert.True(t, verdict2.Redeliver, "Second apply should be rate limited with default params")
}
