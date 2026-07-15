//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGateFactory_RedisQuota_ConcurrencyParsing validates that GateFactory
// correctly parses "redis-quota" params and produces a working Gate.
func TestGateFactory_RedisQuota_ConcurrencyParsing(t *testing.T) {
	s := miniredis.RunT(t)
	factory := flowcontrol.NewGateFactory("")

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "redis-quota", GateParams: map[string]any{
		"address":   s.Addr(),
		"attribute": "model",
		"mode":      "concurrency",
		"limit":     2,
		"window":    "30s",
		"prefix":    "test:",
	}})
	require.NoError(t, err)
	require.NotNil(t, gate)

	// Budget always returns 1.0 for quota gates.
	assert.Equal(t, 1.0, gate.Budget(context.Background()))

	ctx := context.Background()

	// Apply twice (limit=2) — both should succeed.
	var releases1 []pipeline.GateReleaseFunc
	req1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"model": "gpt-4"}})
	verdict1, err := gate.Apply(ctx, req1, &releases1)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict1.Action)

	var releases2 []pipeline.GateReleaseFunc
	req2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"model": "gpt-4"}})
	verdict2, err := gate.Apply(ctx, req2, &releases2)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict2.Action)

	// Third apply — should block/refuse.
	var releases3 []pipeline.GateReleaseFunc
	req3 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"model": "gpt-4"}})
	verdict3, err := gate.Apply(ctx, req3, &releases3)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict3.Action, "Third request should be denied (limit=2)")

	// Release one and retry.
	for _, f := range releases1 {
		f()
	}

	verdict4, err := gate.Apply(ctx, req3, &releases3)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict4.Action, "Should succeed after release")

	// Cleanup.
	for _, f := range releases2 {
		f()
	}
	for _, f := range releases3 {
		f()
	}
}

// TestGateFactory_RedisQuota_RateLimitParsing validates rate-limit mode parsing.
func TestGateFactory_RedisQuota_RateLimitParsing(t *testing.T) {
	s := miniredis.RunT(t)
	factory := flowcontrol.NewGateFactory("")

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "redis-quota", GateParams: map[string]any{
		"address": s.Addr(),
		"mode":    "rate-limit",
		"limit":   3,
		"window":  "1m",
	}})
	require.NoError(t, err)
	require.NotNil(t, gate)

	ctx := context.Background()

	for i := 0; i < 3; i++ {
		var releases []pipeline.GateReleaseFunc
		req := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"userid": "alice"}})
		verdict, err := gate.Apply(ctx, req, &releases)
		require.NoError(t, err)
		assert.Equal(t, pipeline.ActionContinue, verdict.Action, "Request %d should be allowed", i+1)
	}

	// Fourth should be rate limited.
	var releases4 []pipeline.GateReleaseFunc
	req4 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"userid": "alice"}})
	verdict4, err := gate.Apply(ctx, req4, &releases4)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict4.Action, "Fourth request should be rate limited")
}

// TestGateFactory_RedisQuota_MissingParams validates error handling for missing
// required parameters.
func TestGateFactory_RedisQuota_MissingParams(t *testing.T) {
	factory := flowcontrol.NewGateFactory("")

	_, err := factory.CreateGate(pipeline.GateConfig{GateType: "redis-quota", GateParams: map[string]any{
		"limit": 5,
	}})
	assert.Error(t, err, "Should fail when address is missing")

	s := miniredis.RunT(t)
	_, err = factory.CreateGate(pipeline.GateConfig{GateType: "redis-quota", GateParams: map[string]any{
		"address": s.Addr(),
	}})
	assert.Error(t, err, "Should fail when limit is missing")
}

// TestGateFactory_RedisQuota_DefaultParams verifies that omitted optional params
// fall back to documented defaults (attribute=userid, mode=rate-limit, etc.).
func TestGateFactory_RedisQuota_DefaultParams(t *testing.T) {
	s := miniredis.RunT(t)
	factory := flowcontrol.NewGateFactory("")

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "redis-quota", GateParams: map[string]any{
		"address": s.Addr(),
		"limit":   1,
	}})
	require.NoError(t, err)

	// Default attribute is "userid", default mode is "rate-limit".
	ctx := context.Background()
	var releases1 []pipeline.GateReleaseFunc
	req1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"userid": "bob"}})
	verdict1, err := gate.Apply(ctx, req1, &releases1)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict1.Action)

	var releases2 []pipeline.GateReleaseFunc
	req2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"userid": "bob"}})
	verdict2, err := gate.Apply(ctx, req2, &releases2)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict2.Action, "Second apply should be rate limited with default params")
}
