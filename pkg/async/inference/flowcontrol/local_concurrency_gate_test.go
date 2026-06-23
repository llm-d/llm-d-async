package flowcontrol

import (
	"context"
	"sync"
	"testing"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalConcurrencyGate_ApplyAndRelease(t *testing.T) {
	gate := NewLocalConcurrencyGate(3)
	ctx := context.Background()

	// 1. Initial Budget is 1.0
	assert.Equal(t, 1.0, gate.Budget(ctx))

	// 2. Allow 3 requests
	r1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	verdict, err := gate.Apply(ctx, r1)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)

	r2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	verdict, err = gate.Apply(ctx, r2)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)

	r3 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	verdict, err = gate.Apply(ctx, r3)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)

	// Budget should now be 0.0
	assert.Equal(t, 0.0, gate.Budget(ctx))

	// 3. Fourth request should be blocked/refused with redeliver=true
	r4 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	verdict, err = gate.Apply(ctx, r4)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict.Action)

	// 4. Release request 1
	r1.Release()

	// Budget should now be 1/3 (0.333...)
	assert.InDelta(t, 0.333333, gate.Budget(ctx), 1e-4)

	// Now fourth request can be admitted
	verdict, err = gate.Apply(ctx, r4)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)

	// Budget is back to 0.0
	assert.Equal(t, 0.0, gate.Budget(ctx))

	// Release remaining requests
	r2.Release()
	r3.Release()
	r4.Release()

	// Budget should be back to 1.0
	assert.Equal(t, 1.0, gate.Budget(ctx))
}

func TestLocalConcurrencyGate_InvalidLimits(t *testing.T) {
	ctx := context.Background()

	// Zero limit
	gate0 := NewLocalConcurrencyGate(0)
	assert.Equal(t, 0.0, gate0.Budget(ctx))
	r := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	verdict, err := gate0.Apply(ctx, r)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict.Action)

	// Negative limit
	gateNeg := NewLocalConcurrencyGate(-5)
	assert.Equal(t, 0.0, gateNeg.Budget(ctx))
	verdict, err = gateNeg.Apply(ctx, r)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict.Action)
}

func TestLocalConcurrencyGate_Concurrency(t *testing.T) {
	limit := 50
	gate := NewLocalConcurrencyGate(limit)
	ctx := context.Background()

	var wg sync.WaitGroup
	requests := make([]*api.InternalRequest, 100)

	// Simulate 100 concurrent requests trying to enter
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
			verdict, err := gate.Apply(ctx, req)
			if err == nil && verdict.Action == pipeline.ActionContinue {
				requests[idx] = req
			}
		}(i)
	}
	wg.Wait()

	// Count how many requests were admitted (should be exactly limit)
	admittedCount := 0
	for _, r := range requests {
		if r != nil {
			admittedCount++
		}
	}
	assert.Equal(t, limit, admittedCount)

	// Now concurrently release all admitted requests
	for i := 0; i < 100; i++ {
		if requests[i] != nil {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				requests[idx].Release()
			}(i)
		}
	}
	wg.Wait()

	// Budget should be fully recovered to 1.0
	assert.Equal(t, 1.0, gate.Budget(ctx))
}
