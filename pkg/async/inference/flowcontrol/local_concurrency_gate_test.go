package flowcontrol

import (
	"context"
	"sync"
	"testing"
	"time"

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

func TestLocalConcurrencyGate_BlockingMode(t *testing.T) {
	gate := NewLocalConcurrencyGate(2).WithGatingMode(GatingModeBlocking)
	ctx := context.Background()

	// 1. Initial Budget is 1.0
	assert.Equal(t, 1.0, gate.Budget(ctx))

	// 2. Admit 2 requests
	r1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	v1, err := gate.Apply(ctx, r1)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, v1.Action)

	r2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	v2, err := gate.Apply(ctx, r2)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, v2.Action)

	// Budget should now be 0.0
	assert.Equal(t, 0.0, gate.Budget(ctx))

	// 3. Attempt to admit 3rd request in a separate goroutine (should block)
	r3 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	blockedCh := make(chan struct{})
	resultCh := make(chan pipeline.Verdict, 1)
	errCh := make(chan error, 1)

	go func() {
		close(blockedCh)
		v, e := gate.Apply(ctx, r3)
		resultCh <- v
		errCh <- e
	}()

	<-blockedCh
	// Sleep briefly to ensure the goroutine is indeed parked waiting on semaphore
	time.Sleep(50 * time.Millisecond)

	select {
	case <-resultCh:
		t.Fatal("Apply should have blocked, but returned result immediately")
	default:
		// Passed: goroutine is blocked
	}

	// 4. Release request 1
	r1.Release()

	// 5. Goroutine should unblock and the request should be admitted
	select {
	case verdict := <-resultCh:
		require.NoError(t, <-errCh)
		assert.Equal(t, pipeline.ActionContinue, verdict.Action)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for 3rd request to unblock")
	}

	// Budget should be back to 0.0
	assert.Equal(t, 0.0, gate.Budget(ctx))

	// Clean up
	r2.Release()
	r3.Release()
	assert.Equal(t, 1.0, gate.Budget(ctx))
}

func TestLocalConcurrencyGate_BlockingModeCancel(t *testing.T) {
	gate := NewLocalConcurrencyGate(1).WithGatingMode(GatingModeBlocking)
	ctx := context.Background()

	// Admit 1 request to exhaust capacity
	r1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	v1, err := gate.Apply(ctx, r1)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, v1.Action)

	// Try to admit 2nd request with a cancelled context
	r2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // cancel immediately

	_, err = gate.Apply(cancelCtx, r2)
	assert.ErrorIs(t, err, context.Canceled)

	// Clean up
	r1.Release()
	assert.Equal(t, 1.0, gate.Budget(ctx))
}
