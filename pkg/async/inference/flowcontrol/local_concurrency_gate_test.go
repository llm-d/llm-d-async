package flowcontrol

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalConcurrencyGate_ApplyAndRelease(t *testing.T) {
	gate := NewLocalConcurrencyGate(3)
	ctx := context.Background()

	// 1. Initial Budget is 1.0
	assert.Equal(t, 1.0, gate.Budget(ctx))

	// 2. Allow 3 requests
	var releases1 []pipeline.GateReleaseFunc
	r1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	verdict, err := gate.Apply(ctx, r1, &releases1)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)

	var releases2 []pipeline.GateReleaseFunc
	r2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	verdict, err = gate.Apply(ctx, r2, &releases2)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)

	var releases3 []pipeline.GateReleaseFunc
	r3 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	verdict, err = gate.Apply(ctx, r3, &releases3)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)

	// Budget should now be 0.0
	assert.Equal(t, 0.0, gate.Budget(ctx))

	// 3. Fourth request should be blocked/refused with redeliver=true
	var releases4 []pipeline.GateReleaseFunc
	r4 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	verdict, err = gate.Apply(ctx, r4, &releases4)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict.Action)

	// 4. Release request 1
	for _, f := range releases1 {
		f()
	}

	// Budget should now be 1/3 (0.333...)
	assert.InDelta(t, 0.333333, gate.Budget(ctx), 1e-4)

	// Now fourth request can be admitted
	verdict, err = gate.Apply(ctx, r4, &releases4)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)

	// Budget is back to 0.0
	assert.Equal(t, 0.0, gate.Budget(ctx))

	// Release remaining requests
	for _, f := range releases2 {
		f()
	}
	for _, f := range releases3 {
		f()
	}
	for _, f := range releases4 {
		f()
	}

	// Budget should be back to 1.0
	assert.Equal(t, 1.0, gate.Budget(ctx))
}

func TestLocalConcurrencyGate_InvalidLimits(t *testing.T) {
	ctx := context.Background()

	// Zero limit
	gate0 := NewLocalConcurrencyGate(0)
	assert.Equal(t, 0.0, gate0.Budget(ctx))
	r := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	var releases []pipeline.GateReleaseFunc
	verdict, err := gate0.Apply(ctx, r, &releases)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict.Action)

	// Negative limit
	gateNeg := NewLocalConcurrencyGate(-5)
	assert.Equal(t, 0.0, gateNeg.Budget(ctx))
	verdict, err = gateNeg.Apply(ctx, r, &releases)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict.Action)
}

func TestLocalConcurrencyGate_Concurrency(t *testing.T) {
	limit := 50
	gate := NewLocalConcurrencyGate(limit)
	ctx := context.Background()

	var wg sync.WaitGroup
	var mu sync.Mutex
	allReleases := make([][]pipeline.GateReleaseFunc, 100)

	// Simulate 100 concurrent requests trying to enter
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
			var releases []pipeline.GateReleaseFunc
			verdict, err := gate.Apply(ctx, req, &releases)
			if err == nil && verdict.Action == pipeline.ActionContinue {
				mu.Lock()
				allReleases[idx] = releases
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	// Count how many requests were admitted (should be exactly limit)
	admittedCount := 0
	for _, r := range allReleases {
		if r != nil {
			admittedCount++
		}
	}
	assert.Equal(t, limit, admittedCount)

	// Now concurrently release all admitted requests
	for i := 0; i < 100; i++ {
		if allReleases[i] != nil {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				for _, f := range allReleases[idx] {
					f()
				}
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
	var releases1 []pipeline.GateReleaseFunc
	r1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	v1, err := gate.Apply(ctx, r1, &releases1)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, v1.Action)

	var releases2 []pipeline.GateReleaseFunc
	r2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	v2, err := gate.Apply(ctx, r2, &releases2)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, v2.Action)

	// Budget should now be 0.0
	assert.Equal(t, 0.0, gate.Budget(ctx))

	// 3. Attempt to admit 3rd request in a separate goroutine (should block)
	r3 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	blockedCh := make(chan struct{})
	resultCh := make(chan pipeline.Verdict, 1)
	errCh := make(chan error, 1)
	var releases3 []pipeline.GateReleaseFunc

	go func() {
		close(blockedCh)
		v, e := gate.Apply(ctx, r3, &releases3)
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
	for _, f := range releases1 {
		f()
	}

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
	for _, f := range releases2 {
		f()
	}
	for _, f := range releases3 {
		f()
	}
	assert.Equal(t, 1.0, gate.Budget(ctx))
}

func TestLocalConcurrencyGate_BlockingModeCancel(t *testing.T) {
	gate := NewLocalConcurrencyGate(1).WithGatingMode(GatingModeBlocking)
	ctx := context.Background()

	// Admit 1 request to exhaust capacity
	var releases1 []pipeline.GateReleaseFunc
	r1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	v1, err := gate.Apply(ctx, r1, &releases1)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, v1.Action)

	// Try to admit 2nd request with a cancelled context
	var releases2 []pipeline.GateReleaseFunc
	r2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // cancel immediately

	_, err = gate.Apply(cancelCtx, r2, &releases2)
	assert.ErrorIs(t, err, context.Canceled)

	// Clean up
	for _, f := range releases1 {
		f()
	}
	assert.Equal(t, 1.0, gate.Budget(ctx))
}
