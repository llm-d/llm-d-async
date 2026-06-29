//go:build integration

package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d-incubation/llm-d-async/pkg/asyncworker"
	"github.com/stretchr/testify/assert"
)

// TestPoolGating_Blocking verifies that pool-level gating in "blocking" mode
// parks workers when capacity is exhausted and wakes them up once a slot is released,
// without generating retry/nack messages.
func TestPoolGating_Blocking(t *testing.T) {
	serverHitCount := 0
	var mu sync.Mutex

	var closeOnce sync.Once
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		serverHitCount++
		mu.Unlock()
		<-serverDone
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer func() {
		closeOnce.Do(func() { close(serverDone) })
		server.Close()
	}()

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 5)
	retryChannel := make(chan pipeline.RetryMessage, 5)
	resultChannel := make(chan asyncapi.ResultMessage, 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a local concurrency gate with limit = 2 and gating_mode = blocking
	poolGate := flowcontrol.NewLocalConcurrencyGate(2).WithGatingMode(flowcontrol.GatingModeBlocking)

	// Spawn 4 concurrent workers (exceeding the gate limit of 2)
	var wg sync.WaitGroup
	for w := 1; w <= 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			asyncworker.WorkerWithGate(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
				client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil, poolGate)
		}()
	}

	// Send 3 requests into the channel
	ids := []string{"req-1", "req-2", "req-3"}
	for _, id := range ids {
		ir := asyncapi.NewInternalRequest(
			asyncapi.InternalRouting{RequestQueueName: "test-queue"},
			&asyncapi.RequestMessage{
				ID:       id,
				Created:  time.Now().Unix(),
				Deadline: time.Now().Add(5 * time.Minute).Unix(),
				Payload:  map[string]any{"model": "test", "prompt": "hello"},
			},
		)
		requestChannel <- pipeline.EmbelishedRequestMessage{
			InternalRequest: ir,
			HttpHeaders:     map[string]string{"Content-Type": "application/json"},
			RequestURL:      server.URL + "/v1/completions",
		}
	}

	// Wait briefly and verify that exactly 2 requests reached the backend server (gate limit)
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	assert.Equal(t, 2, serverHitCount, "should limit active backend dispatches to 2")
	mu.Unlock()

	// Ensure no retries/nacks were triggered (zero extra latency / retry loop overhead)
	assert.Len(t, retryChannel, 0)
	assert.Len(t, resultChannel, 0)

	// Release the current 2 requests by shutting down backend handler serverDone
	closeOnce.Do(func() { close(serverDone) })

	// Wait and verify that the 3rd request now successfully proceeds and finishes
	timeout := time.After(2 * time.Second)
	completed := make(map[string]bool)
	for range ids {
		select {
		case res := <-resultChannel:
			completed[res.ID] = true
		case <-timeout:
			t.Fatal("timed out waiting for requests to complete")
		}
	}

	assert.True(t, completed["req-1"])
	assert.True(t, completed["req-2"])
	assert.True(t, completed["req-3"])
	assert.Len(t, retryChannel, 0)

	cancel()
	wg.Wait()
}

// TestPoolGating_Timeout verifies that request deadline is respected
// while worker is parked waiting for gate capacity.
func TestPoolGating_Timeout(t *testing.T) {
	var closeOnce sync.Once
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-serverDone
	}))
	defer func() {
		closeOnce.Do(func() { close(serverDone) })
		server.Close()
	}()

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 2)
	retryChannel := make(chan pipeline.RetryMessage, 2)
	resultChannel := make(chan asyncapi.ResultMessage, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// gate limit = 1
	poolGate := flowcontrol.NewLocalConcurrencyGate(1).WithGatingMode(flowcontrol.GatingModeBlocking)

	// Spawn 2 workers
	var wg sync.WaitGroup
	for w := 1; w <= 2; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			asyncworker.WorkerWithGate(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
				client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil, poolGate)
		}()
	}

	// Request 1 takes the capacity slot
	ir1 := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{RequestQueueName: "test-queue"},
		&asyncapi.RequestMessage{
			ID:       "req-slow",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload:  map[string]any{"model": "test"},
		},
	)
	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir1,
		RequestURL:      server.URL + "/v1/completions",
	}

	// Request 2 has a very short deadline and will get parked/blocked
	ir2 := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{RequestQueueName: "test-queue"},
		&asyncapi.RequestMessage{
			ID:       "req-timeout",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(100 * time.Millisecond).Unix(), // 100ms deadline
			Payload:  map[string]any{"model": "test"},
		},
	)
	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir2,
		RequestURL:      server.URL + "/v1/completions",
	}

	// We expect req-timeout to fail with deadline exceeded result
	select {
	case res := <-resultChannel:
		assert.Equal(t, "req-timeout", res.ID)
		assert.Contains(t, res.Payload, "deadline exceeded")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deadline exceeded result")
	}

	cancel()
	closeOnce.Do(func() { close(serverDone) })
	wg.Wait()
}

type customMockGate struct {
	mu       sync.Mutex
	verdicts []pipeline.Verdict
	index    int
}

func (g *customMockGate) Budget(ctx context.Context) float64 {
	return 1.0
}

func (g *customMockGate) Apply(ctx context.Context, msg *asyncapi.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.index >= len(g.verdicts) {
		return pipeline.Continue(), nil
	}
	v := g.verdicts[g.index]
	g.index++
	return v, nil
}

func TestPoolGating_ActionWait(t *testing.T) {
	serverHitCount := 0
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		serverHitCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer server.Close()

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Gate returns Wait twice, then Continue
	gate := &customMockGate{
		verdicts: []pipeline.Verdict{
			pipeline.Wait(),
			pipeline.Wait(),
			pipeline.Continue(),
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		asyncworker.WorkerWithGate(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
			client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil, gate)
	}()

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{RequestQueueName: "test-queue"},
		&asyncapi.RequestMessage{
			ID:       "req-wait",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload:  map[string]any{"model": "test"},
		},
	)
	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		RequestURL:      server.URL + "/v1/completions",
	}

	select {
	case res := <-resultChannel:
		assert.Equal(t, "req-wait", res.ID)
		mu.Lock()
		assert.Equal(t, 1, serverHitCount, "should hit server exactly once")
		mu.Unlock()
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for request to complete with ActionWait")
	}

	assert.Len(t, retryChannel, 0)

	cancel()
	wg.Wait()
}

func TestPoolGating_ActionRefuse(t *testing.T) {
	serverHitCount := 0
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		serverHitCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Gate returns Refuse immediately
	gate := &customMockGate{
		verdicts: []pipeline.Verdict{
			pipeline.Refuse(),
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		asyncworker.WorkerWithGate(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
			client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil, gate)
	}()

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{RequestQueueName: "test-queue"},
		&asyncapi.RequestMessage{
			ID:       "req-refuse",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload:  map[string]any{"model": "test"},
		},
	)
	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		RequestURL:      server.URL + "/v1/completions",
	}

	// We expect the request to be put in the retry channel immediately
	select {
	case retryMsg := <-retryChannel:
		assert.Equal(t, "req-refuse", retryMsg.PublicRequest.ReqID())
		assert.Equal(t, float64(0), retryMsg.BackoffDurationSeconds)
	case <-resultChannel:
		t.Fatal("request should not have completed")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for retry message")
	}

	mu.Lock()
	assert.Equal(t, 0, serverHitCount, "server should not be hit on Refuse")
	mu.Unlock()

	cancel()
	wg.Wait()
}
