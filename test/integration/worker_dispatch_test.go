//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/asyncworker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorkerDispatch_DrainsBufferedOnShutdown verifies that when the parent
// context is cancelled with multiple messages buffered in the request channel
// (only one in-flight), all messages are re-queued via the retry channel.
// This exercises the drain loop added to the Worker's ctx.Done() handler.
func TestWorkerDispatch_DrainsBufferedOnShutdown(t *testing.T) {
	serverHit := make(chan struct{}, 1)
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case serverHit <- struct{}{}:
		default:
		}
		<-serverDone
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer func() {
		close(serverDone)
		server.Close()
	}()

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 3)
	retryChannel := make(chan pipeline.RetryMessage, 3)
	resultChannel := make(chan asyncapi.ResultMessage, 3)

	consumeCtx, consumeCancel := context.WithCancel(context.Background())
	requestCtx, requestCancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		asyncworker.Worker(consumeCtx, requestCtx, pipeline.Characteristics{HasExternalBackoff: false},
			client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil)
	}()

	ids := []string{"drain-int-1", "drain-int-2", "drain-int-3"}
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

	<-serverHit
	consumeCancel()
	requestCancel()
	wg.Wait()

	got := make(map[string]bool)
	timeout := time.After(5 * time.Second)
	for range ids {
		select {
		case msg := <-retryChannel:
			got[msg.PublicRequest.ReqID()] = true
		case <-timeout:
			t.Fatal("timed out waiting for re-queued messages")
		}
	}
	for _, id := range ids {
		assert.True(t, got[id], "message %s should have been re-queued on shutdown", id)
	}
}

// TestWorkerDispatch_MockIGW spawns an httptest.Server as a mock inference
// gateway and verifies that the Worker correctly assembles the request URL,
// forwards headers, sends the payload, and routes the result back.
func TestWorkerDispatch_MockIGW(t *testing.T) {
	var mu sync.Mutex
	var receivedURL string
	var receivedHeaders http.Header
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		receivedURL = r.URL.Path
		receivedHeaders = r.Header.Clone()
		body, _ := io.ReadAll(r.Body)
		receivedBody = body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer server.Close()

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go asyncworker.Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
		client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil)

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{RequestQueueName: "test-queue"},
		&asyncapi.RequestMessage{
			ID:       "dispatch-test-1",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(time.Minute).Unix(),
			Payload:  map[string]any{"model": "test-model", "prompt": "hello world"},
		},
	)

	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		HttpHeaders: map[string]string{
			"Content-Type":                  "application/json",
			"x-gateway-inference-objective": "latency",
			"x-custom-header":               "custom-value",
		},
		RequestURL: server.URL + "/v1/completions",
	}

	select {
	case result := <-resultChannel:
		assert.Equal(t, "dispatch-test-1", result.ID)
		assert.Equal(t, `{"result":"success"}`, result.Payload)
	case retry := <-retryChannel:
		t.Fatalf("Unexpected retry for message %s", retry.PublicRequest.ReqID())
	case <-ctx.Done():
		t.Fatal("Timed out waiting for result")
	}

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, "/v1/completions", receivedURL)
	assert.Equal(t, "application/json", receivedHeaders.Get("Content-Type"))
	assert.Equal(t, "latency", receivedHeaders.Get("X-Gateway-Inference-Objective"))
	assert.Equal(t, "custom-value", receivedHeaders.Get("X-Custom-Header"))

	var payload map[string]any
	require.NoError(t, json.Unmarshal(receivedBody, &payload))
	assert.Equal(t, "test-model", payload["model"])
	assert.Equal(t, "hello world", payload["prompt"])
}

// TestWorkerDispatch_EndpointOverride verifies that when a message carries a
// per-message endpoint override, the Worker uses that endpoint path.
func TestWorkerDispatch_EndpointOverride(t *testing.T) {
	var mu sync.Mutex
	var receivedURL string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedURL = r.URL.Path
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go asyncworker.Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
		client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil)

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{},
		&asyncapi.RequestMessage{
			ID:       "endpoint-override-1",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(time.Minute).Unix(),
			Payload:  map[string]any{"model": "test"},
		},
	)

	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		HttpHeaders:     map[string]string{"Content-Type": "application/json"},
		RequestURL:      server.URL + "/v1/chat/completions",
	}

	select {
	case result := <-resultChannel:
		assert.Equal(t, "endpoint-override-1", result.ID)
	case <-ctx.Done():
		t.Fatal("Timed out waiting for result")
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "/v1/chat/completions", receivedURL)
}

// TestWorkerDispatch_ServerErrorTriggersRetry verifies that a 5xx response
// from the mock IGW causes the worker to send a retry rather than a result.
func TestWorkerDispatch_ServerErrorTriggersRetry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go asyncworker.Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
		client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil)

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{},
		&asyncapi.RequestMessage{
			ID:       "retry-test-1",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(time.Minute).Unix(),
			Payload:  map[string]any{"model": "test"},
		},
	)

	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		HttpHeaders:     map[string]string{},
		RequestURL:      server.URL + "/v1/completions",
	}

	select {
	case retry := <-retryChannel:
		assert.Equal(t, "retry-test-1", retry.PublicRequest.ReqID())
		assert.Equal(t, 1, retry.RetryCount)
	case <-resultChannel:
		t.Fatal("Expected retry, got result")
	case <-ctx.Done():
		t.Fatal("Timed out waiting for retry")
	}
}

// TestWorkerDispatch_ResultCallback verifies the full round-trip: Worker sends
// a request to the mock IGW, receives the response, and puts the correct
// ResultMessage (including routing metadata) on the result channel.
func TestWorkerDispatch_ResultCallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices":[{"text":"generated"}]}`))
	}))
	defer server.Close()

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go asyncworker.Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
		client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil)

	routing := asyncapi.InternalRouting{
		RequestQueueName:       "my-queue",
		TransportCorrelationID: "corr-123",
	}
	ir := asyncapi.NewInternalRequest(routing, &asyncapi.RequestMessage{
		ID:       "callback-test-1",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(time.Minute).Unix(),
		Payload:  map[string]any{"model": "test"},
		Metadata: map[string]string{"trace_id": "abc-123"},
	})

	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		HttpHeaders:     map[string]string{"Content-Type": "application/json"},
		RequestURL:      server.URL + "/v1/completions",
	}

	select {
	case result := <-resultChannel:
		assert.Equal(t, "callback-test-1", result.ID)
		assert.Equal(t, `{"choices":[{"text":"generated"}]}`, result.Payload)
		assert.Equal(t, "my-queue", result.Routing.RequestQueueName)
		assert.Equal(t, "corr-123", result.Routing.TransportCorrelationID)
		assert.Equal(t, "abc-123", result.Metadata["trace_id"])
	case <-retryChannel:
		t.Fatal("Unexpected retry")
	case <-ctx.Done():
		t.Fatal("Timed out waiting for result")
	}
}

// TestWorkerDispatch_RequeuesOnShutdown verifies that when the parent context
// is cancelled (simulating SIGTERM) while an inference request is in-flight,
// the Worker sends the message to the retry channel instead of treating it
// as a fatal error.
func TestWorkerDispatch_RequeuesOnShutdown(t *testing.T) {
	// Server blocks until explicitly released (simulates in-flight request).
	serverHit := make(chan struct{})
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(serverHit)
		<-serverDone
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		asyncworker.Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
			client, requestChannel, retryChannel, resultChannel, 5*time.Minute, nil)
	}()

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{RequestQueueName: "test-queue"},
		&asyncapi.RequestMessage{
			ID:       "shutdown-requeue-1",
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

	// Wait for the server to receive the request before cancelling.
	<-serverHit

	// Simulate SIGTERM.
	cancel()

	select {
	case msg := <-retryChannel:
		assert.Equal(t, "shutdown-requeue-1", msg.PublicRequest.ReqID())
		assert.Equal(t, float64(0), msg.BackoffDurationSeconds)
	case r := <-resultChannel:
		t.Fatalf("Expected retry on shutdown, got fatal result: %s", r.Payload)
	case <-time.After(5 * time.Second):
		t.Fatal("Worker did not send retry within 5s after shutdown")
	}

	// Release the server handler so it returns and server.Close() doesn't hang.
	close(serverDone)
	server.Close()
	wg.Wait()
}

// TestWorkerDispatch_PoolIsolation verifies that saturating one pool's workers
// does not affect or block processing in other pools.
func TestWorkerDispatch_PoolIsolation(t *testing.T) {
	// 1. Setup a blocked server (simulates slow/hung gateway for pool-blocked)
	blockedHit := make(chan struct{})
	blockedRelease := make(chan struct{})
	blockedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(blockedHit)
		<-blockedRelease
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"blocked-resolved"}`))
	}))
	defer blockedServer.Close()

	// 2. Setup an active server (simulates healthy gateway for pool-active)
	activeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"active-success"}`))
	}))
	defer activeServer.Close()

	clientBlocked := asyncworker.NewHTTPInferenceClient(blockedServer.Client())
	clientActive := asyncworker.NewHTTPInferenceClient(activeServer.Client())

	// Channels for pool-blocked
	reqChanBlocked := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChanBlocked := make(chan pipeline.RetryMessage, 1)
	resultChanBlocked := make(chan asyncapi.ResultMessage, 1)

	// Channels for pool-active
	reqChanActive := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChanActive := make(chan pipeline.RetryMessage, 1)
	resultChanActive := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Spawn 1 worker for pool-blocked
	go asyncworker.Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
		clientBlocked, reqChanBlocked, retryChanBlocked, resultChanBlocked, 5*time.Minute, nil)

	// Spawn 1 worker for pool-active
	go asyncworker.Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false},
		clientActive, reqChanActive, retryChanActive, resultChanActive, 5*time.Minute, nil)

	// 3. Send message 1 (to pool-blocked)
	irBlocked := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{RequestQueueName: "queue-blocked"},
		&asyncapi.RequestMessage{
			ID:       "msg-blocked",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload:  map[string]any{},
		},
	)
	reqChanBlocked <- pipeline.EmbelishedRequestMessage{
		InternalRequest: irBlocked,
		RequestURL:      blockedServer.URL + "/completions",
	}

	// Wait until the blocked worker is processing and hits the blocked mock server
	select {
	case <-blockedHit:
		// worker is now stuck inside the server handler
	case <-ctx.Done():
		t.Fatal("Blocked worker did not start processing")
	}

	// 4. Send message 2 (to pool-active)
	irActive := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{RequestQueueName: "queue-active"},
		&asyncapi.RequestMessage{
			ID:       "msg-active",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload:  map[string]any{},
		},
	)
	reqChanActive <- pipeline.EmbelishedRequestMessage{
		InternalRequest: irActive,
		RequestURL:      activeServer.URL + "/completions",
	}

	// 5. Verify that msg-active succeeds immediately
	select {
	case result := <-resultChanActive:
		assert.Equal(t, "msg-active", result.ID)
		assert.Equal(t, `{"result":"active-success"}`, result.Payload)
	case <-ctx.Done():
		t.Fatal("Active pool was blocked by the saturated pool")
	}

	// Verify that the blocked request has NOT completed yet
	select {
	case <-resultChanBlocked:
		t.Fatal("Blocked request should not have completed yet")
	default:
		// success: blocked channel is still empty
	}

	// 6. Release blocked server and verify blocked request completes
	close(blockedRelease)
	select {
	case result := <-resultChanBlocked:
		assert.Equal(t, "msg-blocked", result.ID)
		assert.Equal(t, `{"result":"blocked-resolved"}`, result.Payload)
	case <-ctx.Done():
		t.Fatal("Blocked request did not complete after release")
	}
}
