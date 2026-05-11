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

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/asyncworker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	go asyncworker.Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false},
		client, requestChannel, retryChannel, resultChannel, 5*time.Minute)

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

	go asyncworker.Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false},
		client, requestChannel, retryChannel, resultChannel, 5*time.Minute)

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

	go asyncworker.Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false},
		client, requestChannel, retryChannel, resultChannel, 5*time.Minute)

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

	go asyncworker.Worker(ctx, pipeline.Characteristics{HasExternalBackoff: false},
		client, requestChannel, retryChannel, resultChannel, 5*time.Minute)

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
