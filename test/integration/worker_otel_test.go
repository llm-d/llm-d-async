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
	"github.com/llm-d-incubation/llm-d-async/pkg/asyncworker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func setupTestTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
	})
	return exporter
}

// TestWorkerDispatch_TraceparentInjected verifies that when the HTTP transport
// is wrapped with otelhttp, the outgoing request to the inference gateway
// contains the W3C traceparent header.
func TestWorkerDispatch_TraceparentInjected(t *testing.T) {
	setupTestTracer(t)

	var mu sync.Mutex
	var receivedTraceparent string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedTraceparent = r.Header.Get("Traceparent")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Wrap transport with otelhttp — same as cmd/main.go does
	otelClient := &http.Client{
		Transport: otelhttp.NewTransport(server.Client().Transport,
			otelhttp.WithSpanNameFormatter(func(_ string, _ *http.Request) string {
				return "http-request"
			}),
		),
	}
	client := asyncworker.NewHTTPInferenceClient(otelClient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go asyncworker.Worker(ctx, pipeline.Characteristics{},
		client, requestChannel, retryChannel, resultChannel, 5*time.Minute)

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{},
		&asyncapi.RequestMessage{
			ID:       "traceparent-inject-1",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(time.Minute).Unix(),
			Payload:  map[string]any{"model": "test", "prompt": "hello"},
		},
	)

	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		HttpHeaders:     map[string]string{"Content-Type": "application/json"},
		RequestURL:      server.URL + "/v1/completions",
	}

	select {
	case <-resultChannel:
	case <-ctx.Done():
		t.Fatal("Timed out waiting for result")
	}

	mu.Lock()
	defer mu.Unlock()
	assert.NotEmpty(t, receivedTraceparent, "expected traceparent header to be injected into outgoing request")
	assert.Contains(t, receivedTraceparent, "-", "traceparent should be in W3C format")
}

// TestWorkerDispatch_SpanHierarchy verifies that processing a request through
// the Worker produces both a "process-request" span and an "http-request" span
// (from otelhttp), and that the HTTP span is a child of the process span.
func TestWorkerDispatch_SpanHierarchy(t *testing.T) {
	exporter := setupTestTracer(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer server.Close()

	otelClient := &http.Client{
		Transport: otelhttp.NewTransport(server.Client().Transport,
			otelhttp.WithSpanNameFormatter(func(_ string, _ *http.Request) string {
				return "http-request"
			}),
		),
	}
	client := asyncworker.NewHTTPInferenceClient(otelClient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go asyncworker.Worker(ctx, pipeline.Characteristics{},
		client, requestChannel, retryChannel, resultChannel, 5*time.Minute)

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{},
		&asyncapi.RequestMessage{
			ID:       "hierarchy-1",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(time.Minute).Unix(),
			Payload:  map[string]any{"model": "test", "prompt": "hello"},
		},
	)

	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		HttpHeaders:     map[string]string{"Content-Type": "application/json"},
		RequestURL:      server.URL + "/v1/completions",
	}

	select {
	case <-resultChannel:
	case <-ctx.Done():
		t.Fatal("Timed out waiting for result")
	}

	spans := exporter.GetSpans()

	var processSpan, httpSpan *tracetest.SpanStub
	for i := range spans {
		switch spans[i].Name {
		case "process-request":
			processSpan = &spans[i]
		case "http-request":
			httpSpan = &spans[i]
		}
	}

	require.NotNil(t, processSpan, "expected 'process-request' span")
	require.NotNil(t, httpSpan, "expected 'http-request' span")

	assert.Equal(t, processSpan.SpanContext.TraceID(), httpSpan.SpanContext.TraceID(),
		"HTTP span should share the same trace ID as process span")
}

// TestWorkerDispatch_MetadataTraceContextPropagation verifies end-to-end trace
// context propagation: a producer injects traceparent into request Metadata,
// the Worker extracts it, and all spans (process-request, http-request) share
// the same trace ID from the original producer.
func TestWorkerDispatch_MetadataTraceContextPropagation(t *testing.T) {
	exporter := setupTestTracer(t)

	var mu sync.Mutex
	var receivedTraceparent string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedTraceparent = r.Header.Get("Traceparent")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	otelClient := &http.Client{
		Transport: otelhttp.NewTransport(server.Client().Transport,
			otelhttp.WithSpanNameFormatter(func(_ string, _ *http.Request) string {
				return "http-request"
			}),
		),
	}
	client := asyncworker.NewHTTPInferenceClient(otelClient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Simulate a producer creating a parent span and injecting context into metadata
	producerCtx, producerSpan := otel.Tracer("producer").Start(ctx, "submit-request")
	producerTraceID := producerSpan.SpanContext().TraceID()
	metadata := make(map[string]string)
	otel.GetTextMapPropagator().Inject(producerCtx, propagation.MapCarrier(metadata))
	producerSpan.End()

	go asyncworker.Worker(ctx, pipeline.Characteristics{},
		client, requestChannel, retryChannel, resultChannel, 5*time.Minute)

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{},
		&asyncapi.RequestMessage{
			ID:       "e2e-propagation-1",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(time.Minute).Unix(),
			Payload:  map[string]any{"model": "test", "prompt": "hello"},
			Metadata: metadata,
		},
	)

	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		HttpHeaders:     map[string]string{"Content-Type": "application/json"},
		RequestURL:      server.URL + "/v1/completions",
	}

	select {
	case <-resultChannel:
	case <-ctx.Done():
		t.Fatal("Timed out waiting for result")
	}

	spans := exporter.GetSpans()

	// All spans should share the producer's trace ID
	for _, s := range spans {
		if s.Name == "process-request" || s.Name == "http-request" {
			assert.Equal(t, producerTraceID, s.SpanContext.TraceID(),
				"span %q should carry the producer's trace ID", s.Name)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	assert.NotEmpty(t, receivedTraceparent, "traceparent should reach the inference gateway")
	assert.Contains(t, receivedTraceparent, producerTraceID.String(),
		"traceparent at inference gateway should contain the producer's trace ID")
}
