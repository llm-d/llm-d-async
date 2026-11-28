package asyncworker

import (
	"bytes"
	"context"
	"io"
	"maps"
	"net/http"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// The inference latency observation carries the trace of the request's
// process-request span, so a slow point on the latency graph opens that trace.
func TestWorker_InferenceLatencyExemplarNamesProcessRequestSpan(t *testing.T) {
	exporter := setupTestTracer(t)
	queueID, queueName := "exemplar-qid", "exemplar-queue"

	httpclient := NewTestClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, NewHTTPInferenceClient(httpclient), requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmbR(asyncapi.InternalRouting{QueueID: queueID, RequestQueueName: queueName}, asyncapi.RequestMessage{
		ID: "exemplar", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: testPayload(map[string]any{"model": "test", "prompt": "hi"}),
	}, "http://localhost:30800/v1/completions", nil)
	select {
	case <-resultChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	span := findSpan(getSpansEventually(t, exporter, 1), "process-request")
	if span == nil {
		t.Fatal("expected a 'process-request' span")
	}

	obs, err := metrics.InferenceLatencyTime.GetMetricWithLabelValues(queueID, queueName, "test-pool")
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	m := &dto.Metric{}
	if err := obs.(prometheus.Metric).Write(m); err != nil {
		t.Fatalf("Write: %v", err)
	}
	var got map[string]string
	for _, bucket := range m.GetHistogram().GetBucket() {
		if e := bucket.GetExemplar(); e != nil {
			got = map[string]string{}
			for _, l := range e.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
		}
	}
	want := map[string]string{
		"trace_id": span.SpanContext.TraceID().String(),
		"span_id":  span.SpanContext.SpanID().String(),
	}
	if !maps.Equal(got, want) {
		t.Errorf("inference latency exemplar = %v, want the process-request span %v", got, want)
	}
}
