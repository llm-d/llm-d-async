package asyncworker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/asyncworker/transform"
	"github.com/llm-d/llm-d-async/pkg/plugins"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var _ transform.RequestTransform = (*testTransform)(nil)
var _ transform.ContextRequestTransform = (*testTransform)(nil)

// testTransform is a minimal RequestTransform for worker integration tests.
type testTransform struct {
	validateErr  error
	transformErr error
	contextErr   error
	handled      bool
	body         []byte
	contentType  string
}

func (t *testTransform) TypedName() plugins.TypedName {
	return plugins.TypedName{Type: "test", Name: "test"}
}

func (t *testTransform) Validate(_ []byte, _ map[string]string, _ int64) error {
	return t.validateErr
}

func (t *testTransform) Transform(_ []byte, _ map[string]string) ([]byte, string, bool, error) {
	if t.transformErr != nil {
		return nil, "", false, t.transformErr
	}
	if !t.handled {
		return nil, "", false, nil
	}
	return t.body, t.contentType, true, nil
}

func (t *testTransform) ValidateWithContext(_ []byte, _ map[string]string, _ string, _ int64) error {
	return t.validateErr
}

func (t *testTransform) TransformWithContext(_ context.Context, _ []byte, _ map[string]string, _ string) ([]byte, string, bool, error) {
	if t.contextErr != nil {
		return nil, "", false, t.contextErr
	}
	return t.Transform(nil, nil)
}

// TestWorker_TransformRewritesOutgoingRequest verifies that when a transform
// handles a message, the worker sends the transformed body and Content-Type to
// the inference backend.
func TestWorker_TransformRewritesOutgoingRequest(t *testing.T) {
	exporter := setupTestTracer(t)
	var gotBody, gotContentType string
	httpClient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		gotBody = string(b)
		gotContentType = req.Header.Get("Content-Type")
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpClient)

	chain := transform.NewChain([]transform.RequestTransform{&testTransform{
		handled:     true,
		body:        []byte("url=https://storage.example/audio.mp3?signature=test"),
		contentType: "multipart/form-data; boundary=test",
	}})

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       "m1",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  testPayload(map[string]any{"model": "whisper", "gcs_uri": "https://storage.example/audio.mp3?signature=test"}),
		Metadata: map[string]string{"provider": "whisper"},
	}, "http://localhost/v1/audio/transcriptions", map[string]string{})

	select {
	case res := <-resultChannel:
		if res.ID != "m1" {
			t.Errorf("result ID = %q, want m1", res.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}

	if want := "url=https://storage.example/audio.mp3?signature=test"; gotBody != want {
		t.Errorf("outgoing body = %q, want %q", gotBody, want)
	}
	if want := "multipart/form-data; boundary=test"; gotContentType != want {
		t.Errorf("outgoing Content-Type = %q, want %q", gotContentType, want)
	}
	spans := getSpansEventually(t, exporter, 1)
	s := findSpan(spans, "process-request")
	if s == nil {
		t.Fatal("expected 'process-request' span")
	}
	assertSpanAttributes(t, s, attribute.String("gen_ai.request.model", "whisper"))
}

func TestWorker_SpanOnTransformError(t *testing.T) {
	exporter := setupTestTracer(t)
	httpCalled := false
	httpClient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		httpCalled = true
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpClient)
	chain := transform.NewChain([]transform.RequestTransform{&testTransform{
		transformErr: errors.New("failed to prepare multipart body"),
	}})
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "span-transform", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: testPayload(map[string]any{"model": "whisper"}),
	}, "http://localhost/v1/audio/transcriptions", nil)

	select {
	case result := <-resultChannel:
		if result.ErrorCode != asyncapi.ErrCodeInvalidRequest {
			t.Errorf("error code = %q, want %q", result.ErrorCode, asyncapi.ErrCodeInvalidRequest)
		}
	case <-retryChannel:
		t.Fatal("transform failure must not be retried")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}
	if httpCalled {
		t.Error("no HTTP request should be dispatched when transform fails")
	}

	spans := getSpansEventually(t, exporter, 1)
	s := findSpan(spans, "process-request")
	if s == nil {
		t.Fatal("expected 'process-request' span")
	}
	assertSpanAttributes(t, s,
		attribute.String("gen_ai.request.model", "whisper"),
		attribute.String("llm_d.async.error.category", "INVALID_REQ"),
		attribute.String("error.category", "INVALID_REQ"),
	)
	if s.Status.Code != codes.Error {
		t.Error("expected error status on transform failure span")
	}
	if len(s.Events) == 0 {
		t.Error("expected recorded transform error event on span")
	}
}

// TestWorker_TransformValidateFatal verifies that a transform Validate failure
// is fatal: an error result is produced and no HTTP request is dispatched.
func TestWorker_TransformValidateFatal(t *testing.T) {
	httpCalled := false
	httpClient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		httpCalled = true
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpClient)

	chain := transform.NewChain([]transform.RequestTransform{&testTransform{
		validateErr: errors.New("signed URL expires before request deadline"),
	}})

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       "m2",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  testPayload(map[string]any{"model": "whisper"}),
		Metadata: map[string]string{"provider": "whisper"},
	}, "http://localhost/v1/audio/transcriptions", map[string]string{})

	select {
	case res := <-resultChannel:
		if !strings.Contains(res.Payload, "validate") {
			t.Errorf("result payload = %q, want it to mention validate failure", res.Payload)
		}
	case <-retryChannel:
		t.Fatal("validate failure must not be retried")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}

	if httpCalled {
		t.Error("no HTTP request should be dispatched when validation fails")
	}
}

func TestWorker_TransformRetryableErrorRequeues(t *testing.T) {
	httpClient := NewTestClient(func(*http.Request) (*http.Response, error) {
		t.Error("inference must not be called after a retryable transform error")
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})
	chain := transform.NewChain([]transform.RequestTransform{&testTransform{
		contextErr: &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryServer,
			Message:       "audio download failed",
			RetryAfter:    30 * time.Second,
		},
	}})
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, NewHTTPInferenceClient(httpClient), requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "transform-retry", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: testPayload(map[string]any{"model": "whisper"}),
	}, "http://localhost/v1/audio/transcriptions", nil)

	select {
	case retry := <-retryChannel:
		if retry.PublicRequest.ReqID() != "transform-retry" {
			t.Errorf("retry ID = %q, want transform-retry", retry.PublicRequest.ReqID())
		}
		if retry.BackoffDurationSeconds < 30 || retry.BackoffDurationSeconds >= 37.5 {
			t.Errorf("retry backoff = %f, want [30,37.5)", retry.BackoffDurationSeconds)
		}
	case result := <-resultChannel:
		t.Fatalf("retryable transform error produced result %+v", result)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for transform retry")
	}
}

func TestWorker_ContextTransformRequeuesOnShutdown(t *testing.T) {
	httpClient := NewTestClient(func(*http.Request) (*http.Response, error) {
		t.Error("inference must not be called after shutdown during transform")
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})
	xform := &blockingContextTransform{started: make(chan struct{})}
	chain := transform.NewChain([]transform.RequestTransform{xform})
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Worker(ctx, ctx, pipeline.Characteristics{}, NewHTTPInferenceClient(httpClient), requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)
	}()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "transform-shutdown", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: testPayload(map[string]any{"model": "whisper"}),
	}, "http://localhost/v1/audio/transcriptions", nil)
	<-xform.started
	cancel()

	select {
	case retry := <-retryChannel:
		if retry.PublicRequest.ReqID() != "transform-shutdown" {
			t.Errorf("retry ID = %q, want transform-shutdown", retry.PublicRequest.ReqID())
		}
	case result := <-resultChannel:
		t.Fatalf("shutdown during transform produced result %+v", result)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for transform shutdown retry")
	}
	<-done
}
