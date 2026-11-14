package asyncworker

import (
	"context"
	"errors"
	"fmt"
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

var (
	_ transform.RequestTransform = (*testTransform)(nil)
	_ transform.ContextTransform = (*testTransform)(nil)
)

// testTransform is a minimal RequestTransform for worker integration tests.
type testTransform struct {
	validateErr  error
	transformErr error
	handled      bool
	body         []byte
	contentType  string
	// started, when set, is closed once TransformContext runs. waitForCtx
	// makes it block until its ctx is done and return ctx.Err(), standing in
	// for a slow object fetch.
	started    chan struct{}
	waitForCtx bool
	// gotURL records the requestURL TransformContext was called with.
	gotURL string
}

func (t *testTransform) TypedName() plugins.TypedName {
	return plugins.TypedName{Type: "test", Name: "test"}
}

func (t *testTransform) Validate(_ []byte, _ map[string]string, _ int64) error {
	return t.validateErr
}

func (t *testTransform) Transform(payload []byte, metadata map[string]string) ([]byte, string, bool, error) {
	return t.TransformContext(context.Background(), payload, metadata, "")
}

func (t *testTransform) TransformContext(ctx context.Context, _ []byte, _ map[string]string, requestURL string) ([]byte, string, bool, error) {
	t.gotURL = requestURL
	if t.started != nil {
		close(t.started)
	}
	if t.waitForCtx {
		<-ctx.Done()
		return nil, "", false, fmt.Errorf("fetch object: %w", ctx.Err())
	}
	if t.transformErr != nil {
		return nil, "", false, t.transformErr
	}
	if !t.handled {
		return nil, "", false, nil
	}
	return t.body, t.contentType, true, nil
}

// TestWorker_TransformRewritesOutgoingRequest verifies that when a transform
// handles a message, the worker passes it the dispatch URL and sends the
// transformed body and Content-Type to the inference backend.
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

	xform := &testTransform{
		handled:     true,
		body:        []byte("url=https://storage.example/audio.mp3?signature=test"),
		contentType: "multipart/form-data; boundary=test",
	}
	chain := transform.NewChain([]transform.RequestTransform{xform})

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

	// Read after the result arrives: the channel receive orders it after the
	// worker's write to gotURL.
	if want := "http://localhost/v1/audio/transcriptions"; xform.gotURL != want {
		t.Errorf("transform requestURL = %q, want %q", xform.gotURL, want)
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

// TestWorker_TransformErrorClassification verifies that a transform error is
// retried only when its chain carries a retryable InferenceError category, and
// otherwise fails the request without dispatching it.
func TestWorker_TransformErrorClassification(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantRetry bool
		// wantMinBackoff, when set, is the smallest backoff the retry may
		// carry: a transform's Retry-After must override the ~2-4s first
		// exponential step.
		wantMinBackoff float64
	}{
		{
			name:      "wrapped server error is retried",
			err:       fmt.Errorf("fetch s3://b/k: %w", &asyncapi.ClientError{ErrorCategory: asyncapi.ErrCategoryServer, Message: "storage unavailable"}),
			wantRetry: true,
		},
		{
			name:      "rate limit is retried",
			err:       &asyncapi.ClientError{ErrorCategory: asyncapi.ErrCategoryRateLimit, Message: "slow down"},
			wantRetry: true,
		},
		{
			// Retry-After 40s against a ~100s deadline: retryMessage clamps it
			// to half the remaining time (still 40s) and jitters it up by <25%.
			name:           "retry after from the transform is honored",
			err:            fmt.Errorf("fetch s3://b/k: %w", &asyncapi.ClientError{ErrorCategory: asyncapi.ErrCategoryRateLimit, Message: "slow down", RetryAfter: 40 * time.Second}),
			wantRetry:      true,
			wantMinBackoff: 40,
		},
		{
			name: "auth error is fatal",
			err:  &asyncapi.ClientError{ErrorCategory: asyncapi.ErrCategoryAuth, Message: "access denied"},
		},
		{
			name: "plain error is fatal",
			err:  errors.New("bucket not allowed"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			httpCalled := false
			inferenceClient := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
				httpCalled = true
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
			}))
			chain := transform.NewChain([]transform.RequestTransform{&testTransform{transformErr: tc.err}})
			requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
			retryChannel := make(chan pipeline.RetryMessage, 1)
			resultChannel := make(chan asyncapi.ResultMessage, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)

			requestChannel <- newEmb(asyncapi.RequestMessage{
				ID: "classify", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
				Payload: testPayload(map[string]any{"model": "whisper"}),
			}, "http://localhost/v1/audio/transcriptions", nil)

			select {
			case retry := <-retryChannel:
				if !tc.wantRetry {
					t.Fatalf("got retry, want a terminal result")
				}
				if retry.RetryCount != 1 {
					t.Errorf("retry count = %d, want 1", retry.RetryCount)
				}
				if retry.BackoffDurationSeconds < tc.wantMinBackoff {
					t.Errorf("backoff = %.1fs, want at least %.0fs from Retry-After", retry.BackoffDurationSeconds, tc.wantMinBackoff)
				}
			case result := <-resultChannel:
				if tc.wantRetry {
					t.Fatalf("got result %q, want a retry", result.ErrorCode)
				}
				if result.ErrorCode != asyncapi.ErrCodeInvalidRequest {
					t.Errorf("error code = %q, want %q", result.ErrorCode, asyncapi.ErrCodeInvalidRequest)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for retry or result")
			}
			if httpCalled {
				t.Error("no HTTP request should be dispatched when the transform fails")
			}
		})
	}
}

// TestWorker_TransformHitsRequestDeadline verifies that a transform still
// running when the per-request deadline expires yields DEADLINE_EXCEEDED rather
// than an invalid-request failure or a retry.
func TestWorker_TransformHitsRequestDeadline(t *testing.T) {
	inferenceClient := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		t.Error("no HTTP request should be dispatched when the transform times out")
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	}))
	chain := transform.NewChain([]transform.RequestTransform{&testTransform{waitForCtx: true}})
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, 200*time.Millisecond, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "slow-fetch", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: testPayload(map[string]any{"model": "whisper"}),
	}, "http://localhost/v1/audio/transcriptions", nil)

	select {
	case result := <-resultChannel:
		if result.ErrorCode != asyncapi.ErrCodeDeadlineExceeded {
			t.Errorf("error code = %q, want %q", result.ErrorCode, asyncapi.ErrCodeDeadlineExceeded)
		}
	case <-retryChannel:
		t.Fatal("a transform cut off by the request deadline must not be retried")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

// TestWorker_TransformInterruptedByShutdownReenqueues verifies that shutting
// down while a transform runs re-enqueues the message without counting an
// attempt, instead of failing it.
func TestWorker_TransformInterruptedByShutdownReenqueues(t *testing.T) {
	inferenceClient := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		t.Error("no HTTP request should be dispatched after shutdown")
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	}))
	started := make(chan struct{})
	chain := transform.NewChain([]transform.RequestTransform{&testTransform{waitForCtx: true, started: started}})
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	consumeCtx, stopConsume := context.WithCancel(context.Background())
	defer stopConsume()
	requestCtx, shutdown := context.WithCancel(context.Background())
	defer shutdown()
	go Worker(consumeCtx, requestCtx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "shutdown-fetch", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: testPayload(map[string]any{"model": "whisper"}),
	}, "http://localhost/v1/audio/transcriptions", nil)

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("transform never started")
	}
	shutdown()

	select {
	case retry := <-retryChannel:
		if retry.RetryCount != 0 {
			t.Errorf("retry count = %d, want 0: shutdown must not count an attempt", retry.RetryCount)
		}
	case result := <-resultChannel:
		t.Fatalf("shutdown must re-enqueue, got result %q", result.ErrorCode)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for re-enqueue")
	}
}
