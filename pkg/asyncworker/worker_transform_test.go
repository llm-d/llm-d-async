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

// TestWorker_TransformRetryOutlivesAttemptTimeout checks that a retry waiting
// for room in a full retry channel is not dropped when the per-attempt timeout
// expires. Only worker shutdown may stop that wait.
func TestWorker_TransformRetryOutlivesAttemptTimeout(t *testing.T) {
	httpClient := NewTestClient(func(*http.Request) (*http.Response, error) {
		t.Error("inference must not be called after a retryable transform error")
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})
	chain := transform.NewChain([]transform.RequestTransform{&testTransform{
		contextErr: &asyncapi.ClientError{ErrorCategory: asyncapi.ErrCategoryServer, Message: "audio download failed"},
	}})
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage) // unbuffered: the retry send blocks until the test reads
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attemptTimeout := 50 * time.Millisecond
	go Worker(ctx, ctx, pipeline.Characteristics{}, NewHTTPInferenceClient(httpClient), requestChannel, retryChannel, resultChannel, attemptTimeout, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "transform-retry-blocked", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: testPayload(map[string]any{"model": "whisper"}),
	}, "http://localhost/v1/audio/transcriptions", nil)
	time.Sleep(4 * attemptTimeout)

	select {
	case retry := <-retryChannel:
		if retry.PublicRequest.ReqID() != "transform-retry-blocked" {
			t.Errorf("retry ID = %q, want transform-retry-blocked", retry.PublicRequest.ReqID())
		}
	case result := <-resultChannel:
		t.Fatalf("retryable transform error produced result %+v", result)
	case <-time.After(2 * time.Second):
		t.Fatal("retry was dropped when the per-attempt timeout expired")
	}
}

// waitingRetryTransform blocks until its context ends, then fails with a
// retryable error. It behaves like a download that hit its timeout.
type waitingRetryTransform struct {
	testTransform
}

func (w *waitingRetryTransform) TransformWithContext(ctx context.Context, _ []byte, _ map[string]string, _ string) ([]byte, string, bool, error) {
	<-ctx.Done()
	return nil, "", false, &asyncapi.ClientError{
		ErrorCategory: asyncapi.ErrCategoryServer,
		Message:       "audio download stopped",
		RawError:      ctx.Err(),
	}
}

// TestWorker_TransformAttemptTimeoutRetries checks that a transform that hits
// the per-attempt timeout is retried while the request deadline is still far
// away. Only the request deadline may end the request with DEADLINE_EXCEEDED.
func TestWorker_TransformAttemptTimeoutRetries(t *testing.T) {
	httpClient := NewTestClient(func(*http.Request) (*http.Response, error) {
		t.Error("inference must not be called after a transform timeout")
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})
	chain := transform.NewChain([]transform.RequestTransform{&waitingRetryTransform{}})
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, NewHTTPInferenceClient(httpClient), requestChannel, retryChannel, resultChannel, 50*time.Millisecond, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "transform-attempt-timeout", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: testPayload(map[string]any{"model": "whisper"}),
	}, "http://localhost/v1/audio/transcriptions", nil)

	select {
	case retry := <-retryChannel:
		if retry.PublicRequest.ReqID() != "transform-attempt-timeout" {
			t.Errorf("retry ID = %q, want transform-attempt-timeout", retry.PublicRequest.ReqID())
		}
	case result := <-resultChannel:
		t.Fatalf("attempt timeout produced a final result %+v, want a retry", result)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for transform retry")
	}
}

// TestWorker_TransformRequestDeadlineIsTerminal checks that a transform still
// running when the request deadline passes ends with DEADLINE_EXCEEDED.
func TestWorker_TransformRequestDeadlineIsTerminal(t *testing.T) {
	httpClient := NewTestClient(func(*http.Request) (*http.Response, error) {
		t.Error("inference must not be called after the request deadline")
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})
	chain := transform.NewChain([]transform.RequestTransform{&waitingRetryTransform{}})
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, NewHTTPInferenceClient(httpClient), requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "transform-request-deadline", Created: time.Now().Unix(), Deadline: time.Now().Add(1 * time.Second).Unix(),
		Payload: testPayload(map[string]any{"model": "whisper"}),
	}, "http://localhost/v1/audio/transcriptions", nil)

	select {
	case result := <-resultChannel:
		if result.ErrorCode != asyncapi.ErrCodeDeadlineExceeded {
			t.Errorf("error code = %q, want %q", result.ErrorCode, asyncapi.ErrCodeDeadlineExceeded)
		}
	case retry := <-retryChannel:
		t.Fatalf("request deadline produced a retry %+v, want DEADLINE_EXCEEDED", retry)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for DEADLINE_EXCEEDED")
	}
}

// TestWorker_TransformRetryKeptOnShutdownWhileBlocked checks that a retry
// waiting for room in a full retry channel is not lost when shutdown starts.
// The retry writer outlives the workers, so the worker must still hand the
// retry over.
func TestWorker_TransformRetryKeptOnShutdownWhileBlocked(t *testing.T) {
	httpClient := NewTestClient(func(*http.Request) (*http.Response, error) {
		t.Error("inference must not be called after a retryable transform error")
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK")), Header: make(http.Header)}, nil
	})
	chain := transform.NewChain([]transform.RequestTransform{&testTransform{
		contextErr: &asyncapi.ClientError{ErrorCategory: asyncapi.ErrCategoryServer, Message: "audio download failed"},
	}})
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage) // unbuffered: the retry send blocks until the test reads
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, NewHTTPInferenceClient(httpClient), requestChannel, retryChannel, resultChannel, defaultRequestTimeout, chain)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "transform-retry-shutdown", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: testPayload(map[string]any{"model": "whisper"}),
	}, "http://localhost/v1/audio/transcriptions", nil)
	// Let the worker reach the blocked retry send, then start shutdown.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case retry := <-retryChannel:
		if retry.PublicRequest.ReqID() != "transform-retry-shutdown" {
			t.Errorf("retry ID = %q, want transform-retry-shutdown", retry.PublicRequest.ReqID())
		}
		if retry.RetryCount != 1 {
			t.Errorf("retry count = %d, want 1", retry.RetryCount)
		}
	case result := <-resultChannel:
		t.Fatalf("shutdown produced a result %+v, want a retry", result)
	case <-time.After(2 * time.Second):
		t.Fatal("retry was lost when shutdown cancelled the blocked enqueue")
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
