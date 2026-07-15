package asyncworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	uotel "github.com/llm-d/llm-d-async/internal/otel"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/asyncworker/transform"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/llm-d/llm-d-async/pkg/plugins"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const defaultRequestTimeout = 5 * time.Minute

// newEmb wraps a RequestMessage in a minimal InternalRequest for tests.
func newEmb(rm asyncapi.RequestMessage, requestURL string, h map[string]string) pipeline.EmbelishedRequestMessage {
	if h == nil {
		h = map[string]string{}
	}
	return pipeline.EmbelishedRequestMessage{
		InternalRequest: asyncapi.NewInternalRequest(asyncapi.InternalRouting{}, &rm),
		HttpHeaders:     h,
		RequestURL:      requestURL,
		WorkerPoolID:    "test-pool",
	}
}

// newEmbR uses explicit internal routing (e.g. retry count) for tests.
func newEmbR(routing asyncapi.InternalRouting, rm asyncapi.RequestMessage, requestURL string, h map[string]string) pipeline.EmbelishedRequestMessage {
	if h == nil {
		h = map[string]string{}
	}
	return pipeline.EmbelishedRequestMessage{
		InternalRequest: asyncapi.NewInternalRequest(routing, &rm),
		HttpHeaders:     h,
		RequestURL:      requestURL,
		WorkerPoolID:    "test-pool",
	}
}

func TestRetryMessage_deadlinePassed(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	msg := newEmb(asyncapi.RequestMessage{
		ID:       "123",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(-10 * time.Second).Unix(),
	}, "", map[string]string{})
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 0, asyncapi.InferenceResponse{})
	if len(retryChannel) > 0 {
		t.Errorf("Message that its deadline passed should not be retried. Got a message in the retry channel")
		return
	}
	if len(resultChannel) != 1 {
		t.Errorf("Expected one message in the result channel")
		return

	}
	result := <-resultChannel
	if result.StatusCode != 0 {
		t.Errorf("Expected StatusCode 0 for deadline exceeded, got %d", result.StatusCode)
	}
	if result.ErrorCode != asyncapi.ErrCodeDeadlineExceeded {
		t.Errorf("Expected ErrorCode %q, got %q", asyncapi.ErrCodeDeadlineExceeded, result.ErrorCode)
	}
	if result.ErrorMessage != "deadline exceeded" {
		t.Errorf("Expected ErrorMessage 'deadline exceeded', got %q", result.ErrorMessage)
	}
	var resultMap map[string]any
	json.Unmarshal([]byte(result.Payload), &resultMap) // nolint:errcheck
	if resultMap["error"] != "deadline exceeded" {
		t.Errorf("Expected error to be: 'deadline exceeded', got: %s", resultMap["error"])
	}

}

func TestRetryMessage_retry(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	msg := newEmb(asyncapi.RequestMessage{
		ID:       "123",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(10 * time.Second).Unix(),
	}, "", map[string]string{})
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 0, asyncapi.InferenceResponse{})
	if len(resultChannel) > 0 {
		t.Errorf("Should not have any messages in the result channel")
		return
	}
	if len(retryChannel) != 1 {
		t.Errorf("Expected one message in the retry channel")
		return
	}
	retryMsg := <-retryChannel
	if retryMsg.RetryCount != 1 {
		t.Errorf("Expected retry count to be 1, got %d", retryMsg.RetryCount)
	}

}

// RoundTripFunc is a type that implements http.RoundTripper
type RoundTripFunc func(req *http.Request) (*http.Response, error)

// RoundTrip executes a single HTTP transaction, obtaining the Response for a given Request.
func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// NewTestClient returns an *http.Client with its Transport replaced by a custom RoundTripper.
func NewTestClient(fn RoundTripFunc) *http.Client {
	return &http.Client{
		Transport: RoundTripFunc(fn),
	}
}

func TestSheddedRequest(t *testing.T) {
	msgId := "123"
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       nil,
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case r := <-retryChannel:
		if r.PublicRequest == nil || r.PublicRequest.ReqID() != msgId {
			t.Errorf("Expected retry message id to be %s, got %v", msgId, r.PublicRequest)
		}
	case <-resultChannel:
		t.Errorf("Should not get result from a 5xx response")

	}

}
func TestSuccessfulRequest(t *testing.T) {
	msgId := "123"
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       nil,
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case <-retryChannel:
		t.Errorf("Should not get a retry from a 200 response")
	case r := <-resultChannel:
		if r.ID != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.ID)
		}
		if r.StatusCode != http.StatusOK {
			t.Errorf("Expected StatusCode %d, got %d", http.StatusOK, r.StatusCode)
		}
	}

}

func TestSuccessfulRequest_PreservesActualStatusCode(t *testing.T) {
	msgId := "201-created"
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusCreated,
			Body:       io.NopCloser(bytes.NewBufferString(`{"id":"new-resource"}`)),
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case <-retryChannel:
		t.Errorf("should not retry a 2xx response")
	case r := <-resultChannel:
		if r.ID != msgId {
			t.Errorf("Expected result ID %s, got %s", msgId, r.ID)
		}
		if r.StatusCode != http.StatusCreated {
			t.Errorf("Expected StatusCode %d (201 Created), got %d", http.StatusCreated, r.StatusCode)
		}
		if r.Payload != `{"id":"new-resource"}` {
			t.Errorf("Expected body preserved, got %q", r.Payload)
		}
	}
}

type stubCancellationChecker struct {
	mu        sync.RWMutex
	cancelled map[string]bool
	err       error
	checks    int
}

func (s *stubCancellationChecker) IsCancelled(ctx context.Context, requestID, requestToken string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checks++
	if s.err != nil {
		return false, s.err
	}
	return s.cancelled[requestID+"|"+requestToken], nil
}

func (s *stubCancellationChecker) setCancelled(requestID, requestToken string, cancelled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelled == nil {
		s.cancelled = make(map[string]bool)
	}
	s.cancelled[requestID+"|"+requestToken] = cancelled
}

func (s *stubCancellationChecker) checkCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checks
}

type blockingTransform struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (t *blockingTransform) TypedName() plugins.TypedName {
	return plugins.TypedName{Name: "blocking", Type: "test"}
}

func (t *blockingTransform) Validate(payload []byte, metadata map[string]string, reqDeadline int64) error {
	return nil
}

func (t *blockingTransform) Transform(payload []byte, metadata map[string]string) ([]byte, string, bool, error) {
	t.once.Do(func() {
		if t.started != nil {
			close(t.started)
		}
	})
	if t.release != nil {
		<-t.release
	}
	return nil, "", false, nil
}

func TestWorker_CancelledRequestSkipsInference(t *testing.T) {
	msgID := "cancelled-before-send"
	var called atomic.Int32
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		called.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       nil,
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	requestToken := "token-cancelled-before-send"
	ctx := WithCancellationChecker(context.Background(), &stubCancellationChecker{
		cancelled: map[string]bool{msgID + "|" + requestToken: true},
	})

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmbR(asyncapi.InternalRouting{RequestToken: requestToken}, asyncapi.RequestMessage{
		ID:       msgID,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case <-retryChannel:
		t.Fatal("cancelled request should not be retried")
	case r := <-resultChannel:
		if r.ID != msgID {
			t.Errorf("Expected result message id %s, got %s", msgID, r.ID)
		}
		if r.ErrorCode != asyncapi.ErrCodeCancelled {
			t.Errorf("Expected ErrorCode %q, got %q", asyncapi.ErrCodeCancelled, r.ErrorCode)
		}
		if r.ErrorMessage != "cancelled" {
			t.Errorf("Expected ErrorMessage 'cancelled', got %q", r.ErrorMessage)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for cancelled result")
	}

	if called.Load() != 0 {
		t.Fatalf("expected inference client to be skipped, got %d calls", called.Load())
	}
}

func TestWorker_CancellationCheckErrorRequeuesRequest(t *testing.T) {
	msgID := "cancel-check-error"
	var called atomic.Int32
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		called.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       nil,
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	requestToken := "token-cancel-check-error"
	ctx := WithCancellationChecker(context.Background(), &stubCancellationChecker{
		err: fmt.Errorf("redis unavailable"),
	})

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmbR(asyncapi.InternalRouting{RequestToken: requestToken}, asyncapi.RequestMessage{
		ID:       msgID,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case retryMsg := <-retryChannel:
		if retryMsg.PublicRequest.ReqID() != msgID {
			t.Fatalf("expected retry for %q, got %q", msgID, retryMsg.PublicRequest.ReqID())
		}
		if retryMsg.BackoffDurationSeconds != cancellationCheckRetryAfterSecond {
			t.Fatalf("expected backoff %f, got %f", cancellationCheckRetryAfterSecond, retryMsg.BackoffDurationSeconds)
		}
	case result := <-resultChannel:
		t.Fatalf("expected requeue on cancellation check failure, got result %+v", result)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for re-enqueued request")
	}

	if called.Load() != 0 {
		t.Fatalf("expected inference client to be skipped, got %d calls", called.Load())
	}
}

func TestWorker_FastPathChecksCancellationOnce(t *testing.T) {
	msgID := "fast-path-cancel-check"
	var called atomic.Int32
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		called.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"ok":true}`))),
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	requestToken := "token-fast-path-cancel-check"
	checker := &stubCancellationChecker{cancelled: map[string]bool{}}
	ctx := WithCancellationChecker(context.Background(), checker)

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmbR(asyncapi.InternalRouting{RequestToken: requestToken}, asyncapi.RequestMessage{
		ID:       msgID,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case <-retryChannel:
		t.Fatal("request should not be retried")
	case result := <-resultChannel:
		if result.ID != msgID {
			t.Fatalf("expected result ID %q, got %q", msgID, result.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for successful result")
	}

	if got := checker.checkCount(); got != 1 {
		t.Fatalf("expected exactly 1 cancellation check on fast path, got %d", got)
	}
	if called.Load() != 1 {
		t.Fatalf("expected exactly 1 inference call, got %d", called.Load())
	}
}

func TestWorker_PoolGateActionWaitThrottlesCancellationChecks(t *testing.T) {
	var called atomic.Int32
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		called.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	checker := &stubCancellationChecker{cancelled: map[string]bool{}}
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	ctx := WithCancellationChecker(requestCtx, checker)
	gate := &notifyingWaitGate{applied: make(chan struct{})}

	go WorkerWithGate(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil, gate)

	requestChannel <- newEmbR(asyncapi.InternalRouting{RequestToken: "token-gate-wait-throttled"}, asyncapi.RequestMessage{
		ID:       "gate-wait-throttled",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(30 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-gate.applied:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never entered ActionWait")
	}

	time.Sleep(220 * time.Millisecond)
	cancelRequest()

	select {
	case <-retryChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("expected request to be re-enqueued after shutdown")
	}

	if got := checker.checkCount(); got > 1 {
		t.Fatalf("expected throttled cancellation checks during ActionWait, got %d", got)
	}
	if called.Load() != 0 {
		t.Fatalf("expected inference client to be skipped, got %d calls", called.Load())
	}
}

func TestFatalError_NoRetry(t *testing.T) {
	msgId := "456"
	// Simulate a transport error (fatal)
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("network unreachable")
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case <-retryChannel:
		t.Errorf("Should not retry a fatal error")
	case r := <-resultChannel:
		if r.ID != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.ID)
		}
		if r.StatusCode != 0 {
			t.Errorf("Expected StatusCode 0 for non-HTTP error, got %d", r.StatusCode)
		}
		if r.ErrorCode != asyncapi.ErrCodeInferenceError {
			t.Errorf("Expected ErrorCode %q, got %q", asyncapi.ErrCodeInferenceError, r.ErrorCode)
		}
		if r.ErrorMessage == "" {
			t.Errorf("Expected non-empty ErrorMessage for non-HTTP error")
		}
		var resultMap map[string]any
		err := json.Unmarshal([]byte(r.Payload), &resultMap)
		if err != nil {
			t.Errorf("Failed to unmarshal result payload: %s. Payload was: %s", err, r.Payload)
		}
		if _, hasError := resultMap["error"]; !hasError {
			t.Errorf("Expected error in result payload, got: %s", r.Payload)
		}
	case <-time.After(time.Second):
		t.Errorf("Timeout waiting for result")
	}
}

func TestRateLimitRequest(t *testing.T) {
	msgId := "789"
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       nil,
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case r := <-retryChannel:
		if r.PublicRequest == nil || r.PublicRequest.ReqID() != msgId {
			t.Errorf("Expected retry message id to be %s, got %v", msgId, r.PublicRequest)
		}
	case <-resultChannel:
		t.Errorf("Should not get result from a 429 response, should retry")
	case <-time.After(time.Second):
		t.Errorf("Timeout waiting for retry")
	}
}

func TestRequestTimeout(t *testing.T) {
	msgId := "timeout-test"
	// Simulate a slow server that blocks longer than the request timeout.
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	// Use a very short request timeout to trigger the deadline.
	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, 100*time.Millisecond, nil)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case r := <-resultChannel:
		// The request should fail due to context deadline exceeded (fatal unknown error).
		if r.ID != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.ID)
		}
	case <-retryChannel:
		// Context cancellation errors are wrapped as ErrCategoryUnknown (fatal), so no retry.
		t.Errorf("Timed-out request should not be retried")
	case <-time.After(5 * time.Second):
		t.Errorf("Worker did not return within 5s — per-request timeout was not enforced")
	}
}

func TestExpBackoffDuration(t *testing.T) {
	const iterations = 1000

	t.Run("normal backoff grows exponentially", func(t *testing.T) {
		deadline := 300
		for retry := 0; retry < 5; retry++ {
			expectedTemp := math.Min(float64(maxDelaySeconds), float64(baseDelaySeconds)*math.Pow(2, float64(retry)))
			lo := expectedTemp / 2
			hi := expectedTemp

			for i := 0; i < iterations; i++ {
				got := expBackoffDuration(retry, deadline)
				if got < lo || got >= hi {
					t.Errorf("retry=%d: got %f, want [%f, %f)", retry, got, lo, hi)
				}
			}
		}
	})

	t.Run("capped by maxDelaySeconds", func(t *testing.T) {
		deadline := 300
		// retry=10 → baseDelay*2^10 = 2048, far above maxDelaySeconds=60
		for i := 0; i < iterations; i++ {
			got := expBackoffDuration(10, deadline)
			if got < float64(maxDelaySeconds)/2 || got >= float64(maxDelaySeconds) {
				t.Errorf("got %f, want [%f, %f)", got, float64(maxDelaySeconds)/2, float64(maxDelaySeconds))
			}
		}
	})

	t.Run("capped by secondsToDeadline", func(t *testing.T) {
		deadline := 3
		// retry=10 → exponential is huge, but capped to deadline=3
		for i := 0; i < iterations; i++ {
			got := expBackoffDuration(10, deadline)
			if got < float64(deadline)/2 || got >= float64(deadline) {
				t.Errorf("got %f, want [%f, %f)", got, float64(deadline)/2, float64(deadline))
			}
		}
	})

	t.Run("small deadline respected over baseDelay", func(t *testing.T) {
		// secondsToDeadline=1 → cap=1, temp=1, result in [0.5, 1.0)
		for i := 0; i < iterations; i++ {
			got := expBackoffDuration(1, 1)
			if got < 0.5 || got >= 1.0 {
				t.Errorf("got %f, want [0.5, 1.0)", got)
			}
		}
	})

	t.Run("zero deadline returns zero", func(t *testing.T) {
		got := expBackoffDuration(1, 0)
		if got != 0 {
			t.Errorf("got %f, want 0", got)
		}
	})

	t.Run("negative deadline returns zero", func(t *testing.T) {
		got := expBackoffDuration(1, -5)
		if got != 0 {
			t.Errorf("got %f, want 0", got)
		}
	})
}

func TestRetryMessage_deadlineExact(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	now := time.Now().Unix()
	msg := newEmb(asyncapi.RequestMessage{
		ID:       "exact-deadline",
		Created:  now,
		Deadline: now, // same second → no time left to retry
	}, "", nil)
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 0, asyncapi.InferenceResponse{})
	if len(retryChannel) > 0 {
		t.Errorf("secondsToDeadline==0 should not produce a retry")
	}
	if len(resultChannel) != 1 {
		t.Errorf("expected deadline-exceeded result")
		return
	}
	result := <-resultChannel
	var resultMap map[string]any
	json.Unmarshal([]byte(result.Payload), &resultMap) // nolint:errcheck
	if resultMap["error"] != "deadline exceeded" {
		t.Errorf("expected 'deadline exceeded', got: %s", resultMap["error"])
	}
}

func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		wantOK   bool
		wantZero bool
	}{
		{name: "integer seconds", value: "120", wantOK: true},
		{name: "zero seconds", value: "0", wantOK: true, wantZero: true},
		{name: "HTTP-date future", value: time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat), wantOK: true},
		{name: "HTTP-date past", value: time.Now().Add(-10 * time.Second).UTC().Format(http.TimeFormat), wantOK: true, wantZero: true},
		{name: "negative integer", value: "-5", wantOK: false},
		{name: "invalid value", value: "abc", wantOK: false},
		{name: "empty string", value: "", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, ok := parseRetryAfter(tt.value)
			if ok != tt.wantOK {
				t.Fatalf("parseRetryAfter(%q): ok = %v, want %v", tt.value, ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if tt.wantZero && d != 0 {
				t.Errorf("expected zero duration, got %v", d)
			}
			if !tt.wantZero && d <= 0 {
				t.Errorf("expected positive duration, got %v", d)
			}
		})
	}
}

func TestRetryMessage_retryAfterHonored(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	msg := newEmb(asyncapi.RequestMessage{
		ID:       "retry-after-test",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
	}, "", nil)

	// Server says wait 30s; expBackoff for retry 1 would be ~[1,2) seconds,
	// so the Retry-After value should win.
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 30*time.Second, asyncapi.InferenceResponse{})
	if len(retryChannel) != 1 {
		t.Fatalf("expected one message in retry channel, got %d", len(retryChannel))
	}
	retryMsg := <-retryChannel
	if retryMsg.BackoffDurationSeconds < 30 {
		t.Errorf("expected backoff >= 30s (Retry-After), got %f", retryMsg.BackoffDurationSeconds)
	}
}

func TestRetryMessage_retryAfterIgnoredWhenSmaller(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	msg := newEmbR(asyncapi.InternalRouting{RetryCount: 5}, asyncapi.RequestMessage{
		ID:       "retry-after-small",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
	}, "", nil)

	// Server says wait 1s, but expBackoff at retry 5 is much larger.
	// expBackoff should win.
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 1*time.Second, asyncapi.InferenceResponse{})
	if len(retryChannel) != 1 {
		t.Fatalf("expected one message in retry channel, got %d", len(retryChannel))
	}
	retryMsg := <-retryChannel
	if retryMsg.BackoffDurationSeconds <= 1.0 {
		t.Errorf("expected backoff > 1s (expBackoff should dominate), got %f", retryMsg.BackoffDurationSeconds)
	}
}

func TestRetryMessage_retryAfterExceedsDeadline(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	msg := newEmb(asyncapi.RequestMessage{
		ID:       "retry-after-exceeds-deadline",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(5 * time.Second).Unix(),
	}, "", nil)

	// Server says wait 30s, but deadline is only 5s away → deadline exceeded.
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 30*time.Second, asyncapi.InferenceResponse{})
	if len(retryChannel) > 0 {
		t.Errorf("should not retry when Retry-After exceeds deadline")
	}
	if len(resultChannel) != 1 {
		t.Fatalf("expected deadline-exceeded result, got %d messages", len(resultChannel))
	}
	result := <-resultChannel
	var resultMap map[string]any
	json.Unmarshal([]byte(result.Payload), &resultMap) // nolint:errcheck
	if resultMap["error"] != "deadline exceeded" {
		t.Errorf("expected 'deadline exceeded', got: %s", resultMap["error"])
	}
}

func TestRetryMessage_deadlineExhaustedPreservesHTTPResponse(t *testing.T) {
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	msg := newEmb(asyncapi.RequestMessage{
		ID:       "retry-exhausted-http",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(-1 * time.Second).Unix(),
	}, "", nil)

	lastBody := []byte(`{"error":"too many requests"}`)
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 0, asyncapi.InferenceResponse{StatusCode: 429, Body: lastBody})
	if len(retryChannel) > 0 {
		t.Errorf("should not retry past deadline")
	}
	if len(resultChannel) != 1 {
		t.Fatalf("expected one result, got %d", len(resultChannel))
	}
	result := <-resultChannel
	if result.StatusCode != 429 {
		t.Errorf("StatusCode = %d, want 429", result.StatusCode)
	}
	if result.Payload != string(lastBody) {
		t.Errorf("Payload = %q, want %q", result.Payload, string(lastBody))
	}
	if result.ErrorCode != "" {
		t.Errorf("ErrorCode should be empty for HTTP error result, got %q", result.ErrorCode)
	}
}

func TestRateLimitRequest_WithRetryAfterHeader(t *testing.T) {
	msgId := "429-with-header"
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Retry-After", "25")
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       nil,
			Header:     header,
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case r := <-retryChannel:
		if r.PublicRequest == nil || r.PublicRequest.ReqID() != msgId {
			t.Errorf("expected retry message id %s, got %v", msgId, r.PublicRequest)
		}
		if r.BackoffDurationSeconds < 25 {
			t.Errorf("expected backoff >= 25s (Retry-After header), got %f", r.BackoffDurationSeconds)
		}
	case <-resultChannel:
		t.Errorf("should not get result from a 429 response, should retry")
	case <-time.After(time.Second):
		t.Errorf("timeout waiting for retry")
	}
}

func TestValidateAndMarshal_cancelledCtxDoesNotBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Unbuffered channel: without the select guard this would block forever.
	resultChannel := make(chan asyncapi.ResultMessage)

	emb := newEmb(asyncapi.RequestMessage{
		ID:       "cancel-test",
		Created:  time.Now().Unix(),
		Deadline: 0, // invalid deadline → error path
	}, "", nil)

	done := make(chan struct{})
	go func() {
		validateAndMarshal(ctx, resultChannel, emb, nil)
		close(done)
	}()

	select {
	case <-done:
		// Function returned without blocking — test passes.
	case <-time.After(2 * time.Second):
		t.Fatal("validateAndMarshal blocked on cancelled ctx with full/unbuffered channel")
	}
}

func TestRetryMessage_cancelledCtxDoesNotBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Unbuffered channels: sends would block without the select guard.
	retryChannel := make(chan pipeline.RetryMessage)
	resultChannel := make(chan asyncapi.ResultMessage)

	msg := newEmb(asyncapi.RequestMessage{
		ID:       "cancel-retry-test",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(10 * time.Second).Unix(),
	}, "", nil)

	done := make(chan struct{})
	go func() {
		retryMessage(ctx, msg, retryChannel, resultChannel, 0, asyncapi.InferenceResponse{})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retryMessage blocked on cancelled ctx with unbuffered channels")
	}
}

func TestWorker_cancelledCtxExitsPromptly(t *testing.T) {
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       nil,
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	// Unbuffered result channel: the worker must not block trying to send.
	retryChannel := make(chan pipeline.RetryMessage)
	resultChannel := make(chan asyncapi.ResultMessage)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)
		close(done)
	}()

	deadline := time.Now().Add(100 * time.Second).Unix()
	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       "worker-cancel-test",
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	// Give the worker a moment to pick up the message and attempt the send,
	// then cancel so it must exit via the ctx.Done() branch.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Worker goroutine did not exit after context cancellation")
	}
}

func TestClientError_NoRetry(t *testing.T) {
	msgId := "101112"
	errorBody := `{"error": "invalid request"}`
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(errorBody)),
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx := context.Background()

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)
	deadline := time.Now().Add(time.Second * 100).Unix()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  map[string]any{"model": "food-review", "prompt": "hi", "max_tokens": 10, "temperature": 0},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	select {
	case <-retryChannel:
		t.Errorf("Should not retry a 4xx client error")
	case r := <-resultChannel:
		if r.ID != msgId {
			t.Errorf("Expected result message id to be %s, got %s", msgId, r.ID)
		}
		if r.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected StatusCode 400, got %d", r.StatusCode)
		}
		if r.Payload != errorBody {
			t.Errorf("Expected payload to be response body %q, got %q", errorBody, r.Payload)
		}
		if r.ErrorCode != "" {
			t.Errorf("Expected empty ErrorCode for HTTP error, got %q", r.ErrorCode)
		}
	case <-time.After(time.Second):
		t.Errorf("Timeout waiting for result")
	}
}

func TestWorker_RetriesOnShutdown(t *testing.T) {
	msgId := "shutdown-retry"
	reqStarted := make(chan struct{})
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		close(reqStarted)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithCancel(context.Background())

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(5 * time.Minute).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	<-reqStarted
	cancel()

	select {
	case msg := <-retryChannel:
		if msg.PublicRequest.ReqID() != msgId {
			t.Errorf("Expected retry message id %s, got %s", msgId, msg.PublicRequest.ReqID())
		}
	case r := <-resultChannel:
		t.Errorf("Expected retry, got fatal result: %s", r.Payload)
	case <-time.After(5 * time.Second):
		t.Errorf("Worker did not retry within 5s after shutdown")
	}
}

func TestWorker_DrainsBufferedMessagesOnShutdown(t *testing.T) {
	tests := []struct {
		name        string
		concurrency int
		messages    int
	}{
		{"single worker with buffered messages", 1, 3},
		{"multiple workers fewer than messages", 2, 5},
		{"multiple workers more than messages", 4, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inFlightCount := min(tt.concurrency, tt.messages)
			reqStarted := make(chan struct{}, inFlightCount)
			httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
				select {
				case reqStarted <- struct{}{}:
				default:
				}
				<-req.Context().Done()
				return nil, req.Context().Err()
			})
			inferenceClient := NewHTTPInferenceClient(httpclient)
			requestChannel := make(chan pipeline.EmbelishedRequestMessage, tt.messages)
			retryChannel := make(chan pipeline.RetryMessage)
			resultChannel := make(chan asyncapi.ResultMessage, tt.messages)

			consumeCtx, consumeCancel := context.WithCancel(context.Background())
			requestCtx, requestCancel := context.WithCancel(context.Background())

			got := make(map[string]bool)
			var retryWg sync.WaitGroup
			retryWg.Add(1)
			go func() {
				defer retryWg.Done()
				for msg := range retryChannel {
					got[msg.PublicRequest.ReqID()] = true
				}
			}()

			var wg sync.WaitGroup
			for w := 0; w < tt.concurrency; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					Worker(consumeCtx, requestCtx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)
				}()
			}

			ids := make([]string, tt.messages)
			for i := range tt.messages {
				ids[i] = fmt.Sprintf("drain-%d", i)
				requestChannel <- newEmb(asyncapi.RequestMessage{
					ID:       ids[i],
					Created:  time.Now().Unix(),
					Deadline: time.Now().Add(5 * time.Minute).Unix(),
					Payload:  map[string]any{"model": "test", "prompt": "hi"},
				}, "http://localhost:30800/v1/completions", map[string]string{})
			}

			for range inFlightCount {
				<-reqStarted
			}
			consumeCancel()
			requestCancel()

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Workers did not exit within 5s")
			}

			close(retryChannel)
			retryWg.Wait()

			for _, id := range ids {
				if !got[id] {
					t.Errorf("message %s was not re-queued on shutdown", id)
				}
			}
		})
	}
}

func counterValue(cv *prometheus.CounterVec, queueID, queueName string) float64 {
	c, err := cv.GetMetricWithLabelValues(queueID, queueName, "test-pool")
	if err != nil {
		return 0
	}
	return testutil.ToFloat64(c)
}

func histogramSampleCount(hv *prometheus.HistogramVec, queueID, queueName string) uint64 {
	obs, err := hv.GetMetricWithLabelValues(queueID, queueName, "test-pool")
	if err != nil {
		return 0
	}
	m := &dto.Metric{}
	if err := obs.(prometheus.Metric).Write(m); err != nil {
		return 0
	}
	return m.GetHistogram().GetSampleCount()
}

func gaugeValue(gv *prometheus.GaugeVec, queueID, queueName string) float64 {
	g, err := gv.GetMetricWithLabelValues(queueID, queueName, "test-pool")
	if err != nil {
		return 0
	}
	return testutil.ToFloat64(g)
}

// waitForGauge polls a gauge until it reaches want or the timeout elapses,
// avoiding a race against the worker's deferred decrement.
func waitForGauge(t *testing.T, gv *prometheus.GaugeVec, queueID, queueName string, want float64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := gaugeValue(gv, queueID, queueName)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gauge for (%s,%s) = %f, want %f", queueID, queueName, got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestMetrics_QueueDepthAndInflightBalance verifies the worker's gauge
// bookkeeping: every message read decrements async_queue_depth (the merge
// policy is responsible for the increment, simulated here), and
// async_inflight_requests is bracketed around handling so it returns to zero on
// every exit path (success, retry, and shutdown drain).
func TestMetrics_QueueDepthAndInflightBalance(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		netErr     bool
		drain      bool // cancel consumeCtx before the worker reads, exercising the drain path
	}{
		{name: "success decrements depth and clears inflight", statusCode: http.StatusOK},
		{name: "retry decrements depth and clears inflight", statusCode: http.StatusTooManyRequests},
		{name: "fatal error decrements depth and clears inflight", netErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queueID := "depth-" + tt.name
			queueName := queueID

			httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
				if tt.netErr {
					return nil, fmt.Errorf("connection refused")
				}
				return &http.Response{StatusCode: tt.statusCode, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
			})
			inferenceClient := NewHTTPInferenceClient(httpclient)
			requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
			retryChannel := make(chan pipeline.RetryMessage, 1)
			resultChannel := make(chan asyncapi.ResultMessage, 1)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

			// Simulate the merge policy's increment for one buffered request.
			metrics.IncQueueDepth(queueID, queueName, "test-pool")
			if got := gaugeValue(metrics.QueueDepth, queueID, queueName); got != 1 {
				t.Fatalf("queue depth before processing = %f, want 1", got)
			}

			requestChannel <- newEmbR(asyncapi.InternalRouting{
				QueueID:          queueID,
				RequestQueueName: queueName,
			}, asyncapi.RequestMessage{
				ID:       "depth-msg",
				Created:  time.Now().Unix(),
				Deadline: time.Now().Add(100 * time.Second).Unix(),
				Payload:  map[string]any{"model": "test", "prompt": "hi"},
			}, "http://localhost:30800/v1/completions", nil)

			// Wait for the terminal outcome so handling has completed.
			select {
			case <-resultChannel:
			case <-retryChannel:
			case <-time.After(2 * time.Second):
				t.Fatal("timeout waiting for worker to process message")
			}

			// Depth must return to 0 (decremented on read) and inflight to 0
			// (deferred decrement on every exit path).
			waitForGauge(t, metrics.QueueDepth, queueID, queueName, 0)
			waitForGauge(t, metrics.InflightRequests, queueID, queueName, 0)
		})
	}
}

// TestMetrics_QueueDepthDecrementsOnDrain verifies the worker's drain path
// (consumeCtx cancelled) also decrements async_queue_depth for each buffered
// message it re-enqueues, and never marks them inflight.
func TestMetrics_QueueDepthDecrementsOnDrain(t *testing.T) {
	queueID := "depth-drain-qid"
	queueName := "depth-drain-queue"

	// A blocking client: any message that reaches the normal (in-flight) path
	// stays there until requestCtx is cancelled, at which point it re-enqueues.
	// Combined with cancelling consumeCtx, every message ends up on the retry
	// channel regardless of which path the worker's select happens to take.
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)

	const n = 3
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, n)
	retryChannel := make(chan pipeline.RetryMessage, n)
	resultChannel := make(chan asyncapi.ResultMessage, n)

	consumeCtx, consumeCancel := context.WithCancel(context.Background())
	requestCtx, requestCancel := context.WithCancel(context.Background())

	// Buffer n messages and account for them as the merge policy would, then
	// cancel both contexts so the worker drains/re-enqueues every message.
	for i := 0; i < n; i++ {
		metrics.IncQueueDepth(queueID, queueName, "test-pool")
		requestChannel <- newEmbR(asyncapi.InternalRouting{
			QueueID:          queueID,
			RequestQueueName: queueName,
		}, asyncapi.RequestMessage{
			ID:       fmt.Sprintf("drain-depth-%d", i),
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload:  map[string]any{"model": "test", "prompt": "hi"},
		}, "http://localhost:30800/v1/completions", nil)
	}
	consumeCancel()
	requestCancel()

	done := make(chan struct{})
	go func() {
		Worker(consumeCtx, requestCtx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)
		close(done)
	}()

	for i := 0; i < n; i++ {
		select {
		case <-retryChannel:
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for re-enqueued message %d", i)
		}
	}

	// Depth must drain to 0 on both the normal and drain read paths, and
	// inflight must settle back to 0 (the decrement is deferred until after the
	// re-enqueue send, so poll rather than read once).
	waitForGauge(t, metrics.QueueDepth, queueID, queueName, 0)
	waitForGauge(t, metrics.InflightRequests, queueID, queueName, 0)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit after drain")
	}
}

func TestMetrics_SuccessfulRequest(t *testing.T) {
	queueID := "metrics-success-qid"
	queueName := "metrics-success-queue"

	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	emb := newEmbR(asyncapi.InternalRouting{
		QueueID:          queueID,
		RequestQueueName: queueName,
	}, asyncapi.RequestMessage{
		ID:       "m-success",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)
	// Stamp ingestion time so the worker records queue residence time, mirroring
	// what the broker producers do when a message enters the in-process buffer.
	emb.IngestionTime = time.Now()
	requestChannel <- emb

	select {
	case <-resultChannel:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	if got := counterValue(metrics.AsyncReqs, queueID, queueName); got < 1 {
		t.Errorf("AsyncReqs(%s,%s) = %f, want >= 1", queueID, queueName, got)
	}
	if got := counterValue(metrics.SuccessfulReqs, queueID, queueName); got < 1 {
		t.Errorf("SuccessfulReqs(%s,%s) = %f, want >= 1", queueID, queueName, got)
	}
	if got := histogramSampleCount(metrics.InferenceLatencyTime, queueID, queueName); got < 1 {
		t.Errorf("InferenceLatencyTime(%s,%s) sample count = %d, want >= 1", queueID, queueName, got)
	}
	if got := histogramSampleCount(metrics.QueueResidenceTime, queueID, queueName); got < 1 {
		t.Errorf("QueueResidenceTime(%s,%s) sample count = %d, want >= 1", queueID, queueName, got)
	}
	if got := counterValue(metrics.FailedReqs, queueID, queueName); got != 0 {
		t.Errorf("FailedReqs(%s,%s) = %f, want 0", queueID, queueName, got)
	}
	if got := counterValue(metrics.SheddedRequests, queueID, queueName); got != 0 {
		t.Errorf("SheddedRequests(%s,%s) = %f, want 0", queueID, queueName, got)
	}
}

func TestMetrics_RateLimited(t *testing.T) {
	queueID := "metrics-shedded-qid"
	queueName := "metrics-shedded-queue"

	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmbR(asyncapi.InternalRouting{
		QueueID:          queueID,
		RequestQueueName: queueName,
	}, asyncapi.RequestMessage{
		ID:       "m-shedded",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-retryChannel:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	if got := counterValue(metrics.SheddedRequests, queueID, queueName); got < 1 {
		t.Errorf("SheddedRequests(%s,%s) = %f, want >= 1", queueID, queueName, got)
	}
	if got := counterValue(metrics.Retries, queueID, queueName); got < 1 {
		t.Errorf("Retries(%s,%s) = %f, want >= 1", queueID, queueName, got)
	}
}

func TestMetrics_FatalError(t *testing.T) {
	queueID := "metrics-fatal-qid"
	queueName := "metrics-fatal-queue"

	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("connection refused")
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmbR(asyncapi.InternalRouting{
		QueueID:          queueID,
		RequestQueueName: queueName,
	}, asyncapi.RequestMessage{
		ID:       "m-fatal",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-resultChannel:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	if got := counterValue(metrics.FailedReqs, queueID, queueName); got < 1 {
		t.Errorf("FailedReqs(%s,%s) = %f, want >= 1", queueID, queueName, got)
	}
	if got := counterValue(metrics.SuccessfulReqs, queueID, queueName); got != 0 {
		t.Errorf("SuccessfulReqs(%s,%s) = %f, want 0", queueID, queueName, got)
	}
}

func TestMetrics_DeadlineExceeded(t *testing.T) {
	queueID := "metrics-deadline-qid"
	queueName := "metrics-deadline-queue"

	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmbR(asyncapi.InternalRouting{
		QueueID:          queueID,
		RequestQueueName: queueName,
	}, asyncapi.RequestMessage{
		ID:       "m-deadline",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(-10 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-resultChannel:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	if got := counterValue(metrics.ExceededDeadlineReqs, queueID, queueName); got < 1 {
		t.Errorf("ExceededDeadlineReqs(%s,%s) = %f, want >= 1", queueID, queueName, got)
	}
}

func TestMetrics_LabelsIsolated(t *testing.T) {
	queueA := "metrics-iso-a"
	queueB := "metrics-iso-b"

	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	deadline := time.Now().Add(100 * time.Second).Unix()

	requestChannel <- newEmbR(asyncapi.InternalRouting{
		QueueID: queueA, RequestQueueName: queueA,
	}, asyncapi.RequestMessage{
		ID: "iso-a", Created: time.Now().Unix(), Deadline: deadline,
		Payload: map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-resultChannel:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message A")
	}

	requestChannel <- newEmbR(asyncapi.InternalRouting{
		QueueID: queueB, RequestQueueName: queueB,
	}, asyncapi.RequestMessage{
		ID: "iso-b", Created: time.Now().Unix(), Deadline: deadline,
		Payload: map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-resultChannel:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message B")
	}

	aCount := counterValue(metrics.SuccessfulReqs, queueA, queueA)
	bCount := counterValue(metrics.SuccessfulReqs, queueB, queueB)
	if aCount < 1 {
		t.Errorf("SuccessfulReqs for queue A = %f, want >= 1", aCount)
	}
	if bCount < 1 {
		t.Errorf("SuccessfulReqs for queue B = %f, want >= 1", bCount)
	}
}

// --- OTel span verification tests ---

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

func findSpan(spans tracetest.SpanStubs, name string) *tracetest.SpanStub {
	for i := range spans {
		if spans[i].Name == name {
			return &spans[i]
		}
	}
	return nil
}

func spanAttr(s *tracetest.SpanStub, key string) string {
	for _, attr := range s.Attributes {
		if string(attr.Key) == key {
			return attr.Value.String()
		}
	}
	return ""
}

func getSpansEventually(t *testing.T, exporter *tracetest.InMemoryExporter, expectedCount int) tracetest.SpanStubs {
	t.Helper()
	for i := 0; i < 100; i++ {
		spans := exporter.GetSpans()
		if len(spans) >= expectedCount {
			return spans
		}
		time.Sleep(10 * time.Millisecond)
	}
	return exporter.GetSpans()
}

func TestWorker_SpanOnSuccess(t *testing.T) {
	exporter := setupTestTracer(t)
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "span-success", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-resultChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	spans := getSpansEventually(t, exporter, 1)
	s := findSpan(spans, "process-request")
	if s == nil {
		t.Fatal("expected 'process-request' span")
	}
	if spanAttr(s, uotel.AttrRequestID) != "span-success" {
		t.Errorf("expected request.id=span-success, got %s", spanAttr(s, uotel.AttrRequestID))
	}
	if spanAttr(s, uotel.AttrRetryCount) != "0" {
		t.Errorf("expected retry.count=0, got %s", spanAttr(s, uotel.AttrRetryCount))
	}
	if s.Status.Code == codes.Error {
		t.Error("expected non-error status on success span")
	}
}

func TestWorker_SpanOnFatalError(t *testing.T) {
	exporter := setupTestTracer(t)
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("network unreachable")
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "span-fatal", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-resultChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	spans := getSpansEventually(t, exporter, 1)
	s := findSpan(spans, "process-request")
	if s == nil {
		t.Fatal("expected 'process-request' span")
	}
	if s.Status.Code != codes.Error {
		t.Error("expected error status on fatal error span")
	}
	if spanAttr(s, uotel.AttrErrorCategory) != "UNKNOWN" {
		t.Errorf("expected error.category=UNKNOWN, got %s", spanAttr(s, uotel.AttrErrorCategory))
	}
	if len(s.Events) == 0 {
		t.Error("expected recorded error event on span")
	}
}

func TestWorker_SpanOnRetryableError(t *testing.T) {
	exporter := setupTestTracer(t)
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "span-429", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-retryChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for retry")
	}

	spans := getSpansEventually(t, exporter, 1)
	s := findSpan(spans, "process-request")
	if s == nil {
		t.Fatal("expected 'process-request' span")
	}
	if spanAttr(s, uotel.AttrErrorCategory) != "RATE_LIMIT" {
		t.Errorf("expected error.category=RATE_LIMIT, got %s", spanAttr(s, uotel.AttrErrorCategory))
	}
	if s.Status.Code == codes.Error {
		t.Error("retryable error should not set span error status")
	}
}

func TestWorker_SpanOnServerError(t *testing.T) {
	exporter := setupTestTracer(t)
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "span-500", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-retryChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for retry")
	}

	spans := getSpansEventually(t, exporter, 1)
	s := findSpan(spans, "process-request")
	if s == nil {
		t.Fatal("expected 'process-request' span")
	}
	if spanAttr(s, uotel.AttrErrorCategory) != "SERVER_ERROR" {
		t.Errorf("expected error.category=SERVER_ERROR, got %s", spanAttr(s, uotel.AttrErrorCategory))
	}
}

func TestWorker_TraceContextExtraction(t *testing.T) {
	exporter := setupTestTracer(t)
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a parent span and inject its context into metadata
	parentCtx, parentSpan := otel.Tracer("test").Start(ctx, "parent-operation")
	parentTraceID := parentSpan.SpanContext().TraceID()
	metadata := make(map[string]string)
	otel.GetTextMapPropagator().Inject(parentCtx, propagation.MapCarrier(metadata))
	parentSpan.End()

	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "span-ctx", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
		Metadata: metadata,
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-resultChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	spans := getSpansEventually(t, exporter, 1)
	s := findSpan(spans, "process-request")
	if s == nil {
		t.Fatal("expected 'process-request' span")
	}
	if s.SpanContext.TraceID() != parentTraceID {
		t.Errorf("expected process-request span to share parent trace ID %s, got %s",
			parentTraceID, s.SpanContext.TraceID())
	}
}

func TestWorker_SpanOnShutdownReenqueue(t *testing.T) {
	exporter := setupTestTracer(t)
	reqStarted := make(chan struct{})
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		close(reqStarted)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithCancel(context.Background())
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "span-shutdown", Created: time.Now().Unix(), Deadline: time.Now().Add(5 * time.Minute).Unix(),
		Payload: map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	<-reqStarted
	cancel()

	select {
	case <-retryChannel:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for retry")
	}

	spans := getSpansEventually(t, exporter, 2)
	processSpan := findSpan(spans, "process-request")
	if processSpan == nil {
		t.Fatal("expected 'process-request' span")
	}
	reenqueueSpan := findSpan(spans, "re-enqueue")
	if reenqueueSpan == nil {
		t.Fatal("expected 're-enqueue' span")
	}
	if len(reenqueueSpan.Links) == 0 {
		t.Error("expected re-enqueue span to have a link to process-request span")
	} else {
		linked := false
		for _, link := range reenqueueSpan.Links {
			if link.SpanContext.SpanID() == processSpan.SpanContext.SpanID() {
				linked = true
				break
			}
		}
		if !linked {
			t.Error("re-enqueue span link does not reference process-request span")
		}
	}
	if spanAttr(reenqueueSpan, uotel.AttrRequestID) != "span-shutdown" {
		t.Errorf("expected request.id=span-shutdown on re-enqueue span, got %s", spanAttr(reenqueueSpan, uotel.AttrRequestID))
	}
}

func TestWorker_SpanIncludesQueueName(t *testing.T) {
	exporter := setupTestTracer(t)
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)

	requestChannel <- newEmbR(
		asyncapi.InternalRouting{QueueID: "my-test-qid", RequestQueueName: "my-test-queue"},
		asyncapi.RequestMessage{
			ID: "span-queue", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
			Payload: map[string]any{"model": "test", "prompt": "hi"},
		}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-resultChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	spans := getSpansEventually(t, exporter, 1)
	s := findSpan(spans, "process-request")
	if s == nil {
		t.Fatal("expected 'process-request' span")
	}
	if spanAttr(s, uotel.AttrQueueID) != "my-test-qid" {
		t.Errorf("expected queue.id=my-test-qid, got %s", spanAttr(s, uotel.AttrQueueID))
	}
	if spanAttr(s, uotel.AttrQueueName) != "my-test-queue" {
		t.Errorf("expected queue.name=my-test-queue, got %s", spanAttr(s, uotel.AttrQueueName))
	}
}

func TestWorker_InFlightCompletesOnConsumeCancel(t *testing.T) {
	msgId := "inflight-complete"
	reqStarted := make(chan struct{})
	reqDone := make(chan struct{})
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		close(reqStarted)
		<-reqDone
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(`{"result":"ok"}`)),
			Header:     make(http.Header),
		}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	consumeCtx, consumeCancel := context.WithCancel(context.Background())
	requestCtx, requestCancel := context.WithCancel(context.Background())
	defer requestCancel()

	done := make(chan struct{})
	go func() {
		Worker(consumeCtx, requestCtx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)
		close(done)
	}()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(5 * time.Minute).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	<-reqStarted
	consumeCancel()
	close(reqDone)

	select {
	case r := <-resultChannel:
		if r.ID != msgId {
			t.Errorf("Expected result message id %s, got %s", msgId, r.ID)
		}
	case msg := <-retryChannel:
		t.Errorf("Expected successful result, got retry for %s", msg.PublicRequest.ReqID())
	case <-time.After(5 * time.Second):
		t.Fatal("Worker did not return result within 5s")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Worker did not exit after consumeCtx cancel and request completion")
	}
}

func TestWorker_DrainTimeoutCancelsInFlight(t *testing.T) {
	msgId := "drain-timeout"
	reqStarted := make(chan struct{})
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		close(reqStarted)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	consumeCtx, consumeCancel := context.WithCancel(context.Background())
	requestCtx, requestCancel := context.WithCancel(context.Background())

	var retryMsg pipeline.RetryMessage
	var gotRetry bool
	retryDone := make(chan struct{})
	go func() {
		defer close(retryDone)
		for msg := range retryChannel {
			retryMsg = msg
			gotRetry = true
		}
	}()

	done := make(chan struct{})
	go func() {
		Worker(consumeCtx, requestCtx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)
		close(done)
	}()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       msgId,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(5 * time.Minute).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", map[string]string{})

	<-reqStarted
	consumeCancel()
	requestCancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Worker did not exit within 5s after drain timeout")
	}

	close(retryChannel)
	<-retryDone

	if !gotRetry {
		t.Fatal("Worker did not re-enqueue the in-flight message")
	}
	if retryMsg.PublicRequest.ReqID() != msgId {
		t.Errorf("Expected retry message id %s, got %s", msgId, retryMsg.PublicRequest.ReqID())
	}
	if retryMsg.BackoffDurationSeconds != 0 {
		t.Errorf("Expected zero backoff on shutdown re-enqueue, got %f", retryMsg.BackoffDurationSeconds)
	}
}

func TestWorker_DrainWithCancelledRequestCtx(t *testing.T) {
	const totalMessages = 4
	reqStarted := make(chan struct{}, 1)
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		select {
		case reqStarted <- struct{}{}:
		default:
		}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, totalMessages)
	retryChannel := make(chan pipeline.RetryMessage)
	resultChannel := make(chan asyncapi.ResultMessage, totalMessages)

	consumeCtx, consumeCancel := context.WithCancel(context.Background())
	requestCtx, requestCancel := context.WithCancel(context.Background())

	got := make(map[string]bool)
	var mu sync.Mutex
	retryDone := make(chan struct{})
	go func() {
		defer close(retryDone)
		for msg := range retryChannel {
			mu.Lock()
			got[msg.PublicRequest.ReqID()] = true
			mu.Unlock()
		}
	}()

	done := make(chan struct{})
	go func() {
		Worker(consumeCtx, requestCtx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)
		close(done)
	}()

	ids := make([]string, totalMessages)
	for i := range totalMessages {
		ids[i] = fmt.Sprintf("drain-ctx-%d", i)
		requestChannel <- newEmb(asyncapi.RequestMessage{
			ID:       ids[i],
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload:  map[string]any{"model": "test", "prompt": "hi"},
		}, "http://localhost:30800/v1/completions", map[string]string{})
	}

	<-reqStarted
	consumeCancel()
	requestCancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Worker did not exit within 5s")
	}

	close(retryChannel)
	<-retryDone

	mu.Lock()
	defer mu.Unlock()
	for _, id := range ids {
		if !got[id] {
			t.Errorf("message %s was not re-queued on shutdown", id)
		}
	}
}

type blockingPoolGate struct {
	unblock chan struct{}
}

func (g *blockingPoolGate) Budget(ctx context.Context) float64 {
	return 1.0
}

func (g *blockingPoolGate) Apply(ctx context.Context, _ *asyncapi.InternalRequest, _ *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	select {
	case <-ctx.Done():
		return pipeline.Verdict{}, ctx.Err()
	case <-g.unblock:
		return pipeline.Continue(), nil
	}
}

type waitingPoolGate struct{}

func (g *waitingPoolGate) Budget(ctx context.Context) float64 {
	return 1.0
}

func (g *waitingPoolGate) Apply(ctx context.Context, _ *asyncapi.InternalRequest, _ *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	return pipeline.Wait(), nil
}

type notifyingWaitGate struct {
	applied  chan struct{}
	notified atomic.Bool
}

func (g *notifyingWaitGate) Budget(ctx context.Context) float64 {
	return 1.0
}

func (g *notifyingWaitGate) Apply(ctx context.Context, _ *asyncapi.InternalRequest, _ *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	if g.applied != nil && !g.notified.Swap(true) {
		close(g.applied)
	}
	return pipeline.Wait(), nil
}

type signalContinueGate struct {
	applied chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *signalContinueGate) Budget(ctx context.Context) float64 {
	return 1.0
}

func (g *signalContinueGate) Apply(ctx context.Context, _ *asyncapi.InternalRequest, _ *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	g.once.Do(func() {
		if g.applied != nil {
			close(g.applied)
		}
	})
	if g.release != nil {
		select {
		case <-g.release:
		case <-ctx.Done():
			return pipeline.Verdict{}, ctx.Err()
		}
	}
	return pipeline.Continue(), nil
}

func TestWorker_PoolGateShutdownReenqueues(t *testing.T) {
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithCancel(context.Background())

	gate := &blockingPoolGate{unblock: make(chan struct{})}

	done := make(chan struct{})
	go func() {
		WorkerWithGate(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil, gate)
		close(done)
	}()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       "gate-shutdown-test",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(30 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	// Let the worker block in poolGate.Apply, then cancel (simulating shutdown).
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Worker did not exit after shutdown cancel")
	}

	if len(resultChannel) > 0 {
		t.Errorf("expected no result (should re-enqueue on shutdown), got a result")
	}
	if len(retryChannel) != 1 {
		t.Fatalf("expected message to be re-enqueued, retry channel len = %d", len(retryChannel))
	}
	retryMsg := <-retryChannel
	if retryMsg.PublicRequest.ReqID() != "gate-shutdown-test" {
		t.Errorf("re-enqueued wrong message: got ID %q", retryMsg.PublicRequest.ReqID())
	}
}

func TestWorker_PoolGateActionWaitShutdownReenqueues(t *testing.T) {
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithCancel(context.Background())
	gate := &waitingPoolGate{}

	done := make(chan struct{})
	go func() {
		WorkerWithGate(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil, gate)
		close(done)
	}()

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID:       "gate-wait-shutdown-test",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(30 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	// Let the worker enter the ActionWait polling loop, then cancel (simulating shutdown).
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Worker did not exit after ActionWait shutdown cancel")
	}

	if len(resultChannel) > 0 {
		t.Errorf("expected no result (should re-enqueue on shutdown), got a result")
	}
	if len(retryChannel) != 1 {
		t.Fatalf("expected message to be re-enqueued, retry channel len = %d", len(retryChannel))
	}
	retryMsg := <-retryChannel
	if retryMsg.PublicRequest.ReqID() != "gate-wait-shutdown-test" {
		t.Errorf("re-enqueued wrong message: got ID %q", retryMsg.PublicRequest.ReqID())
	}
}

func TestWorker_PoolGateActionWaitHonorsCancellation(t *testing.T) {
	msgID := "gate-wait-cancelled"
	var called atomic.Int32
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		called.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	checker := &stubCancellationChecker{cancelled: map[string]bool{}}
	ctx := WithCancellationChecker(context.Background(), checker)
	gate := &notifyingWaitGate{applied: make(chan struct{})}
	requestToken := "token-gate-wait-cancelled"

	go WorkerWithGate(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil, gate)

	requestChannel <- newEmbR(asyncapi.InternalRouting{RequestToken: requestToken}, asyncapi.RequestMessage{
		ID:       msgID,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(30 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-gate.applied:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never entered ActionWait")
	}

	checker.setCancelled(msgID, requestToken, true)
	time.Sleep(cancellationCheckPollInterval + 100*time.Millisecond)

	select {
	case <-retryChannel:
		t.Fatal("cancelled request should not be retried")
	case result := <-resultChannel:
		if result.ID != msgID {
			t.Fatalf("Expected result ID %q, got %q", msgID, result.ID)
		}
		if result.ErrorCode != asyncapi.ErrCodeCancelled {
			t.Fatalf("Expected ErrorCode %q, got %q", asyncapi.ErrCodeCancelled, result.ErrorCode)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for cancelled result")
	}

	if called.Load() != 0 {
		t.Fatalf("expected no inference call after cancellation during ActionWait, got %d", called.Load())
	}
}

func TestWorker_RechecksCancellationAfterGateContinue(t *testing.T) {
	msgID := "gate-continue-cancelled"
	var called atomic.Int32
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		called.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	checker := &stubCancellationChecker{cancelled: map[string]bool{}}
	ctx := WithCancellationChecker(context.Background(), checker)
	gate := &signalContinueGate{
		applied: make(chan struct{}),
		release: make(chan struct{}),
	}
	requestToken := "token-gate-continue-cancelled"

	go WorkerWithGate(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil, gate)

	requestChannel <- newEmbR(asyncapi.InternalRouting{RequestToken: requestToken}, asyncapi.RequestMessage{
		ID:       msgID,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(30 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-gate.applied:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never reached pool gate")
	}

	checker.setCancelled(msgID, requestToken, true)
	time.Sleep(cancellationCheckPollInterval + 100*time.Millisecond)
	close(gate.release)

	select {
	case <-retryChannel:
		t.Fatal("cancelled request should not be retried")
	case result := <-resultChannel:
		if result.ID != msgID {
			t.Fatalf("expected result ID %q, got %q", msgID, result.ID)
		}
		if result.ErrorCode != asyncapi.ErrCodeCancelled {
			t.Fatalf("expected ErrorCode %q, got %q", asyncapi.ErrCodeCancelled, result.ErrorCode)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for cancelled result after gate continue")
	}

	if called.Load() != 0 {
		t.Fatalf("expected no inference call after cancellation post-gate, got %d", called.Load())
	}
}

func TestWorker_RechecksCancellationImmediatelyBeforeSend(t *testing.T) {
	msgID := "pre-send-cancelled"
	var called atomic.Int32
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		called.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)

	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	checker := &stubCancellationChecker{cancelled: map[string]bool{}}
	ctx := WithCancellationChecker(context.Background(), checker)
	requestToken := "token-pre-send-cancelled"
	xform := &blockingTransform{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, transform.NewChain([]transform.RequestTransform{xform}))

	requestChannel <- newEmbR(asyncapi.InternalRouting{RequestToken: requestToken}, asyncapi.RequestMessage{
		ID:       msgID,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(30 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-xform.started:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never entered transform phase")
	}

	checker.setCancelled(msgID, requestToken, true)
	time.Sleep(cancellationCheckPollInterval + 100*time.Millisecond)
	close(xform.release)

	select {
	case <-retryChannel:
		t.Fatal("cancelled request should not be retried")
	case result := <-resultChannel:
		if result.ID != msgID {
			t.Fatalf("expected result ID %q, got %q", msgID, result.ID)
		}
		if result.ErrorCode != asyncapi.ErrCodeCancelled {
			t.Fatalf("expected ErrorCode %q, got %q", asyncapi.ErrCodeCancelled, result.ErrorCode)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for cancelled result before send")
	}

	if called.Load() != 0 {
		t.Fatalf("expected no inference call after cancellation during transform, got %d", called.Load())
	}
}

type raceMockPoolGate struct {
	releaseCalled *int32
}

func (g *raceMockPoolGate) Budget(ctx context.Context) float64 {
	return 1.0
}

func (g *raceMockPoolGate) Apply(ctx context.Context, ir *asyncapi.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	if releases != nil {
		*releases = append(*releases, func() {
			atomic.AddInt32(g.releaseCalled, 1)
		})
	}
	return pipeline.Continue(), nil
}

func TestWorker_QueueGateAndPoolGateRace(t *testing.T) {
	httpclient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	inferenceClient := NewHTTPInferenceClient(httpclient)
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var poolReleaseCalled int32
	var queueReleaseCalled int32

	poolGate := &raceMockPoolGate{
		releaseCalled: &poolReleaseCalled,
	}

	go WorkerWithGate(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil, poolGate)

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{RequestQueueName: "test-queue"},
		&asyncapi.RequestMessage{
			ID:       "req-race-test",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload:  map[string]any{"model": "test"},
		},
	)
	var queueReleases []pipeline.GateReleaseFunc
	queueReleases = append(queueReleases, func() {
		atomic.AddInt32(&queueReleaseCalled, 1)
	})

	requestChannel <- pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		RequestURL:      "http://localhost:30800/v1/completions",
	}

	select {
	case <-resultChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	// Concurrently invoke Release on the queue flow's reference (queueReleases) while the worker exits processMessage
	// which executes its deferred poolReleases cleanup
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, f := range queueReleases {
			if f != nil {
				f()
			}
		}
	}()
	wg.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&queueReleaseCalled) < 1 || atomic.LoadInt32(&poolReleaseCalled) < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for releases: queue=%d pool=%d",
				atomic.LoadInt32(&queueReleaseCalled), atomic.LoadInt32(&poolReleaseCalled))
		}
		time.Sleep(5 * time.Millisecond)
	}

	if atomic.LoadInt32(&queueReleaseCalled) != 1 {
		t.Errorf("expected queue release to be called exactly once, got %d", queueReleaseCalled)
	}
	if atomic.LoadInt32(&poolReleaseCalled) != 1 {
		t.Errorf("expected pool release to be called exactly once, got %d", poolReleaseCalled)
	}
}
