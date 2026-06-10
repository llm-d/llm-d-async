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
	"testing"
	"time"

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"
	uotel "github.com/llm-d-incubation/llm-d-async/internal/otel"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 0)
	if len(retryChannel) > 0 {
		t.Errorf("Message that its deadline passed should not be retried. Got a message in the retry channel")
		return
	}
	if len(resultChannel) != 1 {
		t.Errorf("Expected one message in the result channel")
		return

	}
	result := <-resultChannel
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
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 0)
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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
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
	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, 100*time.Millisecond)
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
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 0)
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
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 30*time.Second)
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
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 1*time.Second)
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
	retryMessage(context.Background(), msg, retryChannel, resultChannel, 30*time.Second)
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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
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
		validateAndMarshal(ctx, resultChannel, emb)
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
		retryMessage(ctx, msg, retryChannel, resultChannel, 0)
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
		Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
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
		expectedPayload := `{"error":"Failed to send request to inference: INVALID_REQ: client error: status code 400"}`
		if r.Payload != expectedPayload {
			t.Errorf("Expected payload to be %s, got %s", expectedPayload, r.Payload)
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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

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
					Worker(consumeCtx, requestCtx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
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
	c, err := cv.GetMetricWithLabelValues(queueID, queueName)
	if err != nil {
		return 0
	}
	return testutil.ToFloat64(c)
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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

	requestChannel <- newEmbR(asyncapi.InternalRouting{
		QueueID:          queueID,
		RequestQueueName: queueName,
	}, asyncapi.RequestMessage{
		ID:       "m-success",
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

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

	go Worker(ctx, ctx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

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
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "span-success", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-resultChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	spans := exporter.GetSpans()
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
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "span-fatal", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-resultChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	spans := exporter.GetSpans()
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
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "span-429", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-retryChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for retry")
	}

	spans := exporter.GetSpans()
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
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

	requestChannel <- newEmb(asyncapi.RequestMessage{
		ID: "span-500", Created: time.Now().Unix(), Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload: map[string]any{"model": "test", "prompt": "hi"},
	}, "http://localhost:30800/v1/completions", nil)

	select {
	case <-retryChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for retry")
	}

	spans := exporter.GetSpans()
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

	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

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

	spans := exporter.GetSpans()
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
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

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

	spans := exporter.GetSpans()
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
	go Worker(ctx, ctx, pipeline.Characteristics{}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)

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

	spans := exporter.GetSpans()
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
		Worker(consumeCtx, requestCtx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
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
		Worker(consumeCtx, requestCtx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
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
		Worker(consumeCtx, requestCtx, pipeline.Characteristics{HasExternalBackoff: false}, inferenceClient, requestChannel, retryChannel, resultChannel, defaultRequestTimeout)
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
